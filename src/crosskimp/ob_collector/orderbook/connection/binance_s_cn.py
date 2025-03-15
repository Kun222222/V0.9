# file: orderbook/connection/binance_s_cn.py

import asyncio
import json
import time
import aiohttp
from websockets import connect
from typing import Dict, List, Optional

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import Exchange, WebSocketState, STATUS_EMOJIS, WEBSOCKET_CONFIG
from crosskimp.ob_collector.orderbook.connection.base_ws_connector import BaseWebsocketConnector

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이낸스 현물 웹소켓 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BINANCE.value  # 거래소 코드
BINANCE_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이낸스 설정

# 웹소켓 연결 설정
WS_URL = BINANCE_CONFIG["ws_url"]  # 웹소켓 URL
PING_INTERVAL = BINANCE_CONFIG["ping_interval"]  # 핑 전송 간격 (초)
PING_TIMEOUT = BINANCE_CONFIG["ping_timeout"]    # 핑 응답 타임아웃 (초)
HEALTH_CHECK_INTERVAL = BINANCE_CONFIG["health_check_interval"]  # 헬스 체크 간격 (초)

# 구독 관련 설정
SUBSCRIBE_CHUNK_SIZE = BINANCE_CONFIG["subscribe_chunk_size"]  # 한 번에 구독할 심볼 수
SUBSCRIBE_DELAY = BINANCE_CONFIG["subscribe_delay"]  # 구독 요청 간 딜레이 (초)
DEPTH_UPDATE_STREAM = BINANCE_CONFIG["depth_update_stream"]  # 깊이 업데이트 스트림 형식

class BinanceWebSocketConnector(BaseWebsocketConnector):
    """
    바이낸스 현물 웹소켓 연결 관리 클래스
    
    바이낸스 현물 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    
    특징:
    - 바이낸스 전용 핑/퐁 메커니즘 사용
    - 재연결 전략 구현
    - 배치 구독 지원
    """
    def __init__(self, settings: dict):
        """
        바이낸스 현물 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, EXCHANGE_CODE)
        
        # 웹소켓 URL 설정
        self.ws_url = WS_URL
        
        # 연결 관련 설정
        self.subscribed_symbols = set()
        self.ws = None
        self.session = None
        
        # 헬스 체크 설정
        self.health_check_interval = HEALTH_CHECK_INTERVAL

    async def _do_connect(self):
        """
        실제 연결 로직 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        self.session = aiohttp.ClientSession()
        self.ws = await connect(
            self.ws_url,
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_TIMEOUT,
            compression=None
        )
        # is_connected와 connection_start_time은 부모 클래스의 connect 메소드에서 설정됨

    async def _after_connect(self):
        """
        연결 후 처리 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        # 재연결 성공 알림
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "connect")
            
        # 재연결 시 이미 구독된 심볼들에 대해 스냅샷 다시 요청
        if self.subscribed_symbols and hasattr(self, 'manager') and self.manager:
            self.log_info(f"재연결 후 스냅샷 다시 요청 (심볼: {len(self.subscribed_symbols)}개)")
            for sym in self.subscribed_symbols:
                snapshot = await self.manager.fetch_snapshot(sym)
                if snapshot:
                    init_res = await self.manager.initialize_orderbook(sym, snapshot)
                    if init_res.is_valid:
                        self.log_info(f"{sym} 재연결 후 스냅샷 초기화 성공")
                        if self.connection_status_callback:
                            self.connection_status_callback(self.exchangename, "snapshot")
                    else:
                        self.log_error(f"{sym} 재연결 후 스냅샷 초기화 실패: {init_res.error_messages}")
                else:
                    self.log_error(f"{sym} 재연결 후 스냅샷 요청 실패")

    async def _prepare_start(self, symbols: List[str]) -> None:
        """
        시작 전 초기화 및 설정 (BaseWebsocketConnector 템플릿 메서드 구현)
        
        Args:
            symbols: 구독할 심볼 목록
        """
        # 필요한 초기화 작업 수행
        pass

    async def _run_message_loop(self, symbols: List[str], tasks: List[asyncio.Task]) -> None:
        """
        메시지 처리 루프 실행 (BaseWebsocketConnector 템플릿 메서드 구현)
        
        Args:
            symbols: 구독한 심볼 목록
            tasks: 실행 중인 백그라운드 태스크 목록
        """
        while not self.stop_event.is_set() and self.is_connected:
            try:
                msg = await asyncio.wait_for(
                    self.ws.recv(), timeout=self.health_check_interval
                )
                self.stats.last_message_time = time.time()
                self.stats.message_count += 1

                # 메시지 처리 (자식 클래스에서 구현)
                await self.process_message(msg)
                    
            except asyncio.TimeoutError:
                # 타임아웃은 정상적인 상황일 수 있음 (메시지가 없는 경우)
                continue
                
            except Exception as e:
                self.log_error(f"메시지 루프 오류: {str(e)}")
                # 연결 오류 발생 시 루프 종료 (부모 클래스의 start 메소드에서 재연결 처리)
                break

    async def subscribe(self, symbols: List[str]):
        """
        지정된 심볼 목록을 구독
        
        Args:
            symbols: 구독할 심볼 목록
        """
        if not symbols:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "warning")
            return

        chunk_size = SUBSCRIBE_CHUNK_SIZE
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i+chunk_size]
            sub_params = [f"{sym.lower()}usdt{DEPTH_UPDATE_STREAM}" for sym in chunk]
            msg = {
                "method": "SUBSCRIBE",
                "params": sub_params,
                "id": int(time.time() * 1000)
            }
            await self.ws.send(json.dumps(msg))
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe")
            self.log_info(f"{len(chunk)}개 심볼 구독 요청 전송")
            await asyncio.sleep(SUBSCRIBE_DELAY)

        # 구독된 심볼 추적
        for sym in symbols:
            self.subscribed_symbols.add(sym)

    async def process_message(self, message: str) -> None:
        """
        수신된 메시지 처리 (자식 클래스에서 구현)
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        # 이 메서드는 자식 클래스에서 구현해야 함
        pass

    async def stop(self) -> None:
        """
        웹소켓 연결 종료
        """
        self.log_info(f"웹소켓 연결 종료 중... {STATUS_EMOJIS['DISCONNECTING']}")
        await super().stop()
        self.log_info(f"웹소켓 연결 종료 완료 {STATUS_EMOJIS['DISCONNECTED']}")
        
    def set_manager(self, manager):
        """
        오더북 매니저 설정
        
        Args:
            manager: 오더북 매니저 객체
        """
        self.manager = manager
        self.log_info("오더북 매니저 설정 완료") 