# file: orderbook/connection/binance_f_cn.py

import asyncio
import json
import time
from typing import Dict, List, Optional, Any
import websockets
from websockets import connect

from crosskimp.common.logger.logger import get_unified_logger
# constants_v3 대신 새로운 모듈에서 Exchange만 가져오기
from crosskimp.common.config.common_constants import Exchange

from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, ReconnectStrategy, WebSocketStats

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이낸스 선물 웹소켓 연결 관련 상수
# ============================
# 웹소켓 연결 설정
WS_URL = "wss://fstream.binance.com/ws"  # 웹소켓 URL
PING_INTERVAL = 30  # 핑 전송 간격 (초)
PING_TIMEOUT = 10   # 핑 응답 타임아웃 (초)
MESSAGE_TIMEOUT = 60  # 메시지 타임아웃 (초)
RECONNECT_DELAY = 0.1  # 초기 재연결 시도 시간 (초)
CONNECTION_TIMEOUT = 3  # 연결 타임아웃 (초)

class BinanceFutureWebSocketConnector(BaseWebsocketConnector):
    """
    바이낸스 선물 웹소켓 연결 관리 클래스
    
    바이낸스 선물 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    """
    def __init__(self, settings: dict, exchange_code: str = None, on_status_change=None):
        """
        바이낸스 선물 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
            exchange_code: 거래소 코드 (기본값: None, 자동으로 설정)
            on_status_change: 연결 상태 변경 시 호출될 콜백 함수
        """
        exchange_code = exchange_code or Exchange.BINANCE_FUTURE.value
        super().__init__(settings, exchange_code, on_status_change)
        self.ws_url = WS_URL
        
        # 상태 및 설정값
        self.is_connected = False
        self.connection_timeout = CONNECTION_TIMEOUT
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        self.message_timeout = MESSAGE_TIMEOUT
        
        # 재연결 전략
        self.reconnect_strategy = ReconnectStrategy(
            initial_delay=RECONNECT_DELAY,
            max_delay=60.0,
            multiplier=2.0,
            max_attempts=0  # 무제한 재시도
        )

    # 웹소켓 연결 관리
    # ==================================
    async def connect(self) -> bool:
        """
        바이낸스 선물 웹소켓 서버에 연결
        
        Returns:
            bool: 연결 성공 여부
        """
        try:
            self.log_info("🔵 웹소켓 연결 시도")
            self.is_connected = False
            self.connecting = True  # 연결 중 플래그 추가
            retry_count = 0
            
            while not self.stop_event.is_set():
                try:
                    # 웹소켓 라이브러리의 내장 핑퐁 기능 사용
                    self.ws = await connect(
                        self.ws_url,
                        ping_interval=self.ping_interval,
                        ping_timeout=self.ping_timeout,
                        close_timeout=10,
                        max_size=None,
                        open_timeout=self.connection_timeout
                    )
                    
                    self.is_connected = True
                    self.log_info("🟢 웹소켓 연결 성공")
                    
                    # 재연결 전략 초기화
                    self.reconnect_strategy.reset()
                    
                    self.connecting = False  # 연결 중 플래그 해제
                    return True
                    
                except asyncio.TimeoutError:
                    retry_count += 1
                    self.log_warning(f"연결 타임아웃 ({retry_count}번째 시도), 재시도...")
                    # 재연결 전략에 따른 지연 시간 적용
                    delay = self.reconnect_strategy.next_delay()
                    self.log_info(f"{delay:.2f}초 후 재연결 시도...")
                    await asyncio.sleep(delay)
                    continue
                    
                except Exception as e:
                    retry_count += 1
                    self.log_warning(f"연결 실패 ({retry_count}번째): {str(e)}")
                    
                    # 재연결 전략에 따른 지연 시간 적용
                    delay = self.reconnect_strategy.next_delay()
                    self.log_info(f"{delay:.2f}초 후 재연결 시도...")
                    await asyncio.sleep(delay)
                    
        except Exception as e:
            self.log_error(f"🔴 연결 오류: {str(e)}")
            
            self.is_connected = False
            return False
        finally:
            self.connecting = False  # 연결 시도 종료 플래그

    # PING/PONG 관리
    # ==================================
    async def _send_ping(self) -> None:
        """
        PING 메시지 전송
        
        바이낸스 선물 서버에 PING 메시지를 전송하여 연결 상태를 유지합니다.
        바이낸스는 {"id": <id>, "method": "ping"} 형식의 JSON 객체를 사용합니다.
        """
        try:
            if self.ws and self.is_connected:
                ping_message = {
                    "id": int(time.time() * 1000),
                    "method": "ping"
                }
                await self.ws.send(json.dumps(ping_message))
                self.log_debug("PING 메시지 전송")
        except Exception as e:
            self.log_error(f"PING 메시지 전송 실패: {str(e)}")
            
            # 연결 문제로 핑 전송 실패 시 재연결 시도
            if isinstance(e, websockets.exceptions.ConnectionClosed):
                self.log_warning("PING 전송 실패로 재연결 시도")
                await self.reconnect() 