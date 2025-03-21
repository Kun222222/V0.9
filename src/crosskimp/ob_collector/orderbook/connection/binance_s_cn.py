# file: orderbook/connection/binance_s_cn.py

import asyncio
import json
import time
from typing import Dict, List, Optional, Any
import websockets
from websockets import connect

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import Exchange
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, ReconnectStrategy, WebSocketStats
from crosskimp.ob_collector.orderbook.util.event_bus import EVENT_TYPES

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이낸스 현물 웹소켓 연결 관련 상수
# ============================
# 웹소켓 연결 설정
WS_URL = "wss://stream.binance.com/ws"  # 현물 웹소켓 URL
PING_INTERVAL = 30  # 핑 전송 간격 (초)
PING_TIMEOUT = 10   # 핑 응답 타임아웃 (초)
MESSAGE_TIMEOUT = 60  # 메시지 타임아웃 (초)
RECONNECT_DELAY = 0.1  # 초기 재연결 시도 시간 (초)
HEALTH_CHECK_INTERVAL = 30  # 헬스체크 간격 (초)
CONNECTION_TIMEOUT = 5  # 연결 타임아웃 (초)

class BinanceWebSocketConnector(BaseWebsocketConnector):
    """
    바이낸스 현물 웹소켓 연결 관리 클래스
    
    바이낸스 현물 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    
    책임:
    - 웹소켓 연결 관리 (연결, 재연결, 종료)
    - 연결 상태 모니터링
    - 핑-퐁 메시지 처리
    """
    def __init__(self, settings: dict):
        """
        바이낸스 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, Exchange.BINANCE.value)
        
        # 웹소켓 URL 설정
        self.ws_url = WS_URL
        
        # 상태 및 설정값
        self.is_connected = False
        self.connection_timeout = CONNECTION_TIMEOUT
        self.health_check_interval = HEALTH_CHECK_INTERVAL
        self.message_timeout = MESSAGE_TIMEOUT
        
        # Ping/Pong 설정 추가
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        self.last_ping_time = 0
        self.last_pong_time = 0
        
        # 상태 추적
        self.health_check_task = None
        
        # 재연결 전략
        self.reconnect_strategy = ReconnectStrategy(
            initial_delay=RECONNECT_DELAY,
            max_delay=60.0,
            multiplier=2.0,
            max_attempts=0
        )

    # 웹소켓 연결 관리
    # ==================================
    async def connect(self) -> bool:
        """
        바이낸스 웹소켓 서버에 연결
        
        Returns:
            bool: 연결 성공 여부
        """
        try:
            self.log_info("🔵 웹소켓 연결 시도")
            self.connecting = True  # 연결 시도 중 플래그 설정
            self.is_connected = False
            retry_count = 0
            
            while not self.stop_event.is_set():
                try:
                    # 웹소켓 라이브러리의 내장 핑퐁 기능 사용
                    self.ws = await connect(
                        self.ws_url,
                        ping_interval=self.ping_interval,  # 150초
                        ping_timeout=self.ping_timeout,    # 10초
                        close_timeout=10,
                        max_size=None,
                        open_timeout=self.connection_timeout
                    )
                    
                    self.is_connected = True
                    self.stats.last_message_time = time.time()  # 연결 성공 시 메시지 시간 초기화
                    self.log_info("🟢 웹소켓 연결 성공")
                    
                    # 헬스 체크 태스크 시작
                    if self._should_start_health_check():
                        self.health_check_task = asyncio.create_task(self.health_check())
                    
                    self.connecting = False  # 연결 시도 중 플래그 해제
                    return True
                    
                except asyncio.TimeoutError:
                    retry_count += 1
                    self.log_warning(f"연결 타임아웃 ({retry_count}번째 시도), 재시도...")
                    await asyncio.sleep(self.reconnect_strategy.next_delay())
                    continue
                    
                except Exception as e:
                    retry_count += 1
                    self.log_warning(f"연결 실패 ({retry_count}번째): {str(e)}")
                    await asyncio.sleep(self.reconnect_strategy.next_delay())
                    
        except Exception as e:
            self.log_error(f"🔴 연결 오류: {str(e)}")
            self.is_connected = False
            return False
        finally:
            self.connecting = False  # 연결 시도 중 플래그 해제
            
    async def health_check(self) -> None:
        """웹소켓 상태 체크 (주기적 모니터링)"""
        try:
            self.log_info("상태 모니터링 시작")
            
            while not self.stop_event.is_set():
                try:
                    # 웹소켓 연결 상태 확인
                    if self.ws and not self.ws.closed:
                        # 마지막 메시지 수신 시간 확인
                        current_time = time.time()
                        last_message_time = self.stats.last_message_time or current_time
                        time_since_last_message = current_time - last_message_time
                        
                        # 너무 오래 메시지가 없으면 핑 전송
                        if time_since_last_message > self.ping_interval:
                            # 핑 전송 메서드 호출
                            await self._send_ping()
                            
                    # 대기
                    await asyncio.sleep(self.health_check_interval)
                    
                except asyncio.CancelledError:
                    raise  # 상위로 전파
                    
                except Exception as e:
                    self.log_error(f"상태 체크 중 오류: {str(e)}")
                    await asyncio.sleep(1)  # 오류 발생 시 짧게 대기
                
        except asyncio.CancelledError:
            self.log_info("상태 모니터링 태스크 취소됨")
            
        except Exception as e:
            self.log_error(f"상태 모니터링 루프 오류: {str(e)}")
            
            # 모니터링 태스크가 중단되지 않도록 재시작
            # 단, 종료 이벤트가 설정되지 않은 경우에만 재시작
            if not self.stop_event.is_set():
                asyncio.create_task(self._restart_health_check())
                
    # PING/PONG 관리
    # ==================================
    async def _send_ping(self) -> None:
        """
        PING 메시지 전송
        """
        try:
            if not self.ws or self.ws.closed:
                return
                
            ping_id = int(time.time() * 1000)
            
            # 바이낸스는 JSON 형식의 ping 메시지 사용
            ping_message = {
                "method": "ping",
                "id": ping_id
            }
            
            # 핑 전송 시간 기록
            self.last_ping_time = time.time()
            
            await self.ws.send(json.dumps(ping_message))
            self.log_debug(f"핑 전송 | ID: {ping_id}")
            
        except Exception as e:
            self.log_error(f"핑 전송 중 오류: {str(e)}")
            
    async def process_message(self, message: str) -> Optional[Dict]:
        """
        수신된 메시지 처리
        
        Args:
            message: 수신된 원시 메시지
            
        Returns:
            Dict or None: 파싱된 메시지 또는 None (핑-퐁 메시지인 경우)
        """
        try:
            # 메시지가 텍스트가 아닌 경우 무시
            if not isinstance(message, str):
                return None
                
            # 메시지 파싱
            data = json.loads(message)
            
            # Pong 메시지 처리
            if "result" in data and data.get("id") is not None:
                self.last_pong_time = time.time()
                
                # 레이턴시 계산 (밀리초 단위)
                if self.last_ping_time > 0:
                    latency = (self.last_pong_time - self.last_ping_time) * 1000  # ms 단위
                    
                    # 메트릭에 레이턴시 기록
                    asyncio.create_task(self.event_handler.handle_metric_update(
                        metric_name="latency_ms",
                        value=latency
                    ))
                    
                    self.log_debug(f"퐁 수신 | 지연시간: {latency:.2f}ms")
                    
                # 핑-퐁 메시지는 추가 처리하지 않음
                return None
                
            # 일반 메시지 처리
            self.stats.last_message_time = time.time()
            return data
            
        except json.JSONDecodeError:
            self.log_error(f"JSON 디코딩 실패: {message[:100]}")
            return None
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {str(e)}")
            return None 