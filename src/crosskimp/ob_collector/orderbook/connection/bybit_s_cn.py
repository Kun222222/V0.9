# file: orderbook/connection/bybit_s_cn.py

import asyncio
import json
import time
from websockets import connect
import websockets
from typing import Dict, Optional, Any

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.constants_v3 import Exchange

from crosskimp.ob_collector.eventbus.types import EventTypes
from crosskimp.ob_collector.eventbus.handler import get_orderbook_event_bus
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, WebSocketError, ReconnectStrategy

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이빗 현물 웹소켓 연결 관련 상수
# ============================
WS_URL = "wss://stream.bybit.com/v5/public/spot"
PING_INTERVAL = 20  # 핑 전송 간격 (초)
PING_TIMEOUT = 10   # 핑 응답 타임아웃 (초)
MESSAGE_TIMEOUT = 60  # 메시지 타임아웃 (초)
RECONNECT_DELAY = 0.1  # 초기 재연결 시도 시간 (초)
CONNECTION_TIMEOUT = 0.5  # 연결 타임아웃 (초)

class BybitWebSocketConnector(BaseWebsocketConnector):
    """
    바이빗 현물 웹소켓 연결 관리 클래스
    
    책임:
    - 웹소켓 연결 관리 (연결, 재연결, 종료)
    - 연결 상태 모니터링
    """
    
    def __init__(self, settings: dict):
        """초기화"""
        super().__init__(settings, Exchange.BYBIT.value)
        
        # 웹소켓 URL 및 기본 설정
        self.ws_url = WS_URL if not settings.get("testnet") else "wss://stream-testnet.bybit.com/v5/public/spot"
        
        # 상태 및 설정값
        self.is_connected = False
        self.connection_timeout = CONNECTION_TIMEOUT
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        self.message_timeout = MESSAGE_TIMEOUT
        
        # 재연결 전략
        self.reconnect_strategy = ReconnectStrategy(
            initial_delay=0.5,    # 초기 재연결 대기 시간 (0.5초로 변경)
            max_delay=0.5,       # 최대 재연결 대기 시간도 0.5초로 설정
            multiplier=1.0,       # 대기 시간 증가 배수를 1.0으로 설정 (증가 없음)
            max_attempts=0        # 0 = 무제한 재시도
        )

    # 웹소켓 연결 관리
    # ==================================
    async def connect(self) -> bool:
        """웹소켓 연결 시도"""
        try:
            self.log_info("🔵 웹소켓 연결 시도")
            self.connecting = True  # 연결 중 플래그 추가
            self.is_connected = False
            retry_count = 0
            
            # 연결 시도 중 상태 업데이트
            self._update_connection_metric("status", "connecting")
            
            while not self.stop_event.is_set():
                try:
                    # 연결 시도 이벤트 발행
                    self._connection_attempt_count += 1
                    retry_count += 1
                    
                    if hasattr(self, 'event_bus') and self.event_bus:
                        event_data = {
                            "exchange_code": self.exchange_code,
                            "attempt": self._connection_attempt_count,
                            "timestamp": time.time()
                        }
                        # 첫 연결 시도와 재연결 시도 구분
                        event_type = "connection_attempt" if self._connection_attempt_count == 1 else "connection_retry"
                        # 비동기 컨텍스트에서 호출되므로 create_task 사용
                        asyncio.create_task(self.event_bus.publish(event_type, event_data))
                    
                    # 웹소켓 라이브러리의 내장 핑퐁 기능 사용
                    self.ws = await connect(
                        self.ws_url,
                        ping_interval=self.ping_interval,  # 내장 핑퐁 사용
                        ping_timeout=self.ping_timeout,    # 내장 핑퐁 사용
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
                    self.log_warning(f"연결 타임아웃 ({retry_count}번째 시도), 재시도...")
                    
                    # 오류 메트릭 업데이트
                    self._update_connection_metric("last_error", "연결 타임아웃")
                    self._update_connection_metric("last_error_time", time.time())
                    
                    # 재연결 전략에 따른 지연 시간 적용
                    delay = self.reconnect_strategy.next_delay()
                    self.log_info(f"{delay:.2f}초 후 재연결 시도...")
                    await asyncio.sleep(delay)
                    continue
                    
                except Exception as e:
                    self.log_warning(f"연결 실패 ({retry_count}번째): {str(e)}")
                    
                    # 오류 메트릭 업데이트
                    self._update_connection_metric("last_error", str(e))
                    self._update_connection_metric("last_error_time", time.time())
                    
                    # 재연결 전략에 따른 지연 시간 적용
                    delay = self.reconnect_strategy.next_delay()
                    self.log_info(f"{delay:.2f}초 후 재연결 시도...")
                    await asyncio.sleep(delay)
                    
        except Exception as e:
            self.log_error(f"🔴 연결 오류: {str(e)}")
            
            # 오류 메트릭 업데이트
            self._update_connection_metric("last_error", str(e))
            self._update_connection_metric("last_error_time", time.time())
            
            self.is_connected = False
            return False
        finally:
            self.connecting = False  # 연결 시도 종료 플래그

    # PING/PONG 관리
    # ==================================
    async def _send_ping(self) -> None:
        """
        PING 메시지 전송
        
        바이빗 서버에 PING 메시지를 전송하여 연결 상태를 유지합니다.
        """
        try:
            if self.ws and self.is_connected:
                ping_message = {
                    "req_id": str(int(time.time() * 1000)),
                    "op": "ping"
                }
                await self.ws.send(json.dumps(ping_message))
                self.log_debug(f"PING 메시지 전송")
        except Exception as e:
            self.log_error(f"PING 메시지 전송 실패: {str(e)}")
            
            # 연결 문제로 핑 전송 실패 시 재연결 시도
            if isinstance(e, websockets.exceptions.ConnectionClosed):
                self.log_warning("PING 전송 실패로 재연결 시도")
                await self.reconnect()
