# file: orderbook/connection/binance_s_cn.py

import asyncio
import json
import time
from typing import Dict, List, Optional, Any
import websockets
from websockets import connect

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import Exchange

from crosskimp.ob_collector.eventbus.types import EventTypes
from crosskimp.ob_collector.eventbus.handler import get_orderbook_event_bus
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, ReconnectStrategy, WebSocketStats

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이낸스 현물 웹소켓 연결 관련 상수
# ============================
# 웹소켓 연결 설정
WS_URL = "wss://stream.binance.com:9443/ws"  # 현물 웹소켓 URL (포트 9443 명시)
PING_INTERVAL = 20  # 핑 전송 간격 (초) - 바이낸스 문서 기준 20초마다 핑 프레임 전송
PING_TIMEOUT = 50   # 핑 응답 타임아웃 (초) - 바이낸스 문서 기준 1분 내에 퐁 응답 필요
MESSAGE_TIMEOUT = 60  # 메시지 타임아웃 (초)
RECONNECT_DELAY = 0.1  # 초기 재연결 시도 시간 (초)
CONNECTION_TIMEOUT = 3  # 연결 타임아웃 (초)

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
        self.message_timeout = MESSAGE_TIMEOUT
        
        # Ping/Pong 설정 추가
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        self.last_ping_time = 0
        self.last_pong_time = 0
        
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
            
            # 연결 시도 중 상태 업데이트
            self._update_connection_metric("status", "connecting")
            
            while not self.stop_event.is_set():
                try:
                    # 바이낸스 웹소켓 프로토콜 요구사항에 맞게 설정:
                    # - ping_interval: 바이낸스 서버가 보내는 ping 프레임 간격 (20초)
                    # - ping_timeout: 연결 종료 전 pong 응답 대기 시간 (50초, 문서 상 1분이지만 여유 확보)
                    self.ws = await connect(
                        self.ws_url,
                        ping_interval=None,  # ping은 서버가 보내므로 클라이언트는 보내지 않음
                        ping_timeout=None,   # 서버의 ping을 자동으로 응답
                        close_timeout=10,
                        max_size=None,
                        open_timeout=self.connection_timeout
                    )
                    
                    self.is_connected = True
                    self.log_info("🟢 웹소켓 연결 성공")
                    
                    # 재연결 전략 초기화
                    self.reconnect_strategy.reset()
                    
                    self.connecting = False  # 연결 시도 중 플래그 해제
                    return True
                    
                except asyncio.TimeoutError:
                    retry_count += 1
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
                    retry_count += 1
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
            self.connecting = False  # 연결 시도 중 플래그 해제
            
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
            
            # 일반 메시지 처리
            return data
            
        except json.JSONDecodeError:
            self.log_error(f"JSON 디코딩 실패: {message[:100]}")
            
            # 오류 메트릭 업데이트
            self._update_connection_metric("last_error", "JSON 디코딩 실패")
            self._update_connection_metric("last_error_time", time.time())
            
            return None
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {str(e)}")
            
            # 오류 메트릭 업데이트
            self._update_connection_metric("last_error", f"메시지 처리 오류: {str(e)}")
            self._update_connection_metric("last_error_time", time.time())
            
            return None 