# file: orderbook/connection/upbit_s_cn.py

import asyncio
import json
import time
from websockets import connect
import websockets
import uuid
from typing import Dict, Optional

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import Exchange

from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, ReconnectStrategy

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 업비트 웹소켓 연결 관련 상수
# ============================
# 거래소 정보 - Exchange 열거형 사용
# 거래소 코드 대문자로 통일 부분 제거

# 웹소켓 연결 설정
WS_URL = "wss://api.upbit.com/websocket/v1"  # 웹소켓 URL
PING_INTERVAL = 30  # 핑 전송 간격 (초)
PING_TIMEOUT = 10  # 핑 응답 타임아웃 (초)
MESSAGE_TIMEOUT = 60  # 메시지 타임아웃 (초)
RECONNECT_DELAY = 0.1  # 초기 재연결 시도 시간 (초) - 다른 거래소와 동일하게 설정
CONNECTION_TIMEOUT = 3  # 연결 타임아웃 (초) - 다른 거래소와 동일하게 설정

class UpbitWebSocketConnector(BaseWebsocketConnector):
    """
    업비트 웹소켓 연결 관리 클래스
    
    업비트 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    
    특징:
    - URL: wss://api.upbit.com/websocket/v1
    - 핑/퐁: 표준 WebSocket PING/PONG 프레임 사용
    
    책임:
    - 웹소켓 연결 관리 (연결, 재연결, 종료)
    - 연결 상태 모니터링
    """
    def __init__(self, settings: dict, exchange_code: str = None, on_status_change=None):
        """
        업비트 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
            exchange_code: 거래소 코드 (기본값: None, 자동으로 설정)
            on_status_change: 연결 상태 변경 시 호출될 콜백 함수
        """
        exchange_code = exchange_code or Exchange.UPBIT.value
        super().__init__(settings, exchange_code, on_status_change)
        self.ws_url = WS_URL
        
        # 업비트 전용 설정 - 다른 거래소와 일관되게 설정
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        self.message_timeout = MESSAGE_TIMEOUT
        self.connection_timeout = CONNECTION_TIMEOUT
        
        # 재연결 전략 설정 - 다른 거래소와 일관되게 설정
        self.reconnect_strategy = ReconnectStrategy(
            initial_delay=RECONNECT_DELAY,  # 0.1초로 변경
            max_delay=60.0,
            multiplier=2.0,
            max_attempts=0  # 무제한 재시도
        )
        
        # 연결 중 상태 관리 (is_connected와 별개)
        self.connecting = False

    # 웹소켓 연결 관리
    # ==================================
    async def connect(self) -> bool:
        """
        웹소켓 연결 수행
        
        Returns:
            bool: 연결 성공 여부
        """
        try:
            self.log_info("🔵 웹소켓 연결 시도")
            # 부모 클래스의 setter 사용
            self.is_connected = False
            
            retry_count = 0
            
            while not self.stop_event.is_set():
                try:
                    # 웹소켓 연결 시도 - 타임아웃 설정 추가
                    self.ws = await connect(
                        self.ws_url,
                        ping_interval=self.ping_interval,
                        ping_timeout=self.ping_timeout,
                        close_timeout=10,
                        max_size=None,
                        open_timeout=self.connection_timeout  # 0.5초 타임아웃 추가
                    )
                    
                    # 연결 성공 처리 - 부모 클래스의 setter 사용
                    self.is_connected = True  # 부모 클래스의 setter 사용
                    self.stats.connection_start_time = time.time()
                    self.reconnect_strategy.reset()
                    
                    self.log_info("🟢 웹소켓 연결 성공")
                    return True
                 
                except asyncio.TimeoutError:
                    retry_count += 1
                    self.log_warning(f"연결 타임아웃 ({retry_count}번째 시도), 재시도...")
                    
                    # 재연결 전략에 따른 지연 시간 적용
                    delay = self.reconnect_strategy.next_delay()
                    self.log_info(f"{delay:.2f}초 후 재연결 시도...")
                    await asyncio.sleep(delay)
                    
                except Exception as e:
                    retry_count += 1
                    self.log_warning(f"연결 실패 ({retry_count}번째): {str(e)}")
                    
                    # 재연결 전략에 따른 지연 시간 적용
                    delay = self.reconnect_strategy.next_delay()
                    self.log_info(f"{delay:.2f}초 후 재연결 시도...")
                    await asyncio.sleep(delay)
                    
        except Exception as e:
            self.log_error(f"🔴 연결 오류: {str(e)}")
            
            # 부모 클래스의 setter 사용
            self.is_connected = False  # 부모 클래스의 setter 사용
            return False
            
        finally:
            self.connecting = False