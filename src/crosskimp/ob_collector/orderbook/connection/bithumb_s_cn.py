# file: orderbook/connection/bithumb_s_cn.py

import asyncio
import json
import time
from typing import Dict, List, Optional, Any
import websockets
from websockets import connect

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import Exchange, EXCHANGE_NAMES_KR
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, ReconnectStrategy, WebSocketStats

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 빗썸 웹소켓 연결 관련 상수
# ============================
# 웹소켓 연결 설정
WS_URL = "wss://ws-api.bithumb.com/websocket/v1"  # 웹소켓 URL
PING_INTERVAL = 20  # 핑 전송 간격 (초)
PING_TIMEOUT = 10   # 핑 응답 타임아웃 (초)
MESSAGE_TIMEOUT = 60  # 메시지 타임아웃 (초)
RECONNECT_DELAY = 0.1  # 초기 재연결 시도 시간 (초)
HEALTH_CHECK_INTERVAL = 30  # 헬스체크 간격 (초)
CONNECTION_TIMEOUT = 5  # 연결 타임아웃 (초)

class BithumbWebSocketConnector(BaseWebsocketConnector):
    """
    빗썸 현물 웹소켓 연결 관리 클래스
    
    빗썸 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    
    책임:
    - 웹소켓 연결 관리 (연결, 재연결, 종료)
    - 연결 상태 모니터링
    """
    def __init__(self, settings: dict):
        """
        빗썸 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, Exchange.BITHUMB.value)
        
        # 웹소켓 URL 설정
        self.ws_url = WS_URL
        
        # 상태 및 설정값
        self.is_connected = False
        self.connection_timeout = CONNECTION_TIMEOUT
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        self.health_check_interval = HEALTH_CHECK_INTERVAL
        self.message_timeout = MESSAGE_TIMEOUT
        
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
        빗썸 웹소켓 서버에 연결
        
        Returns:
            bool: 연결 성공 여부
        """
        try:
            self.log_info("🔵 웹소켓 연결 시도")
            self.is_connected = False
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
                    self.stats.last_message_time = time.time()  # 연결 성공 시 메시지 시간 초기화
                    self.log_info("🟢 웹소켓 연결 성공")
                    
                    # 텔레그램 알림 전송
                    connect_msg = "웹소켓 연결 성공"
                    await self.send_telegram_notification("connect", connect_msg)
                    
                    # 헬스 체크 태스크 시작
                    if self._should_start_health_check():
                        self.health_check_task = asyncio.create_task(self.health_check())
                    
                    return True
                    
                except asyncio.TimeoutError:
                    retry_count += 1
                    self.log_warning(f"연결 타임아웃 ({retry_count}번째 시도), 재시도...")
                    continue
                    
                except Exception as e:
                    retry_count += 1
                    self.log_warning(f"연결 실패 ({retry_count}번째): {str(e)}")
                    self.log_info("즉시 재시도...")
                    
        except Exception as e:
            self.log_error(f"🔴 연결 오류: {str(e)}")
            self.is_connected = False
            return False 