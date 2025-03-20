# file: orderbook/connection/bybit_s_cn.py

import asyncio
import json
import time
from websockets import connect
import websockets
from typing import Dict, Optional, Any

from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, WebSocketError, ReconnectStrategy
from crosskimp.config.constants_v3 import Exchange, EXCHANGE_NAMES_KR
from crosskimp.ob_collector.orderbook.util.event_bus import EVENT_TYPES

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이빗 현물 웹소켓 연결 관련 상수
# ============================
# 웹소켓 연결 설정
WS_URL = "wss://stream.bybit.com/v5/public/spot"  # 웹소켓 URL
PING_INTERVAL = 20  # 핑 전송 간격 (초)
PING_TIMEOUT = 10   # 핑 응답 타임아웃 (초)
MESSAGE_TIMEOUT = 60  # 메시지 타임아웃 (초)
HEALTH_CHECK_INTERVAL = 30  # 헬스 체크 간격 (초)
CONNECTION_TIMEOUT = 0.5  # 연결 타임아웃 (초)

class BybitWebSocketConnector(BaseWebsocketConnector):
    """
    바이빗 웹소켓 연결 관리 클래스
    
    바이빗 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    
    책임:
    - 웹소켓 연결 관리 (연결, 재연결, 연결 종료)
    - 연결 상태 모니터링 및 건강 상태 확인
    """
    def __init__(self, settings: dict):
        """
        바이빗 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, Exchange.BYBIT.value)  # Exchange 열거형 사용
        self.ws_url = WS_URL
        
        # 거래소 전용 설정
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        self.message_timeout = MESSAGE_TIMEOUT
        self.health_check_interval = HEALTH_CHECK_INTERVAL
        self.connection_timeout = CONNECTION_TIMEOUT
        
        # 재연결 전략 설정
        self.reconnect_strategy = ReconnectStrategy(
            initial_delay=0.1,    # 초기 재연결 대기 시간 (0.1초로 변경)
            max_delay=60.0,       # 최대 재연결 대기 시간
            multiplier=2.0,       # 대기 시간 증가 배수
            max_attempts=0        # 0 = 무제한 재시도
        )
        
        # 헬스 체크 태스크
        self.health_check_task = None

    # 웹소켓 연결 관리
    # ==================================
    async def connect(self) -> bool:
        """
        웹소켓 연결 시도
        
        Returns:
            bool: 연결 성공 여부
        """
        try:
            self.log_info("🔵 웹소켓 연결 시도")
            # 연결 상태 초기화 (부모 클래스의 setter 사용)
            self.is_connected = False
            
            # 연결 시도 횟수 초기화
            retry_count = 0
            
            while not self.stop_event.is_set():  # 무제한 시도
                try:
                    # 웹소켓 연결 시도 (타임아웃 0.5초로 설정)
                    self.ws = await connect(
                        self.ws_url,
                        ping_interval=self.ping_interval,
                        ping_timeout=self.ping_timeout,
                        close_timeout=10,
                        max_size=None,
                        open_timeout=self.connection_timeout  # 0.5초 타임아웃
                    )
                    
                    # 연결 성공 - 부모 클래스의 setter 사용
                    self.is_connected = True
                    self.log_info("🟢 웹소켓 연결 성공")
                    
                    # 헬스 체크 태스크 시작
                    if self._should_start_health_check():
                        self.health_check_task = asyncio.create_task(self.health_check())
                    
                    return True
                    
                except Exception as e:
                    retry_count += 1
                    self.log_warning(f"연결 시도 {retry_count}번째 실패: {str(e)}")
                    # 즉시 재시도 (대기 없음)
                    self.log_info("즉시 재시도...")
                        
        except Exception as e:
            self.log_error(f"🔴 연결 오류: {str(e)}")
            self.is_connected = False
            return False

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
