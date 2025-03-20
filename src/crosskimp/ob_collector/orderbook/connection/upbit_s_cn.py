# file: orderbook/connection/upbit_s_cn.py

import asyncio
import time
import json
from websockets import connect
import websockets.exceptions
from typing import Dict, Optional, Tuple, Any

from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, ReconnectStrategy
from crosskimp.config.constants_v3 import Exchange, EXCHANGE_NAMES_KR

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
HEALTH_CHECK_INTERVAL = 30  # 헬스 체크 간격 (초)

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
    def __init__(self, settings: dict):
        """
        업비트 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, Exchange.UPBIT.value)
        self.ws_url = WS_URL
        
        # 업비트 전용 설정
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        self.message_timeout = MESSAGE_TIMEOUT
        self.health_check_interval = HEALTH_CHECK_INTERVAL
        
        # 재연결 전략 설정
        self.reconnect_strategy = ReconnectStrategy(
            initial_delay=1.0,
            max_delay=60.0,
            multiplier=2.0,
            max_attempts=0  # 무제한 재시도
        )
        
        # 연결 중 상태 관리 (is_connected와 별개)
        self.connecting = False
        
        # 헬스 체크 태스크
        self.health_check_task = None

    # 웹소켓 연결 관리
    # ==================================
    async def connect(self) -> bool:
        """
        웹소켓 연결 수행
        
        Returns:
            bool: 연결 성공 여부
        """
        # 이미 연결된 경우 바로 반환
        if self.is_connected and self.ws:
            return True
            
        # 연결 중인 경우 대기
        if self.connecting:
            self.log_debug("이미 연결 중")
            return True
            
        self.connecting = True
        
        try:
            # 웹소켓 연결 수립 - 표준 PING/PONG 프레임 사용
            self.ws = await connect(
                self.ws_url,
                ping_interval=self.ping_interval,  # 표준 PING 프레임 자동 전송
                ping_timeout=self.ping_timeout     # PONG 응답 타임아웃
            )
            
            # 연결 성공 처리 - 부모 클래스의 setter 사용
            self.is_connected = True
            self.stats.connection_start_time = time.time()
            self.reconnect_strategy.reset()
            
            # 연결 성공 알림
            connect_msg = "웹소켓 연결 성공"
            await self.send_telegram_notification("connect", connect_msg)
            
            # 헬스 체크 시작
            if self._should_start_health_check():
                self.health_check_task = asyncio.create_task(self.health_check())
            
            self.log_info("웹소켓 연결 성공")
            return True
            
        except asyncio.TimeoutError as e:
            self.log_error(f"연결 타임아웃: {str(e)}")
            
            # 연결 실패 처리
            self.is_connected = False
            return False
                
        except Exception as e:
            self.log_error(f"연결 오류: {str(e)}", exc_info=True)
            
            # 연결 실패 처리
            self.is_connected = False
            return False
            
        finally:
            self.connecting = False