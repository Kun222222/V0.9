# file: orderbook/connection/bybit_f_cn.py

import asyncio
import json
import time
from websockets import connect
import websockets
from typing import Dict, Optional

from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, ReconnectStrategy
from crosskimp.config.constants_v3 import Exchange
from crosskimp.common.events.domains.orderbook import OrderbookEventTypes
from crosskimp.ob_collector.orderbook.util.event_adapter import get_event_adapter

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이빗 선물 웹소켓 연결 관련 상수
# ============================
WS_URL = "wss://stream.bybit.com/v5/public/linear"
PING_INTERVAL = 20  # 핑 전송 간격 (초)
PING_TIMEOUT = 10   # 핑 응답 타임아웃 (초) - 현물과 동일하게 10초로 설정
MESSAGE_TIMEOUT = 60  # 메시지 타임아웃 (초)
RECONNECT_DELAY = 0.1  # 초기 재연결 시도 시간 (초) - 현물과 동일하게 0.1초로 설정
CONNECTION_TIMEOUT = 0.5  # 연결 타임아웃 (초)

class BybitFutureWebSocketConnector(BaseWebsocketConnector):
    """
    바이빗 선물 웹소켓 연결 관리 클래스
    
    책임:
    - 웹소켓 연결 관리 (연결, 재연결, 종료)
    - 연결 상태 모니터링
    """
    
    def __init__(self, settings: dict):
        """초기화"""
        super().__init__(settings, Exchange.BYBIT_FUTURE.value)
        
        # 웹소켓 URL 및 기본 설정
        self.ws_url = WS_URL if not settings.get("testnet") else "wss://stream-testnet.bybit.com/v5/public/linear"
        
        # 상태 및 설정값
        self.is_connected = False
        self.connection_timeout = CONNECTION_TIMEOUT
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        self.message_timeout = MESSAGE_TIMEOUT
        
        # 재연결 전략
        self.reconnect_strategy = ReconnectStrategy(
            initial_delay=RECONNECT_DELAY,  # 0.1로 설정되어 있음
            max_delay=0.1,                  # 최대 재연결 대기 시간도 0.1초로 고정
            multiplier=1.0,                 # 대기 시간 증가 없음
            max_attempts=0                  # 무제한 재시도
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
            
            while not self.stop_event.is_set():
                try:
                    # 현물과 동일하게 설정: 웹소켓 라이브러리의 내장 핑퐁 기능 사용
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
