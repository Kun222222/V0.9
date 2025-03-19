# file: orderbook/connection/bybit_f_cn.py

import asyncio
import json
import time
from websockets import connect
import websockets
from typing import Dict, Optional

from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, ReconnectStrategy
from crosskimp.config.constants_v3 import Exchange, EXCHANGE_NAMES_KR

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
HEALTH_CHECK_INTERVAL = 30  # 헬스체크 간격 (초) - 현물과 동일하게 30초로 설정
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
        self.health_check_interval = HEALTH_CHECK_INTERVAL
        self.message_timeout = MESSAGE_TIMEOUT
        
        # 상태 추적
        self.health_check_task = None  # 누락된 health_check_task 초기화 추가
        
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
        """웹소켓 연결 시도"""
        try:
            self.log_info("🔵 웹소켓 연결 시도")
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
                    self.stats.last_message_time = time.time()  # 연결 성공 시 메시지 시간 초기화
                    self.log_info("🟢 웹소켓 연결 성공")
                    
                    # 헬스 체크 태스크 시작 (핑 루프는 내장 기능으로 대체)
                    if self.health_check_task is None or self.health_check_task.done():
                        self.health_check_task = asyncio.create_task(self.health_check())
                    
                    return True
                    
                except asyncio.TimeoutError:
                    retry_count += 1
                    self.log_warning(f"연결 타임아웃 ({retry_count}번째 시도), 재시도...")
                    continue
                    
                except Exception as e:
                    retry_count += 1
                    self.log_warning(f"연결 실패 ({retry_count}번째): {str(e)}")
                    self.log_info("즉시 재시도...")  # 현물과 동일하게 즉시 재시도
                    
        except Exception as e:
            self.log_error(f"🔴 연결 오류: {str(e)}")
            self.is_connected = False
            return False

    async def disconnect(self) -> bool:
        """웹소켓 연결 종료"""
        try:
            if self.ws:
                await self.ws.close()
            
            # 태스크 취소
            if self.health_check_task and not self.health_check_task.done():
                self.health_check_task.cancel()
            
            self.is_connected = False
            self.log_info("웹소켓 연결 종료됨")
            return True
            
        except Exception as e:
            self.log_error(f"연결 종료 실패: {str(e)}")
            return False

    async def reconnect(self) -> bool:
        """재연결"""
        try:
            self.stats.reconnect_count += 1
            reconnect_msg = f"웹소켓 재연결 시도"
            self.log_info(reconnect_msg)
            await self.send_telegram_notification("reconnect", reconnect_msg)
            
            await self.disconnect()
            
            # 재연결 지연 계산
            delay = self.reconnect_strategy.next_delay()
            await asyncio.sleep(delay)
            
            success = await self.connect()
            
            return success
            
        except Exception as e:
            self.log_error(f"재연결 실패: {str(e)}")
            return False

    async def get_websocket(self):
        """
        현재 연결된 웹소켓 객체 반환
        
        Subscription 클래스에서 직접 웹소켓 객체에 접근할 수 있도록 함
        
        Returns:
            웹소켓 객체 또는 None
        """
        if self.is_connected and self.ws:
            return self.ws
        return None

    # 상태 모니터링
    # ==================================
    async def health_check(self) -> None:
        """상태 모니터링"""
        self.log_info("상태 모니터링 시작")
        
        while not self.stop_event.is_set() and self.is_connected:
            try:
                # 대기
                await asyncio.sleep(self.health_check_interval)
                
            except asyncio.CancelledError:
                self.log_info("상태 모니터링 태스크 취소됨")
                break
                
            except Exception as e:
                self.log_error(f"상태 체크 오류: {str(e)}")
                await asyncio.sleep(1) 