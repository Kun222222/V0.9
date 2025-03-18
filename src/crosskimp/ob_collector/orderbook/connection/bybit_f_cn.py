# file: orderbook/connection/bybit_f_cn.py

import asyncio
import json
import time
from websockets import connect
import websockets
from typing import Dict, List, Optional

from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, ReconnectStrategy

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이빗 선물 웹소켓 연결 관련 상수
# ============================
EXCHANGE_CODE = "BYBIT_FUTURE"
EXCHANGE_KOREAN_NAME = "[바이빗 선물]"
WS_URL = "wss://stream.bybit.com/v5/public/linear"
PING_INTERVAL = 20  # 핑 전송 간격 (초)
PING_TIMEOUT = 10   # 핑 응답 타임아웃 (초) - 현물과 동일하게 10초로 설정
MESSAGE_TIMEOUT = 60  # 메시지 타임아웃 (초)
RECONNECT_DELAY = 0.1  # 초기 재연결 시도 시간 (초) - 현물과 동일하게 0.1초로 설정
HEALTH_CHECK_INTERVAL = 30  # 헬스체크 간격 (초) - 현물과 동일하게 30초로 설정
CONNECTION_TIMEOUT = 0.5  # 연결 타임아웃 (초)

class BybitFutureWebSocketConnector(BaseWebsocketConnector):
    """바이빗 선물 웹소켓 연결 관리 클래스"""
    
    def __init__(self, settings: dict):
        """초기화"""
        super().__init__(settings, EXCHANGE_CODE)
        
        # 웹소켓 URL 및 기본 설정
        self.exchange_korean_name = EXCHANGE_KOREAN_NAME
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

    async def send_message(self, message: str) -> bool:
        """메시지 전송"""
        try:
            if not self.ws or not self.is_connected:
                self.log_error("웹소켓이 연결되지 않음")
                return False
                
            await self.ws.send(message)
            return True
        except Exception as e:
            self.log_error(f"메시지 전송 실패: {str(e)}")
            return False

    async def health_check(self) -> None:
        """상태 모니터링"""
        while not self.stop_event.is_set() and self.is_connected:
            try:
                current_time = time.time()
                
                # 현물과 동일하게 메시지 타임아웃만 체크
                if self.stats.last_message_time > 0:
                    time_since_last_message = current_time - self.stats.last_message_time
                    if time_since_last_message > self.message_timeout:
                        error_msg = f"웹소켓 메시지 타임아웃: 마지막 메시지로부터 {time_since_last_message:.1f}초 경과"
                        self.log_error(error_msg)
                        await self.send_telegram_notification("error", error_msg)
                        await self.reconnect()
                
                await asyncio.sleep(self.health_check_interval)
                
            except Exception as e:
                self.log_error(f"상태 체크 오류: {str(e)}")
                await asyncio.sleep(1)

    async def receive_raw(self) -> Optional[str]:
        """원시 메시지 수신"""
        try:
            if not self.ws or not self.is_connected:
                return None
                
            message = await self.ws.recv()
            
            if message:
                self.stats.last_message_time = time.time()
                self.stats.message_count += 1
                
            return message
            
        except websockets.exceptions.ConnectionClosed as e:
            self.log_error(f"연결 끊김: {str(e)}")
            self.is_connected = False
            return None
            
        except Exception as e:
            self.log_error(f"수신 실패: {e}")
            if hasattr(self, 'metrics') and self.metrics:
                self.metrics.record_error(self.exchangename)
            return None

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

    async def process_message(self, message: str) -> None:
        """메시지 처리"""
        try:
            data = json.loads(message)
            
            # PONG 처리 - 이제는 내장 핑퐁 메커니즘을 사용하므로 간단하게 유지
            if (data.get("op") == "pong" or 
                data.get("ret_msg") == "pong" or 
                (isinstance(data.get("args"), list) and "pong" in str(data.get("args"))) or
                (data.get("success") == True and data.get("ret_msg") == "pong")):
                self.log_debug(f"PONG 수신: {message[:100]}")
                return
            
            # 자식 클래스에서 구현할 메시지 처리
            
        except json.JSONDecodeError:
            self.log_error(f"JSON 파싱 오류: {message[:100]}")
        except Exception as e:
            self.log_error(f"메시지 처리 오류: {str(e)}") 