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

        # 메시지 통계
        self.stats_total_messages = 0
        self.stats_message_count = 0
        self.stats_last_time = time.time()

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
                    
                    # 헬스 체크 태스크 시작
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
                    self.log_info("즉시 재시도...")
                    
        except Exception as e:
            self.log_error(f"🔴 연결 오류: {str(e)}")
            self.is_connected = False
            return False

    async def disconnect(self) -> bool:
        """
        웹소켓 연결 종료
        
        Returns:
            bool: 종료 성공 여부
        """
        try:
            if self.ws:
                await self.ws.close()
            
            # 태스크 취소
            if self.health_check_task and not self.health_check_task.done():
                self.health_check_task.cancel()
            
            self.is_connected = False
            
            # 연결 종료 콜백 호출
            if self.connection_status_callback:
                self.connection_status_callback("disconnected")
                
            self.log_info("웹소켓 연결 종료됨")
            return True
                
        except Exception as e:
            self.log_error(f"연결 종료 실패: {str(e)}")
            self.is_connected = False
            return False
    
    async def send_message(self, message: str) -> bool:
        """
        웹소켓을 통해 메시지 전송
        
        Args:
            message: 전송할 메시지
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            if not self.ws or not self.is_connected:
                self.log_error("웹소켓이 연결되지 않음")
                return False
                
            await self.ws.send(message)
            return True
            
        except Exception as e:
            self.log_error(f"메시지 전송 실패: {str(e)}")
            self.is_connected = False
            return False
    
    async def health_check(self) -> None:
        """
        웹소켓 상태 체크 (백그라운드 태스크)
        """
        while not self.stop_event.is_set() and self.is_connected:
            try:
                current_time = time.time()
                
                # 메시지 타임아웃 체크
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
        """
        웹소켓에서 원시 메시지 수신
        
        Returns:
            Optional[str]: 수신된 원시 메시지 또는 None
        """
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
        """
        웹소켓 재연결
        
        Returns:
            bool: 재연결 성공 여부
        """
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
            
            # 구독 응답 처리
            if "status" in data and "resmsg" in data:
                if data["status"] == "0000":  # 성공 상태 코드
                    self.log_info(f"구독 응답 성공: {data['resmsg']}")
                else:  # 실패 상태 코드
                    self.log_error(f"구독 실패: {data['resmsg']}")
                return
            
            # 자식 클래스에서 구현할 메시지 처리
            
        except json.JSONDecodeError:
            self.log_error(f"JSON 파싱 오류: {message[:100]}")
        except Exception as e:
            self.log_error(f"메시지 처리 오류: {str(e)}") 