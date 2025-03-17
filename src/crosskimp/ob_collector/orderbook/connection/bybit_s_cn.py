# file: orderbook/connection/bybit_s_cn.py

import asyncio
import json
import time
from websockets import connect
import websockets
from typing import Dict, List, Optional, Set, Any

from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, WebSocketError, ReconnectStrategy

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
    """
    def __init__(self, settings: dict):
        """
        바이빗 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, "bybit")  # 거래소 코드 직접 지정
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

    async def connect(self) -> bool:
        """
        웹소켓 연결 시도
        
        Returns:
            bool: 연결 성공 여부
        """
        try:
            logger.info(f"[{self.exchangename}] 🔵 웹소켓 연결 시도")
            # 연결 상태 초기화 (이제 상태 관리자를 통해 관리됨)
            self.is_connected = False
            
            # 연결 시도 횟수 초기화
            retry_count = 0
            
            while not self.stop_event.is_set():  # 무제한 시도
                try:
                    # 웹소켓 연결 시도 (타임아웃 0.5초로 설정)
                    self.ws = await connect(
                        self.ws_url,
                        ping_interval=20,
                        ping_timeout=10,
                        close_timeout=10,
                        max_size=None,
                        open_timeout=self.connection_timeout  # 0.5초 타임아웃
                    )
                    
                    # 연결 성공 - 상태 관리자를 통해 상태 업데이트
                    self.is_connected = True
                    logger.info(f"[{self.exchangename}] 🟢 웹소켓 연결 성공")
                    return True
                    
                except Exception as e:
                    retry_count += 1
                    logger.warning(f"[{self.exchangename}] 연결 시도 {retry_count}번째 실패: {str(e)}")
                    # 즉시 재시도 (대기 없음)
                    logger.info(f"[{self.exchangename}] 즉시 재시도...")
                        
        except Exception as e:
            logger.error(f"[{self.exchangename}] 🔴 연결 오류: {str(e)}")
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
            
            # 연결 상태 업데이트 (상태 관리자를 통해)
            self.is_connected = False
            return True
            
        except Exception as e:
            self.log_error(f"웹소켓 연결 종료 실패: {str(e)}")
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
            return False

    async def health_check(self) -> None:
        """
        웹소켓 상태 체크 (백그라운드 태스크)
        """
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                
                # 메시지 타임아웃 체크
                if self.is_connected and self.stats.last_message_time > 0:
                    if (current_time - self.stats.last_message_time) > self.message_timeout:
                        error_msg = f"{self.exchange_korean_name} 웹소켓 메시지 타임아웃"
                        self.log_error(error_msg)
                        await self.send_telegram_notification("error", error_msg)
                        await self.reconnect()
                
                await asyncio.sleep(self.health_check_interval)
                
            except Exception as e:
                self.log_error(f"웹소켓 상태 체크 중 오류: {str(e)}")
                await asyncio.sleep(1)

    async def reconnect(self) -> bool:
        """
        웹소켓 재연결
        
        Returns:
            bool: 재연결 성공 여부
        """
        try:
            self.stats.reconnect_count += 1
            reconnect_msg = f"{self.exchange_korean_name} 웹소켓 재연결 시도"
            self.log_info(reconnect_msg)
            await self.send_telegram_notification("reconnect", reconnect_msg)
            
            await self.disconnect()
            
            # 재연결 대기 (최소화)
            delay = self.reconnect_strategy.next_delay()
            await asyncio.sleep(delay)
            
            success = await self.connect()
            return success
            
        except Exception as e:
            self.log_error(f"웹소켓 재연결 실패: {str(e)}")
            return False

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
                self.update_message_metrics(message)
                
            return message
            
        except websockets.exceptions.ConnectionClosed:
            self.log_error("웹소켓 연결 끊김")
            # 연결 상태 업데이트 (상태 관리자를 통해)
            self.is_connected = False
            return None
            
        except Exception as e:
            self.log_error(f"메시지 수신 실패: {e}")
            self.metrics.record_error(self.exchangename)
            return None

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
                await self.send_message(json.dumps(ping_message))
                self.stats.last_ping_time = time.time()
                self.log_debug(f"PING 메시지 전송")
        except Exception as e:
            self.log_error(f"PING 메시지 전송 실패: {str(e)}")

    def _handle_pong(self, data: dict) -> bool:
        """
        PONG 메시지 처리
        
        Args:
            data: PONG 메시지 데이터
            
        Returns:
            bool: PONG 메시지 처리 성공 여부
        """
        try:
            self.stats.last_pong_time = time.time()
            self.log_debug(f"PONG 응답 수신")
            return True
        except Exception as e:
            self.log_error(f"PONG 메시지 처리 실패: {str(e)}")
            return False