# file: orderbook/connection/upbit_s_cn.py

import asyncio
import time
from websockets import connect
import websockets.exceptions
from typing import Dict, List, Optional, Tuple, Any

from crosskimp.ob_collector.orderbook.connection.base_ws_connector import BaseWebsocketConnector

# ============================
# 업비트 웹소켓 연결 관련 상수
# ============================
# 거래소 정보
EXCHANGE_CODE = "upbit"  # 거래소 코드
EXCHANGE_NAME_KR = "[업비트]"  # 거래소 한글 이름

# 웹소켓 연결 설정
WS_URL = "wss://api.upbit.com/websocket/v1"  # 웹소켓 URL
PING_INTERVAL = 30  # 핑 전송 간격 (초)
PING_TIMEOUT = 10  # 핑 응답 타임아웃 (초)

class UpbitWebSocketConnector(BaseWebsocketConnector):
    """
    업비트 웹소켓 연결 관리 클래스
    
    업비트 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    
    특징:
    - URL: wss://api.upbit.com/websocket/v1
    - 핑/퐁: 표준 WebSocket PING/PONG 프레임 사용
    """
    def __init__(self, settings: dict):
        """
        업비트 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, EXCHANGE_CODE)
        self.ws_url = WS_URL
        
        # 연결 관련 설정
        self.is_connected = False
        
        # 핑/퐁 설정
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT

    async def _do_connect(self):
        """
        실제 웹소켓 연결 수행 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        # 웹소켓 연결 수립 - 표준 PING/PONG 프레임 사용
        self.ws = await connect(
            self.ws_url,
            ping_interval=self.ping_interval,  # 표준 PING 프레임 자동 전송
            ping_timeout=self.ping_timeout     # PONG 응답 타임아웃
        )
        self.is_connected = True
        self.log_info("업비트 웹소켓 연결 성공")

    async def _cleanup_connection(self) -> None:
        """연결 종료 및 정리"""
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                self.log_error(f"웹소켓 종료 실패: {e}")

        self.is_connected = False
        
        # 연결 종료 상태 콜백
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "disconnect") 
            
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
            
    async def receive_raw(self) -> Optional[str]:
        """
        웹소켓에서 원시 메시지 수신
        
        이 메서드는 Subscription 클래스에서 사용됩니다.
        
        Returns:
            Optional[str]: 수신된 원시 메시지 또는 None
        """
        try:
            if not self.ws or not self.is_connected:
                self.log_debug("웹소켓 연결이 없거나 연결되지 않음")
                return None
                
            # 메시지 수신
            message = await self.ws.recv()
            
            # 메시지 로깅 (너무 길면 일부만)
            if message:
                # 메시지 통계 업데이트 - stats 객체에 total_messages 속성 사용
                self.stats.total_messages += 1
                self.stats.message_count += 1
                self.stats.last_message_time = time.time()
                
                # 일반 메시지는 INFO 레벨로 로깅
                try:
                    # JSON 형식인 경우 파싱하여 타입 확인
                    import json
                    data = json.loads(message)
                    msg_type = data.get("type", "unknown")
                    code = data.get("code", "unknown")
                    self.log_info(f"메시지 수신: 타입={msg_type}, 코드={code}, 길이={len(message)}")
                except:
                    # 파싱 실패 시 원시 메시지 일부만 로깅
                    if len(message) > 200:
                        self.log_info(f"메시지 수신 (길이: {len(message)}): {message[:100]}...{message[-100:]}")
                    else:
                        self.log_info(f"메시지 수신: {message}")
                
            return message
        except websockets.exceptions.ConnectionClosed as e:
            self.log_error(f"웹소켓 연결 끊김: {e}")
            self.is_connected = False
            return None
        except Exception as e:
            self.log_error(f"메시지 수신 실패: {e}")
            return None
        
    async def check_connection(self) -> bool:
        """
        웹소켓 연결 상태 확인
        
        Returns:
            bool: 연결 상태
        """
        return self.ws is not None and self.is_connected
        
    async def keep_alive(self) -> None:
        """
        연결 유지 (PING 전송)
        
        이 메서드는 Subscription 클래스에서 호출됩니다.
        """
        while self.is_connected and not self.stop_event.is_set():
            await self._send_ping()
            await asyncio.sleep(self.ping_interval) 