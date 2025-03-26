# file: orderbook/connection/bithumb_s_cn.py

import asyncio
import json
import time
from typing import Dict, List, Optional, Any
import websockets
from websockets import connect

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import Exchange

from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, ReconnectStrategy, WebSocketStats

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 빗썸 웹소켓 연결 관련 상수
# ============================
# 웹소켓 연결 설정
WS_URL = "wss://ws-api.bithumb.com/websocket/v1"  # 웹소켓 URL
PING_INTERVAL = 15  # 핑 전송 간격 (초)
PING_TIMEOUT = 20   # 핑 응답 타임아웃 (초) - 타임아웃 값 증가
MESSAGE_TIMEOUT = 60  # 메시지 타임아웃 (초)
RECONNECT_DELAY = 0.1  # 초기 재연결 시도 시간 (초)
CONNECTION_TIMEOUT = 3  # 연결 타임아웃 (초)
CUSTOM_HEARTBEAT_INTERVAL = 25  # 커스텀 하트비트 메시지 전송 간격 (초)

class BithumbWebSocketConnector(BaseWebsocketConnector):
    """
    빗썸 웹소켓 연결 관리 클래스
    
    빗썸 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    
    책임:
    - 웹소켓 연결 관리 (연결, 재연결, 종료)
    - 연결 상태 모니터링
    """
    def __init__(self, settings: dict, exchange_code: str = None, on_status_change=None):
        """
        빗썸 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
            exchange_code: 거래소 코드 (기본값: None, 자동으로 설정)
            on_status_change: 연결 상태 변경 시 호출될 콜백 함수
        """
        exchange_code = exchange_code or Exchange.BITHUMB.value
        super().__init__(settings, exchange_code, on_status_change)
        
        # 웹소켓 URL 설정
        self.ws_url = WS_URL
        
        # 상태 및 설정값
        self.is_connected = False
        self.connection_timeout = CONNECTION_TIMEOUT
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        self.message_timeout = MESSAGE_TIMEOUT
        
        # 재연결 전략
        self.reconnect_strategy = ReconnectStrategy(
            initial_delay=RECONNECT_DELAY,
            max_delay=60.0,
            multiplier=2.0,
            max_attempts=0
        )
        
        # 커스텀 하트비트 관련 변수
        self.heartbeat_task = None
        self.last_message_time = 0
        self.custom_heartbeat_interval = CUSTOM_HEARTBEAT_INTERVAL

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
            self.connecting = True  # 연결 중 플래그 추가
            self.is_connected = False
            retry_count = 0
            
            while not self.stop_event.is_set():
                try:
                    # 웹소켓 라이브러리의 내장 핑퐁 기능 사용 (타임아웃 값 증가)
                    self.ws = await connect(
                        self.ws_url,
                        ping_interval=self.ping_interval,
                        ping_timeout=self.ping_timeout,
                        close_timeout=10,
                        max_size=None,
                        open_timeout=self.connection_timeout
                    )
                    
                    self.is_connected = True
                    self.log_info("🟢 웹소켓 연결 성공")
                    
                    # 마지막 메시지 시간 초기화
                    self.last_message_time = time.time()
                    
                    # 커스텀 하트비트 시작
                    await self._start_custom_heartbeat()
                    
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

    async def disconnect(self) -> bool:
        """웹소켓 연결 종료"""
        try:
            # 커스텀 하트비트 중지
            await self._stop_custom_heartbeat()
            
            # 기본 연결 종료 로직 수행 (부모 클래스 메서드 호출)
            return await super().disconnect()
            
        except Exception as e:
            self.log_error(f"연결 종료 중 오류: {str(e)}")
            # 연결 상태 초기화 필요
            self.is_connected = False
            return False
            
    async def handle_message(self, message_type: str, size: int = 0, **kwargs) -> None:
        """
        메시지 처리 공통 메서드 (오버라이드)
        
        Args:
            message_type: 메시지 유형
            size: 메시지 크기 (바이트)
            **kwargs: 추가 데이터
        """
        # 메시지 수신 시간 업데이트 (하트비트 메커니즘에 사용)
        self.last_message_time = time.time()
        
        # 부모 클래스의 메서드 호출
        await super().handle_message(message_type, size, **kwargs)
        
    # 커스텀 하트비트 관련 메서드
    # ==================================
    async def _start_custom_heartbeat(self):
        """커스텀 하트비트 메커니즘 시작"""
        # 이미 실행 중인 경우 중지
        await self._stop_custom_heartbeat()
        
        # 새 태스크 생성
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self.log_debug(f"커스텀 하트비트 태스크 시작 (간격: {self.custom_heartbeat_interval}초)")
        
    async def _stop_custom_heartbeat(self):
        """커스텀 하트비트 메커니즘 중지"""
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
            self.heartbeat_task = None
            self.log_debug("커스텀 하트비트 태스크 중지")
            
    async def _heartbeat_loop(self):
        """커스텀 하트비트 루프"""
        try:
            while self.is_connected and not self.stop_event.is_set():
                try:
                    # 특정 시간 이상 메시지가 없으면 상태 확인
                    current_time = time.time()
                    elapsed = current_time - self.last_message_time
                    
                    if elapsed > self.custom_heartbeat_interval:
                        # 빗썸 커스텀 핑 메시지 전송
                        await self._send_custom_ping()
                        
                    # 메시지 타임아웃 체크 (2배 간격으로 체크)
                    if elapsed > self.custom_heartbeat_interval * 2:
                        self.log_warning(f"메시지 타임아웃 감지: {elapsed:.1f}초 동안 메시지 없음, 연결 재확인")
                        
                        # 연결 확인 및 필요시 재연결 요청
                        if self.is_connected:
                            # 웹소켓 객체 확인
                            if not self.ws or self.ws.closed:
                                self.log_warning("웹소켓 객체가 닫혔거나 없음, 연결 상태 업데이트")
                                self.is_connected = False
                                asyncio.create_task(
                                    self.handle_disconnection("heartbeat_check", "웹소켓 객체가 유효하지 않음")
                                )
                    
                    # 다음 체크까지 대기 (1초마다 체크)
                    await asyncio.sleep(1)
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.log_error(f"하트비트 체크 중 오류: {str(e)}")
                    await asyncio.sleep(1)
                    
        except asyncio.CancelledError:
            self.log_debug("하트비트 루프 태스크 취소됨")
        except Exception as e:
            self.log_error(f"하트비트 루프 중 오류: {str(e)}")
            
    async def _send_custom_ping(self):
        """빗썸 커스텀 핑 메시지 전송"""
        try:
            if not self.is_connected or not self.ws:
                return
                
            # 빗썸 API에 맞는 하트비트 메시지 생성
            ping_message = {
                "type": "ping",
                "timestamp": int(time.time() * 1000)
            }
            
            # 메시지 전송
            await self.ws.send(json.dumps(ping_message))
            self.log_debug("커스텀 핑 메시지 전송")
            
            # 마지막 메시지 시간 업데이트
            self.last_message_time = time.time()
            
        except Exception as e:
            self.log_warning(f"커스텀 핑 전송 중 오류: {str(e)}")
            # 오류 발생 시 연결 문제로 간주하고 재연결 요청
            self.is_connected = False
            asyncio.create_task(
                self.handle_disconnection("ping_error", str(e))
            ) 