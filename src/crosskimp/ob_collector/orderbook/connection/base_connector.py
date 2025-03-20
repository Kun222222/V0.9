# file: orderbook/connection/base_connector.py

import asyncio
import time
from typing import Optional, Callable
from asyncio import Event
from dataclasses import dataclass
from abc import ABC, abstractmethod

from crosskimp.logger.logger import get_unified_logger
from crosskimp.telegrambot.telegram_notification import send_telegram_message
from crosskimp.config.constants_v3 import EXCHANGE_NAMES_KR, normalize_exchange_code
from crosskimp.ob_collector.orderbook.util.event_bus import EVENT_TYPES
from crosskimp.ob_collector.orderbook.util.event_handler import EventHandler, LoggingMixin

# 전역 로거 설정
logger = get_unified_logger()

@dataclass
class WebSocketStats:
    """웹소켓 통계 데이터"""
    message_count: int = 0
    error_count: int = 0
    reconnect_count: int = 0
    last_message_time: float = 0.0
    last_error_time: float = 0.0
    last_error_message: str = ""
    connection_start_time: float = 0.0

class WebSocketError(Exception):
    """웹소켓 관련 기본 예외 클래스"""
    pass

@dataclass
class ReconnectStrategy:
    """웹소켓 재연결 전략"""
    initial_delay: float = 1.0  # 초기 재연결 지연 시간
    max_delay: float = 60.0     # 최대 지연 시간
    multiplier: float = 2.0     # 지연 시간 증가 배수
    max_attempts: int = 0       # 최대 시도 횟수 (0=무제한)
    
    current_delay: float = 1.0
    attempts: int = 0
    
    def next_delay(self) -> float:
        """다음 재연결 지연 시간 계산"""
        self.attempts += 1
        
        # 최대 시도 횟수 초과 확인
        if self.max_attempts > 0 and self.attempts > self.max_attempts:
            raise WebSocketError(f"최대 재연결 시도 횟수 초과: {self.attempts}/{self.max_attempts}")
        
        # 첫 번째 시도는 초기 지연 시간 사용
        if self.attempts == 1:
            self.current_delay = self.initial_delay
            return self.current_delay
        
        # 지연 시간 증가 (최대값 제한)
        self.current_delay = min(self.current_delay * self.multiplier, self.max_delay)
        return self.current_delay
    
    def reset(self) -> None:
        """재연결 시도 횟수 및 지연 시간 초기화"""
        self.attempts = 0
        self.current_delay = self.initial_delay

class BaseWebsocketConnector(ABC, LoggingMixin):
    """웹소켓 연결 관리 기본 클래스"""
    def __init__(self, settings: dict, exchangename: str):
        """초기화"""
        # 기본 정보
        self.exchangename = normalize_exchange_code(exchangename)  # 소문자로 정규화
        self.exchange_code = self.exchangename  # 필드명 일관성을 위한 별칭
        self.settings = settings
        
        # 로거 설정 (LoggingMixin 메서드 사용)
        self.setup_logger(self.exchange_code)
        
        # 기본 상태 변수
        self.ws = None
        self.stop_event = Event()
        
        # 연결 상태 플래그 추가
        self.connecting = False
        self._is_connected = False  # 내부 연결 상태 플래그 추가
        
        # 웹소켓 통계
        self.stats = WebSocketStats()
        
        # 이벤트 핸들러 초기화
        self.event_handler = EventHandler.get_instance(self.exchangename, self.settings)
        
        # 이벤트 버스 가져오기 (이벤트 핸들러로부터)
        self.event_bus = self.event_handler.event_bus
        
        # 자식 클래스에서 설정해야 하는 변수들
        self.reconnect_strategy = None  # 재연결 전략
        self.message_timeout = None     # 메시지 타임아웃 (초)
        self.health_check_interval = 5  # 헬스 체크 간격 기본값 (초)
        self.ping_interval = None       # 핑 전송 간격 (초)
        self.ping_timeout = None        # 핑 응답 타임아웃 (초)
        
        # 헬스 체크 태스크 변수 추가
        self.health_check_task = None
        self._start_health_check_task()

    # 속성 관리
    @property
    def is_connected(self) -> bool:
        """연결 상태 확인 - 모든 다른 클래스가 이 속성을 참조함"""
        return self._is_connected
    
    @is_connected.setter
    def is_connected(self, value: bool) -> None:
        """연결 상태 설정"""
        if self._is_connected != value:
            # 상태 변경만 내부적으로 처리
            prev_status = self._is_connected
            self._is_connected = value
            
            # 연결 시간 기록 (내부 통계용)
            if value:
                self.stats.connection_start_time = time.time()
            
            # 이벤트 핸들러로 연결 상태 변경 이벤트 처리 위임
            # 외부 처리(로깅, 알림, 이벤트 발행)는 모두 EventHandler가 담당
            status = "connected" if value else "disconnected"
            asyncio.create_task(self.event_handler.handle_connection_status(
                status=status,
                timestamp=time.time()
            ))

    def _start_health_check_task(self) -> None:
        """헬스 체크 태스크 시작"""
        # 이미 실행 중인 태스크가 있는지 확인
        if hasattr(self, 'health_check_task') and self.health_check_task and not self.health_check_task.done():
            # 이미 실행 중인 태스크가 있으면 새로 시작하지 않음
            self.log_debug("헬스 체크 태스크가 이미 실행 중입니다")
            return
            
        self.health_check_task = asyncio.create_task(self.health_check())
        self.log_debug("헬스 체크 태스크 시작됨")

    def _should_start_health_check(self) -> bool:
        """
        헬스 체크 태스크를 시작해야 하는지 확인
        
        Returns:
            bool: 태스크가 없거나 완료된 경우 True, 실행 중인 경우 False
        """
        return not hasattr(self, 'health_check_task') or self.health_check_task is None or self.health_check_task.done()

    # 웹소켓 연결 관리
    @abstractmethod
    async def connect(self) -> bool:
        """웹소켓 연결"""
        pass
        
    async def disconnect(self) -> bool:
        """웹소켓 연결 종료"""
        try:
            # 헬스 체크 태스크 취소 (완전히 종료되는 경우)
            local_task = getattr(self, 'health_check_task', None)
            if local_task and not local_task.done():
                try:
                    local_task.cancel()
                    await asyncio.wait_for(asyncio.shield(local_task), timeout=0.5)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                except Exception as e:
                    self.log_debug(f"태스크 취소 중 예외 발생: {e}")
                finally:
                    self.health_check_task = None
            
            # 웹소켓 연결 종료
            if self.ws:
                await self.ws.close()
                self.ws = None
            
            # 연결 상태 업데이트 - 이것이 콜백과 메트릭 업데이트를 모두 트리거합니다
            self.is_connected = False
            
            self.log_info("웹소켓 연결 종료됨")
            return True
                
        except Exception as e:
            self.log_error(f"연결 종료 실패: {str(e)}")
            # 상태는 여전히 업데이트해야 함
            self.is_connected = False
            return False
    
    async def reconnect(self) -> bool:
        """웹소켓 재연결"""
        try:
            self.stats.reconnect_count += 1
            reconnect_msg = f"웹소켓 재연결 시도"
            self.log_info(reconnect_msg)
            
            # 재연결 이벤트 처리 (이벤트 핸들러 사용)
            await self.event_handler.handle_connection_status(
                status="reconnecting",
                message=reconnect_msg
            )
            
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
        웹소켓 객체 반환 (필요시 자동 연결)
        
        Returns:
            웹소켓 객체 또는 None (연결 실패 시)
        """
        # 이미 연결된 상태면 현재 웹소켓 반환
        if self.is_connected and self.ws:
            return self.ws
        
        # 연결 중이면 중복 연결 방지
        if self.connecting:
            self.log_debug("이미 연결 시도 중")
            # 잠시 대기 후 상태 확인 (동시 연결 시도 방지)
            for _ in range(10):  # 최대 1초 대기
                await asyncio.sleep(0.1)
                if self.is_connected and self.ws:
                    return self.ws
            # 여전히 연결되지 않았다면 현재 상태 반환 (None일 수 있음)
            return self.ws
        
        # 연결되지 않았다면 자동으로 연결 시도
        self.log_info("웹소켓 연결이 없어 자동 연결 시도")
        
        try:
            # connect()는 is_connected 속성을 설정하므로 여기서는 설정하지 않음
            success = await self.connect()
            return self.ws if success else None
        except Exception as e:
            self.log_error(f"자동 연결 중 오류 발생: {str(e)}")
            return None

    # 상태 모니터링 (중앙화)
    async def health_check(self) -> None:
        """웹소켓 상태 체크 (주기적 모니터링)"""
        cancel_log_shown = False
        try:
            self.log_info("상태 모니터링 시작")
            
            while not self.stop_event.is_set():
                try:
                    # 연결 상태 확인 로직
                    current_connection_state = False
                    
                    # 웹소켓 객체가 존재하는지 확인
                    if self.ws:
                        # 여기서 추가적인 연결 상태 확인 (자식 클래스에서 구현 가능)
                        current_connection_state = True
                    
                    # 상태가 변경되었거나 특정 조건을 만족하면 상태 업데이트
                    if current_connection_state != self.is_connected:
                        self.is_connected = current_connection_state
                    
                    # 핑 전송 등 추가 상태 체크 로직은 자식 클래스에서 구현
                    
                    # 대기
                    await asyncio.sleep(self.health_check_interval)
                    
                except asyncio.CancelledError:
                    cancel_log_shown = True
                    raise  # 상위로 전파
                    
                except Exception as e:
                    self.log_error(f"상태 체크 중 오류: {str(e)}")
                    await asyncio.sleep(1)  # 오류 발생 시 짧게 대기
                
        except asyncio.CancelledError:
            if not cancel_log_shown:
                self.log_info("상태 모니터링 태스크 취소됨")
        
        except Exception as e:
            self.log_error(f"상태 모니터링 루프 오류: {str(e)}")
            
            # 모니터링 태스크가 중단되지 않도록 재시작
            # 단, 종료 이벤트가 설정되지 않은 경우에만 재시작
            if not self.stop_event.is_set():
                asyncio.create_task(self._restart_health_check())

    async def _restart_health_check(self) -> None:
        """헬스 체크 태스크 재시작 (오류 발생 시)"""
        try:
            await asyncio.sleep(1)  # 잠시 대기
            self._start_health_check_task()
        except Exception as e:
            self.log_error(f"헬스 체크 태스크 재시작 실패: {str(e)}")

    # 이벤트 처리는 이벤트 핸들러에 위임
    async def handle_error(self, error_type: str, message: str, severity: str = "error", **kwargs) -> None:
        """오류 이벤트 처리 (이벤트 핸들러 사용)"""
        await self.event_handler.handle_error(error_type, message, severity, **kwargs)
        
        # 웹소켓 통계 업데이트 - 오류 카운트 증가
        self.stats.error_count += 1
        self.stats.last_error_time = time.time()
        self.stats.last_error_message = message
    
    async def handle_message(self, message_type: str, size: int = 0, **kwargs) -> None:
        """메시지 수신 이벤트 처리 (이벤트 핸들러 사용)"""
        await self.event_handler.handle_message_received(message_type, size, **kwargs)
        
        # 웹소켓 통계 업데이트
        self.stats.message_count += 1
        self.stats.last_message_time = time.time()