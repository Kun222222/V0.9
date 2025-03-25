# file: orderbook/connection/base_connector.py

import asyncio
import time
from typing import Optional, Callable, Dict, Any, List, Tuple
from asyncio import Event
from dataclasses import dataclass
from abc import ABC, abstractmethod
import datetime
import logging
import websockets

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import EXCHANGE_NAMES_KR, normalize_exchange_code
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.events.system_eventbus import get_event_bus
from crosskimp.common.config.common_constants import EventType

# 전역 로거 설정
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

@dataclass
class WebSocketStats:
    """웹소켓 통계 데이터"""
    message_count: int = 0
    error_count: int = 0
    reconnect_count: int = 0
    last_error_time: float = 0.0
    last_error_message: str = ""
    connection_start_time: float = 0.0
    total_bytes_received: int = 0
    total_bytes_sent: int = 0

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

class BaseWebsocketConnector(ABC):
    """웹소켓 연결 관리 기본 클래스"""
    def __init__(self, settings: dict, exchangename: str, on_status_change=None):
        """
        초기화
        
        Args:
            settings: 설정 정보
            exchangename: 거래소 이름
            on_status_change: 연결 상태 변경 시 호출될 콜백 함수
        """
        # 기본 정보
        self.exchangename = normalize_exchange_code(exchangename)  # 소문자로 정규화
        self.exchange_code = self.exchangename  # 필드명 일관성을 위한 별칭
        self.exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, self.exchange_code)  # 한글 거래소명
        self.settings = settings
        
        # 콜백 함수 설정
        self.on_status_change = on_status_change
        
        # 로거 설정
        self.logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)
        
        # 기본 상태 변수
        self.ws = None
        self.stop_event = Event()
        
        # 연결 상태 플래그 추가
        self.connecting = False
        self._is_connected = False  # 내부 연결 상태 플래그 추가
        
        # 연결 시도 카운터 추가
        self._connection_attempt_count = 0
        
        # 웹소켓 통계
        self.stats = WebSocketStats()
        
        # 자식 클래스에서 설정해야 하는 변수들
        self.reconnect_strategy = None  # 재연결 전략
        self.message_timeout = None     # 메시지 타임아웃 (초)
        self.ping_interval = None       # 핑 전송 간격 (초)
        self.ping_timeout = None        # 핑 응답 타임아웃 (초)
        
        # URL 속성 추가 (자식 클래스에서 설정)
        self.ws_url = None
        
        # 초기 메트릭 설정
        self._update_connection_metric("status", "initialized")
        self._update_connection_metric("reconnect_count", 0)
        self._update_connection_metric("last_error", "")
        self._update_connection_metric("last_error_time", 0)
        self._update_connection_metric("uptime", 0)

    # 로깅 메서드
    def log_debug(self, message: str) -> None:
        """디버그 로그 출력"""
        # self.logger.debug(f"{self.exchange_kr} {message}")

    def log_info(self, message: str) -> None:
        """정보 로그 출력"""
        self.logger.info(f"{self.exchange_kr} {message}")

    def log_warning(self, message: str) -> None:
        """경고 로그 출력"""
        self.logger.warning(f"{self.exchange_kr} {message}")

    def log_error(self, message: str, exc_info: bool = False) -> None:
        """오류 로그 출력"""
        self.logger.error(f"{self.exchange_kr} {message}", exc_info=exc_info)

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
            
            # 콜백 함수 호출
            if self.on_status_change:
                status = "connected" if value else "disconnected"
                callback_data = {
                    "exchange_code": self.exchange_code,
                    "status": status,
                    "timestamp": time.time(),
                    "initial_connection": prev_status is False and value is True
                }
                # 비동기 태스크로 콜백 실행
                asyncio.create_task(self.on_status_change(status, **callback_data))
            
            # 연결 상태 메트릭 업데이트
            self._update_connection_metric("status", "connected" if value else "disconnected")
                
            # 연결 상태 메시지 로깅
            if value:
                self.log_info(f"웹소켓 연결 성공! URL: {self.ws_url}")
            else:
                # 연결 해제 메시지는 오류가 아닐 수 있음
                self.log_info("웹소켓 연결 해제됨")

    # 웹소켓 연결 관리
    @abstractmethod
    async def connect(self) -> bool:
        """웹소켓 연결"""
        pass
        
    async def disconnect(self) -> bool:
        """웹소켓 연결 종료"""
        try:
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
            self.log_info("웹소켓 재연결 시도")
            
            # 재연결 카운트 메트릭 업데이트
            self._update_connection_metric("reconnect_count", self.stats.reconnect_count)
            
            await self.disconnect()
            
            # 재연결 지연 계산
            delay = self.reconnect_strategy.next_delay()
            await asyncio.sleep(delay)
            
            success = await self.connect()
            
            return success
            
        except Exception as e:
            self.log_error(f"재연결 실패: {str(e)}")
            return False

    async def connect_with_timeout(self, timeout: float = 1.0) -> bool:
        """
        시간 제한이 있는 웹소켓 연결
        
        여러 거래소가 병렬로 연결될 수 있도록, 지정된 시간 내에 연결을 시도하고
        시간이 초과되면 백그라운드에서 연결을 계속 시도합니다.
        
        Args:
            timeout: 연결 타임아웃 (초)
            
        Returns:
            bool: 연결 성공 여부 (시간 내에 연결 성공시 True, 아니면 False)
        """
        # 이미 연결된 경우 바로 반환
        if self.is_connected and self.ws:
            self.log_debug("이미 연결되어 있음")
            return True
            
        # 연결 중인 경우 대기 중임을 알림
        if self.connecting:
            self.log_debug(f"이미 연결 시도 중")
            
            # 짧은 시간(0.1초) 동안 대기한 후 상태 다시 확인
            for _ in range(int(timeout * 10)):  # timeout을 0.1초 단위로 나눠서 반복
                await asyncio.sleep(0.1)
                if self.is_connected:
                    return True
                
            # 타임아웃 내에 연결 안됨
            return False
            
        # 연결 상태 업데이트
        self.connecting = True
        
        try:
            # 연결 태스크 생성
            connect_task = asyncio.create_task(self.connect())
            
            try:
                # 제한 시간 내에 연결 완료 시도
                await asyncio.wait_for(connect_task, timeout=timeout)
                return self.is_connected
            except asyncio.TimeoutError:
                # 시간 초과되었지만 연결 태스크는 계속 실행
                self.log_debug(f"연결 시간 초과 ({timeout}초), 백그라운드에서 계속 시도")
                # 오류 없이 False 반환
                return False
            except Exception as e:
                # 기타 예외 발생 시, 연결 태스크 취소 
                if not connect_task.done():
                    connect_task.cancel()
                self.log_error(f"연결 중 오류 발생: {str(e)}")
                return False
        finally:
            # 연결 상태가 설정되면 connecting 플래그 해제
            # 별도 태스크로 실행 중인 경우 해당 태스크에서 처리
            if not self.is_connected:
                self.connecting = False

    async def get_websocket(self):
        """
        현재 웹소켓 인스턴스 반환 (존재하는 경우)
        
        Returns:
            WebSocket: 웹소켓 인스턴스 또는 None
        """
        if not self.is_connected or not self.ws:
            return None
        return self.ws

    async def handle_message(self, message_type: str, size: int = 0, **kwargs) -> None:
        """
        메시지 처리 공통 메서드
        
        Args:
            message_type: 메시지 유형
            size: 메시지 크기 (바이트)
            **kwargs: 추가 데이터
        """
        try:
            # 메시지 수신 시간 기록
            self.last_message_received = time.time()
            
            # 내부 통계 업데이트
            self.stats.message_count += 1
            
            # 로깅
            self.log_debug(f"메시지 수신: 유형={message_type}, 크기={size}바이트")
            
        except Exception as e:
            self.log_error(f"메시지 처리 중 예외 발생: {str(e)}")

    async def handle_error(self, error_type: str, message: str, severity: str = "error", **kwargs) -> None:
        """
        오류 처리 공통 메서드
        
        Args:
            error_type: 오류 유형
            message: 오류 메시지
            severity: 오류 심각도 (error, warning, critical)
            **kwargs: 추가 데이터
        """
        try:
            # 내부 통계 업데이트
            self.stats.error_count += 1
            self.stats.last_error_time = time.time()
            self.stats.last_error_message = message
            
            # 마지막 오류 메트릭 업데이트
            self._update_connection_metric("last_error", message)
            self._update_connection_metric("last_error_time", time.time())
            
            # 오류 심각도에 따른 로깅
            if severity == "critical":
                self.log_error(f"심각한 오류 발생: [{error_type}] {message}", exc_info=True)
            elif severity == "error":
                self.log_error(f"오류 발생: [{error_type}] {message}")
            elif severity == "warning":
                self.log_warning(f"경고: [{error_type}] {message}")
            else:
                self.log_info(f"알림: [{error_type}] {message}")
            
        except Exception as e:
            self.log_error(f"오류 처리 중 예외 발생: {str(e)}")

    def _update_connection_metric(self, metric_name, value):
        """연결 관련 메트릭 업데이트 함수"""
        # 직접 호출 방식으로 변경되어 이 함수는 임시로 로깅만 수행
        self.log_debug(f"메트릭 업데이트: {metric_name}={value}")
