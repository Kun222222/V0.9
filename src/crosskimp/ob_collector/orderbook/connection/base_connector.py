# file: core/websocket/base_websocket.py

import asyncio
import time
import json
import logging
from typing import Dict, List, Any, Optional, Callable
from asyncio import Event
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod
from enum import Enum, auto

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.paths import LOG_SUBDIRS
from crosskimp.telegrambot.telegram_notification import send_telegram_message

# ============================
# 거래소 이름 및 상태 관련 상수
# ============================
# 거래소 코드 -> 한글 이름 매핑
EXCHANGE_NAMES_KR = {
    "UPBIT": "[업비트]",
    "BYBIT": "[바이빗]",
    "BINANCE": "[바이낸스]",
    "BITHUMB": "[빗썸]",
    "BINANCE_FUTURE": "[바이낸스 선물]",
    "BYBIT_FUTURE": "[바이빗 선물]",
}

# 상태 이모지
STATUS_EMOJIS = {
    "ERROR": "🔴",
    "WARNING": "🟠",
    "INFO": "🟢",
    "CONNECTED": "🟢",
    "DISCONNECTED": "🔴",
}

# 웹소켓 공통 설정
WEBSOCKET_COMMON_CONFIG = {
    "reconnect_strategy": {
        "initial_delay": 1.0,
        "max_delay": 60.0,
        "multiplier": 2.0,
        "max_attempts": 10
    },
    "connection_timeout": 30.0,
    "message_timeout": 60.0,
    "ping_interval": 30.0,
    "ping_timeout": 5.0,
    "health_check_interval": 10.0
}

# 거래소별 웹소켓 설정
WEBSOCKET_CONFIG = {
    "UPBIT": {
        "url": "wss://api.upbit.com/websocket/v1",
        "ping_message": '{"ticket":"PING"}',
        "ping_interval": 20.0,
        "market": "KRW"
    },
    "BYBIT": {
        "url": "wss://stream.bybit.com/v5/public/spot",
        "ping_message": '{"op":"ping"}',
        "ping_interval": 20.0,
        "market": "USDT"
    },
    "BINANCE": {
        "url": "wss://stream.binance.com:9443/ws",
        "ping_message": '{"method":"ping"}',
        "ping_interval": 30.0,
        "market": "USDT"
    },
    "BITHUMB": {
        "url": "wss://pubwss.bithumb.com/pub/ws",
        "ping_message": '{"type":"ping"}',
        "ping_interval": 20.0,
        "market": "KRW"
    },
    "BINANCE_FUTURE": {
        "url": "wss://fstream.binance.com/ws",
        "ping_message": '{"method":"ping"}',
        "ping_interval": 30.0,
        "market": "USDT"
    },
    "BYBIT_FUTURE": {
        "url": "wss://stream.bybit.com/v5/public/linear",
        "ping_message": '{"op":"ping"}',
        "ping_interval": 20.0,
        "market": "USDT"
    }
}

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
    """
    웹소켓 재연결 전략
    
    Attributes:
        initial_delay: 초기 재연결 지연 시간 (초)
        max_delay: 최대 재연결 지연 시간 (초)
        multiplier: 지연 시간 증가 배수
        max_attempts: 최대 재연결 시도 횟수 (0=무제한)
        current_delay: 현재 지연 시간
        attempts: 현재까지 시도 횟수
    """
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    max_attempts: int = 0  # 0 = 무제한
    
    current_delay: float = 1.0
    attempts: int = 0
    
    def next_delay(self) -> float:
        """
        다음 재연결 지연 시간 계산
        
        Returns:
            float: 다음 재연결 지연 시간 (초)
        """
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
    """
    웹소켓 연결 관리를 위한 기본 클래스
    
    이 클래스는 웹소켓 연결 관리를 위한 공통 기능을 제공합니다.
    각 거래소별 구현체는 이 클래스를 상속받아 필요한 메서드를 구현해야 합니다.
    
    책임:
    - 웹소켓 연결 관리 (연결, 재연결, 종료)
    - 원시 메시지 수신 및 전송
    - 연결 상태 모니터링
    """
    def __init__(self, settings: dict, exchangename: str):
        """
        기본 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
            exchangename: 거래소 이름
        """
        self.exchangename = exchangename
        self.settings = settings
        self.exchange_korean_name = EXCHANGE_NAMES_KR.get(exchangename, f"[{exchangename}]")

        # 기본 상태 변수
        self.ws = None
        self.stop_event = Event()
        
        # 웹소켓 통계
        self.stats = WebSocketStats()
        
        # 메트릭 매니저 인스턴스 가져오기
        from crosskimp.ob_collector.orderbook.metric.metrics_manager import WebsocketMetricsManager
        self.metrics = WebsocketMetricsManager.get_instance()
        
        # 자식 클래스에서 설정해야 하는 변수들
        self.reconnect_strategy = None  # 재연결 전략
        self.message_timeout = None     # 메시지 타임아웃 (초)
        self.health_check_interval = None  # 헬스 체크 간격 (초)
        self.ping_interval = None       # 핑 전송 간격 (초)
        self.ping_timeout = None        # 핑 응답 타임아웃 (초)
        
        # 연결 상태 콜백 (WebsocketManager와 호환)
        self.connection_status_callback = None

    # is_connected 속성 대신 메트릭 매니저를 통해 조회하는 프로퍼티로 변경
    @property
    def is_connected(self) -> bool:
        """연결 상태 확인"""
        return self.metrics.is_connected(self.exchangename)
    
    @is_connected.setter
    def is_connected(self, value: bool) -> None:
        """
        연결 상태 설정
        
        직접 설정하면 메트릭 매니저를 통해 업데이트
        """
        # 현재 저장된 상태 확인 (메트릭스에서 바로 조회)
        current_state = self.metrics.is_connected(self.exchangename)
        
        # 실제 상태 변경이 있을 때만 업데이트 및 로깅
        if current_state != value:
            state = "connected" if value else "disconnected"
            self.metrics.update_connection_state(self.exchangename, state)
            
            # 중요한 상태 변경만 로깅 (디버그 레벨 낮춤)
            if value:
                self.log_debug(f"연결 상태 변경: 연결됨")
            else:
                self.log_debug(f"연결 상태 변경: 연결 끊김")

    # 로깅 헬퍼 메서드들
    def log_error(self, msg: str, exc_info: bool = True):
        """오류 로깅"""
        self.stats.error_count += 1
        self.stats.last_error_time = time.time()
        self.stats.last_error_message = msg
        
        error_msg = f"{self.exchange_korean_name} {STATUS_EMOJIS.get('ERROR', '🔴')} {msg}"
        logger.error(error_msg, exc_info=exc_info)

    def log_info(self, msg: str):
        """정보 로깅"""
        info_msg = f"{self.exchange_korean_name} {msg}"
        logger.info(info_msg)

    def log_debug(self, msg: str):
        """디버그 로깅"""
        debug_msg = f"{self.exchange_korean_name} {msg}"
        logger.debug(debug_msg)

    def log_warning(self, msg: str):
        """경고 로깅"""
        warning_msg = f"{self.exchange_korean_name} {STATUS_EMOJIS.get('WARNING', '🟠')} {msg}"
        logger.warning(warning_msg)

    def update_message_metrics(self, message: str) -> None:
        """
        메시지 수신 시 통계 업데이트
        
        Args:
            message: 수신된 메시지
        """
        # 메시지 통계 업데이트
        self.stats.message_count += 1
        current_time = time.time()
        self.stats.last_message_time = current_time
        
        # 연결 상태 업데이트 - 메시지를 받았으면 연결된 상태지만
        # 매 메시지마다 상태를 업데이트하면 로그가 너무 많이 생성됨
        # 연결이 끊어진 상태에서 메시지를 받은 경우만 상태 업데이트
        if not self.is_connected:
            self.is_connected = True
        
        # 메트릭 매니저에 메시지 수신 기록
        self.metrics.record_message(self.exchangename)

    async def send_telegram_notification(self, event_type: str, message: str) -> None:
        """
        텔레그램 알림 전송
        
        Args:
            event_type: 이벤트 타입 ('error', 'connect', 'disconnect', 'reconnect' 등)
            message: 전송할 메시지
        """
        # 중요 이벤트만 텔레그램으로 전송 (에러, 연결, 재연결)
        if event_type not in ["error", "connect", "disconnect", "reconnect"]:
            return
            
        try:
            # 텔레그램 메시지 전송 시 message_type을 문자열로 전달
            # telegram_notification.py에서는 LogMessageType 객체를 기대하지만,
            # 문자열도 직접 MessageType으로 변환 가능함
            message_type = self._get_message_type_for_event(event_type)
                
            # 텔레그램 메시지 전송
            if isinstance(message, dict):
                await send_telegram_message(self.settings, message_type, message)
            else:
                await send_telegram_message(self.settings, message_type, {"message": message})
            
        except Exception as e:
            self.log_error(f"텔레그램 알림 전송 실패: {event_type} - {str(e)}")

    def _get_message_type_for_event(self, event_type: str) -> str:
        """
        이벤트 타입에 따른 메시지 타입 반환
        
        Args:
            event_type: 이벤트 타입
            
        Returns:
            str: 메시지 타입
        """
        if event_type == "error":
            return "error"  # MessageType.ERROR 대신 문자열 사용
        elif event_type == "connect":
            return "info"   # MessageType.INFO 대신 문자열 사용
        elif event_type == "disconnect":
            return "warning"  # MessageType.WARNING 대신 문자열 사용
        elif event_type == "reconnect":
            return "warning"  # MessageType.WARNING 대신 문자열 사용
        else:
            return "info"   # MessageType.INFO 대신 문자열 사용

    @abstractmethod
    async def connect(self) -> bool:
        """
        웹소켓 연결 수행
        
        Returns:
            bool: 연결 성공 여부
        """
        pass
        
    @abstractmethod
    async def disconnect(self) -> bool:
        """
        웹소켓 연결 종료
        
        Returns:
            bool: 종료 성공 여부
        """
        pass
        
    @abstractmethod
    async def send_message(self, message: str) -> bool:
        """
        웹소켓을 통해 메시지 전송
        
        Args:
            message: 전송할 메시지
            
        Returns:
            bool: 전송 성공 여부
        """
        pass
        
    @abstractmethod
    async def health_check(self) -> None:
        """
        웹소켓 상태 체크 (백그라운드 태스크)
        """
        pass
        
    @abstractmethod
    async def reconnect(self) -> bool:
        """
        웹소켓 재연결
        
        Returns:
            bool: 재연결 성공 여부
        """
        pass
    
    @abstractmethod
    async def receive_raw(self) -> Optional[str]:
        """
        웹소켓에서 원시 메시지 수신 (추상 메소드)
        
        Returns:
            Optional[str]: 수신된 원시 메시지 또는 None
        """
        pass