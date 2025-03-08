# file: core/websocket/base_websocket.py

import asyncio
import time
import json
from typing import Dict, List, Any, Optional, Callable
from asyncio import Event
from dataclasses import dataclass
from datetime import datetime

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.utils.config.constants import EXCHANGE_NAMES_KR, LOG_SYSTEM, STATUS_EMOJIS, WEBSOCKET_CONFIG, WEBSOCKET_COMMON_CONFIG

@dataclass
class WebSocketStats:
    """웹소켓 통계 데이터"""
    connected: bool = False
    message_count: int = 0
    error_count: int = 0
    reconnect_count: int = 0
    last_message_time: float = 0.0
    last_error_time: float = 0.0
    last_error_message: str = ""
    last_ping_time: float = 0.0
    last_pong_time: float = 0.0
    latency_ms: float = 0.0
    connection_lost_count: int = 0
    total_uptime_sec: float = 0.0
    connection_start_time: float = 0.0
    total_messages: int = 0
    raw_logged_messages: int = 0

@dataclass
class ConnectionStatus:
    is_connected: bool
    last_message_time: float
    reconnect_count: int
    error_count: int
    last_error: Optional[str]
    uptime_sec: float
    latency_ms: float

class WebSocketError(Exception):
    pass

class ConnectionError(WebSocketError):
    pass

class ReconnectStrategy:
    """지수 백오프 재연결 전략"""
    def __init__(self, initial_delay: float = 1.0, max_delay: float = 60.0, multiplier: float = 2.0, max_attempts: int = 0):
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.attempt = 0
        self.max_attempts = max_attempts

    def next_delay(self) -> float:
        delay = min(self.initial_delay * (self.multiplier ** self.attempt), self.max_delay)
        self.attempt += 1
        return delay

    def reset(self):
        self.attempt = 0

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class BaseWebsocket:
    """
    웹소켓 공통 기능 (재연결, 헬스체크, 상태 콜백 등).
    자식 클래스(예: binance_spot_websocket.py)에서
    connect/subscribe/parse_message/handle_parsed_message 등을 구현합니다.
    """
    def __init__(self, settings: dict, exchangename: str):
        self.exchangename = exchangename
        self.settings = settings

        self.ws = None
        self.is_connected = False
        self.stop_event = Event()
        self.output_queue: Optional[asyncio.Queue] = None

        # 웹소켓 통계
        self.stats = WebSocketStats()

        # 거래소별 설정 로드
        exchange_config = WEBSOCKET_CONFIG.get(exchangename, {})
        
        # 재연결 설정
        reconnect_cfg = WEBSOCKET_COMMON_CONFIG["reconnect"]
        self.reconnect_strategy = ReconnectStrategy(
            initial_delay=reconnect_cfg["initial_delay"],
            max_delay=reconnect_cfg["max_delay"],
            max_attempts=reconnect_cfg["max_attempts"]
        )

        # 헬스체크 설정
        self.health_check_interval = WEBSOCKET_COMMON_CONFIG["health_check_interval"]
        self.message_timeout = WEBSOCKET_COMMON_CONFIG["message_timeout"]
        self.ping_interval = exchange_config.get("ping_interval", 30)
        self.ping_timeout = exchange_config.get("ping_timeout", 10)

        # 모니터링 콜백
        self.connection_status_callback: Optional[Callable[[str, str], None]] = None

        # 로거 설정
        self.logger = logger
        
        self.message_processing_times = []
        self.last_performance_log = time.time()
        self.performance_log_interval = 300  # 5분

    def get_connection_status(self) -> ConnectionStatus:
        now = time.time()
        uptime = (now - self.stats.connection_start_time) if self.is_connected else 0
        return ConnectionStatus(
            is_connected=self.is_connected,
            last_message_time=self.stats.last_message_time,
            reconnect_count=self.stats.reconnect_count,
            error_count=self.stats.error_count,
            last_error=self.stats.last_error_message,
            uptime_sec=uptime,
            latency_ms=self.stats.latency_ms
        )

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        self.output_queue = queue

    def log_error(self, msg: str, exc_info: bool = True):
        """에러 로깅 공통 메서드"""
        self.stats.error_count += 1
        self.stats.last_error_time = time.time()
        self.stats.last_error_message = msg
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "error")

    async def connect(self):
        """웹소켓 연결 시도 (로깅 처리)"""
        try:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect_attempt")
            # 실제 연결 수행
            await self._do_connect()
            
            # 연결 성공 로깅
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect")
            
        except Exception as e:
            self.log_error(f"연결 실패: {str(e)}")
            raise

    async def _do_connect(self):
        """실제 연결 로직 (자식 클래스에서 구현)"""
        raise NotImplementedError

    async def disconnect(self):
        """웹소켓 연결 종료"""
        try:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "disconnect")
            if self.ws:
                await self.ws.close()
            self.is_connected = False
        except Exception as e:
            self.log_error(f"연결 종료 실패: {str(e)}")

    async def reconnect(self):
        """웹소켓 재연결"""
        try:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "reconnect_attempt")
            await self.disconnect()
            await self.connect()
        except Exception as e:
            self.log_error(f"재연결 실패: {str(e)}")

    async def health_check(self):
        """웹소켓 상태 체크"""
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                if self.is_connected and (current_time - self.stats.last_message_time) > self.message_timeout:
                    self.log_error(f"메시지 타임아웃 발생 ({self.message_timeout}초)")
                    await self.reconnect()
                await asyncio.sleep(self.health_check_interval)
            except Exception as e:
                self.log_error(f"상태 체크 중 오류 발생: {str(e)}")
                await asyncio.sleep(1)

    async def ping(self):
        """주기적으로 핑 메시지 전송"""
        while not self.stop_event.is_set():
            try:
                if self.is_connected:
                    if self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "ping")
                    if self.ws:
                        await self.ws.ping()
                        self.stats.last_ping_time = time.time()
                await asyncio.sleep(self.ping_interval)
            except Exception as e:
                self.log_error(f"핑 전송 중 오류 발생: {str(e)}")
                await asyncio.sleep(1)

    async def subscribe(self, symbols: List[str]):
        """자식 클래스에서 구현"""
        raise NotImplementedError

    async def parse_message(self, message: str) -> Optional[dict]:
        """
        메시지 파싱 공통 로직
        - 모든 메시지 카운팅
        - 자식 클래스에서는 _parse_message를 구현
        """
        try:
            # 모든 수신 메시지 카운트
            self.stats.message_count += 1
            
            # JSON 파싱
            data = json.loads(message)
            
            # 자식 클래스의 구체적인 파싱 로직 호출
            parsed = await self._parse_message(data)
            
            return parsed
            
        except json.JSONDecodeError as je:
            self.log_error(f"JSON 파싱 실패: {je}, msg={message[:200]}", exc_info=False)
        except Exception as e:
            self.log_error(f"parse_message 예외: {e}", exc_info=True)
        return None

    async def _parse_message(self, data: dict) -> Optional[dict]:
        """자식 클래스에서 구현할 실제 파싱 로직"""
        raise NotImplementedError

    async def handle_parsed_message(self, parsed: dict) -> None:
        """자식 클래스에서 구현할 메시지 처리 로직"""
        raise NotImplementedError

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        """자식 클래스에서 구현할 시작 로직"""
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "start")

    async def stop(self) -> None:
        """자식 클래스에서 구현할 종료 로직"""
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "stop")
        self.stop_event.set()
        await self.disconnect()