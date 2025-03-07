# file: core/websocket/base_websocket.py

import asyncio
import time
import json
from typing import Dict, List, Any, Optional, Callable
from asyncio import Event
from dataclasses import dataclass
from datetime import datetime

from utils.logging.logger import get_unified_logger
from config.constants import EXCHANGE_NAMES_KR

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

        # 재연결 설정
        reconnect_cfg = settings.get("websocket", {}).get("reconnect", {})
        self.reconnect_strategy = ReconnectStrategy(
            initial_delay=reconnect_cfg.get("initial_delay", 1.0),
            max_delay=reconnect_cfg.get("max_delay", 60.0),
            multiplier=reconnect_cfg.get("multiplier", 2.0),
            max_attempts=reconnect_cfg.get("max_attempts", 0)
        )

        # 헬스체크 설정
        health_cfg = settings.get("websocket", {}).get("health_check", {})
        self.health_check_interval = health_cfg.get("interval", 10)
        self.message_timeout = health_cfg.get("timeout", 30)
        self.ping_interval = health_cfg.get("ping_interval", 20)

        # 모니터링 콜백
        self.on_status_change: Optional[Callable[[str, ConnectionStatus], None]] = None
        self.connection_status_callback: Optional[Callable[[str, str], None]] = None

        # 거래소별 로거 매핑
        self.logger = logger
        
        self.logger.info(
            f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] 초기화 완료 | "
            f"헬스체크={self.health_check_interval}초, "
            f"메시지 타임아웃={self.message_timeout}초, "
            f"핑 간격={self.ping_interval}초"
        )

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
        self.logger.info(f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] 출력 큐 설정 완료")

    def log_raw_message(self, message_type: str, data: dict, symbol: str = None) -> None:
        """
        웹소켓 raw 메시지 로깅을 위한 공통 메서드
        
        Args:
            message_type: 메시지 타입 (예: depthUpdate)
            data: 원본 메시지 데이터
            symbol: 심볼명 (옵션)
        """
        if symbol:
            self.logger.debug(f"원본: [{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] ({symbol}) {data}")
        else:
            self.logger.debug(f"원본: [{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] {data}")

    def log_sequence_gap(self, symbol: str, last_id: int, first_id: int, gap_size: int = None) -> None:
        """
        시퀀스 갭 발생 시 공통 로깅 메서드
        
        Args:
            symbol: 심볼명
            last_id: 마지막으로 처리된 시퀀스 ID
            first_id: 새로 받은 시퀀스의 시작 ID
            gap_size: 갭 크기 (옵션)
        """
        if gap_size is not None:
            self.logger.warning(
                f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] {symbol} 시퀀스 갭 발생 | "
                f"마지막ID={last_id}, 시작ID={first_id}, 갭={gap_size}"
            )
        else:
            self.logger.warning(
                f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] {symbol} 시퀀스 갭 발생 | "
                f"마지막ID={last_id}, 시작ID={first_id}"
            )

    def log_error(self, msg: str, exc_info: bool = True):
        """에러 로깅 공통 메서드"""
        self.stats.error_count += 1
        self.stats.last_error_time = time.time()
        self.stats.last_error_message = msg
        self.logger.error(f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] {msg}", exc_info=exc_info)

    async def connect(self):
        """웹소켓 연결 시도"""
        try:
            self.logger.info(
                f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] "
                f"연결 시도 중..."
            )
            # ... existing code ...
        except Exception as e:
            self.logger.error(
                f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] "
                f"연결 실패: {str(e)}"
            )
            raise

    async def disconnect(self):
        """웹소켓 연결 종료"""
        try:
            self.logger.info(
                f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] "
                f"연결 종료 중..."
            )
            # ... existing code ...
        except Exception as e:
            self.logger.error(
                f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] "
                f"연결 종료 실패: {str(e)}"
            )

    async def reconnect(self):
        """웹소켓 재연결"""
        try:
            self.logger.warning(
                f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] "
                f"재연결 시도 중..."
            )
            # ... existing code ...
        except Exception as e:
            self.logger.error(
                f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] "
                f"재연결 실패: {str(e)}"
            )

    async def health_check(self):
        """웹소켓 상태 체크"""
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                if self.is_connected and (current_time - self.stats.last_message_time) > self.message_timeout:
                    self.logger.warning(
                        f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] "
                        f"메시지 타임아웃 발생 ({self.message_timeout}초)"
                    )
                    await self.reconnect()
                await asyncio.sleep(self.health_check_interval)
            except Exception as e:
                self.logger.error(
                    f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] "
                    f"상태 체크 중 오류 발생: {str(e)}"
                )
                await asyncio.sleep(1)

    async def ping(self):
        """주기적으로 핑 메시지 전송"""
        while not self.stop_event.is_set():
            try:
                if self.is_connected:
                    self.logger.debug(
                        f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] "
                        f"핑 전송"
                    )
                    # ... existing code ...
                await asyncio.sleep(self.ping_interval)
            except Exception as e:
                self.logger.error(
                    f"[{EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)}] "
                    f"핑 전송 중 오류 발생: {str(e)}"
                )
                await asyncio.sleep(1)

    async def subscribe(self, symbols: List[str]):
        """자식 클래스에서 구현"""
        raise NotImplementedError

    async def parse_message(self, message: str) -> Optional[dict]:
        """
        메시지 파싱 공통 로직
        - 모든 메시지 카운팅
        - raw 메시지 로깅
        - 자식 클래스에서는 _parse_message를 구현
        """
        try:
            # 모든 수신 메시지 카운트
            self.stats.message_count += 1
            
            # JSON 파싱
            data = json.loads(message)
            
            # 자식 클래스의 구체적인 파싱 로직 호출
            parsed = await self._parse_message(data)
            if parsed:
                # raw 메시지 로깅 (파싱 성공한 경우만)
                symbol = self._extract_symbol(parsed)
                self.log_raw_message(self._get_message_type(parsed), message, symbol)
                
                # 주기적 통계 출력
                if self.stats.message_count % 1000 == 0:
                    self.logger.info(
                        f"[{self.exchangename}] 메시지 통계: "
                        f"전체={self.stats.message_count}"
                    )
            
            return parsed
            
        except json.JSONDecodeError as je:
            self.log_error(f"JSON 파싱 실패: {je}, msg={message[:200]}", exc_info=False)
        except Exception as e:
            self.log_error(f"parse_message 예외: {e}", exc_info=True)
        return None

    async def _parse_message(self, data: dict) -> Optional[dict]:
        """자식 클래스에서 구현할 실제 파싱 로직"""
        raise NotImplementedError

    def _extract_symbol(self, parsed: dict) -> Optional[str]:
        """
        심볼 추출 기본 로직 (자식 클래스에서 오버라이드 가능)
        """
        return parsed.get("symbol", "UNKNOWN")

    def _get_message_type(self, parsed: dict) -> str:
        """
        메시지 타입 추출 기본 로직 (자식 클래스에서 오버라이드 가능)
        """
        return "depthUpdate"  # 대부분의 거래소가 사용하는 기본값

    async def handle_parsed_message(self, parsed: dict) -> None:
        start_time = time.time()
        try:
            # 기존 메시지 처리 코드...
            
            # 처리 시간 기록
            processing_time = (time.time() - start_time) * 1000
            self.message_processing_times.append(processing_time)
            
            # 주기적으로 성능 로그 출력
            current_time = time.time()
            if current_time - self.last_performance_log > self.performance_log_interval:
                avg_time = sum(self.message_processing_times) / len(self.message_processing_times)
                max_time = max(self.message_processing_times)
                
                self.logger.info(
                    f"[{self.exchangename}] 메시지 처리 성능 | "
                    f"평균={avg_time:.2f}ms, "
                    f"최대={max_time:.2f}ms, "
                    f"샘플수={len(self.message_processing_times):,}개"
                )
                
                # 메트릭 초기화
                self.message_processing_times = []
                self.last_performance_log = current_time
                
        except Exception as e:
            self.logger.error(f"메시지 처리 중 오류: {e}")

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        """자식 클래스에서 오버라이드"""
        raise NotImplementedError

    async def stop(self) -> None:
        self.logger.info(f"[{self.exchangename}] stop called")
        self.stop_event.set()
        if self.ws:
            await self.ws.close()
        self.is_connected = False
        self.logger.info(f"[{self.exchangename}] websocket stopped")