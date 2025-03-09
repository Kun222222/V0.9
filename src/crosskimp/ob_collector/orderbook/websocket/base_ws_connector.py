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
    """웹소켓 연결 상태 정보"""
    is_connected: bool
    last_message_time: float
    reconnect_count: int
    error_count: int
    last_error: Optional[str]
    uptime_sec: float
    latency_ms: float

class WebSocketError(Exception):
    """웹소켓 관련 기본 예외 클래스"""
    pass

class ConnectionError(WebSocketError):
    """웹소켓 연결 관련 예외 클래스"""
    pass

class ReconnectStrategy:
    """지수 백오프 재연결 전략"""
    def __init__(self, initial_delay: float = 1.0, max_delay: float = 60.0, multiplier: float = 2.0, max_attempts: int = 0):
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.attempt = 0
        self.max_attempts = max_attempts  # 0은 무제한 재시도

    def next_delay(self) -> float:
        """다음 재시도 대기 시간 계산"""
        delay = min(self.initial_delay * (self.multiplier ** self.attempt), self.max_delay)
        self.attempt += 1
        return delay

    def reset(self):
        """재시도 카운터 초기화"""
        self.attempt = 0

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class BaseWebsocketConnector:
    """
    웹소켓 연결 관리 특화 클래스
    
    이 클래스는 웹소켓 연결 관리에 특화되어 있으며, 다음 기능을 제공합니다:
    - 연결 프로세스 표준화 (연결 시도, 성공/실패 처리, 상태 업데이트)
    - 재연결 메커니즘 (지수 백오프 알고리즘)
    - 예외 처리 (타임아웃, 네트워크 오류, 서버 오류 등)
    - 상태 관리 (연결 상태 추적, 통계 데이터 수집)
    - 헬스 체크 (연결 상태 모니터링, 자동 재연결)
    
    자식 클래스에서는 다음 메서드를 구현해야 합니다:
    - _get_connection_params(): 거래소별 연결 파라미터 반환
    - _do_connect(): 실제 연결 수행
    - _after_connect(): 연결 후 처리 (선택적)
    """
    def __init__(self, settings: dict, exchangename: str):
        self.exchangename = exchangename
        self.settings = settings

        self.ws = None
        self.is_connected = False
        self.stop_event = Event()
        self.output_queue: Optional[asyncio.Queue] = None
        self.auto_reconnect = True  # 자동 재연결 활성화 여부

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
        
        # 성능 측정
        self.message_processing_times = []
        self.last_performance_log = time.time()
        self.performance_log_interval = 300  # 5분

    def get_connection_status(self) -> ConnectionStatus:
        """현재 연결 상태 정보 반환"""
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
        """메시지 출력 큐 설정"""
        self.output_queue = queue

    def log_error(self, msg: str, exc_info: bool = True):
        """에러 로깅 공통 메서드"""
        self.stats.error_count += 1
        self.stats.last_error_time = time.time()
        self.stats.last_error_message = msg
        self.logger.error(f"[{self.exchangename}] {msg}", exc_info=exc_info)
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "error")

    def _get_connection_params(self) -> dict:
        """
        거래소별 연결 파라미터 반환 (자식 클래스에서 오버라이드)
        
        Returns:
            dict: 웹소켓 연결에 사용할 파라미터
        """
        return {
            "ping_interval": self.ping_interval,
            "ping_timeout": self.ping_timeout,
            "compression": None
        }

    async def connect(self):
        """
        표준화된 연결 프로세스
        
        연결 시도 → 성공/실패 처리 → 상태 업데이트의 기본 흐름을 구현합니다.
        
        Returns:
            bool: 연결 성공 여부
        """
        try:
            # 1. 연결 전 상태 업데이트
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect_attempt")
            
            # 2. 실제 연결 수행 (자식 클래스에서 구현)
            await self._do_connect()
            
            # 3. 연결 성공 처리
            self.is_connected = True
            self.stats.connection_start_time = time.time()
            self.reconnect_strategy.reset()  # 재연결 전략 리셋
            
            # 4. 연결 후 처리 (자식 클래스에서 선택적으로 구현)
            await self._after_connect()
            
            # 5. 연결 성공 상태 업데이트
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect")
            
            self.logger.info(f"[{self.exchangename}] 웹소켓 연결 성공")
            return True
            
        except asyncio.TimeoutError:
            self.log_error(f"연결 타임아웃")
            return await self._handle_connection_failure("timeout")
            
        except Exception as e:
            self.log_error(f"연결 실패: {str(e)}")
            return await self._handle_connection_failure("error", e)

    async def _do_connect(self):
        """
        실제 연결 로직 (자식 클래스에서 구현)
        
        이 메서드는 자식 클래스에서 거래소별 특화된 연결 로직을 구현해야 합니다.
        """
        raise NotImplementedError("자식 클래스에서 _do_connect 메서드를 구현해야 합니다.")

    async def _after_connect(self):
        """
        연결 후 처리 (자식 클래스에서 선택적으로 구현)
        
        이 메서드는 연결 성공 후 추가 처리가 필요한 경우 자식 클래스에서 구현합니다.
        """
        pass

    async def _handle_connection_failure(self, reason: str, exception: Optional[Exception] = None) -> bool:
        """
        연결 실패 처리
        
        Args:
            reason: 실패 이유 ("timeout", "error" 등)
            exception: 발생한 예외 객체 (선택적)
            
        Returns:
            bool: 재연결 시도 여부
        """
        self.stats.reconnect_count += 1
        
        if not self.auto_reconnect or self.stop_event.is_set():
            self.logger.warning(f"[{self.exchangename}] 자동 재연결 비활성화 상태")
            return False
            
        delay = self.reconnect_strategy.next_delay()
        self.logger.info(f"[{self.exchangename}] 재연결 대기: {delay:.1f}초 (시도: {self.reconnect_strategy.attempt})")
        
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "reconnect_wait")
            
        await asyncio.sleep(delay)
        
        if self.stop_event.is_set():
            return False
            
        self.logger.info(f"[{self.exchangename}] 재연결 시도")
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "reconnect_attempt")
            
        return await self.connect()  # 재귀적 재연결 시도

    async def disconnect(self):
        """
        웹소켓 연결 종료
        
        Returns:
            bool: 종료 성공 여부
        """
        try:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "disconnect")
                
            if self.ws:
                await self.ws.close()
                
            self.is_connected = False
            self.logger.info(f"[{self.exchangename}] 웹소켓 연결 종료")
            return True
            
        except Exception as e:
            self.log_error(f"연결 종료 실패: {str(e)}")
            return False

    async def reconnect(self):
        """
        웹소켓 재연결
        
        현재 연결을 종료하고 새로운 연결을 시도합니다.
        
        Returns:
            bool: 재연결 성공 여부
        """
        try:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "reconnect_attempt")
                
            self.logger.info(f"[{self.exchangename}] 웹소켓 재연결 시작")
            await self.disconnect()
            
            # 잠시 대기 후 재연결
            await asyncio.sleep(1)
            
            return await self.connect()
            
        except Exception as e:
            self.log_error(f"재연결 실패: {str(e)}")
            return False

    async def health_check(self):
        """
        웹소켓 상태 체크
        
        일정 간격으로 연결 상태를 확인하고, 필요시 재연결을 시도합니다.
        이 메서드는 백그라운드 태스크로 실행됩니다.
        """
        self.logger.info(f"[{self.exchangename}] 헬스 체크 시작 (간격: {self.health_check_interval}초)")
        
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                
                # 메시지 타임아웃 체크
                if self.is_connected and self.stats.last_message_time > 0 and (current_time - self.stats.last_message_time) > self.message_timeout:
                    self.log_error(f"메시지 타임아웃 발생 ({self.message_timeout}초), 마지막 메시지: {current_time - self.stats.last_message_time:.1f}초 전")
                    await self.reconnect()
                
                # 연결 상태 로깅 (디버그)
                if self.is_connected:
                    uptime = current_time - self.stats.connection_start_time
                    last_msg_time_diff = current_time - self.stats.last_message_time if self.stats.last_message_time > 0 else -1
                    self.logger.debug(
                        f"[{self.exchangename}] 헬스 체크: 연결됨, "
                        f"업타임={uptime:.1f}초, "
                        f"메시지={self.stats.message_count}개, "
                        f"마지막 메시지={last_msg_time_diff:.1f}초 전, "
                        f"오류={self.stats.error_count}개"
                    )
                
                await asyncio.sleep(self.health_check_interval)
                
            except Exception as e:
                self.log_error(f"상태 체크 중 오류 발생: {str(e)}")
                await asyncio.sleep(1)

    async def ping(self):
        """
        주기적으로 핑 메시지 전송
        
        일정 간격으로 핑 메시지를 전송하여 연결 상태를 유지합니다.
        이 메서드는 백그라운드 태스크로 실행됩니다.
        """
        self.logger.info(f"[{self.exchangename}] 핑 태스크 시작 (간격: {self.ping_interval}초)")
        
        while not self.stop_event.is_set():
            try:
                if self.is_connected and self.ws:
                    if self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "ping")
                        
                    self.logger.debug(f"[{self.exchangename}] 핑 전송")
                    await self.ws.ping()
                    self.stats.last_ping_time = time.time()
                    
                await asyncio.sleep(self.ping_interval)
                
            except Exception as e:
                self.log_error(f"핑 전송 중 오류 발생: {str(e)}")
                await asyncio.sleep(1)

    async def start_background_tasks(self):
        """
        백그라운드 태스크 시작
        
        헬스 체크 및 핑 태스크를 시작합니다.
        """
        return [
            asyncio.create_task(self.health_check()),
            asyncio.create_task(self.ping())
        ]

    async def stop(self) -> None:
        """
        웹소켓 연결 및 모든 태스크 종료
        """
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "stop")
            
        self.stop_event.set()
        await self.disconnect()
        self.logger.info(f"[{self.exchangename}] 웹소켓 연결 및 태스크 종료 완료")

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        """
        웹소켓 연결 시작 및 심볼 구독 (템플릿 메서드)
        
        이 메서드는 다음과 같은 단계로 실행됩니다:
        1. 초기화 및 설정 (_prepare_start)
        2. 연결 (connect)
        3. 심볼 구독 (subscribe)
        4. 백그라운드 태스크 시작 (start_background_tasks)
        5. 메시지 처리 루프 실행 (_run_message_loop)
        
        자식 클래스에서는 필요에 따라 _prepare_start, _run_message_loop 메서드를 오버라이드할 수 있습니다.
        
        Args:
            symbols_by_exchange: 거래소별 구독할 심볼 목록
        """
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "start")
            
        # 이 거래소에 대한 심볼 목록 가져오기
        symbols = symbols_by_exchange.get(self.exchangename, [])
        
        if not symbols:
            self.logger.warning(f"[{self.exchangename}] 구독할 심볼이 없습니다.")
            return
        
        # 1. 초기화 및 설정
        await self._prepare_start(symbols)
        
        try:
            # 2. 연결
            if not self.is_connected:
                await self.connect()
                
            # 3. 심볼 구독
            await self.subscribe(symbols)
            
            # 4. 백그라운드 태스크 시작
            tasks = await self.start_background_tasks()
            
            # 5. 메시지 처리 루프 실행
            await self._run_message_loop(symbols, tasks)
            
            self.logger.info(f"[{self.exchangename}] 웹소켓 시작 완료 (심볼: {len(symbols)}개)")
            
        except Exception as e:
            self.log_error(f"시작 실패: {str(e)}")
            
    async def _prepare_start(self, symbols: List[str]) -> None:
        """
        시작 전 초기화 및 설정 (자식 클래스에서 오버라이드 가능)
        
        Args:
            symbols: 구독할 심볼 목록
        """
        pass
            
    async def _run_message_loop(self, symbols: List[str], tasks: List[asyncio.Task]) -> None:
        """
        메시지 처리 루프 실행 (자식 클래스에서 오버라이드 필요)
        
        이 메서드는 자식 클래스에서 구현해야 합니다.
        웹소켓으로부터 메시지를 수신하고 처리하는 루프를 실행합니다.
        
        Args:
            symbols: 구독한 심볼 목록
            tasks: 실행 중인 백그라운드 태스크 목록
        """
        # 기본 구현은 아무 작업도 하지 않음
        # 자식 클래스에서 오버라이드하여 실제 메시지 처리 루프를 구현해야 함
        pass