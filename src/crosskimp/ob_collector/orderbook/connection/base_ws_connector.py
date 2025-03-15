# file: core/websocket/base_websocket.py

import asyncio
import time
import json
import os
import logging
from typing import Dict, List, Any, Optional, Callable
from asyncio import Event
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from enum import Enum

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import EXCHANGE_NAMES_KR, LOG_SYSTEM, STATUS_EMOJIS, WEBSOCKET_CONFIG, WEBSOCKET_COMMON_CONFIG, Exchange, WebSocketState, LogMessageType
from crosskimp.config.paths import LOG_SUBDIRS
from crosskimp.telegrambot.telegram_notification import send_telegram_message

# 전역 로거 설정
logger = get_unified_logger()

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

        # 거래소 한글 이름 설정
        self.exchange_korean_name = EXCHANGE_NAMES_KR.get(exchangename, f"[{exchangename}]")

        self.ws = None
        self.is_connected = False
        self.connecting = False  # 연결 중 상태 플래그 추가
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
            multiplier=reconnect_cfg["multiplier"],
            max_attempts=reconnect_cfg["max_attempts"]
        )

        # 헬스체크 설정
        self.health_check_interval = WEBSOCKET_COMMON_CONFIG["health_check_interval"]
        self.message_timeout = WEBSOCKET_COMMON_CONFIG["message_timeout"]
        self.ping_interval = exchange_config.get("ping_interval", 30)
        self.ping_timeout = exchange_config.get("ping_timeout", 10)

        # 모니터링 콜백
        self.connection_status_callback: Optional[Callable[[str, str], None]] = None
        
        # 성능 측정
        self.message_processing_times = []
        self.last_performance_log = time.time()
        self.performance_log_interval = 300  # 5분
        
        # 로깅 설정
        logging_config = settings.get("logging", {})
        self.log_raw_data = logging_config.get("log_raw_data", True)
        self.raw_logger = None
        
        # 로깅 파일 경로 설정
        if self.log_raw_data:
            try:
                # 로그 디렉토리 설정
                raw_data_dir = LOG_SUBDIRS['raw_data']
                log_dir = raw_data_dir / exchangename
                log_dir.mkdir(exist_ok=True, parents=True)
                
                # 로그 파일 경로 설정 - 날짜와 시간 포함
                current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.log_file_path = log_dir / f"{exchangename}_raw_{current_datetime}.log"
                
                # 로거 설정
                self.raw_logger = logging.getLogger(f"raw_data.{exchangename}")
                if not self.raw_logger.handlers:
                    file_handler = logging.FileHandler(str(self.log_file_path), encoding="utf-8")
                    formatter = logging.Formatter('%(asctime)s - %(message)s')
                    file_handler.setFormatter(formatter)
                    self.raw_logger.addHandler(file_handler)
                    self.raw_logger.setLevel(logging.INFO)
                    self.raw_logger.propagate = False
                
                logger.info(f"{self.exchange_korean_name} Raw 데이터 로깅 설정 완료: {self.log_file_path}")
            except Exception as e:
                logger.error(f"{self.exchange_korean_name} Raw 데이터 로깅 설정 실패: {str(e)}", exc_info=True)
                self.log_raw_data = False

    # 로깅 헬퍼 메서드들 - 모든 로깅은 이 메서드들을 통해 수행
    def _update_error_stats(self, msg: str):
        """오류 통계 업데이트"""
        self.stats.error_count += 1
        self.stats.last_error_time = time.time()
        self.stats.last_error_message = msg

    def log_error(self, msg: str, exc_info: bool = True):
        """오류 로깅"""
        self._update_error_stats(msg)
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
        warning_msg = f"{self.exchange_korean_name} {STATUS_EMOJIS.get('RECONNECTING', '🟠')} {msg}"
        logger.warning(warning_msg)

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
        웹소켓 연결 수행
        
        이 메서드는 템플릿 메서드 패턴을 사용하여 연결 과정을 정의합니다.
        실제 연결 로직은 자식 클래스의 _do_connect 메서드에서 구현합니다.
        """
        # 이미 연결된 경우 바로 반환
        if self.is_connected and self.ws:
            return True
            
        # 연결 중인 경우 대기
        if self.connecting:
            self.log_debug("이미 연결 중")
            return True
            
        self.connecting = True
        
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
            
            # 연결 성공 알림 추가
            connect_msg = f"{self.exchange_korean_name} 웹소켓 연결 성공"
            await self._send_telegram_notification("connect", connect_msg)
            
            # 4. 연결 후 처리 (자식 클래스에서 선택적으로 구현)
            await self._after_connect()
            
            # 5. 연결 성공 상태 업데이트
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect")
            
            self.log_info("웹소켓 연결 성공")
            return True
            
        except asyncio.TimeoutError as e:
            # 연결 중 플래그 해제
            self.connecting = False
            
            # 타임아웃 처리 (자식 클래스에서 오버라이드 가능)
            return await self._handle_timeout_error(e)
                
        except Exception as e:
            self.log_error(f"연결 오류: {str(e)}", exc_info=True)
            
            # 연결 중 플래그 해제
            self.connecting = False
            
            # 연결 실패 처리 및 재연결 시도
            return await self._handle_connection_failure("error", e)
            
        finally:
            self.connecting = False

    async def _handle_timeout_error(self, exception: asyncio.TimeoutError) -> bool:
        """
        타임아웃 에러 처리 (자식 클래스에서 오버라이드 가능)
        
        Args:
            exception: 타임아웃 예외 객체
            
        Returns:
            bool: 재연결 시도 여부
        """
        # 기본 구현: 타임아웃 에러 로깅 및 재연결 시도
        self.log_error(f"연결 타임아웃: {str(exception)}")
        return await self._handle_connection_failure("timeout", exception)

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
        
        error_msg = f"{self.exchange_korean_name} 웹소켓 연결 실패 ({reason}): {str(exception) if exception else '알 수 없는 오류'}"
        self.log_error(error_msg)
        await self._send_telegram_notification("error", error_msg)
        
        if not self.auto_reconnect or self.stop_event.is_set():
            self.log_warning("자동 재연결 비활성화 상태")
            return False
            
        delay = self.reconnect_strategy.next_delay()
        wait_msg = f"{self.exchange_korean_name} 재연결 대기: {delay:.1f}초 (시도: {self.reconnect_strategy.attempt})"
        self.log_info(wait_msg)
        
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "reconnect_wait")
            
        await asyncio.sleep(delay)
        
        if self.stop_event.is_set():
            return False
            
        reconnect_msg = f"{self.exchange_korean_name} 재연결 시도"
        self.log_info(reconnect_msg)
        await self._send_telegram_notification("reconnect", reconnect_msg)
        
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
            self.log_info("웹소켓 연결 종료")
            return True
            
        except Exception as e:
            self.log_error(f"웹소켓 연결 종료 실패: {str(e)}")
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
                
            reconnect_msg = f"{self.exchange_korean_name} 웹소켓 재연결 시도 중 (시도 횟수: {self.stats.reconnect_count + 1})"
            self.log_info(reconnect_msg)
            
            # 텔레그램 알림 전송
            await self._send_telegram_notification("reconnect", reconnect_msg)
            
            await self.disconnect()
            
            # 잠시 대기 후 재연결
            await asyncio.sleep(1)
            
            success = await self.connect()
            
            if success:
                success_msg = f"{self.exchange_korean_name} 웹소켓 재연결 성공"
                self.log_info(success_msg)
                await self._send_telegram_notification("connect", success_msg)
            else:
                fail_msg = f"{self.exchange_korean_name} 웹소켓 재연결 실패"
                self.log_error(fail_msg)
                await self._send_telegram_notification("error", fail_msg)
            
            return success
            
        except Exception as e:
            error_msg = f"{self.exchange_korean_name} 웹소켓 재연결 실패: {str(e)}"
            self.log_error(error_msg)
            await self._send_telegram_notification("error", error_msg)
            return False

    async def health_check(self):
        """
        웹소켓 상태 체크
        
        일정 간격으로 연결 상태를 확인하고, 필요시 재연결을 시도합니다.
        이 메서드는 백그라운드 태스크로 실행됩니다.
        """
        self.log_info(f"헬스 체크 시작 (간격: {self.health_check_interval}초)")
        
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                
                # 메시지 타임아웃 체크
                if self.is_connected and self.stats.last_message_time > 0 and (current_time - self.stats.last_message_time) > self.message_timeout:
                    error_msg = f"{self.exchange_korean_name} 웹소켓 메시지 타임아웃 발생 ({self.message_timeout}초), 마지막 메시지: {current_time - self.stats.last_message_time:.1f}초 전"
                    self.log_error(error_msg)
                    
                    # 텔레그램 알림 전송
                    await self._send_telegram_notification("error", error_msg)
                    
                    await self.reconnect()
                
                # 연결 상태 로깅 (디버그)
                if self.is_connected:
                    uptime = current_time - self.stats.connection_start_time
                    last_msg_time_diff = current_time - self.stats.last_message_time if self.stats.last_message_time > 0 else -1
                    self.log_debug(
                        f"헬스 체크: 연결됨, "
                        f"업타임={uptime:.1f}초, "
                        f"메시지={self.stats.message_count}개, "
                        f"마지막 메시지={last_msg_time_diff:.1f}초 전, "
                        f"오류={self.stats.error_count}개"
                    )
                
                await asyncio.sleep(self.health_check_interval)
                
            except Exception as e:
                self.log_error(f"웹소켓 상태 체크 중 오류 발생: {str(e)}")
                await asyncio.sleep(1)

    async def ping(self):
        """
        주기적으로 핑 메시지 전송
        
        일정 간격으로 핑 메시지를 전송하여 연결 상태를 유지합니다.
        이 메서드는 백그라운드 태스크로 실행됩니다.
        """
        self.log_info(f"핑 태스크 시작 (간격: {self.ping_interval}초)")
        
        while not self.stop_event.is_set():
            try:
                if self.is_connected and self.ws:
                    if self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "ping")
                        
                    self.log_debug("핑 전송")
                    await self.ws.ping()
                    self.stats.last_ping_time = time.time()
                    
                await asyncio.sleep(self.ping_interval)
                
            except Exception as e:
                self.log_error(f"웹소켓 핑 전송 중 오류 발생: {str(e)}")
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
        self.log_info("웹소켓 연결 및 태스크 종료 완료")

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
            self.log_warning("구독할 심볼이 없습니다.")
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
            
            self.log_info(f"웹소켓 시작 완료 (심볼: {len(symbols)}개)")
            
        except Exception as e:
            self.log_error(f"웹소켓 시작 실패: {str(e)}")
            
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
        # 연결 끊김 감지 시 다음과 같은 코드를 추가해야 함:
        # 
        # except websockets.exceptions.ConnectionClosed as e:
        #     error_msg = f"{self.exchange_korean_name} 웹소켓 연결 끊김: {str(e)}"
        #     self.log_error(error_msg)
        #     await self._send_telegram_notification("error", error_msg)
        #     self.is_connected = False
        #     if not self.stop_event.is_set():
        #         await self.reconnect()
        pass

    def log_raw_message(self, msg_type: str, message: str, symbol: str) -> None:
        """
        원시 메시지 로깅
        
        Args:
            msg_type: 메시지 타입 (예: "orderbook", "trade")
            message: 원시 메시지 내용
            symbol: 관련 심볼
        """
        if not self.log_raw_data:
            return
            
        try:
            # 로그 항목 생성
            timestamp = int(time.time() * 1000)
            log_entry = f"{timestamp}|{msg_type}|{symbol}|{message}"
            
            # 파일 로깅 (raw_logger가 없는 경우)
            if not self.raw_logger:
                try:
                    # 로그 디렉토리 확인
                    if not hasattr(self, 'log_file_path') or not self.log_file_path:
                        # 로그 디렉토리 설정
                        raw_data_dir = LOG_SUBDIRS['raw_data']
                        log_dir = raw_data_dir / self.exchangename
                        log_dir.mkdir(exist_ok=True, parents=True)
                        
                        # 로그 파일 경로 설정
                        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
                        self.log_file_path = log_dir / f"{self.exchangename}_raw_{current_datetime}.log"
                    
                    # 파일에 직접 로깅
                    with open(self.log_file_path, "a", encoding="utf-8") as f:
                        f.write(log_entry + "\n")
                        
                    # 통계 업데이트
                    self.stats.raw_logged_messages += 1
                    
                except Exception as e:
                    logger.error(f"{self.exchange_korean_name} 파일 로깅 실패: {str(e)}")
            
            # 로거 객체를 통한 로깅 (파일 로깅 실패 시 대체 방법)
            if self.raw_logger:
                self.raw_logger.info(log_entry)
        except Exception as e:
            self.log_error(f"Raw 로깅 실패: {str(e)}")

    async def _send_telegram_notification(self, event_type: str, message: str) -> None:
        """
        텔레그램 알림 전송
        
        Args:
            event_type: 이벤트 타입 (connect, error, subscribe 등)
            message: 알림 메시지
        """
        try:
            # 이벤트 타입에 따른 MessageType 결정
            message_type = self._get_message_type_for_event(event_type)
                
            # 텔레그램 메시지 전송
            if isinstance(message, dict):
                await send_telegram_message(self.settings, message_type, message)
            else:
                await send_telegram_message(self.settings, message_type, {"message": message})
            
            # 로깅 추가
            self.log_debug(f"텔레그램 알림 전송 성공: {event_type}")
            
        except Exception as e:
            self.log_error(f"텔레그램 알림 전송 실패: {event_type} - {str(e)}")

    def _get_message_type_for_event(self, event_type: str) -> LogMessageType:
        """
        이벤트 타입에 따른 텔레그램 메시지 타입 반환
        
        Args:
            event_type: 이벤트 타입 문자열
            
        Returns:
            LogMessageType: 해당하는 텔레그램 메시지 타입
        """
        event_map = {
            "error": LogMessageType.ERROR,
            "warning": LogMessageType.WARNING,
            "info": LogMessageType.INFO,
            "connect": LogMessageType.CONNECTION,
            "disconnect": LogMessageType.DISCONNECT,
            "reconnect": LogMessageType.RECONNECT,
            "subscribe": LogMessageType.INFO,
            "unsubscribe": LogMessageType.INFO,
            "ping": LogMessageType.INFO,
            "pong": LogMessageType.INFO,
            "snapshot": LogMessageType.INFO,
            "message": LogMessageType.INFO,
            "trade": LogMessageType.TRADE,
            "orderbook": LogMessageType.INFO,
            "market": LogMessageType.MARKET,
            "system": LogMessageType.SYSTEM
        }
        
        return event_map.get(event_type.lower(), LogMessageType.INFO)