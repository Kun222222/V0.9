"""
통합 오류 관리 시스템

시스템 전체의 오류를 중앙에서 관리하고 적절한 처리를 수행하는 모듈입니다.
오류 분류, 필터링, 알림, 로깅 등을 담당합니다.
"""

import asyncio
import time
import threading
import re
import logging
from typing import Dict, List, Set, Optional, Any, Callable, Union, Tuple
import traceback

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.legacy.constants_v3 import LOG_SYSTEM
from crosskimp.common.events.system_eventbus import get_component_event_bus
from crosskimp.common.config.common_constants import Component, StatusEventTypes
from crosskimp.common.events.domains.orderbook import OrderbookEventTypes
from crosskimp.telegrambot.notification_manager import get_notification_manager

# 로거 설정
logger = get_unified_logger()

# 오류 카테고리 정의
class ErrorCategory:
    SYSTEM = "system"          # 시스템 수준 오류
    CONNECTION = "connection"  # 연결 관련 오류
    DATA = "data"              # 데이터 처리 오류
    EXCHANGE = "exchange"      # 거래소 특정 오류
    SUBSCRIPTION = "subscription"  # 구독 관련 오류
    PROCESS = "process"        # 프로세스 관련 오류
    RESOURCE = "resource"      # 시스템 리소스 관련 오류
    SECURITY = "security"      # 보안 관련 오류
    DATABASE = "database"      # 데이터베이스 관련 오류
    VALIDATION = "validation"  # 데이터 검증 오류
    UNKNOWN = "unknown"        # 분류되지 않은 오류

# 오류 심각도 정의
class ErrorSeverity:
    INFO = "info"          # 정보성 메시지
    WARNING = "warning"    # 경고, 주의 필요
    ERROR = "error"        # 일반 오류
    CRITICAL = "critical"  # 심각한 오류, 즉시 조치 필요

# 알림 채널 정의
class NotificationChannel:
    LOG = "log"            # 로그 메시지
    TELEGRAM = "telegram"  # 텔레그램 메시지
    EMAIL = "email"        # 이메일
    SMS = "sms"            # 문자 메시지
    EVENT = "event"        # 이벤트 버스
    DASHBOARD = "dashboard"  # 대시보드 알림

class ErrorManager:
    """
    통합 오류 관리 클래스
    
    시스템 전체의 오류를 중앙에서 수집, 분류, 처리하는 싱글톤 클래스입니다.
    
    특징:
    1. 오류 분류 및 심각도 평가
    2. 중복 오류 필터링
    3. 적절한 알림 채널 선택
    4. 이벤트 버스와 통합
    5. 오류 통계 및 요약 제공
    """
    
    _instance = None
    _lock = threading.RLock()
    
    @classmethod
    def get_instance(cls) -> 'ErrorManager':
        """싱글톤 인스턴스 반환"""
        with cls._lock:
            if cls._instance is None:
                cls._instance = ErrorManager()
            return cls._instance
    
    def __init__(self):
        """초기화 - ErrorManager는 싱글톤이므로 get_instance()로만 접근해야 함"""
        if ErrorManager._instance is not None:
            raise Exception("ErrorManager는 싱글톤입니다. get_instance()를 사용하세요.")
        
        # 이벤트 버스 가져오기
        self.event_bus = get_component_event_bus(Component.SYSTEM)
        
        # 오류 패턴 정의 - 정규식 패턴을 카테고리 및 심각도에 매핑
        self.error_patterns = {
            r'연결.*실패|연결.*끊김|timeout|connection.*fail': 
                (ErrorCategory.CONNECTION, ErrorSeverity.ERROR),
            r'웹소켓.*오류|websocket.*error': 
                (ErrorCategory.CONNECTION, ErrorSeverity.ERROR),
            r'메시지.*처리.*실패|parse.*fail|json.*error': 
                (ErrorCategory.DATA, ErrorSeverity.WARNING),
            r'구독.*실패|subscribe.*fail': 
                (ErrorCategory.SUBSCRIPTION, ErrorSeverity.ERROR),
            r'프로세스.*종료|process.*terminated': 
                (ErrorCategory.PROCESS, ErrorSeverity.ERROR),
            r'메모리.*부족|cpu.*high|disk.*full': 
                (ErrorCategory.RESOURCE, ErrorSeverity.CRITICAL),
            r'인증.*실패|authentication.*fail|unauthorized': 
                (ErrorCategory.SECURITY, ErrorSeverity.ERROR),
            r'데이터베이스.*오류|database.*error|query.*fail': 
                (ErrorCategory.DATABASE, ErrorSeverity.ERROR),
            r'검증.*실패|validation.*fail|invalid': 
                (ErrorCategory.VALIDATION, ErrorSeverity.WARNING),
        }
        
        # 최근 오류 저장소 - 중복 방지용
        self.recent_errors = {}  # 키: error_key, 값: (시간, 발생 횟수)
        
        # 카테고리별 처리 핸들러
        self.category_handlers = {}
        
        # 심각도별 알림 채널 매핑
        self.severity_channels = {
            ErrorSeverity.INFO: [NotificationChannel.LOG],
            ErrorSeverity.WARNING: [NotificationChannel.LOG, NotificationChannel.EVENT],
            ErrorSeverity.ERROR: [NotificationChannel.LOG, NotificationChannel.EVENT, NotificationChannel.TELEGRAM],
            ErrorSeverity.CRITICAL: [NotificationChannel.LOG, NotificationChannel.EVENT, 
                                    NotificationChannel.TELEGRAM]
        }
        
        # 오류 통계
        self.error_stats = {
            "total_count": 0,
            "by_category": {},
            "by_severity": {},
            "by_source": {}
        }
        
        # 쿨다운 시간 (초) - 동일 오류 반복 알림 방지
        self.cooldown_times = {
            ErrorSeverity.INFO: 300,      # 5분
            ErrorSeverity.WARNING: 600,   # 10분
            ErrorSeverity.ERROR: 1800,    # 30분
            ErrorSeverity.CRITICAL: 3600  # 1시간
        }
        
        # 차단된 오류 패턴 (완전히 무시할 패턴)
        self.blocked_patterns = set()
        
        # 오류 태스크 큐
        self.error_queue = asyncio.Queue()
        self.processor_task = None
        
        logger.info(f"{LOG_SYSTEM} 오류 관리자 초기화 완료")
    
    async def start(self):
        """오류 관리자 시작"""
        if self.processor_task is None or self.processor_task.done():
            self.processor_task = asyncio.create_task(self._error_processor())
            logger.debug(f"{LOG_SYSTEM} 오류 처리 태스크 시작됨")
    
    async def stop(self):
        """오류 관리자 중지"""
        if self.processor_task and not self.processor_task.done():
            self.processor_task.cancel()
            try:
                await self.processor_task
            except asyncio.CancelledError:
                pass
            logger.debug(f"{LOG_SYSTEM} 오류 처리 태스크 중지됨")
    
    async def handle_error(self, 
                        message: str, 
                        source: str = None, 
                        category: str = None, 
                        severity: str = None, 
                        exchange_code: str = None,
                        tags: List[str] = None, 
                        cooldown_override: int = None,
                        notify: bool = True,
                        **metadata) -> bool:
        """
        오류 처리 (외부에서 호출하는 주요 API)
        
        Args:
            message: 오류 메시지
            source: 오류 발생 소스 (모듈/컴포넌트 이름)
            category: 오류 카테고리 (지정하지 않으면 자동 분류)
            severity: 오류 심각도 (지정하지 않으면 자동 분류)
            exchange_code: 거래소 코드 (거래소 관련 오류인 경우)
            tags: 관련 태그 목록
            cooldown_override: 쿨다운 시간 재정의 (초)
            notify: 알림 발송 여부
            **metadata: 추가 메타데이터
            
        Returns:
            bool: 처리 성공 여부
        """
        try:
            # 오류 정보 구성
            error_info = {
                "message": message,
                "source": source or "unknown",
                "timestamp": time.time(),
                "exchange_code": exchange_code,
                "tags": tags or [],
                "metadata": metadata,
                "notify": notify
            }
            
            # 오류 분류 (카테고리, 심각도)
            if category is None or severity is None:
                auto_category, auto_severity = self._classify_error(message)
                error_info["category"] = category or auto_category
                error_info["severity"] = severity or auto_severity
            else:
                error_info["category"] = category
                error_info["severity"] = severity
            
            # 쿨다운 시간 설정
            if cooldown_override is not None:
                error_info["cooldown"] = cooldown_override
            else:
                error_info["cooldown"] = self.cooldown_times.get(
                    error_info["severity"], 
                    self.cooldown_times[ErrorSeverity.WARNING]
                )
            
            # 차단된 패턴 확인
            if self._is_blocked_pattern(message):
                return False
            
            # 큐에 추가 (비동기 처리)
            await self.error_queue.put(error_info)
            return True
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 오류 처리 중 예외 발생: {str(e)}")
            return False
    
    async def _error_processor(self):
        """오류 처리 태스크 - 백그라운드에서 실행"""
        try:
            while True:
                error_info = await self.error_queue.get()
                try:
                    # 중복 체크
                    if not self._check_duplicate(error_info):
                        # 오류 처리
                        await self._process_error(error_info)
                    
                    # 통계 업데이트
                    self._update_stats(error_info)
                    
                except Exception as e:
                    logger.error(f"{LOG_SYSTEM} 오류 처리 중 내부 예외: {str(e)}")
                finally:
                    self.error_queue.task_done()
                    
        except asyncio.CancelledError:
            logger.debug(f"{LOG_SYSTEM} 오류 처리 태스크 취소됨")
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 오류 처리 태스크 예외: {str(e)}")
    
    def _classify_error(self, message: str) -> Tuple[str, str]:
        """
        오류 메시지 분석하여 카테고리와 심각도 결정
        
        Args:
            message: 오류 메시지
            
        Returns:
            Tuple[str, str]: (카테고리, 심각도)
        """
        # 패턴 매칭
        for pattern, (category, severity) in self.error_patterns.items():
            if re.search(pattern, message, re.IGNORECASE):
                return category, severity
                
        # 기본값 반환
        return ErrorCategory.UNKNOWN, ErrorSeverity.WARNING
    
    def _is_blocked_pattern(self, message: str) -> bool:
        """
        차단된 패턴인지 확인
        
        Args:
            message: 오류 메시지
            
        Returns:
            bool: 차단 여부
        """
        for pattern in self.blocked_patterns:
            if re.search(pattern, message, re.IGNORECASE):
                return True
        return False
    
    def _check_duplicate(self, error_info: Dict) -> bool:
        """
        중복 오류 확인
        
        Args:
            error_info: 오류 정보
            
        Returns:
            bool: 중복 여부 (True: 중복, False: 새 오류)
        """
        # 오류 키 생성 (중복 확인용)
        key_parts = [
            error_info["category"],
            error_info["source"]
        ]
        
        if error_info.get("exchange_code"):
            key_parts.append(error_info["exchange_code"])
            
        # 메시지의 첫 50자만 포함 (너무 긴 키 방지)
        msg_part = error_info["message"][:50].strip()
        key_parts.append(msg_part)
        
        error_key = ":".join(key_parts)
        
        # 현재 시간
        now = time.time()
        
        # 이전 발생 여부 확인
        if error_key in self.recent_errors:
            last_time, count = self.recent_errors[error_key]
            cooldown = error_info["cooldown"]
            
            # 쿨다운 시간 내에 재발생
            if now - last_time < cooldown:
                # 카운트만 증가시키고 중복으로 처리
                self.recent_errors[error_key] = (last_time, count + 1)
                return True
        
        # 새 오류 또는 쿨다운 시간 지남
        self.recent_errors[error_key] = (now, 1)
        return False
    
    async def _process_error(self, error_info: Dict) -> None:
        """
        오류 처리 수행
        
        Args:
            error_info: 오류 정보
        """
        # 카테고리 기반 커스텀 핸들러가 있으면 호출
        category = error_info["category"]
        if category in self.category_handlers:
            try:
                await self.category_handlers[category](error_info)
            except Exception as e:
                logger.error(f"{LOG_SYSTEM} 카테고리 핸들러 오류: {str(e)}")
        
        # 알림 처리 (notify가 True인 경우만)
        if error_info.get("notify", True):
            await self._send_notifications(error_info)
    
    async def _send_notifications(self, error_info: Dict) -> None:
        """
        알림 전송
        
        Args:
            error_info: 오류 정보
        """
        severity = error_info["severity"]
        channels = self.severity_channels.get(severity, [NotificationChannel.LOG])
        
        # 알림 메시지 생성
        source = error_info["source"]
        category = error_info["category"]
        message = error_info["message"]
        
        # 거래소 코드가 있으면 포함
        exchange_str = ""
        if error_info.get("exchange_code"):
            exchange_str = f"[{error_info['exchange_code']}] "
        
        # 기본 알림 메시지 형식
        notification_message = f"[{category}] {exchange_str}{source}: {message}"
        
        # 채널별 전송
        for channel in channels:
            if channel == NotificationChannel.LOG:
                self._log_error(error_info)
                
            elif channel == NotificationChannel.TELEGRAM:
                await self._send_telegram(error_info, notification_message)
                
            elif channel == NotificationChannel.EVENT:
                await self._publish_error_event(error_info)
    
    def _log_error(self, error_info: Dict) -> None:
        """
        로그에 오류 기록
        
        Args:
            error_info: 오류 정보
        """
        severity = error_info["severity"]
        exchange_str = ""
        if error_info.get("exchange_code"):
            exchange_str = f"[{error_info['exchange_code']}] "
            
        log_message = f"{LOG_SYSTEM} {exchange_str}{error_info['message']}"
        
        # 심각도에 따른 로그 레벨 선택
        if severity == ErrorSeverity.CRITICAL:
            logger.critical(log_message)
        elif severity == ErrorSeverity.ERROR:
            logger.error(log_message)
        elif severity == ErrorSeverity.WARNING:
            logger.warning(log_message)
        else:
            logger.info(log_message)
    
    async def _send_telegram(self, error_info: Dict, message: str) -> None:
        """
        텔레그램으로 오류 메시지 전송 (NotificationManager 활용)
        
        Args:
            error_info: 오류 정보
            message: 전송할 메시지
        """
        try:
            # 알림 관리자 가져오기
            notification_manager = get_notification_manager()
            
            # 심각도에 따른 알림 타입 결정
            severity = error_info["severity"]
            notification_type = "INFO"
            
            if severity == ErrorSeverity.CRITICAL:
                notification_type = "CRITICAL"
            elif severity == ErrorSeverity.ERROR:
                notification_type = "ERROR"
            elif severity == ErrorSeverity.WARNING:
                notification_type = "WARNING"
            
            # 알림 키 생성 (중복 방지용)
            key = f"error:{error_info['category']}:{hash(message)}"
            if error_info.get("exchange_code"):
                key = f"error:{error_info['exchange_code']}:{error_info['category']}:{hash(message)}"
            
            # 메타데이터 설정
            metadata = {
                "error_category": error_info["category"],
                "error_severity": severity
            }
            
            # 거래소 코드가 있으면 메타데이터에 추가
            if error_info.get("exchange_code"):
                metadata["exchange_code"] = error_info["exchange_code"]
            
            # 소스 정보 설정
            source = error_info.get("source", "error_manager")
            
            # 알림 관리자를 통해 알림 전송
            await notification_manager.send_notification(
                message=message,
                notification_type=notification_type,
                source=source,
                key=key,
                cooldown_override=error_info.get("cooldown"),
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 텔레그램 메시지 전송 실패: {str(e)}")
    
    async def _publish_error_event(self, error_info: Dict) -> None:
        """
        이벤트 버스에 오류 이벤트 발행
        
        Args:
            error_info: 오류 정보
        """
        try:
            # 이벤트 데이터 준비
            event_data = {
                "error_type": error_info["category"],
                "message": error_info["message"],
                "severity": error_info["severity"],
                "source": error_info["source"],
                "timestamp": error_info["timestamp"]
            }
            
            # 거래소 코드가 있으면 추가
            if error_info.get("exchange_code"):
                event_data["exchange_code"] = error_info["exchange_code"]
                
            # 태그가 있으면 추가
            if error_info.get("tags"):
                event_data["tags"] = error_info["tags"]
                
            # 메타데이터가 있으면 추가
            if error_info.get("metadata"):
                for key, value in error_info["metadata"].items():
                    if key not in event_data:
                        event_data[key] = value
            
            # 이벤트 발행
            await self.event_bus.publish(OrderbookEventTypes.ERROR_EVENT, event_data)
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 오류 이벤트 발행 실패: {str(e)}")
    
    def _update_stats(self, error_info: Dict) -> None:
        """
        오류 통계 업데이트
        
        Args:
            error_info: 오류 정보
        """
        try:
            # 전체 카운트 증가
            self.error_stats["total_count"] += 1
            
            # 카테고리별 통계
            category = error_info["category"]
            self.error_stats["by_category"][category] = self.error_stats["by_category"].get(category, 0) + 1
            
            # 심각도별 통계
            severity = error_info["severity"]
            self.error_stats["by_severity"][severity] = self.error_stats["by_severity"].get(severity, 0) + 1
            
            # 소스별 통계
            source = error_info["source"]
            self.error_stats["by_source"][source] = self.error_stats["by_source"].get(source, 0) + 1
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 오류 통계 업데이트 실패: {str(e)}")
    
    def register_category_handler(self, category: str, handler: Callable[[Dict], Any]) -> None:
        """
        특정 카테고리에 대한 처리 핸들러 등록
        
        Args:
            category: 오류 카테고리
            handler: 처리 함수
        """
        self.category_handlers[category] = handler
        logger.debug(f"{LOG_SYSTEM} '{category}' 카테고리 핸들러 등록됨")
    
    def register_error_pattern(self, pattern: str, category: str, severity: str) -> None:
        """
        새로운 오류 패턴 등록
        
        Args:
            pattern: 정규식 패턴
            category: 매핑할 카테고리
            severity: 매핑할 심각도
        """
        self.error_patterns[pattern] = (category, severity)
        logger.debug(f"{LOG_SYSTEM} 오류 패턴 등록: '{pattern}' -> {category}({severity})")
    
    def block_pattern(self, pattern: str) -> None:
        """
        특정 패턴 차단 (완전히 무시)
        
        Args:
            pattern: 차단할 정규식 패턴
        """
        self.blocked_patterns.add(pattern)
        logger.debug(f"{LOG_SYSTEM} 오류 패턴 차단: '{pattern}'")
    
    def unblock_pattern(self, pattern: str) -> None:
        """
        특정 패턴 차단 해제
        
        Args:
            pattern: 차단 해제할 정규식 패턴
        """
        if pattern in self.blocked_patterns:
            self.blocked_patterns.remove(pattern)
            logger.debug(f"{LOG_SYSTEM} 오류 패턴 차단 해제: '{pattern}'")
    
    def set_cooldown(self, severity: str, seconds: int) -> None:
        """
        심각도별 쿨다운 시간 설정
        
        Args:
            severity: 심각도
            seconds: 쿨다운 시간 (초)
        """
        self.cooldown_times[severity] = seconds
        logger.debug(f"{LOG_SYSTEM} '{severity}' 심각도 쿨다운 설정: {seconds}초")
    
    def set_notification_channels(self, severity: str, channels: List[str]) -> None:
        """
        심각도별 알림 채널 설정
        
        Args:
            severity: 심각도
            channels: 알림 채널 목록
        """
        self.severity_channels[severity] = channels
        logger.debug(f"{LOG_SYSTEM} '{severity}' 심각도 알림 채널 설정: {channels}")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        오류 통계 조회
        
        Returns:
            Dict: 오류 통계 정보
        """
        return self.error_stats.copy()
    
    def get_recent_errors(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        최근 오류 목록 조회
        
        Args:
            limit: 최대 개수
            
        Returns:
            List[Dict]: 최근 오류 목록
        """
        # 시간 기준 정렬
        sorted_errors = sorted(
            [(key, time_stamp, count) for key, (time_stamp, count) in self.recent_errors.items()],
            key=lambda x: x[1],
            reverse=True
        )
        
        # 결과 변환
        result = []
        for key, timestamp, count in sorted_errors[:limit]:
            parts = key.split(":")
            if len(parts) >= 4:  # 카테고리:소스:거래소:메시지
                category, source, exchange_or_msg = parts[0], parts[1], parts[2]
                message = ":".join(parts[3:])
                
                # 거래소 코드가 있는지 확인
                if len(parts) > 4:
                    exchange = exchange_or_msg
                else:
                    exchange = None
                    message = exchange_or_msg + ":" + message
                
                result.append({
                    "category": category,
                    "source": source,
                    "exchange_code": exchange,
                    "message": message,
                    "timestamp": timestamp,
                    "count": count
                })
            else:  # 잘못된 형식
                result.append({
                    "key": key,
                    "timestamp": timestamp,
                    "count": count
                })
                
        return result
    
    def clear_stats(self) -> None:
        """통계 초기화"""
        self.error_stats = {
            "total_count": 0,
            "by_category": {},
            "by_severity": {},
            "by_source": {}
        }
        logger.debug(f"{LOG_SYSTEM} 오류 통계 초기화됨")
    
    def clear_recent_errors(self) -> None:
        """최근 오류 목록 초기화"""
        self.recent_errors.clear()
        logger.debug(f"{LOG_SYSTEM} 최근 오류 목록 초기화됨")

# 편의를 위한 글로벌 함수
def get_error_manager() -> ErrorManager:
    """ErrorManager 싱글톤 인스턴스 반환"""
    return ErrorManager.get_instance()
