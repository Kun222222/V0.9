"""
시스템 알림 관리 모듈

이 모듈은 시스템의 모든 알림을 중앙에서 관리하는 싱글톤 클래스를 제공합니다.
다양한 모듈과 서비스에서 발생하는 알림을 일관된 방식으로 텔레그램에 전송합니다.
"""

import asyncio
import time
import threading
from datetime import datetime
from typing import Dict, Any, Optional, Union, List, Callable
import logging

from crosskimp.logger.logger import get_unified_logger
from crosskimp.telegrambot import get_bot_manager
from crosskimp.common.events import get_component_event_bus, Component, EventTypes

# NotificationType 정의 (문자열 리터럴 사용)
class NotificationType:
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    SYSTEM = "SYSTEM"
    METRIC = "METRIC"

# NotificationType 재내보내기
__all__ = ['NotificationType', 'get_notification_manager', 'initialize_notification_manager']

# 로거 설정
logger = get_unified_logger()

# 프로그램별 알림 설정
PROGRAM_NOTIFICATIONS = {
    "orderbook": {
        "display_name": "오더북 수집기",
        "startup_emoji": "🚀",
        "shutdown_emoji": "🔴",
        "error_emoji": "⚠️"
    },
    "radar": {
        "display_name": "레이더 서비스",
        "startup_emoji": "📡",
        "shutdown_emoji": "🔴",
        "error_emoji": "⚠️"
    },
    # 여기에 다른 프로그램 알림 설정을 쉽게 추가할 수 있습니다
}

class NotificationManager:
    """
    시스템 알림 관리 클래스
    
    애플리케이션 전체의 알림을 중앙에서 관리하고 처리하는 싱글톤 클래스입니다.
    
    특징:
    1. 모든 모듈의 알림을 일관된 방식으로 처리
    2. 알림 소스 및 유형 관리
    3. 중복 알림 필터링
    4. telegrambot 모듈과 통합
    5. 이벤트 버스 구독을 통한 자동화된 알림
    """
    
    _instance = None
    _lock = threading.RLock()
    
    @classmethod
    def get_instance(cls) -> 'NotificationManager':
        """싱글톤 인스턴스 반환"""
        with cls._lock:
            if cls._instance is None:
                cls._instance = NotificationManager()
            return cls._instance
    
    def __init__(self):
        """초기화 - 싱글톤 강제"""
        if NotificationManager._instance is not None:
            raise RuntimeError("싱글톤 클래스입니다. get_instance()를 사용하세요.")
        
        # 알림 쿨다운 설정 (중복 알림 방지를 위한 추가 레이어)
        self.last_notification_time = {}
        self.notification_cooldown = {
            "default": 60,  # 기본 쿨다운: 1분
            "error": 300,   # 오류: 5분
            "warning": 180, # 경고: 3분
            "system": 300,  # 시스템: 5분
        }
        
        # 알림 통계
        self.stats = {
            "total_attempts": 0,
            "total_sent": 0,
            "total_filtered": 0,
            "by_source": {},
            "by_type": {},
        }
        
        # 이벤트 버스 및 구독 정보
        self.event_bus = None
        self.event_subscriptions = []
        
        # 초기화 상태
        self.is_initialized = False
        
        logger.info("알림 관리자가 초기화되었습니다.")
    
    async def initialize(self):
        """알림 관리자 초기화 및 관련 서비스 시작"""
        if self.is_initialized:
            return
            
        # 텔레그램 봇 매니저 가져오기
        bot_manager = get_bot_manager()
        # 봇 시작은 main.py에서 처리
        
        # 이벤트 버스 초기화 및 이벤트 구독 설정
        try:
            self.event_bus = get_component_event_bus(Component.SYSTEM)
            # 시스템 이벤트 구독
            await self._subscribe_to_system_events()
            logger.info("이벤트 버스 구독 완료")
        except Exception as e:
            logger.error(f"이벤트 버스 구독 중 오류: {str(e)}")
            
        self.is_initialized = True
        logger.info("알림 관리자가 완전히 초기화되었습니다.")
    
    async def _subscribe_to_system_events(self):
        """시스템 이벤트를 구독하고 알림을 처리하는 핸들러 등록"""
        if not self.event_bus:
            logger.warning("이벤트 버스가 초기화되지 않았습니다.")
            return
            
        # 시스템 이벤트 구독
        subscription_id = await self.event_bus.subscribe(
            EventTypes.SYSTEM_EVENT,
            self._handle_system_event
        )
        self.event_subscriptions.append(subscription_id)
        
        # 프로세스 상태 이벤트 구독
        subscription_id = await self.event_bus.subscribe(
            EventTypes.PROCESS_STATUS,
            self._handle_process_status
        )
        self.event_subscriptions.append(subscription_id)
        
        # 오류 이벤트 구독
        subscription_id = await self.event_bus.subscribe(
            EventTypes.ERROR_EVENT,
            self._handle_error_event
        )
        self.event_subscriptions.append(subscription_id)
        
        logger.info(f"시스템 이벤트 구독 완료: {len(self.event_subscriptions)}개의 구독 설정됨")
    
    async def _handle_system_event(self, event_data: Dict[str, Any]):
        """
        시스템 이벤트 핸들러
        
        Args:
            event_data: 이벤트 데이터
        """
        try:
            event_type = event_data.get("type", "")
            
            # 프로그램 시작 이벤트 처리
            if event_type == "orderbook_startup":
                await self._send_program_startup_notification("orderbook", event_data)
            # 프로그램 종료 이벤트 처리
            elif event_type == "orderbook_shutdown":
                await self._send_program_shutdown_notification("orderbook", event_data)
            # 기타 프로그램 시작/종료 이벤트 처리 (프로그램 ID로 매핑)
            elif event_type.endswith("_startup") and event_type.split("_")[0] in PROGRAM_NOTIFICATIONS:
                program_id = event_type.split("_")[0]
                await self._send_program_startup_notification(program_id, event_data)
            elif event_type.endswith("_shutdown") and event_type.split("_")[0] in PROGRAM_NOTIFICATIONS:
                program_id = event_type.split("_")[0]
                await self._send_program_shutdown_notification(program_id, event_data)
                
        except Exception as e:
            logger.error(f"시스템 이벤트 처리 중 오류: {str(e)}")
    
    async def _handle_process_status(self, event_data: Dict[str, Any]):
        """
        프로세스 상태 이벤트 핸들러
        
        Args:
            event_data: 이벤트 데이터
        """
        try:
            process_name = event_data.get("process_name", "")
            status = event_data.get("status", "")
            description = event_data.get("description", process_name)
            
            # 프로세스 시작 상태 변경 알림
            if status == "running" and process_name in ["ob_collector", "radar"]:
                program_id = "orderbook" if process_name == "ob_collector" else process_name
                if program_id in PROGRAM_NOTIFICATIONS:
                    await self._send_process_start_notification(program_id, description, event_data)
            
            # 프로세스 오류 상태 알림
            elif status == "error" and process_name in ["ob_collector", "radar"]:
                program_id = "orderbook" if process_name == "ob_collector" else process_name
                if program_id in PROGRAM_NOTIFICATIONS:
                    error_msg = event_data.get("error", "알 수 없는 오류")
                    await self._send_process_error_notification(program_id, description, error_msg, event_data)
                    
        except Exception as e:
            logger.error(f"프로세스 상태 이벤트 처리 중 오류: {str(e)}")
    
    async def _handle_error_event(self, event_data: Dict[str, Any]):
        """
        오류 이벤트 핸들러
        
        Args:
            event_data: 이벤트 데이터
        """
        try:
            message = event_data.get("message", "알 수 없는 오류")
            source = event_data.get("source", "unknown")
            severity = event_data.get("severity", "ERROR")
            category = event_data.get("category", "SYSTEM")
            
            # 중요 오류만 알림
            if severity in ["ERROR", "CRITICAL"] and category in ["PROCESS", "SYSTEM"]:
                notification_type = NotificationType.ERROR if severity == "ERROR" else NotificationType.CRITICAL
                
                await self.send_notification(
                    message=f"⚠️ <b>시스템 오류 발생</b>\n\n<b>출처:</b> {source}\n<b>심각도:</b> {severity}\n<b>메시지:</b> {message}",
                    notification_type=notification_type,
                    source=source,
                    key=f"error:{source}:{hash(message)}"
                )
                
        except Exception as e:
            logger.error(f"오류 이벤트 처리 중 오류: {str(e)}")
    
    async def _send_program_startup_notification(self, program_id: str, event_data: Dict[str, Any]):
        """
        프로그램 시작 알림 전송
        
        Args:
            program_id: 프로그램 ID
            event_data: 이벤트 데이터
        """
        if program_id not in PROGRAM_NOTIFICATIONS:
            return
            
        program_info = PROGRAM_NOTIFICATIONS[program_id]
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        environment = event_data.get("environment", "개발")
        if environment == "production":
            environment = "프로덕션"
            
        message = f"""{program_info['startup_emoji']} <b>{program_info['display_name']} 시작</b>

<b>시간:</b> {current_time}
<b>환경:</b> {environment}
<b>상태:</b> 시스템 초기화 중...

⏳ {program_info['display_name']}가 시작되었습니다.
"""
        
        await self.send_notification(
            message=message,
            notification_type=NotificationType.SYSTEM,
            source=program_id,
            key=f"{program_id}_startup"
        )
    
    async def _send_program_shutdown_notification(self, program_id: str, event_data: Dict[str, Any]):
        """
        프로그램 종료 알림 전송
        
        Args:
            program_id: 프로그램 ID
            event_data: 이벤트 데이터
        """
        if program_id not in PROGRAM_NOTIFICATIONS:
            return
            
        program_info = PROGRAM_NOTIFICATIONS[program_id]
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        message = f"""{program_info['shutdown_emoji']} <b>{program_info['display_name']} 종료</b>

<b>시간:</b> {current_time}
<b>상태:</b> 안전하게 종료되었습니다

📊 {program_info['display_name']}가 중단되었습니다.
"""
        
        await self.send_notification(
            message=message,
            notification_type=NotificationType.SYSTEM,
            source=program_id,
            key=f"{program_id}_shutdown"
        )
    
    async def _send_process_start_notification(self, program_id: str, description: str, event_data: Dict[str, Any]):
        """
        프로세스 시작 알림 전송
        
        Args:
            program_id: 프로그램 ID
            description: 프로세스 설명
            event_data: 이벤트 데이터
        """
        if program_id not in PROGRAM_NOTIFICATIONS:
            return
            
        program_info = PROGRAM_NOTIFICATIONS[program_id]
        pid = event_data.get("pid", "알 수 없음")
            
        message = f"""✅ <b>{program_info['display_name']} 프로세스 시작됨</b>

<b>프로세스:</b> {description}
<b>PID:</b> {pid}
<b>시간:</b> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

{program_info['display_name']}가 정상적으로 시작되었습니다.
"""
        
        # 이 알림은 매번 보낼 필요가 없으므로 쿨다운 설정
        await self.send_notification(
            message=message,
            notification_type=NotificationType.SYSTEM,
            source=program_id,
            key=f"{program_id}_process_start",
            cooldown_override=3600  # 1시간에 한 번만
        )
    
    async def _send_process_error_notification(self, program_id: str, description: str, error_msg: str, event_data: Dict[str, Any]):
        """
        프로세스 오류 알림 전송
        
        Args:
            program_id: 프로그램 ID
            description: 프로세스 설명
            error_msg: 오류 메시지
            event_data: 이벤트 데이터
        """
        if program_id not in PROGRAM_NOTIFICATIONS:
            return
            
        program_info = PROGRAM_NOTIFICATIONS[program_id]
            
        message = f"""{program_info['error_emoji']} <b>{program_info['display_name']} 프로세스 오류</b>

<b>프로세스:</b> {description}
<b>시간:</b> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
<b>오류:</b> {error_msg}

프로세스 시작 중 문제가 발생했습니다. 로그를 확인해주세요.
"""
        
        await self.send_notification(
            message=message,
            notification_type=NotificationType.ERROR,
            source=program_id,
            key=f"{program_id}_process_error:{hash(error_msg)}"
        )
    
    async def send_notification(
        self,
        message: str,
        notification_type: Union[str, NotificationType] = NotificationType.INFO,
        source: Optional[str] = None,
        key: Optional[str] = None,
        cooldown_override: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        알림 전송 메서드
        
        Args:
            message: 알림 메시지
            notification_type: 알림 유형
            source: 알림 발생지 (모듈/서비스명)
            key: 중복 방지용 키 (None이면 자동 생성)
            cooldown_override: 쿨다운 시간 재정의 (초)
            metadata: 추가 메타데이터
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            # 초기화 확인
            if not self.is_initialized:
                await self.initialize()
                
            self.stats["total_attempts"] += 1
            
            # 알림 키 생성
            if not key:
                if source:
                    key = f"{source}:{notification_type}:{hash(message)}"
                else:
                    key = f"{notification_type}:{hash(message)}"
            
            # 추가적인 중복 알림 필터링 (NotificationService에도 자체 필터링이 있음)
            if not self._check_cooldown(key, notification_type, cooldown_override):
                self.stats["total_filtered"] += 1
                return False
            
            # 통계 업데이트
            source_str = source or "unknown"
            type_str = str(notification_type)
            
            if source_str not in self.stats["by_source"]:
                self.stats["by_source"][source_str] = 0
            self.stats["by_source"][source_str] += 1
            
            if type_str not in self.stats["by_type"]:
                self.stats["by_type"][type_str] = 0
            self.stats["by_type"][type_str] += 1
            
            # 텔레그램 알림 전송
            bot_manager = get_bot_manager()
            notification_service = bot_manager.notification_service
            admin_only = True  # 기본적으로 관리자에게만 전송
            
            # 알림 메타데이터 설정
            if not metadata:
                metadata = {}
            
            # 알림 서비스에 전송
            await notification_service.send_notification(
                message=message,
                admin_only=admin_only
            )
            
            self.stats["total_sent"] += 1
            return True
            
        except Exception as e:
            logger.error(f"알림 전송 중 오류: {str(e)}")
            return False
    
    def _check_cooldown(
        self, 
        key: str, 
        notification_type: Union[str, NotificationType],
        cooldown_override: Optional[int] = None
    ) -> bool:
        """
        쿨다운 확인 (중복 알림 방지)
        
        Args:
            key: 알림 키
            notification_type: 알림 유형
            cooldown_override: 쿨다운 시간 재정의 (초)
            
        Returns:
            bool: 쿨다운 중이 아니면 True, 쿨다운 중이면 False
        """
        current_time = time.time()
        
        # 마지막 발송 시간 확인
        if key in self.last_notification_time:
            last_time = self.last_notification_time[key]
            
            # 쿨다운 시간 결정
            if cooldown_override is not None:
                cooldown = cooldown_override
            else:
                type_str = str(notification_type).lower()
                if type_str in self.notification_cooldown:
                    cooldown = self.notification_cooldown[type_str]
                else:
                    cooldown = self.notification_cooldown["default"]
            
            # 쿨다운 체크
            if current_time - last_time < cooldown:
                return False  # 쿨다운 중
        
        # 마지막 발송 시간 업데이트
        self.last_notification_time[key] = current_time
        return True
    
    def set_cooldown(self, alert_type: str, seconds: int) -> None:
        """
        알림 유형별 쿨다운 설정
        
        Args:
            alert_type: 알림 유형
            seconds: 쿨다운 시간 (초)
        """
        self.notification_cooldown[alert_type.lower()] = seconds
    
    def clear_cooldowns(self) -> None:
        """모든 쿨다운 초기화"""
        self.last_notification_time = {}
    
    def add_program_notification_config(self, program_id: str, config: Dict[str, str]) -> None:
        """
        프로그램 알림 설정 추가
        
        Args:
            program_id: 프로그램 ID
            config: 설정 딕셔너리 (display_name, startup_emoji, shutdown_emoji, error_emoji)
        """
        if not all(k in config for k in ["display_name", "startup_emoji", "shutdown_emoji", "error_emoji"]):
            logger.error(f"프로그램 알림 설정에 필수 키가 누락되었습니다: {program_id}")
            return
            
        PROGRAM_NOTIFICATIONS[program_id] = config
        logger.info(f"프로그램 알림 설정 추가됨: {program_id} ({config['display_name']})")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        알림 통계 반환
        
        Returns:
            Dict: 알림 통계 정보
        """
        return self.stats
    
    def reset_stats(self) -> None:
        """통계 초기화"""
        self.stats = {
            "total_attempts": 0,
            "total_sent": 0,
            "total_filtered": 0,
            "by_source": {},
            "by_type": {},
        }
    
    async def shutdown(self):
        """알림 관리자 종료"""
        # 이벤트 구독 해제
        if self.event_bus:
            for subscription_id in self.event_subscriptions:
                try:
                    await self.event_bus.unsubscribe(subscription_id)
                except Exception as e:
                    logger.error(f"이벤트 구독 해제 중 오류: {str(e)}")
        
        self.event_subscriptions = []
        self.is_initialized = False
        logger.info("알림 관리자가 종료되었습니다.")

# 싱글톤 매니저 인스턴스
_notification_manager = None

def get_notification_manager() -> NotificationManager:
    """NotificationManager 싱글톤 인스턴스 반환"""
    global _notification_manager
    if _notification_manager is None:
        _notification_manager = NotificationManager.get_instance()
    return _notification_manager

# 편의 함수: 알림 매니저 초기화
async def initialize_notification_manager():
    """알림 매니저 및 관련 서비스 초기화"""
    manager = get_notification_manager()
    await manager.initialize()
    return manager 