"""
이벤트 핸들러 모듈

이 모듈은 거래소 이벤트 처리를 위한 핸들러 클래스를 제공합니다.
기존에 각 거래소 커넥터 및 이벤트 관련 파일에 흩어져 있던 이벤트 처리 기능을 중앙화합니다.
"""

import asyncio
import time
from typing import Dict, Optional, Any
from crosskimp.logger.logger import get_unified_logger
from crosskimp.telegrambot.telegram_notification import send_telegram_message
from crosskimp.config.constants_v3 import EXCHANGE_NAMES_KR
from crosskimp.ob_collector.orderbook.util.event_bus import EventBus, EVENT_TYPES
from crosskimp.ob_collector.orderbook.util.event_manager import SystemEventManager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class EventHandler:
    """
    거래소 이벤트 처리 클래스
    
    거래소 연결, 상태 변경, 오류 등의 이벤트를 처리하고 필요한 액션을 수행합니다.
    이 클래스는 주로 이벤트의 외부 처리(로깅, 알림 등)에 집중하며,
    메트릭 저장은 SystemEventManager에 위임합니다.
    """
    
    def __init__(self, exchange_code: str, settings: Dict[str, Any]):
        """
        초기화
        
        Args:
            exchange_code: 거래소 코드 (예: 'upbit', 'bithumb', 'bybit' 등)
            settings: 설정 정보
        """
        self.exchange_code = exchange_code.lower()  # 소문자로 정규화
        self.settings = settings
        
        # 한글 거래소명 가져오기
        self.exchange_name_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        
        # 이벤트 버스 가져오기
        self.event_bus = EventBus.get_instance()
        
        # 시스템 이벤트 관리자 가져오기
        self.system_event_manager = SystemEventManager.get_instance()
        
        # 거래소 초기화
        self.system_event_manager.initialize_exchange(self.exchange_code)
        
        # 현재 거래소 코드 설정
        self.system_event_manager.set_current_exchange(self.exchange_code)
        
        # 알림 제한 관련 변수
        self._last_notification_time = {}  # 이벤트 타입별 마지막 알림 시간
        self._notification_cooldown = 60  # 알림 쿨다운 (초)
    
    # 연결 상태 이벤트 처리
    async def handle_connection_status(self, status: str, message: Optional[str] = None, **kwargs) -> None:
        """
        연결 상태 변경 이벤트 처리
        
        Args:
            status: 연결 상태 ('connected', 'disconnected', 'reconnecting' 등)
            message: 상태 메시지 (없으면 기본값 사용)
            **kwargs: 추가 데이터
        """
        # 기본 메시지 설정
        if not message:
            if status == "connected":
                message = "웹소켓 연결됨"
            elif status == "disconnected":
                message = "웹소켓 연결 끊김"
            elif status == "reconnecting":
                message = "웹소켓 재연결 중"
            else:
                message = f"웹소켓 상태 변경: {status}"
        
        # 로깅
        log_method = self.log_info
        if status == "disconnected" or status == "reconnecting":
            log_method = self.log_warning
        log_method(message)
        
        # 이벤트 데이터 준비
        event_data = {
            "status": status, 
            "message": message
        }
        
        # kwargs에서 timestamp 처리 (중복 방지)
        if "timestamp" not in kwargs:
            event_data["timestamp"] = time.time()
        else:
            event_data["timestamp"] = kwargs.pop("timestamp")
            
        # 나머지 kwargs 추가
        for key, value in kwargs.items():
            event_data[key] = value
        
        # 시스템 이벤트 발행
        await self.publish_system_event(
            EVENT_TYPES["CONNECTION_STATUS"],
            **event_data
        )
        
        # 텔레그램 알림 전송
        event_type = "connect" if status == "connected" else "disconnect" if status == "disconnected" else "reconnect"
        
        # 이모지 선택
        emoji = "🟢" if event_type == "connect" else "🔴" if event_type == "disconnect" else "🟠"
        
        # 거래소 이름에서 대괄호 제거
        exchange_name = self.exchange_name_kr.replace('[', '').replace(']', '')
        
        # 메시지 직접 포맷팅
        formatted_message = f"{emoji} {exchange_name} 웹소켓: {message}"
        
        # 메시지 직접 전송
        await self.send_telegram_message(event_type, formatted_message)
        
        # 이벤트 채널 발행 (UI 및 외부 시스템용)
        event_data = {
            "exchange_code": self.exchange_code,
            "status": status,
            "message": message,
            "timestamp": time.time()
        }
        await self.event_bus.publish("connection_status_changed", **event_data)
    
    # 오류 이벤트 처리
    async def handle_error(self, error_type: str, message: str, severity: str = "error", **kwargs) -> None:
        """
        오류 이벤트 처리
        
        Args:
            error_type: 오류 유형 (예: 'connection_error', 'timeout', 'auth_error' 등)
            message: 오류 메시지
            severity: 심각도 ('error', 'warning', 'critical' 등)
            **kwargs: 추가 데이터
        """
        # 로깅
        if severity == "critical":
            self.log_error(f"{error_type}: {message}")
        elif severity == "warning":
            self.log_warning(f"{error_type}: {message}")
        else:
            self.log_error(f"{error_type}: {message}")
        
        # 타임스탬프 설정
        timestamp = kwargs.get("timestamp", time.time())
        
        # 시스템 이벤트 발행
        await self.publish_system_event(
            EVENT_TYPES["ERROR_EVENT"],
            error_type=error_type,
            message=message,
            severity=severity,
            timestamp=timestamp,
            **kwargs
        )
        
        # 오류 메트릭 기록
        self.system_event_manager.record_metric(self.exchange_code, "error_count")
        
        # 텔레그램 알림 전송 (심각한 오류만)
        if severity in ["error", "critical"]:
            # 거래소 이름 포함 오류 메시지 생성
            exchange_name = self.exchange_name_kr.replace('[', '').replace(']', '')
            error_message = f"🚨 {exchange_name} 오류: {error_type} - {message}"
            await self.send_telegram_message("error", error_message)
    
    # 메트릭 업데이트 이벤트 처리
    async def handle_metric_update(self, metric_name: str, value: Any, **kwargs) -> None:
        """
        메트릭 업데이트 이벤트 처리
        
        Args:
            metric_name: 메트릭 이름
            value: 메트릭 값
            **kwargs: 추가 데이터
        """
        # 메트릭 직접 기록
        self.system_event_manager.record_metric(
            self.exchange_code, 
            metric_name, 
            **kwargs
        )
        
        # 시스템 이벤트 발행
        await self.publish_system_event(
            EVENT_TYPES["METRIC_UPDATE"],
            metric_name=metric_name,
            value=value,
            timestamp=kwargs.get("timestamp", time.time()),
            **kwargs
        )
    
    # 구독 상태 이벤트 처리
    async def handle_subscription_status(self, status: str, symbols: list, **kwargs) -> None:
        """
        구독 상태 이벤트 처리
        
        Args:
            status: 구독 상태 ('subscribed', 'unsubscribed', 'error' 등)
            symbols: 구독 중인 심볼 목록
            **kwargs: 추가 데이터
        """
        # 로깅
        self.log_info(f"구독 상태 변경: {status}, 심볼: {len(symbols)}개")
        
        # 시스템 이벤트 발행
        await self.publish_system_event(
            EVENT_TYPES["SUBSCRIPTION_STATUS"],
            status=status,
            symbols=symbols,
            timestamp=kwargs.get("timestamp", time.time()),
            **kwargs
        )
    
    # 메시지 수신 이벤트 처리
    async def handle_message_received(self, message_type: str, size: int = 0, **kwargs) -> None:
        """
        메시지 수신 이벤트 처리
        
        Args:
            message_type: 메시지 유형 ('snapshot', 'delta', 'heartbeat' 등)
            size: 메시지 크기 (바이트)
            **kwargs: 추가 데이터
        """
        # 메시지 카운트 증가
        self.system_event_manager.increment_message_count(self.exchange_code)
        
        # 추가 메트릭 기록
        if size > 0:
            self.system_event_manager.record_metric(
                self.exchange_code, 
                "data_size", 
                byte_size=size
            )
        
        # 특정 타입의 메시지 카운트 기록
        if message_type in ["snapshot", "delta"]:
            self.system_event_manager.record_metric(
                self.exchange_code,
                f"{message_type}_count"
            )
    
    # 시스템 이벤트 발행
    async def publish_system_event(self, event_type: str, **data) -> None:
        """
        시스템 이벤트 발행
        
        Args:
            event_type: 이벤트 타입 (EVENT_TYPES 사용)
            **data: 이벤트 데이터
        """
        try:
            # data에 이미 timestamp가 있는 경우, 이를 그대로 사용
            if 'timestamp' not in data:
                data['timestamp'] = time.time()
                
            # exchange_code가 없으면 추가
            if 'exchange_code' not in data:
                data['exchange_code'] = self.exchange_code
                
            await self.system_event_manager.publish_system_event(
                event_type,
                **data
            )
        except Exception as e:
            self.log_error(f"이벤트 발행 실패: {str(e)}")
    
    # 텔레그램 알림 전송 (단순화된 버전)
    async def send_telegram_message(self, event_type: str, message: str) -> None:
        """
        텔레그램 알림 전송
        
        Args:
            event_type: 이벤트 타입 ('error', 'connect', 'disconnect', 'reconnect')
            message: 알림 메시지 (이미 포맷팅된 상태)
        """
        # 지원되는 이벤트 타입 확인
        if event_type not in ["error", "connect", "disconnect", "reconnect"]:
            return
            
        current_time = time.time()
        
        # 쿨다운 체크 - 동일 이벤트 타입에 대해 일정 시간 내 중복 알림 방지
        last_time = self._last_notification_time.get(event_type, 0)
        if current_time - last_time < self._notification_cooldown:
            return
            
        # 현재 시간 기록
        self._last_notification_time[event_type] = current_time
        
        try:
            # 현재 시간 추가
            from datetime import datetime
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            message_with_time = f"[{current_time}]\n{message}"
            
            # 단순화된 메시지 전송
            await send_telegram_message(message_with_time)
        except Exception as e:
            self.log_error(f"텔레그램 알림 전송 실패: {str(e)}")
    
    # 로깅 함수들
    def log_error(self, message: str) -> None:
        """오류 로깅 (거래소 이름 포함)"""
        logger.error(f"{self.exchange_name_kr} {message}")
    
    def log_warning(self, message: str) -> None:
        """경고 로깅 (거래소 이름 포함)"""
        logger.warning(f"{self.exchange_name_kr} {message}")
    
    def log_info(self, message: str) -> None:
        """정보 로깅 (거래소 이름 포함)"""
        logger.info(f"{self.exchange_name_kr} {message}")
    
    def log_debug(self, message: str) -> None:
        """
        디버그 로그 출력
        
        Args:
            message: 로그 메시지
        """
        # 디버그 메시지를 INFO 레벨로 출력하여 항상 보이도록 함
        logger.info(f"{self.exchange_name_kr} {message}")

# 싱글톤 패턴 구현
class EventHandlerFactory:
    """
    이벤트 핸들러 팩토리 클래스
    
    각 거래소마다 하나의 이벤트 핸들러 인스턴스를 유지합니다.
    """
    
    _instances = {}  # 거래소 코드 -> 핸들러 인스턴스
    
    @classmethod
    def get_handler(cls, exchange_code: str, settings: Dict[str, Any]) -> EventHandler:
        """
        거래소별 이벤트 핸들러 인스턴스 반환
        
        Args:
            exchange_code: 거래소 코드
            settings: 설정 정보
            
        Returns:
            EventHandler: 이벤트 핸들러 인스턴스
        """
        exchange_code = exchange_code.lower()  # 정규화
        
        if exchange_code not in cls._instances:
            cls._instances[exchange_code] = EventHandler(exchange_code, settings)
            
        return cls._instances[exchange_code] 