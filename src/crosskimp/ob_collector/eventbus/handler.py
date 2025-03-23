"""
오더북 콜렉터 이벤트 핸들러

이 모듈은 오더북 콜렉터의 이벤트 처리 및 통합 관리를 담당하는 클래스를 제공합니다.
내부 이벤트 버스를 사용하여 거래소별 이벤트를 처리합니다.
"""

import time
import asyncio
from typing import Dict, List, Any, Optional, Set, Tuple

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.constants_v3 import EXCHANGE_NAMES_KR

from crosskimp.ob_collector.eventbus.bus import EventBus
from crosskimp.ob_collector.eventbus.types import EventTypes, EventPriority


# 로거 설정
logger = get_unified_logger()

# 싱글톤 이벤트 버스 인스턴스 저장
_event_bus_instance = None

def get_orderbook_event_bus() -> EventBus:
    """
    오더북 이벤트 버스 인스턴스 반환
    
    이벤트 버스가 초기화되어 있지 않으면 새로 생성하고,
    이벤트 버스가 실행 중이 아니면 자동으로 시작합니다.
    
    Returns:
        EventBus: 오더북 이벤트 버스 인스턴스
    """
    global _event_bus_instance
    
    if _event_bus_instance is None:
        _event_bus_instance = EventBus.get_instance()
        
        # 이벤트 버스가 실행 중이 아니면 시작
        if not _event_bus_instance.is_running:
            try:
                # 현재 실행 중인 이벤트 루프가 있는지 확인
                loop = asyncio.get_running_loop()
                
                # 비동기 태스크로 이벤트 버스 시작
                start_task = loop.create_task(_event_bus_instance.start())
                logger.info("이벤트 버스 시작 태스크가 생성되었습니다.")
                
            except RuntimeError:
                # 실행 중인 이벤트 루프가 없음 - 동기 컨텍스트에서 호출됨
                logger.warning("실행 중인 이벤트 루프가 없어 이벤트 버스 자동 시작이 지연됩니다.")
                # 이벤트 버스의 자동 시작 기능으로 처리됨
    
    return _event_bus_instance

class LoggingMixin:
    """
    로깅 믹스인 클래스
    
    로깅 기능을 제공하는 믹스인 클래스입니다.
    이벤트 핸들러 및 기타 클래스에서 로깅 기능을 사용할 수 있도록 합니다.
    """
    
    def setup_logger(self, exchange_code: str):
        """
        로거 설정
        
        Args:
            exchange_code: 거래소 코드
        """
        self.exchange_code = exchange_code.lower()
        self.exchange_name_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        self._logger = logger  # 전역 로거 참조
    
    def log_error(self, message: str, exc_info=False) -> None:
        """오류 로깅 (거래소 이름 포함)"""
        if not hasattr(self, 'exchange_name_kr'):
            self._logger.error(message, exc_info=exc_info)
        else:
            self._logger.error(f"{self.exchange_name_kr} {message}", exc_info=exc_info)
    
    def log_warning(self, message: str, exc_info=False) -> None:
        """경고 로깅 (거래소 이름 포함)"""
        if not hasattr(self, 'exchange_name_kr'):
            self._logger.warning(message, exc_info=exc_info)
        else:
            self._logger.warning(f"{self.exchange_name_kr} {message}", exc_info=exc_info)
    
    def log_info(self, message: str, exc_info=False) -> None:
        """정보 로깅 (거래소 이름 포함)"""
        if not hasattr(self, 'exchange_name_kr'):
            self._logger.info(message, exc_info=exc_info)
        else:
            self._logger.info(f"{self.exchange_name_kr} {message}", exc_info=exc_info)
    
    def log_debug(self, message: str, exc_info=False) -> None:
        """디버그 로깅 (거래소 이름 포함)"""
        if not hasattr(self, 'exchange_name_kr'):
            self._logger.debug(message, exc_info=exc_info)
        else:
            self._logger.debug(f"{self.exchange_name_kr} {message}", exc_info=exc_info)

    def log_critical(self, message: str, exc_info=False) -> None:
        """심각한 오류 로깅 (거래소 이름 포함)"""
        if not hasattr(self, 'exchange_name_kr'):
            self._logger.critical(message, exc_info=exc_info)
        else:
            self._logger.critical(f"{self.exchange_name_kr} {message}", exc_info=exc_info)

class EventHandler(LoggingMixin):
    """
    거래소 이벤트 처리 및 통합 관리 클래스
    
    거래소 이벤트 처리와 이벤트 통합을 담당합니다.
    로깅, 이벤트 발행, 알림 전송 등의 역할을 수행합니다.
    """
    
    _instances = {}  # 거래소 코드 -> 핸들러 인스턴스
    
    @classmethod
    def get_instance(cls, exchange_code: str, settings: Dict[str, Any] = None) -> 'EventHandler':
        """
        거래소별 이벤트 핸들러 인스턴스 반환 (싱글톤 패턴)
        
        Args:
            exchange_code: 거래소 코드
            settings: 설정 정보
            
        Returns:
            EventHandler: 이벤트 핸들러 인스턴스
        """
        exchange_code = exchange_code.lower()  # 정규화
        
        if exchange_code not in cls._instances:
            cls._instances[exchange_code] = EventHandler(exchange_code, settings or {})
            
        return cls._instances[exchange_code]
    
    def __init__(self, exchange_code: str, settings: Dict[str, Any]):
        """
        초기화
        
        Args:
            exchange_code: 거래소 코드 (예: 'upbit', 'bithumb', 'bybit' 등)
            settings: 설정 정보
        """
        # 로거 설정 (믹스인 메서드 사용)
        self.setup_logger(exchange_code.lower())
        
        self.settings = settings
        self.exchange_code = exchange_code.lower()
        
        # 거래소 이름 (한글) 설정
        self.exchange_name_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        
        # 이벤트 버스 가져오기 - 내부 이벤트 버스 사용
        self.event_bus = get_orderbook_event_bus()
        
        # 연결 상태 저장소 (운영 로직용)
        self.connection_status = "disconnected"
        
        # 구독 상태 저장소 (운영 로직용)
        self.subscriptions = {
            "symbols": [],
            "status": "unsubscribed"
        }
        
        # 알림 제한 관련 변수
        self._last_notification_time = {}  # 이벤트 타입별 마지막 알림 시간
        self._notification_cooldown = 60  # 알림 쿨다운 (초)
    
        # 종료 상태 플래그
        self._is_shutting_down = False
        
        # 초기화 완료 로그
        self.log_info(f"{self.exchange_name_kr} 이벤트 핸들러 초기화 완료")

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
            self.log_critical(f"{error_type}: {message}")
        elif severity == "warning":
            self.log_warning(f"{error_type}: {message}")
        else:
            self.log_error(f"{error_type}: {message}")
        
        # 이벤트 발행
        event_data = {
            "exchange_code": self.exchange_code,
            "error_type": error_type,
            "message": message,
            "severity": severity,
            "timestamp": time.time(),
            **kwargs
        }
        await self.event_bus.publish(EventTypes.ERROR_EVENT, event_data, EventPriority.HIGH)

    def get_status(self) -> Dict[str, Any]:
        """
        상태 정보 조회
        
        Returns:
            Dict: 상태 정보
        """
        # 기본 정보로 보강
        exchange_status = {
            "exchange_code": self.exchange_code,
            "exchange_name": self.exchange_name_kr,
            "connection_status": self.connection_status,
            "subscriptions": self.subscriptions,
            "is_shutting_down": self._is_shutting_down
        }
        
        return exchange_status
    
    def get_exchange_status(self) -> Dict[str, Any]:
        """
        거래소 상태 정보 조회 (축약 버전)
        
        Returns:
            Dict: 거래소 상태 정보
        """
        return {
            "exchange": self.exchange_name_kr,
            "code": self.exchange_code,
            "connection": self.connection_status,
            "subscribed_symbols": len(self.subscriptions["symbols"])
        }

    async def handle_connection_status(self, status: str, timestamp: float = None, **kwargs) -> None:
        """
        연결 상태 변경 이벤트 처리
        
        Args:
            status: 연결 상태 ("connected", "disconnected", "reconnecting")
            timestamp: 이벤트 발생 시간 (None이면 현재 시간)
            **kwargs: 추가 데이터
        """
        # 중복 상태 변경 이벤트 필터링
        if status == self.connection_status:
            return
            
        # 타임스탬프 설정
        if timestamp is None:
            timestamp = time.time()
        
        # 이전 연결 상태 저장
        old_status = self.connection_status
        
        # 현재 연결 상태 업데이트 (운영 로직용 상태 저장)
        self.connection_status = status
        
        # 로그 기록
        status_msg = kwargs.get('message', '')
        status_emoji = {
            'connected': '🟢',
            'disconnected': '🔴',
            'reconnecting': '🔵',
        }.get(status, '')
        
        # 연결 상태 변경 로그
        if status_msg:
            self.log_info(f"{status_emoji} 연결 상태 변경: {old_status} → {status} ({status_msg})")
        else:
            self.log_info(f"{status_emoji} 연결 상태 변경: {old_status} → {status}")
        
        # 이벤트 발행
        event_data = {
            "exchange_code": self.exchange_code,
            "status": status,
            "old_status": old_status,
            "timestamp": timestamp,
            "message": status_msg
        }
        if status == "connected":
            await self.event_bus.publish(EventTypes.CONNECTION_SUCCESS, event_data)
        elif status == "disconnected":
            await self.event_bus.publish(EventTypes.CONNECTION_FAILURE, event_data)
        else:
            await self.event_bus.publish(EventTypes.CONNECTION_STATUS, event_data)

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
        
        # 타임스탬프 설정
        timestamp = kwargs.get("timestamp", time.time())
        
        # 상태 저장 (운영 로직용)
        self.subscriptions = {
            "status": status,
            "symbols": symbols,
            "timestamp": timestamp
        }
        
        # 이벤트 버스 발행
        event_data = {
            "exchange_code": self.exchange_code,
            "status": status,
            "symbols": symbols,
            "count": len(symbols),
            "timestamp": timestamp
        }
        
        if status == 'subscribed':
            await self.event_bus.publish(EventTypes.SUBSCRIPTION_SUCCESS, event_data)
        elif status == 'error':
            await self.event_bus.publish(EventTypes.SUBSCRIPTION_FAILURE, event_data)
        else:
            await self.event_bus.publish(EventTypes.SUBSCRIPTION_STATUS, event_data)
    
    async def handle_message_received(self, message_type: str, size: int = 0, **kwargs) -> None:
        """
        메시지 수신 이벤트 처리
        
        Args:
            message_type: 메시지 유형 ('snapshot', 'delta', 'heartbeat' 등)
            size: 메시지 크기 (바이트)
            **kwargs: 추가 데이터
        """
        # 타임스탬프 설정
        timestamp = time.time()
        
        # 이벤트 버스를 통한 메시지 이벤트 발행
        event_data = {
            "exchange_code": self.exchange_code,
            "message_type": message_type,
            "size": size,
            "timestamp": timestamp
        }
        
        # 추가 데이터 병합
        for key, value in kwargs.items():
            event_data[key] = value
            
        # 메시지 수신 이벤트 발행
        await self.event_bus.publish(EventTypes.MESSAGE_RECEIVED, event_data)
        
        # 메시지 처리 완료 이벤트 발행
        await self.event_bus.publish(EventTypes.MESSAGE_PROCESSED, event_data)

    async def handle_data_event(self, event_type: str, symbol: str, data: Any, **kwargs) -> None:
        """
        데이터 이벤트 처리
        
        Args:
            event_type: 이벤트 타입 ('snapshot', 'delta' 등)
            symbol: 심볼명
            data: 이벤트 데이터
            **kwargs: 추가 데이터
        """
        # 타임스탬프 설정
        timestamp = kwargs.get("timestamp", time.time())
        
        # 이벤트 버스 발행
        event_data = {
            "exchange_code": self.exchange_code,
            "symbol": symbol,
            "data": data,
            "timestamp": timestamp,
            "event_type": event_type  # event_type을 event_data에 포함
        }
        
        # 추가 데이터 병합
        for key, value in kwargs.items():
            if key != "timestamp":  # timestamp는 이미 처리함
                event_data[key] = value
        
        # 이벤트 타입에 따라 다른 이벤트 타입으로 발행
        mapped_event_type = None
        if event_type == 'snapshot':
            mapped_event_type = EventTypes.SNAPSHOT_RECEIVED
        elif event_type == 'delta':
            mapped_event_type = EventTypes.DELTA_RECEIVED
        elif event_type == 'orderbook_updated':
            mapped_event_type = EventTypes.ORDERBOOK_UPDATED
        elif event_type == 'ticker':
            mapped_event_type = EventTypes.TICKER_UPDATED
        else:
            # 매핑이 없는 경우 그대로 사용
            mapped_event_type = event_type
            
        await self.event_bus.publish(mapped_event_type, event_data)

    def set_shutting_down(self) -> None:
        """
        프로그램 종료 상태 설정
        
        이 메서드는 프로그램이 종료 중임을 표시하여 
        불필요한 로깅이나 알림을 방지합니다.
        """
        self._is_shutting_down = True
        self.log_debug("종료 상태로 설정됨")

    async def publish_system_event(self, event_type: str, **data) -> None:
        """
        시스템 이벤트 발행
        
        Args:
            event_type: 이벤트 타입
            **data: 이벤트 데이터
        """
        try:
            # 기본 이벤트 데이터 생성
            event_data = {
                "exchange_code": self.exchange_code,
                "timestamp": time.time()
            }
            
            # 추가 데이터 병합
            if data:
                event_data.update(data)
            
            # 이벤트 버스를 통해 발행
            if hasattr(self, 'event_bus') and self.event_bus:
                # 이벤트 타입 매핑 - EventTypes 사용
                mapped_event_type = None
                
                # 문자열 기반 이벤트 타입을 EventTypes 상수로 매핑
                if event_type == "SYSTEM_STARTUP" or event_type.lower() == "system_startup":
                    mapped_event_type = EventTypes.SYSTEM_STARTUP
                elif event_type == "SYSTEM_SHUTDOWN" or event_type.lower() == "system_shutdown":
                    mapped_event_type = EventTypes.SYSTEM_SHUTDOWN
                elif event_type == "CONNECTION_STATUS" or event_type.lower() == "connection_status":
                    mapped_event_type = EventTypes.CONNECTION_STATUS
                elif event_type == "SUBSCRIPTION_STATUS" or event_type.lower() == "subscription_status":
                    mapped_event_type = EventTypes.SUBSCRIPTION_STATUS
                elif event_type == "ERROR_EVENT" or event_type.lower() == "error_event":
                    mapped_event_type = EventTypes.ERROR_EVENT
                else:
                    # 매핑이 없는 경우 원래 이벤트 타입 사용
                    mapped_event_type = event_type
                    
                # 이벤트 발행
                await self.event_bus.publish(mapped_event_type, event_data)
            else:
                self.log_warning(f"이벤트 버스가 없어 이벤트 발행 불가: {event_type}")
                
        except Exception as e:
            self.log_error(f"시스템 이벤트 발행 중 오류: {str(e)}", exc_info=True)

# 싱글톤 인스턴스 접근 함수
def get_event_handler(exchange_code: str, settings: Dict[str, Any] = None) -> EventHandler:
    """
    이벤트 핸들러 인스턴스 반환
    
    Args:
        exchange_code: 거래소 코드
        settings: 설정 정보
        
    Returns:
        EventHandler: 이벤트 핸들러 인스턴스
    """
    return EventHandler.get_instance(exchange_code, settings)