"""
이벤트 어댑터 모듈

이 모듈은 기존 이벤트 시스템(EventBus)과 새로운 통합 이벤트 시스템(ComponentEventBus) 간의 
다리 역할을 하는 어댑터를 제공합니다.

이 어댑터를 통해 기존 코드가 중단 없이 계속 작동하면서도 새 이벤트 시스템으로 
점진적으로 마이그레이션할 수 있습니다.
"""

import asyncio
from typing import Callable, Dict, Any, Optional, Set, List

# 통합 이벤트 시스템 임포트
from crosskimp.common.events import (
    get_component_event_bus,
    Component, 
    EventTypes,
    EventPriority
)
from crosskimp.common.events.domains.orderbook import OrderbookEventTypes, EVENT_TYPE_MAPPING

# 기존 이벤트 시스템 임포트 (레거시 시스템 제거)
# from crosskimp.ob_collector.orderbook.util.event_bus import EventBus, EventPriority as OldEventPriority

# 레거시 EventPriority 상수들 직접 정의
class OldEventPriority:
    HIGH = 0      # 높은 우선순위
    NORMAL = 1    # 일반 우선순위
    LOW = 2       # 낮은 우선순위
    BACKGROUND = 3 # 백그라운드 처리 우선순위

from crosskimp.logger.logger import get_unified_logger

# 로거 설정
logger = get_unified_logger()

class EventAdapter:
    """
    이벤트 어댑터 클래스
    
    기존 EventBus와 동일한 인터페이스를 제공하면서 내부적으로는 
    새로운 ComponentEventBus를 사용합니다.
    """
    
    _instance = None
    
    @classmethod
    def get_instance(cls) -> 'EventAdapter':
        """싱글톤 인스턴스 반환"""
        if cls._instance is None:
            cls._instance = EventAdapter()
        return cls._instance
    
    def __init__(self):
        """초기화"""
        # 컴포넌트 이름 설정
        self.component_name = Component.ORDERBOOK
        
        # 실제 구현 이름 가져오기
        impl_name = Component.get_impl_name(self.component_name)
            
        # 새 이벤트 버스 가져오기
        self.event_bus = get_component_event_bus(self.component_name)
        
        # 이벤트 버스 시작 - 초기화 시 이미 시작되어 있지 않다면 시작
        if not self.event_bus.running:
            # 비동기 문맥에서 실행하기 위한 트릭
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self.event_bus.start())
            else:
                # 이 경우는 잘 발생하지 않지만 안전을 위해 추가
                loop.run_until_complete(self.event_bus.start())
        
        # 구독 추적을 위한 딕셔너리
        # {old_event_type: {callback: new_callback}}
        self.subscriptions = {}
        
        # 어댑터 초기화 로그
        logger.info(f"이벤트 어댑터가 초기화되었습니다. 컴포넌트: {self.component_name}")
        
    def _map_event_type(self, old_event_type: str) -> str:
        """기존 이벤트 타입을 새 이벤트 타입으로 변환"""
        # 이벤트 타입이 상수 객체인 경우 문자열로 변환
        if hasattr(old_event_type, 'value'):
            old_event_type = old_event_type.value
            
        # 이벤트 타입을 로깅
        logger.debug(f"이벤트 타입 변환 중: {old_event_type}")
            
        # 매핑에서 찾기
        if old_event_type in EVENT_TYPE_MAPPING:
            mapped_type = EVENT_TYPE_MAPPING[old_event_type]
            logger.debug(f"매핑에서 찾음: {old_event_type} -> {mapped_type}")
            return mapped_type
            
        # CONNECTION_STATUS와 같은 경우 별도 처리 (예: connection:status -> orderbook:connection:status)
        if ':' in old_event_type:
            group, subtype = old_event_type.split(':', 1)
            mapped_type = f"orderbook:{old_event_type}"
            logger.debug(f"그룹:서브타입 형식 변환: {old_event_type} -> {mapped_type}")
            return mapped_type
        
        # 매핑에 없는 경우 orderbook: 접두사 추가
        if not old_event_type.startswith('orderbook:'):
            mapped_type = f"orderbook:{old_event_type}"
            logger.debug(f"접두사 추가: {old_event_type} -> {mapped_type}")
            return mapped_type
            
        # 이미 올바른 형식이면 그대로 반환
        logger.debug(f"변환 없음: {old_event_type}")
        return old_event_type
    
    def _map_priority(self, old_priority: int) -> EventPriority:
        """기존 우선순위를 새 우선순위로 변환"""
        if old_priority == OldEventPriority.HIGH:
            return EventPriority.HIGH
        elif old_priority == OldEventPriority.NORMAL:
            return EventPriority.NORMAL
        elif old_priority == OldEventPriority.LOW:
            return EventPriority.LOW
        elif old_priority == OldEventPriority.BACKGROUND:
            return EventPriority.BACKGROUND
        else:
            return EventPriority.NORMAL
    
    async def subscribe(self, event_type: str, callback: Callable, event_filter=None) -> None:
        """
        이벤트 구독
        
        Args:
            event_type: 구독할 이벤트 타입 (기존 이벤트 타입)
            callback: 콜백 함수
            event_filter: 이벤트 필터 (현재는 무시됨)
        """
        if not asyncio.iscoroutinefunction(callback):
            logger.warning(f"비동기 함수가 아닌 콜백이 전달되었습니다: {callback.__name__}")
            
        # 새 이벤트 타입으로 변환
        new_event_type = self._map_event_type(event_type)
        
        # 콜백 래퍼 생성 (이벤트 데이터 형식 변환 포함)
        async def callback_wrapper(event_data):
            # 원래 콜백 호출
            try:
                await callback(event_data)
            except Exception as e:
                logger.error(f"이벤트 콜백 실행 중 오류 발생: {str(e)}")
        
        # 구독 추적 정보 저장
        if event_type not in self.subscriptions:
            self.subscriptions[event_type] = {}
        self.subscriptions[event_type][callback] = callback_wrapper
        
        # 새 이벤트 버스에 구독
        await self.event_bus.subscribe(new_event_type, callback_wrapper)
        
        logger.debug(f"이벤트 구독 등록: {event_type} -> {new_event_type}")
    
    def unsubscribe(self, event_type: str, callback: Callable) -> None:
        """
        이벤트 구독 해제
        
        Args:
            event_type: 구독 해제할 이벤트 타입 (기존 이벤트 타입)
            callback: 콜백 함수
        """
        # 매핑된 콜백 찾기
        if (event_type in self.subscriptions and 
            callback in self.subscriptions[event_type]):
            
            # 새 이벤트 타입으로 변환
            new_event_type = self._map_event_type(event_type)
            callback_wrapper = self.subscriptions[event_type][callback]
            
            # 새 이벤트 버스에서 구독 해제
            self.event_bus.unsubscribe(new_event_type, callback_wrapper)
            
            # 구독 추적 정보에서 제거
            del self.subscriptions[event_type][callback]
            
            logger.debug(f"이벤트 구독 해제: {event_type} -> {new_event_type}")
    
    async def publish(self, event_type: str, data: Dict, 
                    priority: int = OldEventPriority.NORMAL) -> None:
        """
        이벤트 발행
        
        Args:
            event_type: 발행할 이벤트 타입 (기존 이벤트 타입)
            data: 이벤트 데이터
            priority: 이벤트 우선순위 (기존 우선순위)
        """
        # 새 이벤트 타입과 우선순위로 변환
        new_event_type = self._map_event_type(event_type)
        new_priority = self._map_priority(priority)
        
        # 소스 정보 추가
        if isinstance(data, dict) and "source" not in data:
            data["source"] = "orderbook"
            
        # 디버그 로깅
        logger.info(f"이벤트 발행: {event_type} -> {new_event_type} (우선순위: {new_priority}, 데이터: {data})")
            
        # 새 이벤트 버스로 발행
        await self.event_bus.publish(new_event_type, data, new_priority)
    
    def clear_all_subscribers(self) -> None:
        """모든 구독자 제거"""
        for event_type, callbacks in self.subscriptions.items():
            new_event_type = self._map_event_type(event_type)
            for callback, wrapper in callbacks.items():
                self.event_bus.unsubscribe(new_event_type, wrapper)
        
        self.subscriptions.clear()
        logger.debug("모든 이벤트 구독이 제거되었습니다.")
    
    def get_subscriber_count(self, event_type: Optional[str] = None) -> int:
        """
        구독자 수 반환
        
        Args:
            event_type: 이벤트 타입 (None이면 모든 이벤트)
            
        Returns:
            int: 구독자 수
        """
        if event_type is None:
            # 모든 이벤트의 구독자 수
            return sum(len(callbacks) for callbacks in self.subscriptions.values())
        
        # 특정 이벤트의 구독자 수
        return len(self.subscriptions.get(event_type, {}))
    
    def get_stats(self) -> Dict[str, Any]:
        """이벤트 통계 반환"""
        # 새 이벤트 버스에서 통계 가져오기
        return self.event_bus.get_stats()
    
    def reset_stats(self) -> None:
        """통계 초기화"""
        # 이 기능은 새 이벤트 버스에는 없음
        pass
    
    def set_debug_mode(self, enabled: bool) -> None:
        """디버그 모드 설정"""
        # 이 기능은 새 이벤트 버스에는 없음
        pass
    
    async def flush_queues(self, timeout: float = 5.0) -> bool:
        """큐 비우기"""
        return await self.event_bus.flush_queues(timeout)

# 싱글톤 인스턴스 접근 함수
def get_event_adapter() -> EventAdapter:
    """이벤트 어댑터 인스턴스 반환"""
    return EventAdapter.get_instance() 