"""
이벤트 버스 모듈

이 모듈은 컴포넌트 간의 이벤트 통신을 위한 간단한 이벤트 버스를 제공합니다.
싱글톤 패턴을 사용하여 애플리케이션 전체에서 하나의 인스턴스만 사용합니다.
"""

import asyncio
from typing import Dict, List, Callable, Any, Optional
from crosskimp.logger.logger import get_unified_logger

# 로거 설정
logger = get_unified_logger()

class EventBus:
    """
    이벤트 버스 클래스
    
    컴포넌트 간의 느슨한 결합을 위한 이벤트 전파 메커니즘을 제공합니다.
    싱글톤 패턴으로 구현되어 애플리케이션 전체에서 하나의 인스턴스만 사용합니다.
    """
    
    _instance = None
    
    @classmethod
    def get_instance(cls) -> 'EventBus':
        """싱글톤 인스턴스 반환"""
        if cls._instance is None:
            cls._instance = EventBus()
        return cls._instance
    
    def __init__(self):
        """초기화 - 이벤트 구독자 딕셔너리 생성"""
        if EventBus._instance is not None:
            raise Exception("EventBus는 싱글톤입니다. get_instance()를 사용하세요.")
            
        self._subscribers: Dict[str, List[Callable]] = {}
        self._logger = logger
        
    def subscribe(self, event_type: str, callback: Callable) -> None:
        """
        이벤트 구독
        
        Args:
            event_type: 이벤트 타입 (예: "connection_status_changed")
            callback: 이벤트 처리 콜백 함수 - 이벤트 데이터를 받는 함수
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
            
        if callback not in self._subscribers[event_type]:
            self._subscribers[event_type].append(callback)
            self._logger.debug(f"이벤트 {event_type}에 구독자 추가됨 (총 {len(self._subscribers[event_type])}개)")
        else:
            self._logger.debug(f"이벤트 {event_type}에 이미 동일한 구독자가 있음")
    
    def unsubscribe(self, event_type: str, callback: Callable) -> None:
        """
        이벤트 구독 취소
        
        Args:
            event_type: 이벤트 타입
            callback: 제거할 콜백 함수
        """
        if event_type in self._subscribers and callback in self._subscribers[event_type]:
            self._subscribers[event_type].remove(callback)
            self._logger.debug(f"이벤트 {event_type}에서 구독자 제거됨")
            
            # 구독자가 없으면 딕셔너리에서 키 자체를 제거
            if not self._subscribers[event_type]:
                del self._subscribers[event_type]
    
    async def _publish_to_subscribers(self, event_type: str, data: Any) -> None:
        """
        구독자들에게 이벤트 전달 (내부 공통 메서드)
        
        Args:
            event_type: 이벤트 타입
            data: 이벤트 데이터 (콜백에 전달됨)
        """
        if event_type not in self._subscribers:
            return
            
        # 모든 구독자에게 이벤트 전달
        for callback in self._subscribers[event_type]:
            try:
                # 콜백이 코루틴 함수인지 확인
                if asyncio.iscoroutinefunction(callback):
                    await callback(data)
                else:
                    callback(data)
            except Exception as e:
                self._logger.error(f"이벤트 {event_type} 처리 중 오류: {str(e)}")
    
    async def publish(self, event_type: str, data: Any) -> None:
        """
        이벤트 발행 (비동기)
        
        Args:
            event_type: 이벤트 타입
            data: 이벤트 데이터 (콜백에 전달됨)
        """
        await self._publish_to_subscribers(event_type, data)
    
    def publish_sync(self, event_type: str, data: Any) -> None:
        """
        이벤트 발행 (동기식)
        
        동기식 코드에서 이벤트를 발행할 때 사용합니다.
        
        Args:
            event_type: 이벤트 타입
            data: 이벤트 데이터 (콜백에 전달됨)
        """
        # 현재 실행 중인 이벤트 루프 확인
        try:
            loop = asyncio.get_running_loop()
            # 이미 이벤트 루프가 실행 중이면 future 생성
            asyncio.create_task(self._publish_to_subscribers(event_type, data))
        except RuntimeError:
            # 이벤트 루프가 실행 중이 아니면 새로 생성하여 실행
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._publish_to_subscribers(event_type, data))
            finally:
                loop.close()
    
    def clear_all_subscribers(self) -> None:
        """모든 구독자 제거"""
        self._subscribers.clear()
        self._logger.debug("모든 이벤트 구독자가 제거됨")
    
    def get_subscriber_count(self, event_type: Optional[str] = None) -> int:
        """
        구독자 수 반환
        
        Args:
            event_type: 이벤트 타입 (None이면 모든 구독자 수 반환)
            
        Returns:
            int: 구독자 수
        """
        if event_type is None:
            # 모든 이벤트 타입의 모든 구독자 수
            return sum(len(callbacks) for callbacks in self._subscribers.values())
        else:
            # 특정 이벤트 타입의 구독자 수
            return len(self._subscribers.get(event_type, [])) 