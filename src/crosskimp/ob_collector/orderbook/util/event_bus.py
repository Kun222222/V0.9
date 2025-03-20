"""
이벤트 버스 모듈

이 모듈은 컴포넌트 간의 이벤트 통신을 위한 간단한 이벤트 버스를 제공합니다.
싱글톤 패턴을 사용하여 애플리케이션 전체에서 하나의 인스턴스만 사용합니다.
"""

import asyncio
from typing import Dict, List, Callable, Any, Optional
from crosskimp.logger.logger import get_unified_logger
import time

# 로거 설정
logger = get_unified_logger()

# 이벤트 타입 정의 - 통합된 상수 집합
EVENT_TYPES = {
    # 시스템 이벤트
    "CONNECTION_STATUS": "connection_status",     # 연결 상태 변경
    "METRIC_UPDATE": "metric_update",             # 메트릭 업데이트
    "ERROR_EVENT": "error_event",                 # 오류 이벤트
    "SUBSCRIPTION_STATUS": "subscription_status", # 구독 상태 변경
    
    # 메시지 처리 이벤트
    "MESSAGE_RECEIVED": "message_received",       # 메시지 수신
    "MESSAGE_PROCESSED": "message_processed",     # 메시지 처리 완료
    "BATCH_COMPLETED": "batch_completed",         # 배치 처리 완료
    
    # 데이터 관련 이벤트
    "DATA_UPDATED": "data_updated",               # 데이터 업데이트
    "DATA_SNAPSHOT": "data_snapshot",             # 데이터 스냅샷
    "DATA_DELTA": "data_delta",                   # 데이터 변경
    
    # 시스템 상태 이벤트
    "SYSTEM_STARTUP": "system_startup",           # 시스템 시작
    "SYSTEM_SHUTDOWN": "system_shutdown",         # 시스템 종료
    "SYSTEM_HEARTBEAT": "system_heartbeat",       # 시스템 하트비트
    
    # 성능 관련 이벤트
    "PERFORMANCE_METRIC": "performance_metric",   # 성능 측정
    "RESOURCE_USAGE": "resource_usage",           # 리소스 사용량
    "LATENCY_REPORT": "latency_report",           # 지연 시간
    
    # 확장 가능한 커스텀 이벤트
    "CUSTOM_EVENT": "custom_event"                # 커스텀 이벤트
}

class EventBus:
    """
    이벤트 버스 클래스
    
    1. 단일 채널 사용으로 단순화
    2. 비동기 이벤트 처리 지원
    3. 이벤트 구독 및 발행 기능
    4. 싱글톤 패턴 구현
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
            event_type: 이벤트 타입 (예: "connection_status")
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
            
        # 비동기 태스크 리스트 생성
        tasks = []
        for callback in self._subscribers[event_type]:
            try:
                # 콜백이 코루틴 함수인지 확인
                if asyncio.iscoroutinefunction(callback):
                    # 비동기 태스크 생성하여 리스트에 추가
                    tasks.append(asyncio.create_task(callback(data)))
                else:
                    # 동기 함수는 바로 실행
                    callback(data)
            except Exception as e:
                self._logger.error(f"이벤트 {event_type} 처리 중 오류: {str(e)}")
        
        # 비동기 태스크가 있으면 동시에 실행하고 완료 대기
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def publish(self, event_type: str, data):
        """
        이벤트 발행
        
        Args:
            event_type: 이벤트 타입
            data: 이벤트 데이터 객체
        """
        # 타임스탬프가 없는 경우 자동으로 추가
        if isinstance(data, dict) and "timestamp" not in data:
            data["timestamp"] = time.time()
            
        await self._publish_to_subscribers(event_type, data)
    
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