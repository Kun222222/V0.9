"""
오더북 콜렉터 이벤트 버스 

간소화된 이벤트 버스 구현으로 이벤트 발행/구독 기능을 제공합니다.
"""
import asyncio
import time
from typing import Dict, List, Callable, Any, Optional, Set
from collections import defaultdict

from crosskimp.common.logger.logger import get_unified_logger

from crosskimp.ob_collector.eventbus.types import EventPriority, EventTypes

# 로거 설정
logger = get_unified_logger()

# 로깅 제한이 필요한 이벤트 타입 목록
FREQUENT_EVENTS = {
    "orderbook_updated": 30,     # 30초마다 로깅
    "ticker_updated": 60,        # 60초마다 로깅
    "delta_received": 30,        # 30초마다 로깅
    "snapshot_received": 30,     # 30초마다 로깅
    "message_received": 60,      # 60초마다 로깅
    "message_processed": 60      # 60초마다 로깅
}

# 추가적으로 제한할 필요가 있는 이벤트 패턴 (접두어 매칭)
FREQUENT_EVENT_PATTERNS = [
    "message_",    # message_ 로 시작하는 모든 이벤트
    "data_",       # data_ 로 시작하는 모든 이벤트
    "processing_"  # processing_ 으로 시작하는 모든 이벤트
]

# 연결 시도 이벤트 - 별도 로깅 규칙 적용
CONNECTION_EVENTS = [
    "connection_attempt",     # 연결 시도
    "connection_retry",       # 연결 재시도
    "reconnect_attempt"       # 재연결 시도
]

# 연결 이벤트의 로깅 간격 (초)
CONNECTION_LOG_INTERVAL = 1  # 매번 로그 출력하도록 변경 (기존 10에서 1로 수정)

# 항상 로깅할 중요 이벤트 목록
IMPORTANT_EVENTS = {
    "connection_status",
    "connection_success",     # 연결 성공
    "connection_failure",     # 연결 실패
    "subscription_status",
    "subscription_success",   # 구독 성공
    "subscription_failure",   # 구독 실패
    "error_event",
    "system_startup",
    "system_shutdown"
}

class EventBus:
    """
    이벤트 버스 클래스 - 싱글톤 패턴
    
    이벤트 발행/구독 기능을 제공합니다.
    """
    _instance = None
    
    @classmethod
    def get_instance(cls) -> 'EventBus':
        """싱글톤 인스턴스 반환"""
        if cls._instance is None:
            cls._instance = EventBus()
        return cls._instance
    
    def __init__(self):
        """초기화"""
        # 구독자 딕셔너리 (이벤트 타입 -> 콜백 목록)
        self._subscribers = defaultdict(list)
        
        # 이벤트 처리 큐 (우선순위별)
        self._event_queues = {
            EventPriority.HIGH: asyncio.Queue(maxsize=500),
            EventPriority.NORMAL: asyncio.Queue(maxsize=1000),
            EventPriority.LOW: asyncio.Queue(maxsize=1500),
            EventPriority.BACKGROUND: asyncio.Queue(maxsize=2000),
        }
        
        # 프로세서 태스크
        self._processor_tasks = {
            EventPriority.HIGH: None,
            EventPriority.NORMAL: None,
            EventPriority.LOW: None,
            EventPriority.BACKGROUND: None,
        }
        
        # 통계
        self.stats = {
            "total_published": 0,
            "total_processed": 0,
            "by_type": defaultdict(int),
            "error_count": 0,
        }
        
        # 상태 플래그
        self.running = False
        self.shutting_down = False
        
        # 자동 시작 여부
        self._auto_start_in_progress = False
        
        # 로깅 제한을 위한 변수
        self._last_log_time = defaultdict(float)
        self._event_counts = defaultdict(int)
    
    async def start(self):
        """이벤트 버스 시작"""
        if self.running:
            return
            
        self.running = True
        self.shutting_down = False
        self._auto_start_in_progress = False
        
        # 각 우선순위별 이벤트 프로세서 시작
        for priority in EventPriority:
            self._processor_tasks[priority] = asyncio.create_task(
                self._process_events(priority)
            )
            
        logger.info("이벤트 버스 시작됨")
    
    async def stop(self):
        """이벤트 버스 종료"""
        if not self.running:
            return
            
        self.shutting_down = True
        self.running = False
        
        # 각 프로세서 태스크 취소
        for priority, task in self._processor_tasks.items():
            if task and not task.done():
                task.cancel()
                
        # 모든 태스크 완료 대기
        pending_tasks = [task for task in self._processor_tasks.values() 
                         if task and not task.done()]
        
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
            
        # 큐 초기화
        for queue in self._event_queues.values():
            while not queue.empty():
                try:
                    await queue.get()
                    queue.task_done()
                except:
                    pass
                    
        logger.info("이벤트 버스 종료됨")
        
        # 로깅 통계 출력
        self._log_event_stats()
    
    def _log_event_stats(self):
        """이벤트 통계 로깅"""
        try:
            log_msg = "이벤트 버스 통계 요약:\n"
            log_msg += f"총 발행된 이벤트: {self.stats['total_published']}\n"
            log_msg += f"총 처리된 이벤트: {self.stats['total_processed']}\n"
            log_msg += f"오류 횟수: {self.stats['error_count']}\n"
            
            # 이벤트 타입별 통계
            log_msg += "이벤트 타입별 발행 횟수 (상위 10개):\n"
            sorted_events = sorted(self.stats["by_type"].items(), key=lambda x: x[1], reverse=True)
            for event_type, count in sorted_events[:10]:
                log_msg += f"  {event_type}: {count}회\n"
                
            # 통계 로그 출력
            logger.info(log_msg)
        except Exception as e:
            logger.error(f"통계 로깅 중 오류: {str(e)}")
    
    async def subscribe(self, event_type: str, callback: Callable) -> None:
        """
        이벤트 구독
        
        Args:
            event_type: 구독할 이벤트 타입
            callback: 비동기 콜백 함수
        """
        if not asyncio.iscoroutinefunction(callback):
            logger.warning(f"비동기 함수가 아닌 콜백이 전달되었습니다: {callback.__name__}")
            
        if callback not in self._subscribers[event_type]:
            self._subscribers[event_type].append(callback)
            logger.debug(f"이벤트 구독 등록: {event_type}, 콜백: {callback.__name__}")
    
    def unsubscribe(self, event_type: str, callback: Callable) -> None:
        """
        이벤트 구독 해제
        
        Args:
            event_type: 구독 해제할 이벤트 타입
            callback: 콜백 함수
        """
        if callback in self._subscribers[event_type]:
            self._subscribers[event_type].remove(callback)
            logger.debug(f"이벤트 구독 해제: {event_type}, 콜백: {callback.__name__}")
    
    async def publish(self, event_type: str, data: Dict[str, Any], 
                     priority: EventPriority = EventPriority.NORMAL) -> None:
        """
        이벤트 발행
        
        Args:
            event_type: 발행할 이벤트 타입
            data: 이벤트 데이터
            priority: 이벤트 우선순위
        """
        # 이벤트 버스가 실행 중이 아니면 자동 시작
        if not self.running and not self._auto_start_in_progress:
            logger.info(f"이벤트 버스가 실행 중이 아닙니다. 자동으로 시작합니다.")
            self._auto_start_in_progress = True
            asyncio.create_task(self._auto_start_and_publish(event_type, data, priority))
            return
            
        if not self.running:
            logger.warning(f"이벤트 버스가 실행 중이 아닙니다. 이벤트 발행 무시: {event_type}")
            return
            
        # 이벤트 데이터 보강
        event_data = {
            "event_type": event_type,
            "timestamp": data.get("timestamp", time.time()),
            "data": data
        }
        
        # 통계 업데이트
        self.stats["total_published"] += 1
        self.stats["by_type"][event_type] += 1
        
        # 이벤트 카운트 증가 - 로깅 빈도 제한을 위해 사용
        self._event_counts[event_type] += 1
        
        # 이벤트 큐에 추가
        try:
            await self._event_queues[priority].put(event_data)
            
            # 로깅 제한 적용
            self._log_event_with_rate_limiting(event_type, priority)
        except asyncio.QueueFull:
            logger.error(f"이벤트 큐가 가득 찼습니다. 이벤트 무시: {event_type}")
            self.stats["error_count"] += 1
    
    def _log_event_with_rate_limiting(self, event_type: str, priority: EventPriority):
        """
        이벤트 로깅에 빈도 제한 적용
        
        Args:
            event_type: 이벤트 타입
            priority: 이벤트 우선순위
        """
        current_time = time.time()
        
        # 중요 이벤트는 항상 로깅
        if event_type in IMPORTANT_EVENTS:
            logger.debug(f"이벤트 발행: {event_type}, 우선순위: {priority.name}")
            return
        
        # 연결 시도 관련 이벤트 - 특별 처리 (10회 마다)
        if event_type in CONNECTION_EVENTS:
            count = self._event_counts[event_type]
            # 첫 시도이거나 CONNECTION_LOG_INTERVAL(10)의 배수일 때만 로깅
            if count == 1 or count % CONNECTION_LOG_INTERVAL == 0:
                logger.debug(f"이벤트 발행: {event_type}, 우선순위: {priority.name} (시도 횟수: {count})")
            return
        
        # 자주 발생하는 이벤트인 경우 로깅 제한 적용
        if event_type in FREQUENT_EVENTS:
            log_interval = FREQUENT_EVENTS[event_type]
            last_log_time = self._last_log_time[event_type]
            
            # 지정된 간격이 지났거나 처음 발생한 경우에만 로깅
            if current_time - last_log_time >= log_interval or last_log_time == 0:
                count = self._event_counts[event_type]
                if count > 1:
                    logger.debug(f"이벤트 발행: {event_type}, 우선순위: {priority.name} (마지막 {log_interval}초 동안 {count}회 발생)")
                else:
                    logger.debug(f"이벤트 발행: {event_type}, 우선순위: {priority.name}")
                
                # 카운트 초기화 및 마지막 로깅 시간 업데이트
                self._event_counts[event_type] = 0
                self._last_log_time[event_type] = current_time
            return
        
        # 패턴에 맞는 빈번한 이벤트인지 확인
        for pattern in FREQUENT_EVENT_PATTERNS:
            if event_type.startswith(pattern):
                # 기본 60초 간격으로 제한 적용
                log_interval = 60
                last_log_time = self._last_log_time[event_type]
                
                # 지정된 간격이 지났거나 처음 발생한 경우에만 로깅
                if current_time - last_log_time >= log_interval or last_log_time == 0:
                    count = self._event_counts[event_type]
                    if count > 1:
                        logger.debug(f"이벤트 발행: {event_type}, 우선순위: {priority.name} (마지막 {log_interval}초 동안 {count}회 발생)")
                    else:
                        logger.debug(f"이벤트 발행: {event_type}, 우선순위: {priority.name}")
                    
                    # 카운트 초기화 및 마지막 로깅 시간 업데이트
                    self._event_counts[event_type] = 0
                    self._last_log_time[event_type] = current_time
                return
        
        # 기타 이벤트는 모두 로깅
        logger.debug(f"이벤트 발행: {event_type}, 우선순위: {priority.name}")
    
    async def _auto_start_and_publish(self, event_type: str, data: Dict[str, Any], 
                                    priority: EventPriority = EventPriority.NORMAL):
        """
        이벤트 버스 자동 시작 및 이벤트 발행
        
        Args:
            event_type: 발행할 이벤트 타입
            data: 이벤트 데이터
            priority: 이벤트 우선순위
        """
        # 이벤트 버스 시작
        await self.start()
        
        # 버스가 시작되어 있는지 확인
        if self.running:
            # 이벤트 발행
            await self.publish(event_type, data, priority)
        else:
            logger.error(f"이벤트 버스 자동 시작 실패. 이벤트 발행 무시: {event_type}")
        
        self._auto_start_in_progress = False
    
    @property
    def is_running(self) -> bool:
        """이벤트 버스 실행 상태 확인"""
        return self.running
    
    async def _process_events(self, priority: EventPriority):
        """
        이벤트 처리 루프
        
        Args:
            priority: 처리할 이벤트 우선순위
        """
        queue = self._event_queues[priority]
        
        while self.running:
            try:
                # 이벤트 가져오기
                event_data = await queue.get()
                
                # 이벤트 처리
                await self._dispatch_event(event_data)
                
                # 태스크 완료 표시
                queue.task_done()
                
            except asyncio.CancelledError:
                # 취소되면 루프 종료
                break
            except Exception as e:
                logger.error(f"이벤트 처리 중 오류 발생: {str(e)}")
                self.stats["error_count"] += 1
    
    async def _dispatch_event(self, event_data: Dict[str, Any]):
        """
        이벤트 디스패치 (구독자에게 전달)
        
        Args:
            event_data: 이벤트 데이터
        """
        event_type = event_data["event_type"]
        data = event_data["data"]
        
        subscribers = self._subscribers[event_type]
        if not subscribers:
            return
            
        # 각 구독자에게 이벤트 전달
        for callback in subscribers:
            try:
                await callback(data)
                self.stats["total_processed"] += 1
            except Exception as e:
                logger.error(f"이벤트 콜백 실행 중 오류: {str(e)}, 이벤트: {event_type}")
                self.stats["error_count"] += 1
                
    def get_stats(self) -> Dict[str, Any]:
        """이벤트 통계 반환"""
        # 큐 크기 정보 추가
        queue_sizes = {p.name: q.qsize() for p, q in self._event_queues.items()}
        
        stats = {
            **self.stats,
            "queue_sizes": queue_sizes,
            "running": self.running,
            "subscribers_count": {et: len(subs) for et, subs in self._subscribers.items()},
            "total_subscribers": sum(len(subs) for subs in self._subscribers.values())
        }
        
        return stats
