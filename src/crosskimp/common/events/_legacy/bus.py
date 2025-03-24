"""
컴포넌트 이벤트 버스 모듈

이 모듈은 개별 컴포넌트의 이벤트 처리를 위한 기능을 제공합니다.
- EventFilter: 이벤트 필터링 기능
- ComponentEventBus: 컴포넌트별 독립 이벤트 버스
"""

import asyncio
import logging
import time
import re
from enum import Enum
from typing import Dict, List, Set, Callable, Any, Optional, Pattern
from collections import defaultdict
from functools import partial

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events._legacy.types import EventPriority, EventTypes, DEFAULT_PRIORITY

# 로거 설정
logger = get_unified_logger()

class EventFilter:
    """이벤트 필터링 클래스"""
    
    def __init__(self, 
                include_types: Optional[Set[str]] = None,
                exclude_types: Optional[Set[str]] = None,
                include_groups: Optional[Set[str]] = None,
                exclude_groups: Optional[Set[str]] = None,
                source_pattern: Optional[Pattern] = None,
                max_frequency: Optional[float] = None):
        """
        이벤트 필터 초기화
        
        Args:
            include_types: 포함할 이벤트 타입 (None이면 모든 타입)
            exclude_types: 제외할 이벤트 타입
            include_groups: 포함할 이벤트 그룹 (None이면 모든 그룹)
            exclude_groups: 제외할 이벤트 그룹
            source_pattern: 소스 패턴 (정규식)
            max_frequency: 최대 이벤트 빈도 (Hz 단위, None이면 제한 없음)
        """
        self.include_types = include_types
        self.exclude_types = exclude_types or set()
        self.include_groups = include_groups
        self.exclude_groups = exclude_groups or set()
        self.source_pattern = source_pattern
        self.max_frequency = max_frequency
        
        # 빈도 제한을 위한 마지막 이벤트 시간 저장
        self.last_times = {}
    
    def match(self, event_type: str, data: Dict) -> bool:
        """
        이벤트가 필터 조건과 일치하는지 확인
        
        Args:
            event_type: 이벤트 타입
            data: 이벤트 데이터
            
        Returns:
            bool: 필터 조건 일치 여부
        """
        # 이벤트 타입 필터링
        if self.include_types is not None and event_type not in self.include_types:
            return False
        if event_type in self.exclude_types:
            return False
        
        # 이벤트 그룹 필터링
        event_group = event_type.split(':')[0] if ':' in event_type else None
        if event_group:
            if self.include_groups is not None and event_group not in self.include_groups:
                return False
            if self.exclude_groups and event_group in self.exclude_groups:
                return False
        
        # 소스 패턴 필터링
        if self.source_pattern and 'source' in data:
            if not self.source_pattern.match(str(data['source'])):
                return False
        
        # 빈도 제한 필터링
        if self.max_frequency is not None:
            current_time = time.time()
            min_interval = 1.0 / self.max_frequency
            last_time = self.last_times.get(event_type, 0)
            
            if current_time - last_time < min_interval:
                return False
                
            self.last_times[event_type] = current_time
        
        return True


class ComponentEventBus:
    """컴포넌트별 이벤트 버스 클래스"""
    
    def __init__(self, component_name: str):
        """
        이벤트 버스 초기화
        
        Args:
            component_name: 컴포넌트 이름
        """
        self.component_name = component_name
        
        # 이벤트 구독자 딕셔너리 (이벤트 유형 -> 구독자 목록)
        self._subscribers = {}
        
        # 구독자별 필터 딕셔너리 (구독자 -> 필터)
        self._subscriber_filters = {}
        
        # 이벤트 처리 큐 (우선순위별)
        self._event_queues = {
            EventPriority.HIGH: asyncio.Queue(maxsize=1000),     # 크기 제한
            EventPriority.NORMAL: asyncio.Queue(maxsize=2000),   # 크기 제한
            EventPriority.LOW: asyncio.Queue(maxsize=5000),      # 크기 제한 (BACKGROUND 제거)
        }
        
        # 프로세서 태스크
        self._processor_tasks = {
            EventPriority.HIGH: None,
            EventPriority.NORMAL: None,
            EventPriority.LOW: None,
        }
        
        # 통계
        self.stats = {
            "total_published": 0,
            "total_processed": 0,
            "by_type": defaultdict(int),
            "error_count": 0,
            "last_events": {},
            "queue_sizes": {},
            "avg_processing_time": defaultdict(float),
        }
        
        # 상태 플래그
        self.running = False
        self.shutting_down = False
        
        # 로거
        self._logger = logger
    
    async def start(self):
        """이벤트 프로세서 시작"""
        if self.running:
            return
            
        self.running = True
        self.shutting_down = False
        
        # 모든 우선순위의 이벤트 프로세서 시작
        for priority in self._event_queues.keys():
            if self._processor_tasks[priority] is None or self._processor_tasks[priority].done():
                self._processor_tasks[priority] = asyncio.create_task(
                    self._event_processor(priority)
                )
                
        self._logger.info(f"[이벤트] '{self.component_name}' 오더북 메니저 이벤트 버스 시작됨")
    
    async def stop(self):
        """이벤트 프로세서 중지"""
        if not self.running:
            return
            
        self.running = False
        self.shutting_down = True
        
        # 모든 프로세서 태스크 취소
        for priority, task in self._processor_tasks.items():
            if task and not task.done():
                task.cancel()
                
        # 태스크 완료 대기
        pending_tasks = [task for task in self._processor_tasks.values() if task and not task.done()]
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
            
        self._logger.info(f"[이벤트] '{self.component_name}' 이벤트 버스 중지됨")
    
    async def _event_processor(self, priority: EventPriority):
        """
        이벤트 프로세서 태스크
        
        Args:
            priority: 이벤트 우선순위
        """
        queue = self._event_queues[priority]
        
        try:
            while self.running:
                try:
                    # 타임아웃을 추가하여 주기적으로 종료 체크
                    try:
                        event_type, data, timestamp = await asyncio.wait_for(queue.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        # 주기적으로 종료 체크
                        if self.shutting_down:
                            break
                        continue
                    
                    # 이벤트 처리
                    try:
                        await self._process_event(event_type, data, timestamp)
                    except Exception as e:
                        self.stats["error_count"] += 1
                        self._logger.error(f"[이벤트] '{self.component_name}' 이벤트 처리 중 오류 (우선순위 {priority.name}): {str(e)}")
                    finally:
                        queue.task_done()
                        
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    if not self.shutting_down:
                        self._logger.error(f"[이벤트] '{self.component_name}' 프로세서 오류: {str(e)}")
                        # 잠시 대기 후 계속
                        await asyncio.sleep(0.5)
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self._logger.error(f"[이벤트] '{self.component_name}' 프로세서 태스크 종료: {str(e)}")
    
    async def _process_event(self, event_type: str, data: Dict, timestamp: float):
        """
        이벤트 처리 (내부 메서드)
        
        Args:
            event_type: 이벤트 타입
            data: 이벤트 데이터
            timestamp: 이벤트 타임스탬프
        """
        # 처리 시작 시간
        start_time = time.time()
        
        # 구독자 목록 생성
        subscribers = set(self._subscribers.get(event_type, []))
        
        # 구독자가 없는 경우
        if not subscribers:
            return
        
        # 구독자별 이벤트 전달 (비동기 태스크 생성)
        tasks = []
        for subscriber in subscribers:
            # 필터 확인
            if subscriber in self._subscriber_filters:
                event_filter = self._subscriber_filters[subscriber]
                if not event_filter.match(event_type, data):
                    continue
            
            try:
                # 코루틴 함수인지 확인
                if asyncio.iscoroutinefunction(subscriber):
                    # 타임아웃 추가
                    wrapped_task = asyncio.create_task(
                        asyncio.wait_for(subscriber(data), timeout=5.0)
                    )
                    tasks.append(wrapped_task)
                else:
                    # 동기 함수는 바로 실행
                    subscriber(data)
            except Exception as e:
                self._logger.error(f"[이벤트] '{self.component_name}' 이벤트 {event_type} 처리 중 오류: {str(e)}")
        
        # 비동기 태스크가 있으면 동시에 실행하고 완료 대기
        if tasks:
            try:
                # 'return_exceptions' 인자 제거
                done, pending = await asyncio.wait(tasks)
                
                # 오류 확인
                for task in done:
                    try:
                        if task.exception():
                            self._logger.error(f"[이벤트] '{self.component_name}' 이벤트 처리 태스크 오류: {task.exception()}")
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        self._logger.error(f"[이벤트] '{self.component_name}' 이벤트 처리 결과 확인 중 오류: {str(e)}")
                        
                # 타임아웃된 태스크 취소
                for task in pending:
                    task.cancel()
            except Exception as e:
                self._logger.error(f"[이벤트] '{self.component_name}' 이벤트 처리 대기 중 오류: {str(e)}")
        
        # 이벤트 처리 완료
        processing_time = time.time() - start_time
        
        # 통계 업데이트
        self.stats["total_processed"] += 1
        self.stats["by_type"][event_type] += 1
        
        # 평균 처리 시간 업데이트 (이동 평균)
        prev_avg = self.stats["avg_processing_time"].get(event_type, 0)
        if prev_avg == 0:
            self.stats["avg_processing_time"][event_type] = processing_time
        else:
            # 이동 평균 (90% 기존값 + 10% 새 값)
            self.stats["avg_processing_time"][event_type] = 0.9 * prev_avg + 0.1 * processing_time
        
        # 오래 걸린 처리 로깅
        if processing_time > 0.5:  # 500ms 이상 걸린 경우
            self._logger.warning(f"[이벤트] '{self.component_name}' 이벤트 {event_type} 처리에 {processing_time:.3f}초 소요됨")
    
    async def subscribe(self, event_type: str, callback: Callable, event_filter: Optional[EventFilter] = None) -> None:
        """
        이벤트 구독
        
        Args:
            event_type: 이벤트 타입
            callback: 이벤트 처리할 콜백 함수
            event_filter: 이벤트 필터 (선택 사항)
        """
        # 이벤트 타입에 대한 구독자 목록 초기화
        if event_type not in self._subscribers:
            self._subscribers[event_type] = set()
        
        # 이미 구독 중인지 확인
        if callback in self._subscribers[event_type]:
            # 필터만 업데이트하고 반환
            if event_filter:
                self._subscriber_filters[callback] = event_filter
            return
        
        # 구독자 추가
        self._subscribers[event_type].add(callback)
        
        # 필터 설정
        if event_filter:
            self._subscriber_filters[callback] = event_filter
        
        # 콜백 함수 이름 구하기 (로깅용)
        if hasattr(callback, '__name__'):
            callback_name = callback.__name__
        elif hasattr(callback, '__qualname__'):
            callback_name = callback.__qualname__
        else:
            callback_name = str(callback)
        
        self._logger.debug(f"[이벤트] '{self.component_name}' 이벤트 {event_type}에 구독자 추가: {callback_name}")
    
    def unsubscribe(self, event_type: str, callback: Callable) -> None:
        """
        이벤트 구독 취소
        
        Args:
            event_type: 이벤트 타입
            callback: 제거할 콜백 함수
        """
        # 구독자 목록에서 제거
        if event_type in self._subscribers and callback in self._subscribers[event_type]:
            self._subscribers[event_type].remove(callback)
            
            # 필터가 있으면 함께 제거
            if callback in self._subscriber_filters:
                del self._subscriber_filters[callback]
            
            # 구독자가 없으면 딕셔너리에서 키 자체를 제거
            if not self._subscribers[event_type]:
                del self._subscribers[event_type]
    
    async def publish(self, event_type: str, data: Dict, priority: Optional[EventPriority] = None) -> bool:
        """
        이벤트 발행
        
        Args:
            event_type: 이벤트 타입
            data: 이벤트 데이터
            priority: 이벤트 우선순위 (기본값: DEFAULT_PRIORITY)
            
        Returns:
            bool: 성공 여부
        """
        if not self.running:
            return False
        
        # 우선순위가 지정되지 않은 경우 기본값 사용
        if priority is None:
            from .types import EVENT_PRIORITIES
            priority = EVENT_PRIORITIES.get(event_type, DEFAULT_PRIORITY)
            
        # 현재 시간 기록
        timestamp = time.time()
        
        # 타임스탬프가 없는 경우 자동으로 추가
        if isinstance(data, dict) and "timestamp" not in data:
            data["timestamp"] = timestamp
        
        # 컴포넌트 정보 추가
        if isinstance(data, dict) and "component" not in data:
            data["component"] = self.component_name
        
        # 통계 업데이트
        self.stats["total_published"] += 1
        self.stats["last_events"][event_type] = timestamp
        
        try:
            # 해당 우선순위 큐에 이벤트 추가 (타임아웃 및 큐 가득참 처리)
            try:
                # 큐가 가득 찬 경우 낮은 우선순위 이벤트는 드롭
                if priority.value >= EventPriority.LOW.value and self._event_queues[priority].full():
                    self._logger.warning(f"[이벤트] '{self.component_name}' 큐 가득참, 이벤트 드롭: {event_type}")
                    return False
                    
                # 1초 타임아웃으로 이벤트 추가
                await asyncio.wait_for(
                    self._event_queues[priority].put((event_type, data, timestamp)),
                    timeout=1.0
                )
                return True
                
            except asyncio.TimeoutError:
                self._logger.warning(f"[이벤트] '{self.component_name}' 이벤트 큐 추가 타임아웃: {event_type}")
                return False
                
            except asyncio.QueueFull:
                self._logger.warning(f"[이벤트] '{self.component_name}' 이벤트 큐 가득참: {event_type}")
                return False
                
        except Exception as e:
            self._logger.error(f"[이벤트] '{self.component_name}' 이벤트 발행 중 오류: {str(e)}")
            return False
    
    def update_stats(self):
        """통계 정보 업데이트"""
        # 큐 크기 업데이트
        for priority, queue in self._event_queues.items():
            self.stats["queue_sizes"][priority.name] = queue.qsize()
    
    def get_stats(self):
        """
        이벤트 버스 통계 정보 반환
        
        Returns:
            Dict: 통계 정보
        """
        self.update_stats()
        return self.stats
    
    def clear_all_subscribers(self) -> None:
        """모든 구독자 제거"""
        self._subscribers.clear()
        self._subscriber_filters.clear()
        self._logger.debug(f"[이벤트] '{self.component_name}' 모든 이벤트 구독자가 제거됨")
    
    async def flush_queues(self, timeout: float = 5.0) -> bool:
        """
        모든 이벤트 큐 처리 완료 대기
        
        Args:
            timeout: 최대 대기 시간 (초)
            
        Returns:
            bool: 성공 여부
        """
        try:
            # 각 우선순위 큐에 대해 완료 대기
            tasks = []
            for priority, queue in self._event_queues.items():
                if queue.qsize() > 0:
                    tasks.append(queue.join())
            
            if not tasks:
                return True
                
            # 모든 큐 완료 대기 - 안전하게 구현
            try:
                tasks_with_timeout = [asyncio.create_task(task) for task in tasks]
                done_tasks = []
                
                # 제한 시간 내에 모든 태스크 완료 대기
                end_time = time.time() + timeout
                while tasks_with_timeout and time.time() < end_time:
                    remaining_time = end_time - time.time()
                    if remaining_time <= 0:
                        raise asyncio.TimeoutError("큐 플러시 대기 시간 초과")
                    
                    # 하나씩 대기 (안전한 방법)
                    done, tasks_with_timeout = await asyncio.wait(
                        tasks_with_timeout, 
                        timeout=remaining_time,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    done_tasks.extend(done)
                
                # 완료되지 않은 태스크가 있으면 타임아웃
                if tasks_with_timeout:
                    raise asyncio.TimeoutError(f"{len(tasks_with_timeout)}개 태스크 미완료")
                
                # 완료된 태스크에서 예외 확인
                for task in done_tasks:
                    # 예외가 있으면 가져오기 (발생시키지 않음)
                    if task.exception():
                        self._logger.warning(f"[이벤트] '{self.component_name}' 큐 플러시 중 예외: {task.exception()}")
                
                return True
            except asyncio.TimeoutError as e:
                self._logger.warning(f"[이벤트] '{self.component_name}' 이벤트 큐 플러시 타임아웃 ({timeout}초): {str(e)}")
                
                # 남은 태스크 취소
                for task in tasks_with_timeout:
                    task.cancel()
                
                return False
            
        except asyncio.TimeoutError:
            self._logger.warning(f"[이벤트] '{self.component_name}' 이벤트 큐 플러시 타임아웃 ({timeout}초)")
            return False
        except Exception as e:
            self._logger.error(f"[이벤트] '{self.component_name}' 이벤트 큐 플러시 중 오류: {str(e)}")
            return False


# 이벤트 필터 생성 헬퍼 함수
def create_event_filter(include_types=None, exclude_types=None, 
                      include_groups=None, exclude_groups=None,
                      source_pattern=None, max_frequency=None):
    """
    이벤트 필터 생성
    
    Args:
        include_types: 포함할 이벤트 타입 목록
        exclude_types: 제외할 이벤트 타입 목록
        include_groups: 포함할 이벤트 그룹 목록
        exclude_groups: 제외할 이벤트 그룹 목록
        source_pattern: 소스 패턴 (정규식 문자열)
        max_frequency: 최대 이벤트 빈도 (Hz)
            
    Returns:
        EventFilter: 생성된 필터 객체
    """
    # 문자열 목록을 집합으로 변환
    inc_types = set(include_types) if include_types else None
    exc_types = set(exclude_types) if exclude_types else None
    inc_groups = set(include_groups) if include_groups else None
    exc_groups = set(exclude_groups) if exclude_groups else None
    
    # 정규식 패턴 컴파일
    pattern = re.compile(source_pattern) if source_pattern else None
    
    return EventFilter(
        include_types=inc_types,
        exclude_types=exc_types,
        include_groups=inc_groups,
        exclude_groups=exc_groups,
        source_pattern=pattern,
        max_frequency=max_frequency
    ) 