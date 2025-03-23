"""
간소화된 이벤트 버스 시스템

이 모듈은 복잡한 기존 이벤트 버스를 대체하는 간단한 이벤트 버스를 제공합니다.
subscribe 대신 register_handler를 사용하여 이벤트 핸들러를 등록합니다.
"""

import asyncio
import logging
import time
from typing import Dict, List, Callable, Any, Optional
from enum import Enum, auto

from crosskimp.common.logger.logger import get_unified_logger

# 로거 설정
logger = get_unified_logger()

class EventType(Enum):
    """시스템 이벤트 유형"""
    # 시스템 이벤트
    SYSTEM_START = auto()
    SYSTEM_STOP = auto()
    
    # 프로세스 이벤트
    PROCESS_START = auto()
    PROCESS_STOP = auto()
    PROCESS_STATUS = auto()
    
    # 명령 이벤트
    COMMAND = auto()
    
    # 상태 이벤트
    STATUS_UPDATE = auto()
    
    # 오류 이벤트
    ERROR = auto()

class SimpleEventBus:
    """
    간소화된 이벤트 버스 구현
    
    복잡한 필터링 없이 기본적인 발행-구독 패턴만 구현합니다.
    """
    
    def __init__(self):
        """이벤트 버스 초기화"""
        # 이벤트 핸들러 저장 (이벤트 유형 -> 핸들러 목록)
        self._handlers: Dict[EventType, List[Callable]] = {event_type: [] for event_type in EventType}
        
        # 이벤트 큐
        self._queue = asyncio.Queue()
        
        # 처리 작업 참조
        self._processor_task = None
        
        # 실행 중 플래그
        self._running = False
        
        # 초기화 여부
        self._initialized = False
        
        # 로깅 
        self._logger = logger
        
        # 통계
        self.stats = {
            "published_events": 0,
            "processed_events": 0,
            "errors": 0
        }
    
    async def initialize(self):
        """이벤트 버스 초기화 (start 호출)"""
        if not self._initialized:
            await self.start()
            self._initialized = True
            
    def is_initialized(self) -> bool:
        """초기화 완료 여부 확인"""
        return self._initialized
        
    async def shutdown(self):
        """이벤트 버스 종료 (stop 호출)"""
        if self._initialized:
            await self.stop()
            self._initialized = False
            
    async def start(self):
        """이벤트 버스 시작"""
        if self._running:
            return
            
        self._running = True
        self._processor_task = asyncio.create_task(self._process_events())
        self._logger.info("간소화된 이벤트 버스가 시작되었습니다.")
    
    async def stop(self):
        """이벤트 버스 종료"""
        if not self._running:
            return
            
        self._running = False
        
        if self._processor_task and not self._processor_task.done():
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
                
        self._logger.info("간소화된 이벤트 버스가 종료되었습니다.")
    
    def register_handler(self, event_type: EventType, handler: Callable):
        """
        이벤트 핸들러 등록 (subscribe 대신 사용)
        
        Args:
            event_type: 이벤트 유형
            handler: 이벤트 핸들러 함수 (async def handler(data))
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []
            
        if handler not in self._handlers[event_type]:
            self._handlers[event_type].append(handler)
            self._logger.debug(f"이벤트 핸들러가 등록되었습니다: {event_type.name}")
    
    def unregister_handler(self, event_type: EventType, handler: Callable):
        """
        이벤트 핸들러 등록 해제
        
        Args:
            event_type: 이벤트 유형
            handler: 등록 해제할 핸들러 함수
        """
        if event_type in self._handlers and handler in self._handlers[event_type]:
            self._handlers[event_type].remove(handler)
            self._logger.debug(f"이벤트 핸들러가 등록 해제되었습니다: {event_type.name}")
    
    async def publish(self, event_type: EventType, data: Dict[str, Any]):
        """
        이벤트 발행
        
        Args:
            event_type: 이벤트 유형
            data: 이벤트 데이터
        """
        if not self._running:
            self._logger.warning("이벤트 버스가 실행 중이 아닙니다.")
            return
            
        # 공통 필드 추가
        event_data = {
            **data,
            "timestamp": time.time(),
            "event_type": event_type.name
        }
        
        # 이벤트 큐에 추가
        try:
            await self._queue.put((event_type, event_data))
            self.stats["published_events"] += 1
        except Exception as e:
            self._logger.error(f"이벤트 발행 중 오류: {str(e)}")
            self.stats["errors"] += 1
    
    async def _process_events(self):
        """이벤트 처리 루프"""
        try:
            while self._running:
                try:
                    # 큐에서 이벤트 가져오기
                    event_type, data = await self._queue.get()
                    
                    # 핸들러 실행
                    if event_type in self._handlers:
                        handlers = self._handlers[event_type]
                        if handlers:
                            for handler in handlers:
                                try:
                                    await handler(data)
                                except Exception as e:
                                    self._logger.error(f"이벤트 핸들러 오류 ({event_type.name}): {str(e)}")
                                    self.stats["errors"] += 1
                        else:
                            self._logger.debug(f"이벤트 {event_type.name}에 등록된 핸들러가 없습니다.")
                    
                    self.stats["processed_events"] += 1
                    self._queue.task_done()
                    
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self._logger.error(f"이벤트 처리 중 오류: {str(e)}")
                    self.stats["errors"] += 1
                    await asyncio.sleep(0.1)  # 오류 발생 시 잠시 대기
                    
        except asyncio.CancelledError:
            self._logger.info("이벤트 처리 루프가 취소되었습니다.")
        except Exception as e:
            self._logger.error(f"이벤트 처리 루프 종료: {str(e)}")
            self.stats["errors"] += 1 