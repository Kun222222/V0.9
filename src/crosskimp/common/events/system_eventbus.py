"""
시스템 이벤트 버스 시스템

이 모듈은 복잡한 기존 이벤트 버스를 대체하는 간단한 이벤트 버스를 제공합니다.
subscribe 대신 register_handler를 사용하여 이벤트 핸들러를 등록합니다.
"""

import asyncio
import time
from typing import Dict, List, Callable, Any, Optional, Tuple, Union, Type
from enum import Enum

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.events.system_types import (
    EventCategory, SystemEventType, ObCollectorEventType, 
    RadarEventType, TradeEventType, NotificationEventType, 
    PerformanceEventType, TelegramEventType
)

# 로거 설정
logger = get_unified_logger()

# 글로벌 이벤트 버스 인스턴스
_event_bus_instance: Optional['SystemEventBus'] = None
_component_event_buses = {}

def get_event_bus() -> 'SystemEventBus':
    """글로벌 이벤트 버스 인스턴스를 반환합니다."""
    global _event_bus_instance
    
    if _event_bus_instance is None:
        _event_bus_instance = SystemEventBus()
        
    return _event_bus_instance

def get_component_event_bus(component: SystemComponent) -> 'SystemEventBus':
    """
    컴포넌트별 이벤트 버스 인스턴스를 반환합니다.
    실제로는 글로벌 이벤트 버스와 동일한 인스턴스이지만,
    컴포넌트별로 구분하여 사용할 수 있도록 합니다.
    """
    global _component_event_buses
    
    if component not in _component_event_buses:
        _component_event_buses[component] = get_event_bus()
        
    return _component_event_buses[component]

class SystemEventBus:
    """
    시스템 이벤트 버스 구현
    
    복잡한 필터링 없이 기본적인 발행-구독 패턴만 구현합니다.
    각 이벤트 카테고리와 타입에 따라 핸들러를 관리합니다.
    """
    
    def __init__(self):
        """이벤트 버스 초기화"""
        # 이벤트 핸들러 저장 (이벤트 유형 -> 핸들러 목록)
        self._handlers: Dict[Any, List[Callable]] = {}
        
        # 각 이벤트 타입 열거형에 대한 핸들러 초기화
        self._init_handlers(SystemEventType)
        self._init_handlers(ObCollectorEventType)
        self._init_handlers(RadarEventType)
        self._init_handlers(TradeEventType)
        self._init_handlers(NotificationEventType)
        self._init_handlers(PerformanceEventType)
        self._init_handlers(TelegramEventType)
        
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
    
    def _init_handlers(self, event_enum: Type[Enum]):
        """특정 이벤트 열거형에 대한 핸들러 초기화"""
        for event_type in event_enum:
            self._handlers[event_type] = []
    
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
            self._logger.info("시스템 이벤트 버스가 이미 실행 중입니다.")
            return
            
        self._running = True
        self._processor_task = asyncio.create_task(self._process_events())
        self._logger.info("✅ 시스템 이벤트 버스가 시작되었습니다. (ID: %s)", id(self))
        self._logger.debug(f"[오더북 핸들러] start() 메서드 호출됨")
    
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
                
        self._logger.info("시스템 이벤트 버스가 종료되었습니다.")
    
    @property
    def is_running(self) -> bool:
        """이벤트 버스 실행 상태 확인"""
        return self._running
    
    def register_handler(self, event_type: Enum, handler: Callable):
        """
        이벤트 핸들러 등록 (subscribe 대신 사용)
        
        Args:
            event_type: 이벤트 유형 (SystemEventType, ObCollectorEventType 등)
            handler: 이벤트 핸들러 함수 (async def handler(data))
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []
            
        if handler not in self._handlers[event_type]:
            self._handlers[event_type].append(handler)
            # 핸들러 함수의 소속 정보 추출
            handler_module = handler.__module__.split('.')[-1]
            handler_name = handler.__qualname__
            self._logger.debug(f"이벤트 핸들러가 등록되었습니다: {event_type.value} → {handler_module}.{handler_name}")
    
    def unregister_handler(self, event_type: Enum, handler: Callable):
        """
        이벤트 핸들러 등록 해제
        
        Args:
            event_type: 이벤트 유형 (SystemEventType, ObCollectorEventType 등)
            handler: 등록 해제할 핸들러 함수
        """
        if event_type in self._handlers and handler in self._handlers[event_type]:
            self._handlers[event_type].remove(handler)
            self._logger.debug(f"이벤트 핸들러가 등록 해제되었습니다: {event_type.value}")
    
    async def publish(self, event_type: Enum, data: Dict[str, Any]):
        """
        이벤트 발행
        
        Args:
            event_type: 이벤트 유형 (SystemEventType, ObCollectorEventType 등)
            data: 이벤트 데이터
        """
        if not self._running:
            self._logger.warning("이벤트 버스가 실행 중이 아닙니다.")
            return
        
        # 타임스탬프만 추가하고 다른 필드는 수정하지 않음
        if "timestamp" not in data:
            event_data = {**data, "timestamp": time.time()}
        else:
            event_data = data
        
        # 이벤트 카테고리 추가 (없는 경우)
        if "category" not in event_data:
            # 이벤트 타입에 따라 카테고리 결정
            if isinstance(event_type, SystemEventType):
                category = EventCategory.SYSTEM
            elif isinstance(event_type, ObCollectorEventType):
                category = EventCategory.OB_COLLECTOR
            elif isinstance(event_type, RadarEventType):
                category = EventCategory.RADAR
            elif isinstance(event_type, TradeEventType):
                category = EventCategory.TRADE
            elif isinstance(event_type, NotificationEventType):
                category = EventCategory.NOTIFICATION
            elif isinstance(event_type, PerformanceEventType):
                category = EventCategory.PERFORMANCE
            elif isinstance(event_type, TelegramEventType):
                category = EventCategory.TELEGRAM
            else:
                category = EventCategory.SYSTEM  # 기본값
                
            event_data["category"] = category.value
        
        # 디버그 로그 추가
        if hasattr(event_type, 'value'):
            event_type_str = event_type.value
        else:
            event_type_str = str(event_type)
        self._logger.debug(f"이벤트 발행: {event_type_str}, 데이터: {event_data}")
        
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
                                    # event_type.value 사용 대신 문자열 안전하게 가져오기
                                    event_type_str = event_type.value if hasattr(event_type, 'value') else str(event_type)
                                    self._logger.error(f"이벤트 핸들러 오류 ({event_type_str}): {str(e)}")
                                    self.stats["errors"] += 1
                        else:
                            # event_type.value 사용 대신 문자열 안전하게 가져오기
                            event_type_str = event_type.value if hasattr(event_type, 'value') else str(event_type)
                            self._logger.debug(f"이벤트 {event_type_str}에 등록된 핸들러가 없습니다.")
                    
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