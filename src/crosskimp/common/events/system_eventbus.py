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
from crosskimp.common.events.system_types import EventPaths, EventPriority

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
    각 이벤트 경로에 따라 핸들러를 관리합니다.
    """
    
    def __init__(self):
        """이벤트 버스 초기화"""
        # 이벤트 핸들러 저장 (이벤트 경로 -> 핸들러 목록)
        self._handlers: Dict[str, List[Callable]] = {}
        
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
    
    def _init_handlers(self, event_path: str):
        """특정 이벤트 경로에 대한 핸들러 초기화"""
        if event_path not in self._handlers:
            self._handlers[event_path] = []
    
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
    
    def register_handler(self, event_path: str, handler: Callable):
        """이벤트 핸들러 등록"""
        if event_path not in self._handlers:
            self._handlers[event_path] = []
            
        if handler not in self._handlers[event_path]:
            self._handlers[event_path].append(handler)
            # 핸들러 함수 정보 추출
            handler_module = handler.__module__.split('.')[-1]
            handler_name = handler.__qualname__
            # 핸들러 ID 생성 (디버깅 용이성 위해)
            handler_id = id(handler)
            
            # 상세 로깅
            self._logger.info(f"🔌 이벤트 핸들러 등록: {event_path} → {handler_module}.{handler_name} (ID: {handler_id})")
            self._logger.info(f"📋 핸들러 등록 후 상태: 경로={event_path}, 총 핸들러={len(self._handlers[event_path])}")
            
            # 핸들러 목록 로깅
            handler_names = [f"{h.__module__.split('.')[-1]}.{h.__qualname__}" for h in self._handlers[event_path]]
            self._logger.debug(f"📑 '{event_path}' 등록된 모든 핸들러: {handler_names}")
        else:
            self._logger.warning(f"⚠️ 이벤트 핸들러가 이미 등록되어 있습니다: {event_path}")
    
    def unregister_handler(self, event_path: str, handler: Callable):
        """
        이벤트 핸들러 등록 해제
        
        Args:
            event_path: 이벤트 경로 (EventPaths 클래스의 상수)
            handler: 등록 해제할 핸들러 함수
        """
        if event_path in self._handlers and handler in self._handlers[event_path]:
            self._handlers[event_path].remove(handler)
            self._logger.debug(f"이벤트 핸들러가 등록 해제되었습니다: {event_path}")
    
    async def publish(self, event_path: str, data: Dict[str, Any]):
        """
        이벤트 발행
        
        Args:
            event_path: 이벤트 경로 (EventPaths 클래스의 상수)
            data: 이벤트 데이터
        """
        if not self._running:
            self._logger.warning(f"이벤트 버스가 실행 중이 아닙니다. 이벤트 발행 실패: {event_path}")
            return
        
        # 타임스탬프만 추가하고 다른 필드는 수정하지 않음
        if "timestamp" not in data:
            event_data = {**data, "timestamp": time.time()}
        else:
            event_data = data
        
        # 이벤트 경로의 첫 부분을 카테고리로 추가 (없는 경우)
        if "category" not in event_data:
            # 이벤트 경로에서 카테고리 추출 (첫 '/' 이전 부분)
            if "/" in event_path:
                category = event_path.split("/")[0]
            else:
                category = event_path
                
            event_data["category"] = category
        
        # 디버그 로그 추가
        self._logger.debug(f"이벤트 발행: {event_path}, 데이터: {event_data}")
        
        # 이벤트 큐에 추가
        try:
            await self._queue.put((event_path, event_data))
            self.stats["published_events"] += 1
            self._logger.debug(f"이벤트가 큐에 추가됨: {event_path} (큐 크기: {self._queue.qsize()})")
        except Exception as e:
            self._logger.error(f"이벤트 발행 중 오류: {str(e)}", exc_info=True)
            self.stats["errors"] += 1
    
    async def _process_events(self):
        """이벤트 처리 루프"""
        try:
            while self._running:
                try:
                    # 큐에서 이벤트 가져오기
                    event_path, data = await self._queue.get()
                    self._logger.info(f"📨 이벤트 처리 시작: {event_path}")
                    
                    # 핸들러 실행
                    if event_path in self._handlers:
                        handlers = self._handlers[event_path]
                        if handlers:
                            handler_count = len(handlers)
                            self._logger.info(f"📨 이벤트 {event_path}에 등록된 핸들러 {handler_count}개 호출 시작")
                            
                            for idx, handler in enumerate(handlers):
                                try:
                                    # 핸들러 정보 추출
                                    handler_name = getattr(handler, "__qualname__", handler.__name__ if hasattr(handler, "__name__") else "unknown")
                                    handler_module = handler.__module__ if hasattr(handler, "__module__") else "unknown"
                                    
                                    self._logger.info(f"📨 핸들러 호출 ({idx+1}/{handler_count}): {handler_module}.{handler_name}")
                                    await handler(data)
                                    self._logger.info(f"📨 핸들러 호출 완료: {handler_module}.{handler_name}")
                                except Exception as e:
                                    self._logger.error(f"❌ 이벤트 핸들러 오류 ({event_path}): {str(e)}", exc_info=True)
                                    self.stats["errors"] += 1
                        else:
                            self._logger.warning(f"⚠️ 이벤트 {event_path}에 등록된 핸들러가 없습니다.")
                    else:
                        self._logger.warning(f"⚠️ 이벤트 {event_path}에 대한 핸들러가 등록되지 않았습니다.")
                    
                    self.stats["processed_events"] += 1
                    self._logger.info(f"📨 이벤트 처리 완료: {event_path}")
                    
                    # 큐 작업 완료 표시
                    self._queue.task_done()
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._logger.error(f"❌ 이벤트 처리 루프 오류: {str(e)}", exc_info=True)
                    self.stats["errors"] += 1
        except asyncio.CancelledError:
            self._logger.debug("이벤트 처리 루프가 취소되었습니다.")
        except Exception as e:
            self._logger.error(f"❌ 이벤트 처리 루프 치명적 오류: {str(e)}", exc_info=True) 