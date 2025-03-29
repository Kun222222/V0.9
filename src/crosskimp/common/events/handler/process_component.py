"""
프로세스 컴포넌트 기본 클래스

모든 시스템 프로세스 컴포넌트가 상속받는 기본 클래스를 정의합니다.
"""

import asyncio
import traceback
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.system_eventbus import get_event_bus
from crosskimp.common.events.system_types import EventPaths, ProcessStatusEvent
from crosskimp.common.config.common_constants import SystemComponent

# 로거 설정
logger = get_unified_logger()

class ProcessComponent(ABC):
    """
    프로세스 컴포넌트 기본 클래스
    
    모든 시스템 프로세스 컴포넌트가 상속받는 기본 클래스입니다.
    이벤트 버스를 통해 제어 이벤트를 수신하고 상태 이벤트를 발행합니다.
    """
    
    def __init__(self, process_name: str, event_bus=None):
        """
        프로세스 컴포넌트 초기화
        
        Args:
            process_name: 프로세스 이름
            event_bus: 이벤트 버스 인스턴스 (기본값: None, 없으면 자동 가져옴)
        """
        self.process_name = process_name
        self.event_bus = event_bus if event_bus is not None else get_event_bus()
        self.status = EventPaths.PROCESS_STATUS_STOPPED
        self.initialized = False
        self.logger = logger
        self._tasks = []  # 실행 중인 태스크 추적
        
    async def setup(self):
        """
        프로세스 컴포넌트 설정
        
        이벤트 핸들러를 등록하고 초기 설정을 수행합니다.
        프로세스 관련 이벤트를 모두 처리할 수 있도록 등록합니다.
        """
        if self.initialized:
            return
            
        # 계층적 이벤트 경로 핸들러 등록
        self.event_bus.register_handler(
            EventPaths.PROCESS_COMMAND_START,
            self._handle_process_control
        )
        
        self.event_bus.register_handler(
            EventPaths.PROCESS_COMMAND_STOP,
            self._handle_process_control
        )
        
        self.event_bus.register_handler(
            EventPaths.PROCESS_COMMAND_RESTART,
            self._handle_process_control
        )
        
        self.initialized = True
        self.logger.info(f"프로세스 '{self.process_name}' 컴포넌트가 설정되었습니다.")
        
    async def _handle_process_control(self, data: Dict[str, Any]):
        """
        프로세스 제어 이벤트 처리
        
        process/start, process/stop, process/restart 이벤트를 처리합니다.
        발행된 이벤트의 process_name이 자신의 프로세스 이름과 일치할 경우에만 처리합니다.
        
        Args:
            data: 이벤트 데이터
        """
        # 다른 프로세스 이벤트는 무시
        if data.get("process_name") != self.process_name:
            return
            
        # 이벤트 타입 확인
        event_type = data.get("event_type")
        
        try:
            # 명확한 이벤트 타입에 따른 처리
            if event_type == "process/start_requested":
                await self._start_process()
            elif event_type == "process/stop_requested":
                await self._stop_process()
            elif event_type == "process/restart_requested":
                await self._restart_process()
            else:
                self.logger.warning(f"지원되지 않는 이벤트 타입: {event_type}")
        except Exception as e:
            self.logger.error(f"프로세스 '{self.process_name}' 제어 이벤트 처리 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 상태 이벤트 발행
            await self._publish_status(
                EventPaths.PROCESS_STATUS_ERROR,
                error_message=str(e)
            )
    
    def is_running(self) -> bool:
        """
        프로세스가 실행 중인지 확인
        
        Returns:
            bool: 프로세스가 실행 중이면 True, 아니면 False
        """
        return self.status == EventPaths.PROCESS_STATUS_RUNNING
    
    def is_starting(self) -> bool:
        """
        프로세스가 시작 중인지 확인
        
        Returns:
            bool: 프로세스가 시작 중이면 True, 아니면 False
        """
        return self.status == EventPaths.PROCESS_STATUS_STARTING
    
    def is_error(self) -> bool:
        """
        프로세스가 오류 상태인지 확인
        
        Returns:
            bool: 프로세스가 오류 상태이면 True, 아니면 False
        """
        return self.status == EventPaths.PROCESS_STATUS_ERROR
            
    async def _start_process(self):
        """
        프로세스 시작 처리
        """
        if self.status == EventPaths.PROCESS_STATUS_RUNNING:
            self.logger.info(f"프로세스 '{self.process_name}'이(가) 이미 실행 중입니다.")
            return
            
        try:
            # 상태 변경 이벤트 발행
            await self._publish_status(EventPaths.PROCESS_STATUS_STARTING)
            
            # 실제 시작 로직 실행
            success = await self._do_start()
            
            if success:
                # 이 시점에서는 상태를 RUNNING으로 변경하지 않음
                # 상태는 STARTING으로 유지하고, 도메인 로직에서 필요한 시점에
                # _publish_status(RUNNING)을 호출하여 변경하도록 함
                self.logger.info(f"프로세스 '{self.process_name}' 초기화 완료")
            else:
                # 시작 실패 이벤트 발행
                await self._publish_status(
                    EventPaths.PROCESS_STATUS_ERROR,
                    error_message=f"프로세스 '{self.process_name}' 시작 실패"
                )
                self.logger.error(f"프로세스 '{self.process_name}' 시작 실패")
                
        except Exception as e:
            self.logger.error(f"프로세스 '{self.process_name}' 시작 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 상태 이벤트 발행
            await self._publish_status(
                EventPaths.PROCESS_STATUS_ERROR,
                error_message=f"시작 중 오류: {str(e)}"
            )
            
    async def _stop_process(self):
        """
        프로세스 중지 처리
        """
        if self.status == EventPaths.PROCESS_STATUS_STOPPED:
            self.logger.info(f"프로세스 '{self.process_name}'이(가) 이미 중지되었습니다.")
            return
            
        try:
            # 상태 변경 이벤트 발행
            await self._publish_status(EventPaths.PROCESS_STATUS_STOPPING)
            
            # 모든 실행 중인 태스크 취소
            for task in self._tasks:
                if not task.done():
                    task.cancel()
            
            self._tasks.clear()
            
            # 실제 중지 로직 실행
            success = await self._do_stop()
            
            if success:
                # 중지 성공 이벤트 발행
                await self._publish_status(EventPaths.PROCESS_STATUS_STOPPED)
                self.logger.info(f"프로세스 '{self.process_name}'이(가) 중지되었습니다.")
            else:
                # 중지 실패 이벤트 발행
                await self._publish_status(
                    EventPaths.PROCESS_STATUS_ERROR,
                    error_message=f"프로세스 '{self.process_name}' 중지 실패"
                )
                self.logger.error(f"프로세스 '{self.process_name}' 중지 실패")
                
        except Exception as e:
            self.logger.error(f"프로세스 '{self.process_name}' 중지 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 상태 이벤트 발행
            await self._publish_status(
                EventPaths.PROCESS_STATUS_ERROR,
                error_message=f"중지 중 오류: {str(e)}"
            )
            
    async def _restart_process(self):
        """
        프로세스 재시작 처리
        """
        try:
            # 중지 요청
            await self._stop_process()
            
            # 잠시 대기
            await asyncio.sleep(1)
            
            # 시작 요청
            await self._start_process()
            
        except Exception as e:
            self.logger.error(f"프로세스 '{self.process_name}' 재시작 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 상태 이벤트 발행
            await self._publish_status(
                EventPaths.PROCESS_STATUS_ERROR,
                error_message=f"재시작 중 오류: {str(e)}"
            )
            
    async def _publish_status(self, status: str, error_message: Optional[str] = None):
        """
        프로세스 상태 이벤트 발행
        
        프로세스 상태가 변경될 때 process/status 이벤트를 발행합니다.
        오류가 발생한 경우 process/error 이벤트도 함께 발행합니다.
        
        Args:
            status: 새 상태 (EventPaths.PROCESS_STATUS_* 상수 사용)
            error_message: 오류 메시지 (있는 경우)
        """
        self.status = status
        
        # 상태에 따른 이벤트 타입 결정
        event_type = "process/started" if status == EventPaths.PROCESS_STATUS_RUNNING else \
                     "process/stopped" if status == EventPaths.PROCESS_STATUS_STOPPED else \
                     "process/error" if status == EventPaths.PROCESS_STATUS_ERROR else \
                     "process/start_requested" if status == EventPaths.PROCESS_STATUS_STARTING else \
                     "process/stop_requested" if status == EventPaths.PROCESS_STATUS_STOPPING else \
                     None
        
        # ProcessStatusEvent 클래스 사용
        event_data = ProcessStatusEvent(
            process_name=self.process_name,
            status=status,
            event_type=event_type or "unknown",
            error_message=error_message,
            details={}
        )
        
        # 상태 이벤트 발행 (process/status 이벤트로 발행)
        await self.event_bus.publish(EventPaths.PROCESS_STATUS, event_data.__dict__)
        
        # 오류 상태인 경우 오류 이벤트도 발행
        if status == EventPaths.PROCESS_STATUS_ERROR and error_message:
            await self.event_bus.publish(EventPaths.PROCESS_ERROR, event_data.__dict__)
        
    async def start(self) -> bool:
        """
        프로세스 시작 메서드
        
        이 메서드는 외부에서 호출하여 프로세스를 시작합니다.
        상태 관리는 이 메서드에서 자동으로 처리되며, 
        실제 작업은 _do_start 메서드에 위임됩니다.
        
        Returns:
            bool: 시작 요청 처리 성공 여부
        """
        await self._start_process()
        return self.status != EventPaths.PROCESS_STATUS_ERROR
        
    async def stop(self) -> bool:
        """
        프로세스 중지 메서드
        
        이 메서드는 외부에서 호출하여 프로세스를 중지합니다.
        상태 관리는 이 메서드에서 자동으로 처리되며, 
        실제 작업은 _do_stop 메서드에 위임됩니다.
        
        Returns:
            bool: 중지 요청 처리 성공 여부
        """
        await self._stop_process()
        return self.status == EventPaths.PROCESS_STATUS_STOPPED
    
    def add_task(self, task):
        """
        실행 중인 태스크 추가
        
        Args:
            task: 추적할 asyncio 태스크
        """
        self._tasks.append(task)
    
    @abstractmethod
    async def _do_start(self) -> bool:
        """
        프로세스 시작 구현
        
        실제 프로세스 시작 로직을 구현합니다.
        자식 클래스에서는 이 메서드를 구현하여 실제 시작 작업을 수행합니다.
        상태 관리는 부모 클래스에서 자동으로 처리됩니다.
        
        Returns:
            bool: 시작 성공 여부
        """
        pass
        
    @abstractmethod
    async def _do_stop(self) -> bool:
        """
        프로세스 중지 구현
        
        실제 프로세스 중지 로직을 구현합니다.
        자식 클래스에서는 이 메서드를 구현하여 실제 중지 작업을 수행합니다.
        상태 관리는 부모 클래스에서 자동으로 처리됩니다.
        
        Returns:
            bool: 중지 성공 여부
        """
        pass 