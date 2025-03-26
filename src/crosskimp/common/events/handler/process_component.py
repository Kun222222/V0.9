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
from crosskimp.common.events.system_types import EventPaths
from crosskimp.common.config.common_constants import SystemComponent

# 로거 설정
logger = get_unified_logger()

class ProcessComponent(ABC):
    """
    프로세스 컴포넌트 기본 클래스
    
    모든 시스템 프로세스 컴포넌트가 상속받는 기본 클래스입니다.
    이벤트 버스를 통해 제어 이벤트를 수신하고 상태 이벤트를 발행합니다.
    """
    
    def __init__(self, process_name: str):
        """
        프로세스 컴포넌트 초기화
        
        Args:
            process_name: 프로세스 이름
        """
        self.process_name = process_name
        self.event_bus = get_event_bus()
        self.status = EventPaths.PROCESS_STATUS_STOPPED  # ProcessStatus.STOPPED 대신 직접 상수 사용
        self.initialized = False
        self.logger = logger
        
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
            EventPaths.PROCESS_START,
            self._handle_process_control
        )
        
        self.event_bus.register_handler(
            EventPaths.PROCESS_STOP,
            self._handle_process_control
        )
        
        self.event_bus.register_handler(
            EventPaths.PROCESS_RESTART,
            self._handle_process_control
        )
        
        self.initialized = True
        self.logger.info(f"프로세스 '{self.process_name}' 컴포넌트가 설정되었습니다.")
        
    async def _handle_process_control(self, data: Dict[str, Any]):
        """
        프로세스 제어 이벤트 처리
        
        process/start, process/stop, process/restart, process/control 이벤트를 처리합니다.
        발행된 이벤트의 process_name이 자신의 프로세스 이름과 일치할 경우에만 처리합니다.
        
        Args:
            data: 이벤트 데이터
        """
        # 디버그 로그 추가
        self.logger.debug(f"프로세스 제어 이벤트 수신: {data}")
        
        # 다른 프로세스 이벤트는 무시
        if data.get("process_name") != self.process_name:
            self.logger.debug(f"다른 프로세스 이벤트 무시: 요청={data.get('process_name')}, 현재={self.process_name}")
            return
            
        # 이벤트 타입 확인
        event_type = data.get("event_type")
        self.logger.debug(f"이벤트 타입: {event_type}")
        
        try:
            # 명확한 이벤트 타입에 따른 처리 (ProcessEvent 대신 직접 상수 비교)
            if event_type == EventPaths.PROCESS_EVENT_START_REQUESTED:
                self.logger.debug(f"프로세스 '{self.process_name}' 시작 요청 처리 시작")
                await self._start_process()
            elif event_type == EventPaths.PROCESS_EVENT_STOP_REQUESTED:
                self.logger.debug(f"프로세스 '{self.process_name}' 종료 요청 처리 시작")
                await self._stop_process()
            elif event_type == EventPaths.PROCESS_EVENT_RESTART_REQUESTED:
                self.logger.debug(f"프로세스 '{self.process_name}' 재시작 요청 처리 시작")
                await self._restart_process()
            else:
                self.logger.warning(f"지원되지 않는 이벤트 타입: {event_type}")
        except Exception as e:
            self.logger.error(f"프로세스 '{self.process_name}' 제어 이벤트 처리 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 상태 이벤트 발행
            await self._publish_status(
                EventPaths.PROCESS_STATUS_ERROR,  # ProcessStatus.ERROR 대신 직접 상수 사용
                error_message=str(e)
            )
            
    async def _start_process(self):
        """
        프로세스 시작 처리
        """
        if self.status == EventPaths.PROCESS_STATUS_RUNNING:  # ProcessStatus.RUNNING 대신 직접 상수 사용
            self.logger.warning(f"프로세스 '{self.process_name}'이(가) 이미 실행 중입니다.")
            return
            
        try:
            # 상태 변경 이벤트 발행
            self.logger.debug(f"프로세스 '{self.process_name}' 상태를 STARTING으로 변경")
            await self._publish_status(EventPaths.PROCESS_STATUS_STARTING)  # ProcessStatus.STARTING 대신 직접 상수 사용
            
            # 실제 시작 로직 실행
            self.logger.debug(f"프로세스 \1 _do__do_start() 메서드 호출")
            success = await self._do_start()
            self.logger.debug(f"프로세스 '{self.process_name}' _do_start() 결과: {success}")
            
            if success:
                # 시작 성공 이벤트 발행
                self.logger.debug(f"프로세스 '{self.process_name}' 성공적으로 시작됨")
                await self._publish_status(EventPaths.PROCESS_STATUS_RUNNING)  # ProcessStatus.RUNNING 대신 직접 상수 사용
                self.logger.info(f"프로세스 '{self.process_name}'이(가) 시작되었습니다.")
            else:
                # 시작 실패 이벤트 발행
                self.logger.debug(f"프로세스 '{self.process_name}' 시작 실패")
                await self._publish_status(
                    EventPaths.PROCESS_STATUS_ERROR,  # ProcessStatus.ERROR 대신 직접 상수 사용
                    error_message=f"프로세스 '{self.process_name}' 시작 실패"
                )
                self.logger.error(f"프로세스 '{self.process_name}' 시작 실패")
                
        except Exception as e:
            self.logger.error(f"프로세스 '{self.process_name}' 시작 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 상태 이벤트 발행
            await self._publish_status(
                EventPaths.PROCESS_STATUS_ERROR,  # ProcessStatus.ERROR 대신 직접 상수 사용
                error_message=f"시작 중 오류: {str(e)}"
            )
            
    async def _stop_process(self):
        """
        프로세스 중지 처리
        """
        if self.status == EventPaths.PROCESS_STATUS_STOPPED:  # ProcessStatus.STOPPED 대신 직접 상수 사용
            self.logger.warning(f"프로세스 '{self.process_name}'이(가) 이미 중지되었습니다.")
            return
            
        try:
            # 상태 변경 이벤트 발행
            await self._publish_status(EventPaths.PROCESS_STATUS_STOPPING)  # ProcessStatus.STOPPING 대신 직접 상수 사용
            
            # 실제 중지 로직 실행
            success = await self._do_stop()
            
            if success:
                # 중지 성공 이벤트 발행
                await self._publish_status(EventPaths.PROCESS_STATUS_STOPPED)  # ProcessStatus.STOPPED 대신 직접 상수 사용
                self.logger.info(f"프로세스 '{self.process_name}'이(가) 중지되었습니다.")
            else:
                # 중지 실패 이벤트 발행
                await self._publish_status(
                    EventPaths.PROCESS_STATUS_ERROR,  # ProcessStatus.ERROR 대신 직접 상수 사용
                    error_message=f"프로세스 '{self.process_name}' 중지 실패"
                )
                self.logger.error(f"프로세스 '{self.process_name}' 중지 실패")
                
        except Exception as e:
            self.logger.error(f"프로세스 '{self.process_name}' 중지 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 상태 이벤트 발행
            await self._publish_status(
                EventPaths.PROCESS_STATUS_ERROR,  # ProcessStatus.ERROR 대신 직접 상수 사용
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
                EventPaths.PROCESS_STATUS_ERROR,  # ProcessStatus.ERROR 대신 직접 상수 사용
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
        event_type = EventPaths.PROCESS_EVENT_STARTED if status == EventPaths.PROCESS_STATUS_RUNNING else \
                     EventPaths.PROCESS_EVENT_STOPPED if status == EventPaths.PROCESS_STATUS_STOPPED else \
                     EventPaths.PROCESS_EVENT_ERROR if status == EventPaths.PROCESS_STATUS_ERROR else \
                     EventPaths.PROCESS_EVENT_START_REQUESTED if status == EventPaths.PROCESS_STATUS_STARTING else \
                     EventPaths.PROCESS_EVENT_STOP_REQUESTED if status == EventPaths.PROCESS_STATUS_STOPPING else \
                     None
        
        # 이벤트 데이터 생성 (ProcessEventData 클래스 대신 직접 딕셔너리 사용)
        event_data = {
            "process_name": self.process_name,
            "event_type": event_type,
            "status": status,
            "error_message": error_message,
            "details": {}
        }
        
        # 상태 이벤트 발행 (process/status 이벤트로 발행)
        await self.event_bus.publish(EventPaths.PROCESS_STATUS, event_data)
        
        # 오류 상태인 경우 오류 이벤트도 발행
        if status == EventPaths.PROCESS_STATUS_ERROR and error_message:
            await self.event_bus.publish(EventPaths.PROCESS_ERROR, event_data)
        
        # 컴포넌트별 상태 이벤트도 추가로 발행 (컴포넌트별 이벤트 핸들링을 위함)
        # 컴포넌트 이름에 따라 적절한 이벤트 경로 선택
        if self.process_name == "ob_collector":
            if status == EventPaths.PROCESS_STATUS_RUNNING:
                await self.event_bus.publish(EventPaths.OB_COLLECTOR_START, event_data)
            elif status == EventPaths.PROCESS_STATUS_STOPPED:
                await self.event_bus.publish(EventPaths.OB_COLLECTOR_STOP, event_data)
            elif status == EventPaths.PROCESS_STATUS_ERROR:
                await self.event_bus.publish(EventPaths.OB_COLLECTOR_ERROR, event_data)
        elif self.process_name == "radar":
            if status == EventPaths.PROCESS_STATUS_RUNNING:
                await self.event_bus.publish(EventPaths.RADAR_START, event_data)
            elif status == EventPaths.PROCESS_STATUS_STOPPED:
                await self.event_bus.publish(EventPaths.RADAR_STOP, event_data)
        # 추가 컴포넌트는 필요에 따라 여기에 추가
        
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
        return self.status == EventPaths.PROCESS_STATUS_RUNNING  # ProcessStatus.RUNNING 대신 직접 상수 사용
        
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
        return self.status == EventPaths.PROCESS_STATUS_STOPPED  # ProcessStatus.STOPPED 대신 직접 상수 사용
    
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