"""
프로세스 컴포넌트 기본 클래스

모든 프로세스 핸들러의 공통 기능을 제공하는 추상 기본 클래스입니다.
이 클래스는 프로세스 생명주기 관리 및 이벤트 기반 통신을 지원합니다.
"""

import asyncio
import traceback
import time
from typing import Dict, Any, Optional, List
from abc import ABC, abstractmethod

from crosskimp.common.events.system_eventbus import get_event_bus
from crosskimp.common.events.system_types import EventChannels, EventValues, ProcessStatusEvent
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent

class ProcessComponent(ABC):
    """
    프로세스 컴포넌트 추상 기본 클래스
    
    모든 프로세스 핸들러(예: ObCollectorHandler, TraderHandler 등)는 이 클래스를 상속받아 구현해야 합니다.
    """
    
    def __init__(self, process_name: str, event_bus=None, logger_component=None):
        """
        프로세스 컴포넌트 초기화
        
        Args:
            process_name: 프로세스 식별자 (예: "ob_collector", "trader" 등)
            event_bus: 이벤트 버스 인스턴스 (기본값: None, None일 경우 get_event_bus()로 가져옴)
            logger_component: 로거 컴포넌트 명 (기본: SystemComponent.SYSTEM)
        """
        self.process_name = process_name
        self.logger_component = logger_component or SystemComponent.SYSTEM.value
        self.logger = get_unified_logger(component=self.logger_component)
        self.event_bus = event_bus if event_bus is not None else get_event_bus()
        
        # 프로세스 상태 초기화 (기본값: STOPPED)
        self.status = EventValues.PROCESS_STOPPED
        
        # 에러 상태 초기화
        self.last_error = None
        self.error_count = 0
        
        # 이벤트 처리 플래그
        self.is_handling_event = False
        
        self.logger.info(f"프로세스 컴포넌트 초기화: {process_name}")
    
    async def register_event_handlers(self):
        """
        이벤트 핸들러 등록
        
        프로세스 관련 이벤트 핸들러를 등록합니다.
        """
        # 프로세스 시작 명령 이벤트 핸들러 등록
        self.event_bus.register_handler(
            EventChannels.Process.COMMAND_START,
            self._handle_process_command
        )
        
        # 프로세스 중지 명령 이벤트 핸들러 등록
        self.event_bus.register_handler(
            EventChannels.Process.COMMAND_STOP,
            self._handle_process_command
        )
        
        # 프로세스 재시작 명령 이벤트 핸들러 등록
        self.event_bus.register_handler(
            EventChannels.Process.COMMAND_RESTART,
            self._handle_process_command
        )
        
        self.logger.info(f"이벤트 핸들러가 등록되었습니다: {self.process_name}")
    
    async def _handle_process_command(self, data):
        """
        프로세스 명령 이벤트 핸들러
        
        프로세스 명령(시작/중지/재시작) 이벤트를 처리합니다.
        """
        try:
            # 이미 이벤트 처리 중인 경우 무시
            if self.is_handling_event:
                self.logger.warning(f"이미 이벤트 처리 중입니다. 무시됨: {data}")
                return
            
            # 이벤트 처리 플래그 설정
            self.is_handling_event = True
            
            # 이벤트 경로 확인
            event_path = data.get("_event_path", "")
            target_process = data.get("process_name", "all")
            
            self.logger.info(f"[디버깅] 명령 이벤트 수신: {event_path}, 대상 프로세스: {target_process}, 현재 프로세스: {self.process_name}")
            
            # 특정 프로세스 대상 명령이고 현재 프로세스가 대상이 아닌 경우 무시
            if target_process != "all" and target_process != self.process_name:
                self.logger.debug(f"[디버깅] 현재 프로세스({self.process_name})가 대상({target_process})이 아니므로 무시")
                self.is_handling_event = False
                return
            
            # 이벤트 타입에 따라 처리
            if EventChannels.Process.COMMAND_START in event_path:
                self.logger.info(f"[디버깅] {self.process_name} 프로세스 시작 명령 처리 시작")
                await self.start()
                self.logger.info(f"[디버깅] {self.process_name} 프로세스 시작 명령 처리 완료")
            elif EventChannels.Process.COMMAND_STOP in event_path:
                self.logger.info(f"[디버깅] {self.process_name} 프로세스 중지 명령 처리 시작")
                await self.stop()
                self.logger.info(f"[디버깅] {self.process_name} 프로세스 중지 명령 처리 완료")
            elif EventChannels.Process.COMMAND_RESTART in event_path:
                self.logger.info(f"[디버깅] {self.process_name} 프로세스 재시작 명령 처리 시작")
                await self.restart()
                self.logger.info(f"[디버깅] {self.process_name} 프로세스 재시작 명령 처리 완료")
                
        except Exception as e:
            error_msg = f"프로세스 명령 처리 중 오류: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            await self._publish_status(
                EventValues.PROCESS_ERROR,
                error_message=f"{error_msg}\n{traceback.format_exc()}"
            )
        finally:
            # 이벤트 처리 플래그 해제
            self.is_handling_event = False
    
    @property
    def is_running(self) -> bool:
        """프로세스가 실행 중인지 확인"""
        return self.status == EventValues.PROCESS_RUNNING
    
    @property
    def is_starting(self) -> bool:
        """프로세스가 시작 중인지 확인"""
        return self.status == EventValues.PROCESS_STARTING
    
    @property
    def is_error(self) -> bool:
        """프로세스가 오류 상태인지 확인"""
        return self.status == EventValues.PROCESS_ERROR
        
    async def start(self) -> bool:
        """
        프로세스 시작
        
        이미 실행 중인 경우 무시됩니다.
        
        Returns:
            bool: 시작 성공 여부
        """
        try:
            if self.status == EventValues.PROCESS_RUNNING:
                self.logger.info(f"프로세스가 이미 실행 중입니다: {self.process_name}")
                return True
                
            self.logger.info(f"프로세스 시작 중: {self.process_name}")
            
            # 시작 중 상태로 변경
            self.logger.info(f"[디버깅] {self.process_name} 프로세스 상태를 STARTING으로 변경")
            await self._publish_status(EventValues.PROCESS_STARTING)
            
            # 하위 클래스 구현 호출
            self.logger.info(f"[디버깅] {self.process_name} 프로세스 _do_start 메서드 호출")
            success = await self._do_start()
            self.logger.info(f"[디버깅] {self.process_name} _do_start 결과: {success}")
            
            if success:
                self.logger.info(f"프로세스 시작 완료: {self.process_name}")
                return True
            else:
                await self._publish_status(
                    EventValues.PROCESS_ERROR,
                    error_message=f"프로세스 시작 실패: {self.process_name}"
                )
                return False
                
        except Exception as e:
            error_msg = f"프로세스 시작 중 오류: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            await self._publish_status(
                EventValues.PROCESS_ERROR,
                error_message=f"{error_msg}\n{traceback.format_exc()}"
            )
            return False
            
    async def stop(self) -> bool:
        """
        프로세스 중지
        
        이미 중지된 경우 무시됩니다.
        
        Returns:
            bool: 중지 성공 여부
        """
        try:
            if self.status == EventValues.PROCESS_STOPPED:
                self.logger.info(f"프로세스가 이미 중지되었습니다: {self.process_name}")
                return True
                
            self.logger.info(f"프로세스 중지 중: {self.process_name}")
            
            # 종료 중 상태로 변경
            await self._publish_status(EventValues.PROCESS_STOPPING)
            
            # 하위 클래스 구현 호출
            success = await self._do_stop()
            
            if success:
                self.logger.info(f"프로세스 중지 완료: {self.process_name}")
                # 중지 완료 상태로 변경
                await self._publish_status(EventValues.PROCESS_STOPPED)
                return True
            else:
                await self._publish_status(
                    EventValues.PROCESS_ERROR,
                    error_message=f"프로세스 중지 실패: {self.process_name}"
                )
                return False
                
        except Exception as e:
            error_msg = f"프로세스 중지 중 오류: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            await self._publish_status(
                EventValues.PROCESS_ERROR,
                error_message=f"{error_msg}\n{traceback.format_exc()}"
            )
            return False
    
    async def restart(self) -> bool:
        """
        프로세스 재시작
        
        중지 후 시작을 수행합니다.
        
        Returns:
            bool: 재시작 성공 여부
        """
        self.logger.info(f"프로세스 재시작 중: {self.process_name}")
        
        # 중지
        await self.stop()
        
        # 잠시 대기
        await asyncio.sleep(1)
        
        # 시작
        return await self.start()
    
    @property
    def is_healthy(self) -> bool:
        """
        프로세스 건강 상태 확인
        
        기본 구현은 단순히 오류 상태가 아닌지 확인합니다.
        복잡한 건강 체크가 필요한 경우 하위 클래스에서 재정의할 수 있습니다.
        
        Returns:
            bool: 프로세스 정상 여부
        """
        return self.status != EventValues.PROCESS_ERROR
    
    @property
    def is_stopped(self) -> bool:
        """
        프로세스가 중지 상태인지 확인
        
        Returns:
            bool: 중지 상태 여부
        """
        return self.status == EventValues.PROCESS_STOPPED
    
    async def _publish_status(self, status: str, error_message: Optional[str] = None):
        """
        프로세스 상태 이벤트 발행
        
        프로세스 상태가 변경될 때 process/status 이벤트를 발행합니다.
        오류가 발생한 경우에도 동일한 채널을 통해 전달합니다.
        
        Args:
            status: 새 상태 (EventValues.PROCESS_* 상수 사용)
            error_message: 오류 메시지 (있는 경우)
        """
        self.logger.info(f"[디버깅] {self.process_name} 프로세스 상태 변경: {self.status} -> {status}")
        self.status = status
        
        # ProcessStatusEvent 클래스 사용 - 단순화됨
        event_data = ProcessStatusEvent(
            process_name=self.process_name,
            status=status,
            event_type="process/status", # 단순화된 이벤트 타입
            error_message=error_message,
            details={}
        )
        
        # 상태 이벤트 발행
        self.logger.info(f"[디버깅] {self.process_name} 프로세스 상태 이벤트 발행: {status}")
        await self.event_bus.publish(EventChannels.Process.STATUS, event_data.__dict__)
        self.logger.info(f"[디버깅] {self.process_name} 프로세스 상태 이벤트 발행 완료: {status}")
        
        # 주석 처리: 오류 이벤트는 더 이상 별도로 발행하지 않음
        # 오류 상태인 경우 오류 이벤트도 발행
        if status == EventValues.PROCESS_ERROR and error_message:
            await self.event_bus.publish(EventChannels.Process.ERROR, event_data.__dict__)
        
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