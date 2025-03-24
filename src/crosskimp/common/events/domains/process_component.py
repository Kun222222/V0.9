"""
프로세스 컴포넌트 기본 클래스

모든 시스템 프로세스 컴포넌트가 상속받는 기본 클래스를 정의합니다.
"""

import asyncio
import traceback
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events import get_event_bus
from crosskimp.common.events.system_eventbus import EventType
from crosskimp.common.events.domains.process_types import ProcessStatus, ProcessEvent, ProcessEventData

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
        self.status = ProcessStatus.STOPPED
        self.initialized = False
        self.logger = logger
        
    async def setup(self):
        """
        프로세스 컴포넌트 설정
        
        이벤트 핸들러를 등록하고 초기 설정을 수행합니다.
        """
        if self.initialized:
            return
            
        # 이벤트 버스에 제어 이벤트 핸들러 등록
        self.event_bus.register_handler(
            EventType.PROCESS_CONTROL,
            self._handle_process_control
        )
        
        self.initialized = True
        self.logger.info(f"프로세스 '{self.process_name}' 컴포넌트가 설정되었습니다.")
        
    async def _handle_process_control(self, data: Dict[str, Any]):
        """
        프로세스 제어 이벤트 처리
        
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
            # 명확한 이벤트 타입에 따른 처리
            if event_type == ProcessEvent.START_REQUESTED.value:
                self.logger.debug(f"프로세스 '{self.process_name}' 시작 요청 처리 시작")
                await self._start_process()
            elif event_type == ProcessEvent.STOP_REQUESTED.value:
                self.logger.debug(f"프로세스 '{self.process_name}' 종료 요청 처리 시작")
                await self._stop_process()
            elif event_type == ProcessEvent.RESTART_REQUESTED.value:
                self.logger.debug(f"프로세스 '{self.process_name}' 재시작 요청 처리 시작")
                await self._restart_process()
            else:
                self.logger.warning(f"지원되지 않는 이벤트 타입: {event_type}")
        except Exception as e:
            self.logger.error(f"프로세스 '{self.process_name}' 제어 이벤트 처리 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 상태 이벤트 발행
            await self._publish_status(
                ProcessStatus.ERROR,
                error_message=str(e)
            )
            
    async def _start_process(self):
        """
        프로세스 시작 처리
        """
        if self.status == ProcessStatus.RUNNING:
            self.logger.warning(f"프로세스 '{self.process_name}'이(가) 이미 실행 중입니다.")
            return
            
        try:
            # 상태 변경 이벤트 발행
            self.logger.debug(f"프로세스 '{self.process_name}' 상태를 STARTING으로 변경")
            await self._publish_status(ProcessStatus.STARTING)
            
            # 실제 시작 로직 실행
            self.logger.debug(f"프로세스 '{self.process_name}' start() 메서드 호출")
            success = await self.start()
            self.logger.debug(f"프로세스 '{self.process_name}' start() 결과: {success}")
            
            if success:
                # 시작 성공 이벤트 발행
                self.logger.debug(f"프로세스 '{self.process_name}' 성공적으로 시작됨")
                await self._publish_status(ProcessStatus.RUNNING)
                self.logger.info(f"프로세스 '{self.process_name}'이(가) 시작되었습니다.")
            else:
                # 시작 실패 이벤트 발행
                self.logger.debug(f"프로세스 '{self.process_name}' 시작 실패")
                await self._publish_status(
                    ProcessStatus.ERROR,
                    error_message=f"프로세스 '{self.process_name}' 시작 실패"
                )
                self.logger.error(f"프로세스 '{self.process_name}' 시작 실패")
                
        except Exception as e:
            self.logger.error(f"프로세스 '{self.process_name}' 시작 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 상태 이벤트 발행
            await self._publish_status(
                ProcessStatus.ERROR,
                error_message=f"시작 중 오류: {str(e)}"
            )
            
    async def _stop_process(self):
        """
        프로세스 중지 처리
        """
        if self.status == ProcessStatus.STOPPED:
            self.logger.warning(f"프로세스 '{self.process_name}'이(가) 이미 중지되었습니다.")
            return
            
        try:
            # 상태 변경 이벤트 발행
            await self._publish_status(ProcessStatus.STOPPING)
            
            # 실제 중지 로직 실행
            success = await self.stop()
            
            if success:
                # 중지 성공 이벤트 발행
                await self._publish_status(ProcessStatus.STOPPED)
                self.logger.info(f"프로세스 '{self.process_name}'이(가) 중지되었습니다.")
            else:
                # 중지 실패 이벤트 발행
                await self._publish_status(
                    ProcessStatus.ERROR,
                    error_message=f"프로세스 '{self.process_name}' 중지 실패"
                )
                self.logger.error(f"프로세스 '{self.process_name}' 중지 실패")
                
        except Exception as e:
            self.logger.error(f"프로세스 '{self.process_name}' 중지 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 상태 이벤트 발행
            await self._publish_status(
                ProcessStatus.ERROR,
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
                ProcessStatus.ERROR,
                error_message=f"재시작 중 오류: {str(e)}"
            )
            
    async def _publish_status(self, status: ProcessStatus, error_message: Optional[str] = None):
        """
        프로세스 상태 이벤트 발행
        
        Args:
            status: 새 상태
            error_message: 오류 메시지 (있는 경우)
        """
        self.status = status
        
        # 상태에 따른 이벤트 타입 결정
        event_type = ProcessEvent.STARTED if status == ProcessStatus.RUNNING else \
                     ProcessEvent.STOPPED if status == ProcessStatus.STOPPED else \
                     ProcessEvent.ERROR if status == ProcessStatus.ERROR else \
                     ProcessEvent.START_REQUESTED if status == ProcessStatus.STARTING else \
                     ProcessEvent.STOP_REQUESTED if status == ProcessStatus.STOPPING else \
                     None
        
        # 이벤트 데이터 생성
        event_data = ProcessEventData(
            process_name=self.process_name,
            event_type=event_type,
            status=status,
            error_message=error_message
        ).to_dict()
        
        # 상태 이벤트 발행
        await self.event_bus.publish(EventType.PROCESS_STATUS, event_data)
        
    @abstractmethod
    async def start(self) -> bool:
        """
        프로세스 시작 구현
        
        실제 프로세스 시작 로직을 구현합니다.
        
        Returns:
            bool: 시작 성공 여부
        """
        pass
        
    @abstractmethod
    async def stop(self) -> bool:
        """
        프로세스 중지 구현
        
        실제 프로세스 중지 로직을 구현합니다.
        
        Returns:
            bool: 중지 성공 여부
        """
        pass 