"""
오케스트레이터 모듈

시스템의 전체 생명주기를 관리하고 프로세스들을 조율합니다.
프로세스 관리 및 명령 처리 기능을 포함합니다.
"""

import asyncio
import uuid
from typing import Dict, Optional, Set, Any, Callable, List
from datetime import datetime
import traceback
import os
import time

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.events import get_event_bus
from crosskimp.common.events.system_eventbus import EventType
from crosskimp.common.events.domains.process_types import ProcessStatus, ProcessEvent, ProcessEventData
from crosskimp.common.config.app_config import get_config

logger = get_unified_logger(component=SystemComponent.SYSTEM.value)

class Orchestrator:
    """시스템 오케스트레이터 및 명령 처리기"""
    
    def __init__(self):
        """오케스트레이터 초기화"""
        self.event_bus = None
        self.initialized = False
        self.shutting_down = False
        
        # 프로세스 의존성 정의
        self.process_dependencies = {
            "orderbook": set(),  # 오더북은 독립적
            "telegram": set(),  # 텔레그램 커맨더는 독립적
            "monitoring": {"orderbook"},  # 모니터링은 오더북에 의존
            "data_collector": {"orderbook"},  # 데이터 수집기는 오더북에 의존
            "trade_executor": {"orderbook", "data_collector"}  # 거래 실행기는 오더북과 데이터 수집기에 의존
        }
        
        # 프로세스 실행 상태 추적
        self.running_processes = set()
        
        # 명령 핸들러 관련 속성
        self._command_handlers = {}
        self._pending_responses = {}
        
    async def initialize(self):
        """오케스트레이터 초기화"""
        try:
            # 이벤트 버스 초기화
            self.event_bus = get_event_bus()
            
            # 이벤트 핸들러 등록
            self.event_bus.register_handler(
                EventType.COMMAND,
                self._process_command
            )
            
            # 프로세스 상태 이벤트 구독
            self.event_bus.register_handler(
                EventType.PROCESS_STATUS,
                self._handle_process_status
            )
            
            # 표준 명령 핸들러 등록
            self._register_standard_handlers()
            
            # 프로세스 컴포넌트 초기화
            await self._initialize_process_components()
            
            self.initialized = True
            logger.info("오케스트레이터가 초기화되었습니다.")
            
        except Exception as e:
            logger.error(f"오케스트레이터 초기화 중 오류 발생: {str(e)}")
            raise
        
    async def _initialize_process_components(self):
        """프로세스 컴포넌트 초기화"""
        try:
            logger.info("프로세스 컴포넌트 초기화 중...")
            
            # 오더북 프로세스 핸들러 초기화
            from crosskimp.common.events.handler import initialize_orderbook_process
            await initialize_orderbook_process()
            logger.info("오더북 프로세스 핸들러가 초기화되었습니다.")
            
            # 추가 프로세스 핸들러 초기화 (필요에 따라 추가)
            # ...
            
            logger.info("프로세스 컴포넌트 초기화 완료")
        except Exception as e:
            logger.error(f"프로세스 컴포넌트 초기화 중 오류 발생: {str(e)}")
            raise
        
    async def start(self):
        """시스템 시작"""
        if not self.initialized:
            await self.initialize()
            
        try:
            logger.info("시스템 시작 중...")
            
            # 텔레그램 커맨더 실행
            try:
                from crosskimp.services.telegram_commander import get_telegram_commander
                
                telegram = get_telegram_commander()
                # 텔레그램 커맨더 시작
                await telegram.start()
                logger.info("텔레그램 커맨더 시작 요청 완료")
            except Exception as e:
                logger.error(f"텔레그램 커맨더 시작 중 오류 발생: {str(e)}")
                logger.error(traceback.format_exc())
                logger.warning("텔레그램 커맨더 없이 계속 진행합니다.")
            
            # 프로세스 의존성을 고려하여 시작
            for process_name in self._get_process_start_order():
                # 텔레그램은 이미 시작했으므로 건너뜀
                if process_name == "telegram":
                    continue
                    
                # 시작 요청 이벤트 발행
                await self.start_process(process_name)
            
            logger.info("시스템이 시작되었습니다.")
            
        except Exception as e:
            logger.error(f"시스템 시작 중 오류 발생: {str(e)}")
            raise
            
    async def shutdown_system(self):
        """시스템 종료"""
        if self.shutting_down:
            return
            
        self.shutting_down = True
        logger.info("시스템 종료를 시작합니다...")
        
        try:
            # 의존성을 고려하여 프로세스 중지 (의존성 역순으로)
            stop_order = list(reversed(self._get_process_start_order()))
            
            for process_name in stop_order:
                if process_name in self.running_processes:
                    await self.stop_process(process_name)
                
            logger.info("시스템이 종료되었습니다.")
            
        except Exception as e:
            logger.error(f"시스템 종료 중 오류: {str(e)}")
            
        finally:
            self.shutting_down = False
            
    async def _handle_process_status(self, data: Dict):
        """
        프로세스 상태 변경 이벤트 처리
        
        Args:
            data: 이벤트 데이터
        """
        try:
            process_name = data.get("process_name")
            status = data.get("status")
            
            if not process_name or not status:
                logger.warning(f"프로세스 상태 이벤트 데이터 불완전: {data}")
                return
            
            # 상태에 따라 running_processes 집합 업데이트
            if status == ProcessStatus.RUNNING.value:
                self.running_processes.add(process_name)
                logger.info(f"프로세스 '{process_name}'이(가) 실행 중 상태로 변경되었습니다.")
            elif status == ProcessStatus.STOPPED.value:
                self.running_processes.discard(process_name)
                logger.info(f"프로세스 '{process_name}'이(가) 중지 상태로 변경되었습니다.")
            elif status == ProcessStatus.ERROR.value:
                self.running_processes.discard(process_name)
                error_msg = data.get("error_message", "알 수 없는 오류")
                logger.error(f"프로세스 '{process_name}' 오류 발생: {error_msg}")
            
        except Exception as e:
            logger.error(f"프로세스 상태 이벤트 처리 중 오류: {str(e)}")
            logger.error(traceback.format_exc())
            
    async def start_process(self, process_name: str):
        """
        프로세스 시작
        
        이벤트 버스를 통해 프로세스 시작 요청을 발행합니다.
        
        Args:
            process_name: 프로세스 이름
            
        Returns:
            bool: 요청 성공 여부 (실제 시작 성공 여부가 아님)
        """
        # 프로세스가 등록되어 있는지 확인
        if process_name not in self.process_dependencies:
            logger.error(f"프로세스 '{process_name}'이(가) 등록되지 않았습니다.")
            return False
            
        # 이미 실행 중인지 확인
        if process_name in self.running_processes:
            logger.warning(f"프로세스 '{process_name}'이(가) 이미 실행 중입니다.")
            return True
            
        try:
            # 이벤트 데이터 생성
            logger.debug(f"프로세스 '{process_name}' 시작 요청 이벤트 데이터 생성")
            event_data = ProcessEventData(
                process_name=process_name,
                event_type=ProcessEvent.START_REQUESTED,
                status=ProcessStatus.STARTING
            ).to_dict()
            
            # 디버그 로그로 이벤트 데이터 확인
            logger.debug(f"이벤트 데이터: {event_data}")
            
            # 프로세스 시작 요청 이벤트 발행
            logger.info(f"프로세스 '{process_name}' 시작 요청을 발행합니다.")
            await self.event_bus.publish(EventType.PROCESS_CONTROL, event_data)
            
            # 요청 성공으로 간주 (실제 시작은 비동기적으로 처리)
            return True
            
        except Exception as e:
            logger.error(f"프로세스 '{process_name}' 시작 요청 중 오류 발생: {str(e)}")
            logger.error(traceback.format_exc())
            return False
        
    async def stop_process(self, process_name: str):
        """
        프로세스 중지
        
        이벤트 버스를 통해 프로세스 중지 요청을 발행합니다.
        
        Args:
            process_name: 프로세스 이름
            
        Returns:
            bool: 요청 성공 여부 (실제 중지 성공 여부가 아님)
        """
        # 프로세스가 등록되어 있는지 확인
        if process_name not in self.process_dependencies:
            logger.error(f"프로세스 '{process_name}'이(가) 등록되지 않았습니다.")
            return False
            
        # 실행 중이 아니면 무시
        if process_name not in self.running_processes:
            logger.warning(f"프로세스 '{process_name}'이(가) 실행 중이 아닙니다.")
            return True
            
        try:
            # 이벤트 데이터 생성
            logger.debug(f"프로세스 '{process_name}' 중지 요청 이벤트 데이터 생성")
            event_data = ProcessEventData(
                process_name=process_name,
                event_type=ProcessEvent.STOP_REQUESTED,
                status=ProcessStatus.STOPPING
            ).to_dict()
            
            # 디버그 로그로 이벤트 데이터 확인
            logger.debug(f"이벤트 데이터: {event_data}")
            
            # 프로세스 중지 요청 이벤트 발행
            logger.info(f"프로세스 '{process_name}' 중지 요청을 발행합니다.")
            await self.event_bus.publish(EventType.PROCESS_CONTROL, event_data)
            
            # 요청 성공으로 간주 (실제 중지는 비동기적으로 처리)
            return True
            
        except Exception as e:
            logger.error(f"프로세스 '{process_name}' 중지 요청 중 오류 발생: {str(e)}")
            logger.error(traceback.format_exc())
            return False
        
    def is_process_running(self, process_name: str) -> bool:
        """
        프로세스가 실행 중인지 확인
        
        Args:
            process_name: 프로세스 이름
            
        Returns:
            bool: 실행 중이면 True, 아니면 False
        """
        return process_name in self.running_processes
        
    def is_process_registered(self, process_name: str) -> bool:
        """
        프로세스가 등록되어 있는지 확인
        
        Args:
            process_name: 프로세스 이름
            
        Returns:
            bool: 프로세스가 등록되어 있으면 True, 아니면 False
        """
        return process_name in self.process_dependencies
            
    #
    # 명령 처리 관련 메서드
    #
    
    def _register_standard_handlers(self):
        """기본 명령 핸들러 등록"""
        self.register_command_handler("start_process", self._handle_start_process)
        self.register_command_handler("stop_process", self._handle_stop_process)
        self.register_command_handler("restart_process", self._handle_restart_process)
        logger.debug("기본 명령 핸들러가 등록되었습니다.")
    
    def register_command_handler(self, command: str, handler: Callable):
        """
        명령 핸들러 등록
        
        Args:
            command: 명령 이름
            handler: 핸들러 함수 (coroutine)
        """
        self._command_handlers[command] = handler
        logger.debug(f"명령 핸들러 등록: {command}")
    
    async def _process_command(self, data: Dict):
        """
        명령 이벤트 처리
        
        Args:
            data: 명령 이벤트 데이터
        """
        if not isinstance(data, dict):
            return
            
        # 응답 메시지인 경우 처리
        if data.get("is_response", False):
            await self._handle_command_response(data)
            return
            
        command = data.get("command")
        args = data.get("args", {})
        source = data.get("source", "unknown")
        request_id = data.get("request_id")
        
        if not command:
            return
            
        logger.info(f"명령 수신: {command} (소스: {source})")
        
        # 명령 핸들러 실행
        handler = self._command_handlers.get(command)
        if handler:
            try:
                result = await handler(args)
                
                # 요청 ID가 있으면 응답 전송
                if request_id:
                    await self._send_command_response(command, result, request_id)
                    
            except Exception as e:
                logger.error(f"명령 '{command}' 처리 중 오류: {str(e)}")
                logger.error(traceback.format_exc())
                
                # 오류 응답 전송
                if request_id:
                    await self._send_command_response(
                        command, 
                        {"success": False, "error": str(e)}, 
                        request_id
                    )
        else:
            logger.warning(f"알 수 없는 명령: {command}")
            
            # 알 수 없는 명령 응답
            if request_id:
                await self._send_command_response(
                    command, 
                    {"success": False, "error": f"알 수 없는 명령: {command}"}, 
                    request_id
                )
    
    async def _handle_command_response(self, data: Dict):
        """
        명령 응답 처리
        
        Args:
            data: 응답 데이터
        """
        request_id = data.get("request_id")
        if not request_id:
            return
            
        # 대기 중인 응답이 있는지 확인
        if request_id in self._pending_responses:
            future = self._pending_responses.pop(request_id)
            if not future.done():
                # Future에 결과 설정
                future.set_result(data.get("data", {}))
    
    async def send_command(self, command: str, args: Dict = None, source: str = "system", wait_response: bool = False, timeout: float = 5.0) -> Optional[Dict]:
        """
        명령 전송 메서드
        
        Args:
            command: 명령 이름
            args: 명령 인자
            source: 명령 소스
            wait_response: 응답 대기 여부
            timeout: 응답 대기 시간 (초)
            
        Returns:
            Dict or None: 응답 데이터 (wait_response=True인 경우)
        """
        if not self.event_bus:
            logger.error("이벤트 버스가 초기화되지 않았습니다.")
            return None
            
        request_id = None
        
        if wait_response:
            # 고유 요청 ID 생성
            request_id = str(uuid.uuid4())
            
            # 응답 대기 Future 생성
            response_future = asyncio.Future()
            self._pending_responses[request_id] = response_future
        
        # 명령 이벤트 발행
        command_data = {
            "command": command,
            "args": args or {},
            "source": source,
        }
        
        if request_id:
            command_data["request_id"] = request_id
            
        await self.event_bus.publish(EventType.COMMAND, command_data)
        
        # 응답 대기
        if wait_response:
            try:
                return await asyncio.wait_for(response_future, timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning(f"명령 응답 시간 초과: {command}")
                if request_id in self._pending_responses:
                    del self._pending_responses[request_id]
                return {"success": False, "error": "응답 시간 초과"}
            except Exception as e:
                logger.error(f"명령 응답 대기 중 오류: {str(e)}")
                if request_id in self._pending_responses:
                    del self._pending_responses[request_id]
                return {"success": False, "error": str(e)}
        
        return None
    
    async def _send_command_response(self, command: str, data: Dict, request_id):
        """
        명령 응답 전송
        
        Args:
            command: 명령어
            data: 응답 데이터
            request_id: 요청 ID
        """
        if not self.event_bus:
            return
            
        await self.event_bus.publish(EventType.COMMAND, {
            "command": command,
            "is_response": True,
            "data": data,
            "request_id": request_id,
            "timestamp": datetime.now().isoformat()
        })
    
    # 프로세스 관련 명령 핸들러
    async def _handle_start_process(self, args: Dict) -> Dict:
        """
        프로세스 시작 명령 처리
        
        Args:
            args: 명령 인자 (process_name 포함)
        
        Returns:
            Dict: 처리 결과
        """
        process_name = args.get("process_name")
        if not process_name:
            return {"success": False, "error": "프로세스 이름이 필요합니다."}
            
        if not self.is_process_registered(process_name):
            return {"success": False, "error": f"프로세스 '{process_name}'이(가) 등록되지 않았습니다."}
            
        # 의존성이 있는 프로세스들 확인
        dependencies = self.process_dependencies.get(process_name, set())
        for dep in dependencies:
            if not self.is_process_running(dep):
                # 의존성 프로세스 먼저 시작
                await self.start_process(dep)
                
        # 요청된 프로세스 시작
        result = await self.start_process(process_name)
        return {"success": result, "process_name": process_name}
    
    async def _handle_stop_process(self, args: Dict) -> Dict:
        """
        프로세스 중지 명령 처리
        
        Args:
            args: 명령 인자 (process_name 포함)
        
        Returns:
            Dict: 처리 결과
        """
        process_name = args.get("process_name")
        if not process_name:
            return {"success": False, "error": "프로세스 이름이 필요합니다."}
            
        if not self.is_process_registered(process_name):
            return {"success": False, "error": f"프로세스 '{process_name}'이(가) 등록되지 않았습니다."}
            
        # 이 프로세스에 의존하는 다른 프로세스들 먼저 중지
        dependent_processes = self._get_dependent_processes(process_name)
        for dep_proc in dependent_processes:
            if self.is_process_running(dep_proc):
                await self.stop_process(dep_proc)
                
        # 요청된 프로세스 중지
        result = await self.stop_process(process_name)
        return {"success": result, "process_name": process_name}
    
    async def _handle_restart_process(self, args: Dict) -> Dict:
        """
        프로세스 재시작 명령 처리
        
        Args:
            args: 명령 인자 (process_name 포함)
        
        Returns:
            Dict: 처리 결과
        """
        process_name = args.get("process_name")
        if not process_name:
            return {"success": False, "error": "프로세스 이름이 필요합니다."}
            
        await self.restart_process(process_name)
        return {"success": True, "process_name": process_name}
    
    async def restart_process(self, process_name: str):
        """
        프로세스 재시작 처리
        
        의존성을 고려하여 프로세스와 관련 프로세스를 재시작합니다.
        
        Args:
            process_name: 재시작할 프로세스 이름
        """
        if self.shutting_down:
            logger.warning("시스템 종료 중에는 프로세스 재시작이 불가능합니다.")
            return
            
        try:
            # 독립 프로세스는 단독 재시작
            if not self.process_dependencies.get(process_name) and not self._get_dependent_processes(process_name):
                await self._restart_process(process_name)
                return
                
            # 의존하는 프로세스들도 함께 재시작
            dependent_processes = self._get_dependent_processes(process_name)
            
            # 1. 의존 프로세스들 중지 (역순)
            for dep_process in reversed(list(dependent_processes)):
                if self.is_process_running(dep_process):
                    await self.stop_process(dep_process)
            
            # 2. 대상 프로세스 중지
            if self.is_process_running(process_name):
                await self.stop_process(process_name)
            
            # 잠시 대기
            await asyncio.sleep(1)
            
            # 3. 대상 프로세스 시작
            await self.start_process(process_name)
            
            # 4. 의존 프로세스들 시작
            for dep_process in dependent_processes:
                await self.start_process(dep_process)
            
            logger.info(f"프로세스 '{process_name}' 및 의존 프로세스 재시작 완료")
            
        except Exception as e:
            logger.error(f"프로세스 '{process_name}' 재시작 중 오류: {str(e)}")
            logger.error(traceback.format_exc())
            
    async def _restart_process(self, process_name: str):
        """
        단일 프로세스 재시작
        
        Args:
            process_name: 재시작할 프로세스 이름
        """
        try:
            # 이벤트 데이터 생성
            logger.debug(f"프로세스 '{process_name}' 재시작 요청 이벤트 데이터 생성")
            event_data = ProcessEventData(
                process_name=process_name,
                event_type=ProcessEvent.RESTART_REQUESTED,
                status=ProcessStatus.STOPPING
            ).to_dict()
            
            # 디버그 로그로 이벤트 데이터 확인
            logger.debug(f"이벤트 데이터: {event_data}")
            
            # 프로세스 재시작 요청 이벤트 발행
            logger.info(f"프로세스 '{process_name}' 재시작 요청을 발행합니다.")
            await self.event_bus.publish(EventType.PROCESS_CONTROL, event_data)
            
        except Exception as e:
            logger.error(f"프로세스 '{process_name}' 재시작 실패: {str(e)}")
            raise
            
    def _get_dependent_processes(self, process_name: str) -> Set[str]:
        """
        프로세스에 의존하는 다른 프로세스들 찾기
        
        Args:
            process_name: 프로세스 이름
            
        Returns:
            Set[str]: 의존하는 프로세스 이름 집합
        """
        dependent_processes = set()
        for proc_name, deps in self.process_dependencies.items():
            if process_name in deps:
                dependent_processes.add(proc_name)
                # 재귀적으로 의존성 검색 (의존하는 프로세스에 의존하는 프로세스)
                for dep_of_dep in self._get_dependent_processes(proc_name):
                    dependent_processes.add(dep_of_dep)
        return dependent_processes
            
    def is_initialized(self) -> bool:
        """초기화 완료 여부 확인"""
        return self.initialized
        
    def is_shutting_down(self) -> bool:
        """종료 중인지 확인"""
        return self.shutting_down
        
    def _get_process_start_order(self) -> List[str]:
        """
        프로세스 시작 순서 결정
        
        의존성 그래프를 기반으로 프로세스 시작 순서를 결정합니다.
        의존성이 있는 프로세스는 의존하는 프로세스 이후에 시작됩니다.
        
        Returns:
            List[str]: 시작 순서로 정렬된 프로세스 이름 목록
        """
        # 시작 순서를 저장할 리스트
        result = []
        # 방문한 프로세스를 추적하는 집합
        visited = set()
        
        def visit(process):
            if process in visited:
                return
            visited.add(process)
            # 먼저 의존하는 프로세스 방문
            for dep in self.process_dependencies.get(process, set()):
                visit(dep)
            # 모든 의존성을 방문한 후 현재 프로세스 추가
            result.append(process)
        
        # 모든 프로세스 방문
        for process in self.process_dependencies:
            visit(process)
            
        return result