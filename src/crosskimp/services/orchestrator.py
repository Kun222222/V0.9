"""
오케스트레이터 모듈

시스템의 전체 생명주기를 관리하고 프로세스들을 조율합니다.
프로세스 관리 및 명령 처리 기능을 포함합니다.
"""

import asyncio
import uuid
from typing import Dict, Optional, Set, Any, Callable, List
from datetime import datetime, timedelta
import traceback
import time

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.events.system_eventbus import get_event_bus
from crosskimp.common.events.system_types import EventPaths, ProcessStatusEvent
from crosskimp.common.config.app_config import get_config

logger = get_unified_logger(component=SystemComponent.SYSTEM.value)

class Orchestrator:
    """시스템 오케스트레이터 및 명령 처리기"""
    
    def __init__(self, event_bus):
        """
        오케스트레이터 초기화
        
        Args:
            event_bus: 이벤트 버스 인스턴스 (필수)
        """
        self.event_bus = event_bus
        self.initialized = False
        self.shutting_down = False
        
        # 프로세스 의존성 정의
        self.process_dependencies = {
            "ob_collector": set(),  # 오더북은 독립적
            "telegram": set(),  # 텔레그램 커맨더는 독립적
            "radar": set(),  # 레이더는 독립적으로 설정 (아직 구현되지 않음)
            "trader": set(),  # 트레이더는 독립적으로 설정 (아직 구현되지 않음)
            "web_server": set()  # 웹서비스도 독립적으로 설정 (아직 구현되지 않음)
        }
        
        # 프로세스 실행 상태 추적
        self.running_processes = set()
        
        # 시작 요청된 프로세스 추적 (중복 요청 방지)
        self.starting_processes = set()
        
        # 명령 핸들러 관련 속성
        self._command_handlers = {}
        self._pending_responses = {}
        
    async def initialize(self):
        """오케스트레이터 초기화"""
        try:
            # 이벤트 핸들러 등록
            self.event_bus.register_handler(
                EventPaths.SYSTEM_COMMAND,
                self._process_command
            )
            
            # 프로세스 상태 이벤트 구독
            self.event_bus.register_handler(
                EventPaths.PROCESS_STATUS,
                self._handle_process_status
            )
            
            # 표준 명령 핸들러 등록
            self._register_standard_handlers()
            
            # 프로세스 컴포넌트 초기화
            await self._initialize_process_components()
            
            # 주기적 상태 보고 태스크 시작
            asyncio.create_task(self._schedule_status_report())
            
            self.initialized = True
            logger.info("오케스트레이터가 초기화되었습니다.")
            
        except Exception as e:
            logger.error(f"오케스트레이터 초기화 중 오류 발생: {str(e)}")
            raise
        
    async def _initialize_process_components(self):
        """프로세스 컴포넌트 초기화"""
        try:
            logger.info("프로세스 컴포넌트 초기화 중...")
            
            # 텔레그램 노티파이어 초기화
            from crosskimp.telegram_bot.notify import get_telegram_notifier
            notifier = get_telegram_notifier()
            logger.info("텔레그램 노티파이어 초기화 완료")
            
            # 오더북 프로세스 핸들러 초기화
            from crosskimp.common.events.handler.obcollector_handler import initialize_orderbook_process
            await initialize_orderbook_process(eventbus=self.event_bus, config=get_config())
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
            from crosskimp.telegram_bot.commander import get_telegram_commander
            telegram = get_telegram_commander()
            await telegram.start()
            logger.info("텔레그램 커맨더 시작 완료")
            
            # 현재는 ob_collector만 시작 (다른 프로세스는 아직 구현되지 않음)
            await self.start_process("ob_collector")
            
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
            data: 이벤트 데이터 (ProcessStatusEvent.__dict__)
        """
        try:
            # 필수 필드 추출
            process_name = data.get("process_name")
            status = data.get("status")
            
            if not process_name or not status:
                logger.warning(f"프로세스 상태 이벤트 데이터 불완전: {data}")
                return
            
            # 상태에 따라 running_processes 집합 업데이트
            if status == EventPaths.PROCESS_STATUS_RUNNING:
                self.running_processes.add(process_name)
                # 시작 요청 목록에서도 제거 (안전을 위해)
                self.starting_processes.discard(process_name)
                logger.info(f"프로세스 '{process_name}'이(가) 실행 중 상태로 변경되었습니다.")
            elif status == EventPaths.PROCESS_STATUS_STOPPED:
                self.running_processes.discard(process_name)
                self.starting_processes.discard(process_name)
                logger.info(f"프로세스 '{process_name}'이(가) 중지 상태로 변경되었습니다.")
            elif status == EventPaths.PROCESS_STATUS_ERROR:
                self.running_processes.discard(process_name)
                self.starting_processes.discard(process_name)
                error_msg = data.get("error_message", "알 수 없는 오류")
                logger.error(f"프로세스 '{process_name}' 오류 발생: {error_msg}")
            
        except Exception as e:
            logger.error(f"프로세스 상태 이벤트 처리 중 오류: {str(e)}")
            
    async def start_process(self, process_name: str):
        """
        프로세스 시작
        
        이벤트 버스를 통해 프로세스 시작 요청을 발행합니다.
        
        Args:
            process_name: 프로세스 이름
        """
        # 프로세스가 등록되어 있는지 확인
        if process_name not in self.process_dependencies:
            logger.error(f"프로세스 '{process_name}'이(가) 등록되지 않았습니다.")
            return False
            
        # 이미 실행 중인지 확인
        if process_name in self.running_processes:
            logger.info(f"프로세스 '{process_name}'이(가) 이미 실행 중입니다.")
            return True
            
        # 이미 시작 요청 중인지 확인 (중복 요청 방지)
        if process_name in self.starting_processes:
            logger.info(f"프로세스 '{process_name}'이(가) 이미 시작 요청 중입니다.")
            return True
            
        # 시작 요청 중인 프로세스로 표시
        self.starting_processes.add(process_name)
        
        try:
            # 의존성이 있는 프로세스들 확인 및 시작
            dependencies = self.process_dependencies.get(process_name, set())
            for dep in dependencies:
                if not self.is_process_running(dep):
                    await self.start_process(dep)
                
            # 이벤트 데이터 생성
            event_data = ProcessStatusEvent(
                process_name=process_name,
                status=EventPaths.PROCESS_STATUS_STARTING,
                event_type=EventPaths.PROCESS_EVENT_START_REQUESTED
            )
            
            # 프로세스 시작 요청 이벤트 발행
            logger.info(f"프로세스 '{process_name}' 시작 요청을 발행합니다.")
            await self.event_bus.publish(EventPaths.PROCESS_START, event_data.__dict__)
            return True
        except Exception as e:
            logger.error(f"프로세스 '{process_name}' 시작 요청 중 오류: {str(e)}")
            return False
        finally:
            # 시작 요청 완료 후 목록에서 제거
            self.starting_processes.discard(process_name)
        
    async def stop_process(self, process_name: str):
        """
        프로세스 중지
        
        이벤트 버스를 통해 프로세스 중지 요청을 발행합니다.
        
        Args:
            process_name: 프로세스 이름
        """
        # 프로세스가 등록되어 있는지 확인
        if process_name not in self.process_dependencies:
            logger.error(f"프로세스 '{process_name}'이(가) 등록되지 않았습니다.")
            return False
            
        # 실행 중이 아니면 무시
        if process_name not in self.running_processes:
            logger.info(f"프로세스 '{process_name}'이(가) 실행 중이 아닙니다.")
            return True
        
        # 이 프로세스에 의존하는 다른 프로세스들 먼저 중지
        dependent_processes = self._get_dependent_processes(process_name)
        for dep_proc in dependent_processes:
            if self.is_process_running(dep_proc):
                await self.stop_process(dep_proc)
            
        # 이벤트 데이터 생성
        event_data = ProcessStatusEvent(
            process_name=process_name,
            status=EventPaths.PROCESS_STATUS_STOPPING,
            event_type=EventPaths.PROCESS_EVENT_STOP_REQUESTED
        )
        
        # 프로세스 중지 요청 이벤트 발행
        logger.info(f"프로세스 '{process_name}' 중지 요청을 발행합니다.")
        await self.event_bus.publish(EventPaths.PROCESS_STOP, event_data.__dict__)
        return True
        
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
        self.register_command_handler("get_process_status", self._handle_get_process_status)
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
            
        await self.event_bus.publish(EventPaths.SYSTEM_COMMAND, command_data)
        
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
        await self.event_bus.publish(EventPaths.SYSTEM_COMMAND, {
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
        
        # 프로세스 중지 후 시작
        await self.stop_process(process_name)
        await asyncio.sleep(1)  # 종료 대기
        result = await self.start_process(process_name)
        
        return {"success": result, "process_name": process_name}
    
    async def _handle_get_process_status(self, args: Dict) -> Dict:
        """
        프로세스 상태 조회 명령 처리
        
        Args:
            args: 명령 인자
        
        Returns:
            Dict: 프로세스 상태 정보
        """
        specific_process = args.get("process_name")
        
        if specific_process:
            # 특정 프로세스 상태만 조회
            if specific_process not in self.process_dependencies:
                return {"success": False, "error": f"프로세스 '{specific_process}'이(가) 등록되지 않았습니다."}
                
            is_running = self.is_process_running(specific_process)
            return {
                "success": True,
                "process_name": specific_process,
                "status": "running" if is_running else "stopped"
            }
        else:
            # 모든 프로세스 상태 조회
            all_statuses = {}
            for process_name in self.process_dependencies:
                all_statuses[process_name] = "running" if self.is_process_running(process_name) else "stopped"
                
            return {
                "success": True,
                "statuses": all_statuses
            }
                
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

    async def _schedule_status_report(self):
        """매 시간 00분마다 시스템 상태 보고 태스크"""
        try:
            logger.info("주기적 시스템 상태 보고 태스크 시작")
            
            while not self.shutting_down:
                # 다음 정각 (00분)까지의 대기 시간 계산
                now = datetime.now()
                next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                wait_seconds = (next_hour - now).total_seconds()
                
                # 다음 보고 시간까지 대기
                await asyncio.sleep(wait_seconds)
                
                # 시스템이 종료 중이면 중단
                if self.shutting_down:
                    break
                    
                # 상태 보고 이벤트 발행
                try:
                    # 모든 프로세스 상태 수집
                    processes_status = {}
                    for process_name in self.process_dependencies.keys():
                        is_running = process_name in self.running_processes
                        processes_status[process_name] = {
                            "running": is_running,
                            "status": "running" if is_running else "stopped"
                        }
                    
                    # 현재 시간 포맷팅
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    # 이벤트 데이터 생성
                    status_data = {
                        "message": f"🕒 정시 시스템 상태 보고 ({current_time})\n\n" + self._format_system_status(processes_status),
                        "timestamp": datetime.now().timestamp(),
                        "processes": processes_status
                    }
                    
                    # 시스템 상태 이벤트 발행
                    await self.event_bus.publish(EventPaths.SYSTEM_STATUS, status_data)
                    logger.info(f"시스템 상태 보고 이벤트 발행됨 ({current_time})")
                    
                except Exception as e:
                    logger.error(f"상태 보고 이벤트 발행 중 오류: {str(e)}")
                
        except asyncio.CancelledError:
            logger.info("주기적 상태 보고 태스크 취소됨")
        except Exception as e:
            logger.error(f"주기적 상태 보고 태스크 오류: {str(e)}")
            
    def _format_system_status(self, processes_status):
        """시스템 상태 메시지 포맷팅"""
        message = "📊 시스템 상태:\n\n"
        
        # 프로세스별 상태
        for process_name, status in processes_status.items():
            if status["running"]:
                message += f"✅ {process_name}: 실행 중\n"
            else:
                message += f"⚫ {process_name}: 중지됨\n"
        
        # 추가 시스템 정보 (선택적)
        message += f"\n💻 시스템 업타임: {self._get_uptime()}"
        
        return message
        
    def _get_uptime(self):
        """시스템 업타임 반환"""
        import psutil
        
        try:
            # 시스템 부팅 시간 가져오기
            boot_time = psutil.boot_time()
            uptime_seconds = time.time() - boot_time
            
            # 가독성 있는 형식으로 변환
            days = int(uptime_seconds // (24 * 3600))
            hours = int((uptime_seconds % (24 * 3600)) // 3600)
            minutes = int((uptime_seconds % 3600) // 60)
            
            if days > 0:
                return f"{days}일 {hours}시간 {minutes}분"
            else:
                return f"{hours}시간 {minutes}분"
                
        except Exception as e:
            logger.warning(f"업타임 가져오기 실패: {str(e)}")
            return "확인 불가"