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
from crosskimp.common.events.system_types import EventChannels, EventValues, ProcessStatusEvent
from crosskimp.common.config.app_config import get_config

logger = get_unified_logger(component=SystemComponent.SYSTEM.value)

class Orchestrator:
    """시스템 오케스트레이터 및 명령 처리기"""
    
    def __init__(self, event_bus = None):
        """
        오케스트레이터 초기화
        
        Args:
            event_bus: 이벤트 버스 인스턴스 (없으면 새로 생성)
        """
        logger.info("오케스트레이터 초기화 중...")
        
        # 이벤트 버스
        self.event_bus = event_bus or get_event_bus()
        
        # 프로세스 의존성 정의 (프로세스가 시작되기 위해 필요한 다른 프로세스)
        self.process_dependencies = {
            "ob_collector": set(),         # 의존성 없음
            "radar": {"ob_collector"},     # 오더북에 의존
            "trader": {"ob_collector", "radar"},  # 오더북, 레이더에 의존
            "web_server": set()            # 의존성 없음 (독립적으로 실행 가능)
        }
        
        # 프로세스 상태 추적
        self.running_processes = set()        # 실행 중인 프로세스
        self.starting_processes = set()       # 시작 요청 중인 프로세스 (중복 시작 방지)
        
        # 초기화 상태
        self.initialized = False
        self.shutting_down = False
        
        # 프로세스 시작/종료 타임스탬프
        self.start_time = time.time()
        
        logger.info("오케스트레이터 인스턴스가 생성되었습니다.")
        
    async def initialize(self):
        """오케스트레이터 초기화"""
        try:
            # 텔레그램 명령 이벤트 구독은 더 이상 필요하지 않음 (텔레그램 커맨더가 직접 호출)
            
            # 프로세스 상태 이벤트 구독
            self.event_bus.register_handler(
                EventChannels.Process.STATUS,
                self._handle_process_status
            )
            
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
            logger.info("프로세스 컴포넌트 초기화")
            
            # 오더북 프로세스 핸들러 초기화
            from crosskimp.common.events.handler.obcollector_handler import initialize_orderbook_process
            await initialize_orderbook_process(eventbus=self.event_bus, config=get_config())
            
            logger.info("프로세스 컴포넌트 초기화 완료")
        except Exception as e:
            logger.error(f"프로세스 컴포넌트 초기화 오류: {str(e)}")
            raise
        
    async def start(self):
        """시스템 시작"""
        if not self.initialized:
            await self.initialize()
            
        try:
            logger.info("시스템 시작 중...")
            
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
            error_message = data.get("error_message")
            
            if not process_name or not status:
                logger.warning(f"프로세스 상태 이벤트 데이터 불완전: {data}")
                return
            
            # 상태에 따라 running_processes 집합 업데이트
            if status == EventValues.PROCESS_RUNNING:
                self.running_processes.add(process_name)
                # 시작 요청 목록에서도 제거 (안전을 위해)
                self.starting_processes.discard(process_name)
                logger.info(f"프로세스 '{process_name}'이(가) 실행 중 상태로 변경되었습니다.")
            elif status == EventValues.PROCESS_STOPPED:
                self.running_processes.discard(process_name)
                self.starting_processes.discard(process_name)
                logger.info(f"프로세스 '{process_name}'이(가) 중지 상태로 변경되었습니다.")
            elif status == EventValues.PROCESS_ERROR:
                self.running_processes.discard(process_name)
                self.starting_processes.discard(process_name)
                logger.error(f"프로세스 '{process_name}'이(가) 오류 상태로 변경되었습니다: {error_message}")
            
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
                
            # 상태 이벤트 생성
            event_data = ProcessStatusEvent(
                process_name=process_name,
                status=EventValues.PROCESS_STARTING,
                event_type="process/start_requested"
            )
            
            # 프로세스 시작 요청 이벤트 발행
            logger.info(f"프로세스 '{process_name}' 시작 요청을 발행합니다.")
            await self.event_bus.publish(EventChannels.Process.COMMAND_START, event_data.__dict__)
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
        try:
            if process_name not in self.get_available_processes():
                logger.error(f"지원되지 않는 프로세스 '{process_name}'")
                return False
            
            # 이벤트 데이터 생성
            event_data = ProcessStatusEvent(
                process_name=process_name,
                status=EventValues.PROCESS_STOPPING,
                event_type="process/stop_requested"
            )
            
            # 프로세스 중지 요청 이벤트 발행
            logger.info(f"프로세스 '{process_name}' 중지 요청을 발행합니다.")
            await self.event_bus.publish(EventChannels.Process.COMMAND_STOP, event_data.__dict__)
            return True
        except Exception as e:
            logger.error(f"프로세스 중지 요청 중 오류: {str(e)}")
            return False
        
    def is_process_running(self, process_name: str) -> bool:
        """프로세스 실행 중 여부 확인"""
        return process_name in self.running_processes
        
    def is_process_registered(self, process_name: str) -> bool:
        """프로세스가 등록되어 있는지 확인"""
        return process_name in self.process_dependencies
            
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
                    await self.event_bus.publish(EventChannels.System.STATUS, status_data)
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