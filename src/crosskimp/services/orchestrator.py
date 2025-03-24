"""
시스템 오케스트레이터 모듈

이 모듈은 시스템 프로세스의 라이프사이클을 관리합니다. 
오더북 관리자 및 기타 프로세스의 시작, 중지, 모니터링을 담당합니다.
매일 지정된 시간에 오더북 수집기를 자동으로 재시작하는 스케줄러를 포함합니다.
"""

import asyncio
import time
from datetime import datetime
from typing import Dict, Any, List, Callable, Union
import traceback
import inspect

from crosskimp.common.logger.logger import get_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.events.sys_event_bus import EventType
from crosskimp.services.scheduler import schedule_orderbook_restart
from crosskimp.services.command_handler import get_command_handler

# 로거 설정
logger = get_logger(__name__, component=SystemComponent.MAIN_SYSTEM.value)

# 싱글톤 인스턴스 
_orchestrator_instance = None

def get_orchestrator():
    """글로벌 오케스트레이터 인스턴스를 반환합니다."""
    global _orchestrator_instance
    if _orchestrator_instance is None:
        _orchestrator_instance = Orchestrator()
    return _orchestrator_instance

class Orchestrator:
    """
    시스템 오케스트레이터 클래스
    
    시스템의 다양한 프로세스들의 수명 주기를 관리합니다.
    - 프로세스 시작/중지/재시작
    - 프로세스 상태 모니터링
    - 스케줄링된 작업 관리
    """
    
    def __init__(self):
        """오케스트레이터 초기화"""
        # 초기화 완료 여부
        self.initialized = False
        
        # 시작 시간
        self.start_time = time.time()
        
        # 이벤트 버스 참조
        self.event_bus = None
        
        # 명령 핸들러 참조
        self.command_handler = None
        
        # 프로세스 상태와 참조
        self.processes = {}
        
        # 스케줄러
        self.scheduler = None
        
        # 로거
        self._logger = logger
        
        # 스케줄링된 작업 ID 저장
        self._scheduled_jobs = {}
    
    async def initialize(self):
        """오케스트레이터 초기화"""
        if self.initialized:
            return
            
        # 이벤트 버스 참조
        from crosskimp.common.events import get_event_bus
        self.event_bus = get_event_bus()
        
        # 명령 핸들러 초기화
        self.command_handler = get_command_handler()
        if not self.command_handler.is_initialized():
            await self.command_handler.initialize()
        self.command_handler.set_orchestrator(self)
        
        # 프로세스 상태 변경 이벤트 핸들러 등록
        if self.event_bus:
            self.event_bus.register_handler(EventType.PROCESS_START, self._handle_process_event)
            self.event_bus.register_handler(EventType.PROCESS_STOP, self._handle_process_event)
            self.event_bus.register_handler(EventType.ERROR, self._handle_error_event)
        
        # 지정된 시간에 오더북 수집기 재시작 스케줄링 (스케줄러 사용)
        schedule_orderbook_restart(self._restart_orderbook_job)
        
        self.initialized = True
        self._logger.info("시스템 오케스트레이터가 초기화되었습니다.")
        
        # 초기화 완료 이벤트 발행
        await self._publish_orchestrator_event("initialized")
    
    async def initialize_system_components(self):
        """
        시스템의 모든 컴포넌트 초기화 및 등록
        
        - 이벤트 버스 초기화
        - 텔레그램 커맨더 초기화
        - 오더북 매니저 초기화 및 등록
        - 기타 필요한 컴포넌트 초기화
        
        Returns:
            bool: 초기화 성공 여부
        """
        if not self.initialized:
            await self.initialize()
        
        try:
            # 이벤트 버스 초기화 확인
            from crosskimp.common.events import get_event_bus
            event_bus = get_event_bus()
            if not event_bus.is_initialized():
                await event_bus.initialize()
                self._logger.info("이벤트 버스가 초기화되었습니다.")
                
            # 텔레그램 커맨더 초기화
            from crosskimp.services.telegram_commander import get_telegram_commander
            telegram_commander = get_telegram_commander()
            if not telegram_commander.is_initialized():
                await telegram_commander.initialize()
                self._logger.info("텔레그램 커맨더가 초기화되었습니다.")
            
            # 오더북 매니저 초기화 및 등록
            self._logger.info("오더북 매니저를 초기화합니다...")
            
            # 새로운 방식 - 직접 OrderManager 모듈을 사용
            from crosskimp.ob_collector.orderbook.order_manager import start_orderbook_collection, stop_orderbook_collection
            
            # 오케스트레이터에 프로세스 등록 - 시작 및 중지 함수 직접 전달
            await self.register_process(
                "ob_collector",          # 프로세스 이름
                start_orderbook_collection,  # 시작 함수
                stop_orderbook_collection,   # 중지 함수
                description="오더북 수집기" # 설명
            )
            self._logger.info("오더북 수집기가 등록되었습니다.")
            
            # 시스템 시작 시 자동으로 시작할 프로세스 시작
            await self.start_initial_processes()
            
            self._logger.info("모든 시스템 컴포넌트가 초기화되었습니다.")
            return True
            
        except Exception as e:
            self._logger.error(f"시스템 컴포넌트 초기화 중 오류 발생: {str(e)}")
            self._logger.error(traceback.format_exc())
            
            # 오류 이벤트 발행
            await self._publish_error_event(
                f"시스템 컴포넌트 초기화 실패: {str(e)}",
                source="orchestrator",
                severity="critical"
            )
            return False

    async def start_initial_processes(self):
        """
        시스템 시작 시 자동으로 시작할 프로세스들을 시작합니다.
        """
        self._logger.info("초기 프로세스들을 시작합니다...")
        
        # 오더북 수집기 시작
        if "ob_collector" in self.processes:
            await self.start_process("ob_collector", "시스템 시작 시 자동 시작")
        
        # 필요한 경우 다른 프로세스들도 여기서 시작
        # 예: await self.start_process("other_process", "시스템 시작 시 자동 시작")
        
        self._logger.info("모든 초기 프로세스 시작이 완료되었습니다.")
    
    async def shutdown(self):
        """오케스트레이터 종료"""
        if not self.initialized:
            return
            
        self._logger.info("시스템 오케스트레이터를 종료합니다...")
        
        # 등록된 모든 프로세스 중지
        tasks = []
        for process_name in list(self.processes.keys()):
            tasks.append(self.stop_process(process_name))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # 스케줄러 종료
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown()
        
        # 이벤트 핸들러 등록 해제
        if self.event_bus:
            self.event_bus.unregister_handler(EventType.PROCESS_START, self._handle_process_event)
            self.event_bus.unregister_handler(EventType.PROCESS_STOP, self._handle_process_event)
            self.event_bus.unregister_handler(EventType.ERROR, self._handle_error_event)
        
        # 명령 핸들러 종료
        if self.command_handler and self.command_handler.is_initialized():
            await self.command_handler.shutdown()
        
        self.initialized = False
        self._logger.info("시스템 오케스트레이터가 종료되었습니다.")
        
        # 종료 이벤트 발행
        await self._publish_orchestrator_event("shutdown")
    
    async def _restart_orderbook_job(self):
        """
        오더북 수집기 재시작 작업
        스케줄러에 의해 호출됩니다.
        """
        try:
            self._logger.info("일일 정기 작업 - 오더북 수집기 재시작을 시작합니다...")
            await self.restart_process("ob_collector", "일일 정기 재시작")
            self._logger.info("오더북 수집기 정기 재시작이 완료되었습니다.")
        except Exception as e:
            self._logger.error(f"오더북 수집기 정기 재시작 중 오류: {str(e)}")
            # 추적 정보 로깅
            self._logger.error(traceback.format_exc())
    
    async def _status_update_job(self):
        """
        상태 업데이트 작업
        텔레그램 명령어로 호출될 수 있습니다.
        """
        try:
            # 시스템 상태 수집
            status = self._collect_system_status()
            
            # 이벤트 버스에 상태 발행
            if self.event_bus:
                await self.event_bus.publish(EventType.STATUS_UPDATE, status)
                
            self._logger.debug("시스템 상태 업데이트 완료")
            return status
        except Exception as e:
            self._logger.error(f"시스템 상태 업데이트 중 오류: {str(e)}")
            # 추적 정보 로깅
            self._logger.error(traceback.format_exc())
            return {"error": str(e)}
    
    async def _publish_orchestrator_event(self, event_type: str, data: Dict = None):
        """
        오케스트레이터 이벤트 발행
        
        Args:
            event_type: 이벤트 유형
            data: 추가 데이터
        """
        if not self.event_bus:
            return
            
        event_data = {
            "source": "orchestrator",
            "event_type": event_type,
            "timestamp": datetime.now().isoformat(),
            **(data or {})
        }
        
        await self.event_bus.publish(EventType.STATUS_UPDATE, event_data)
    
    async def register_process(
        self, 
        process_name: str, 
        start_func: Union[Callable, object], 
        stop_func: Union[Callable, object] = None, 
        description: str = None
    ):
        """
        프로세스 등록 - 유연한 인터페이스 지원
        
        Args:
            process_name: 프로세스 이름
            start_func: 프로세스 시작 함수 또는 객체 (객체일 경우 run, start, initialize 메서드를 찾음)
            stop_func: 프로세스 중지 함수 또는 객체 (객체일 경우 stop, shutdown, close 메서드를 찾음)
                       None이면 start_func이 객체일 경우 해당 객체에서 중지 메서드를 찾습니다
            description: 프로세스 설명
        """
        if process_name in self.processes:
            self._logger.warning(f"프로세스 '{process_name}'가 이미 등록되어 있습니다.")
            return
            
        # 유연한 시작 함수 처리
        final_start_func = self._prepare_callable_function(
            start_func, 
            ['run', 'start', 'initialize', '__call__'], 
            f"프로세스 '{process_name}'를 시작할 수 있는 메서드를 찾을 수 없습니다."
        )
        
        # 유연한 중지 함수 처리
        if stop_func is None and not isinstance(start_func, Callable):
            # 시작 함수가 객체이고 중지 함수가 제공되지 않은 경우, 객체에서 중지 메서드를 찾습니다
            stop_func = start_func
        
        final_stop_func = self._prepare_callable_function(
            stop_func, 
            ['stop', 'shutdown', 'close', 'terminate'],
            f"프로세스 '{process_name}'를 중지할 수 있는 메서드를 찾을 수 없습니다."
        )
        
        self.processes[process_name] = {
            "name": process_name,
            "description": description or process_name,
            "start_func": final_start_func,
            "stop_func": final_stop_func,
            "running": False,
            "start_time": None,
            "restart_count": 0,
            "last_error": None
        }
        
        self._logger.info(f"프로세스 '{process_name}'가 등록되었습니다.")
    
    def _prepare_callable_function(self, obj: Union[Callable, object], method_names: List[str], error_msg: str) -> Callable:
        """
        호출 가능한 함수를 준비합니다.
        객체인 경우 주어진 이름의 메서드를 찾고, 호출 가능한 함수로 변환합니다.
        
        Args:
            obj: 함수 또는 객체
            method_names: 객체에서 찾을 메서드 이름 목록 (우선순위 순)
            error_msg: 메서드를 찾지 못했을 때 출력할 오류 메시지
            
        Returns:
            호출 가능한 함수
        """
        if obj is None:
            # 기본 더미 함수 반환
            async def dummy_func():
                pass
            return dummy_func
            
        if callable(obj):
            # 이미 호출 가능한 경우 그대로 반환
            return obj
            
        # 객체에서 메서드 찾기
        for method_name in method_names:
            if hasattr(obj, method_name) and callable(getattr(obj, method_name)):
                method = getattr(obj, method_name)
                
                # 메서드가 코루틴 함수인지 확인
                is_coroutine = inspect.iscoroutinefunction(method)
                
                if is_coroutine:
                    # 이미 코루틴 함수면 그대로 반환
                    return method
                else:
                    # 일반 함수면 코루틴으로 래핑
                    async def wrapper_func(*args, **kwargs):
                        return method(*args, **kwargs)
                    return wrapper_func
        
        # 메서드를 찾지 못한 경우 오류 로깅 및 더미 함수 반환
        self._logger.warning(error_msg)
        
        async def dummy_func():
            self._logger.warning(f"더미 함수가 호출되었습니다. {error_msg}")
            pass
            
        return dummy_func
    
    async def start_process(self, process_name: str, reason: str = None):
        """
        프로세스 시작
        
        Args:
            process_name: 프로세스 이름
            reason: 시작 이유
        
        Returns:
            bool: 시작 성공 여부
        """
        if not self.initialized:
            self._logger.error("오케스트레이터가 초기화되지 않았습니다.")
            return False
            
        if process_name not in self.processes:
            self._logger.error(f"프로세스 '{process_name}'가 등록되지 않았습니다.")
            return False
            
        process = self.processes[process_name]
        
        if process["running"]:
            self._logger.info(f"프로세스 '{process_name}'가 이미 실행 중입니다.")
            return True
            
        try:
            self._logger.info(f"프로세스 '{process_name}'를 시작합니다...")
            await process["start_func"]()
            
            # 프로세스 상태 업데이트
            process["running"] = True
            process["start_time"] = time.time()
            
            # 프로세스 시작 이벤트 발행
            await self._publish_process_event(
                EventType.PROCESS_START,
                process_name,
                process["description"],
                reason=reason
            )
            
            self._logger.info(f"프로세스 '{process_name}'가 시작되었습니다.")
            return True
            
        except Exception as e:
            process["last_error"] = str(e)
            self._logger.error(f"프로세스 '{process_name}' 시작 중 오류: {str(e)}")
            
            # 스택 트레이스 로깅
            self._logger.error(traceback.format_exc())
            
            # 오류 이벤트 발행
            await self._publish_error_event(
                f"프로세스 '{process_name}' 시작 실패: {str(e)}",
                source=process_name,
                severity="error"
            )
            
            return False
    
    async def stop_process(self, process_name: str, reason: str = None, was_error: bool = False):
        """
        프로세스 중지
        
        Args:
            process_name: 프로세스 이름
            reason: 중지 이유
            was_error: 오류로 인한 중지 여부
        
        Returns:
            bool: 중지 성공 여부
        """
        if not self.initialized:
            self._logger.error("오케스트레이터가 초기화되지 않았습니다.")
            return False
            
        if process_name not in self.processes:
            self._logger.error(f"프로세스 '{process_name}'가 등록되지 않았습니다.")
            return False
            
        process = self.processes[process_name]
        
        if not process["running"]:
            self._logger.info(f"프로세스 '{process_name}'가 이미 중지되었습니다.")
            return True
            
        try:
            self._logger.info(f"프로세스 '{process_name}'를 중지합니다...")
            await process["stop_func"]()
            
            # 프로세스 상태 업데이트
            process["running"] = False
            process["start_time"] = None
            
            # 프로세스 중지 이벤트 발행
            await self._publish_process_event(
                EventType.PROCESS_STOP,
                process_name,
                process["description"],
                reason=reason,
                was_error=was_error
            )
            
            self._logger.info(f"프로세스 '{process_name}'가 중지되었습니다.")
            return True
            
        except Exception as e:
            process["last_error"] = str(e)
            self._logger.error(f"프로세스 '{process_name}' 중지 중 오류: {str(e)}")
            
            # 스택 트레이스 로깅
            self._logger.error(traceback.format_exc())
            
            # 프로세스 상태는 강제로 중지됨으로 설정
            process["running"] = False
            process["start_time"] = None
            
            # 오류 이벤트 발행
            await self._publish_error_event(
                f"프로세스 '{process_name}' 중지 실패: {str(e)}",
                source=process_name,
                severity="error"
            )
            
            # 프로세스 중지 이벤트 발행 (오류 상태)
            await self._publish_process_event(
                EventType.PROCESS_STOP,
                process_name,
                process["description"],
                reason=f"중지 오류: {str(e)}",
                was_error=True
            )
            
            return False
    
    async def restart_process(self, process_name: str, reason: str = None):
        """
        프로세스 재시작
        
        Args:
            process_name: 프로세스 이름
            reason: 재시작 이유
            
        Returns:
            bool: 재시작 성공 여부
        """
        if not self.initialized:
            self._logger.error("오케스트레이터가 초기화되지 않았습니다.")
            return False
            
        if process_name not in self.processes:
            self._logger.error(f"프로세스 '{process_name}'가 등록되지 않았습니다.")
            return False
            
        process = self.processes[process_name]
        restart_reason = reason or "수동 재시작"
        
        self._logger.info(f"프로세스 '{process_name}'를 재시작합니다... 사유: {restart_reason}")
        
        # 프로세스가 실행 중이면 중지
        if process["running"]:
            stop_result = await self.stop_process(process_name, reason=f"재시작: {restart_reason}")
            if not stop_result:
                self._logger.warning(f"프로세스 '{process_name}' 중지 실패, 강제로 재시작을 시도합니다.")
        
        # 약간의 지연 후 재시작
        await asyncio.sleep(1)
        
        # 프로세스 재시작 카운트 증가
        process["restart_count"] += 1
        
        # 프로세스 시작
        start_result = await self.start_process(process_name, reason=f"재시작: {restart_reason}")
        
        if start_result:
            self._logger.info(f"프로세스 '{process_name}' 재시작 완료")
        else:
            self._logger.error(f"프로세스 '{process_name}' 재시작 실패")
            
        return start_result
    
    async def _publish_process_event(
        self, 
        event_type: EventType, 
        process_name: str, 
        description: str, 
        reason: str = None,
        was_error: bool = False
    ):
        """
        프로세스 이벤트 발행
        
        Args:
            event_type: 이벤트 유형
            process_name: 프로세스 이름
            description: 프로세스 설명
            reason: 이벤트 이유
            was_error: 오류로 인한 이벤트 여부
        """
        if not self.event_bus:
            return
            
        event_data = {
            "event_type": event_type.name,
            "process_name": process_name,
            "description": description,
            "timestamp": datetime.now().isoformat(),
            "was_error": was_error
        }
        
        if reason:
            event_data["reason"] = reason
            
        await self.event_bus.publish(event_type, event_data)
    
    async def _publish_error_event(self, message: str, source: str, severity: str = "error"):
        """
        오류 이벤트 발행
        
        Args:
            message: 오류 메시지
            source: 오류 소스
            severity: 오류 심각도 (error, warning, critical)
        """
        if not self.event_bus:
            return
            
        await self.event_bus.publish(EventType.ERROR, {
            "message": message,
            "source": source,
            "severity": severity,
            "timestamp": datetime.now().isoformat()
        })
    
    def _collect_system_status(self) -> Dict:
        """
        시스템 상태 수집
        
        Returns:
            Dict: 시스템 상태 정보
        """
        # 현재 시간
        current_time = time.time()
        
        # 프로세스 상태 수집
        processes_status = {}
        for name, process in self.processes.items():
            uptime = None
            if process["running"] and process["start_time"]:
                uptime = current_time - process["start_time"]
                
            processes_status[name] = {
                "running": process["running"],
                "uptime": uptime,
                "restart_count": process["restart_count"],
                "last_error": process["last_error"],
                "description": process["description"]
            }
        
        # 이벤트 버스 통계
        event_bus_stats = {}
        if self.event_bus:
            event_bus_stats = {
                "published_events": self.event_bus.get_published_count(),
                "processed_events": self.event_bus.get_processed_count(),
                "errors": self.event_bus.get_error_count()
            }
        
        # 시스템 상태 구성
        status = {
            "system_running": self.initialized,
            "processes": processes_status,
            "stats": {
                "uptime": current_time - self.start_time,
                "process_count": len(self.processes),
                "running_process_count": sum(1 for p in self.processes.values() if p["running"])
            },
            "event_bus_stats": event_bus_stats,
            "timestamp": datetime.now().isoformat()
        }
        
        return status
    
    def is_initialized(self) -> bool:
        """
        초기화 완료 여부 확인
        
        Returns:
            bool: 초기화 완료 여부
        """
        return self.initialized
    
    def is_process_running(self, process_name: str) -> bool:
        """
        프로세스 실행 여부 확인
        
        Args:
            process_name: 프로세스 이름
            
        Returns:
            bool: 프로세스 실행 여부
        """
        if process_name not in self.processes:
            return False
            
        return self.processes[process_name]["running"]

    async def _handle_process_event(self, event_data: Dict):
        """
        프로세스 이벤트 핸들러 - 상태 변경 감지
        
        Args:
            event_data: 이벤트 데이터
        """
        try:
            process_name = event_data.get("process_name")
            if not process_name:
                return
                
            event_type = event_data.get("event_type")
            was_error = event_data.get("was_error", False)
            
            self._logger.debug(f"프로세스 이벤트 감지: {process_name}, 유형: {event_type}, 오류: {was_error}")
            
            # 상태 변경 감지 - 필요에 따라 여기서 추가 처리
            if was_error and process_name in self.processes:
                self._logger.warning(f"프로세스 {process_name}에서 오류가 감지되었습니다.")
                # 오류 상태를 저장하거나 필요한 추가 조치
                
            # 상태 업데이트 발행 - 상태가 변경되었을 때만 발행
            await self._status_update_job()
                
        except Exception as e:
            self._logger.error(f"프로세스 이벤트 처리 중 오류: {str(e)}")
            self._logger.error(traceback.format_exc())

    async def _handle_error_event(self, event_data: Dict):
        """
        오류 이벤트 핸들러
        
        Args:
            event_data: 이벤트 데이터
        """
        try:
            message = event_data.get("message", "알 수 없는 오류")
            source = event_data.get("source", "unknown")
            severity = event_data.get("severity", "error")
            
            self._logger.warning(f"오류 이벤트 감지: {message} (소스: {source}, 심각도: {severity})")
            
            # 심각한 오류인 경우 추가 처리
            if severity == "critical" and source in self.processes:
                self._logger.error(f"심각한 오류 감지: {source} - {message}")
                # 필요한 복구 조치
                
        except Exception as e:
            self._logger.error(f"오류 이벤트 처리 중 오류: {str(e)}")
            self._logger.error(traceback.format_exc())
