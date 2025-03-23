"""
시스템 오케스트레이터 모듈

이 모듈은 시스템 프로세스의 라이프사이클을 관리합니다. 
오더북 관리자 및 기타 프로세스의 시작, 중지, 모니터링을 담당합니다.
매일 자정에 오더북 수집기를 자동으로 재시작하는 스케줄러를 포함합니다.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
import signal
from typing import Dict, Any, Optional, List, Callable, Coroutine, Union
import traceback
import inspect

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.sys_event_bus import EventType
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# 로거 설정
logger = get_unified_logger()

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
    - 텔레그램 명령 이벤트 처리
    """
    
    def __init__(self):
        """오케스트레이터 초기화"""
        # 초기화 완료 여부
        self.initialized = False
        
        # 시작 시간
        self.start_time = time.time()
        
        # 이벤트 버스 참조
        self.event_bus = None
        
        # 프로세스 상태와 참조
        self.processes = {}
        
        # 스케줄러
        self.scheduler = None
        
        # 로거
        self._logger = logger
        
        # 명령 핸들러 등록
        self._command_handlers = {
            "start_process": self._handle_start_process,
            "stop_process": self._handle_stop_process,
            "restart_process": self._handle_restart_process,
            "get_status": self._handle_get_status,
        }
        
        # 스케줄링된 작업 ID 저장
        self._scheduled_jobs = {}
    
    async def initialize(self):
        """오케스트레이터 초기화"""
        if self.initialized:
            return
            
        # 이벤트 버스 참조
        from crosskimp.common.events import get_event_bus
        self.event_bus = get_event_bus()
        
        # 스케줄러 초기화
        self.scheduler = AsyncIOScheduler()
        
        # 명령 이벤트 핸들러 등록
        if self.event_bus:
            self.event_bus.register_handler(EventType.COMMAND, self._process_command)
        
        # 자정에 오더북 수집기 재시작 스케줄링
        self._schedule_orderbook_restart()
        
        # 스케줄러 시작
        self.scheduler.start()
        
        self.initialized = True
        self._logger.info("시스템 오케스트레이터가 초기화되었습니다.")
        
        # 시스템 상태 업데이트 스케줄링 (5분마다)
        self._schedule_status_update()
        
        # 초기화 완료 이벤트 발행
        await self._publish_orchestrator_event("initialized")
    
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
            self.event_bus.unregister_handler(EventType.COMMAND, self._process_command)
        
        self.initialized = False
        self._logger.info("시스템 오케스트레이터가 종료되었습니다.")
        
        # 종료 이벤트 발행
        await self._publish_orchestrator_event("shutdown")
    
    def _schedule_orderbook_restart(self):
        """오더북 수집기 자정 재시작 스케줄링"""
        # 기존 작업이 있다면 제거
        if "orderbook_restart" in self._scheduled_jobs:
            self.scheduler.remove_job(self._scheduled_jobs["orderbook_restart"])
        
        # 매일 자정에 실행
        job = self.scheduler.add_job(
            self._restart_orderbook_job,
            'cron',
            hour=0,
            minute=0,
            second=0,
            id='orderbook_restart'
        )
        
        self._scheduled_jobs["orderbook_restart"] = job.id
        self._logger.info("오더북 수집기 자정 재시작이 스케줄링되었습니다.")
    
    async def _restart_orderbook_job(self):
        """스케줄러에 의해 자정에 실행되는 오더북 재시작 작업"""
        try:
            self._logger.info("일일 자정 작업 - 오더북 수집기 재시작을 시작합니다...")
            await self.restart_process("ob_collector", "일일 정기 재시작")
            self._logger.info("오더북 수집기 자정 재시작이 완료되었습니다.")
        except Exception as e:
            self._logger.error(f"오더북 수집기 자정 재시작 중 오류: {str(e)}")
            # 추적 정보 로깅
            self._logger.error(traceback.format_exc())
    
    def _schedule_status_update(self):
        """시스템 상태 업데이트 스케줄링 (5분마다)"""
        # 기존 작업이 있다면 제거
        if "status_update" in self._scheduled_jobs:
            self.scheduler.remove_job(self._scheduled_jobs["status_update"])
        
        # 5분마다 실행
        job = self.scheduler.add_job(
            self._status_update_job,
            'interval',
            minutes=5,
            id='status_update'
        )
        
        self._scheduled_jobs["status_update"] = job.id
    
    async def _status_update_job(self):
        """시스템 상태 업데이트 작업"""
        try:
            # 시스템 상태 수집
            status = self._collect_system_status()
            
            # 이벤트 버스에 상태 발행
            if self.event_bus:
                await self.event_bus.publish(EventType.STATUS_UPDATE, status)
        except Exception as e:
            self._logger.error(f"시스템 상태 업데이트 중 오류: {str(e)}")
    
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
                       None이면 start_func이 객체일 경우 해당 객체에서 중지 메서드를 찾음
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
    
    async def _process_command(self, data: Dict):
        """
        명령 이벤트 처리
        
        Args:
            data: 명령 이벤트 데이터
        """
        if not isinstance(data, dict):
            return
            
        command = data.get("command")
        args = data.get("args", {})
        source = data.get("source", "unknown")
        request_id = data.get("request_id")
        
        if not command:
            return
            
        self._logger.info(f"명령 수신: {command} (소스: {source})")
        
        # 명령 핸들러 실행
        handler = self._command_handlers.get(command)
        if handler:
            try:
                result = await handler(args)
                
                # 요청 ID가 있으면 응답 전송
                if request_id:
                    await self._send_command_response(command, result, request_id)
                    
            except Exception as e:
                self._logger.error(f"명령 '{command}' 처리 중 오류: {str(e)}")
                self._logger.error(traceback.format_exc())
                
                # 오류 이벤트 발행
                await self._publish_error_event(
                    f"명령 처리 오류: {str(e)}",
                    source="orchestrator",
                    severity="error"
                )
                
                # 오류 응답 전송
                if request_id:
                    await self._send_command_response(
                        command, 
                        {"success": False, "error": str(e)}, 
                        request_id
                    )
        else:
            self._logger.warning(f"알 수 없는 명령: {command}")
            
            # 알 수 없는 명령 응답
            if request_id:
                await self._send_command_response(
                    command, 
                    {"success": False, "error": f"알 수 없는 명령: {command}"}, 
                    request_id
                )
    
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
    
    # 명령 핸들러
    async def _handle_start_process(self, args: Dict) -> Dict:
        """
        프로세스 시작 명령 처리
        
        Args:
            args: 명령 인자
        
        Returns:
            Dict: 처리 결과
        """
        process_name = args.get("process_name")
        if not process_name:
            return {"success": False, "error": "프로세스 이름이 필요합니다."}
            
        result = await self.start_process(process_name, "명령으로 시작")
        return {"success": result, "process_name": process_name}
    
    async def _handle_stop_process(self, args: Dict) -> Dict:
        """
        프로세스 중지 명령 처리
        
        Args:
            args: 명령 인자
        
        Returns:
            Dict: 처리 결과
        """
        process_name = args.get("process_name")
        if not process_name:
            return {"success": False, "error": "프로세스 이름이 필요합니다."}
            
        result = await self.stop_process(process_name, "명령으로 중지")
        return {"success": result, "process_name": process_name}
    
    async def _handle_restart_process(self, args: Dict) -> Dict:
        """
        프로세스 재시작 명령 처리
        
        Args:
            args: 명령 인자
        
        Returns:
            Dict: 처리 결과
        """
        process_name = args.get("process_name")
        if not process_name:
            return {"success": False, "error": "프로세스 이름이 필요합니다."}
            
        result = await self.restart_process(process_name, "명령으로 재시작")
        return {"success": result, "process_name": process_name}
    
    async def _handle_get_status(self, args: Dict) -> Dict:
        """
        상태 조회 명령 처리
        
        Args:
            args: 명령 인자
        
        Returns:
            Dict: 시스템 상태
        """
        return self._collect_system_status()
    
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
