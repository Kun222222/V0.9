"""
시스템 명령 핸들러 모듈

이 모듈은 시스템 명령 이벤트를 처리하고 응답을 생성합니다.
오케스트레이터 및 텔레그램 커맨더와 연동하여 명령을 처리합니다.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List, Callable, Union
from datetime import datetime
import traceback
import uuid

from crosskimp.common.logger.logger import get_unified_logger, get_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.events.sys_event_bus import EventType
from crosskimp.common.events import get_event_bus

# 로거 설정
logger = get_logger(__name__, component=SystemComponent.MAIN_SYSTEM.value)

# 싱글톤 인스턴스
_command_handler_instance = None

def get_command_handler():
    """글로벌 명령 핸들러 인스턴스를 반환합니다."""
    global _command_handler_instance
    if _command_handler_instance is None:
        _command_handler_instance = CommandHandler()
    return _command_handler_instance

class CommandHandler:
    """
    시스템 명령 핸들러 클래스
    
    시스템 명령을 이벤트 버스를 통해 수신하고 처리합니다.
    명령 등록, 처리, 응답 전송 기능을 제공합니다.
    """
    
    def __init__(self):
        """명령 핸들러 초기화"""
        # 초기화 완료 여부
        self.initialized = False
        
        # 이벤트 버스 참조
        self.event_bus = None
        
        # 오케스트레이터 참조 (나중에 설정)
        self.orchestrator = None
        
        # 로거
        self._logger = logger
        
        # 명령 핸들러 매핑
        self._command_handlers = {}
        
        # 응답 대기 매핑 (request_id -> Future)
        self._pending_responses = {}
    
    async def initialize(self):
        """명령 핸들러 초기화"""
        if self.initialized:
            return
            
        # 이벤트 버스 참조
        self.event_bus = get_event_bus()
        
        # 이벤트 핸들러 등록
        if self.event_bus:
            self.event_bus.register_handler(EventType.COMMAND, self._process_command)
        
        # 표준 명령 핸들러 등록
        self._register_standard_handlers()
        
        self.initialized = True
        self._logger.info("명령 핸들러가 초기화되었습니다.")
    
    def set_orchestrator(self, orchestrator):
        """
        오케스트레이터 참조 설정
        
        Args:
            orchestrator: 오케스트레이터 인스턴스
        """
        self.orchestrator = orchestrator
        self._logger.debug("오케스트레이터 참조가 설정되었습니다.")
    
    async def shutdown(self):
        """명령 핸들러 종료"""
        if not self.initialized:
            return
            
        # 이벤트 핸들러 등록 해제
        if self.event_bus:
            self.event_bus.unregister_handler(EventType.COMMAND, self._process_command)
        
        # 대기 중인 응답 취소
        for future in self._pending_responses.values():
            if not future.done():
                future.cancel()
        
        self.initialized = False
        self._logger.info("명령 핸들러가 종료되었습니다.")
    
    def _register_standard_handlers(self):
        """기본 명령 핸들러 등록"""
        self.register_command_handler("start_process", self._handle_start_process)
        self.register_command_handler("stop_process", self._handle_stop_process)
        self.register_command_handler("restart_process", self._handle_restart_process)
        self.register_command_handler("get_status", self._handle_get_status)
        self._logger.debug("기본 명령 핸들러가 등록되었습니다.")
    
    def register_command_handler(self, command: str, handler: Callable):
        """
        명령 핸들러 등록
        
        Args:
            command: 명령 이름
            handler: 핸들러 함수 (coroutine)
        """
        self._command_handlers[command] = handler
        self._logger.debug(f"명령 핸들러 등록: {command}")
    
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
                    source="command_handler",
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
        명령 전송 (편의 메서드)
        
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
            self._logger.error("이벤트 버스가 초기화되지 않았습니다.")
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
                self._logger.warning(f"명령 응답 시간 초과: {command}")
                if request_id in self._pending_responses:
                    del self._pending_responses[request_id]
                return {"success": False, "error": "응답 시간 초과"}
            except Exception as e:
                self._logger.error(f"명령 응답 대기 중 오류: {str(e)}")
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
    
    # 표준 명령 핸들러
    async def _handle_start_process(self, args: Dict) -> Dict:
        """
        프로세스 시작 명령 처리
        
        Args:
            args: 명령 인자
        
        Returns:
            Dict: 처리 결과
        """
        if not self.orchestrator:
            return {"success": False, "error": "오케스트레이터가 설정되지 않았습니다."}
            
        process_name = args.get("process_name")
        if not process_name:
            return {"success": False, "error": "프로세스 이름이 필요합니다."}
            
        result = await self.orchestrator.start_process(process_name, "명령으로 시작")
        return {"success": result, "process_name": process_name}
    
    async def _handle_stop_process(self, args: Dict) -> Dict:
        """
        프로세스 중지 명령 처리
        
        Args:
            args: 명령 인자
        
        Returns:
            Dict: 처리 결과
        """
        if not self.orchestrator:
            return {"success": False, "error": "오케스트레이터가 설정되지 않았습니다."}
            
        process_name = args.get("process_name")
        if not process_name:
            return {"success": False, "error": "프로세스 이름이 필요합니다."}
            
        result = await self.orchestrator.stop_process(process_name, "명령으로 중지")
        return {"success": result, "process_name": process_name}
    
    async def _handle_restart_process(self, args: Dict) -> Dict:
        """
        프로세스 재시작 명령 처리
        
        Args:
            args: 명령 인자
        
        Returns:
            Dict: 처리 결과
        """
        if not self.orchestrator:
            return {"success": False, "error": "오케스트레이터가 설정되지 않았습니다."}
            
        process_name = args.get("process_name")
        if not process_name:
            return {"success": False, "error": "프로세스 이름이 필요합니다."}
            
        result = await self.orchestrator.restart_process(process_name, "명령으로 재시작")
        return {"success": result, "process_name": process_name}
    
    async def _handle_get_status(self, args: Dict) -> Dict:
        """
        상태 조회 명령 처리
        
        Args:
            args: 명령 인자
        
        Returns:
            Dict: 시스템 상태
        """
        if not self.orchestrator:
            return {"success": False, "error": "오케스트레이터가 설정되지 않았습니다."}
            
        return self.orchestrator._collect_system_status()
    
    def is_initialized(self) -> bool:
        """
        초기화 완료 여부 확인
        
        Returns:
            bool: 초기화 완료 여부
        """
        return self.initialized 