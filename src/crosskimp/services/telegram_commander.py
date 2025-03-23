"""
텔레그램 봇 어댑터 모듈

이 모듈은 텔레그램 봇 명령을 이벤트 버스에 연결하고, 
이벤트 버스의 메시지를 텔레그램 알림으로 변환합니다.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events import EventTypes, EventType, get_event_bus

# 로거 설정
logger = get_unified_logger()

# 싱글톤 인스턴스
_telegram_commander_instance = None

def get_telegram_commander():
    """글로벌 텔레그램 커맨더 인스턴스를 반환합니다."""
    global _telegram_commander_instance
    if _telegram_commander_instance is None:
        _telegram_commander_instance = TelegramCommander()
    return _telegram_commander_instance

class TelegramCommander:
    """
    텔레그램 봇과 이벤트 버스를 연결하는 어댑터 클래스
    """
    
    def __init__(self):
        """텔레그램 어댑터 초기화"""
        # 이벤트 버스만 참조하고 컨트롤러 의존성 제거
        self.event_bus = None
        
        # 텔레그램 봇 서비스 참조 (필요 시 구현)
        self.telegram_service = None
        
        # 초기화 완료 여부
        self.initialized = False
        
        # 상태 메시지 캐시
        self.last_status_message = None
        
        # 응답 대기 Future 저장
        self._pending_responses = {}
        
        # 로거
        self._logger = logger
    
    async def initialize(self):
        """텔레그램 어댑터 초기화"""
        if self.initialized:
            return
            
        # 순환 참조 방지 - 이벤트 버스만 가져옴
        self.event_bus = get_event_bus()
        
        # 텔레그램 봇 관련 코드 제거
        # 필요시 여기에 텔레그램 봇 구현 추가
        
        # 이벤트 핸들러 등록
        self.event_bus.register_handler(EventType.STATUS_UPDATE, self._handle_status_update)
        self.event_bus.register_handler(EventType.ERROR, self._handle_error)
        self.event_bus.register_handler(EventType.PROCESS_START, self._handle_process_event)
        self.event_bus.register_handler(EventType.PROCESS_STOP, self._handle_process_event)
        self.event_bus.register_handler(EventType.COMMAND, self._handle_command_response)
        
        # 텔레그램 명령어 핸들러 - 봇 연동 시 구현
        # self._setup_telegram_handlers()
        
        self.initialized = True
        self._logger.info("텔레그램 커맨더가 초기화되었습니다.")
    
    async def shutdown(self):
        """텔레그램 커맨더 종료"""
        if not self.initialized:
            return
            
        # 이벤트 핸들러 등록 해제
        if self.event_bus:
            self.event_bus.unregister_handler(EventType.STATUS_UPDATE, self._handle_status_update)
            self.event_bus.unregister_handler(EventType.ERROR, self._handle_error)
            self.event_bus.unregister_handler(EventType.PROCESS_START, self._handle_process_event)
            self.event_bus.unregister_handler(EventType.PROCESS_STOP, self._handle_process_event)
            self.event_bus.unregister_handler(EventType.COMMAND, self._handle_command_response)
            
        # 대기 중인 응답 취소
        for future in self._pending_responses.values():
            if not future.done():
                future.cancel()
        
        self.initialized = False
        self._logger.info("텔레그램 커맨더가 종료되었습니다.")
    
    def _setup_telegram_handlers(self):
        """텔레그램 명령어 핸들러 설정 (나중에 직접 구현)"""
        self._logger.info("텔레그램 명령어 핸들러가 등록되었습니다.")
    
    # 텔레그램 명령어 핸들러 - 간소화
    async def _cmd_start_ob(self, args: List[str], chat_id: int) -> str:
        """오더북 수집기 시작 명령"""
        await self._publish_command("start_process", {"process_name": "ob_collector"})
        return "오더북 수집기 시작 명령을 전송했습니다."
    
    async def _cmd_stop_ob(self, args: List[str], chat_id: int) -> str:
        """오더북 수집기 중지 명령"""
        await self._publish_command("stop_process", {"process_name": "ob_collector"})
        return "오더북 수집기 중지 명령을 전송했습니다."
    
    async def _cmd_restart_ob(self, args: List[str], chat_id: int) -> str:
        """오더북 수집기 재시작 명령"""
        await self._publish_command("restart_process", {"process_name": "ob_collector"})
        return "오더북 수집기 재시작 명령을 전송했습니다."
    
    async def _cmd_start_radar(self, args: List[str], chat_id: int) -> str:
        """레이더 시작 명령"""
        await self._publish_command("start_process", {"process_name": "radar"})
        return "레이더 시작 명령을 전송했습니다."
    
    async def _cmd_stop_radar(self, args: List[str], chat_id: int) -> str:
        """레이더 중지 명령"""
        await self._publish_command("stop_process", {"process_name": "radar"})
        return "레이더 중지 명령을 전송했습니다."
    
    async def _cmd_restart_radar(self, args: List[str], chat_id: int) -> str:
        """레이더 재시작 명령"""
        await self._publish_command("restart_process", {"process_name": "radar"})
        return "레이더 재시작 명령을 전송했습니다."
    
    async def _cmd_status(self, args: List[str], chat_id: int) -> str:
        """시스템 상태 확인 명령"""
        # 시스템 컨트롤러 직접 호출 대신 이벤트로 요청
        request_id = id(args) # 고유 요청 ID 생성
        
        # 응답 대기를 위한 Future 생성
        response_future = asyncio.Future()
        self._pending_responses[request_id] = response_future
        
        # 상태 정보 요청
        await self.event_bus.publish(EventType.COMMAND, {
            "command": "get_status",
            "args": {},
            "source": "telegram",
            "request_id": request_id
        })
        
        try:
            # 응답 대기 (5초 타임아웃)
            status = await asyncio.wait_for(response_future, timeout=5.0)
            
            # 응답받은 상태로 메시지 구성
            status_msg = "📊 시스템 상태 보고:\n\n"
            
            # 시스템 가동 상태
            status_msg += f"시스템: {'✅ 실행 중' if status['system_running'] else '❌ 중지됨'}\n"
            
            # 프로세스 상태
            status_msg += "\n📋 프로세스 상태:\n"
            for name, proc_status in status["processes"].items():
                running = proc_status.get("running", False)
                uptime = proc_status.get("uptime", 0)
                restart_count = proc_status.get("restart_count", 0)
                
                # 가동 시간 포맷팅
                hours, remainder = divmod(int(uptime), 3600)
                minutes, seconds = divmod(remainder, 60)
                uptime_str = f"{hours}시간 {minutes}분 {seconds}초"
                
                status_msg += f"- {name}: {'✅ 실행 중' if running else '❌ 중지됨'}"
                if running:
                    status_msg += f" (가동시간: {uptime_str})"
                if restart_count > 0:
                    status_msg += f" (재시작: {restart_count}회)"
                status_msg += "\n"
            
            # 통계
            status_msg += f"\n⏱ 가동 시간: {int(status['stats']['uptime'] // 3600)}시간 {int((status['stats']['uptime'] % 3600) // 60)}분\n"
            
            # 이벤트 버스 통계
            bus_stats = status.get("event_bus_stats", {})
            status_msg += f"\n📨 이벤트 통계:\n"
            status_msg += f"- 발행: {bus_stats.get('published_events', 0)}\n"
            status_msg += f"- 처리: {bus_stats.get('processed_events', 0)}\n"
            status_msg += f"- 오류: {bus_stats.get('errors', 0)}\n"
            
            return status_msg
            
        except asyncio.TimeoutError:
            # 타임아웃 발생 시
            if request_id in self._pending_responses:
                del self._pending_responses[request_id]
            return "⚠️ 상태 정보 요청 시간 초과. 시스템이 응답하지 않습니다."
        
        except Exception as e:
            # 기타 오류 발생 시
            self._logger.error(f"상태 조회 중 오류: {str(e)}")
            if request_id in self._pending_responses:
                del self._pending_responses[request_id]
            return f"⚠️ 상태 정보 조회 중 오류 발생: {str(e)}"
    
    async def _publish_command(self, command: str, args: Dict = None):
        """
        명령 이벤트 발행
        
        Args:
            command: 명령어
            args: 명령어 인자
        """
        if not self.event_bus:
            self._logger.error("이벤트 버스가 초기화되지 않았습니다.")
            return
            
        await self.event_bus.publish(EventType.COMMAND, {
            "command": command,
            "args": args or {},
            "source": "telegram"
        })
    
    # 이벤트 핸들러 - 로깅만 구현
    async def _handle_status_update(self, data: Dict):
        """
        상태 업데이트 이벤트 처리
        
        Args:
            data: 상태 이벤트 데이터
        """
        # 주기적인 상태 업데이트는 텔레그램으로 전송하지 않음
        # 필요한 경우 여기서 중요한 상태 변경만 알림으로 전송
        pass
    
    async def _handle_error(self, data: Dict):
        """
        오류 이벤트 처리
        
        Args:
            data: 오류 이벤트 데이터
        """
        message = data.get("message", "알 수 없는 오류")
        source = data.get("source", "unknown")
        severity = data.get("severity", "error")
        
        # 로깅만 하고 나중에 필요시 텔레그램 알림 구현
        self._logger.error(f"오류 발생: [{severity.upper()}] {source} - {message}")
    
    async def _handle_process_event(self, data: Dict):
        """
        프로세스 이벤트 처리
        
        Args:
            data: 프로세스 이벤트 데이터
        """
        event_type = data.get("event_type")
        process_name = data.get("process_name")
        description = data.get("description", process_name)
        was_error = data.get("was_error", False)
        
        if not process_name:
            return
            
        # 프로세스 이벤트 로깅
        if event_type == EventType.PROCESS_START.name:
            self._logger.info(f"프로세스 시작됨: {description}")
        elif event_type == EventType.PROCESS_STOP.name:
            status = "오류로 인해 중지됨" if was_error else "중지됨"
            self._logger.info(f"프로세스 {status}: {description}")
    
    async def _handle_command_response(self, data: Dict):
        """
        명령 응답 이벤트 처리
        
        Args:
            data: 명령 응답 데이터
        """
        # 응답 데이터인지 확인
        if data.get("is_response") and "request_id" in data:
            request_id = data["request_id"]
            
            # 해당 요청 ID의 Future가 있는지 확인
            if request_id in self._pending_responses:
                future = self._pending_responses.pop(request_id)
                if not future.done():
                    # 응답 데이터를 Future에 설정
                    future.set_result(data.get("data", {}))
    
    async def send_system_notification(self, message: str, level: str = "info"):
        """
        시스템 알림 로깅 (텔레그램 연동 시 확장)
        
        Args:
            message: 알림 메시지
            level: 알림 레벨 (info, warning, error)
        """
        if level == "info":
            self._logger.info(f"시스템 알림: {message}")
        elif level == "warning":
            self._logger.warning(f"시스템 경고: {message}")
        elif level == "error":
            self._logger.error(f"시스템 오류: {message}")
    
    def is_initialized(self) -> bool:
        """
        초기화 완료 여부 확인
        
        Returns:
            bool: 초기화 완료 여부
        """
        return self.initialized
