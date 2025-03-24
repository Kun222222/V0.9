"""
프로세스 관련 타입 정의

프로세스 상태, 이벤트 등 프로세스 관련 타입들을 정의합니다.
"""

from enum import Enum, auto
from typing import Dict, Any, Optional, List, Set

class ProcessStatus(Enum):
    """프로세스 상태"""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"

class ProcessEvent(Enum):
    """프로세스 이벤트 타입"""
    START_REQUESTED = "start_requested"   # 시작 요청됨
    STARTED = "started"                   # 시작됨
    STOP_REQUESTED = "stop_requested"     # 종료 요청됨
    STOPPED = "stopped"                   # 종료됨
    RESTART_REQUESTED = "restart_requested"  # 재시작 요청됨
    ERROR = "error"                       # 오류 발생

class ProcessEventData:
    """
    프로세스 이벤트 데이터
    
    프로세스 관련 이벤트에 포함되는 데이터 구조입니다.
    """
    
    def __init__(
        self, 
        process_name: str, 
        event_type: ProcessEvent,
        status: Optional[ProcessStatus] = None,
        error_message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        self.process_name = process_name
        self.event_type = event_type
        self.status = status
        self.error_message = error_message
        self.details = details or {}
        
    def to_dict(self) -> Dict[str, Any]:
        """이벤트 데이터를 딕셔너리로 변환"""
        return {
            "process_name": self.process_name,
            "event_type": self.event_type.value,
            "status": self.status.value if self.status else None,
            "error_message": self.error_message,
            "details": self.details
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProcessEventData':
        """딕셔너리에서 이벤트 데이터 생성"""
        return cls(
            process_name=data["process_name"],
            event_type=ProcessEvent(data["event_type"]),
            status=ProcessStatus(data["status"]) if data.get("status") else None,
            error_message=data.get("error_message"),
            details=data.get("details", {})
        ) 