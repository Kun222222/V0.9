"""
process_events 이벤트 모듈

이 모듈은 process_events 관련 이벤트 정의와 핸들러를 포함합니다.
"""

import time
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.system_eventbus import get_event_bus
from crosskimp.common.events.system_types import EventChannels

# 로거 설정
logger = get_unified_logger()


@dataclass
class ProcessStatusEvent:
    """프로세스 상태 변경 이벤트"""
    process_name: str
    status: str
    event_type: str
    error_message: Optional[str] = field(default_factory=dict)
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=lambda: __import__('time').time())  # 이벤트 발생 시간

async def publish_process_status_event(
    event_bus, 
    process_name: str,
    status: str,
    event_type: str,
    error_message: Optional[str],
    details: Dict[str, Any],
) -> None:
    """
    프로세스 상태 변경 이벤트
    
    Args:
        event_bus: 이벤트 버스 인스턴스
        process_name: str 타입의 process_name
        status: str 타입의 status
        event_type: str 타입의 event_type
        error_message: Optional[str] 타입의 error_message
        details: Dict[str, Any] 타입의 details
    """
    # 이벤트 데이터 생성
    event_data = ProcessStatusEvent(
        process_name=process_name,
        status=status,
        event_type=event_type,
        error_message=error_message,
        details=details,
    )
    
    # 이벤트 발행
    await event_bus.publish(
        EventChannels.Process.STATUS, 
        event_data.__dict__
    )


@dataclass
class ProcessErrorEvent:
    """프로세스 오류 이벤트"""
    process_name: str
    error_message: str
    error_type: str
    stack_trace: Optional[str] = field(default_factory=dict)
    timestamp: float = field(default_factory=lambda: __import__('time').time())  # 이벤트 발생 시간

async def publish_process_error_event(
    event_bus, 
    process_name: str,
    error_message: str,
    error_type: str,
    stack_trace: Optional[str],
) -> None:
    """
    프로세스 오류 이벤트
    
    Args:
        event_bus: 이벤트 버스 인스턴스
        process_name: str 타입의 process_name
        error_message: str 타입의 error_message
        error_type: str 타입의 error_type
        stack_trace: Optional[str] 타입의 stack_trace
    """
    # 이벤트 데이터 생성
    event_data = ProcessErrorEvent(
        process_name=process_name,
        error_message=error_message,
        error_type=error_type,
        stack_trace=stack_trace,
    )
    
    # 이벤트 발행
    await event_bus.publish(
        EventChannels.Process.ERROR, 
        event_data.__dict__
    )


async def handle_process_status(data: Dict[str, Any]) -> None:
    """
    프로세스 상태 변경 이벤트
    
    Args:
        data: 이벤트 데이터 - 다음 필드를 포함합니다:
            * process_name (str): process_name 필드
            * status (str): status 필드
            * event_type (str): event_type 필드
            * error_message (Optional[str]): error_message 필드
            * details (Dict[str, Any]): details 필드
    """
    # 이벤트 데이터 추출
    process_name = data.get("process_name", "")
    status = data.get("status", "")
    event_type = data.get("event_type", "")
    error_message = data.get("error_message", None)
    details = data.get("details", {})
    
    # TODO: 이벤트 처리 로직 구현
    logger.info(f"프로세스 상태 변경 이벤트 수신됨")

# 이벤트 핸들러 등록 코드
def register_process_status_handler(event_bus) -> None:
    """
    프로세스 상태 변경 이벤트 핸들러를 등록합니다.
    
    Args:
        event_bus: 이벤트 버스 인스턴스
    """
    event_bus.register_handler(
        EventChannels.Process.STATUS, 
        handle_process_status
    )


async def handle_process_error(data: Dict[str, Any]) -> None:
    """
    프로세스 오류 이벤트
    
    Args:
        data: 이벤트 데이터 - 다음 필드를 포함합니다:
            * process_name (str): process_name 필드
            * error_message (str): error_message 필드
            * error_type (str): error_type 필드
            * stack_trace (Optional[str]): stack_trace 필드
    """
    # 이벤트 데이터 추출
    process_name = data.get("process_name", "")
    error_message = data.get("error_message", "")
    error_type = data.get("error_type", "")
    stack_trace = data.get("stack_trace", None)
    
    # TODO: 이벤트 처리 로직 구현
    logger.info(f"프로세스 오류 이벤트 수신됨")

# 이벤트 핸들러 등록 코드
def register_process_error_handler(event_bus) -> None:
    """
    프로세스 오류 이벤트 핸들러를 등록합니다.
    
    Args:
        event_bus: 이벤트 버스 인스턴스
    """
    event_bus.register_handler(
        EventChannels.Process.ERROR, 
        handle_process_error
    )
