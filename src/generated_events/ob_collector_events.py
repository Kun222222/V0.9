"""
ob_collector 이벤트 모듈

이 모듈은 ob_collector 관련 이벤트 정의와 핸들러를 포함합니다.
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
class ComponentObCollectorRunningEvent:
    """오더북 수집기 구동 완료 이벤트"""
    process_name: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=lambda: __import__('time').time())  # 이벤트 발생 시간

async def publish_component_ob_collector_running_event(
    event_bus, 
    process_name: str,
    message: str,
    details: Dict[str, Any],
) -> None:
    """
    오더북 수집기 구동 완료 이벤트
    
    Args:
        event_bus: 이벤트 버스 인스턴스
        process_name: str 타입의 process_name
        message: str 타입의 message
        details: Dict[str, Any] 타입의 details
    """
    # 이벤트 데이터 생성
    event_data = ComponentObCollectorRunningEvent(
        process_name=process_name,
        message=message,
        details=details,
    )
    
    # 이벤트 발행
    await event_bus.publish(
        EventChannels.OB_COLLECTOR_RUNNING, 
        event_data.__dict__
    )


@dataclass
class ComponentObCollectorConnectionLostEvent:
    """오더북 수집기 연결 끊김 이벤트"""
    process_name: str
    message: str
    exchange_name: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=lambda: __import__('time').time())  # 이벤트 발생 시간

async def publish_component_ob_collector_connection_lost_event(
    event_bus, 
    process_name: str,
    message: str,
    exchange_name: str,
    details: Dict[str, Any],
) -> None:
    """
    오더북 수집기 연결 끊김 이벤트
    
    Args:
        event_bus: 이벤트 버스 인스턴스
        process_name: str 타입의 process_name
        message: str 타입의 message
        exchange_name: str 타입의 exchange_name
        details: Dict[str, Any] 타입의 details
    """
    # 이벤트 데이터 생성
    event_data = ComponentObCollectorConnectionLostEvent(
        process_name=process_name,
        message=message,
        exchange_name=exchange_name,
        details=details,
    )
    
    # 이벤트 발행
    await event_bus.publish(
        EventChannels.Component.ObCollector.CONNECTION_LOST, 
        event_data.__dict__
    )


async def handle_component_ob_collector_running(data: Dict[str, Any]) -> None:
    """
    오더북 수집기 구동 완료 이벤트
    
    Args:
        data: 이벤트 데이터 - 다음 필드를 포함합니다:
            * process_name (str): process_name 필드
            * message (str): message 필드
            * details (Dict[str, Any]): details 필드
    """
    # 이벤트 데이터 추출
    process_name = data.get("process_name", "")
    message = data.get("message", "")
    details = data.get("details", {})
    
    # TODO: 이벤트 처리 로직 구현
    logger.info(f"오더북 수집기 구동 완료 이벤트 수신됨")

# 이벤트 핸들러 등록 코드
def register_component_ob_collector_running_handler(event_bus) -> None:
    """
    오더북 수집기 구동 완료 이벤트 핸들러를 등록합니다.
    
    Args:
        event_bus: 이벤트 버스 인스턴스
    """
    event_bus.register_handler(
        EventChannels.OB_COLLECTOR_RUNNING, 
        handle_component_ob_collector_running
    )


async def handle_component_ob_collector_connection_lost(data: Dict[str, Any]) -> None:
    """
    오더북 수집기 연결 끊김 이벤트
    
    Args:
        data: 이벤트 데이터 - 다음 필드를 포함합니다:
            * process_name (str): process_name 필드
            * message (str): message 필드
            * exchange_name (str): exchange_name 필드
            * details (Dict[str, Any]): details 필드
    """
    # 이벤트 데이터 추출
    process_name = data.get("process_name", "")
    message = data.get("message", "")
    exchange_name = data.get("exchange_name", "")
    details = data.get("details", {})
    
    # TODO: 이벤트 처리 로직 구현
    logger.info(f"오더북 수집기 연결 끊김 이벤트 수신됨")

# 이벤트 핸들러 등록 코드
def register_component_ob_collector_connection_lost_handler(event_bus) -> None:
    """
    오더북 수집기 연결 끊김 이벤트 핸들러를 등록합니다.
    
    Args:
        event_bus: 이벤트 버스 인스턴스
    """
    event_bus.register_handler(
        EventChannels.OB_COLLECTOR_CONNECTION_LOST, 
        handle_component_ob_collector_connection_lost
    )
