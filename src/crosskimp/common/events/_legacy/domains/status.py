"""
상태 관리 관련 이벤트 타입 정의

이 모듈은 시스템 상태 관리에 관련된 모든 이벤트 타입을 정의합니다.
"""

from typing import Dict


class StatusEventTypes:
    """
    상태 관리 관련 이벤트 타입

    시스템 상태, 서비스 상태, 리소스 상태 등 상태 관리와 관련된
    모든 이벤트 타입을 상수로 정의합니다.
    """
    # 시스템 이벤트
    SYSTEM_STARTUP = "system:startup"
    SYSTEM_SHUTDOWN = "system:shutdown"
    SYSTEM_STATUS_CHANGE = "system:status_change"
    
    # 프로세스 관련 이벤트
    PROCESS_STATUS = "process:status"
    
    # 리소스 관련 이벤트
    RESOURCE_USAGE = "resource:usage"
    
    # 거래소 관련 이벤트
    CONNECTION_STATUS = "connection:status"
    EXCHANGE_HEALTH_UPDATE = "exchange:health_update"
    
    # 메트릭 관련 이벤트
    METRIC_UPDATE = "metric:update"
    
    # 오류 관련 이벤트
    ERROR_EVENT = "error:event"


# 이전 이벤트 타입에서 새 이벤트 타입으로의 매핑
EVENT_TYPE_MAPPING: Dict[str, str] = {
    "SYSTEM_STARTUP": StatusEventTypes.SYSTEM_STARTUP,
    "SYSTEM_SHUTDOWN": StatusEventTypes.SYSTEM_SHUTDOWN,
    "SYSTEM_STATUS_CHANGE": StatusEventTypes.SYSTEM_STATUS_CHANGE,
    "PROCESS_STATUS": StatusEventTypes.PROCESS_STATUS,
    "RESOURCE_USAGE": StatusEventTypes.RESOURCE_USAGE,
    "CONNECTION_STATUS": StatusEventTypes.CONNECTION_STATUS,
    "EXCHANGE_HEALTH_UPDATE": StatusEventTypes.EXCHANGE_HEALTH_UPDATE,
    "METRIC_UPDATE": StatusEventTypes.METRIC_UPDATE,
    "ERROR_EVENT": StatusEventTypes.ERROR_EVENT
} 