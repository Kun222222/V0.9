"""
크로스 김프 아비트리지 - 시스템 이벤트 타입 모듈

이 모듈은 이벤트 버스 시스템에서 사용되는 모든 이벤트 경로와 값을 정의합니다.
- EventChannels: 이벤트 발행/구독에 사용되는 채널 경로 (네임스페이스 패턴)
- EventValues: 이벤트 데이터 내에 포함되는 상태값
"""

from enum import Enum
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

####################################
# 이벤트 데이터 클래스
####################################
@dataclass
class ProcessStatusEvent:
    """
    프로세스 상태 이벤트 데이터 클래스
    
    프로세스의 상태 변경 시 발행되는 이벤트의 데이터 구조를 정의합니다.
    이 클래스를 사용하면 타입 체크와 필수 필드 검증이 가능해집니다.
    
    사용 예시:
        event_data = ProcessStatusEvent(
            process_name="ob_collector",
            status=EventValues.PROCESS_RUNNING,  # 상태값
            event_type="process/started"
        )
        await event_bus.publish(EventChannels.Process.STATUS, event_data.__dict__)  # 채널
    """
    process_name: str  # 프로세스 이름 (필수)
    status: str        # 프로세스 상태 (필수) - EventValues.PROCESS_* 상수 사용
    event_type: str    # 이벤트 유형 (필수) - 예: "process/started", "process/stopped" 등
    error_message: Optional[str] = None  # 오류 메시지 (선택, 오류 시에만 사용)
    details: Dict[str, Any] = field(default_factory=dict)  # 추가 상세 정보 (선택)
    timestamp: float = field(default_factory=lambda: __import__('time').time())  # 이벤트 발생 시간 (자동 생성)

####################################
# 이벤트 채널 (발행/구독 경로)
####################################
class EventChannels:
    """
    이벤트 채널 - 구독 및 발행에 사용되는 경로
    
    이벤트 버스의 publish/subscribe 기능에 사용되는 채널 경로를 정의합니다.
    '우체통 번호'의 개념으로, 이 채널을 통해 이벤트를 발행하고 구독합니다.
    
    네임스페이스 패턴으로 구성되어 각 카테고리별로 체계적으로 정리되어 있습니다.
    
    사용 방법:
        # 이벤트 발행
        await event_bus.publish(EventChannels.System.ERROR, data)
        
        # 이벤트 구독
        event_bus.register_handler(EventChannels.Process.STATUS, handler)
    """
    
    class System:
        """시스템 수준 채널"""
        ERROR = "system/error"         # 시스템 오류 채널
        STATUS = "system/status"       # 시스템 상태 채널
    
    class Process:
        """프로세스 관리 채널"""
        COMMAND_START = "process/command/start"     # 프로세스 시작 명령 채널
        COMMAND_STOP = "process/command/stop"       # 프로세스 중지 명령 채널
        COMMAND_RESTART = "process/command/restart" # 프로세스 재시작 명령 채널
        STATUS = "process/status"           # 프로세스 상태 변경 채널
    
    class Component:
        """컴포넌트별 채널 기본 클래스"""
        
        class ObCollector:
            """오더북 수집기 관련 채널"""
            RUNNING = "component/ob_collector/running"             # 오더북 구동 완료 채널
            METRICS = "component/ob_collector/metrics"             # 오더북 메트릭 채널
            CONNECTION = "component/ob_collector/connection"       # 오더북 연결 상태 채널
            CONNECTION_LOST = "component/ob_collector/connection_lost"  # 웹소켓 연결 끊김 채널
            EXCHANGE_STATUS = "component/ob_collector/exchange_status"  # 거래소별 상태 채널

####################################
# 이벤트 값 (이벤트 데이터 내 상태값)
####################################
class EventValues:
    """
    이벤트 값 - 이벤트 데이터에 포함되는 상태 값
    
    이벤트 메시지 내부에 포함되는 구체적인 상태값을 정의합니다.
    '편지 내용'의 개념으로, 이벤트 데이터의 status 필드 등에 사용됩니다.
    
    사용 방법:
        # 프로세스 상태 이벤트 생성 시
        event_data = {
            "process_name": "ob_collector",
            "status": EventValues.PROCESS_RUNNING  # 상태값으로 사용
        }
    """
    
    # 프로세스 상태값
    PROCESS_STOPPED = "process/stopped"    # 프로세스 정지됨 (상태값)
    PROCESS_STARTING = "process/starting"  # 프로세스 시작 중 (상태값)
    PROCESS_RUNNING = "process/running"    # 프로세스 실행 중 (상태값)
    PROCESS_STOPPING = "process/stopping"  # 프로세스 정지 중 (상태값)
    PROCESS_ERROR = "process/error"        # 프로세스 오류 상태 (상태값)
