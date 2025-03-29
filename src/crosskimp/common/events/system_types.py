"""
크로스 김프 아비트리지 - 시스템 이벤트 타입 모듈

이 모듈은 이벤트 버스 시스템에서 사용되는 모든 이벤트 경로 상수를 정의합니다.
계층적 구조를 가진 경로 문자열을 통해 이벤트를 카테고리화합니다.
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
            status=EventPaths.PROCESS_STATUS_RUNNING,
            event_type="process/started"
        )
        await event_bus.publish(EventPaths.PROCESS_STATUS, event_data.__dict__)
    """
    process_name: str  # 프로세스 이름 (필수)
    status: str        # 프로세스 상태 (필수) - EventPaths.PROCESS_STATUS_* 상수 사용
    event_type: str    # 이벤트 유형 (필수) - 예: "process/started", "process/stopped" 등
    error_message: Optional[str] = None  # 오류 메시지 (선택, 오류 시에만 사용)
    details: Dict[str, Any] = field(default_factory=dict)  # 추가 상세 정보 (선택)
    timestamp: float = field(default_factory=lambda: __import__('time').time())  # 이벤트 발생 시간 (자동 생성)

####################################
# 이벤트 경로 체계 (계층 구조)
####################################
class EventPaths:
    """
    이벤트 경로 체계 - 계층적 문자열 상수
    
    이벤트 버스에서 사용되는 모든 이벤트의 경로 상수를 정의합니다.
    계층적 구조로 체계화되어 이벤트의 소속과 목적을 명확히 합니다.
    
    경로 구조: <카테고리>/<서브카테고리>/<이벤트이름>
    
    사용 방법:
        # 이벤트 발행
        await event_bus.publish(EventPaths.SYSTEM_ERROR, data)
        
        # 이벤트 구독
        event_bus.register_handler(EventPaths.PROCESS_START, handler)
    
    주요 이벤트 카테고리:
    1. system/* - 시스템 전체 수준의 이벤트 (시작, 종료 등)
    2. process/* - 프로세스 관리 이벤트 (시작/종료 명령)
    3. component/* - 컴포넌트별 상태 및 기능 이벤트
    """
    
    ###################################
    # 시스템 레벨 이벤트
    ###################################
    # 시스템 이벤트: 전체 애플리케이션 수준의 이벤트
    # 주로 시스템 글로벌 상태에 관련됨
    SYSTEM_ERROR = "system/error"                    # 시스템 오류
    SYSTEM_STATUS = "system/status"                  # 시스템 상태
    
    ###################################
    # 프로세스 공통 이벤트
    ###################################
    # 프로세스 명령 이벤트: 오케스트레이터가 프로세스 생명주기를 관리하기 위해 발행하는 명령 이벤트
    # "이 프로세스를 시작/중지하라"는 명령의 성격을 가짐
    PROCESS_COMMAND_START = "process/start"           # 프로세스 시작 명령 - 오케스트레이터가 발행
    PROCESS_COMMAND_STOP = "process/stop"             # 프로세스 중지 명령 - 오케스트레이터가 발행
    PROCESS_COMMAND_RESTART = "process/restart"       # 프로세스 재시작 명령 - 오케스트레이터가 발행
    PROCESS_STATUS = "process/status"                # 프로세스 상태 변경 - 프로세스 핸들러가 발행
    PROCESS_ERROR = "process/error"                  # 프로세스 오류 - 프로세스 핸들러가 발행
    
    ###################################
    # 프로세스 상태 값
    ###################################
    # 프로세스 상태: 프로세스의 현재 실행 상태를 나타내는 값
    PROCESS_STATUS_STOPPED = "process/stopped"    # 중지됨
    PROCESS_STATUS_STARTING = "process/starting"  # 시작 중
    PROCESS_STATUS_RUNNING = "process/running"    # 실행 중
    PROCESS_STATUS_STOPPING = "process/stopping"  # 종료 중
    PROCESS_STATUS_ERROR = "process/error"        # 오류 상태
    
    ###################################
    # 컴포넌트별 이벤트 (오더북 수집기)
    ###################################
    # 컴포넌트별 이벤트: 각 컴포넌트가 자신의 상태 변화를 알리기 위해 발행하는 이벤트
    # 세부적인 모니터링과 알림에 활용됨
    
    # 오더북 수집기 이벤트
    OB_COLLECTOR_RUNNING = "component/ob_collector/running"        # 오더북 구동 완료됨
    OB_COLLECTOR_METRICS = "component/ob_collector/metrics"        # 오더북 메트릭 데이터
    OB_COLLECTOR_CONNECTION = "component/ob_collector/connection"  # 오더북 연결 상태
    OB_COLLECTOR_CONNECTION_LOST = "component/ob_collector/connection_lost"  # 오더북 연결 끊김
    OB_COLLECTOR_EXCHANGE_STATUS = "component/ob_collector/exchange_status"  # 거래소별 상태 정보
