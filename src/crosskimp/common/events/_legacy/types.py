"""
이벤트 타입 및 상수 정의 모듈

이 모듈은 이벤트 시스템에서 사용되는 모든 상수와 타입을 정의합니다.
- 이벤트 우선순위
- 이벤트 그룹
- 이벤트 타입
- 이벤트 별칭 (이전 버전 호환성)
"""

from enum import Enum, auto

# 이벤트 우선순위 정의
class EventPriority(Enum):
    """이벤트 처리 우선순위 정의"""
    HIGH = 0        # 높은 우선순위 (즉시 처리가 필요한 중요 이벤트)
    NORMAL = 1      # 일반 우선순위 (대부분의 이벤트)
    LOW = 2         # 낮은 우선순위 (지연 처리 가능한 이벤트)
    BACKGROUND = 3  # 백그라운드 처리 (시스템 부하가 적을 때 처리)

# 이벤트 그룹 정의
class EventGroup:
    """이벤트 그룹 상수"""
    SYSTEM = "system"           # 시스템 수준 이벤트
    CONNECTION = "connection"   # 연결 관련 이벤트
    DATA = "data"               # 데이터 관련 이벤트
    EXCHANGE = "exchange"       # 거래소 관련 이벤트
    METRIC = "metric"           # 메트릭/측정 관련 이벤트
    NOTIFICATION = "notification" # 알림 관련 이벤트
    ERROR = "error"             # 오류 관련 이벤트
    TRADING = "trading"         # 거래 관련 이벤트
    RADAR = "radar"             # 레이더(기회 탐지) 관련 이벤트
    TELEGRAM = "telegram"       # 텔레그램 관련 이벤트
    SERVER = "server"           # 서버/웹 관련 이벤트

# 컴포넌트 정의
class Component:
    """시스템 컴포넌트 상수"""
    ORDERBOOK = "orderbook"           # 오더북 수집기 (표준 이름)
    CALCULATOR = "calculator"          # 차익 계산기
    TRADER = "trader"                  # 거래 실행기
    RADAR = "radar"                    # 기회 탐지기
    TELEGRAM = "telegram"              # 텔레그램 봇
    SERVER = "server"                  # 웹 서버
    SYSTEM = "system"                  # 시스템 관리자
    
    # 내부 구현명 매핑
    _IMPL_NAMES = {
        "orderbook": "orderbook_collector"
    }
    
    @classmethod
    def get_impl_name(cls, component_name):
        """컴포넌트의 실제 구현 이름 반환"""
        if isinstance(component_name, str):
            return cls._IMPL_NAMES.get(component_name, component_name)
        return component_name

# 이벤트 타입 정의 - 체계적 분류
class EventTypes:
    """모든 이벤트 타입 정의"""
    
    # 시스템 이벤트 (system)
    SYSTEM_STARTUP = "system:startup"                 # 시스템 시작
    SYSTEM_SHUTDOWN = "system:shutdown"               # 시스템 종료
    SYSTEM_HEARTBEAT = "system:heartbeat"             # 시스템 하트비트
    SYSTEM_STATUS_CHANGE = "system:status_change"     # 시스템 상태 변경
    SYSTEM_EVENT = "system:event"                     # 일반 시스템 이벤트
    PROCESS_STATUS = "system:process_status"          # 프로세스 상태 변경
    RESOURCE_USAGE = "system:resource_usage"          # 리소스 사용량
    COMPONENT_INITIALIZED = "system:component_initialized"  # 컴포넌트 초기화 완료
    COMPONENT_ERROR = "system:component_error"        # 컴포넌트 오류
    CONFIG_UPDATED = "system:config_updated"          # 설정 업데이트
    
    # 연결 관련 이벤트 (connection)
    CONNECTION_STATUS = "connection:status"           # 연결 상태 변경
    CONNECTION_ATTEMPT = "connection:attempt"         # 연결 시도
    CONNECTION_SUCCESS = "connection:success"         # 연결 성공
    CONNECTION_FAILURE = "connection:failure"         # 연결 실패
    
    # 구독 관련 이벤트 (connection)
    SUBSCRIPTION_STATUS = "connection:subscription_status"  # 구독 상태 변경
    SUBSCRIPTION_SUCCESS = "connection:subscription_success"  # 구독 성공
    SUBSCRIPTION_FAILURE = "connection:subscription_failure"  # 구독 실패
    
    # 메시지 관련 이벤트 (data)
    MESSAGE_RECEIVED = "data:message_received"        # 메시지 수신
    MESSAGE_PROCESSED = "data:message_processed"      # 메시지 처리 완료
    BATCH_COMPLETED = "data:batch_completed"          # 배치 처리 완료
    
    # 데이터 관련 이벤트 (data)
    DATA_UPDATED = "data:updated"                     # 데이터 업데이트
    DATA_SNAPSHOT = "data:snapshot"                   # 데이터 스냅샷
    DATA_DELTA = "data:delta"                         # 데이터 변경
    MARKET_DATA_UPDATED = "data:market_data_updated"  # 시장 데이터 업데이트
    ORDERBOOK_UPDATED = "data:orderbook_updated"      # 오더북 업데이트
    
    # 거래소 관련 이벤트 (exchange)
    EXCHANGE_HEALTH_UPDATE = "exchange:health_update"  # 거래소 건강 상태 요약
    EXCHANGE_STATUS_CHANGE = "exchange:status_change"  # 거래소 상태 변경
    EXCHANGE_RATE_LIMIT = "exchange:rate_limit"        # 거래소 속도 제한 도달
    
    # 메트릭 관련 이벤트 (metric)
    METRIC_UPDATE = "metric:update"                   # 메트릭 업데이트
    PERFORMANCE_METRIC = "metric:performance"         # 성능 측정
    LATENCY_REPORT = "metric:latency"                 # 지연 시간
    
    # 오류 관련 이벤트 (error)
    ERROR_EVENT = "error:general"                     # 일반 오류 이벤트
    ERROR_CRITICAL = "error:critical"                 # 중요 오류 이벤트
    ERROR_WARNING = "error:warning"                   # 경고 이벤트
    ERROR_NETWORK = "error:network"                   # 네트워크 오류
    ERROR_EXCHANGE = "error:exchange"                 # 거래소 오류
    
    # 알림 관련 이벤트 (notification)
    NOTIFICATION_EVENT = "notification:general"        # 일반 알림 이벤트
    ALERT_EVENT = "notification:alert"                # 경고 알림 이벤트
    INFO_NOTIFICATION = "notification:info"           # 정보 알림
    
    # 거래 관련 이벤트 (trading)
    TRADE_OPPORTUNITY = "trading:opportunity"         # 거래 기회 발견
    TRADE_DECISION = "trading:decision"               # 거래 결정
    TRADE_EXECUTION = "trading:execution"             # 거래 실행
    TRADE_COMPLETED = "trading:completed"             # 거래 완료
    TRADE_FAILED = "trading:failed"                   # 거래 실패
    POSITION_UPDATED = "trading:position_updated"     # 포지션 업데이트
    BALANCE_UPDATED = "trading:balance_updated"       # 잔고 업데이트
    
    # 레이더 관련 이벤트 (radar)
    OPPORTUNITY_DETECTED = "radar:opportunity_detected"  # 기회 감지
    OPPORTUNITY_EVALUATION = "radar:opportunity_evaluation"  # 기회 평가
    OPPORTUNITY_EXPIRED = "radar:opportunity_expired"    # 기회 만료
    
    # 텔레그램 관련 이벤트 (telegram)
    TELEGRAM_MESSAGE_RECEIVED = "telegram:message_received"  # 텔레그램 메시지 수신
    TELEGRAM_COMMAND = "telegram:command"                    # 텔레그램 명령
    TELEGRAM_NOTIFICATION_SENT = "telegram:notification_sent"  # 텔레그램 알림 전송
    
    # 서버/웹 관련 이벤트 (server)
    SERVER_REQUEST = "server:request"                 # 서버 요청
    SERVER_RESPONSE = "server:response"               # 서버 응답
    UI_INTERACTION = "server:ui_interaction"          # UI 상호작용
    API_CALL = "server:api_call"                      # API 호출

# 이전 버전 호환성을 위한 이벤트 타입 별칭
EVENT_ALIASES = {
    "connection_status": EventTypes.CONNECTION_STATUS,
    "metric_update": EventTypes.METRIC_UPDATE,
    "error_event": EventTypes.ERROR_EVENT,
    "subscription_status": EventTypes.SUBSCRIPTION_STATUS,
    "message_received": EventTypes.MESSAGE_RECEIVED,
    "message_processed": EventTypes.MESSAGE_PROCESSED,
    "data_updated": EventTypes.DATA_UPDATED,
    "data_snapshot": EventTypes.DATA_SNAPSHOT,
    "data_delta": EventTypes.DATA_DELTA,
    "system_startup": EventTypes.SYSTEM_STARTUP,
    "system_shutdown": EventTypes.SYSTEM_SHUTDOWN,
    "notification_event": EventTypes.NOTIFICATION_EVENT
}

# 이벤트 우선순위 기본값 (이벤트 타입별)
EVENT_PRIORITIES = {
    # 높은 우선순위 이벤트
    EventTypes.SYSTEM_SHUTDOWN: EventPriority.HIGH,
    EventTypes.ERROR_CRITICAL: EventPriority.HIGH,
    EventTypes.ALERT_EVENT: EventPriority.HIGH,
    EventTypes.TRADE_OPPORTUNITY: EventPriority.HIGH,
    EventTypes.TRADE_EXECUTION: EventPriority.HIGH,
    
    # 낮은 우선순위 이벤트
    EventTypes.SYSTEM_HEARTBEAT: EventPriority.LOW,
    EventTypes.METRIC_UPDATE: EventPriority.LOW,
    EventTypes.PERFORMANCE_METRIC: EventPriority.LOW,
    
    # 백그라운드 우선순위 이벤트
    EventTypes.RESOURCE_USAGE: EventPriority.LOW,
}

# 기본 우선순위 (명시적으로 지정되지 않은 이벤트용)
DEFAULT_PRIORITY = EventPriority.NORMAL 