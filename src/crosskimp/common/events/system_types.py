"""
크로스 김프 아비트리지 - 시스템 이벤트 타입 모듈

이 모듈은 이벤트 버스 시스템에서 사용되는 모든 상수 및 타입을 정의합니다.
이벤트 유형, 우선순위, 상태 등의 열거형들이 포함되어 있습니다.
"""

from enum import Enum, auto

####################################
# 최상위 이벤트 카테고리 (프로그램별 분류)
####################################
class EventCategory(Enum):
    """이벤트 카테고리 (프로그램별 분류)"""
    SYSTEM = "system"              # 시스템 공통 이벤트
    OB_COLLECTOR = "ob_collector"  # 오더북 수집기 이벤트
    RADAR = "radar"                # 레이더 이벤트
    TRADE = "trade"                # 거래 이벤트
    NOTIFICATION = "notification"  # 알림 이벤트
    PERFORMANCE = "performance"    # 성능 모니터링 이벤트
    TELEGRAM = "telegram"          # 텔레그램 이벤트

####################################
# 시스템 공통 이벤트 유형
####################################
class SystemEventType(Enum):
    """시스템 공통 이벤트 유형"""
    START = "start"                    # 시작
    STOP = "stop"                      # 종료
    PROCESS_CONTROL = "process_control"  # 프로세스 제어 (시작/종료/재시작)
    STATUS = "status"                  # 상태 업데이트
    ERROR = "error"                    # 오류
    WARNING = "warning"                # 경고
    INFO = "info"                      # 정보
    HEARTBEAT = "heartbeat"            # 하트비트 (생존 신호)
    COMMAND = "command"                # 일반 명령

####################################
# 오더북 수집기 이벤트 유형
####################################
class ObCollectorEventType(Enum):
    """오더북 수집기 이벤트 유형"""
    # 상태 이벤트
    STATUS = "status"                  # 상태 업데이트
    CONNECTION = "connection"          # 연결 상태
    SUBSCRIPTION = "subscription"      # 구독 상태
    
    # 일반 메트릭 
    METRIC = "metric"                  # 일반 메트릭 데이터
    
    # 연결 관련 메트릭
    CONNECTION_STATUS = "connection_status"      # 연결 상태 (성공/실패)
    CONNECTION_UPTIME = "connection_uptime"      # 연결 유지 시간
    RECONNECT_COUNT = "reconnect_count"          # 재연결 시도 횟수
    LAST_CONNECTED = "last_connected"            # 마지막 연결 시간
    
    # 메시지 관련 메트릭
    MESSAGE_COUNT = "message_count"              # 총 메시지 수
    MESSAGE_RATE = "message_rate"                # 초당 메시지 수
    LAST_MESSAGE_TIME = "last_message_time"      # 마지막 메시지 수신 시간
    ERROR_COUNT = "error_count"                  # 총 오류 수
    ERROR_RATE = "error_rate"                    # 오류 비율
    LAST_ERROR_TIME = "last_error_time"          # 마지막 오류 시간
    
    # 구독 관련 메트릭
    SUBSCRIPTION_COUNT = "subscription_count"    # 거래소별 구독 중인 심볼 수
    SUBSCRIPTION_STATUS = "subscription_status"  # 거래소별 구독 상태
    SYMBOL_UPDATES = "symbol_updates"            # 거래소별 심볼별 업데이트 정보
    
    # 시스템 상태 메트릭
    COMPONENT_STATUS = "component_status"        # 컴포넌트 상태
    START_TIME = "start_time"                    # 시작 시간
    TOTAL_RUNTIME = "total_runtime"              # 총 실행 시간

####################################
# 레이더 이벤트 유형
####################################
class RadarEventType(Enum):
    """레이더 이벤트 유형"""
    STATUS = "status"                  # 상태 업데이트
    SIGNAL = "signal"                  # 신호 발생
    METRIC = "metric"                  # 메트릭 데이터

####################################
# 거래 이벤트 유형
####################################
class TradeEventType(Enum):
    """거래 이벤트 유형"""
    ORDER_CREATED = "order_created"      # 주문 생성됨 
    ORDER_FILLED = "order_filled"        # 주문 체결됨
    ORDER_CANCELED = "order_canceled"    # 주문 취소됨
    TRADE_COMPLETED = "trade_completed"  # 거래 완료
    STATUS = "status"                    # 상태 업데이트
    METRIC = "metric"                    # 메트릭 데이터

####################################
# 알림 이벤트 유형
####################################
class NotificationEventType(Enum):
    """알림 이벤트 유형"""
    ALERT = "alert"                    # 알림
    MESSAGE = "message"                # 메시지
    COMMAND = "command"                # 명령

####################################
# 성능 이벤트 유형
####################################
class PerformanceEventType(Enum):
    """성능 이벤트 유형"""
    # 일반 성능 데이터
    CPU = "cpu"                        # CPU 사용률
    MEMORY = "memory"                  # 메모리 사용량
    LATENCY = "latency"                # 지연 시간
    THROUGHPUT = "throughput"          # 처리량
    
    # 상세 성능 메트릭
    CPU_USAGE = "cpu_usage"                  # CPU 사용률 상세
    MEMORY_USAGE = "memory_usage"            # 메모리 사용량 상세
    THREAD_COUNT = "thread_count"            # 스레드 수
    EVENT_QUEUE_SIZE = "event_queue_size"    # 이벤트 큐 크기
    PROCESSING_LATENCY = "processing_latency"# 처리 지연 시간

####################################
# 이벤트 우선순위
####################################
class EventPriority(Enum):
    """이벤트 처리 우선순위를 정의합니다."""
    LOW = 0         # 낮은 우선순위
    NORMAL = 1      # 일반 우선순위
    HIGH = 2        # 높은 우선순위
    CRITICAL = 3    # 중요 우선순위

####################################
# 텔레그램 관련 이벤트 타입
####################################
class TelegramEventType(Enum):
    """텔레그램 관련 이벤트 타입"""
    COMMAND = auto()        # 텔레그램 명령
    NOTIFICATION = auto()   # 텔레그램 알림
    USER_MESSAGE = auto()   # 사용자 메시지

