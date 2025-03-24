"""
오더북 수집기 모듈 상수

오더북 수집기 모듈에 필요한 상수와 열거형을 정의합니다.
"""

from enum import Enum
from typing import Dict, List, Any

from crosskimp.common.config.common_constants import Exchange, EventPriority

# ============================
# 오더북 이벤트 유형
# ============================
class OrderbookEventTypes(str, Enum):
    """오더북 이벤트 유형"""
    # 연결 관련 이벤트
    CONNECTION_SUCCESS = "connection_success"
    CONNECTION_FAILED = "connection_failed"
    CONNECTION_LOST = "connection_lost"
    RECONNECT_ATTEMPT = "reconnect_attempt"
    
    # 구독 관련 이벤트
    SUBSCRIPTION_SUCCESS = "subscription_success"
    SUBSCRIPTION_FAILED = "subscription_failed"
    UNSUBSCRIBE_SUCCESS = "unsubscribe_success"
    UNSUBSCRIBE_FAILED = "unsubscribe_failed"
    
    # 데이터 관련 이벤트
    ORDERBOOK_UPDATE = "orderbook_update"
    ORDERBOOK_SNAPSHOT = "orderbook_snapshot"
    TRADE_UPDATE = "trade_update"
    
    # 오류 이벤트
    ERROR = "error"
    CONNECTION_ERROR = "connection_error"
    MESSAGE_ERROR = "message_error"
    VALIDATION_ERROR = "validation_error"
    
    # 시스템 이벤트
    SYSTEM_STARTUP = "system_startup"
    SYSTEM_SHUTDOWN = "system_shutdown"
    SYSTEM_ERROR = "system_error"
    
    # 메트릭 이벤트
    METRIC_UPDATE = "metric_update"

# ============================
# 웹소켓 상수
# ============================
# 재연결 전략 기본값
DEFAULT_RECONNECT_STRATEGY = {
    "initial_delay": 1.0,  # 초기 재연결 지연 시간
    "max_delay": 60.0,     # 최대 지연 시간
    "multiplier": 2.0,     # 지연 시간 증가 배수
    "max_attempts": 0      # 최대 시도 횟수 (0=무제한)
}

# 웹소켓 메시지 타임아웃 (초)
DEFAULT_MESSAGE_TIMEOUT = 30.0

# 핑 간격 기본값 (초)
DEFAULT_PING_INTERVAL = 30.0

# 핑 타임아웃 기본값 (초)
DEFAULT_PING_TIMEOUT = 10.0

# 웹소켓 연결 타임아웃 (초)
DEFAULT_CONNECT_TIMEOUT = 5.0


# ============================
# 메트릭 관련 상수
# ============================
# 처리율 계산 주기 (초)
METRICS_RATE_CALCULATION_INTERVAL = 1.0

# 지연 감지 임계값 (밀리초)
METRICS_DELAY_THRESHOLD_MS = 1000

# 알림 쿨다운 (초)
METRICS_ALERT_COOLDOWN = 300

# 메트릭 보관 기간 (초)
METRICS_MAX_HISTORY = 3600

# 최대 이벤트 저장 수
METRICS_MAX_EVENTS = 300

# 상태 점수 임계값
METRICS_HEALTH_THRESHOLD = 70

# ============================
# API 엔드포인트
# ============================
# 거래소별 웹소켓 엔드포인트 기본값
DEFAULT_WEBSOCKET_ENDPOINTS = {
    Exchange.BINANCE.value: "wss://stream.binance.com:9443/ws",
    Exchange.BYBIT.value: "wss://stream.bybit.com/spot/public/v3",
    Exchange.UPBIT.value: "wss://api.upbit.com/websocket/v1",
    Exchange.BITHUMB.value: "wss://pubwss.bithumb.com/pub/ws",
    Exchange.BINANCE_FUTURE.value: "wss://fstream.binance.com/ws",
    Exchange.BYBIT_FUTURE.value: "wss://stream.bybit.com/realtime_public"
}

# ============================
# 로깅 관련 상수
# ============================
# 오더북 데이터 로그 포맷
ORDERBOOK_LOG_FORMAT = "{timestamp}|{exchange}|{symbol}|{bids}|{asks}"

# 로그 최대 크기 (바이트) - 100MB
ORDERBOOK_LOG_MAX_BYTES = 100 * 1024 * 1024

# 로그 백업 개수
ORDERBOOK_LOG_BACKUP_COUNT = 100

# 로그 보관 기간 (일)
ORDERBOOK_LOG_CLEANUP_DAYS = 30

# ============================
# 상태 코드
# ============================
class OrderbookStatusCode(int, Enum):
    """오더북 상태 코드"""
    # 성공 코드
    SUCCESS = 0                 # 성공
    
    # 경고 코드
    WARNING_NO_DATA = 100       # 데이터 없음
    WARNING_STALE_DATA = 101    # 오래된 데이터
    WARNING_PARTIAL_DATA = 102  # 부분 데이터
    
    # 오류 코드
    ERROR_CONNECTION = 200      # 연결 오류
    ERROR_SUBSCRIPTION = 201    # 구독 오류
    ERROR_MESSAGE = 202         # 메시지 처리 오류
    ERROR_VALIDATION = 203      # 검증 오류
    ERROR_TIMEOUT = 204         # 타임아웃
    ERROR_RATE_LIMIT = 205      # 속도 제한
    
    # 심각한 오류 코드
    CRITICAL_SYSTEM = 300       # 시스템 오류 