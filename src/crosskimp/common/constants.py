"""
CrossKimp 시스템 전체에서 사용되는 공통 상수 정의

이 모듈은 여러 컴포넌트에서 공통으로 사용되는 상수들을 중앙에서 관리합니다.
"""

class Components:
    """시스템 컴포넌트 이름 상수"""
    ORDERBOOK = "orderbook"
    TELEGRAM = "telegram"
    SYSTEM = "system"
    TRADER = "trader"
    CALCULATOR = "calculator"
    RADAR = "radar"
    SERVER = "server"

class EventTypes:
    """이벤트 타입 상수"""
    # 오더북 연결 관련 이벤트
    OB_CONNECTION_STATUS = "orderbook:connection_status"
    OB_CONNECTION_ATTEMPT = "orderbook:connection_attempt"
    OB_CONNECTION_SUCCESS = "orderbook:connection_success"
    OB_CONNECTION_FAILURE = "orderbook:connection_failure"
    OB_CONNECTION_CLOSED = "orderbook:connection_closed"
    
    # 오더북 구독 관련 이벤트
    OB_SUBSCRIPTION_STATUS = "orderbook:subscription_status"
    OB_SUBSCRIPTION_REQUEST = "orderbook:subscription_request"
    OB_SUBSCRIPTION_SUCCESS = "orderbook:subscription_success"
    OB_SUBSCRIPTION_FAILURE = "orderbook:subscription_failure"
    OB_UNSUBSCRIBE_REQUEST = "orderbook:unsubscribe_request"
    
    # 오더북 메시지 관련 이벤트
    OB_MESSAGE_RECEIVED = "orderbook:message_received"
    OB_MESSAGE_PROCESSED = "orderbook:message_processed"
    
    # 오더북 데이터 이벤트
    OB_SNAPSHOT_RECEIVED = "orderbook:snapshot_received"
    OB_DELTA_RECEIVED = "orderbook:delta_received"
    OB_ORDERBOOK_UPDATED = "orderbook:updated"
    OB_TICKER_UPDATED = "orderbook:ticker_updated"
    
    # 오더북 오류 이벤트
    OB_ERROR = "orderbook:error"
    OB_VALIDATION_ERROR = "orderbook:validation_error"
    OB_WEBSOCKET_ERROR = "orderbook:websocket_error"
    OB_PARSING_ERROR = "orderbook:parsing_error"
    
    # 오더북 메트릭 이벤트
    OB_EXCHANGE_METRIC = "orderbook:exchange_metric"
    OB_LATENCY_METRIC = "orderbook:latency_metric"
    
    # 오더북 마켓 이벤트
    OB_MARKET_STATUS = "orderbook:market_status"
    OB_SYMBOL_STATUS = "orderbook:symbol_status"
    
    # 시스템 이벤트
    SYS_STARTUP = "system:startup"
    SYS_SHUTDOWN = "system:shutdown"
    SYS_STATUS_CHANGE = "system:status_change"
    SYS_PROCESS_STATUS = "system:process_status"
    SYS_RESOURCE_USAGE = "system:resource_usage"
    
    # 텔레그램 이벤트
    TG_BOT_STARTUP = "telegram:bot_startup"
    TG_BOT_SHUTDOWN = "telegram:bot_shutdown"
    TG_NOTIFICATION_SENT = "telegram:notification_sent"
    TG_COMMAND_RECEIVED = "telegram:command_received"
    TG_COMMAND_EXECUTED = "telegram:command_executed"
    TG_BOT_ERROR = "telegram:bot_error"

class Priorities:
    """이벤트 우선순위 상수"""
    HIGH = 0
    NORMAL = 1
    LOW = 2
    BACKGROUND = 3

class ExchangeConstants:
    """거래소 관련 상수"""
    # 연결 설정 (기본값)
    DEFAULT_PING_INTERVAL = 30  # 핑 전송 간격 (초)
    DEFAULT_PING_TIMEOUT = 10   # 핑 응답 타임아웃 (초)
    DEFAULT_MESSAGE_TIMEOUT = 60  # 메시지 타임아웃 (초)
    DEFAULT_RECONNECT_DELAY = 0.1  # 초기 재연결 시도 시간 (초)
    DEFAULT_CONNECTION_TIMEOUT = 3  # 연결 타임아웃 (초)
    DEFAULT_MAX_RECONNECT_DELAY = 60.0  # 최대 재연결 지연 시간
    DEFAULT_RECONNECT_MULTIPLIER = 2.0  # 지연 시간 증가 배수

    # 바이낸스 연결 설정
    BINANCE_WS_URL = "wss://stream.binance.com/ws"  # 현물
    BINANCE_F_WS_URL = "wss://fstream.binance.com/ws"  # 선물
    
    # 바이빗 연결 설정
    BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/spot"  # 현물
    BYBIT_F_WS_URL = "wss://stream.bybit.com/v5/public/linear"  # 선물
    BYBIT_TESTNET_WS_URL = "wss://stream-testnet.bybit.com/v5/public/spot"  # 테스트넷
    BYBIT_F_TESTNET_WS_URL = "wss://stream-testnet.bybit.com/v5/public/linear"  # 테스트넷
    
    # 업비트 연결 설정
    UPBIT_WS_URL = "wss://api.upbit.com/websocket/v1"
    
    # 빗썸 연결 설정
    BITHUMB_WS_URL = "wss://ws-api.bithumb.com/websocket/v1"

class ErrorMessages:
    """오류 메시지 상수"""
    CONNECT_TIMEOUT = "연결 타임아웃"
    CONNECT_ERROR = "연결 오류"
    SUBSCRIBE_ERROR = "구독 오류"
    WEBSOCKET_CLOSED = "웹소켓 연결이 종료됨"
    MAX_RECONNECT_ATTEMPTS = "최대 재연결 시도 횟수 초과"
    PARSE_ERROR = "메시지 파싱 오류"
    VALIDATION_ERROR = "데이터 검증 오류"
