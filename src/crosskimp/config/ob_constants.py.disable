"""
오더북 전용 상수 정의 모듈

이 모듈은 오더북 수집 및 처리에 필요한 모든 상수를 정의합니다.
- 거래소 식별자
- 거래소 한글 이름
- 웹소켓 설정
- 오더북 깊이 설정
- 메시지 처리 관련 상수

이 파일은 기존 constants.py에서 오더북 관련 상수들을 분리하여 만들어졌습니다.
"""

from enum import Enum

# ============================
# 로깅 메시지 타입
# ============================
class LogMessageType(Enum):
    """로깅 메시지 타입 정의"""
    INFO = "info"
    ERROR = "error"
    WARNING = "warning"
    DEBUG = "debug"
    CRITICAL = "critical"
    CONNECTION = "connection"
    RECONNECT = "reconnect"
    DISCONNECT = "disconnect"
    TRADE = "trade"
    MARKET = "market"
    SYSTEM = "system"

# ============================
# 거래소 식별자
# ============================
class Exchange(Enum):
    """
    거래소 식별자
    
    이 열거형은 지원되는 모든 거래소의 식별자를 정의합니다.
    코드 전체에서 일관된 거래소 식별을 위해 사용됩니다.
    """
    BINANCE = "binance"        # 바이낸스 현물
    BYBIT = "bybit"            # 바이빗 현물
    UPBIT = "upbit"            # 업비트 현물
    BITHUMB = "bithumb"        # 빗썸 현물
    BINANCE_FUTURE = "binance_future"  # 바이낸스 선물
    BYBIT_FUTURE = "bybit_future"      # 바이빗 선물

# ============================
# 거래소 한글 이름
# ============================
EXCHANGE_NAMES_KR = {
    Exchange.BINANCE.value: "[바이낸스]",
    Exchange.BYBIT.value: "[바이빗]",
    Exchange.UPBIT.value: "[업비트]",
    Exchange.BITHUMB.value: "[빗썸]",
    Exchange.BINANCE_FUTURE.value: "[바이낸스 선물]",
    Exchange.BYBIT_FUTURE.value: "[바이빗 선물]",
}

# ============================
# 웹소켓 상태 코드
# ============================
class WebSocketState:
    """
    웹소켓 연결 상태 코드
    
    웹소켓 연결의 다양한 상태를 정의합니다.
    """
    CONNECTING = 0      # 연결 시도 중
    CONNECTED = 1       # 연결됨
    DISCONNECTING = 2   # 연결 종료 중
    DISCONNECTED = 3    # 연결 종료됨
    ERROR = 4           # 오류 발생
    RECONNECTING = 5    # 재연결 시도 중

# 웹소켓 상태 이모지
STATUS_EMOJIS = {
    "CONNECTED": "🟢",      # 연결됨
    "CONNECTING": "🟡",     # 연결 시도 중
    "DISCONNECTED": "⚪",   # 연결 종료됨
    "ERROR": "🔴",          # 오류 발생
    "RECONNECTING": "🟠"    # 재연결 시도 중
}

# ============================
# 웹소켓 공통 설정
# ============================
WEBSOCKET_COMMON_CONFIG = {
    "reconnect_delay_initial": 1.0,    # 초기 재연결 딜레이 (초)
    "reconnect_delay_max": 60.0,       # 최대 재연결 딜레이 (초)
    "reconnect_delay_multiplier": 1.5, # 재연결 딜레이 증가 배수
    "max_reconnect_attempts": 0,       # 최대 재연결 시도 횟수 (0=무제한)
    "message_timeout": 60,             # 메시지 타임아웃 (초)
    "health_check_interval": 30,       # 헬스 체크 간격 (초)
    "log_raw_messages": False,         # 원시 메시지 로깅 여부
    "log_raw_message_sample_rate": 0.01, # 원시 메시지 로깅 샘플링 비율
    "log_message_preview_length": 200,  # 로깅할 메시지 미리보기 길이
    
    # 재연결 전략 설정
    "reconnect": {
        "initial_delay": 1.0,          # 초기 재연결 딜레이 (초)
        "max_delay": 60.0,             # 최대 재연결 딜레이 (초)
        "multiplier": 2.0,             # 재연결 딜레이 증가 배수
        "max_attempts": 0              # 최대 재연결 시도 횟수 (0=무제한)
    }
}

# ============================
# 거래소별 웹소켓 설정
# ============================
WEBSOCKET_CONFIG = {
    Exchange.UPBIT.value: {
        # 웹소켓 URL
        "ws_url": "wss://api.upbit.com/websocket/v1",
        
        # REST API URL
        "api_urls": {
            "market": "https://api.upbit.com/v1/market/all",
            "ticker": "https://api.upbit.com/v1/ticker"
        },
        
        # 오더북 설정
        "depth_levels": 15,            # 오더북 깊이 레벨 (표시할 호가 수)
        "default_depth": 15,           # 기본 오더북 깊이
        
        # 웹소켓 연결 설정
        "ping_interval": 60,           # 핑 전송 간격 (초)
        "ping_timeout": 120,           # 핑 응답 대기 시간 (초)
        "update_speed_ms": 100,        # 업데이트 속도 (밀리초)
        "pong_response": '{"status":"UP"}'  # 업비트 PONG 응답 형식
    },
    
    Exchange.BITHUMB.value: {
        # 웹소켓 URL
        "ws_url": "wss://pubwss.bithumb.com/pub/ws",
        
        # REST API URL
        "api_urls": {
            "ticker": "https://api.bithumb.com/public/ticker/ALL_KRW"
        },
        
        # 오더북 설정
        "depth_levels": 15,            # 오더북 깊이 레벨 (표시할 호가 수)
        "default_depth": 15,           # 기본 오더북 깊이
        "symbol_suffix": "_KRW",       # 심볼 접미사
        "message_type_depth": "orderbookdepth",  # 오더북 메시지 타입
        
        # 웹소켓 연결 설정
        "ping_interval": 30,           # 핑 전송 간격 (초)
        "ping_timeout": 60,            # 핑 응답 대기 시간 (초)
        "update_speed_ms": 100,        # 업데이트 속도 (밀리초)
        "tick_types": ["1M"],          # 틱 타입 (1분)
        "message_timeout": 30,         # 메시지 수신 타임아웃 (초)
        "log_sample_count": 5,         # 로깅할 샘플 메시지 수
        "log_message_preview_length": 200,  # 로깅할 메시지 미리보기 길이
    },
    
    Exchange.BINANCE.value: {
        # 웹소켓 URL
        "ws_url": "wss://stream.binance.com:9443/ws",
        
        # REST API URL
        "api_urls": {
            "spot": "https://api.binance.com/api/v3/exchangeInfo",
            "depth": "https://api.binance.com/api/v3/depth"
        },
        
        # 오더북 설정
        "depth_levels": 20,            # 오더북 깊이 레벨 (표시할 호가 수)
        "default_depth": 500,          # 기본 오더북 깊이
        "depth_update_stream": "@depth@100ms",  # 깊이 업데이트 스트림 형식
        
        # 웹소켓 연결 설정
        "ping_interval": 20,           # 핑 전송 간격 (초)
        "ping_timeout": 20,            # 핑 응답 대기 시간 (초)
        "update_speed_ms": 100,        # 업데이트 속도 (밀리초)
        "subscribe_chunk_size": 10,    # 한 번에 구독할 심볼 수
        "subscribe_delay": 1,          # 구독 요청 간 딜레이 (초)
        "health_check_interval": 30,   # 헬스 체크 간격 (초)
    },
    
    Exchange.BINANCE_FUTURE.value: {
        # 웹소켓 URL
        "ws_url": "wss://fstream.binance.com/ws",
        "ws_base_url": "wss://fstream.binance.com/stream?streams=",  # 스트림 URL
        
        # REST API URL
        "api_urls": {
            "future": "https://fapi.binance.com/fapi/v1/exchangeInfo",
            "depth": "https://fapi.binance.com/fapi/v1/depth"
        },
        
        # 오더북 설정
        "depth_levels": 20,            # 오더북 깊이 레벨 (표시할 호가 수)
        "default_depth": 500,          # 기본 오더북 깊이
        
        # 웹소켓 연결 설정
        "ping_interval": 20,           # 핑 전송 간격 (초)
        "ping_timeout": 20,            # 핑 응답 대기 시간 (초)
        "update_speed": "100ms",       # 웹소켓 업데이트 속도 (100ms, 250ms, 500ms 중 선택)
        "max_retry_count": 3,          # 최대 재시도 횟수
        "snapshot_retry_delay": 1,     # 스냅샷 요청 재시도 초기 딜레이 (초)
        "snapshot_request_timeout": 10, # 스냅샷 요청 타임아웃 (초)
        "dns_cache_ttl": 300,          # DNS 캐시 TTL (초)
    },
    
    Exchange.BYBIT.value: {
        # 웹소켓 URL
        "ws_url": "wss://stream.bybit.com/v5/public/spot",
        
        # REST API URL
        "api_urls": {
            "spot": "https://api.bybit.com/v5/market/instruments-info?category=spot"
        },
        
        # 오더북 설정
        "depth_levels": 20,            # 오더북 깊이 레벨 (표시할 호가 수)
        "default_depth": 50,           # 기본 오더북 깊이
        "snapshot_interval": 3,        # 스냅샷 간격 (초)
        
        # 웹소켓 연결 설정
        "ping_interval": 20,           # 핑 전송 간격 (초)
        "ping_timeout": 10,            # 핑 응답 대기 시간 (초)
        "update_speed_ms": 100,        # 업데이트 속도 (밀리초)
        "max_symbols_per_subscription": 10,  # 한 번에 구독할 최대 심볼 수
    },
    
    Exchange.BYBIT_FUTURE.value: {
        # 웹소켓 URL
        "ws_url": "wss://stream.bybit.com/v5/public/linear",
        
        # REST API URL
        "api_urls": {
            "future": "https://api.bybit.com/v5/market/instruments-info?category=linear"
        },
        
        # 오더북 설정
        "depth_levels": 20,            # 오더북 깊이 레벨 (표시할 호가 수)
        "default_depth": 50,           # 기본 오더북 깊이
        "snapshot_interval": 3,        # 스냅샷 간격 (초)
        
        # 웹소켓 연결 설정
        "ping_interval": 20,           # 핑 전송 간격 (초)
        "ping_timeout": 10,            # 핑 응답 대기 시간 (초)
        "update_speed_ms": 100,        # 업데이트 속도 (밀리초)
        "max_symbols_per_batch": 10,   # 한 번에 구독할 최대 심볼 수
    }
}

# ============================
# 웹소켓 클래스 매핑
# ============================
EXCHANGE_CLASS_MAP = {
    Exchange.BINANCE.value: "BinanceSpotWebsocket",
    Exchange.BINANCE_FUTURE.value: "BinanceFutureWebsocket",
    Exchange.BYBIT.value: "BybitSpotWebsocket",
    Exchange.BYBIT_FUTURE.value: "BybitFutureWebsocket",
    Exchange.UPBIT.value: "UpbitWebsocket",
    Exchange.BITHUMB.value: "BithumbSpotWebsocket"
}

# ============================
# 오더북 매니저 클래스 매핑
# ============================
ORDERBOOK_MANAGER_MAP = {
    Exchange.BINANCE.value: "BinanceSpotOrderBookManager",
    Exchange.BINANCE_FUTURE.value: "BinanceFutureOrderBookManagerV2",
    Exchange.BYBIT.value: "BybitSpotOrderBookManager",
    Exchange.BYBIT_FUTURE.value: "BybitFutureOrderBookManager",
    Exchange.UPBIT.value: "UpbitOrderBookManager",
    Exchange.BITHUMB.value: "BithumbSpotOrderBookManager"
}

# ============================
# 메트릭 관련 상수
# ============================
METRICS_CONFIG = {
    "delay_threshold_ms": 1000,        # 지연 경고 임계값 (밀리초)
    "ping_interval": 30,               # 핑 전송 간격 (초)
    "pong_timeout": 10,                # 핑 응답 타임아웃 (초)
    "health_threshold": 0.8,           # 헬스 체크 임계값 (0.0~1.0)
    "metrics_save_interval": 60,       # 메트릭 저장 간격 (초)
    "alert_check_interval": 300,       # 알림 체크 간격 (초)
    "alert_cooldown": 1800,            # 알림 쿨다운 (초)
    "max_delay_alert_ms": 5000,        # 최대 지연 알림 임계값 (밀리초)
    "min_message_rate": 0.5,           # 최소 메시지 비율 (정상 대비)
    "max_error_rate": 0.1,             # 최대 오류 비율
}

# ============================
# 로깅 관련 상수
# ============================
LOG_SYSTEM = "[시스템]" 