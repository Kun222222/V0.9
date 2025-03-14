"""
오더북 수집기에서 사용하는 공통 상수 정의
"""

from enum import Enum, auto


class MessageType(Enum):
    """메시지 타입 열거형"""
    SNAPSHOT = "snapshot"
    DELTA = "delta"
    PING = "ping"
    PONG = "pong"
    SUBSCRIBE = "subscribe"
    SUBSCRIBE_RESPONSE = "subscribe_response"
    ERROR = "error"
    INFO = "info"
    UNKNOWN = "unknown"


class WebSocketState(Enum):
    """웹소켓 상태 열거형"""
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    SUBSCRIBING = auto()
    SUBSCRIBED = auto()
    RECONNECTING = auto()
    ERROR = auto()


class Exchange(Enum):
    """거래소 코드 열거형"""
    BINANCE = "binance"
    BINANCE_FUTURE = "binance_future"
    BYBIT = "bybit"
    BYBIT_FUTURE = "bybit_future"
    UPBIT = "upbit"
    BITHUMB = "bithumb"


# 거래소 한글 이름 매핑
EXCHANGE_NAMES_KR = {
    Exchange.BINANCE.value: "바이낸스",
    Exchange.BINANCE_FUTURE.value: "바이낸스선물",
    Exchange.BYBIT.value: "바이빗",
    Exchange.BYBIT_FUTURE.value: "바이빗선물",
    Exchange.UPBIT.value: "업비트",
    Exchange.BITHUMB.value: "빗썸",
}

# 웹소켓 상태 이모지 매핑
STATUS_EMOJIS = {
    "CONNECTED": "🟢",
    "CONNECTING": "🟡",
    "DISCONNECTED": "🔴",
    "DISCONNECTING": "🟠",
    "ERROR": "❌",
    "SUBSCRIBING": "🔄",
    "SUBSCRIBED": "✅",
    "WARNING": "⚠️",
    "INFO": "ℹ️",
    "SNAPSHOT": "📸",
    "MESSAGE": "📨",
}

# 웹소켓 URL 설정
WEBSOCKET_URLS = {
    Exchange.BINANCE.value: "wss://stream.binance.com:9443/ws",
    Exchange.BINANCE_FUTURE.value: "wss://fstream.binance.com/ws",
    Exchange.BYBIT.value: "wss://stream.bybit.com/v5/public/spot",
    Exchange.BYBIT_FUTURE.value: "wss://stream.bybit.com/v5/public/linear",
    Exchange.UPBIT.value: "wss://api.upbit.com/websocket/v1",
    Exchange.BITHUMB.value: "wss://pubwss.bithumb.com/pub/ws",
}

# REST API URL 설정
REST_API_URLS = {
    Exchange.BINANCE.value: "https://api.binance.com",
    Exchange.BINANCE_FUTURE.value: "https://fapi.binance.com",
    Exchange.BYBIT.value: "https://api.bybit.com",
    Exchange.BYBIT_FUTURE.value: "https://api.bybit.com",
    Exchange.UPBIT.value: "https://api.upbit.com",
    Exchange.BITHUMB.value: "https://api.bithumb.com",
}

# 기본 오더북 깊이 설정
DEFAULT_DEPTHS = {
    Exchange.BINANCE.value: 500,
    Exchange.BINANCE_FUTURE.value: 500,
    Exchange.BYBIT.value: 50,
    Exchange.BYBIT_FUTURE.value: 50,
    Exchange.UPBIT.value: 15,
    Exchange.BITHUMB.value: 30,
}

# 핑 간격 설정 (초)
PING_INTERVALS = {
    Exchange.BINANCE.value: 20,
    Exchange.BINANCE_FUTURE.value: 20,
    Exchange.BYBIT.value: 20,
    Exchange.BYBIT_FUTURE.value: 20,
    Exchange.UPBIT.value: 30,
    Exchange.BITHUMB.value: 30,
}

# 핑 타임아웃 설정 (초)
PING_TIMEOUTS = {
    Exchange.BINANCE.value: 10,
    Exchange.BINANCE_FUTURE.value: 10,
    Exchange.BYBIT.value: 10,
    Exchange.BYBIT_FUTURE.value: 10,
    Exchange.UPBIT.value: 15,
    Exchange.BITHUMB.value: 15,
}

# 구독 청크 크기 설정
SUBSCRIBE_CHUNK_SIZES = {
    Exchange.BINANCE.value: 10,
    Exchange.BINANCE_FUTURE.value: 10,
    Exchange.BYBIT.value: 10,
    Exchange.BYBIT_FUTURE.value: 10,
    Exchange.UPBIT.value: 5,
    Exchange.BITHUMB.value: 5,
}

# 구독 딜레이 설정 (초)
SUBSCRIBE_DELAYS = {
    Exchange.BINANCE.value: 1.0,
    Exchange.BINANCE_FUTURE.value: 1.0,
    Exchange.BYBIT.value: 1.0,
    Exchange.BYBIT_FUTURE.value: 1.0,
    Exchange.UPBIT.value: 0.5,
    Exchange.BITHUMB.value: 0.5,
}

# 거래소별 Ping-Pong 메시지 형식
PING_FORMATS = {
    Exchange.BINANCE.value: {"method": "ping"},
    Exchange.BINANCE_FUTURE.value: {"method": "ping"},
    Exchange.BYBIT.value: {"op": "ping"},
    Exchange.BYBIT_FUTURE.value: {"op": "ping"},
    Exchange.UPBIT.value: {"type": "ping"},
    Exchange.BITHUMB.value: {"type": "ping"},
}

# 거래소별 Pong 메시지 형식 (응답 확인용)
PONG_PATTERNS = {
    Exchange.BINANCE.value: {"result": None},
    Exchange.BINANCE_FUTURE.value: {"result": None},
    Exchange.BYBIT.value: {"op": "pong"},
    Exchange.BYBIT_FUTURE.value: {"op": "pong"},
    Exchange.UPBIT.value: {"type": "pong"},
    Exchange.BITHUMB.value: {"type": "pong"},
} 