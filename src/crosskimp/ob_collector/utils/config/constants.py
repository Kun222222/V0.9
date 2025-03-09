"""
크로스킴프 아비트리지 봇 상수 정의

이 모듈은 프로그램 전반에서 사용되는 주요 상수들을 정의합니다.
- 설정 파일 관련 상수
- 거래소 관련 상수
- 웹소켓 관련 상수
- API 관련 상수
- 로깅 관련 상수
- 메트릭 관련 상수

텔레그램 관련 상수는 crosskimp.telegrambot.bot_constants에 정의되어 있습니다.

작성자: CrossKimp Arbitrage Bot 개발팀
최종수정: 2024.03
"""

import os
from enum import Enum

# 텔레그램 관련 상수 가져오기 (순환 참조 방지를 위해 필요한 경우에만 사용)
# from crosskimp.telegrambot.bot_constants import MessageType, MessageIcon, MESSAGE_TEMPLATES, BotCommands

# ============================
# 시스템 메시지 상수
# ============================
LOG_SYSTEM = "[시스템]"

# ============================
# 설정 파일 관련 상수
# ============================
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = "settings.json"
API_KEYS_FILE = "api_keys.json"
BACKUP_DIR = os.path.join(CONFIG_DIR, "backups")

# 설정 파일 전체 경로
SETTINGS_PATH = os.path.join(CONFIG_DIR, SETTINGS_FILE)
API_KEYS_PATH = os.path.join(CONFIG_DIR, API_KEYS_FILE)

# 프로젝트 루트 디렉토리 찾기
# 기존 방식: PROJECT_ROOT = os.path.abspath(os.path.join(CONFIG_DIR, "../../../.."))
# 수정된 방식: src 디렉토리의 상위 디렉토리를 프로젝트 루트로 설정
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../../../.."))
ENV_FILE_PATHS = [
    os.path.join(PROJECT_ROOT, ".env"),
    os.path.join(PROJECT_ROOT, "src/crosskimp/.env")
]

# API 키 환경 변수 이름
API_ENV_VARS = {
    "binance": {
        "api_key": "BINANCE_API_KEY",
        "api_secret": "BINANCE_API_SECRET"
    },
    "bybit": {
        "api_key": "BYBIT_API_KEY",
        "api_secret": "BYBIT_API_SECRET"
    },
    "bithumb": {
        "api_key": "BITHUMB_API_KEY",
        "api_secret": "BITHUMB_API_SECRET"
    },
    "upbit": {
        "api_key": "UPBIT_API_KEY",
        "api_secret": "UPBIT_API_SECRET"
    },
    "telegram": {
        "bot_token": "TELEGRAM_BOT_TOKEN",
        "command_bot_token": "TELEGRAM_COMMAND_BOT_TOKEN",
        "chat_id": "TELEGRAM_CHAT_ID"
    },
    "security": {
        "access_token_secret": "ACCESS_TOKEN_SECRET"
    },
    "user": {
        "first_superuser": "FIRST_SUPERUSER",
        "first_superuser_password": "FIRST_SUPERUSER_PASSWORD"
    }
}

# 설정 관련 타임아웃
LOAD_TIMEOUT = 10  # 초
SAVE_TIMEOUT = 5   # 초
RETRY_DELAY = 1    # 초
MAX_RETRIES = 3    # 최대 재시도 횟수

# ============================
# 거래소 관련 상수
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
    BINANCE_FUTURE = "binancefuture"  # 바이낸스 선물
    BYBIT_FUTURE = "bybitfuture"      # 바이빗 선물
    BYBIT_V2 = "bybit2"               # 바이빗 v2 API
    BYBIT_FUTURE_V2 = "bybitfuture2"  # 바이빗 선물 v2 API

# 거래소 한글 이름
EXCHANGE_NAMES_KR = {
    Exchange.BINANCE.value: "바이낸스",
    Exchange.BYBIT.value: "바이빗",
    Exchange.UPBIT.value: "업비트",
    Exchange.BITHUMB.value: "빗썸",
    Exchange.BINANCE_FUTURE.value: "바이낸스 선물",
    Exchange.BYBIT_FUTURE.value: "바이빗 선물",
    Exchange.BYBIT_V2.value: "바이빗 v2",
    Exchange.BYBIT_FUTURE_V2.value: "바이빗 선물 v2"
}

# 거래소 그룹화
EXCHANGE_GROUPS = {
    "korean": [Exchange.UPBIT.value, Exchange.BITHUMB.value],
    "global": [Exchange.BINANCE.value, Exchange.BYBIT.value],
    "spot": [Exchange.BINANCE.value, Exchange.BYBIT.value, Exchange.UPBIT.value, Exchange.BITHUMB.value],
    "futures": [Exchange.BINANCE_FUTURE.value, Exchange.BYBIT_FUTURE.value]
}

# 거래소 기본 설정
EXCHANGE_DEFAULTS = {
    Exchange.BINANCE.value: {
        "enabled": True,
        "symbols": ["BTC/USDT", "ETH/USDT"],
        "update_interval_ms": 1000
    },
    Exchange.BYBIT.value: {
        "enabled": True,
        "symbols": ["BTC/USDT", "ETH/USDT"],
        "update_interval_ms": 1000
    },
    Exchange.UPBIT.value: {
        "enabled": True,
        "symbols": ["BTC/KRW", "ETH/KRW"],
        "update_interval_ms": 1000
    },
    Exchange.BITHUMB.value: {
        "enabled": True,
        "symbols": ["BTC/KRW", "ETH/KRW"],
        "update_interval_ms": 1000
    }
}

# ============================
# 웹소켓 관련 상수
# ============================
# 웹소켓 상태 코드 및 이모지
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

# 웹소켓 상태 코드 매핑
WEBSOCKET_STATES = {
    'CONNECTING': WebSocketState.CONNECTING,
    'CONNECTED': WebSocketState.CONNECTED,
    'DISCONNECTING': WebSocketState.DISCONNECTING,
    'DISCONNECTED': WebSocketState.DISCONNECTED,
    'ERROR': WebSocketState.ERROR,
    'RECONNECTING': WebSocketState.RECONNECTING
}

# 웹소켓 URL
WEBSOCKET_URLS = {
    Exchange.UPBIT.value: "wss://api.upbit.com/websocket/v1",
    Exchange.BITHUMB.value: "wss://pubwss.bithumb.com/pub/ws",
    Exchange.BINANCE.value: "wss://stream.binance.com:9443/ws",
    Exchange.BINANCE_FUTURE.value: "wss://fstream.binance.com/ws",
    Exchange.BYBIT.value: "wss://stream.bybit.com/v5/public/spot",
    Exchange.BYBIT_FUTURE.value: "wss://stream.bybit.com/v5/public/linear"
}

# REST API URL
API_URLS = {
    Exchange.UPBIT.value: {
        "market": "https://api.upbit.com/v1/market/all",
        "ticker": "https://api.upbit.com/v1/ticker"
    },
    Exchange.BITHUMB.value: {
        "ticker": "https://api.bithumb.com/public/ticker/ALL_KRW"
    },
    Exchange.BINANCE.value: {
        "spot": "https://api.binance.com/api/v3/exchangeInfo",
        "depth": "https://api.binance.com/api/v3/depth"
    },
    Exchange.BINANCE_FUTURE.value: {
        "future": "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "depth": "https://fapi.binance.com/fapi/v1/depth"
    },
    Exchange.BYBIT.value: {
        "spot": "https://api.bybit.com/v5/market/instruments-info?category=spot"
    },
    Exchange.BYBIT_FUTURE.value: {
        "future": "https://api.bybit.com/v5/market/instruments-info?category=linear"
    }
}

# 웹소켓 설정 상수
WEBSOCKET_CONFIG = {
    Exchange.UPBIT.value: {
        "depth_levels": 15,
        "ping_interval": 60,
        "ping_timeout": 10,
        "update_speed_ms": 100
    },
    Exchange.BITHUMB.value: {
        "depth_levels": 15,
        "ping_interval": 30,
        "ping_timeout": 10,
        "update_speed_ms": 100
    },
    Exchange.BINANCE.value: {
        "depth_levels": 20,
        "ping_interval": 150,  # 바이낸스는 더 긴 핑 간격 사용
        "ping_timeout": 10,
        "update_speed_ms": 100
    },
    Exchange.BINANCE_FUTURE.value: {
        "depth_levels": 20,
        "ping_interval": 150,  # 바이낸스는 더 긴 핑 간격 사용
        "ping_timeout": 10,
        "update_speed_ms": 100
    },
    Exchange.BYBIT.value: {
        "depth_levels": 25,
        "ping_interval": 20,
        "ping_timeout": 10,
        "update_speed_ms": 100
    },
    Exchange.BYBIT_FUTURE.value: {
        "depth_levels": 25,
        "ping_interval": 20,
        "ping_timeout": 10,
        "update_speed_ms": 100
    }
}

# 웹소켓 공통 설정
WEBSOCKET_COMMON_CONFIG = {
    "health_check_interval": 10,  # 헬스체크 간격 (초)
    "message_timeout": 30,        # 메시지 타임아웃 (초)
    "reconnect": {
        "initial_delay": 1.0,     # 초기 재연결 대기 시간
        "max_delay": 60.0,        # 최대 재연결 대기 시간
        "max_attempts": 0         # 무제한 재시도 (0)
    }
}

# ============================
# 로깅 관련 상수
# ============================
# 로그 포맷
# 기존 포맷: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
# 수정된 포맷: 파일명과 위치 표시, 간격 20으로 설정, 로그 레벨 오른쪽 정렬
LOG_FORMAT = "%(asctime)s - %(filename)-20s:%(lineno)-3d / %(levelname)-7s - %(message)s"
DEBUG_LOG_FORMAT = "%(asctime)s - %(filename)-20s:%(lineno)-3d / %(levelname)-7s - %(message)s"
LOG_ENCODING = "utf-8"
LOG_MODE = "a"

# 로그 레벨 설정
DEFAULT_CONSOLE_LEVEL = 20  # INFO
DEFAULT_FILE_LEVEL = 10     # DEBUG

# 로그 파일 관리
LOG_MAX_BYTES = 100 * 1024 * 1024  # 100MB
LOG_BACKUP_COUNT = 100
LOG_CLEANUP_DAYS = 30

# ============================
# 메트릭 관련 상수
# ============================
# 메트릭 수집 설정
METRICS_RATE_CALCULATION_INTERVAL = 1.0  # 처리율 계산 주기 (초)
METRICS_DELAY_THRESHOLD_MS = 1000        # 지연 감지 임계값 (ms)
METRICS_ALERT_COOLDOWN = 300             # 알림 쿨다운 (초)
METRICS_MAX_HISTORY = 3600               # 메트릭 보관 기간 (초)
METRICS_MAX_EVENTS = 100                 # 최대 이벤트 저장 수
METRICS_HEALTH_THRESHOLD = 70            # 상태 점수 임계값

# 메트릭 저장 설정
METRICS_SAVE_INTERVAL = 60  # 메트릭 저장 간격 (초)
METRICS_CLEANUP_DAYS = 30   # 30일 이상 된 메트릭 파일 자동 삭제

# Export all constants
__all__ = [
    # 시스템 메시지
    'LOG_SYSTEM',
    
    # 설정 파일 관련
    'CONFIG_DIR', 'SETTINGS_FILE', 'API_KEYS_FILE',
    'BACKUP_DIR', 'SETTINGS_PATH', 'API_KEYS_PATH',
    'LOAD_TIMEOUT', 'SAVE_TIMEOUT', 'RETRY_DELAY', 'MAX_RETRIES',
    'PROJECT_ROOT', 'ENV_FILE_PATHS', 'API_ENV_VARS',
    
    # 거래소 관련
    'Exchange', 'EXCHANGE_NAMES_KR', 'EXCHANGE_GROUPS', 'EXCHANGE_DEFAULTS',
    
    # 웹소켓 관련
    'WebSocketState', 'STATUS_EMOJIS', 'WEBSOCKET_STATES',
    'WEBSOCKET_URLS', 'API_URLS', 'WEBSOCKET_CONFIG', 'WEBSOCKET_COMMON_CONFIG',
    
    # 로깅 관련
    'LOG_FORMAT', 'DEBUG_LOG_FORMAT', 'LOG_ENCODING', 'LOG_MODE',
    'DEFAULT_CONSOLE_LEVEL', 'DEFAULT_FILE_LEVEL',
    'LOG_MAX_BYTES', 'LOG_BACKUP_COUNT', 'LOG_CLEANUP_DAYS',
    
    # 메트릭 관련
    'METRICS_RATE_CALCULATION_INTERVAL', 'METRICS_DELAY_THRESHOLD_MS',
    'METRICS_ALERT_COOLDOWN', 'METRICS_MAX_HISTORY', 'METRICS_MAX_EVENTS',
    'METRICS_HEALTH_THRESHOLD', 'METRICS_SAVE_INTERVAL', 'METRICS_CLEANUP_DAYS'
] 