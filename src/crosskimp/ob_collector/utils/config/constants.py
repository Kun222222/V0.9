"""
크로스킴프 아비트리지 봇 상수 정의

이 모듈은 프로그램 전반에서 사용되는 주요 상수들을 정의합니다.
- 설정 파일 관련 상수
- 거래소 관련 상수
- 웹소켓 관련 상수
- API 관련 상수

작성자: CrossKimp Arbitrage Bot 개발팀
최종수정: 2024.03
"""

import os
from enum import Enum

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

# 설정 관련 타임아웃
LOAD_TIMEOUT = 10  # 초
SAVE_TIMEOUT = 5   # 초
RETRY_DELAY = 1    # 초
MAX_RETRIES = 3    # 최대 재시도 횟수

# ============================
# 거래소 관련 상수
# ============================
class Exchange(Enum):
    """거래소 식별자"""
    BINANCE = "binance"
    BYBIT = "bybit"
    UPBIT = "upbit"
    BITHUMB = "bithumb"
    BINANCE_FUTURE = "binancefuture"
    BYBIT_FUTURE = "bybitfuture"

# 거래소 한글 이름
EXCHANGE_NAMES_KR = {
    "binance": "바이낸스",
    "bybit": "바이빗",
    "upbit": "업비트",
    "bithumb": "빗썸",
    "binancefuture": "바이낸스 선물",
    "bybitfuture": "바이빗 선물"
}

# ============================
# 웹소켓 관련 상수
# ============================
# 웹소켓 상태 이모지
STATUS_EMOJIS = {
    "CONNECTED": "🟢",
    "CONNECTING": "🟡",
    "DISCONNECTED": "⚪",
    "ERROR": "🔴",
    "RECONNECTING": "🟠"
}

# ============================
# 시스템 메시지 상수
# ============================
LOG_SYSTEM = "[시스템]"  # 기존 [Config] 대체

# ============================
# API 엔드포인트 상수
# ============================
# 웹소켓 URL
WEBSOCKET_URLS = {
    "upbit": "wss://api.upbit.com/websocket/v1",
    "bithumb": "wss://pubwss.bithumb.com/pub/ws",
    "binance": "wss://stream.binance.com:9443/ws",
    "binance_futures": "wss://fstream.binance.com/ws",
    "bybit": "wss://stream.bybit.com/v5/public/spot",
    "bybit_futures": "wss://stream.bybit.com/v5/public/linear"
}

# REST API URL
API_URLS = {
    "upbit": {
        "market": "https://api.upbit.com/v1/market/all",
        "ticker": "https://api.upbit.com/v1/ticker"
    },
    "bithumb": {
        "ticker": "https://api.bithumb.com/public/ticker/ALL_KRW"
    },
    "binance": {
        "spot": "https://api.binance.com/api/v3/exchangeInfo",
        "depth": "https://api.binance.com/api/v3/depth"
    },
    "binance_futures": {
        "future": "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "depth": "https://fapi.binance.com/fapi/v1/depth"
    },
    "bybit": {
        "spot": "https://api.bybit.com/v5/market/instruments-info?category=spot"
    },
    "bybit_futures": {
        "future": "https://api.bybit.com/v5/market/instruments-info?category=linear"
    }
}

# ============================
# 웹소켓 설정 상수
# ============================
WEBSOCKET_CONFIG = {
    "upbit": {
        "depth_levels": 15,
        "ping_interval": 60,
        "ping_timeout": 10,
        "update_speed_ms": 100
    },
    "bithumb": {
        "depth_levels": 15,
        "ping_interval": 30,
        "ping_timeout": 10,
        "update_speed_ms": 100
    },
    "binance": {
        "depth_levels": 20,
        "ping_interval": 150,  # 바이낸스는 더 긴 핑 간격 사용
        "ping_timeout": 10,
        "update_speed_ms": 100
    },
    "binance_futures": {
        "depth_levels": 20,
        "ping_interval": 150,  # 바이낸스는 더 긴 핑 간격 사용
        "ping_timeout": 10,
        "update_speed_ms": 100
    },
    "bybit": {
        "depth_levels": 25,
        "ping_interval": 20,
        "ping_timeout": 10,
        "update_speed_ms": 100
    },
    "bybit_futures": {
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

# Export all constants
__all__ = [
    'CONFIG_DIR', 'SETTINGS_FILE', 'API_KEYS_FILE',
    'BACKUP_DIR', 'SETTINGS_PATH', 'API_KEYS_PATH',
    'LOAD_TIMEOUT', 'SAVE_TIMEOUT', 'RETRY_DELAY', 'MAX_RETRIES',
    'Exchange', 'EXCHANGE_NAMES_KR',
    'STATUS_EMOJIS', 'LOG_SYSTEM',
    'WEBSOCKET_URLS', 'API_URLS', 'WEBSOCKET_CONFIG', 'WEBSOCKET_COMMON_CONFIG'
] 