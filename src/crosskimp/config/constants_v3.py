"""
크로스킴프 아비트리지 - 핵심 상수 정의

거래소 식별자와 시스템 상수를 정의합니다.
"""

import os
import json
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Union

# ============================
# 거래소 식별자
# ============================
class Exchange(Enum):
    """
    거래소 식별자
    
    모든 지원 거래소의 식별자를 정의합니다.
    코드 전체에서 일관된 거래소 식별을 위해 사용됩니다.
    """
    BINANCE = "binance"        # 바이낸스 현물
    BYBIT = "bybit"            # 바이빗 현물
    UPBIT = "upbit"            # 업비트 현물
    BITHUMB = "bithumb"        # 빗썸 현물
    BINANCE_FUTURE = "binance_future"  # 바이낸스 선물
    BYBIT_FUTURE = "bybit_future"      # 바이빗 선물

# ============================
# 거래소 그룹화
# ============================
EXCHANGE_GROUPS = {
    "korean": [Exchange.UPBIT.value, Exchange.BITHUMB.value],
    "global": [Exchange.BINANCE.value, Exchange.BYBIT.value],
    "spot": [Exchange.BINANCE.value, Exchange.BYBIT.value, Exchange.UPBIT.value, Exchange.BITHUMB.value],
    "futures": [Exchange.BINANCE_FUTURE.value, Exchange.BYBIT_FUTURE.value]
}

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

# 한글 거래소 이름 사용 예시 (상수에서 가져옴)
exchange_name = Exchange.BINANCE.value
exchange_name_kr = EXCHANGE_NAMES_KR.get(exchange_name, f"[{exchange_name}]")

# ============================
# 거래소 기본 설정
# ============================
EXCHANGE_DEFAULT_SETTINGS = {
    Exchange.BINANCE.value: {
        "maker_fee": 0.1,
        "taker_fee": 0.1,
        "min_order_usdt": 10,
        "max_order_usdt": 100000
    },
    Exchange.BYBIT.value: {
        "maker_fee": 0.1,
        "taker_fee": 0.1,
        "min_order_usdt": 10,
        "max_order_usdt": 10000
    },
    Exchange.UPBIT.value: {
        "maker_fee": 0.05,
        "taker_fee": 0.05,
        "min_order_krw": 5000,
        "max_order_krw": 100000000
    },
    Exchange.BITHUMB.value: {
        "maker_fee": 0.037,
        "taker_fee": 0.037,
        "min_order_krw": 5000,
        "max_order_krw": 100000000
    },
    Exchange.BINANCE_FUTURE.value: {
        "maker_fee": 0.05,
        "taker_fee": 0.05,
        "max_leverage": 5,
        "default_leverage": 1
    },
    Exchange.BYBIT_FUTURE.value: {
        "maker_fee": 0.055,
        "taker_fee": 0.055,
        "max_leverage": 5,
        "default_leverage": 1
    }
}

# ============================
# 경로 상수
# ============================
def get_project_root():
    """프로젝트 루트 디렉토리 찾기"""
    # 직접 지정된 경로 사용
    project_root = "/Users/kun/Desktop/CrossKimpArbitrage/v0.6"
    if os.path.exists(project_root):
        return Path(project_root)
        
    # 환경 변수에서 확인
    if 'PROJECT_ROOT' in os.environ:
        return Path(os.environ['PROJECT_ROOT'])
    
    # Docker 환경 확인
    if os.path.exists('/.dockerenv') or os.environ.get('PYTHONPATH') == '/app':
        return Path('/app')
    
    # 패키지 기반 검색
    try:
        import crosskimp
        package_path = Path(crosskimp.__file__).parent
        return package_path.parent.parent
    except (ImportError, AttributeError):
        pass
    
    # 현재 경로 사용
    return Path.cwd()

# 프로젝트 루트 경로
PROJECT_ROOT = get_project_root()

# 주요 디렉토리 경로 정의
SRC_DIR = PROJECT_ROOT / 'src'
LOGS_DIR = PROJECT_ROOT / 'logs'
DATA_DIR = PROJECT_ROOT / 'data'
CONFIG_DIR = PROJECT_ROOT / 'src/crosskimp/config'

# 로그 하위 디렉토리 경로 설정
LOG_SUBDIRS = {
    'queue': LOGS_DIR / 'queue',
    'error': LOGS_DIR / 'error',
    'telegram': LOGS_DIR / 'telegram',
    'archive': LOGS_DIR / 'archive',
    'raw_data': LOGS_DIR / 'raw_data',
    'serialized_data': LOGS_DIR / 'serialized_data'
}

# 설정 파일 경로
SETTINGS_FILE = "settings.json"
SETTINGS_PATH = os.path.join(CONFIG_DIR, SETTINGS_FILE)
BACKUP_DIR = os.path.join(CONFIG_DIR, "backups")

# 환경 변수 파일 경로
ENV_FILE = os.path.join(PROJECT_ROOT, ".env")

# ============================
# 로깅 관련 상수
# ============================
LOG_SYSTEM = "[시스템]"
LOG_FORMAT = "%(asctime)s.%(msecs)03d - %(filename)-20s:%(lineno)-3d / %(levelname)-7s - %(message)s"
DEBUG_LOG_FORMAT = "%(asctime)s.%(msecs)03d - %(filename)-20s:%(lineno)-3d / %(levelname)-7s - %(message)s"
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
METRICS_RATE_CALCULATION_INTERVAL = 1.0  # 처리율 계산 주기 (초)
METRICS_DELAY_THRESHOLD_MS = 1000        # 지연 감지 임계값 (ms)
METRICS_ALERT_COOLDOWN = 300             # 알림 쿨다운 (초)
METRICS_MAX_HISTORY = 3600               # 메트릭 보관 기간 (초)
METRICS_MAX_EVENTS = 300                 # 최대 이벤트 저장 수
METRICS_HEALTH_THRESHOLD = 70            # 상태 점수 임계값

# ============================
# API 키 환경 변수 이름
# ============================
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
    "security": {
        "access_token_secret": "ACCESS_TOKEN_SECRET"
    },
    "user": {
        "first_superuser": "FIRST_SUPERUSER",
        "first_superuser_password": "FIRST_SUPERUSER_PASSWORD"
    }
}

# ============================
# 설정 로딩 및 유틸리티 함수
# ============================
_cached_settings = None

def get_settings() -> Dict[str, Any]:
    """전체 설정 로드"""
    global _cached_settings
    if _cached_settings is None:
        try:
            with open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
                _cached_settings = json.load(f)
        except Exception as e:
            print(f"설정 파일 로드 오류: {e}")
            _cached_settings = {}
    return _cached_settings

def get_value_from_settings(path: str, default: Any = None) -> Any:
    """설정에서 특정 경로의 값을 반환합니다.
    
    경로 형식: "section.subsection.key" 또는 "section.subsection.key.value"
    예: "trading.settings.base_order_amount_krw" 또는 "trading.settings.base_order_amount_krw.value"
    """
    settings = get_settings()
    parts = path.split('.')
    current = settings
    
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return default
    
    # 새로운 구조에서는 값이 dict이고 'value' 키가 있으면 그 값을 반환
    if isinstance(current, dict) and 'value' in current:
        return current['value']
    
    return current

def normalize_exchange_code(code: str) -> str:
    """거래소 코드를 정규화하여 반환 (소문자로 변환)"""
    return code.lower() if code else ""

def get_exchange_enum(code: str) -> Exchange:
    """문자열 코드에서 Exchange 열거형 값 반환"""
    code = normalize_exchange_code(code)
    for exchange in Exchange:
        if exchange.value == code:
            return exchange
    raise ValueError(f"알 수 없는 거래소 코드: {code}")

def get_exchange_fee(code: str, fee_type: str = "taker") -> float:
    """거래소 수수료 반환 (설정에서 로드)"""
    code = normalize_exchange_code(code)
    fee_key = f"{fee_type}_fee"
    
    # 설정에서 값 가져오기 시도
    fee = get_value_from_settings(f"exchanges.{code}.{fee_key}")
    
    # 설정에 없으면 기본값 사용
    if fee is None:
        fee = EXCHANGE_DEFAULT_SETTINGS.get(code, {}).get(fee_key, 0.1)
    
    return fee

def get_min_order_amount(code: str) -> dict:
    """거래소 최소 주문 금액 반환 (설정에서 로드)"""
    code = normalize_exchange_code(code)
    
    # KRW 거래소 확인
    if code in EXCHANGE_GROUPS["korean"]:
        min_amount = get_value_from_settings(f"exchanges.{code}.min_order_krw")
        if min_amount is not None:
            return {"currency": "KRW", "amount": min_amount}
    # 글로벌 거래소
    else:
        min_amount = get_value_from_settings(f"exchanges.{code}.min_order_usdt")
        if min_amount is not None:
            return {"currency": "USDT", "amount": min_amount}
    
    # 설정에 없으면 기본값 사용
    settings = EXCHANGE_DEFAULT_SETTINGS.get(code, {})
    if "min_order_krw" in settings:
        return {"currency": "KRW", "amount": settings["min_order_krw"]}
    elif "min_order_usdt" in settings:
        return {"currency": "USDT", "amount": settings["min_order_usdt"]}
    else:
        return {"currency": "USDT", "amount": 10}  # 기본값

def get_symbol_filters() -> Dict[str, list]:
    """심볼 필터 설정 반환"""
    excluded = get_value_from_settings("trading.symbols.excluded.value", [])
    included = get_value_from_settings("trading.symbols.included.value", [])
    patterns = get_value_from_settings("trading.symbols.excluded_patterns.value", [])
    
    return {
        "excluded": excluded,
        "included": included,
        "excluded_patterns": patterns
    }

def is_notification_enabled() -> bool:
    """알림 기능이 활성화되어 있는지 확인"""
    return get_value_from_settings("notifications.telegram.enabled.value", False)

def get_notification_types() -> list:
    """활성화된 알림 유형 목록 반환"""
    return get_value_from_settings("notifications.telegram.alert_types.value", [])

# ============================
# 디렉토리 생성 함수
# ============================
def ensure_directories():
    """필요한 모든 디렉토리가 존재하는지 확인하고, 없으면 생성합니다."""
    # 기본 디렉토리 생성
    for dir_path in [SRC_DIR, LOGS_DIR, DATA_DIR]:
        dir_path.mkdir(exist_ok=True, parents=True)
    
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR, exist_ok=True)
    
    # 로그 하위 디렉토리 생성
    for subdir_path in LOG_SUBDIRS.values():
        os.makedirs(subdir_path, exist_ok=True)

# 모듈 로드 시 디렉토리 생성 실행
ensure_directories()
