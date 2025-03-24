"""
크로스 김프 아비트리지 - 공통 상수 모듈

이 모듈은 시스템 전체에서 공통으로 사용되는 상수값을 정의합니다.
"""

from enum import Enum
from typing import Dict, List, Any, Union

# ============================
# 시스템 컴포넌트 식별자
# ============================
class SystemComponent(Enum):
    """시스템 컴포넌트 식별자"""
    ORDERBOOK = "orderbook"    # 오더북 수집기
    RADAR = "radar"            # 레이더 (시장 감시기)
    TRADER = "trader"          # 트레이더 (자동 거래 시스템)
    TELEGRAM = "telegram"      # 텔레그램 서비스
    SYSTEM = "system"          # 시스템 (통합 프로그램)
    WEB = "web"                # 웹 서비스

# ============================
# 시스템 컴포넌트 이름 (한글)
# ============================
COMPONENT_NAMES_KR = {
    SystemComponent.ORDERBOOK.value: "[오더북수집기]",
    SystemComponent.RADAR.value: "[레이더]",
    SystemComponent.TRADER.value: "[트레이더]",
    SystemComponent.TELEGRAM.value: "[텔레그램]",
    SystemComponent.SYSTEM.value: "[시스템]",
    SystemComponent.WEB.value: "[웹서비스]",
}

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

# ============================
# 이벤트 유형
# ============================
class EventCategory(Enum):
    """이벤트 카테고리"""
    SYSTEM = "system"
    CONNECTION = "connection"
    ORDERBOOK = "orderbook"
    TRADE = "trade"
    ERROR = "error"
    METRIC = "metric"
    NOTIFICATION = "notification"

# ============================
# 이벤트 우선순위
# ============================
class EventPriority(Enum):
    """이벤트 우선순위"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3

# ============================
# 기본 상태 코드
# ============================
class StatusCode(Enum):
    """시스템 상태 코드"""
    OK = 200
    CREATED = 201
    ACCEPTED = 202
    NO_CONTENT = 204
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    TIMEOUT = 408
    CONFLICT = 409
    INTERNAL_ERROR = 500
    SERVICE_UNAVAILABLE = 503

# ============================
# 유틸리티 함수
# ============================
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

def is_korean_exchange(exchange_code: str) -> bool:
    """거래소가 한국 거래소인지 확인"""
    return normalize_exchange_code(exchange_code) in EXCHANGE_GROUPS["korean"]

def is_global_exchange(exchange_code: str) -> bool:
    """거래소가 글로벌 거래소인지 확인"""
    return normalize_exchange_code(exchange_code) in EXCHANGE_GROUPS["global"]

def is_futures_exchange(exchange_code: str) -> bool:
    """거래소가 선물 거래소인지 확인"""
    return normalize_exchange_code(exchange_code) in EXCHANGE_GROUPS["futures"] 