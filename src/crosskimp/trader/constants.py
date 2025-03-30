"""
거래 모듈 상수

거래 기능에 필요한 상수와 열거형을 정의합니다.
"""

from enum import Enum, auto
from typing import Dict, List, Any
from decimal import Decimal

from crosskimp.common.config.common_constants import Exchange

# ============================
# 거래 상태
# ============================
class TradeStatus(str, Enum):
    """거래 상태"""
    # 주문 상태
    PENDING = "pending"              # 주문 대기 중
    CREATED = "created"              # 주문 생성됨
    PARTIALLY_FILLED = "partially_filled"  # 일부 체결
    FILLED = "filled"                # 완전 체결
    CANCELED = "canceled"            # 취소됨
    REJECTED = "rejected"            # 거부됨
    EXPIRED = "expired"              # 만료됨
    
    # 포지션 상태
    OPEN = "open"                    # 오픈 포지션
    CLOSED = "closed"                # 클로즈 포지션
    
    # 기타 상태
    UNKNOWN = "unknown"              # 알 수 없는 상태
    ERROR = "error"                  # 오류 상태

# ============================
# 주문 유형
# ============================
class OrderType(str, Enum):
    """주문 유형"""
    MARKET = "market"                # 시장가 주문
    LIMIT = "limit"                  # 지정가 주문
    STOP_MARKET = "stop_market"      # 스탑 시장가
    STOP_LIMIT = "stop_limit"        # 스탑 지정가
    TAKE_PROFIT = "take_profit"      # 이익 실현
    TAKE_PROFIT_LIMIT = "take_profit_limit"  # 이익 실현 지정가

# ============================
# 주문 방향
# ============================
class OrderSide(str, Enum):
    """주문 방향"""
    BUY = "buy"                      # 매수
    SELL = "sell"                    # 매도

# ============================
# 포지션 유형
# ============================
class PositionType(str, Enum):
    """포지션 유형"""
    SPOT = "spot"                    # 현물 포지션
    LONG = "long"                    # 롱 포지션
    SHORT = "short"                  # 숏 포지션

# ============================
# 거래 유형
# ============================
class TradeType(str, Enum):
    """거래 유형"""
    MARKET_BUY = "market_buy"        # 시장가 매수
    MARKET_SELL = "market_sell"      # 시장가 매도
    LIMIT_BUY = "limit_buy"          # 지정가 매수
    LIMIT_SELL = "limit_sell"        # 지정가 매도
    
    # 선물 거래 전용
    OPEN_LONG = "open_long"          # 롱 오픈
    CLOSE_LONG = "close_long"        # 롱 클로즈
    OPEN_SHORT = "open_short"        # 숏 오픈
    CLOSE_SHORT = "close_short"      # 숏 클로즈

# ============================
# 김프 유형
# ============================
class KimpType(str, Enum):
    """김프 유형"""
    NORMAL = "normal"                # 일반 김프
    REVERSE = "reverse"              # 역김프

# ============================
# 전략 유형
# ============================
class StrategyType(str, Enum):
    """전략 유형"""
    KIMP_NORMAL = "kimp_normal"      # 일반 김프 전략
    KIMP_REVERSE = "kimp_reverse"    # 역김프 전략
    ARBITRAGE = "arbitrage"          # 일반 아비트리지
    GRID = "grid"                    # 그리드 트레이딩
    DCA = "dca"                      # 달러 코스트 애버리징
    SCALPING = "scalping"            # 스캘핑

# ============================
# 거래 상수
# ============================
# 최소 김프 기준(%)
MIN_KIMP_THRESHOLD = Decimal('1.5')

# 최대 김프 기준(%)
MAX_KIMP_THRESHOLD = Decimal('10.0')

# 기본 거래 수수료 (수수료 정보가 없을 때 사용)
DEFAULT_MAKER_FEE = {
    Exchange.BINANCE.value: Decimal('0.1'),
    Exchange.BYBIT.value: Decimal('0.1'),
    Exchange.UPBIT.value: Decimal('0.05'),
    Exchange.BITHUMB.value: Decimal('0.037'),
    Exchange.BINANCE_FUTURE.value: Decimal('0.02'),
    Exchange.BYBIT_FUTURE.value: Decimal('0.01')
}

DEFAULT_TAKER_FEE = {
    Exchange.BINANCE.value: Decimal('0.1'),
    Exchange.BYBIT.value: Decimal('0.1'),
    Exchange.UPBIT.value: Decimal('0.05'),
    Exchange.BITHUMB.value: Decimal('0.037'),
    Exchange.BINANCE_FUTURE.value: Decimal('0.04'),
    Exchange.BYBIT_FUTURE.value: Decimal('0.05')
}

# 최소 주문 금액
DEFAULT_MIN_ORDER_AMOUNT = {
    "krw": {
        Exchange.UPBIT.value: Decimal('5000'),
        Exchange.BITHUMB.value: Decimal('5000')
    },
    "usdt": {
        Exchange.BINANCE.value: Decimal('10'),
        Exchange.BYBIT.value: Decimal('10'),
        Exchange.BINANCE_FUTURE.value: Decimal('5'),
        Exchange.BYBIT_FUTURE.value: Decimal('5')
    }
}

# 거래 제한 시간 (초)
ORDER_TIMEOUT = 30

# ============================
# 통화 및 심볼 관련
# ============================
# 기본 기축 통화
BASE_CURRENCIES = {
    "korean": "KRW",
    "global": "USDT"
}

# 마켓 코드 구분자
MARKET_SEPARATOR = {
    Exchange.BINANCE.value: "",      # BTC 형태로 사용
    Exchange.BYBIT.value: "",        # BTC 형태로 사용
    Exchange.UPBIT.value: "-",       # KRW-BTC 형태로 사용
    Exchange.BITHUMB.value: "_",     # BTC_KRW 형태로 사용
    Exchange.BINANCE_FUTURE.value: "",  # BTC 형태로 사용
    Exchange.BYBIT_FUTURE.value: ""     # BTC 형태로 사용
}

# ============================
# 에러 코드
# ============================
class TradeErrorCode(int, Enum):
    """거래 오류 코드"""
    SUCCESS = 0                       # 성공
    
    # 거래소 관련 오류
    EXCHANGE_ERROR = 1000             # 거래소 일반 오류
    EXCHANGE_CONNECTION_ERROR = 1001  # 거래소 연결 오류
    EXCHANGE_AUTH_ERROR = 1002        # 거래소 인증 오류
    EXCHANGE_RATE_LIMIT = 1003        # 거래소 API 속도 제한
    EXCHANGE_INSUFFICIENT_FUNDS = 1004  # 잔액 부족
    
    # 주문 관련 오류
    ORDER_ERROR = 2000                # 주문 일반 오류
    ORDER_REJECTED = 2001             # 주문 거부됨
    ORDER_NOT_FOUND = 2002            # 주문을 찾을 수 없음
    ORDER_TIMEOUT = 2003              # 주문 시간 초과
    
    # 거래 전략 관련 오류
    STRATEGY_ERROR = 3000             # 전략 일반 오류
    STRATEGY_INVALID_PARAMS = 3001    # 잘못된 전략 파라미터
    STRATEGY_EXECUTION_ERROR = 3002   # 전략 실행 오류
    
    # 시스템 관련 오류
    SYSTEM_ERROR = 9000               # 시스템 일반 오류
    INVALID_ARGS = 9001               # 잘못된 인수
    CONFIG_ERROR = 9002               # 설정 오류 