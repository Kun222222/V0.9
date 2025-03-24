"""
거래 모듈

거래 실행, 김프 계산, 아비트리지 전략 등을 제공합니다.
"""

from crosskimp.trading.constants import (
    TradeStatus,
    OrderType,
    OrderSide,
    PositionType,
    TradeType,
    KimpType,
    StrategyType,
    MIN_KIMP_THRESHOLD,
    MAX_KIMP_THRESHOLD,
    DEFAULT_MAKER_FEE,
    DEFAULT_TAKER_FEE,
    DEFAULT_MIN_ORDER_AMOUNT,
    ORDER_TIMEOUT,
    BASE_CURRENCIES,
    MARKET_SEPARATOR,
    TradeErrorCode
)

__all__ = [
    # 클래스
    'TradeStatus',
    'OrderType',
    'OrderSide',
    'PositionType',
    'TradeType',
    'KimpType',
    'StrategyType',
    'TradeErrorCode',
    
    # 상수
    'MIN_KIMP_THRESHOLD',
    'MAX_KIMP_THRESHOLD',
    'DEFAULT_MAKER_FEE',
    'DEFAULT_TAKER_FEE',
    'DEFAULT_MIN_ORDER_AMOUNT',
    'ORDER_TIMEOUT',
    'BASE_CURRENCIES',
    'MARKET_SEPARATOR'
] 