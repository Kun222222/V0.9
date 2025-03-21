"""
컴포넌트 팩토리 모듈

이 모듈은 거래소별 웹소켓 연결, 구독, 검증기 등의 컴포넌트 객체를 생성하는 팩토리를 제공합니다.
"""

from typing import Optional, Dict, Any, Type
from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import Exchange, EXCHANGE_NAMES_KR

# 기본 클래스 임포트
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator

# 모든 거래소 컴포넌트 임포트
# 연결 컴포넌트
from crosskimp.ob_collector.orderbook.connection.upbit_s_cn import UpbitWebSocketConnector
from crosskimp.ob_collector.orderbook.connection.bybit_s_cn import BybitWebSocketConnector
from crosskimp.ob_collector.orderbook.connection.bybit_f_cn import BybitFutureWebSocketConnector
from crosskimp.ob_collector.orderbook.connection.bithumb_s_cn import BithumbWebSocketConnector
from crosskimp.ob_collector.orderbook.connection.binance_s_cn import BinanceWebSocketConnector
from crosskimp.ob_collector.orderbook.connection.binance_f_cn import BinanceFutureWebSocketConnector

# 구독 컴포넌트
from crosskimp.ob_collector.orderbook.subscription.upbit_s_sub import UpbitSubscription
from crosskimp.ob_collector.orderbook.subscription.bybit_s_sub import BybitSubscription
from crosskimp.ob_collector.orderbook.subscription.bybit_f_sub import BybitFutureSubscription
from crosskimp.ob_collector.orderbook.subscription.bithumb_s_sub import BithumbSubscription
from crosskimp.ob_collector.orderbook.subscription.binance_s_sub import BinanceSubscription
from crosskimp.ob_collector.orderbook.subscription.binance_f_sub import BinanceFutureSubscription

# 로거 설정
logger = get_unified_logger()

# 컴포넌트 클래스 매핑
EXCHANGE_CONNECTORS = {
    Exchange.UPBIT.value: UpbitWebSocketConnector,
    Exchange.BYBIT.value: BybitWebSocketConnector,
    Exchange.BYBIT_FUTURE.value: BybitFutureWebSocketConnector,
    Exchange.BITHUMB.value: BithumbWebSocketConnector,
    Exchange.BINANCE.value: BinanceWebSocketConnector,
    Exchange.BINANCE_FUTURE.value: BinanceFutureWebSocketConnector
}

EXCHANGE_SUBSCRIPTIONS = {
    Exchange.UPBIT.value: UpbitSubscription,
    Exchange.BYBIT.value: BybitSubscription,
    Exchange.BYBIT_FUTURE.value: BybitFutureSubscription,
    Exchange.BITHUMB.value: BithumbSubscription,
    Exchange.BINANCE.value: BinanceSubscription,
    Exchange.BINANCE_FUTURE.value: BinanceFutureSubscription
}

def create_connector(exchange_code: str, settings: Dict[str, Any]) -> Optional[BaseWebsocketConnector]:
    """
    거래소별 웹소켓 연결 객체 생성
    
    Args:
        exchange_code: 거래소 코드
        settings: 설정 딕셔너리
        
    Returns:
        BaseWebsocketConnector: 웹소켓 연결 객체 또는 None (실패 시)
    """
    exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, f"[{exchange_code}]")
    
    try:
        # 거래소 코드에 해당하는 클래스 찾기
        connector_class = EXCHANGE_CONNECTORS.get(exchange_code)
        if not connector_class:
            logger.warning(f"{exchange_kr} 해당 거래소의 연결 클래스를 찾을 수 없습니다")
            return None
            
        # 연결 객체 생성
        connector = connector_class(settings)
        logger.debug(f"{exchange_kr} 연결 객체 생성됨")
        return connector
        
    except Exception as e:
        logger.error(f"{exchange_kr} 연결 객체 생성 실패: {str(e)}")
        return None

def create_subscription(
    exchange_code: str, 
    connector: BaseWebsocketConnector
) -> Optional[BaseSubscription]:
    """
    거래소별 구독 객체 생성
    
    Args:
        exchange_code: 거래소 코드
        connector: 웹소켓 연결 객체
        
    Returns:
        BaseSubscription: 구독 객체 또는 None (실패 시)
    """
    exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, f"[{exchange_code}]")
    
    try:
        # 거래소 코드에 해당하는 클래스 찾기
        subscription_class = EXCHANGE_SUBSCRIPTIONS.get(exchange_code)
        if not subscription_class:
            logger.warning(f"{exchange_kr} 해당 거래소의 구독 클래스를 찾을 수 없습니다")
            return None
            
        # 구독 객체 생성
        subscription = subscription_class(connector)
        logger.debug(f"{exchange_kr} 구독 객체 생성됨")
        return subscription
        
    except Exception as e:
        logger.error(f"{exchange_kr} 구독 객체 생성 실패: {str(e)}")
        return None

def create_validator(exchange_code: str) -> Optional[BaseOrderBookValidator]:
    """
    오더북 검증기 객체 생성
    
    Args:
        exchange_code: 거래소 코드
        
    Returns:
        BaseOrderBookValidator: 검증기 객체 또는 None (실패 시)
    """
    exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, f"[{exchange_code}]")
    
    try:
        # 현재는 기본 검증기만 사용
        validator_class = BaseOrderBookValidator
            
        # 검증기 객체 생성
        validator = validator_class(exchange_code)
        logger.debug(f"{exchange_kr} 검증기 객체 생성됨")
        return validator
        
    except Exception as e:
        logger.warning(f"{exchange_kr} 검증기 객체 생성 실패: {str(e)}")
        return None 