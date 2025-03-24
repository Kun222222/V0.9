"""
OrderManager 생성 팩토리 모듈

이 모듈은 OrderManager 인스턴스를 생성하는 팩토리 함수를 제공합니다.
"""
from typing import Optional, Dict, Any, Union

from crosskimp.common.config.common_constants import Exchange
from crosskimp.common.config.app_config import get_config
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.order_manager import OrderManager

logger = get_unified_logger()

def create_order_manager(exchange_code: Optional[str] = None, settings: Optional[Dict[str, Any]] = None) -> OrderManager:
    """
    OrderManager 인스턴스를 생성하는 팩토리 함수
    
    Args:
        exchange_code: 거래소 코드 (기본값: 'upbit')
        settings: 설정 (기본값: 시스템 설정에서 가져옴)
    
    Returns:
        OrderManager: 생성된 OrderManager 인스턴스
    """
    # 기본 설정 불러오기
    if settings is None:
        try:
            config = get_config()
            settings = config.get_system()
        except Exception as e:
            logger.error(f"설정을 불러오는 중 오류 발생: {e}")
            # 최소한의 기본 설정 제공
            settings = {
                "websocket_reconnect_interval": 5,
                "websocket_ping_interval": 30,
                "websocket_ping_timeout": 10,
                "max_orderbook_depth": 100,
                "log_raw_messages": False
            }
    
    # 기본 거래소 코드 설정
    if exchange_code is None:
        exchange_code = Exchange.UPBIT.value
    
    logger.info(f"OrderManager 생성: exchange_code={exchange_code}")
    
    # OrderManager 인스턴스 생성 및 반환
    return OrderManager(settings, exchange_code) 