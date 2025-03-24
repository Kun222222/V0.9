"""
오더북 수집기 모듈

거래소의 오더북 데이터를 실시간으로 수집하고 처리하는 기능을 제공합니다.
"""

# 외부에서 사용할 팩토리 함수 노출
from crosskimp.ob_collector.orderbook.manager_factory import create_order_manager

# 하위 호환성을 위한 함수
def get_orderbook_manager():
    """
    오더북 관리자 모듈을 가져옵니다.
    
    order_manager 모듈의 주요 기능에 접근하기 위한 편의 함수입니다.
    
    Returns:
        object: 오더북 관리 기능이 있는 모듈
    """
    from crosskimp.ob_collector.orderbook.order_manager import (
        start_orderbook_collection,
        stop_orderbook_collection,
        get_orderbook_managers
    )
    
    return {
        'start': start_orderbook_collection,
        'stop': stop_orderbook_collection,
        'get_managers': get_orderbook_managers
    }
