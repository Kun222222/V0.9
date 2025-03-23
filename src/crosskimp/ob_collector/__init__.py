"""
오더북 수집기 패키지
"""

from typing import Optional

# 글로벌 인스턴스
_orderbook_manager_instance = None

def get_orderbook_manager():
    """오더북 매니저 싱글톤 인스턴스를 반환합니다."""
    global _orderbook_manager_instance
    
    if _orderbook_manager_instance is None:
        from crosskimp.ob_collector.run_orderbook import OrderbookCollector
        
        # 설정 가져오기
        from crosskimp.common.config.constants_v3 import get_settings
        settings = get_settings()
        
        _orderbook_manager_instance = OrderbookCollector(settings)
        
    return _orderbook_manager_instance
