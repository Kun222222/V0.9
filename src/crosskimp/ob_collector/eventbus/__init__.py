"""
오더북 콜렉터 이벤트 시스템

간소화된 이벤트 시스템을 제공합니다.
"""

from crosskimp.ob_collector.eventbus.types import EventPriority, EventTypes
from crosskimp.ob_collector.eventbus.bus import EventBus
from crosskimp.ob_collector.eventbus.handler import EventHandler, get_event_handler

def get_event_bus() -> EventBus:
    """이벤트 버스 싱글톤 인스턴스 반환"""
    return EventBus.get_instance()

__all__ = [
    'EventBus',
    'EventPriority',
    'EventTypes',
    'get_event_bus',
    'EventHandler',
    'get_event_handler'
]
