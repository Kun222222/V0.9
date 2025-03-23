"""
이벤트 유틸리티 모듈

이 모듈은 중앙 이벤트 버스를 사용하기 위한 유틸리티 함수를 제공합니다.
"""

from crosskimp.common.constants import Components
from crosskimp.common.events.manager import get_component_event_bus
from crosskimp.common.events.types import EventPriority

def get_orderbook_event_bus():
    """오더북 컴포넌트의 이벤트 버스 반환"""
    return get_component_event_bus(Components.ORDERBOOK)

def get_telegram_event_bus():
    """텔레그램 컴포넌트의 이벤트 버스 반환"""
    return get_component_event_bus(Components.TELEGRAM)

def get_system_event_bus():
    """시스템 컴포넌트의 이벤트 버스 반환"""
    return get_component_event_bus(Components.SYSTEM)
