"""
이벤트 시스템 패키지

이 패키지는 시스템 내 컴포넌트 간 통신을 위한 이벤트 버스를 제공합니다.
"""

from enum import Enum, auto
from typing import Optional
from crosskimp.common.events.sys_event_bus import SimpleEventBus, EventType

# 글로벌 이벤트 버스 인스턴스
_event_bus_instance: Optional[SimpleEventBus] = None
_component_event_buses = {}

def get_event_bus() -> SimpleEventBus:
    """글로벌 이벤트 버스 인스턴스를 반환합니다."""
    global _event_bus_instance
    
    if _event_bus_instance is None:
        _event_bus_instance = SimpleEventBus()
        
    return _event_bus_instance

# 컴포넌트 타입 정의
class Component(Enum):
    """시스템 컴포넌트 타입"""
    SYSTEM = "system"
    TELEGRAM = "telegram"
    ORDERBOOK = "orderbook"
    TRADER = "trader"
    CALCULATOR = "calculator"
    RADAR = "radar"
    SERVER = "server"

# 상태 이벤트 타입
class StatusEventTypes(Enum):
    """상태 관련 이벤트 타입"""
    UPDATE = auto()
    ALERT = auto()
    HEARTBEAT = auto()

# 텔레그램 이벤트 타입
class TelegramEventTypes(Enum):
    """텔레그램 관련 이벤트 타입"""
    COMMAND = auto()
    NOTIFICATION = auto()
    USER_MESSAGE = auto()

# 일반 이벤트 타입
class EventTypes(Enum):
    """일반 이벤트 타입"""
    PROCESS_START = "process_start"
    PROCESS_STOP = "process_stop"
    PROCESS_ERROR = "process_error"
    SYSTEM_STATUS = "system_status"
    ORDER_CREATED = "order_created"
    ORDER_FILLED = "order_filled"
    ORDER_CANCELED = "order_canceled"
    TRADE_COMPLETED = "trade_completed"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

# 이벤트 우선순위
class EventPriority(Enum):
    """이벤트 우선순위"""
    HIGH = 0
    NORMAL = 1
    LOW = 2
    BACKGROUND = 3

def get_component_event_bus(component: Component) -> SimpleEventBus:
    """
    컴포넌트별 이벤트 버스 인스턴스를 반환합니다.
    실제로는 글로벌 이벤트 버스와 동일한 인스턴스이지만,
    컴포넌트별로 구분하여 사용할 수 있도록 합니다.
    """
    global _component_event_buses
    
    if component not in _component_event_buses:
        _component_event_buses[component] = get_event_bus()
        
    return _component_event_buses[component]

# 패키지 레벨로 내보낼 항목들
__all__ = [
    'get_event_bus',
    'get_component_event_bus',
    'EventType',
    'EventTypes',
    'EventPriority',
    'SimpleEventBus',
    'Component',
    'StatusEventTypes',
    'TelegramEventTypes'
] 