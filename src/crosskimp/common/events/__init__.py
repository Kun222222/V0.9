"""
크로스킴프 이벤트 시스템 패키지

이 패키지는 컴포넌트 간 이벤트 기반 통신을 위한 기능을 제공합니다.
- 컴포넌트별 독립 이벤트 버스
- 컴포넌트 간 이벤트 브릿지
- 이벤트 타입 관리
- 이벤트 필터링
"""

# 이벤트 타입과 상수
from crosskimp.common.events.types import (
    EventPriority, 
    EventGroup, 
    EventTypes,
    Component,
    EVENT_ALIASES,
    EVENT_PRIORITIES,
    DEFAULT_PRIORITY
)

# 이벤트 버스 및 필터
from crosskimp.common.events.bus import (
    EventFilter,
    ComponentEventBus,
    create_event_filter
)

# 이벤트 관리자
from crosskimp.common.events.manager import (
    EventManager,
    get_event_manager,
    get_component_event_bus,
    publish_system_event,
    initialize_event_system,
    shutdown_event_system
)

# 도메인별 이벤트 타입
from crosskimp.common.events.domains import (
    OrderbookEventTypes
)

from crosskimp.common.events.domains.status import StatusEventTypes
from crosskimp.common.events.domains.telegram import TelegramEventTypes

# 편의상 제공하는 함수와 클래스
__all__ = [
    # 클래스
    'EventPriority', 'EventGroup', 'EventTypes', 'Component',
    'EventFilter', 'ComponentEventBus', 'EventManager',
    
    # 도메인 이벤트 타입
    'OrderbookEventTypes',
    'StatusEventTypes',
    'TelegramEventTypes',
    
    # 함수
    'get_event_manager', 'get_component_event_bus',
    'create_event_filter', 'publish_system_event',
    'initialize_event_system', 'shutdown_event_system',
    
    # 상수
    'EVENT_ALIASES', 'EVENT_PRIORITIES', 'DEFAULT_PRIORITY'
] 