"""
이벤트 도메인 패키지

이 패키지는 각 도메인별 이벤트 타입을 정의합니다.
"""

from crosskimp.common.events.domains.orderbook import OrderbookEventTypes, EVENT_TYPE_MAPPING as ORDERBOOK_EVENT_MAPPING
from crosskimp.common.events.domains.status import StatusEventTypes, EVENT_TYPE_MAPPING as STATUS_EVENT_MAPPING
from crosskimp.common.events.domains.telegram import TelegramEventTypes, EVENT_TYPE_MAPPING as TELEGRAM_EVENT_MAPPING

# 모든 이벤트 타입 매핑 통합
EVENT_TYPE_MAPPING = {
    **ORDERBOOK_EVENT_MAPPING,
    **STATUS_EVENT_MAPPING,
    **TELEGRAM_EVENT_MAPPING
}

__all__ = [
    'OrderbookEventTypes',
    'StatusEventTypes',
    'TelegramEventTypes',
    'EVENT_TYPE_MAPPING'
]