#!/usr/bin/env python
import logging
logging.basicConfig(level=logging.DEBUG)
import sys
import os

# 현재 디렉토리를 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from crosskimp.ob_collector.orderbook.util.system_event_manager import SystemEventManager, EVENT_TYPES
from crosskimp.ob_collector.orderbook.util.event_bus import EventBus

def handle_test_event(data):
    print(f'테스트 이벤트 수신: {data}')

def main():
    print("이벤트 테스트 시작...")
    manager = SystemEventManager.get_instance()
    event_bus = EventBus.get_instance()
    
    # 시스템 이벤트 구독
    event_bus.subscribe('system_event', handle_test_event)
    print("이벤트 구독 완료")
    
    # 테스트 이벤트 발행
    print("첫 번째 이벤트 발행...")
    manager.publish_event_sync(
        EVENT_TYPES['METRIC_UPDATE'],
        exchange_code='test_exchange',
        metric_name='message_count',
        value=1
    )
    
    # 한 번 더 발행해서 누적되는지 확인
    print("두 번째 이벤트 발행...")
    manager.publish_event_sync(
        EVENT_TYPES['METRIC_UPDATE'],
        exchange_code='test_exchange',
        metric_name='message_count',
        value=1
    )
    
    print('현재 메트릭:', manager.metrics)
    print("이벤트 테스트 완료")

if __name__ == "__main__":
    main() 