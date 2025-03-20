#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
BaseWebsocketConnector 테스트
"""

from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.util.system_event_manager import EVENT_TYPES

# 테스트용 커넥터 클래스
class TestConnector(BaseWebsocketConnector):
    async def connect(self):
        # 추상 메서드 구현 (테스트용이므로 아무것도 하지 않음)
        return True

def main():
    # 테스트 커넥터 생성
    connector = TestConnector({}, 'UPBIT')
    
    # 1. exchange_code 명시적 지정 없이 이벤트 발행
    connector.publish_system_event_sync(
        EVENT_TYPES["ERROR_EVENT"],
        message="명시적 거래소 코드 없이 이벤트 발행",
        severity="info"
    )
    print("거래소 컨텍스트 활용한 이벤트 발행 성공!")
    
    # 2. 명시적 지정 테스트
    connector.publish_system_event_sync(
        EVENT_TYPES["ERROR_EVENT"],
        exchange_code="BYBIT",  # 다른 거래소 코드 지정
        message="명시적 거래소 코드로 이벤트 발행",
        severity="info"
    )
    print("명시적 거래소 코드로 이벤트 발행 성공!")
    
    print("모든 테스트 완료!")

if __name__ == "__main__":
    main() 