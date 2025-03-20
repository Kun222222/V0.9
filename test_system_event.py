#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SystemEventManager 직접 테스트
"""

from crosskimp.ob_collector.orderbook.util.system_event_manager import SystemEventManager, EVENT_TYPES

def main():
    # SystemEventManager 인스턴스 가져오기
    manager = SystemEventManager.get_instance()
    
    # 거래소 초기화
    exchange_code = "UPBIT"
    manager.initialize_exchange(exchange_code)
    
    # 현재 컨텍스트 설정
    manager.set_current_exchange(exchange_code)
    
    # 이벤트 발행 (거래소 코드 명시 안함)
    manager.publish_system_event_sync(
        EVENT_TYPES["ERROR_EVENT"],
        message="컨텍스트 사용 테스트",
        severity="info"
    )
    print("컨텍스트를 통한 이벤트 발행 성공!")
    
    # 명시적 거래소 코드로 이벤트 발행
    manager.publish_system_event_sync(
        EVENT_TYPES["ERROR_EVENT"],
        exchange_code="BYBIT",
        message="명시적 거래소 코드 테스트",
        severity="info"
    )
    print("명시적 거래소 코드로 이벤트 발행 성공!")
    
    # 컨텍스트 관리자 테스트
    with manager.with_exchange("BITHUMB"):
        manager.publish_system_event_sync(
            EVENT_TYPES["ERROR_EVENT"],
            message="컨텍스트 관리자 테스트",
            severity="info"
        )
    print("컨텍스트 관리자 테스트 성공!")
    
    # 컨텍스트 초기화
    manager.clear_current_exchange()
    print(f"현재 컨텍스트 거래소: {manager.current_exchange}")
    
    # 거래소 코드 없는 경우 (오류 예상)
    print("거래소 코드 없는 테스트를 시도합니다...")
    try:
        manager.publish_system_event_sync(
            EVENT_TYPES["ERROR_EVENT"],
            message="거래소 코드 없는 테스트",
            severity="info"
        )
        print("예외가 발생하지 않았습니다! 테스트 실패!")
    except ValueError as e:
        print(f"✓ 예상된 ValueError 발생: {e}")
    except Exception as e:
        print(f"X 다른 예외 발생: {e} ({type(e).__name__})")
    
    # 마무리
    print("모든 테스트 완료!")

if __name__ == "__main__":
    main() 