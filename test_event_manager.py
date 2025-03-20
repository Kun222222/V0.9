#!/usr/bin/env python
# -*- coding: utf-8 -*-

from crosskimp.ob_collector.orderbook.util.system_event_manager import SystemEventManager, EVENT_TYPES

# 테스트 함수
def test_with_exchange_context():
    """거래소 컨텍스트 관리자 테스트"""
    manager = SystemEventManager.get_instance()
    
    # 1. 거래소 코드를 명시적으로 전달
    manager.publish_system_event_sync(
        EVENT_TYPES["ERROR_EVENT"],
        exchange_code="UPBIT",
        message="명시적 거래소 코드 테스트",
        severity="info"
    )
    print("명시적 거래소 코드로 이벤트 발행 완료")
    
    # 2. 컨텍스트 관리자 사용
    with manager.with_exchange("BITHUMB"):
        manager.publish_system_event_sync(
            EVENT_TYPES["ERROR_EVENT"],
            message="컨텍스트 관리자 테스트 (빗썸)",
            severity="info"
        )
    print("빗썸 컨텍스트에서 이벤트 발행 완료")
    
    # 3. 컨텍스트 관리자 중첩 사용
    with manager.with_exchange("UPBIT"):
        manager.publish_system_event_sync(
            EVENT_TYPES["ERROR_EVENT"],
            message="컨텍스트 관리자 테스트 (업비트)",
            severity="info"
        )
        
        # 내부에서 다른 거래소로 변경
        with manager.with_exchange("BYBIT"):
            manager.publish_system_event_sync(
                EVENT_TYPES["ERROR_EVENT"],
                message="중첩 컨텍스트 테스트 (바이비트)",
                severity="info"
            )
            
        # 다시 외부 컨텍스트로 복원
        manager.publish_system_event_sync(
            EVENT_TYPES["ERROR_EVENT"],
            message="컨텍스트 복원 테스트 (업비트)",
            severity="info"
        )
    
    print("컨텍스트 중첩 테스트 완료")
    
    # 4. 거래소 코드 없이 호출 (오류 발생 예상)
    try:
        manager.clear_current_exchange()  # 현재 컨텍스트 초기화
        manager.publish_system_event_sync(
            EVENT_TYPES["ERROR_EVENT"],
            message="거래소 코드 없음 테스트",
            severity="info"
        )
    except Exception as e:
        print(f"예상된 오류 발생: {e}")
    
    print("테스트 완료!")

if __name__ == "__main__":
    test_with_exchange_context() 