"""
이벤트 시스템 마이그레이션 테스트 스크립트

이 스크립트는 새로운 이벤트 시스템이 제대로 작동하는지 확인합니다.
여러 컴포넌트 간의 이벤트 발행 및 구독을 테스트합니다.
"""

import asyncio
import time
from typing import Dict, Any, List

from crosskimp.common.events import (
    initialize_event_system,
    shutdown_event_system,
    get_component_event_bus,
    Component,
    StatusEventTypes,
    TelegramEventTypes
)

# 테스트 결과 저장
test_results = []
received_events = []

# 테스트 이벤트 핸들러
async def test_event_handler(event_data: Dict[str, Any]) -> None:
    """테스트용 이벤트 핸들러"""
    print(f"이벤트 수신: {event_data}")
    received_events.append(event_data)

async def run_test():
    """테스트 실행"""
    print("이벤트 시스템 마이그레이션 테스트 시작...")
    
    # 1. 이벤트 시스템 초기화
    await initialize_event_system()
    print("이벤트 시스템 초기화 완료")
    
    # 2. 각 컴포넌트의 이벤트 버스 획득
    server_bus = get_component_event_bus(Component.SERVER)
    telegram_bus = get_component_event_bus(Component.TELEGRAM)
    
    print("각 컴포넌트 이벤트 버스 획득 완료")
    
    # 3. 이벤트 구독 설정
    await server_bus.subscribe(StatusEventTypes.SYSTEM_STATUS_CHANGE, test_event_handler)
    await telegram_bus.subscribe(TelegramEventTypes.BOT_STARTUP, test_event_handler)
    
    print("이벤트 구독 설정 완료")
    
    # 4. 이벤트 발행 테스트
    print("\n이벤트 발행 테스트 시작...")
    
    # 4.1 시스템 상태 이벤트 발행
    test_data = {
        "old_status": "NORMAL", 
        "new_status": "WARNING", 
        "message": "테스트 상태 변경", 
        "timestamp": time.time()
    }
    await server_bus.publish(StatusEventTypes.SYSTEM_STATUS_CHANGE, test_data)
    print(f"상태 이벤트 발행: {test_data}")
    
    # 4.2 텔레그램 이벤트 발행
    test_data = {
        "message": "텔레그램 봇 시작", 
        "timestamp": time.time()
    }
    await telegram_bus.publish(TelegramEventTypes.BOT_STARTUP, test_data)
    print(f"텔레그램 이벤트 발행: {test_data}")
    
    # 5. 이벤트 수신 확인을 위한 대기
    await asyncio.sleep(2)  # 대기 시간 증가
    
    # 6. 테스트 결과 확인
    print(f"\n수신된 이벤트 수: {len(received_events)}")
    
    success = True
    if len(received_events) != 2:  # 2개의 이벤트만 확인
        print(f"❌ 일부 이벤트가 수신되지 않았습니다. 수신된 이벤트 수: {len(received_events)}")
        success = False
    else:
        print("✅ 모든 이벤트가 정상적으로 수신되었습니다.")
    
    # 안전한 구독 해제 (None 체크 추가)
    try:
        if server_bus:
            await server_bus.unsubscribe(StatusEventTypes.SYSTEM_STATUS_CHANGE, test_event_handler)
        if telegram_bus:
            await telegram_bus.unsubscribe(TelegramEventTypes.BOT_STARTUP, test_event_handler)
    except Exception as e:
        print(f"구독 해제 중 오류: {e}")
        success = False
    
    # 8. 이벤트 시스템 종료
    try:
        await shutdown_event_system()
        print("이벤트 시스템 종료 완료")
    except Exception as e:
        print(f"이벤트 시스템 종료 중 오류: {e}")
        success = False
    
    # 9. 테스트 결과 요약
    if success:
        test_results.append("이벤트 발행 및 수신 테스트: 성공")
    else:
        test_results.append("이벤트 발행 및 수신 테스트: 실패")
    
    print("\n테스트 결과 요약:")
    for result in test_results:
        print(f"- {result}")
    
    print("\n이벤트 시스템 마이그레이션 테스트 완료.")

if __name__ == "__main__":
    asyncio.run(run_test()) 