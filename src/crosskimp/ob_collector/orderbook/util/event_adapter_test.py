"""
이벤트 어댑터 테스트 모듈

이 모듈은 이벤트 어댑터의 기능을 테스트하는 코드를 제공합니다.
"""

import asyncio
import time
from typing import Dict, Any
import inspect
import pprint

from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.util.event_adapter import get_event_adapter
from crosskimp.common.events.domains.orderbook import OrderbookEventTypes, EVENT_TYPE_MAPPING

# 로거 설정
logger = get_unified_logger()

# 콜백 호출 여부를 추적하기 위한 전역 변수
callback_called = False

# 테스트 콜백 함수
async def test_callback(event_data: Dict[str, Any]) -> None:
    """테스트 이벤트 콜백"""
    global callback_called
    callback_called = True
    logger.info(f"테스트 이벤트 수신: {event_data}")

async def run_tests():
    """어댑터 테스트 함수 실행"""
    global callback_called
    
    logger.info("이벤트 어댑터 테스트 시작")
    
    # 매핑 정보 출력
    logger.info(f"이벤트 타입 매핑: {EVENT_TYPE_MAPPING}")
    
    # 이벤트 어댑터 가져오기
    adapter = get_event_adapter()
    logger.info(f"어댑터 인스턴스: {adapter}")
    logger.info(f"어댑터 컴포넌트: {adapter.component_name}")
    logger.info(f"어댑터 이벤트버스: {adapter.event_bus}")
    
    # 이벤트 버스가 시작되고 안정화될 때까지 대기
    logger.info("이벤트 버스가 시작되길 대기 중...")
    await asyncio.sleep(1.0)
    
    # 테스트할 이벤트 타입
    test_event = OrderbookEventTypes.CONNECTION_STATUS
    logger.info(f"테스트 이벤트 타입: {test_event}")
    mapped_event = adapter._map_event_type(test_event)
    logger.info(f"매핑된 이벤트 타입: {mapped_event}")
    
    # 1. 이벤트 구독 테스트
    logger.info("1. 이벤트 구독 테스트")
    await adapter.subscribe(test_event, test_callback)
    logger.info(f"구독자 수: {adapter.get_subscriber_count(test_event)}")
    logger.info(f"전체 구독 정보: {adapter.subscriptions}")
    
    # 구독이 완료될 때까지 잠시 대기
    await asyncio.sleep(0.5)
    
    # 2. 이벤트 발행 테스트
    logger.info("2. 이벤트 발행 테스트")
    test_data = {
        "exchange": "test",
        "status": "connected",
        "timestamp": time.time()
    }
    logger.info(f"발행 데이터: {test_data}")
    await adapter.publish(test_event, test_data)
    
    # 이벤트 처리 대기
    logger.info("이벤트 처리 대기 중...")
    for i in range(5):  # 최대 5번 반복 (각 0.1초)
        await asyncio.sleep(0.1)
        if callback_called:
            logger.info("콜백이 호출됨!")
            break
    
    if not callback_called:
        logger.warning("콜백이 호출되지 않았습니다!")
    
    # 3. 이벤트 구독 해제 테스트
    logger.info("3. 이벤트 구독 해제 테스트")
    adapter.unsubscribe(test_event, test_callback)
    logger.info(f"구독 해제 후 구독자 수: {adapter.get_subscriber_count(test_event)}")
    
    # 4. 이벤트 통계 테스트
    logger.info("4. 이벤트 통계 테스트")
    stats = adapter.get_stats()
    logger.info(f"이벤트 통계: {pprint.pformat(stats)}")
    
    logger.info("이벤트 어댑터 테스트 완료")

# 메인 함수
async def main():
    """메인 테스트 함수"""
    try:
        await run_tests()
    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {str(e)}")
        logger.error(f"오류 세부정보: {str(e.__class__.__name__)}, {str(e.__traceback__.tb_lineno)}")
    finally:
        # 어댑터 큐 비우기
        adapter = get_event_adapter()
        await adapter.flush_queues()

# 스크립트로 실행 시
if __name__ == "__main__":
    asyncio.run(main())