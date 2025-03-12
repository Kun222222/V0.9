import asyncio
import json
import sys
import traceback
from crosskimp.ob_collector.orderbook.orderbook.bithumb_s_ob import BithumbSpotOrderBookManager
from crosskimp.ob_collector.utils.logging.logger import get_unified_logger

async def test_bithumb_snapshot():
    try:
        print("테스트 시작...")
        logger = get_unified_logger()
        print("로거 초기화 완료")
        
        manager = BithumbSpotOrderBookManager(logger=logger)
        print("매니저 초기화 완료")
        
        # 스냅샷 요청
        symbol = 'BTC'
        print(f"빗썸 {symbol} 스냅샷 요청 시작")
        snapshot = await manager.fetch_snapshot(symbol)
        print(f"스냅샷 요청 완료: {snapshot is not None}")
        
        if snapshot:
            # 스냅샷 내용 출력
            print(f"스냅샷 데이터: {json.dumps(snapshot, indent=2)[:500]}...")
            
            # 스냅샷 파싱
            parsed = manager.parse_snapshot(snapshot, symbol)
            print("스냅샷 파싱 완료")
            
            # 파싱 결과 출력
            print(f"파싱 결과: 매수 {len(parsed.get('bids', []))}개, 매도 {len(parsed.get('asks', []))}개")
            if parsed.get('bids'):
                print(f"첫 번째 매수 호가: {parsed.get('bids', [])[0]}")
            else:
                print("매수 호가 없음")
                
            if parsed.get('asks'):
                print(f"첫 번째 매도 호가: {parsed.get('asks', [])[0]}")
            else:
                print("매도 호가 없음")
        else:
            print("스냅샷 요청 실패")
            
    except Exception as e:
        print(f"오류 발생: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    print(f"Python 버전: {sys.version}")
    print(f"현재 작업 디렉토리: {sys.path}")
    asyncio.run(test_bithumb_snapshot()) 