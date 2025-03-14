import asyncio
import json
import time
from crosskimp.ob_collector.orderbook.orderbook.binance_f_ob import BinanceFutureOrderBook, BinanceFutureOrderBookManager, parse_binance_future_depth_update
from crosskimp.ob_collector.utils.logging.logger import get_unified_logger

logger = get_unified_logger()

# 테스트용 샘플 데이터
SAMPLE_DEPTH_UPDATE = {
    "e": "depthUpdate",
    "E": 1741940983152,
    "T": 1741940983151,
    "s": "LINKUSDT",
    "U": 7023488347477,
    "u": 7023488358620,
    "pu": 7023488347473,
    "b": [
        ["13.579", "1481.45"],
        ["14.105", "2916.94"],
        ["14.106", "11826.62"],
        ["14.110", "4081.66"],
        ["14.136", "487.44"],
        ["14.137", "2973.89"]
    ],
    "a": [
        ["14.206", "185.20"],
        ["14.207", "494.72"],
        ["14.210", "542.97"],
        ["14.211", "1065.67"],
        ["14.214", "552.47"],
        ["14.215", "575.86"]
    ]
}

# 가격 역전 테스트용 데이터
PRICE_INVERSION_UPDATE = {
    "e": "depthUpdate",
    "E": 1741940983152,
    "T": 1741940983151,
    "s": "LINKUSDT",
    "U": 7023488358621,
    "u": 7023488358630,
    "pu": 7023488358620,
    "b": [
        ["14.210", "1000.0"]  # 매수가가 매도가보다 높음 (14.210 > 14.206)
    ],
    "a": []
}

# 스냅샷 데이터
SNAPSHOT_DATA = {
    "exchangename": "binancefuture",
    "symbol": "LINK",
    "bids": [
        [13.579, 1481.45],
        [13.578, 2916.94],
        [13.577, 11826.62]
    ],
    "asks": [
        [13.580, 185.20],
        [13.581, 494.72],
        [13.582, 542.97]
    ],
    "timestamp": 1741940983152,
    "sequence": 7023488347476
}

# 시퀀스 연속성 테스트용 데이터
SEQUENCE_CONTINUITY_UPDATE1 = {
    "e": "depthUpdate",
    "E": 1741940983153,
    "T": 1741940983152,
    "s": "LINKUSDT",
    "U": 7023488358631,
    "u": 7023488358640,
    "pu": 7023488358630,  # 이전 이벤트의 u와 일치
    "b": [
        ["14.150", "500.0"]
    ],
    "a": []
}

SEQUENCE_CONTINUITY_UPDATE2 = {
    "e": "depthUpdate",
    "E": 1741940983154,
    "T": 1741940983153,
    "s": "LINKUSDT",
    "U": 7023488358641,
    "u": 7023488358650,
    "pu": 7023488358640,  # 이전 이벤트의 u와 일치
    "b": [
        ["14.160", "600.0"]
    ],
    "a": []
}

# 시퀀스 불일치 테스트용 데이터
SEQUENCE_MISMATCH_UPDATE = {
    "e": "depthUpdate",
    "E": 1741940983155,
    "T": 1741940983154,
    "s": "LINKUSDT",
    "U": 7023488358651,
    "u": 7023488358660,
    "pu": 7023488358645,  # 이전 이벤트의 u와 불일치
    "b": [
        ["14.170", "700.0"]
    ],
    "a": []
}

class MockQueue:
    def __init__(self):
        self.items = []
        self.put_count = 0
    
    async def put(self, item):
        self.items.append(item)
        self.put_count += 1
        logger.info(f"큐에 데이터 추가: {item[0]}, {item[1]['symbol']}, 시퀀스: {item[1]['sequence']}")

async def test_orderbook_update():
    logger.info("바이낸스 선물 오더북 업데이트 테스트 시작")
    
    # 오더북 매니저 초기화
    manager = BinanceFutureOrderBookManager()
    
    # 출력 큐 설정
    mock_queue = MockQueue()
    manager.set_output_queue(mock_queue)
    
    # 매니저 시작
    await manager.start()
    
    # 스냅샷으로 초기화
    result = await manager.initialize_orderbook("LINK", SNAPSHOT_DATA)
    logger.info(f"스냅샷 초기화 결과: {result.is_valid}, {result.error_messages}")
    
    # 오더북 객체 확인
    ob = manager.get_orderbook("LINK")
    if ob:
        logger.info(f"오더북 초기화 상태: {ob.bids[0] if ob.bids else None}, {ob.asks[0] if ob.asks else None}")
    else:
        logger.error("오더북 객체가 생성되지 않음")
        return
    
    # 델타 업데이트 테스트
    logger.info("델타 업데이트 테스트")
    result = await manager.update("LINK", SAMPLE_DEPTH_UPDATE)
    logger.info(f"델타 업데이트 결과: {result.is_valid}, {result.error_messages}")
    
    # 업데이트 후 오더북 상태 확인
    logger.info(f"업데이트 후 오더북 상태: {ob.bids[0] if ob.bids else None}, {ob.asks[0] if ob.asks else None}")
    logger.info(f"큐 전송 횟수: {mock_queue.put_count}")
    
    # 가격 역전 테스트
    logger.info("가격 역전 테스트")
    result = await manager.update("LINK", PRICE_INVERSION_UPDATE)
    logger.info(f"가격 역전 업데이트 결과: {result.is_valid}, {result.error_messages}")
    
    # 가격 역전 후 오더북 상태 확인
    logger.info(f"가격 역전 후 오더북 상태: {ob.bids[0] if ob.bids else None}, {ob.asks[0] if ob.asks else None}")
    logger.info(f"큐 전송 횟수: {mock_queue.put_count}")
    
    # 시퀀스 연속성 테스트
    logger.info("시퀀스 연속성 테스트")
    result = await manager.update("LINK", SEQUENCE_CONTINUITY_UPDATE1)
    logger.info(f"시퀀스 연속성 업데이트1 결과: {result.is_valid}, {result.error_messages}")
    
    result = await manager.update("LINK", SEQUENCE_CONTINUITY_UPDATE2)
    logger.info(f"시퀀스 연속성 업데이트2 결과: {result.is_valid}, {result.error_messages}")
    
    # 시퀀스 불일치 테스트
    logger.info("시퀀스 불일치 테스트")
    result = await manager.update("LINK", SEQUENCE_MISMATCH_UPDATE)
    logger.info(f"시퀀스 불일치 업데이트 결과: {result.is_valid}, {result.error_messages}")
    
    # 업데이트 후 오더북 상태 확인
    logger.info(f"최종 오더북 상태: {ob.bids[0] if ob.bids else None}, {ob.asks[0] if ob.asks else None}")
    logger.info(f"최종 큐 전송 횟수: {mock_queue.put_count}")
    
    # 파싱 함수 테스트
    logger.info("파싱 함수 테스트")
    parsed_data = parse_binance_future_depth_update(SAMPLE_DEPTH_UPDATE)
    logger.info(f"파싱 결과: {parsed_data['symbol'] if parsed_data else None}")
    
    # 직접 오더북 객체 테스트
    logger.info("직접 오더북 객체 테스트")
    direct_ob = BinanceFutureOrderBook("binancefuture", "LINK")
    direct_ob.set_output_queue(mock_queue)
    direct_ob.update_orderbook(
        bids=[[13.579, 1481.45], [13.578, 2916.94]],
        asks=[[13.580, 185.20], [13.581, 494.72]],
        timestamp=1741940983152,
        sequence=7023488347476
    )
    logger.info(f"직접 업데이트 후 오더북 상태: {direct_ob.bids[0] if direct_ob.bids else None}, {direct_ob.asks[0] if direct_ob.asks else None}")
    
    # C++로 전송 테스트
    logger.info("C++ 전송 테스트")
    await direct_ob.send_to_cpp()
    
    # 큐로 전송 테스트
    logger.info("큐 전송 테스트")
    await direct_ob.send_to_queue()
    logger.info(f"최종 큐 전송 횟수: {mock_queue.put_count}")
    
    logger.info("바이낸스 선물 오더북 업데이트 테스트 완료")

async def main():
    await test_orderbook_update()

if __name__ == "__main__":
    asyncio.run(main()) 