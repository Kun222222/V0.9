from crosskimp.ob_collector.orderbook.validator.validators import OrderBookValidator, ValidationError

def test_validator():
    # 검증기 초기화
    validator = OrderBookValidator("upbit", depth=5)
    
    # 테스트 케이스 1: 정상적인 오더북
    bids = [[1000, 1], [999, 2], [998, 3]]
    asks = [[1001, 1], [1002, 2], [1003, 3]]
    result = validator.validate_orderbook("KRW-BTC", bids, asks, timestamp=1234567890, sequence=1)
    print("테스트 1 - 정상 오더북:", result.is_valid)
    
    # 테스트 케이스 2: 가격 역전
    bids = [[1000, 1], [999, 2], [998, 3]]
    asks = [[999, 1], [1002, 2], [1003, 3]]  # 999가 매수/매도 모두에 있음
    result = validator.validate_orderbook("KRW-BTC", bids, asks, timestamp=1234567891, sequence=2)
    print("테스트 2 - 가격 역전:", result.is_valid)
    print("에러:", result.errors)
    
    # 테스트 케이스 3: 뎁스 초과
    bids = [[1000, 1], [999, 2], [998, 3], [997, 4], [996, 5], [995, 6]]  # 6개
    asks = [[1001, 1], [1002, 2], [1003, 3], [1004, 4], [1005, 5], [1006, 6]]  # 6개
    result = validator.validate_orderbook("KRW-BTC", bids, asks, timestamp=1234567892, sequence=3)
    print("테스트 3 - 뎁스 초과:", result.is_valid)
    print("에러:", result.errors)
    
    # 테스트 케이스 4: 순서 오류
    bids = [[1000, 1], [998, 2], [999, 3]]  # 순서가 잘못됨
    asks = [[1001, 1], [1003, 2], [1002, 3]]  # 순서가 잘못됨
    result = validator.validate_orderbook("KRW-BTC", bids, asks, timestamp=1234567893, sequence=4)
    print("테스트 4 - 순서 오류:", result.is_valid)
    print("에러:", result.errors)

if __name__ == "__main__":
    test_validator() 