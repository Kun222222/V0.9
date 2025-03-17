from crosskimp.ob_collector.orderbook.validator.validators import OrderBookValidator, ValidationError
import asyncio
import json
from src.crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
import sys

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

class BybitOrderBookValidator(BaseOrderBookValidator):
    """
    바이빗 전용 오더북 검증 클래스
    
    바이빗은 스냅샷과 델타 업데이트를 모두 제공합니다.
    - 스냅샷: 전체 오더북 데이터
    - 델타: 변경된 호가 데이터 (추가/수정/삭제)
    """
    
    def __init__(self, exchange_code: str = "bybit", depth: int = 10):
        super().__init__(exchange_code, depth)
        self.orderbooks = {}  # symbol -> {"bids": {price: size}, "asks": {price: size}, ...}
    
    async def initialize_orderbook(self, symbol: str, data: dict) -> dict:
        """
        오더북 초기화 (스냅샷)
        """
        # 기본 검증 수행
        result = self.validate_orderbook(
            symbol=symbol,
            bids=data.get("bids", []),
            asks=data.get("asks", []),
            timestamp=data.get("timestamp"),
            sequence=data.get("sequence")
        )
        
        # 검증 통과한 경우 오더북 초기화
        if result.is_valid:
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = {
                    "bids": {},
                    "asks": {},
                    "timestamp": None,
                    "sequence": None
                }
            
            # 스냅샷 데이터로 오더북 초기화
            bids_dict = {bid[0]: bid[1] for bid in data.get("bids", [])}
            asks_dict = {ask[0]: ask[1] for ask in data.get("asks", [])}
            
            self.orderbooks[symbol].update({
                "bids": bids_dict,
                "asks": asks_dict,
                "timestamp": data.get("timestamp"),
                "sequence": data.get("sequence")
            })
            
            # 출력용 뎁스 제한 적용
            result.limited_bids = self._get_sorted_bids(symbol)[:self.depth]
            result.limited_asks = self._get_sorted_asks(symbol)[:self.depth]
        
        return result
    
    async def update(self, symbol: str, data: dict) -> dict:
        """
        오더북 업데이트 (델타)
        """
        # 기본 검증 수행
        result = self.validate_orderbook(
            symbol=symbol,
            bids=data.get("bids", []),
            asks=data.get("asks", []),
            timestamp=data.get("timestamp"),
            sequence=data.get("sequence")
        )
        
        # 검증 통과한 경우 오더북 업데이트
        if result.is_valid:
            # 심볼이 없으면 초기화
            if symbol not in self.orderbooks:
                return await self.initialize_orderbook(symbol, data)
            
            # 타임스탬프/시퀀스 업데이트
            self.orderbooks[symbol]["timestamp"] = data.get("timestamp")
            self.orderbooks[symbol]["sequence"] = data.get("sequence")
            
            # 매수 호가 업데이트
            for bid in data.get("bids", []):
                price, size = bid
                if size == 0:  # 수량이 0이면 삭제
                    self.orderbooks[symbol]["bids"].pop(price, None)
                else:  # 그 외에는 추가/수정
                    self.orderbooks[symbol]["bids"][price] = size
            
            # 매도 호가 업데이트
            for ask in data.get("asks", []):
                price, size = ask
                if size == 0:  # 수량이 0이면 삭제
                    self.orderbooks[symbol]["asks"].pop(price, None)
                else:  # 그 외에는 추가/수정
                    self.orderbooks[symbol]["asks"][price] = size
            
            # 출력용 뎁스 제한 적용
            result.limited_bids = self._get_sorted_bids(symbol)[:self.depth]
            result.limited_asks = self._get_sorted_asks(symbol)[:self.depth]
        
        return result
    
    def _get_sorted_bids(self, symbol: str) -> list:
        """매수 호가 정렬 (가격 내림차순)"""
        if symbol not in self.orderbooks:
            return []
        
        bids_dict = self.orderbooks[symbol]["bids"]
        return [[price, size] for price, size in sorted(bids_dict.items(), key=lambda x: float(x[0]), reverse=True)]
    
    def _get_sorted_asks(self, symbol: str) -> list:
        """매도 호가 정렬 (가격 오름차순)"""
        if symbol not in self.orderbooks:
            return []
        
        asks_dict = self.orderbooks[symbol]["asks"]
        return [[price, size] for price, size in sorted(asks_dict.items(), key=lambda x: float(x[0]))]
    
    def get_orderbook(self, symbol: str) -> dict:
        """
        심볼의 오더북 데이터 반환
        """
        if symbol not in self.orderbooks:
            return None
        
        # 출력용 뎁스 제한 적용
        return {
            "bids": self._get_sorted_bids(symbol)[:self.depth],
            "asks": self._get_sorted_asks(symbol)[:self.depth],
            "timestamp": self.orderbooks[symbol]["timestamp"],
            "sequence": self.orderbooks[symbol]["sequence"]
        }

async def test_bybit_validator():
    # 바이빗 로그 파일 경로
    log_file = 'logs/raw_data/bybit/250318_001251_bybit_raw_logger.log'

    # 검증기 초기화
    validator = BaseOrderBookValidator('bybit', 10)

    # 통계 변수
    snapshot_count = 0
    delta_count = 0
    valid_count = 0
    invalid_count = 0
    error_types = {}

    # 로그 파일 읽기
    with open(log_file, 'r') as f:
        lines = f.readlines()

    # 처음 100줄만 처리
    for line in lines[:100]:
        # 스냅샷 메시지 처리
        if 'type":"snapshot' in line:
            snapshot_count += 1
            data_start = line.find('{"topic')
            if data_start != -1:
                json_data = json.loads(line[data_start:])
                symbol = json_data['data']['s']
                
                # 가격과 수량을 float로 변환
                bids = [[float(b[0]), float(b[1])] for b in json_data['data']['b']]
                asks = [[float(a[0]), float(a[1])] for a in json_data['data']['a']]
                
                # 오더북 검증
                result = validator.validate_orderbook(
                    symbol, 
                    bids, 
                    asks, 
                    json_data['ts'], 
                    json_data['data']['u']
                )
                
                if result.is_valid:
                    valid_count += 1
                else:
                    invalid_count += 1
                    for error in result.errors:
                        error_types[error] = error_types.get(error, 0) + 1
                        
                print(f'Snapshot validation for {symbol}: {result.is_valid}, errors: {result.errors}')
                
        # 델타 메시지 처리
        elif 'type":"delta' in line:
            delta_count += 1
            data_start = line.find('{"topic')
            if data_start != -1:
                json_data = json.loads(line[data_start:])
                symbol = json_data['data']['s']
                
                # 가격과 수량을 float로 변환
                bids = [[float(b[0]), float(b[1])] for b in json_data['data']['b']]
                asks = [[float(a[0]), float(a[1])] for a in json_data['data']['a']]
                
                # 오더북 검증
                result = validator.validate_orderbook(
                    symbol, 
                    bids, 
                    asks, 
                    json_data['ts'], 
                    json_data['data']['u']
                )
                
                if result.is_valid:
                    valid_count += 1
                else:
                    invalid_count += 1
                    for error in result.errors:
                        error_types[error] = error_types.get(error, 0) + 1
                        
                print(f'Delta validation for {symbol}: {result.is_valid}, errors: {result.errors}')

    # 결과 요약 출력
    print(f'\nSummary: {snapshot_count} snapshots, {delta_count} deltas, {valid_count} valid, {invalid_count} invalid')
    print(f'Error types: {error_types}')

    # 현재 오더북 상태 출력
    print('\nCurrent orderbooks:')
    for symbol, data in validator.orderbooks.items():
        print(f'{symbol}: {len(data["bids"])} bids, {len(data["asks"])} asks')
        
        # 상위 5개 매수/매도 호가 출력
        limited_bids = validator._limit_depth(data["bids"])
        limited_asks = validator._limit_depth(data["asks"])
        
        print(f'  Top 5 bids: {limited_bids[:5]}')
        print(f'  Top 5 asks: {limited_asks[:5]}')

# 테스트 실행
if __name__ == "__main__":
    asyncio.run(test_bybit_validator()) 