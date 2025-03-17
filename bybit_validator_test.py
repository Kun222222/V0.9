import json
import sys

# 바이빗 로그 파일 경로
log_file = 'logs/raw_data/bybit/250318_001251_bybit_raw_logger.log'

# BaseOrderBookValidator 클래스 정의 (validators.py에서 가져온 코드)
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from enum import Enum

class ValidationError(Enum):
    """검증 오류 유형"""
    PRICE_INVERSION = "price_inversion"  # 가격 역전
    DEPTH_EXCEEDED = "depth_exceeded"    # 뎁스 초과
    ORDER_INVALID = "order_invalid"      # 순서 오류
    EMPTY_ORDERBOOK = "empty_orderbook"  # 빈 오더북
    INVALID_PRICE = "invalid_price"      # 유효하지 않은 가격
    INVALID_SIZE = "invalid_size"        # 유효하지 않은 수량
    SEQUENCE_GAP = "sequence_gap"        # 시퀀스 갭
    TIMESTAMP_INVALID = "timestamp_invalid"  # 유효하지 않은 타임스탬프

@dataclass
class ValidationResult:
    """검증 결과"""
    is_valid: bool
    errors: List[str] = None
    error_types: List[ValidationError] = None
    limited_bids: List[List[float]] = None  # 뎁스 제한된 매수 호가
    limited_asks: List[List[float]] = None  # 뎁스 제한된 매도 호가

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.error_types is None:
            self.error_types = []

class BaseOrderBookValidator:
    """
    기본 오더북 검증 클래스
    
    거래소별 오더북 데이터의 유효성을 검증합니다.
    - 가격 역전 검증
    - 뎁스 검증
    - 순서 검증
    - 가격/수량 유효성 검증
    - 시퀀스/타임스탬프 검증
    - 내부 뎁스 관리 (100개 이상) 및 출력 뎁스 제한 (10개)
    """
    
    def __init__(self, exchange_code: str, depth: int = 10):
        """
        초기화
        
        Args:
            exchange_code: 거래소 코드
            depth: 출력 뎁스 (기본값: 10)
        """
        self.exchange_code = exchange_code
        self.depth = depth
        self.last_sequence = {}  # symbol -> sequence
        self.last_timestamp = {}  # symbol -> timestamp
        self.orderbooks = {}  # symbol -> (bids, asks, timestamp, sequence)
        
    def validate_orderbook(self, symbol: str, bids: List[List[float]], 
                          asks: List[List[float]], timestamp: Optional[int] = None,
                          sequence: Optional[int] = None) -> ValidationResult:
        """
        오더북 데이터 검증 (델타 업데이트용)
        
        Args:
            symbol: 심볼
            bids: 매수 호가 목록 [[price, size], ...]
            asks: 매도 호가 목록 [[price, size], ...]
            timestamp: 타임스탬프
            sequence: 시퀀스 번호
            
        Returns:
            ValidationResult: 검증 결과
        """
        errors = []
        error_types = []
        
        # 1. 기본 검증
        if not bids and not asks:
            errors.append("빈 오더북")
            error_types.append(ValidationError.EMPTY_ORDERBOOK)
            return ValidationResult(False, errors, error_types)
            
        # 2. 가격/수량 유효성 검증
        price_errors = self._validate_prices_and_sizes(bids, asks)
        if price_errors:
            errors.extend(price_errors)
            error_types.extend([ValidationError.INVALID_PRICE] * len(price_errors))
            
        # 3. 가격 역전 검증
        if bids and asks:
            max_bid = max(bid[0] for bid in bids)
            min_ask = min(ask[0] for ask in asks)
            if max_bid >= min_ask:
                errors.append(f"가격 역전: {max_bid} >= {min_ask}")
                error_types.append(ValidationError.PRICE_INVERSION)
                
        # 4. 순서 검증
        if not self._validate_order(bids, reverse=True):
            errors.append("매수 호가 순서 오류")
            error_types.append(ValidationError.ORDER_INVALID)
        if not self._validate_order(asks, reverse=False):
            errors.append("매도 호가 순서 오류")
            error_types.append(ValidationError.ORDER_INVALID)
            
        # 5. 시퀀스/타임스탬프 검증
        if sequence is not None:
            seq_errors = self._validate_sequence(symbol, sequence)
            if seq_errors:
                errors.extend(seq_errors)
                error_types.extend([ValidationError.SEQUENCE_GAP] * len(seq_errors))
                
        if timestamp is not None:
            ts_errors = self._validate_timestamp(symbol, timestamp)
            if ts_errors:
                errors.extend(ts_errors)
                error_types.extend([ValidationError.TIMESTAMP_INVALID] * len(ts_errors))
        
        # 6. 오더북 업데이트
        if len(errors) == 0:  # 검증 통과한 경우에만 업데이트
            self._update_orderbook(symbol, bids, asks, timestamp, sequence)
        
        # 7. 출력용 뎁스 제한 적용
        limited_bids = self._limit_depth(self.orderbooks.get(symbol, {}).get("bids", []))
        limited_asks = self._limit_depth(self.orderbooks.get(symbol, {}).get("asks", []))
                
        return ValidationResult(len(errors) == 0, errors, error_types, limited_bids, limited_asks)
        
    def _validate_prices_and_sizes(self, bids: List[List[float]], 
                                 asks: List[List[float]]) -> List[str]:
        """가격과 수량의 유효성 검증"""
        errors = []
        
        # 매수 호가 검증
        for bid in bids:
            if len(bid) != 2:
                errors.append(f"잘못된 매수 호가 형식: {bid}")
                continue
                
            price, size = bid
            if price <= 0:
                errors.append(f"유효하지 않은 매수가: {price}")
            if size < 0:  # 0은 삭제를 의미하므로 허용
                errors.append(f"유효하지 않은 매수량: {size}")
                
        # 매도 호가 검증
        for ask in asks:
            if len(ask) != 2:
                errors.append(f"잘못된 매도 호가 형식: {ask}")
                continue
                
            price, size = ask
            if price <= 0:
                errors.append(f"유효하지 않은 매도가: {price}")
            if size < 0:  # 0은 삭제를 의미하므로 허용
                errors.append(f"유효하지 않은 매도량: {size}")
                
        return errors
        
    def _validate_order(self, orders: List[List[float]], reverse: bool) -> bool:
        """호가 순서 검증"""
        if not orders:
            return True
        prices = [order[0] for order in orders]
        return prices == sorted(prices, reverse=reverse)
        
    def _validate_sequence(self, symbol: str, sequence: int) -> List[str]:
        """시퀀스 검증"""
        errors = []
        
        if symbol in self.last_sequence:
            last_seq = self.last_sequence[symbol]
            if sequence <= last_seq:
                errors.append(f"시퀀스 갭: last={last_seq}, current={sequence}")
                
        self.last_sequence[symbol] = sequence
        return errors
        
    def _validate_timestamp(self, symbol: str, timestamp: int) -> List[str]:
        """타임스탬프 검증"""
        errors = []
        
        if symbol in self.last_timestamp:
            last_ts = self.last_timestamp[symbol]
            if timestamp < last_ts:
                errors.append(f"타임스탬프 역전: last={last_ts}, current={timestamp}")
                
        self.last_timestamp[symbol] = timestamp
        return errors
        
    def _limit_depth(self, orders: List[List[float]]) -> List[List[float]]:
        """호가 뎁스 제한"""
        if not orders:
            return []
        return orders[:self.depth]
        
    def _update_orderbook(self, symbol: str, bids: List[List[float]], 
                         asks: List[List[float]], timestamp: Optional[int] = None,
                         sequence: Optional[int] = None) -> None:
        """
        오더북 데이터 업데이트
        
        Args:
            symbol: 심볼
            bids: 매수 호가 목록
            asks: 매도 호가 목록
            timestamp: 타임스탬프
            sequence: 시퀀스 번호
        """
        # 심볼별 오더북 데이터 초기화
        if symbol not in self.orderbooks:
            self.orderbooks[symbol] = {
                "bids": [],
                "asks": [],
                "timestamp": None,
                "sequence": None
            }
        
        # 델타 업데이트 처리 (바이빗 방식)
        current_bids = {float(bid[0]): float(bid[1]) for bid in self.orderbooks[symbol].get("bids", [])}
        current_asks = {float(ask[0]): float(ask[1]) for ask in self.orderbooks[symbol].get("asks", [])}
        
        # 매수 호가 업데이트
        for price, size in bids:
            if size == 0:  # 삭제
                current_bids.pop(price, None)
            else:  # 추가 또는 수정
                current_bids[price] = size
                
        # 매도 호가 업데이트
        for price, size in asks:
            if size == 0:  # 삭제
                current_asks.pop(price, None)
            else:  # 추가 또는 수정
                current_asks[price] = size
        
        # 정렬된 리스트로 변환
        sorted_bids = sorted(current_bids.items(), key=lambda x: x[0], reverse=True)
        sorted_asks = sorted(current_asks.items(), key=lambda x: x[0])
        
        # 데이터 업데이트
        self.orderbooks[symbol].update({
            "bids": [[price, size] for price, size in sorted_bids],
            "asks": [[price, size] for price, size in sorted_asks],
            "timestamp": timestamp,
            "sequence": sequence
        })
        
    def get_orderbook(self, symbol: str) -> Optional[Dict]:
        """
        심볼의 오더북 데이터 반환
        
        Args:
            symbol: 심볼
            
        Returns:
            Optional[Dict]: 오더북 데이터 (없으면 None)
        """
        if symbol not in self.orderbooks:
            return None
            
        # 출력용 뎁스 제한 적용
        return {
            "bids": self._limit_depth(self.orderbooks[symbol]["bids"]),
            "asks": self._limit_depth(self.orderbooks[symbol]["asks"]),
            "timestamp": self.orderbooks[symbol]["timestamp"],
            "sequence": self.orderbooks[symbol]["sequence"]
        }

# 테스트 코드
if __name__ == "__main__":
    # 검증기 초기화 - 뎁스를 10으로 설정
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
        
        # 상위 10개 매수/매도 호가 출력
        limited_bids = validator._limit_depth(data["bids"])
        limited_asks = validator._limit_depth(data["asks"])
        
        print(f'  Top 10 bids: {limited_bids}')
        print(f'  Top 10 asks: {limited_asks}') 