"""
오더북 검증 모듈

이 모듈은 거래소별 오더북 데이터의 유효성을 검증하는 클래스를 제공합니다.
"""

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
        limited_bids = self._limit_depth(self.orderbooks[symbol]["bids"])
        limited_asks = self._limit_depth(self.orderbooks[symbol]["asks"])
                
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
            if size <= 0:
                errors.append(f"유효하지 않은 매수량: {size}")
                
        # 매도 호가 검증
        for ask in asks:
            if len(ask) != 2:
                errors.append(f"잘못된 매도 호가 형식: {ask}")
                continue
                
            price, size = ask
            if price <= 0:
                errors.append(f"유효하지 않은 매도가: {price}")
            if size <= 0:
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
        
        # 데이터 업데이트
        self.orderbooks[symbol].update({
            "bids": bids,
            "asks": asks,
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
        
    def clear_symbol(self, symbol: str) -> None:
        """심볼 데이터 제거"""
        self.orderbooks.pop(symbol, None)
        
    def clear_all(self) -> None:
        """전체 데이터 제거"""
        self.orderbooks.clear()

class UpbitOrderBookValidator(BaseOrderBookValidator):
    """
    업비트 전용 오더북 검증 클래스
    
    업비트의 스냅샷 형태 오더북 데이터를 검증합니다.
    - 타임스탬프 기반 시퀀스 관리
    - 전체 스냅샷 검증
    """
    
    def __init__(self, exchange_code: str = "upbit", depth: int = 10):
        super().__init__(exchange_code, depth)
        self.last_timestamp = {}  # symbol -> timestamp
        
    async def initialize_orderbook(self, symbol: str, data: Dict) -> ValidationResult:
        """
        오더북 초기화 (업비트 전용)
        
        Args:
            symbol: 심볼
            data: 오더북 데이터
            
        Returns:
            ValidationResult: 검증 결과
        """
        # 스냅샷 데이터 검증
        return self.validate_orderbook(
            symbol=symbol,
            bids=data.get("bids", []),
            asks=data.get("asks", []),
            timestamp=data.get("timestamp")
        )
        
    async def update(self, symbol: str, data: Dict) -> ValidationResult:
        """
        오더북 업데이트 (업비트 전용)
        
        업비트는 항상 스냅샷을 받으므로, update도 initialize와 동일하게 처리합니다.
        
        Args:
            symbol: 심볼
            data: 오더북 데이터
            
        Returns:
            ValidationResult: 검증 결과
        """
        return await self.initialize_orderbook(symbol, data)
        
    def validate_orderbook(self, symbol: str, bids: List[List[float]], 
                          asks: List[List[float]], timestamp: Optional[int] = None,
                          sequence: Optional[int] = None) -> ValidationResult:
        """
        업비트 오더북 데이터 검증 (스냅샷)
        
        Args:
            symbol: 심볼
            bids: 매수 호가 목록 [[price, size], ...]
            asks: 매도 호가 목록 [[price, size], ...]
            timestamp: 타임스탬프
            sequence: 시퀀스 번호 (업비트에서는 사용하지 않음)
            
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
            
        # 2. 타임스탬프 검증 (업비트는 타임스탬프 기반)
        if timestamp is not None:
            ts_errors = self._validate_timestamp(symbol, timestamp)
            if ts_errors:
                errors.extend(ts_errors)
                error_types.extend([ValidationError.TIMESTAMP_INVALID] * len(ts_errors))
                return ValidationResult(False, errors, error_types)  # 타임스탬프 오류는 즉시 반환
        
        # 3. 가격/수량 유효성 검증
        price_errors = self._validate_prices_and_sizes(bids, asks)
        if price_errors:
            errors.extend(price_errors)
            error_types.extend([ValidationError.INVALID_PRICE] * len(price_errors))
            
        # 4. 가격 역전 검증
        if bids and asks:
            max_bid = max(bid[0] for bid in bids)
            min_ask = min(ask[0] for ask in asks)
            if max_bid >= min_ask:
                errors.append(f"가격 역전: {max_bid} >= {min_ask}")
                error_types.append(ValidationError.PRICE_INVERSION)
                
        # 5. 순서 검증
        if not self._validate_order(bids, reverse=True):
            errors.append("매수 호가 순서 오류")
            error_types.append(ValidationError.ORDER_INVALID)
        if not self._validate_order(asks, reverse=False):
            errors.append("매도 호가 순서 오류")
            error_types.append(ValidationError.ORDER_INVALID)
        
        # 6. 오더북 업데이트 (스냅샷 방식)
        if len(errors) == 0:  # 검증 통과한 경우에만 업데이트
            self._update_orderbook(symbol, bids, asks, timestamp)
        
        # 7. 출력용 뎁스 제한 적용
        limited_bids = self._limit_depth(self.orderbooks[symbol]["bids"])
        limited_asks = self._limit_depth(self.orderbooks[symbol]["asks"])
                
        return ValidationResult(len(errors) == 0, errors, error_types, limited_bids, limited_asks)
        
    def _validate_timestamp(self, symbol: str, timestamp: int) -> List[str]:
        """
        타임스탬프 검증 (업비트 전용)
        - 타임스탬프가 이전보다 작거나 같으면 오류
        """
        errors = []
        
        if symbol in self.last_timestamp:
            last_ts = self.last_timestamp[symbol]
            if timestamp <= last_ts:
                errors.append(f"타임스탬프 역전: last={last_ts}, current={timestamp}")
                
        self.last_timestamp[symbol] = timestamp
        return errors
        
    def _update_orderbook(self, symbol: str, bids: List[List[float]], 
                         asks: List[List[float]], timestamp: Optional[int] = None) -> None:
        """
        오더북 데이터 업데이트 (업비트 전용)
        - 스냅샷 방식으로 전체 데이터 교체
        """
        # 심볼별 오더북 데이터 초기화
        if symbol not in self.orderbooks:
            self.orderbooks[symbol] = {
                "bids": [],
                "asks": [],
                "timestamp": None
            }
        
        # 데이터 업데이트 (스냅샷 방식)
        self.orderbooks[symbol].update({
            "bids": bids,
            "asks": asks,
            "timestamp": timestamp
        }) 