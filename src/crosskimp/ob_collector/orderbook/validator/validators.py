"""
오더북 검증 모듈

이 모듈은 거래소별 오더북 데이터의 유효성을 검증하는 클래스를 제공합니다.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from enum import Enum

from crosskimp.logger.logger import get_unified_logger

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
    is_valid: bool = False
    errors: List[str] = None
    error_types: List[ValidationError] = None
    warnings: List[str] = None
    limited_bids: List[List[float]] = None  # 뎁스 제한된 매수 호가
    limited_asks: List[List[float]] = None  # 뎁스 제한된 매도 호가

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.error_types is None:
            self.error_types = []
        if self.warnings is None:
            self.warnings = []

class BaseOrderBookValidator:
    """기본 오더북 검증기"""
    
    def __init__(self, exchange_code: str):
        """
        초기화
        
        Args:
            exchange_code: 거래소 코드
        """
        # 거래소 코드는 소문자로 통일
        self.exchange_code = exchange_code.lower()
        
        # 오더북 상태 관리
        self.orderbooks = {}  # symbol -> orderbook
        self.sequences = {}   # symbol -> sequence
        self.logger = get_unified_logger()
        
        # 제한적인 오더북 깊이 (일반적으로 20-50개 사용)
        self.max_orderbook_depth = 50
        
        # 출력용 깊이 제한 (로그 및 출력용)
        self.output_depth = 10
        
        self.last_sequence = {}  # symbol -> sequence
        self.last_timestamp = {}  # symbol -> timestamp
        self.orderbooks_dict = {}  # symbol -> {"bids": {price: size}, "asks": {price: size}}
        
    async def initialize_orderbook(self, symbol: str, data: Dict) -> ValidationResult:
        """
        오더북 초기화 (스냅샷)
        
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
            timestamp=data.get("timestamp"),
            sequence=data.get("sequence"),
            is_snapshot=True
        )
        
    async def update(self, symbol: str, data: Dict) -> ValidationResult:
        """
        오더북 업데이트 (델타)
        
        Args:
            symbol: 심볼
            data: 오더북 데이터
            
        Returns:
            ValidationResult: 검증 결과
        """
        # 델타 업데이트 데이터 검증
        return self.validate_orderbook(
            symbol=symbol,
            bids=data.get("bids", []),
            asks=data.get("asks", []),
            timestamp=data.get("timestamp"),
            sequence=data.get("sequence"),
            is_snapshot=False
        )
        
    def validate_orderbook(self, symbol: str, bids: List[List[float]], 
                          asks: List[List[float]], timestamp: Optional[int] = None,
                          sequence: Optional[int] = None, is_snapshot: bool = False) -> ValidationResult:
        """
        오더북 데이터 검증
        
        Args:
            symbol: 심볼
            bids: 매수 호가 목록 [[price, size], ...]
            asks: 매도 호가 목록 [[price, size], ...]
            timestamp: 타임스탬프
            sequence: 시퀀스 번호
            is_snapshot: 스냅샷 여부
            
        Returns:
            ValidationResult: 검증 결과
        """
        errors = []
        error_types = []
        warnings = []  # 경고 메시지 추가
        
        # 1. 기본 검증 (스냅샷인 경우에만 빈 오더북 검증)
        if is_snapshot and not bids and not asks:
            errors.append("빈 오더북")
            error_types.append(ValidationError.EMPTY_ORDERBOOK)
            return ValidationResult(
                is_valid=False,
                errors=errors,
                error_types=error_types
            )
            
        # 2. 가격/수량 유효성 검증 (0 크기는 삭제 명령이므로 허용)
        price_errors = self._validate_prices_and_sizes(bids, asks, allow_zero_size=not is_snapshot)
        if price_errors:
            errors.extend(price_errors)
            error_types.extend([ValidationError.INVALID_PRICE] * len(price_errors))
            
        # 3. 시퀀스 검증
        if sequence is not None:
            seq_errors = self._validate_sequence(symbol, sequence)
            if seq_errors:
                errors.extend(seq_errors)
                error_types.extend([ValidationError.SEQUENCE_GAP] * len(seq_errors))
                
        # 4. 타임스탬프 검증
        if timestamp is not None:
            ts_errors = self._validate_timestamp(symbol, timestamp)
            if ts_errors:
                errors.extend(ts_errors)
                error_types.extend([ValidationError.TIMESTAMP_INVALID] * len(ts_errors))
        
        # 5. 오더북 업데이트
        if len(errors) == 0:  # 검증 통과한 경우에만 업데이트
            self._update_orderbook_delta(symbol, bids, asks, timestamp, sequence, is_snapshot)
            
            # 업데이트 후 가격 역전 검증 (경고만 추가하고 오류로 처리하지 않음)
            if symbol in self.orderbooks_dict:
                sorted_bids = sorted(self.orderbooks_dict[symbol]["bids"].items(), key=lambda x: x[0], reverse=True)
                sorted_asks = sorted(self.orderbooks_dict[symbol]["asks"].items(), key=lambda x: x[0])
                
                if sorted_bids and sorted_asks:
                    max_bid = sorted_bids[0][0]
                    min_ask = sorted_asks[0][0]
                    if max_bid >= min_ask:
                        warnings.append(f"가격 역전: {max_bid} >= {min_ask}")
                        # 경고로만 처리하고 error_types에 추가하지 않음
            
            # 정렬된 오더북 생성 및 저장
            self._update_sorted_orderbook(symbol)
        
        # 6. 출력용 뎁스 제한 적용
        limited_bids = self._limit_depth(self.orderbooks[symbol]["bids"])
        limited_asks = self._limit_depth(self.orderbooks[symbol]["asks"])
                
        # 경고가 있어도 검증은 통과한 것으로 처리
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            error_types=error_types,
            warnings=warnings,
            limited_bids=limited_bids,
            limited_asks=limited_asks
        )
        
    def _validate_prices_and_sizes(self, bids: List[List[float]], 
                                 asks: List[List[float]], allow_zero_size: bool = False) -> List[str]:
        """
        가격과 수량의 유효성 검증
        
        Args:
            bids: 매수 호가 목록
            asks: 매도 호가 목록
            allow_zero_size: 0 크기 허용 여부 (델타 업데이트에서는 허용)
            
        Returns:
            List[str]: 오류 메시지 목록
        """
        errors = []
        
        # 매수 호가 검증
        for bid in bids:
            if len(bid) != 2:
                errors.append(f"잘못된 매수 호가 형식: {bid}")
                continue
                
            price, size = bid
            if price <= 0:
                errors.append(f"유효하지 않은 매수가: {price}")
            if size < 0 or (size == 0 and not allow_zero_size):
                errors.append(f"유효하지 않은 매수량: {size}")
                
        # 매도 호가 검증
        for ask in asks:
            if len(ask) != 2:
                errors.append(f"잘못된 매도 호가 형식: {ask}")
                continue
                
            price, size = ask
            if price <= 0:
                errors.append(f"유효하지 않은 매도가: {price}")
            if size < 0 or (size == 0 and not allow_zero_size):
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
        
        if symbol in self.sequences:
            last_seq = self.sequences[symbol]
            if sequence <= last_seq:
                errors.append(f"시퀀스 갭: last={last_seq}, current={sequence}")
                
        self.sequences[symbol] = sequence
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
        return orders[:self.output_depth]
        
    def _update_orderbook_delta(self, symbol: str, bids: List[List[float]], 
                               asks: List[List[float]], timestamp: Optional[int] = None,
                               sequence: Optional[int] = None, is_snapshot: bool = False) -> None:
        """
        오더북 데이터 업데이트 (델타 방식)
        
        Args:
            symbol: 심볼
            bids: 매수 호가 목록
            asks: 매도 호가 목록
            timestamp: 타임스탬프
            sequence: 시퀀스 번호
            is_snapshot: 스냅샷 여부
        """
        # 심볼별 오더북 데이터 초기화
        if symbol not in self.orderbooks_dict:
            self.orderbooks_dict[symbol] = {"bids": {}, "asks": {}}
            
        # 심볼별 정렬된 오더북 데이터 초기화
        if symbol not in self.orderbooks:
            self.orderbooks[symbol] = {
                "bids": [],
                "asks": [],
                "timestamp": None,
                "sequence": None
            }
        
        # 스냅샷인 경우 오더북 초기화
        if is_snapshot:
            self.orderbooks_dict[symbol]["bids"] = {float(bid[0]): float(bid[1]) for bid in bids}
            self.orderbooks_dict[symbol]["asks"] = {float(ask[0]): float(ask[1]) for ask in asks}
        # 델타인 경우 오더북 업데이트
        else:
            # 매수 호가 업데이트
            for bid in bids:
                price = float(bid[0])
                size = float(bid[1])
                if size == 0:  # 삭제
                    self.orderbooks_dict[symbol]["bids"].pop(price, None)
                else:  # 추가 또는 수정
                    self.orderbooks_dict[symbol]["bids"][price] = size
            
            # 매도 호가 업데이트
            for ask in asks:
                price = float(ask[0])
                size = float(ask[1])
                if size == 0:  # 삭제
                    self.orderbooks_dict[symbol]["asks"].pop(price, None)
                else:  # 추가 또는 수정
                    self.orderbooks_dict[symbol]["asks"][price] = size
        
        # 가격 역전 해결: 매수가가 매도가보다 높은 경우 충돌하는 주문 제거
        if symbol in self.orderbooks_dict and self.orderbooks_dict[symbol]["bids"] and self.orderbooks_dict[symbol]["asks"]:
            max_bid = max(self.orderbooks_dict[symbol]["bids"].keys())
            min_ask = min(self.orderbooks_dict[symbol]["asks"].keys())
            
            if max_bid >= min_ask:
                # 가격 역전 발생 - 충돌하는 주문 제거
                # 매수 주문 중 매도 최저가 이상인 주문 제거
                bids_to_remove = [p for p in self.orderbooks_dict[symbol]["bids"].keys() if p >= min_ask]
                for p in bids_to_remove:
                    self.orderbooks_dict[symbol]["bids"].pop(p, None)
                
                # 매도 주문 중 매수 최고가 이하인 주문 제거
                asks_to_remove = [p for p in self.orderbooks_dict[symbol]["asks"].keys() if p <= max_bid]
                for p in asks_to_remove:
                    self.orderbooks_dict[symbol]["asks"].pop(p, None)
        
        # 타임스탬프 및 시퀀스 업데이트
        self.orderbooks[symbol]["timestamp"] = timestamp
        self.orderbooks[symbol]["sequence"] = sequence
        
    def _update_sorted_orderbook(self, symbol: str) -> None:
        """
        정렬된 오더북 생성 및 저장
        
        Args:
            symbol: 심볼
        """
        if symbol not in self.orderbooks_dict:
            return
            
        # 정렬된 오더북 생성
        sorted_bids = sorted(self.orderbooks_dict[symbol]["bids"].items(), key=lambda x: x[0], reverse=True)
        sorted_asks = sorted(self.orderbooks_dict[symbol]["asks"].items(), key=lambda x: x[0])
        
        # 리스트 형태로 변환
        bids_list = [[price, size] for price, size in sorted_bids]
        asks_list = [[price, size] for price, size in sorted_asks]
        
        # 정렬된 오더북 저장
        self.orderbooks[symbol]["bids"] = bids_list
        self.orderbooks[symbol]["asks"] = asks_list
        
    def _update_orderbook(self, symbol: str, bids: List[List[float]], 
                         asks: List[List[float]], timestamp: Optional[int] = None,
                         sequence: Optional[int] = None) -> None:
        """
        오더북 데이터 업데이트 (기존 메서드, 호환성 유지)
        
        Args:
            symbol: 심볼
            bids: 매수 호가 목록
            asks: 매도 호가 목록
            timestamp: 타임스탬프
            sequence: 시퀀스 번호
        """
        # 델타 업데이트 메서드 호출 (스냅샷 모드)
        self._update_orderbook_delta(symbol, bids, asks, timestamp, sequence, is_snapshot=True)
        
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
            
        # 출력용 뎁스 제한 적용 (self.output_depth 사용)
        return {
            "bids": self.orderbooks[symbol]["bids"][:self.output_depth],
            "asks": self.orderbooks[symbol]["asks"][:self.output_depth],
            "timestamp": self.orderbooks[symbol]["timestamp"],
            "sequence": self.orderbooks[symbol]["sequence"]
        }
        
    def clear_symbol(self, symbol: str) -> None:
        """심볼 데이터 제거"""
        self.orderbooks.pop(symbol, None)
        self.orderbooks_dict.pop(symbol, None)
        
    def clear_all(self) -> None:
        """전체 데이터 제거"""
        self.orderbooks.clear()
        self.orderbooks_dict.clear() 