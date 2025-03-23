"""
오더북 검증 모듈

이 모듈은 거래소별 오더북 데이터의 유효성을 검증하는 클래스를 제공합니다.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from enum import Enum
import time

from crosskimp.common.logger.logger import get_unified_logger

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
        
        # 검증 설정
        self.max_depth = 20  # 최대 깊이
        self.output_depth = 10  # 출력용 최대 깊이
        self.price_precision = 8  # 가격 정밀도
        self.size_precision = 8  # 수량 정밀도
        
        # 검증 결과 저장
        self.validation_errors = {}  # symbol -> list of errors
        self.last_sequence = {}  # symbol -> sequence
        self.last_timestamp = {}  # symbol -> timestamp
        self.orderbooks_dict = {}  # symbol -> {"bids": {price: size}, "asks": {price: size}}
        
        # 데이터 관리
        self.last_update_time = {}  # 각 심볼별 마지막 업데이트 시간 (시스템 시간)
        self.max_data_age_seconds = 60  # 데이터 최대 유지 시간 (초)
        self.removed_symbols = set()  # 이미 제거된 심볼 추적

    async def initialize_orderbook(self, symbol: str, data: Dict) -> ValidationResult:
        """
        오더북 초기화 (스냅샷)
        
        Args:
            symbol: 심볼
            data: 오더북 데이터
            
        Returns:
            ValidationResult: 검증 결과
        """
        # 오래된 데이터 정리
        self._cleanup_old_data()
        
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
        # 오래된 데이터 정리
        self._cleanup_old_data()
        
        # 델타 업데이트 데이터 검증
        return self.validate_orderbook(
            symbol=symbol,
            bids=data.get("bids", []),
            asks=data.get("asks", []),
            timestamp=data.get("timestamp"),
            sequence=data.get("sequence"),
            is_snapshot=False
        )

    def _cleanup_old_data(self) -> None:
        """
        오래된 오더북 데이터 정리
        
        60초 이상 경과된 데이터를 메모리에서 제거합니다.
        """
        current_time = time.time()
        symbols_to_remove = []
        
        # 마지막 업데이트 시간 기준으로 오래된 데이터 식별
        for symbol, last_update in self.last_update_time.items():
            if (current_time - last_update) > self.max_data_age_seconds:
                symbols_to_remove.append(symbol)
        
        # 식별된 오래된 데이터 제거
        for symbol in symbols_to_remove:
            # 이미 로그를 출력한 심볼인지 확인
            if symbol not in self.removed_symbols:
                self.clear_symbol(symbol)
                self.logger.debug(f"[{self.exchange_code}] 오래된 데이터 제거: {symbol} (마지막 업데이트: {self.max_data_age_seconds}초 이상 경과)")
                self.removed_symbols.add(symbol)
            else:
                # 로그 없이 제거만 수행
                self.clear_symbol(symbol)

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
        try:
            # 현재 시간 기록 (데이터 정리용)
            self.last_update_time[symbol] = time.time()
            
            # 심볼별 오더북 데이터 초기화
            if symbol not in self.orderbooks_dict:
                self.orderbooks_dict[symbol] = {
                    "bids": {},
                    "asks": {}
                }
                
            # 스냅샷인 경우 기존 데이터 초기화
            if is_snapshot:
                self.orderbooks_dict[symbol]["bids"] = {}
                self.orderbooks_dict[symbol]["asks"] = {}
                
            # 매수 호가 업데이트
            for bid in bids:
                if len(bid) < 2:
                    continue
                    
                try:
                    price = float(bid[0])
                    size = float(bid[1])
                    
                    if size > 0:
                        self.orderbooks_dict[symbol]["bids"][price] = size
                    else:
                        # 수량이 0이면 해당 가격 삭제
                        self.orderbooks_dict[symbol]["bids"].pop(price, None)
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"[{self.exchange_code}] {symbol} 매수 호가 변환 오류: {e}, 데이터: {bid}")
                    continue
                    
            # 매도 호가 업데이트
            for ask in asks:
                if len(ask) < 2:
                    continue
                    
                try:
                    price = float(ask[0])
                    size = float(ask[1])
                    
                    if size > 0:
                        self.orderbooks_dict[symbol]["asks"][price] = size
                    else:
                        # 수량이 0이면 해당 가격 삭제
                        self.orderbooks_dict[symbol]["asks"].pop(price, None)
                except (ValueError, TypeError) as e:
                    self.logger.warning(f"[{self.exchange_code}] {symbol} 매도 호가 변환 오류: {e}, 데이터: {ask}")
                    continue
            
            # 오더북 정리 후 정렬
            self._update_sorted_orderbook(symbol)
            
            # 오더북 객체 업데이트
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = {}
                
            # 최종 오더북에 저장
            if symbol in self.orderbooks:
                self.orderbooks[symbol]["timestamp"] = timestamp
                self.orderbooks[symbol]["sequence"] = sequence
                
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] {symbol} 오더북 업데이트 중 오류 발생: {e}")
            # 오류 발생 시 빈 오더북 생성
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = {"bids": [], "asks": [], "timestamp": timestamp, "sequence": sequence}
        
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
        
    def _update_sorted_orderbook(self, symbol: str) -> None:
        """
        정렬된 오더북 생성 및 저장
        
        Args:
            symbol: 심볼
        """
        try:
            if symbol not in self.orderbooks_dict:
                return
                
            # 정렬된 오더북 생성
            sorted_bids = sorted(self.orderbooks_dict[symbol]["bids"].items(), key=lambda x: float(x[0]), reverse=True)
            sorted_asks = sorted(self.orderbooks_dict[symbol]["asks"].items(), key=lambda x: float(x[0]))
            
            # 리스트 형태로 변환
            bids_list = [[float(price), float(size)] for price, size in sorted_bids]
            asks_list = [[float(price), float(size)] for price, size in sorted_asks]
            
            # 정렬된 오더북 저장
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = {}
                
            self.orderbooks[symbol]["bids"] = bids_list
            self.orderbooks[symbol]["asks"] = asks_list
            
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] {symbol} 오더북 정렬 중 오류 발생: {str(e)}")
            # 오류 발생 시 빈 리스트로 초기화
            if symbol in self.orderbooks:
                self.orderbooks[symbol]["bids"] = []
                self.orderbooks[symbol]["asks"] = []
        
    def get_orderbook(self, symbol: str) -> Optional[Dict]:
        """
        심볼의 오더북 데이터 반환
        
        Args:
            symbol: 심볼
            
        Returns:
            Optional[Dict]: 오더북 데이터 (없으면 None)
        """
        try:
            if symbol not in self.orderbooks:
                return None
                
            # 출력용 뎁스 제한 적용
            depth = min(self.output_depth, len(self.orderbooks[symbol].get("bids", [])))
            bids = self.orderbooks[symbol].get("bids", [])[:depth] if "bids" in self.orderbooks[symbol] else []
            
            depth = min(self.output_depth, len(self.orderbooks[symbol].get("asks", [])))
            asks = self.orderbooks[symbol].get("asks", [])[:depth] if "asks" in self.orderbooks[symbol] else []
            
            return {
                "bids": bids,
                "asks": asks,
                "timestamp": self.orderbooks[symbol].get("timestamp"),
                "sequence": self.orderbooks[symbol].get("sequence")
            }
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] {symbol} 오더북 조회 중 오류 발생: {e}")
            # 오류 발생 시 빈 오더북 반환
            return {
                "bids": [],
                "asks": [],
                "timestamp": int(time.time() * 1000),
                "sequence": 0
            }
        
    def clear_symbol(self, symbol: str) -> None:
        """심볼 데이터 제거"""
        self.orderbooks.pop(symbol, None)
        self.orderbooks_dict.pop(symbol, None)
        
    def clear_all(self) -> None:
        """전체 데이터 제거"""
        self.orderbooks.clear()
        self.orderbooks_dict.clear()
        self.removed_symbols.clear()  # 제거된 심볼 목록 초기화 