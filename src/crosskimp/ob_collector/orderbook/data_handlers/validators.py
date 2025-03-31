"""
오더북 유효성 검증 모듈

이 모듈은 오더북 데이터의 유효성을 검증하는 클래스와 함수를 제공합니다.
"""

import time
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
from dataclasses import dataclass, field

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import Exchange, EXCHANGE_NAMES_KR, SystemComponent
from crosskimp.ob_collector.orderbook.data_handlers.ob_data_manager import get_orderbook_data_manager

# 로거 설정
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

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
        
        # 오더북 데이터 관리자에서 설정값 가져오기
        self.data_manager = get_orderbook_data_manager()
        
        # 검증 설정
        self.output_depth = self.data_manager.get_orderbook_output_depth()  # 오더북 최대 깊이
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
        오더북 델타 업데이트
        
        Args:
            symbol: 심볼
            bids: 매수 호가 목록 [[price, size], ...]
            asks: 매도 호가 목록 [[price, size], ...]
            timestamp: 타임스탬프
            sequence: 시퀀스 번호
            is_snapshot: 스냅샷 여부
        """
        try:
            # 오더북이 없으면 생성
            if symbol not in self.orderbooks_dict:
                self.orderbooks_dict[symbol] = {
                    "bids": {},
                    "asks": {},
                    "timestamp": timestamp or 0,
                    "sequence": sequence or 0
                }
            
            # 스냅샷인 경우 기존 데이터 교체
            if is_snapshot:
                # 기존 데이터 초기화
                self.orderbooks_dict[symbol]["bids"].clear()
                self.orderbooks_dict[symbol]["asks"].clear()
                
                # 새 데이터 추가
                for bid in bids:
                    if len(bid) >= 2 and bid[1] > 0:  # 수량이 0보다 큰 경우만 추가
                        self.orderbooks_dict[symbol]["bids"][str(bid[0])] = bid[1]
                        
                for ask in asks:
                    if len(ask) >= 2 and ask[1] > 0:  # 수량이 0보다 큰 경우만 추가
                        self.orderbooks_dict[symbol]["asks"][str(ask[0])] = ask[1]
            else:
                # 델타 업데이트
                # 새로운 호가 데이터 (신규 매수/매도 가격들)
                new_bid_prices = set()
                new_ask_prices = set()
                
                # 매수 호가 업데이트
                for bid in bids:
                    if len(bid) >= 2:
                        price, size = str(bid[0]), bid[1]
                        if size > 0:
                            self.orderbooks_dict[symbol]["bids"][price] = size
                            new_bid_prices.add(float(price))
                        else:
                            # 수량이 0이면 해당 가격의 호가 삭제
                            self.orderbooks_dict[symbol]["bids"].pop(price, None)
                
                # 매도 호가 업데이트
                for ask in asks:
                    if len(ask) >= 2:
                        price, size = str(ask[0]), ask[1]
                        if size > 0:
                            self.orderbooks_dict[symbol]["asks"][price] = size
                            new_ask_prices.add(float(price))
                        else:
                            # 수량이 0이면 해당 가격의 호가 삭제
                            self.orderbooks_dict[symbol]["asks"].pop(price, None)
            
            # 가격 역전 문제 검증 및 수정
            # 공통 로직: 가격 역전이 있는지 확인하고 있으면 수정
            if self.orderbooks_dict[symbol]["bids"] and self.orderbooks_dict[symbol]["asks"]:
                bid_prices = [float(p) for p in self.orderbooks_dict[symbol]["bids"].keys()]
                ask_prices = [float(p) for p in self.orderbooks_dict[symbol]["asks"].keys()]
                
                if bid_prices and ask_prices:
                    max_bid = max(bid_prices)
                    min_ask = min(ask_prices)
                    
                    # 가격 역전 확인 (최고 매수가 >= 최저 매도가)
                    if max_bid >= min_ask:
                        # 가격 역전이 감지됨 - 새로운 호가를 우선시하고 기존 호가 중 충돌하는 것 제거
                        if not is_snapshot:  # 델타 업데이트인 경우만 처리
                            # 새 매수가가 있는 경우와 없는 경우 구분
                            if new_bid_prices:
                                # 새 매수가가 있으면, 해당 매수가들보다 작거나 같은 매도가 제거
                                for new_bid in new_bid_prices:
                                    for p in list(self.orderbooks_dict[symbol]["asks"].keys()):
                                        if float(p) <= new_bid:
                                            # self.logger.debug(f"[{self.exchange_code}] {symbol} 매수가 {new_bid}로 인한 매도가 {p} 제거")
                                            self.orderbooks_dict[symbol]["asks"].pop(p, None)
                            
                            # 새 매도가가 있는 경우와 없는 경우 구분
                            if new_ask_prices:
                                # 새 매도가가 있으면, 해당 매도가들보다 크거나 같은 매수가 제거
                                for new_ask in new_ask_prices:
                                    for p in list(self.orderbooks_dict[symbol]["bids"].keys()):
                                        if float(p) >= new_ask:
                                            # self.logger.debug(f"[{self.exchange_code}] {symbol} 매도가 {new_ask}로 인한 매수가 {p} 제거")
                                            self.orderbooks_dict[symbol]["bids"].pop(p, None)
                        else:
                            # 스냅샷의 경우 기존 로직 유지
                            self.logger.warning(f"[{self.exchange_code}] {symbol} 스냅샷 가격 역전 감지: {max_bid} >= {min_ask}")
                            
                            # 가격 역전 해소 - 모든 역전된 가격 제거
                            # 1. 역전된 매수 호가 제거
                            for p in list(self.orderbooks_dict[symbol]["bids"].keys()):
                                if float(p) >= min_ask:
                                    self.orderbooks_dict[symbol]["bids"].pop(p, None)
                            
                            # 2. 역전된 매도 호가 제거
                            for p in list(self.orderbooks_dict[symbol]["asks"].keys()):
                                if float(p) <= max_bid:
                                    self.orderbooks_dict[symbol]["asks"].pop(p, None)
            
            # 타임스탬프 및 시퀀스 업데이트
            if timestamp is not None:
                self.orderbooks_dict[symbol]["timestamp"] = timestamp
                
            if sequence is not None:
                self.orderbooks_dict[symbol]["sequence"] = sequence
                
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
                
        # 중요: 검증 결과와 상관없이 시퀀스와 타임스탬프 정보 저장
        if sequence is not None:
            self.sequences[symbol] = sequence
        if timestamp is not None:
            self.last_timestamp[symbol] = timestamp
        
        # 5. 오더북 업데이트 수행
        if len(errors) == 0:  # 검증 통과한 경우에만 업데이트
            self._update_orderbook_delta(symbol, bids, asks, timestamp, sequence, is_snapshot)
            
            # 업데이트 후 가격 역전 검증 (심각한 역전만 로그로 기록)
            if symbol in self.orderbooks_dict:
                sorted_bids = sorted(self.orderbooks_dict[symbol]["bids"].items(), key=lambda x: float(x[0]), reverse=True)
                sorted_asks = sorted(self.orderbooks_dict[symbol]["asks"].items(), key=lambda x: float(x[0]))
                
                if sorted_bids and sorted_asks:
                    max_bid = float(sorted_bids[0][0])
                    min_ask = float(sorted_asks[0][0])
                    
                    # 가격 역전이 여전히 있는지 확인 (정상적으로는 없어야 함)
                    if max_bid >= min_ask:
                        # 여전히 역전이 있으면 로그만 남기고 계속 진행
                        warnings.append(f"오더북 업데이트 후에도 가격 역전 감지됨: {max_bid} >= {min_ask}")
            
            # 정렬된 오더북 생성 및 저장
            self._update_sorted_orderbook(symbol)
            
            # 현재 시간 기록 (오래된 데이터 정리용)
            self.last_update_time[symbol] = time.time()
            
            # 중앙 로깅 시스템에 오더북 데이터 로깅 (최신 오더북)
            orderbook = self.get_orderbook(symbol)
            if orderbook and self.data_manager:
                self.data_manager.log_orderbook_data(
                    self.exchange_code,
                    symbol,
                    orderbook
                )
        
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
            
            # 가격 역전 최종 확인 (일반적으로 이 단계에서는 발생하지 않아야 함)
            if sorted_bids and sorted_asks:
                highest_bid = float(sorted_bids[0][0])
                lowest_ask = float(sorted_asks[0][0])
                
                if highest_bid >= lowest_ask:
                    # 가격 역전 감지 - 로그 기록
                    self.logger.warning(f"[{self.exchange_code}] {symbol} 최종 정렬 단계 가격 역전 감지: {highest_bid} >= {lowest_ask}")
                    
                    # 심각한 경우만 최소한으로 수정 - 기존 가격 구조를 최대한 보존
                    if highest_bid > lowest_ask:
                        # 명확한 역전은 모두 제거 
                        filtered_bids = [(price, size) for price, size in sorted_bids if float(price) < lowest_ask]
                        filtered_asks = [(price, size) for price, size in sorted_asks if float(price) > highest_bid]
                        
                        # 필터링 결과가 비어있지 않은지 확인
                        if filtered_bids or len(sorted_bids) <= 1:
                            sorted_bids = filtered_bids
                        else:
                            # 적어도 가장 큰 매수가는 제거
                            sorted_bids = sorted_bids[1:]
                        
                        if filtered_asks or len(sorted_asks) <= 1:
                            sorted_asks = filtered_asks
                        else:
                            # 적어도 가장 작은 매도가는 제거
                            sorted_asks = sorted_asks[1:]
                    else:
                        # 같은 가격인 경우 매수 쪽을 1개 제거
                        if len(sorted_bids) > 1:
                            sorted_bids = sorted_bids[1:]  # 최고가만 제거
            
            # 리스트 형태로 변환
            bids_list = [[float(price), float(size)] for price, size in sorted_bids]
            asks_list = [[float(price), float(size)] for price, size in sorted_asks]
            
            # 정렬된 오더북 저장
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = {}
                
            self.orderbooks[symbol]["bids"] = bids_list
            self.orderbooks[symbol]["asks"] = asks_list
            
            # 시퀀스와 타임스탬프 정보 저장 (추가된 부분)
            # 시퀀스 정보 - sequences 딕셔너리에서 가져옴
            if symbol in self.sequences:
                self.orderbooks[symbol]["sequence"] = self.sequences[symbol]
            else:
                self.orderbooks[symbol]["sequence"] = 0
                
            # 타임스탬프 정보 - last_timestamp 딕셔너리에서 가져옴
            if symbol in self.last_timestamp:
                self.orderbooks[symbol]["timestamp"] = self.last_timestamp[symbol]
            else:
                self.orderbooks[symbol]["timestamp"] = int(time.time() * 1000)
            
            # orderbooks_dict의 시퀀스, 타임스탬프 정보도 업데이트
            if "sequence" in self.orderbooks[symbol]:
                self.orderbooks_dict[symbol]["sequence"] = self.orderbooks[symbol]["sequence"]
            if "timestamp" in self.orderbooks[symbol]:
                self.orderbooks_dict[symbol]["timestamp"] = self.orderbooks[symbol]["timestamp"]
            
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] {symbol} 오더북 정렬 중 오류 발생: {str(e)}")
        
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