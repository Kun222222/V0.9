import asyncio
import json
import re
from src.crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator, ValidationResult, ValidationError

class BybitOrderBookValidator:
    """
    바이빗 전용 오더북 검증 클래스
    
    바이빗은 스냅샷과 델타 업데이트를 모두 제공합니다.
    - 스냅샷: 전체 오더북 데이터
    - 델타: 변경된 호가 데이터 (추가/수정/삭제)
    """
    
    def __init__(self, exchange_code: str = "bybit", depth: int = 10):
        self.exchange_code = exchange_code
        self.depth = depth
        self.orderbooks = {}  # symbol -> {"bids": {price_str: size}, "asks": {price_str: size}, ...}
        self.last_sequence = {}  # symbol -> sequence
        self.last_timestamp = {}  # symbol -> timestamp
    
    async def initialize_orderbook(self, symbol: str, data: dict) -> ValidationResult:
        """
        오더북 초기화 (스냅샷)
        """
        # 데이터 검증
        result = self._validate_orderbook(
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
            bids_dict = {}
            for bid in data.get("bids", []):
                price, size = bid
                bids_dict[str(price)] = float(size)
            
            asks_dict = {}
            for ask in data.get("asks", []):
                price, size = ask
                asks_dict[str(price)] = float(size)
            
            self.orderbooks[symbol].update({
                "bids": bids_dict,
                "asks": asks_dict,
                "timestamp": data.get("timestamp"),
                "sequence": data.get("sequence")
            })
            
            # 시퀀스/타임스탬프 업데이트
            if data.get("sequence") is not None:
                self.last_sequence[symbol] = data.get("sequence")
            if data.get("timestamp") is not None:
                self.last_timestamp[symbol] = data.get("timestamp")
            
            # 출력용 뎁스 제한 적용
            result.limited_bids = self._get_sorted_bids(symbol)[:self.depth]
            result.limited_asks = self._get_sorted_asks(symbol)[:self.depth]
        
        return result
    
    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """
        오더북 업데이트 (델타)
        """
        # 데이터 검증
        result = self._validate_orderbook(
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
            if data.get("timestamp") is not None:
                self.orderbooks[symbol]["timestamp"] = data.get("timestamp")
                self.last_timestamp[symbol] = data.get("timestamp")
            if data.get("sequence") is not None:
                self.orderbooks[symbol]["sequence"] = data.get("sequence")
                self.last_sequence[symbol] = data.get("sequence")
            
            # 매수 호가 업데이트
            for bid in data.get("bids", []):
                price, size = bid
                price_str = str(price)
                size_float = float(size)
                
                if size_float == 0:  # 수량이 0이면 삭제
                    self.orderbooks[symbol]["bids"].pop(price_str, None)
                else:  # 그 외에는 추가/수정
                    self.orderbooks[symbol]["bids"][price_str] = size_float
            
            # 매도 호가 업데이트
            for ask in data.get("asks", []):
                price, size = ask
                price_str = str(price)
                size_float = float(size)
                
                if size_float == 0:  # 수량이 0이면 삭제
                    self.orderbooks[symbol]["asks"].pop(price_str, None)
                else:  # 그 외에는 추가/수정
                    self.orderbooks[symbol]["asks"][price_str] = size_float
            
            # 출력용 뎁스 제한 적용
            result.limited_bids = self._get_sorted_bids(symbol)[:self.depth]
            result.limited_asks = self._get_sorted_asks(symbol)[:self.depth]
        
        return result
    
    def _validate_orderbook(self, symbol: str, bids, asks, timestamp=None, sequence=None) -> ValidationResult:
        """
        오더북 데이터 검증
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
            max_bid = max(float(bid[0]) for bid in bids)
            min_ask = min(float(ask[0]) for ask in asks)
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
        
        # 5. 시퀀스 검증
        if sequence is not None:
            seq_errors = self._validate_sequence(symbol, sequence)
            if seq_errors:
                errors.extend(seq_errors)
                error_types.extend([ValidationError.SEQUENCE_GAP] * len(seq_errors))
        
        # 6. 타임스탬프 검증
        if timestamp is not None:
            ts_errors = self._validate_timestamp(symbol, timestamp)
            if ts_errors:
                errors.extend(ts_errors)
                error_types.extend([ValidationError.TIMESTAMP_INVALID] * len(ts_errors))
        
        return ValidationResult(len(errors) == 0, errors, error_types)
    
    def _validate_prices_and_sizes(self, bids, asks) -> list:
        """가격과 수량의 유효성 검증"""
        errors = []
        
        # 매수 호가 검증
        for bid in bids:
            if len(bid) != 2:
                errors.append(f"잘못된 매수 호가 형식: {bid}")
                continue
            
            price, size = bid
            try:
                price_float = float(price)
                size_float = float(size)
                
                if price_float <= 0:
                    errors.append(f"유효하지 않은 매수가: {price}")
                if size_float <= 0:
                    errors.append(f"유효하지 않은 매수량: {size}")
            except (ValueError, TypeError):
                errors.append(f"유효하지 않은 매수 호가 데이터: {bid}")
        
        # 매도 호가 검증
        for ask in asks:
            if len(ask) != 2:
                errors.append(f"잘못된 매도 호가 형식: {ask}")
                continue
            
            price, size = ask
            try:
                price_float = float(price)
                size_float = float(size)
                
                if price_float <= 0:
                    errors.append(f"유효하지 않은 매도가: {price}")
                if size_float <= 0:
                    errors.append(f"유효하지 않은 매도량: {size}")
            except (ValueError, TypeError):
                errors.append(f"유효하지 않은 매도 호가 데이터: {ask}")
        
        return errors
    
    def _validate_order(self, orders, reverse: bool) -> bool:
        """호가 순서 검증"""
        if not orders:
            return True
        
        try:
            prices = [float(order[0]) for order in orders]
            return prices == sorted(prices, reverse=reverse)
        except (ValueError, TypeError):
            return False
    
    def _validate_sequence(self, symbol: str, sequence: int) -> list:
        """시퀀스 검증"""
        errors = []
        
        if symbol in self.last_sequence:
            last_seq = self.last_sequence[symbol]
            if sequence <= last_seq:
                errors.append(f"시퀀스 갭: last={last_seq}, current={sequence}")
        
        return errors
    
    def _validate_timestamp(self, symbol: str, timestamp: int) -> list:
        """타임스탬프 검증"""
        errors = []
        
        if symbol in self.last_timestamp:
            last_ts = self.last_timestamp[symbol]
            if timestamp < last_ts:
                errors.append(f"타임스탬프 역전: last={last_ts}, current={timestamp}")
        
        return errors
    
    def _get_sorted_bids(self, symbol: str) -> list:
        """매수 호가 정렬 (가격 내림차순)"""
        if symbol not in self.orderbooks:
            return []
        
        bids_dict = self.orderbooks[symbol]["bids"]
        return [[float(price), size] for price, size in sorted(bids_dict.items(), key=lambda x: float(x[0]), reverse=True)]
    
    def _get_sorted_asks(self, symbol: str) -> list:
        """매도 호가 정렬 (가격 오름차순)"""
        if symbol not in self.orderbooks:
            return []
        
        asks_dict = self.orderbooks[symbol]["asks"]
        return [[float(price), size] for price, size in sorted(asks_dict.items(), key=lambda x: float(x[0]))]
    
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

def parse_bybit_log(log_line):
    """로그 라인에서 바이빗 오더북 데이터 파싱"""
    try:
        # JSON 데이터 추출
        json_match = re.search(r'\{.*\}', log_line)
        if not json_match:
            return None
        
        json_data = json.loads(json_match.group(0))
        
        # 오더북 데이터인지 확인
        if "topic" not in json_data or not json_data["topic"].startswith("orderbook.50."):
            return None
        
        # 심볼 추출
        symbol = json_data["topic"].split(".")[-1].replace("USDT", "")
        
        # 타입 확인 (스냅샷 또는 델타)
        data_type = json_data.get("type")
        if data_type not in ["snapshot", "delta"]:
            return None
        
        # 데이터 추출
        orderbook_data = json_data.get("data", {})
        if not orderbook_data:
            return None
        
        # 결과 데이터 구성
        result = {
            "exchangename": "bybit",
            "symbol": symbol,
            "bids": [[price, size] for price, size in orderbook_data.get("b", [])],
            "asks": [[price, size] for price, size in orderbook_data.get("a", [])],
            "timestamp": json_data.get("ts"),
            "sequence": orderbook_data.get("seq"),
            "type": data_type
        }
        
        return result
    except Exception as e:
        print(f"파싱 오류: {e}")
        return None

async def test_with_log_file(log_file_path):
    """로그 파일을 사용하여 바이빗 오더북 검증기 테스트"""
    # 바이빗 검증기 생성
    validator = BybitOrderBookValidator()
    
    # 심볼별 처리 결과 통계
    stats = {}
    
    # 로그 파일 읽기
    with open(log_file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            # 로그 라인 파싱
            data = parse_bybit_log(line)
            if not data:
                continue
            
            symbol = data["symbol"]
            data_type = data.pop("type", None)
            
            # 심볼별 통계 초기화
            if symbol not in stats:
                stats[symbol] = {
                    "snapshot_count": 0,
                    "delta_count": 0,
                    "valid_count": 0,
                    "invalid_count": 0,
                    "errors": []
                }
            
            # 데이터 타입에 따라 처리
            if data_type == "snapshot":
                stats[symbol]["snapshot_count"] += 1
                result = await validator.initialize_orderbook(symbol, data)
            else:  # delta
                stats[symbol]["delta_count"] += 1
                result = await validator.update(symbol, data)
            
            # 검증 결과 통계
            if result.is_valid:
                stats[symbol]["valid_count"] += 1
            else:
                stats[symbol]["invalid_count"] += 1
                stats[symbol]["errors"].extend(result.errors)
            
            # 진행 상황 출력 (100개마다)
            if line_num % 100 == 0:
                print(f"처리 중... {line_num}줄")
    
    # 결과 출력
    print("\n===== 검증 결과 =====")
    for symbol, stat in stats.items():
        print(f"\n{symbol} 심볼:")
        print(f"  스냅샷 처리: {stat['snapshot_count']}개")
        print(f"  델타 처리: {stat['delta_count']}개")
        print(f"  유효 데이터: {stat['valid_count']}개")
        print(f"  무효 데이터: {stat['invalid_count']}개")
        
        if stat["invalid_count"] > 0:
            print(f"  오류 유형:")
            error_counts = {}
            for error in stat["errors"]:
                error_counts[error] = error_counts.get(error, 0) + 1
            
            for error, count in error_counts.items():
                print(f"    - {error}: {count}개")
        
        # 현재 오더북 상태 출력
        ob = validator.get_orderbook(symbol)
        if ob:
            print(f"  현재 매수 호가 (상위 5개): {ob['bids'][:5]}")
            print(f"  현재 매도 호가 (상위 5개): {ob['asks'][:5]}")

# 테스트 실행
if __name__ == "__main__":
    log_file_path = "/Users/kun/Desktop/CrossKimpArbitrage/v0.6/logs/raw_data/bybit/250318_001251_bybit_raw_logger.log"
    asyncio.run(test_with_log_file(log_file_path)) 