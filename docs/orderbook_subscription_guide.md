# 오더북 구독 및 검증 코드 작성 가이드

## 1. 구독 클래스 기본 구조

### 1.1 상수 정의
```python
# 거래소 정보
EXCHANGE_CODE = "거래소코드"  # 예: "upbit"

# API 설정
REST_URL = "https://api.example.com/v1/orderbook"  # REST API URL
WS_URL = "wss://api.example.com/websocket/v1"      # 웹소켓 URL
```

### 1.2 클래스 구조
```python
class ExchangeSubscription(BaseSubscription):
    """
    거래소 구독 클래스
    
    거래소의 웹소켓 구독을 담당하는 클래스입니다.
    
    책임:
    - 구독 관리 (구독, 구독 취소)
    - 메시지 처리 및 파싱
    - 콜백 호출
    - 원시 데이터 로깅
    """
    
    def __init__(self, connection: BaseWebsocketConnector, parser=None):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
            parser: 메시지 파싱 객체 (선택적)
        """
        super().__init__(connection, EXCHANGE_CODE, parser)
        
        # 로깅 설정
        self.log_raw_data = True
        self.raw_logger = None
        self._setup_raw_logging()
        
        # 거래소 전용 검증기 초기화
        self.validator = ExchangeOrderBookValidator(EXCHANGE_CODE)
        
        # 오더북 데이터 저장소
        self.orderbooks = {}  # symbol -> orderbook_data
```

## 2. 필수 구현 메서드

### 2.1 스냅샷/델타 메소드 정의
```python
def _get_snapshot_method(self) -> SnapshotMethod:
    """
    스냅샷 수신 방법 반환
    
    Returns:
        SnapshotMethod: WEBSOCKET 또는 REST
    """
    return SnapshotMethod.WEBSOCKET  # 또는 REST

def _get_delta_method(self) -> DeltaMethod:
    """
    델타 수신 방법 반환
    
    Returns:
        DeltaMethod: WEBSOCKET 또는 NONE
    """
    return DeltaMethod.WEBSOCKET  # 또는 NONE
```

### 2.2 구독 메시지 생성
```python
async def create_subscribe_message(self, symbol: Union[str, List[str]]) -> Dict:
    """
    구독 메시지 생성
    
    Args:
        symbol: 구독할 심볼 또는 심볼 리스트
        
    Returns:
        Dict: 구독 메시지
    """
    # 심볼이 리스트인지 확인하고 처리
    if isinstance(symbol, list):
        symbols = symbol
    else:
        symbols = [symbol]
        
    # 심볼 형식 변환 (거래소별 형식에 맞게)
    market_codes = [f"KRW-{s.upper()}" for s in symbols]
    
    # 구독 메시지 생성
    message = [
        {"ticket": f"orderbook_{int(time.time())}"},
        {
            "type": "orderbook",
            "codes": market_codes,
            "is_only_realtime": False
        },
        {"format": "DEFAULT"}
    ]
    
    return message
```

### 2.3 메시지 타입 확인
```python
def is_snapshot_message(self, message: str) -> bool:
    """
    메시지가 스냅샷인지 확인
    
    Args:
        message: 수신된 메시지
        
    Returns:
        bool: 스냅샷 메시지인 경우 True
    """
    try:
        if isinstance(message, bytes):
            message = message.decode('utf-8')
            
        if isinstance(message, str):
            data = json.loads(message)
        else:
            data = message
            
        return data.get("type") == "orderbook"
        
    except Exception as e:
        self.logger.error(f"스냅샷 메시지 확인 중 오류: {e}")
        return False
```

### 2.4 메시지 처리
```python
async def _on_message(self, message: str) -> None:
    """
    메시지 수신 처리
    
    Args:
        message: 수신된 원시 메시지
    """
    try:
        # 원본 메시지 로깅
        self.log_raw_message(message)
        
        # 메트릭 업데이트
        self.metrics_manager.record_message(self.exchange_code)
        
        # 메시지 크기 기록
        if isinstance(message, str):
            message_size = len(message.encode('utf-8'))
        elif isinstance(message, bytes):
            message_size = len(message)
        else:
            message_size = 0
        self.metrics_manager.record_bytes(self.exchange_code, message_size)
        
        # 메시지 파싱
        parsed_data = None
        if self.parser:
            parsed_data = self.parser.parse_message(message)
            
        if not parsed_data:
            return
            
        symbol = parsed_data.get("symbol")
        if not symbol or symbol not in self.subscribed_symbols:
            return
            
        # 검증
        validation = self.validator.validate_orderbook(
            symbol=symbol,
            bids=parsed_data["bids"],
            asks=parsed_data["asks"],
            timestamp=parsed_data.get("timestamp"),
            sequence=parsed_data.get("sequence")
        )
        
        if not validation.is_valid:
            self.logger.warning(f"{symbol} 검증 실패: {validation.errors}")
            return
            
        # 오더북 업데이트
        self.orderbooks[symbol] = {
            "bids": parsed_data["bids"][:10],
            "asks": parsed_data["asks"][:10],
            "timestamp": parsed_data["timestamp"],
            "sequence": parsed_data["sequence"]
        }
        
        # 콜백 호출
        if symbol in self.snapshot_callbacks:
            await self.snapshot_callbacks[symbol](symbol, self.orderbooks[symbol])
            
    except Exception as e:
        self.logger.error(f"메시지 처리 실패: {str(e)}")
        self.metrics_manager.record_error(self.exchange_code)
```

## 3. 검증 클래스 구현

### 3.1 기본 검증 클래스
```python
class BaseOrderBookValidator:
    """
    기본 오더북 검증 클래스
    
    거래소별 오더북 데이터의 유효성을 검증합니다.
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
```

### 3.2 검증 메서드
```python
def validate_orderbook(self, symbol: str, bids: List[List[float]], 
                      asks: List[List[float]], timestamp: Optional[int] = None,
                      sequence: Optional[int] = None) -> ValidationResult:
    """
    오더북 데이터 검증
    
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
    if len(errors) == 0:
        self._update_orderbook(symbol, bids, asks, timestamp, sequence)
    
    # 7. 출력용 뎁스 제한 적용
    limited_bids = self._limit_depth(self.orderbooks[symbol]["bids"])
    limited_asks = self._limit_depth(self.orderbooks[symbol]["asks"])
            
    return ValidationResult(len(errors) == 0, errors, error_types, limited_bids, limited_asks)
```

## 4. 주의사항

1. **구독 관련**
   - 모든 예외 처리는 구체적으로 해야 합니다.
   - 메시지 수신 시 메트릭을 업데이트해야 합니다.
   - 중요 이벤트는 로깅해야 합니다.
   - 구독 상태를 정확하게 관리해야 합니다.

2. **검증 관련**
   - 가격 역전이 발생하지 않아야 합니다.
   - 호가 순서가 올바르게 유지되어야 합니다.
   - 시퀀스/타임스탬프가 순차적으로 증가해야 합니다.
   - 0인 값은 필터링해야 합니다.

3. **데이터 형식**
   - 오더북 데이터는 표준 형식을 따르야 합니다:
     ```python
     {
         "exchangename": "거래소코드",
         "symbol": "심볼",
         "bids": [[가격, 수량], ...],
         "asks": [[가격, 수량], ...],
         "timestamp": 타임스탬프,
         "sequence": 시퀀스번호,
         "type": "snapshot" 또는 "delta"
     }
     ```
   - 가격과 수량은 float 타입이어야 합니다.
   - 타임스탬프는 밀리초 단위여야 합니다.

4. **성능 관련**
   - 메시지 처리 시간을 모니터링해야 합니다.
   - 메모리 사용량을 관리해야 합니다.
   - 불필요한 데이터 복사를 피해야 합니다. 