# 오더북 파서 코드 작성 가이드

## 1. 기본 구조

### 1.1 상수 정의
```python
# 거래소 정보
EXCHANGE_CODE = Exchange.거래소코드.value  # 예: Exchange.UPBIT.value
거래소_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 거래소 설정
```

### 1.2 클래스 구조
```python
class ExchangeParser(BaseParser):
    """
    거래소 파서 클래스
    
    거래소의 웹소켓 메시지를 파싱하는 클래스입니다.
    
    특징:
    - 메시지 형식: 전체 오더북 스냅샷 또는 델타 업데이트
    """
    def __init__(self):
        """
        거래소 파서 초기화
        """
        super().__init__(EXCHANGE_CODE)
```

## 2. 필수 구현 메서드

### 2.1 parse_message() - 메시지 파싱
```python
def parse_message(self, raw_message: str) -> Optional[Dict[str, Any]]:
    """
    수신된 메시지 파싱
    
    Args:
        raw_message: 수신된 웹소켓 메시지
        
    Returns:
        Optional[Dict[str, Any]]: 파싱된 오더북 데이터 또는 None
    """
    try:
        # 바이트 메시지 디코딩
        if isinstance(raw_message, bytes):
            raw_message = raw_message.decode("utf-8")
        
        # JSON 파싱
        data = json.loads(raw_message)
        
        # 오더북 메시지 타입 확인
        if data.get("type") != "orderbook":
            return None

        # 심볼 추출
        symbol = data.get("code", "").replace("KRW-","")
        if not symbol:
            self.log_error("심볼 정보 누락")
            return None

        # 타임스탬프 추출
        ts = data.get("timestamp", int(time.time()*1000))
        
        # 오더북 데이터 추출 및 변환
        bids_dict, asks_dict = self._extract_orderbook_data(data.get("orderbook_units", []), symbol)

        # 유효성 검사
        if not bids_dict or not asks_dict:
            return None
            
        # 딕셔너리 형식에서 리스트 형식으로 변환
        bids = [[item['price'], item['size']] for item in bids_dict]
        asks = [[item['price'], item['size']] for item in asks_dict]
        
        # 표준화된 오더북 데이터 반환
        return {
            "exchangename": EXCHANGE_CODE,
            "symbol": symbol.upper(),
            "bids": bids,
            "asks": asks,
            "timestamp": ts,
            "sequence": ts,
            "type": "snapshot"  # 또는 "delta"
        }
    except json.JSONDecodeError:
        self.log_error(f"JSON 파싱 실패: {raw_message[:100]}...")
        return None
    except Exception as e:
        self.log_error(f"메시지 파싱 실패: {str(e)}")
        return None
```

### 2.2 _extract_orderbook_data() - 오더북 데이터 추출
```python
def _extract_orderbook_data(self, units: List[Dict[str, Any]], symbol: str) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
    """
    오더북 유닛에서 매수/매도 데이터 추출
    
    Args:
        units: 오더북 유닛 목록
        symbol: 심볼 이름
        
    Returns:
        Tuple[List[Dict[str, float]], List[Dict[str, float]]]: 매수(bids)와 매도(asks) 데이터
    """
    bids = []
    asks = []
    
    for unit in units:
        # 가격과 수량 추출
        bid_price = float(unit.get("bid_price", 0))
        bid_size = float(unit.get("bid_size", 0))
        ask_price = float(unit.get("ask_price", 0))
        ask_size = float(unit.get("ask_size", 0))
        
        # 데이터 추가
        bids.append({'price': bid_price, 'size': bid_size})
        asks.append({'price': ask_price, 'size': ask_size})

    # 0인 값 필터링 적용
    filtered_bids, filtered_asks = self.filter_zero_values(bids, asks)
        
    return filtered_bids, filtered_asks
```

## 3. 공통 유틸리티 메서드

### 3.1 filter_zero_values() - 0인 값 필터링
```python
def filter_zero_values(self, bids: List[Dict[str, float]], asks: List[Dict[str, float]]) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
    """
    0인 값을 필터링하는 공통 메서드
    
    Args:
        bids: 매수 호가 목록 [{'price': float, 'size': float}, ...]
        asks: 매도 호가 목록 [{'price': float, 'size': float}, ...]
        
    Returns:
        Tuple[List[Dict[str, float]], List[Dict[str, float]]]: 필터링된 매수/매도 호가 목록
    """
    # 매수 호가 필터링
    filtered_bids = []
    zero_bids_count = 0
    
    for bid in bids:
        if bid['size'] > 0:
            filtered_bids.append(bid)
        else:
            zero_bids_count += 1
    
    # 매도 호가 필터링
    filtered_asks = []
    zero_asks_count = 0
    
    for ask in asks:
        if ask['size'] > 0:
            filtered_asks.append(ask)
        else:
            zero_asks_count += 1
    
    # 통계 업데이트
    self.stats["filtered_zero_bids"] += zero_bids_count
    self.stats["filtered_zero_asks"] += zero_asks_count
    
    # 필터링 로그 (많은 항목이 필터링된 경우에만)
    if zero_bids_count > 5 or zero_asks_count > 5:
        self.log_debug(
            f"0인 값 필터링 | "
            f"매수: {zero_bids_count}건, 매도: {zero_asks_count}건"
        )
    
    return filtered_bids, filtered_asks
```

### 3.2 filter_zero_values_array() - 배열 형식 0인 값 필터링
```python
def filter_zero_values_array(self, bids: List[List[float]], asks: List[List[float]]) -> Tuple[List[List[float]], List[List[float]]]:
    """
    0인 값을 필터링하는 공통 메서드 (배열 형식용)
    
    Args:
        bids: 매수 호가 목록 [[price, size], ...]
        asks: 매도 호가 목록 [[price, size], ...]
        
    Returns:
        Tuple[List[List[float]], List[List[float]]]: 필터링된 매수/매도 호가 목록
    """
    # 매수 호가 필터링
    filtered_bids = []
    zero_bids_count = 0
    
    for bid in bids:
        if len(bid) >= 2 and bid[1] > 0:
            filtered_bids.append(bid)
        else:
            zero_bids_count += 1
    
    # 매도 호가 필터링
    filtered_asks = []
    zero_asks_count = 0
    
    for ask in asks:
        if len(ask) >= 2 and ask[1] > 0:
            filtered_asks.append(ask)
        else:
            zero_asks_count += 1
    
    # 통계 업데이트
    self.stats["filtered_zero_bids"] += zero_bids_count
    self.stats["filtered_zero_asks"] += zero_asks_count
    
    return filtered_bids, filtered_asks
```

## 4. 로깅 메서드

### 4.1 기본 로깅
```python
# 오류 로깅
self.log_error("오류 메시지")

# 정보 로깅
self.log_info("정보 메시지")

# 디버그 로깅
self.log_debug("디버그 메시지")

# 경고 로깅
self.log_warning("경고 메시지")
```

### 4.2 통계 조회
```python
# 파싱 통계 조회
stats = self.get_stats()
```

## 5. 주의사항

1. 모든 예외 처리는 구체적으로 해야 합니다.
2. 파싱 실패 시 None을 반환하고 오류를 로깅해야 합니다.
3. 0인 값은 반드시 필터링해야 합니다.
4. 타임스탬프는 밀리초 단위로 통일해야 합니다.
5. 심볼은 대문자로 통일해야 합니다.
6. 오더북 데이터는 표준 형식을 따르야 합니다:
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