# 오더북 수집 시스템 개발 가이드

## 1. 시스템 개요

오더북 수집 시스템은 다양한 암호화폐 거래소의 오더북 데이터를 실시간으로 수집하고 처리하는 시스템입니다. 웹소켓 연결을 통해 데이터를 수신하고, 검증 및 정규화 과정을 거쳐 일관된 형태로 제공합니다.

### 주요 기능

- 다양한 거래소 지원 (바이낸스 선물, 업비트, 빗썸 등)
- 실시간 오더북 데이터 수집 및 처리
- 웹소켓 연결 관리 및 자동 재연결
- 오더북 데이터 검증 및 정규화
- 거래량 기반 심볼 필터링
- 중앙 집중식 데이터 관리 및 로깅

### 구조 다이어그램

```
OrderbookCollectorManager
│
├── ConnectionManager (연결 상태 모니터링)
│   └── [Exchange]Connector (ExchangeConnectorInterface 구현)
│
├── ExchangeConnectorFactory
│   └── [Exchange]Connector (e.g. BinanceFutureConnector)
│       ├── [Exchange]ConnectionStrategy
│       └── [Exchange]DataHandler
│
├── OrderbookDataManager (중앙 데이터 관리)
│   ├── 로깅 관리
│   ├── 통계 수집
│   └── C++ 연동 인터페이스
│
└── Aggregator
    └── 심볼 필터링
```

## 2. 주요 컴포넌트

### 2.1 OrderbookCollectorManager

전체 시스템의 진입점으로, 오더북 수집 프로세스를 관리합니다.

**주요 역할**:
- 시스템 초기화 및 시작/종료
- 거래소 연결 관리자 조율
- 거래소 커넥터 생성 및 등록
- 오더북 업데이트 콜백 처리

**주요 메서드**:
- `initialize()`: 시스템 초기화
- `start()`: 오더북 수집 시작
- `stop()`: 오더북 수집 종료
- `add_orderbook_callback()`: 오더북 업데이트 콜백 등록

### 2.2 ConnectionManager

여러 거래소의 연결 상태를 모니터링합니다.

**주요 역할**:
- 연결 객체 관리
- 연결 상태 추적 (단일 진실 공급원)
- 연결 상태 모니터링

**주요 메서드**:
- `register_connector()`: 거래소 연결 객체 등록
- `update_exchange_status()`: 거래소 연결 상태 업데이트
- `get_connection_status()`: 거래소 연결 상태 조회
- `start_monitoring()`: 연결 상태 모니터링 시작

### 2.3 ExchangeConnectorFactory

거래소별 커넥터 객체를 생성하는 팩토리 클래스입니다.

**주요 역할**:
- 거래소 코드에 따라 적절한 커넥터 객체 생성
- 커넥터 인스턴스 캐싱
- ConnectionManager 주입

**주요 메서드**:
- `create_connector()`: 거래소 커넥터 생성 (ConnectionManager 전달)
- `get_connector()`: 캐시된 거래소 커넥터 가져오기
- `get_supported_exchanges()`: 지원하는 거래소 코드 목록 반환

### 2.4 ExchangeConnectorInterface

모든 거래소 커넥터가 구현해야 하는 표준 인터페이스를 정의합니다.

**주요 역할**:
- 모든 거래소 커넥터의 일관된 API 정의
- ConnectionManager와 OrderbookCollectorManager가 일관된 방식으로 커넥터와 상호작용

**주요 메서드**:
- `connect()`: 거래소 웹소켓 연결
- `disconnect()`: 거래소 웹소켓 연결 종료
- `subscribe()`: 심볼 구독
- `unsubscribe()`: 심볼 구독 해제
- `add_message_callback()`: 웹소켓 메시지 콜백 등록
- `remove_message_callback()`: 웹소켓 메시지 콜백 제거
- `send_message()`: 웹소켓으로 메시지 전송
- `add_orderbook_callback()`: 오더북 업데이트 콜백 등록
- `remove_orderbook_callback()`: 오더북 업데이트 콜백 제거
- `get_orderbook_snapshot()`: 오더북 스냅샷 조회
- `refresh_snapshots()`: 오더북 스냅샷 갱신

### 2.5 OrderbookDataManager

모든 거래소의 오더북 데이터를 중앙에서 관리하는 컴포넌트입니다.

**주요 역할**:
- 거래소별 raw 데이터 로깅 관리
- 가공된 오더북 데이터 로깅
- 메시지 통계 수집 및 모니터링
- C++ 연동을 위한 인터페이스 제공

**주요 메서드**:
- `register_exchange()`: 거래소 등록
- `log_raw_message()`: 원시 메시지 로깅
- `log_orderbook_data()`: 가공된 오더북 데이터 로깅
- `register_callback()`: 데이터 처리 콜백 등록
- `print_stats()`: 메시지 통계 출력
- `prepare_for_cpp_export()`: C++ 내보내기용 데이터 준비

**사용 예시**:
```python
# 데이터 관리자 가져오기
data_manager = get_orderbook_data_manager()

# 거래소 등록
data_manager.register_exchange("binance_future")

# 원시 메시지 로깅
data_manager.log_raw_message(
    exchange_code="binance_future",
    message="depthUpdate raw: {...}"
)

# 오더북 데이터 로깅
data_manager.log_orderbook_data(
    exchange_code="binance_future",
    symbol="btc",
    orderbook={...}
)

# 통계 출력
data_manager.print_stats()
```

### 2.6 거래소별 커넥터 (BinanceFutureConnector 등)

특정 거래소의 웹소켓 연결 및 데이터 처리를 담당합니다. 각 거래소 커넥터는 ExchangeConnectorInterface를 구현하여 웹소켓 연결을 직접 관리합니다.

**주요 역할**:
- 거래소별 웹소켓 연결 관리 (연결, 재연결, 종료)
- 메시지 처리 및 콜백 실행
- 오더북 스냅샷 요청 및 처리
- 오더북 업데이트 처리
- ConnectionManager와 상태 동기화

**구성 요소**:
- `ConnectionStrategy`: 거래소별 연결 전략 구현
- `DataHandler`: 거래소별 데이터 처리 로직

### 2.7 거래소별 데이터 핸들러 (BinanceFutureDataHandler 등)

거래소 데이터 처리 로직을 담당합니다.

**주요 역할**:
- 오더북 스냅샷 요청 및 처리
- 델타 업데이트 처리
- 시퀀스 검증 및 관리

**주요 메서드**:
- `get_orderbook_snapshot()`: 오더북 스냅샷 요청
- `process_orderbook_update()`: 오더북 업데이트 메시지 처리
- `process_delta_update()`: 델타 업데이트 처리

### 2.8 검증기 (BaseOrderBookValidator)

오더북 데이터의 유효성을 검증합니다.

**주요 역할**:
- 오더북 데이터 검증
- 가격 역전 처리
- 시퀀스 및 타임스탬프 검증

**주요 메서드**:
- `initialize_orderbook()`: 오더북 초기화 (스냅샷)
- `update()`: 오더북 업데이트 (델타)
- `validate_orderbook()`: 오더북 데이터 검증
- `get_orderbook()`: 심볼의 오더북 데이터 반환

## 3. 데이터 흐름

### 3.1 오더북 데이터 수집 흐름

1. `OrderbookCollectorManager`가 초기화 및 시작됩니다.
2. `OrderbookDataManager`가 초기화되고 각 거래소가 등록됩니다.
3. `ExchangeConnectorFactory`를 통해 거래소별 커넥터가 생성되고 `ConnectionManager`에 등록됩니다.
4. 각 커넥터는 자체적으로 웹소켓 연결을 수립하고 지정된 심볼을 구독합니다.
5. 오더북 스냅샷을 요청하여 초기 오더북 상태를 설정합니다.
6. 웹소켓으로 델타 업데이트를 수신하고 처리합니다.
7. 처리된 데이터는 `OrderbookDataManager`를 통해 로깅되고 필요한 경우 C++로 전송됩니다.
8. 주기적으로 통계가 수집되고 출력됩니다.

### 3.2 데이터 로깅 흐름

1. **원시 데이터 로깅**:
```
[YYYY-MM-DD H:M:S.ms] depthUpdate raw: {
  "type": "depth_update",
  "symbol": "btcusdt",
  "first_update_id": 123456789,
  "final_update_id": 123456790,
  "bids": [[price, quantity], ...],
  "asks": [[price, quantity], ...]
}
```

2. **가공된 오더북 로깅**:
```
[H:M:S.ms] 매수 (n) / 매도 (n) {
  "exchange": "거래소코드",
  "symbol": "심볼명",
  "ts": 타임스탬프,
  "seq": 시퀀스번호,
  "bids": [[가격, 수량], ...],
  "asks": [[가격, 수량], ...]
}
```

3. **통계 로깅**:
```
== 오더북 데이터 관리자 통계 ==
총 메시지 수: 1,234,567개
경과 시간: 3600.00초
초당 메시지: 342.94개/초
[binance_future] 원시 메시지: 500,000개, 오더북 메시지: 450,000개, 초당 메시지: 138.89개/초
[binance_spot] 원시 메시지: 600,000개, 오더북 메시지: 550,000개, 초당 메시지: 166.67개/초
```

## 4. 새로운 거래소 추가 방법

새로운 거래소를 시스템에 추가하려면 다음 클래스를 구현해야 합니다:

### 4.1 거래소 연결 전략 구현

```python
# src/crosskimp/ob_collector/orderbook/connection/strategies/new_exchange.py
class NewExchangeConnectionStrategy:
    """새로운 거래소 웹소켓 연결 전략 클래스"""
    
    def __init__(self):
        # 초기화 코드
        
    def get_ws_url(self) -> str:
        # 웹소켓 URL 반환
        
    def requires_ping(self) -> bool:
        # ping 필요 여부
        
    async def connect(self, timeout: int) -> websockets.WebSocketClientProtocol:
        # 웹소켓 연결 수립 (타임아웃 파라미터 필수)
        
    async def disconnect(self, ws: websockets.WebSocketClientProtocol) -> None:
        # 웹소켓 연결 종료
        
    async def on_connected(self, ws: websockets.WebSocketClientProtocol) -> None:
        # 연결 후 초기화 작업
        
    async def subscribe(self, ws: websockets.WebSocketClientProtocol, symbols: List[str]) -> bool:
        # 심볼 구독
        
    async def unsubscribe(self, ws: websockets.WebSocketClientProtocol, symbols: List[str]) -> bool:
        # 심볼 구독 해제
        
    async def send_ping(self, ws: websockets.WebSocketClientProtocol) -> bool:
        # Ping 메시지 전송
        
    def preprocess_message(self, message: str) -> Dict[str, Any]:
        # 수신된 메시지 전처리
```

### 4.2 거래소 데이터 핸들러 구현

```python
# src/crosskimp/ob_collector/orderbook/data_handlers/new_exchange.py
class NewExchangeDataHandler:
    """새로운 거래소 데이터 핸들러 클래스"""
    
    def __init__(self):
        # 초기화 코드
        
    async def get_orderbook_snapshot(self, symbol: str, limit: int = None) -> Dict[str, Any]:
        # 오더북 스냅샷 요청
        
    async def process_snapshot(self, symbol: str, data: Dict) -> None:
        # 스냅샷 데이터 처리
        
    def process_orderbook_update(self, message: Dict[str, Any]) -> Dict[str, Any]:
        # 오더북 업데이트 메시지 처리
        
    async def process_delta_update(self, symbol: str, data: Dict) -> None:
        # 델타 업데이트 처리
        
    def normalize_symbol(self, symbol: str) -> str:
        # 심볼 형식 정규화
```

### 4.3 거래소 커넥터 구현

```python
# src/crosskimp/ob_collector/orderbook/exchange/new_exchange_connector.py
from crosskimp.ob_collector.orderbook.connection.exchange_connector_interface import ExchangeConnectorInterface

class NewExchangeConnector(ExchangeConnectorInterface):
    """새로운 거래소 커넥터 클래스"""
    
    def __init__(self, connection_manager=None):
        # 초기화 코드
        # 연결 관리자 참조 저장
        self.connection_manager = connection_manager
        
        # 연결 전략 및 데이터 핸들러 생성
        self.connection_strategy = NewExchangeConnectionStrategy()
        self.data_handler = NewExchangeDataHandler()
        
        # 웹소켓 관련 상태 변수 설정
        self._is_connected = False
        self.ws = None
        self.message_callbacks = []
        self.message_task = None
        
        # 연결 설정
        self.connect_timeout = self.config.get_system("connection.connect_timeout", 30)
        
        # 커넥터 관련 상태 변수
        self.orderbook_callbacks = []
        self.subscribed_symbols = []
        self.snapshot_cache = {}
        
    @property
    def is_connected(self) -> bool:
        """연결 상태 반환"""
        return self._is_connected
        
    @is_connected.setter
    def is_connected(self, value: bool):
        """연결 상태 설정"""
        self._is_connected = value
        
    async def connect(self) -> bool:
        """웹소켓 연결 수립"""
        # 이미 연결되어 있는지 확인
        if self.is_connected:
            self.logger.info("이미 연결되어 있음")
            return True
            
        try:
            # 웹소켓 연결 직접 생성 (타임아웃 전달)
            self.ws = await self.connection_strategy.connect(self.connect_timeout)
            
            if self.ws:
                # 연결 성공 시 상태 업데이트
                self.is_connected = True
                
                # ConnectionManager에 상태 업데이트
                if self.connection_manager:
                    self.connection_manager.update_exchange_status(Exchange.NEW_EXCHANGE.value, True)
                
                # 메시지 처리 작업 시작
                if not self.message_task or self.message_task.done():
                    self.message_task = asyncio.create_task(self._message_handler())
                
                # 초기화 작업 수행
                await self.connection_strategy.on_connected(self.ws)
                
                return True
            else:
                self.logger.error("웹소켓 연결 실패")
                return False
        except Exception as e:
            self.logger.error(f"연결 실패: {str(e)}")
            return False
        
    async def disconnect(self) -> bool:
        """웹소켓 연결 종료"""
        # 리소스 해제 코드
        
    async def subscribe(self, symbols: List[str]) -> bool:
        """심볼 구독"""
        # 구독 코드
        
    async def unsubscribe(self, symbols: List[str]) -> bool:
        """심볼 구독 해제"""
        # 구독 해제 코드
        
    def add_message_callback(self, callback: Callable) -> None:
        """메시지 수신 콜백 추가"""
        # 콜백 추가 코드
        
    def remove_message_callback(self, callback: Callable) -> bool:
        """메시지 수신 콜백 제거"""
        # 콜백 제거 코드
        
    def add_orderbook_callback(self, callback: Callable) -> None:
        """오더북 업데이트 콜백 추가"""
        # 콜백 추가 코드
        
    def remove_orderbook_callback(self, callback: Callable) -> bool:
        """오더북 업데이트 콜백 제거"""
        # 콜백 제거 코드
        
    async def get_orderbook_snapshot(self, symbol: str) -> Dict[str, Any]:
        """특정 심볼의 오더북 스냅샷 조회"""
        # 오더북 스냅샷 조회 코드
        
    async def refresh_snapshots(self, symbols: List[str] = None) -> None:
        """오더북 스냅샷 갱신"""
        # 스냅샷 갱신 코드
        
    async def get_websocket(self) -> Optional[websockets.WebSocketClientProtocol]:
        """웹소켓 객체 반환"""
        return self.ws
        
    async def reconnect(self) -> bool:
        """재연결 수행"""
        # 재연결 코드
        
    async def send_message(self, message: Union[str, Dict, List]) -> bool:
        """웹소켓으로 메시지 전송"""
        # 메시지 전송 코드
```

### 4.4 팩토리 클래스 업데이트

```python
# src/crosskimp/ob_collector/orderbook/exchange/factory.py
from crosskimp.ob_collector.orderbook.connection.exchange_connector_interface import ExchangeConnectorInterface
from crosskimp.ob_collector.orderbook.exchange.new_exchange_connector import NewExchangeConnector

class ExchangeConnectorFactory:
    """거래소 커넥터 팩토리 클래스"""
    
    def create_connector(self, exchange_code: str, connection_manager=None) -> ExchangeConnectorInterface:
        """
        거래소 커넥터 생성
        
        Args:
            exchange_code: 거래소 코드
            connection_manager: 연결 관리자 객체 (선택 사항)
            
        Returns:
            ExchangeConnectorInterface: 생성된 커넥터 객체
        """
        # 기존 코드...
        
        # 새로운 거래소 추가
        elif exchange_code == Exchange.NEW_EXCHANGE.value:
            connector = NewExchangeConnector(connection_manager)
            self.logger.info(f"새로운 거래소 커넥터 생성 완료")
        
        # 이하 기존 코드...
        
    def get_supported_exchanges(self) -> list:
        """지원하는 거래소 코드 목록 반환"""
        return [Exchange.BINANCE_FUTURE.value, Exchange.NEW_EXCHANGE.value]  # 새 거래소 추가
```

### 4.5 상수 추가

```python
# src/crosskimp/common/config/common_constants.py
class Exchange(Enum):
    """거래소 코드"""
    # 기존 코드...
    NEW_EXCHANGE = "new_exchange"  # 새 거래소 추가

# 거래소 이름 매핑 (한글)
EXCHANGE_NAMES_KR = {
    # 기존 코드...
    Exchange.NEW_EXCHANGE.value: "새거래소",  # 새 거래소 추가
}
```

## 5. 모범 사례 및 주의사항

### 5.1 웹소켓 연결 관리

- 모든 거래소 커넥터는 동일한 연결 패턴을 따라야 합니다.
- 연결 전에 항상 이미 연결되어 있는지 확인합니다.
- 연결 성공 시 ConnectionManager에 상태를 업데이트합니다.
- 타임아웃 설정을 일관되게 적용합니다.

```python
# 웹소켓 연결 처리 표준 패턴
async def connect(self) -> bool:
    # 이미 연결되어 있는지 확인
    if self.is_connected:
        self.logger.info("이미 연결되어 있음")
        return True
        
    try:
        # 타임아웃 설정으로 웹소켓 연결
        self.ws = await self.connection_strategy.connect(self.connect_timeout)
        
        if self.ws:
            # 연결 성공 시 상태 업데이트
            self.is_connected = True
            
            # ConnectionManager에 상태 업데이트
            if self.connection_manager:
                self.connection_manager.update_exchange_status(Exchange.MY_EXCHANGE.value, True)
            
            # 메시지 처리 작업 시작
            if not self.message_task or self.message_task.done():
                self.message_task = asyncio.create_task(self._message_handler())
            
            # 초기화 작업 수행
            await self.connection_strategy.on_connected(self.ws)
            
            return True
    except Exception as e:
        self.logger.error(f"웹소켓 연결 실패: {str(e)}")
        return False
```

### 5.2 시퀀스 관리

- 시퀀스 역전이 발생한 경우에만 스냅샷을 재요청합니다.
- 큰 시퀀스 갭은 무시하고 계속 진행합니다.

```python
# 시퀀스 역전 감지 및 처리 예시
if first_update_id < last_update_id:
    # 시퀀스 역전 발생 - 스냅샷 재요청
    self.logger.warning(f"시퀀스 역전 감지: {first_update_id} < {last_update_id}, 새 스냅샷 요청")
    await self.get_orderbook_snapshot(symbol)
```

### 5.3 API 요청 제한 관리

- 거래소 API 요청 제한을 준수합니다.
- 스냅샷 요청에 쿨다운 메커니즘을 적용합니다.

```python
# API 요청 제한 관리 예시
current_time = time.time()
if (current_time - self.last_api_call_time) < 0.1:  # 최소 100ms 간격
    await asyncio.sleep(0.1)

self.last_api_call_time = time.time()
```

### 5.4 일관된 인터페이스 사용

- 모든 거래소 커넥터는 `ExchangeConnectorInterface`를 구현해야 합니다.
- 이를 통해 ConnectionManager와 OrderbookCollectorManager가 일관된 방식으로 상호작용합니다.

```python
# 인터페이스 구현 예시
class MyExchangeConnector(ExchangeConnectorInterface):
    @property
    def is_connected(self) -> bool:
        return self._is_connected
        
    async def connect(self) -> bool:
        # 연결 구현
        
    async def disconnect(self) -> bool:
        # 연결 종료 구현
        
    # 기타 필수 메서드 구현
```

### 5.5 의존성 주입 패턴 활용

- 커넥터 생성 시 ConnectionManager를 명시적으로 주입합니다.
- 이를 통해 컴포넌트 간 책임 분리와 상태 동기화를 개선합니다.

```python
# 팩토리에서 커넥터 생성 시 ConnectionManager 주입
connector = ExchangeConnectorFactory().create_connector(
    exchange_code=Exchange.BINANCE_FUTURE.value,
    connection_manager=self.connection_manager
)

# 커넥터 내부에서 상태 업데이트
if self.connection_manager:
    self.connection_manager.update_exchange_status(Exchange.BINANCE_FUTURE.value, True)
```

### 5.6 오더북 로깅 포맷

오더북 데이터는 다음 포맷으로 로깅합니다:

```python
# 오더북 로깅 예시
def on_orderbook_update(data):
    if "current_orderbook" in data:
        orderbook = data["current_orderbook"]
        bids = orderbook.get('bids', [])[:10]  # 상위 10개 매수 호가만
        asks = orderbook.get('asks', [])[:10]  # 상위 10개 매도 호가만
        
        current_time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        
        log_data = {
            "exchange": exchange,
            "symbol": symbol,
            "ts": orderbook.get("timestamp", int(time.time() * 1000)),
            "seq": orderbook.get("sequence", 0),
            "bids": bids,
            "asks": asks
        }
        
        logger.debug(f"[{current_time}] 매수 ({len(bids)}) / 매도 ({len(asks)}) {json.dumps(log_data)}")
```

## 6. 문제 해결

### 6.1 연결 문제

- 각 거래소 커넥터는 자체적으로, 거래소별 특성에 맞는 연결 관리 로직을 구현합니다.
- 웹소켓 연결이 끊어진 경우, 자동 재연결 메커니즘이 동작합니다.
- 재연결 지연은 지수 백오프 방식으로 증가합니다.

### 6.2 API 속도 제한

- "Too many requests" 오류가 발생하면 API 요청 주기를 늘립니다.
- 스냅샷 요청에 쿨다운 시간을 적용합니다.

### 6.3 시퀀스 관리

- 시퀀스 역전 시에만 스냅샷을 재요청합니다.
- 시퀀스 갭이 크더라도 역전이 아니면 업데이트를 계속 진행합니다.

### 6.4 연결 상태 불일치

- ConnectionManager와 커넥터 간의 연결 상태 동기화 문제가 발생할 수 있습니다.
- 상태 변경 시 항상 ConnectionManager에 알리는 패턴을 지켜야 합니다.
- ConnectionManager의 주기적 상태 확인으로 불일치를 감지하고 수정합니다.

## 7. 부록

### 7.1 로그 레벨 가이드

- ERROR: 시스템 동작에 문제가 생긴 심각한 오류
- WARNING: 처리 가능하지만 주의가 필요한 상황
- INFO: 주요 시스템 동작 정보
- DEBUG: 상세한 디버깅 정보 (오더북 데이터 포함)

### 7.2 환경 변수

- `DEBUG_MESSAGE_LOGGING`: 메시지 로깅 활성화 여부 (1=활성화, 0=비활성화)
- `LOG_LEVEL`: 로그 레벨 설정 (ERROR, WARNING, INFO, DEBUG)

## 8. 향후 확장 계획

### 8.1 데이터 관리 확장

1. **심볼 관리 중앙화**
   - 모든 거래소의 심볼 매핑과 정규화를 중앙에서 처리
   - 일관된 심볼 형식 적용

2. **오더북 검증 통합**
   - 검증 로직을 데이터 관리자로 이동
   - 일관된 검증 규칙 적용

3. **스냅샷 관리 개선**
   - REST API 호출 중앙 관리
   - 요청 제한 통합 관리

4. **메모리 관리 최적화**
   - 오더북 데이터 캐싱
   - 메모리 사용량 모니터링

### 8.2 성능 모니터링

1. **시간 동기화**
   - 거래소별 이벤트 타임스탬프 통일
   - 지연 시간 측정 및 보정

2. **품질 지표**
   - 스프레드, 깊이, 유동성 등 모니터링
   - 이상 패턴 감지

3. **데이터 저장**
   - 실시간 데이터 영구 저장
   - 분석용 데이터 다운샘플링

### 8.3 아키텍처 개선

1. **멀티 프로세스**
   - 수집과 처리 분리
   - IPC 기반 데이터 전송

2. **C++ 엔진**
   - 고성능 처리 엔진 통합
   - 제로카피 데이터 전송

---

이 가이드는 오더북 수집 시스템의 주요 컴포넌트와 확장 방법을 설명합니다. 코드 이해 및 새로운 거래소 통합에 참고하시기 바랍니다. 