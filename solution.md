# 크로스 김프 차익거래 프로젝트 구조 개선 방안

## 현재 상황 분석

현재 프로젝트의 구조는 네 가지 주요 디렉토리로 구성되어 있습니다:

1. `connection`: 웹소켓 연결을 담당하는 클래스들
2. `orderbook`: 주문책(orderbook) 데이터 관리 클래스들
3. `parser`: 웹소켓 메시지 파싱 클래스들
4. `websocket`: 웹소켓 전체 관리 클래스들

### 현재 구조의 문제점

현재 구조에서는 상속 관계가 복잡하게 얽혀 있어 다음과 같은 문제가 발생합니다:

1. **순환 의존성**: 예를 들어, `binance_f_ws.py`는 `binance_f_cn.py`를 상속받지만, `binance_f_cn.py`는 다시 `binance_f_pa.py`를 임포트하고 있습니다.

2. **과도한 상호 참조**: 각 모듈이 다른 모듈을 너무 많이 참조하여 의존성이 복잡해졌습니다.

3. **일관성 없는 상속 구조**: 일부 클래스는 Base 클래스를 상속받고, 다른 클래스는 구현 클래스를 상속받는 등 일관성이 없습니다.

4. **책임 분리 부족**: 각 클래스의 역할과 책임이 명확히 분리되지 않았습니다.

5. **데이터 흐름과 상속 구조의 불일치**: 실제 데이터 흐름(연결 → 데이터 수신 → 파싱 → 오더북 업데이트)과 상속 구조가 맞지 않아 코드 이해와 유지보수가 어렵습니다.

## 개선된 구조 제안

### 1. 실제 데이터 흐름에 맞는 계층 구조 설계

실제 데이터 흐름을 고려하여 다음과 같은 계층 구조를 제안합니다:

```
Base Classes
    ↓
Connection Classes (웹소켓 연결 담당)
    ↓
Parser Classes (수신된 raw 데이터 처리)
    ↓
OrderBook Classes (파싱된 데이터로 주문책 업데이트)
    ↓
WebSocket Manager Classes (전체 흐름 관리)
```

이 구조는 실제 데이터 흐름과 일치하며, 모든 모듈이 단방향 의존성을 가지도록 합니다.

### 2. 각 계층의 역할 명확화

1. **Base Classes**: 모든 기본 인터페이스와 공통 기능 정의
2. **Connection Classes**: 웹소켓 연결 수립 및 유지만 담당
3. **Parser Classes**: 연결을 통해 수신된 raw 데이터 파싱만 담당
4. **OrderBook Classes**: 파싱된 데이터로 주문책 구조체 관리 및 업데이트 담당
5. **WebSocket Manager Classes**: 전체 웹소켓 흐름 조율 및 관리

### 3. logger.py를 활용한 로깅 전략

이미 구현된 `logger.py`를 활용하여 각 모듈에서 일관된 로깅 전략을 사용합니다:

1. **모듈별 로깅**: 각 모듈은 `get_unified_logger()`를 사용하여 중앙 집중식 로깅을 구현합니다.
2. **에러 처리**: 중요한 에러는 에러 전용 로그 파일에 자동으로 저장됩니다.
3. **성능 모니터링**: `SafeRotatingFileHandler`를 활용한 안정적인 로그 관리 시스템을 사용합니다.

### 4. 구체적인 개선 방안

#### 4.1. 인터페이스 분리

각 Base 클래스는 인터페이스 역할을 하도록 명확히 설계합니다:

```python
# 예시: connection/base_connection.py
class BaseConnection(ABC):
    @abstractmethod
    async def connect(self) -> bool:
        """웹소켓 연결 수립"""
        pass
        
    @abstractmethod
    async def disconnect(self) -> bool:
        """웹소켓 연결 종료"""
        pass
        
    @abstractmethod
    async def send_message(self, message: str) -> bool:
        """웹소켓을 통해 메시지 전송"""
        pass
        
    @abstractmethod
    async def receive_message(self) -> Optional[str]:
        """웹소켓으로부터 메시지 수신"""
        pass
```

#### 4.2. 의존성 주입 패턴 적용

상위 클래스에서 하위 클래스를 직접 임포트하는 대신, 의존성 주입 패턴을 사용합니다:

```python
# 예시: parser/binance_f_parser.py
class BinanceFutureParser:
    def __init__(self, connection: BaseConnection, logger=None):
        self.connection = connection
        self.logger = logger or get_unified_logger()
        # ...
    
    def parse_message(self, raw_message: str) -> Dict[str, Any]:
        try:
            # 파싱 로직
            return parsed_data
        except Exception as e:
            self.logger.error(f"바이낸스 선물 메시지 파싱 오류: {e}")
            return {}
```

#### 4.3. 팩토리 패턴 도입

다양한 거래소 구현체를 생성하기 위해 팩토리 패턴을 사용합니다:

```python
# 예시: core/factory.py
class ExchangeComponentFactory:
    @staticmethod
    def create_connection(exchange_type: str, settings: dict) -> BaseConnection:
        if exchange_type == "binance_future":
            return BinanceFutureConnection(settings)
        # ...
    
    @staticmethod
    def create_parser(exchange_type: str, connection: BaseConnection) -> BaseParser:
        if exchange_type == "binance_future":
            return BinanceFutureParser(connection)
        # ...
    
    @staticmethod
    def create_orderbook(exchange_type: str, parser: BaseParser) -> BaseOrderBook:
        if exchange_type == "binance_future":
            return BinanceFutureOrderBook(parser)
        # ...
```

### 5. 단계별 리팩토링 계획

#### 단계 1: Base 클래스 구현 (1주차)

1. 각 레이어의 Base 클래스를 설계하고 추상 메소드 정의
2. 현재 코드에 있는 공통 기능 추출 및 Base 클래스로 이동
3. 인터페이스 문서화 (각 메소드의 역할, 파라미터, 반환값 등)

구체적인 작업:
- `base_connection.py` - 연결 관리 인터페이스 정의
    연결 수립/종료
    스냅샷 수신
    델타 메시지 수신
    핑/퐁 관리
    재연결 메커니즘
    연결 상태 모니터링
    에러 처리
- `base_parser.py` - 파싱 인터페이스 정의
    메시지 형식 검증
    JSON 데이터 파싱
    오더북 데이터 구조화
    에러 메시지 처리
    마켓 데이터 추출
    구독 메시지 생성
    0값 제거
- `base_orderbook.py` - 오더북 관리 인터페이스 정의
    오더북 데이터 구조 정의
    스냅샷 처리
    증분 업데이트 처리
    오더북 검증
    오더북 상태 관리
- `base_websocket.py` - 웹소켓 관리 인터페이스 정의
    다중 연결 관리
    심볼 구독 관리
    메시지 라우팅
    재시작 정책
    상태 모니터링
    이벤트 처리


#### 단계 2: Connection 클래스 리팩토링 (2주차)

1. 각 거래소별 Connection 클래스가 Base 클래스만 의존하도록 수정
2. 공통 기능 추출 및 유틸리티 클래스로 분리
3. 로깅 시스템 통합

구체적인 작업:
- 각 `{exchange}_cn.py` 파일을 `connection/{exchange}_connection.py`로 이동
- 순수한 연결 관리 책임만 남기고 다른 책임 제거
- `logger.py`의 `get_unified_logger()`를 활용한 일관된 로깅 구현

#### 단계 3: Parser 클래스 리팩토링 (3주차)

1. 각 거래소별 Parser 클래스가 Base 클래스와 Connection만 의존하도록 수정
2. 로깅 및 에러 처리 강화

구체적인 작업:
- 각 `{exchange}_pa.py` 파일을 순수한 파싱 책임만 가지도록 리팩토링
- Connection 객체를 의존성으로 주입받도록 수정
- 로깅 및 에러 처리 강화 (try-except 블록, 상세한 에러 메시지 등)

#### 단계 4: OrderBook 클래스 리팩토링 (4주차)

1. 각 거래소별 OrderBook 클래스가 상위 계층만 의존하도록 수정
2. 데이터 검증 로직 강화

구체적인 작업:
- 각 `{exchange}_ob.py` 파일을 순수한 오더북 관리 책임만 가지도록 리팩토링
- Parser 객체를 의존성으로 주입받도록 수정
- 데이터 검증 로직 추가 (불완전한 데이터, 오류 데이터 검출 등)

#### 단계 5: WebSocket Manager 클래스 리팩토링 (5주차)

1. 각 거래소별 WebSocket Manager 클래스가 단방향 의존성을 가지도록 수정
2. 재연결 및 에러 복구 메커니즘 강화

구체적인 작업:
- 각 `{exchange}_ws.py` 파일을 웹소켓 전체 관리 책임만 가지도록 리팩토링
- OrderBook 객체를 의존성으로 주입받도록 수정
- 재연결 로직 개선 및 에러 복구 메커니즘 강화

#### 단계 6: 팩토리 및 설정 관리 구현 (6주차)

1. 팩토리 클래스 구현으로 객체 생성 로직 중앙화
2. 설정 관리 시스템 개선
3. 초기화 순서 관리

구체적인 작업:
- `core/factory.py` 구현 (각 거래소 컴포넌트 생성 팩토리)
- 설정 관리 시스템 개선 (환경변수, 설정 파일 등)
- 초기화 순서 로직 구현 (Connection → Parser → OrderBook → WebSocket)

#### 단계 7: 테스트 및 안정화 (7주차)

1. 단위 테스트 구현
2. 통합 테스트 구현
3. 스트레스 테스트 및 성능 최적화

구체적인 작업:
- 각 컴포넌트별 단위 테스트 작성
- 전체 시스템 통합 테스트 구현
- 부하 테스트 및 성능 최적화

### 6. 코드 중복 제거 방안

#### 6.1. 유틸리티 클래스 도입

공통 기능을 유틸리티 클래스로 추출하여 중복 코드를 제거합니다:

```python
# 예시: core/utils/ws_utils.py
class WebSocketUtils:
    @staticmethod
    def create_subscribe_message(exchange: str, symbol: str, channel: str) -> Dict[str, Any]:
        """다양한 거래소에 대한 구독 메시지 생성"""
        if exchange == "binance":
            return {"method": "SUBSCRIBE", "params": [f"{symbol.lower()}@{channel}"], "id": 1}
        elif exchange == "bybit":
            return {"op": "subscribe", "args": [f"{channel}.{symbol}"]}
        # ...

# 예시: core/utils/validation_utils.py
class ValidationUtils:
    @staticmethod
    def validate_orderbook(bids: List[List[float]], asks: List[List[float]]) -> bool:
        """오더북 데이터 유효성 검증"""
        if not bids or not asks:
            return False
        
        # 가격 순서 확인 (bids는 내림차순, asks는 오름차순)
        for i in range(1, len(bids)):
            if bids[i][0] >= bids[i-1][0]:
                return False
                
        for i in range(1, len(asks)):
            if asks[i][0] <= asks[i-1][0]:
                return False
        
        # 가격 겹침 확인 (최고 매수가 < 최저 매도가)
        if bids[0][0] >= asks[0][0]:
            return False
            
        return True
```

#### 6.2. 믹스인 활용

공통 메서드를 믹스인 클래스로 분리하여 필요한 클래스에서 다중 상속으로 사용합니다:

```python
# 예시: core/mixins/reconnect_mixin.py
class ReconnectMixin:
    """재연결 기능을 제공하는 믹스인"""
    def __init__(self, max_retries=5, backoff_factor=1.5):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_count = 0
        self.logger = get_unified_logger()
    
    async def with_retry(self, func, *args, **kwargs):
        """지수 백오프 방식의 재시도 로직"""
        self.retry_count = 0
        while self.retry_count <= self.max_retries:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                self.retry_count += 1
                if self.retry_count > self.max_retries:
                    self.logger.error(f"최대 재시도 횟수 초과: {e}")
                    raise
                
                wait_time = self.backoff_factor ** self.retry_count
                self.logger.warning(f"재연결 시도 ({self.retry_count}/{self.max_retries}), {wait_time}초 후 재시도")
                await asyncio.sleep(wait_time)
```

#### 6.3. 컴포지션 활용

상속 대신 컴포지션(구성)을 사용하여 코드 재사용성을 높입니다:

```python
# 예시: orderbook/binance_f_orderbook.py
class BinanceFutureOrderBook:
    def __init__(self, parser, validator=None, metrics_manager=None):
        self.parser = parser
        self.validator = validator or ValidationUtils()
        self.metrics_manager = metrics_manager or WebsocketMetricsManager()
        self.logger = get_unified_logger()
        # ...
    
    def update(self, data):
        parsed_data = self.parser.parse_message(data)
        if not self.validator.validate_orderbook(parsed_data["bids"], parsed_data["asks"]):
            self.logger.warning("유효하지 않은 오더북 데이터")
            return
            
        # 오더북 업데이트 로직
        # ...
        
        # 메트릭 업데이트
        self.metrics_manager.update_orderbook_metrics(len(parsed_data["bids"]), len(parsed_data["asks"]))
```

### 7. 새로운 디렉토리 구조 제안

```
src/crosskimp/ob_collector/
├── core/
│   ├── base/
│   │   ├── base_connection.py
│   │   ├── base_parser.py
│   │   ├── base_orderbook.py
│   │   └── base_websocket.py
│   ├── factory/
│   │   ├── exchange_factory.py
│   │   └── component_factory.py
│   ├── mixins/
│   │   ├── reconnect_mixin.py
│   │   ├── validation_mixin.py
│   │   └── logging_mixin.py
│   └── utils/
│       ├── ws_utils.py
│       ├── validation_utils.py
│       └── metric_utils.py
├── connection/
│   ├── binance_f_connection.py
│   ├── binance_s_connection.py
│   ├── bybit_f_connection.py
│   └── ...
├── parser/
│   ├── binance_f_parser.py
│   ├── binance_s_parser.py
│   ├── bybit_f_parser.py
│   └── ...
├── orderbook/
│   ├── binance_f_orderbook.py
│   ├── binance_s_orderbook.py
│   ├── bybit_f_orderbook.py
│   └── ...
└── websocket/
    ├── binance_f_websocket.py
    ├── binance_s_websocket.py
    ├── bybit_f_websocket.py
    └── ...
```

### 8. 데이터 흐름 개선

#### 8.1. 이벤트 기반 아키텍처 도입

실제 데이터 흐름과 동일한 이벤트 기반 아키텍처를 도입합니다:

```
Connection → Parser → OrderBook → WebSocket Manager → 외부 시스템
    ↑                                    |
    └────────────────────────────────────┘
```

각 컴포넌트는 이벤트를 발행하고 구독하는 방식으로 통신합니다:

```python
# 예시: core/event/event_bus.py
class EventBus:
    def __init__(self):
        self.subscribers = defaultdict(list)
        self.logger = get_unified_logger()
    
    def subscribe(self, event_type: str, callback: Callable):
        """이벤트 구독"""
        self.subscribers[event_type].append(callback)
    
    async def publish(self, event_type: str, data: Any):
        """이벤트 발행"""
        if event_type not in self.subscribers:
            return
            
        for callback in self.subscribers[event_type]:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(data)
                else:
                    callback(data)
            except Exception as e:
                self.logger.error(f"이벤트 처리 중 오류: {e}")
```

#### 8.2. 비동기 처리 최적화

1. **Task 관리 개선**: 불필요한 Task 생성을 줄이고 적절한 에러 처리를 추가합니다.
2. **Queue 최적화**: 각 컴포넌트 간 데이터 전달에 최적화된 Queue 구성을 설계합니다.
3. **세마포어 활용**: 동시 접근 제한이 필요한 리소스에 세마포어를 활용합니다.

```python
# 예시: websocket/base_ws_manager.py
class BaseWebSocketManager:
    def __init__(self, max_concurrent_connections=5):
        self.semaphore = asyncio.Semaphore(max_concurrent_connections)
        self.tasks = {}
        self.logger = get_unified_logger()
    
    async def add_connection(self, exchange, symbol):
        """세마포어를 사용하여 동시 연결 제한"""
        async with self.semaphore:
            try:
                connection = await self.create_connection(exchange, symbol)
                task = asyncio.create_task(self.manage_connection(connection))
                self.tasks[f"{exchange}_{symbol}"] = task
                return True
            except Exception as e:
                self.logger.error(f"연결 추가 중 오류: {e}")
                return False
    
    def cancel_all_tasks(self):
        """모든 태스크 정리"""
        for key, task in self.tasks.items():
            if not task.done():
                task.cancel()
```

### 9. 테스트 용이성 개선

#### 9.1. 인터페이스 기반 설계와 모킹

각 컴포넌트를 인터페이스로 정의하여 테스트 시 모킹을 쉽게 합니다:

```python
# 예시: tests/mocks/mock_connection.py
class MockConnection(BaseConnection):
    """테스트용 모의 연결 클래스"""
    def __init__(self, test_data=None):
        self.test_data = test_data or []
        self.is_connected = False
        self.sent_messages = []
    
    async def connect(self):
        self.is_connected = True
        return True
    
    async def disconnect(self):
        self.is_connected = False
        return True
    
    async def send_message(self, message):
        self.sent_messages.append(message)
        return True
    
    async def receive_message(self):
        if not self.test_data:
            return None
        return self.test_data.pop(0)
```

#### 9.2. 의존성 주입을 통한 테스트

테스트 시 실제 구현체 대신 모의 객체를 주입하여 단위 테스트를 용이하게 합니다:

```python
# 예시: tests/test_parser.py
async def test_binance_parser():
    # 테스트 데이터 설정
    test_data = [
        '{"e":"depthUpdate","E":1623151677,"s":"BTCUSDT","U":12345,"u":12346,"b":[["40000","1.5"]],"a":[["40100","0.8"]]}'
    ]
    
    # 모의 연결 생성
    mock_connection = MockConnection(test_data)
    
    # 파서 생성 (의존성 주입)
    parser = BinanceFutureParser(mock_connection)
    
    # 파싱 테스트
    message = await mock_connection.receive_message()
    result = parser.parse_message(message)
    
    # 결과 검증
    assert result["symbol"] == "BTCUSDT"
    assert len(result["bids"]) == 1
    assert result["bids"][0][0] == 40000
    assert result["bids"][0][1] == 1.5
```

### 10. 로깅 및 모니터링 전략

기존 `logger.py`를 활용하여 통합 로깅 및 모니터링 시스템을 구축합니다:

#### 10.1. 각 계층별 로깅 전략

```python
# 예시: connection/binance_f_connection.py
class BinanceFutureConnection(BaseConnection):
    def __init__(self, settings):
        self.settings = settings
        self.logger = get_unified_logger()
        # ...
    
    async def connect(self):
        try:
            # 연결 로직
            self.logger.info(f"바이낸스 선물 연결 성공: {self.settings['url']}")
            return True
        except Exception as e:
            self.logger.error(f"바이낸스 선물 연결 실패: {e}")
            return False
```

#### 10.2. 메트릭 수집 및 모니터링

성능 및 상태 모니터링을 위한 메트릭 수집 시스템을 구현합니다:

```python
# 예시: core/monitoring/metrics_manager.py
class MetricsManager:
    def __init__(self):
        self.metrics = defaultdict(dict)
        self.logger = get_unified_logger()
    
    def update_connection_metric(self, exchange, symbol, metric_name, value):
        """연결 관련 메트릭 업데이트"""
        key = f"{exchange}_{symbol}"
        self.metrics[key][f"connection_{metric_name}"] = value
    
    def update_orderbook_metric(self, exchange, symbol, metric_name, value):
        """오더북 관련 메트릭 업데이트"""
        key = f"{exchange}_{symbol}"
        self.metrics[key][f"orderbook_{metric_name}"] = value
    
    def log_metrics(self):
        """현재 메트릭 로깅"""
        for key, values in self.metrics.items():
            self.logger.info(f"메트릭 [{key}]: {values}")
```

### 11. 결론 및 기대 효과

제안된 구조 개선을 통해 다음과 같은 효과를 기대할 수 있습니다:

1. **유지보수성 향상**: 명확한 책임 분리로 코드 이해와 유지보수가 쉬워집니다.
2. **확장성 개선**: 새로운 거래소 추가 시 기존 코드 수정 없이 새 구현체만 추가하면 됩니다.
3. **코드 재사용성 증가**: 공통 기능을 Base 클래스나 유틸리티로 분리하여 중복 코드를 줄입니다.
4. **테스트 용이성**: 컴포넌트 간 의존성이 명확해져 단위 테스트가 쉬워집니다.
5. **안정성 향상**: 단방향 의존성으로 한 모듈의 변경이 다른 모듈에 미치는 영향이 최소화됩니다.
6. **디버깅 용이성**: 명확한 책임 분리로 문제 발생 시 원인 파악이 쉬워집니다.
7. **성능 최적화**: 비동기 처리 최적화로 시스템 성능이 향상됩니다.
8. **로깅 통합**: 기존 logger.py를 활용한 일관된 로깅 시스템으로 문제 추적이 용이해집니다.

이러한 구조 개선은 개인 프로젝트로서 장기적인 유지보수와 확장을 고려할 때 매우 중요한 투자입니다. 처음에는 리팩토링에 시간이 소요되지만, 장기적으로 더 안정적이고 관리하기 쉬운 코드베이스를 얻을 수 있습니다.