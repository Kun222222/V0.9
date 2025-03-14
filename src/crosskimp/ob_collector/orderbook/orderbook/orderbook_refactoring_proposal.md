# 오더북 수집기 코드 리팩토링 제안서

## 1. 현재 상황 분석

현재 오더북 수집기 코드는 다음과 같은 구조로 되어 있습니다:

- **웹소켓 연결 관리**: `base_ws_connector.py`, 거래소별 웹소켓 구현
- **오더북 데이터 관리**: `base_ob_v2.py`, 거래소별 오더북 구현
- **웹소켓 관리자**: `base_ws_manager.py`

이 구조는 기능적으로는 작동하지만 다음과 같은 문제점이 있습니다:

1. **코드 중복**: 거래소별 구현에 유사한 코드가 반복됨
2. **책임 분산**: 웹소켓 파싱과 오더북 업데이트 로직이 혼재
3. **일관성 부족**: 거래소별로 구현 방식이 다름
4. **유지보수 어려움**: 변경 시 여러 파일을 수정해야 함

## 2. 개선 목표

1. **코드 구조 명확화**: 책임과 역할에 따라 코드 구조 재정립
2. **중복 코드 제거**: 공통 로직을 추상화하여 중복 최소화
3. **확장성 향상**: 새로운 거래소 추가가 용이하도록 설계
4. **테스트 용이성**: 각 컴포넌트를 독립적으로 테스트할 수 있는 구조

## 3. 새로운 디렉토리 구조 제안

```
src/crosskimp/ob_collector/
├── websocket/                  # 웹소켓 관련 코드
│   ├── base/                   # 기본 클래스
│   │   ├── connector.py        # 기본 웹소켓 연결 클래스
│   │   └── manager.py          # 웹소켓 관리자
│   ├── parsers/                # 메시지 파싱 전용
│   │   ├── binance_parser.py
│   │   ├── bybit_parser.py
│   │   └── ...
│   └── exchanges/              # 거래소별 웹소켓 구현
│       ├── binance.py
│       ├── bybit.py
│       └── ...
├── orderbook/                  # 오더북 관련 코드
│   ├── base/                   # 기본 클래스
│   │   ├── orderbook.py        # 기본 오더북 클래스
│   │   └── manager.py          # 오더북 관리자
│   └── exchanges/              # 거래소별 오더북 구현
│       ├── binance.py
│       ├── bybit.py
│       └── ...
└── common/                     # 공통 유틸리티
    ├── models.py               # 데이터 모델 (dataclass)
    ├── constants.py            # 상수 정의
    └── utils.py                # 유틸리티 함수
```

## 4. 핵심 인터페이스 정의

### 4.1 메시지 파서 인터페이스

```python
class MessageParser:
    """웹소켓 메시지 파싱 인터페이스"""
    
    def parse(self, message: str) -> Optional[dict]:
        """
        웹소켓 메시지를 파싱하여 표준화된 형태로 반환
        
        Args:
            message: 원본 웹소켓 메시지
            
        Returns:
            파싱된 메시지 또는 None (파싱 불가능한 경우)
        """
        raise NotImplementedError
```

### 4.2 오더북 인터페이스

```python
class OrderBook:
    """오더북 데이터 관리 인터페이스"""
    
    def update(self, bids: List[List[float]], asks: List[List[float]], 
              timestamp: Optional[int] = None, sequence: Optional[int] = None,
              is_snapshot: bool = False) -> None:
        """
        오더북 데이터 업데이트
        
        Args:
            bids: 매수 호가 목록 [[가격, 수량], ...]
            asks: 매도 호가 목록 [[가격, 수량], ...]
            timestamp: 타임스탬프
            sequence: 시퀀스 번호
            is_snapshot: 스냅샷 여부
        """
        raise NotImplementedError
        
    def to_dict(self) -> dict:
        """오더북 데이터를 딕셔너리로 변환"""
        raise NotImplementedError
        
    async def send_to_queue(self) -> None:
        """큐로 데이터 전송"""
        raise NotImplementedError
```

### 4.3 오더북 매니저 인터페이스

```python
class OrderBookManager:
    """오더북 관리자 인터페이스"""
    
    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """
        오더북 업데이트 처리
        
        Args:
            symbol: 심볼
            data: 파싱된 데이터
            
        Returns:
            검증 결과
        """
        raise NotImplementedError
        
    async def fetch_snapshot(self, symbol: str) -> Optional[dict]:
        """
        스냅샷 요청
        
        Args:
            symbol: 심볼
            
        Returns:
            스냅샷 데이터 또는 None (요청 실패 시)
        """
        raise NotImplementedError
        
    def get_orderbook(self, symbol: str) -> Optional[OrderBook]:
        """심볼의 오더북 반환"""
        raise NotImplementedError
```

### 4.4 웹소켓 커넥터 인터페이스

```python
class WebSocketConnector:
    """웹소켓 연결 인터페이스"""
    
    async def connect(self) -> bool:
        """웹소켓 연결"""
        raise NotImplementedError
        
    async def subscribe(self, symbols: List[str]) -> None:
        """심볼 구독"""
        raise NotImplementedError
        
    async def parse_message(self, message: str) -> Optional[dict]:
        """메시지 파싱"""
        raise NotImplementedError
        
    async def handle_parsed_message(self, parsed: dict) -> None:
        """파싱된 메시지 처리"""
        raise NotImplementedError
```

## 5. 이벤트 기반 아키텍처 도입

메시지 수신 → 파싱 → 오더북 업데이트 → 데이터 전송의 흐름을 이벤트 기반으로 구현:

```python
class EventEmitter:
    """이벤트 발행자"""
    
    def __init__(self):
        self.listeners = defaultdict(list)
        
    def on(self, event: str, callback: Callable):
        """이벤트 리스너 등록"""
        self.listeners[event].append(callback)
        
    def emit(self, event: str, *args, **kwargs):
        """이벤트 발행"""
        for callback in self.listeners[event]:
            asyncio.create_task(callback(*args, **kwargs))
```

## 6. 팩토리 패턴 도입

```python
class ExchangeFactory:
    """거래소 구현체 생성 팩토리"""
    
    @staticmethod
    def create_websocket(exchange: str, settings: dict) -> WebSocketConnector:
        """거래소별 웹소켓 커넥터 생성"""
        if exchange == "binance":
            return BinanceWebSocketConnector(settings)
        elif exchange == "bybit":
            return BybitWebSocketConnector(settings)
        # ...
        
    @staticmethod
    def create_orderbook_manager(exchange: str, depth: int) -> OrderBookManager:
        """거래소별 오더북 매니저 생성"""
        if exchange == "binance":
            return BinanceOrderBookManager(depth)
        # ...
```

## 7. 구현 단계

### 7.1 1단계: 공통 모델 및 인터페이스 정의 (1주)
- 데이터 모델 정의 (OrderBook, OrderBookUpdate 등)
- 인터페이스 정의 (WebSocketConnector, OrderBookManager 등)
- 이벤트 시스템 구현

### 7.2 2단계: 파서 분리 (1주)
- 각 거래소별 파싱 로직을 별도 파일로 분리
- 표준화된 데이터 형식 정의

### 7.3 3단계: 기본 클래스 리팩토링 (1주)
- base_ws_connector.py → connector.py
- base_ob_v2.py → orderbook.py
- 템플릿 메서드 패턴 적용

### 7.4 4단계: 거래소별 구현체 리팩토링 (2주)
- 바이낸스 구현체 리팩토링
- 바이빗 구현체 리팩토링
- 업비트 구현체 리팩토링
- 빗썸 구현체 리팩토링

### 7.5 5단계: 팩토리 및 이벤트 시스템 통합 (1주)
- 팩토리 클래스 구현
- 이벤트 시스템 통합
- 의존성 주입 적용

### 7.6 6단계: 테스트 작성 (1주)
- 단위 테스트 작성
- 통합 테스트 작성
- 성능 테스트 작성

## 8. 예상 효과

1. **코드 가독성 향상**: 책임 분리로 각 컴포넌트의 역할이 명확해짐
2. **유지보수성 향상**: 중복 코드 제거로 변경 시 한 곳만 수정하면 됨
3. **확장성 향상**: 새로운 거래소 추가 시 인터페이스만 구현하면 됨
4. **테스트 용이성**: 각 컴포넌트를 독립적으로 테스트할 수 있음
5. **버그 감소**: 표준화된 인터페이스와 데이터 형식으로 오류 가능성 감소

## 9. 리스크 및 대응 방안

### 9.1 리스크
1. **기존 코드와의 호환성**: 리팩토링 과정에서 기존 코드와의 호환성 문제 발생 가능
2. **성능 저하**: 추상화 레이어 추가로 인한 성능 저하 가능성
3. **개발 일정 지연**: 리팩토링에 예상보다 많은 시간 소요 가능성

### 9.2 대응 방안
1. **점진적 리팩토링**: 한 번에 모든 것을 바꾸지 않고 단계적으로 진행
2. **성능 테스트**: 각 단계마다 성능 테스트를 통해 성능 저하 여부 확인
3. **우선순위 설정**: 중요도에 따라 리팩토링 우선순위 설정

## 10. 결론

현재 오더북 수집기 코드는 기능적으로는 문제가 없지만, 유지보수와 확장성 측면에서 개선이 필요합니다. 제안된 리팩토링을 통해 코드의 구조를 명확히 하고, 책임을 분리하며, 중복을 제거하여 더 유지보수하기 쉬운 코드베이스를 만들 수 있을 것입니다.

특히 파서 분리와 이벤트 기반 아키텍처 도입이 가장 중요한 개선점이라고 생각합니다. 이를 통해 코드의 결합도를 낮추고 테스트 용이성을 높일 수 있습니다.

리팩토링은 한 번에 모든 것을 바꾸기보다는 점진적으로 진행하는 것이 좋습니다. 먼저 공통 인터페이스와 모델을 정의하고, 한 거래소씩 새 구조로 마이그레이션하는 방식을 추천합니다. 