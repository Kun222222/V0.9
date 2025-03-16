# 크로스킴프 아비트라지 코드베이스 리팩토링 설계안

## 1. 현재 구조 분석

### 1.1 현재 디렉토리 구조
```
src/crosskimp/ob_collector/orderbook/
├── connection/           # 웹소켓 연결 관리
│   ├── base_ws_connector.py
│   ├── binance_f_cn.py
│   └── ...
├── parser/               # 메시지 파싱 (신규)
│   ├── base_parser.py
│   ├── binance_f_pa.py
│   └── ...
├── orderbook/            # 오더북 데이터 관리
│   ├── base_ob.py
│   ├── binance_f_ob.py
│   └── ...
└── websocket/            # 통합 레이어
    ├── base_ws_manager.py
    ├── binance_f_ws.py
    └── ...
```

### 1.2 현재 책임 구조
- **connection**: 웹소켓 연결 관리, 일부 파싱 로직 포함
- **parser**: 메시지 파싱 (일부만 구현됨)
- **orderbook**: 오더북 데이터 관리, 큐 전송
- **websocket**: 통합 레이어, 일부 파싱 로직 포함

### 1.3 주요 문제점
1. 파싱 로직이 여러 곳에 분산됨 (connection, websocket, parser)
2. 책임 경계가 불명확함
3. 의존성 관계가 복잡함
4. 큐 설정 로직이 여러 곳에 분산됨

## 2. 리팩토링 목표

1. 명확한 책임 분리
2. 의존성 단순화
3. 테스트 용이성 향상
4. 확장성 개선
5. 점진적 리팩토링으로 기능 중단 최소화

## 3. 단계별 리팩토링 계획

### 3.1 1단계: 파서 분리 완료 (현재 진행 중)

#### 작업 내용
1. 모든 거래소에 대한 파서 클래스 구현
2. 기존 파싱 로직을 파서 클래스로 이동
3. 웹소켓 클래스에서 파서 사용하도록 수정

#### 테스트 포인트
- 각 거래소별 파서 단위 테스트
- 파싱된 데이터 형식 일관성 확인
- 웹소켓 연결 및 메시지 수신 테스트

#### 구현 예시 (binance_f_pa.py)
```python
class BinanceFutureParser(BaseParser):
    def __init__(self):
        super().__init__(Exchange.BINANCE_FUTURE.value)
        # 설정 로드
        
    def parse_message(self, raw_message: str) -> Optional[Dict[str, Any]]:
        # 기존 binance_f_ws.py의 parse_message 로직 이동
        
    def create_subscribe_message(self, symbols: List[str]) -> Dict[str, Any]:
        # 구독 메시지 생성 로직
```

### 3.2 2단계: 웹소켓 클래스 정리

#### 작업 내용
1. 웹소켓 클래스에서 파싱 로직 제거
2. 파서와 오더북 매니저 연결 로직 명확화
3. 큐 설정 로직 통합

#### 테스트 포인트
- 웹소켓 연결 및 메시지 수신 테스트
- 파싱된 메시지가 오더북 매니저로 전달되는지 확인
- 오더북 데이터가 큐로 전송되는지 확인

#### 구현 예시 (binance_f_ws.py)
```python
class BinanceFutureWebsocket(BinanceFutureWebSocketConnector):
    def __init__(self, settings: dict):
        super().__init__(settings)
        # 오더북 매니저 초기화
        self.orderbook_manager = BinanceFutureOrderBookManagerV2(...)
        self.set_manager(self.orderbook_manager)
        
    def set_output_queue(self, queue: asyncio.Queue) -> None:
        # 부모 클래스 큐 설정
        super().set_output_queue(queue)
        # 오더북 매니저 큐 설정
        self.orderbook_manager.set_output_queue(queue)
        
    async def process_message(self, message: str) -> None:
        # 부모 클래스의 process_message 호출 (파싱 및 기본 처리)
        await super().process_message(message)
```

### 3.3 3단계: 커넥터 클래스 정리

#### 작업 내용
1. 커넥터 클래스에서 파싱 로직 제거
2. 메시지 콜백 메커니즘 개선
3. 오더북 매니저 참조 제거 (파서로 대체)

#### 테스트 포인트
- 웹소켓 연결 및 재연결 테스트
- 메시지 수신 및 콜백 호출 테스트
- 파싱된 메시지 처리 테스트

#### 구현 예시 (binance_f_cn.py)
```python
class BinanceFutureWebSocketConnector(BaseWebsocketConnector):
    def __init__(self, settings: dict):
        super().__init__(settings, EXCHANGE_CODE)
        # 파서 생성
        self.parser = BinanceFutureParser()
        # 메시지 콜백
        self.message_callback = None
        
    async def process_message(self, message: str) -> None:
        # 파서를 사용하여 메시지 파싱
        parsed_data = self.parser.parse_message(message)
        
        if parsed_data and self.message_callback:
            # 파싱된 메시지 콜백 호출
            await self.message_callback(parsed_data)
```

### 3.4 4단계: 이벤트 기반 아키텍처 도입 (선택적)

#### 작업 내용
1. 이벤트 버스 구현
2. 컴포넌트 간 통신을 이벤트 기반으로 변경
3. 콜백 대신 이벤트 구독 사용

#### 테스트 포인트
- 이벤트 발행 및 구독 테스트
- 전체 메시지 흐름 테스트
- 성능 테스트

#### 구현 예시 (event_bus.py)
```python
class EventBus:
    def __init__(self):
        self.subscribers = defaultdict(list)
        
    def subscribe(self, event_type: str, callback: Callable):
        self.subscribers[event_type].append(callback)
        
    async def publish(self, event_type: str, data: Any):
        for callback in self.subscribers[event_type]:
            await callback(data)
```

## 4. 단계별 테스트 전략

### 4.1 단위 테스트
- 각 파서 클래스의 파싱 로직 테스트
- 커넥터 클래스의 연결 관리 테스트
- 오더북 매니저의 데이터 처리 테스트

### 4.2 통합 테스트
- 웹소켓 연결부터 오더북 데이터 처리까지 전체 흐름 테스트
- 큐 처리 및 데이터 전송 테스트
- 재연결 및 오류 복구 테스트

### 4.3 성능 테스트
- 메시지 처리 속도 측정
- 메모리 사용량 모니터링
- 대량 메시지 처리 시 안정성 테스트

## 5. 단계별 구현 계획

### 5.1 1단계: 파서 분리 완료 (1주)

#### 작업 항목
1. 바이낸스 선물 파서 구현 완료 (binance_f_pa.py) ✅
2. 바이낸스 현물 파서 구현 (binance_s_pa.py) ✅
3. 바이빗 선물 파서 구현 (bybit_f_pa.py) ✅
4. 바이빗 현물 파서 구현 (bybit_s_pa.py) ✅
5. 업비트 파서 구현 (upbit_s_pa.py) ✅
6. 빗썸 파서 구현 (bithumb_s_pa.py) ✅

#### 테스트 계획
- 각 파서별 단위 테스트 작성
- 실제 메시지 샘플로 파싱 테스트
- 파싱된 데이터 형식 검증

### 5.2 2단계: 웹소켓 클래스 정리 (1주)

#### 작업 항목
1. 바이낸스 선물 웹소켓 클래스 수정 (binance_f_ws.py)
2. 바이낸스 현물 웹소켓 클래스 수정 (binance_s_ws.py)
3. 바이빗 선물/현물 웹소켓 클래스 수정 (bybit_f_ws.py, bybit_s_ws.py)
4. 업비트 웹소켓 클래스 수정 (upbit_s_ws.py)
5. 빗썸 웹소켓 클래스 수정 (bithumb_s_ws.py)

#### 테스트 계획
- 각 거래소별 웹소켓 연결 테스트
- 메시지 수신 및 처리 테스트
- 큐 설정 및 데이터 전송 테스트

### 5.3 3단계: 커넥터 클래스 정리 (1주)

#### 작업 항목
1. 바이낸스 선물 커넥터 클래스 수정 (binance_f_cn.py)
2. 바이낸스 현물 커넥터 클래스 수정 (binance_s_cn.py)
3. 바이빗 선물/현물 커넥터 클래스 수정 (bybit_f_cn.py, bybit_s_cn.py)
4. 업비트 커넥터 클래스 수정 (upbit_s_cn.py)
5. 빗썸 커넥터 클래스 수정 (bithumb_s_cn.py)

#### 테스트 계획
- 각 거래소별 연결 관리 테스트
- 재연결 로직 테스트
- 메시지 콜백 메커니즘 테스트

### 5.4 4단계: 이벤트 기반 아키텍처 도입 (2주, 선택적)

#### 작업 항목
1. 이벤트 버스 구현 (event_bus.py)
2. 커넥터 클래스에 이벤트 발행 추가
3. 파서 클래스에 이벤트 구독/발행 추가
4. 오더북 매니저에 이벤트 구독 추가
5. 웹소켓 매니저에 이벤트 기반 통합 로직 추가

#### 테스트 계획
- 이벤트 발행/구독 메커니즘 테스트
- 전체 메시지 흐름 테스트
- 성능 및 안정성 테스트

## 6. 점진적 배포 전략

### 6.1 각 단계별 배포 계획
1. 파서 분리 완료 후 테스트 환경에서 검증
2. 웹소켓 클래스 정리 후 테스트 환경에서 검증
3. 커넥터 클래스 정리 후 테스트 환경에서 검증
4. 이벤트 기반 아키텍처는 별도 브랜치에서 개발 후 충분한 테스트 후 배포

### 6.2 롤백 계획
- 각 단계별 배포 전 코드 스냅샷 저장
- 문제 발생 시 이전 버전으로 즉시 롤백
- 로그 및 모니터링 강화로 문제 조기 발견

### 6.3 모니터링 계획
- 각 거래소별 연결 상태 모니터링
- 메시지 처리량 및 지연 시간 모니터링
- 오류 발생 빈도 및 유형 모니터링

## 7. 리팩토링 후 기대 효과

### 7.1 코드 품질 향상
- 명확한 책임 분리로 가독성 향상
- 단일 책임 원칙 준수로 유지보수성 향상
- 의존성 단순화로 코드 이해도 향상

### 7.2 개발 생산성 향상
- 독립적인 컴포넌트로 병렬 개발 가능
- 테스트 용이성 향상으로 버그 감소
- 명확한 인터페이스로 협업 효율 향상

### 7.3 확장성 개선
- 새로운 거래소 추가 용이
- 기능 확장 시 영향 범위 최소화
- 성능 최적화 용이

## 8. 리스크 및 대응 방안

### 8.1 기능 중단 리스크
- **리스크**: 리팩토링 중 기존 기능 중단
- **대응**: 점진적 리팩토링 및 철저한 테스트

### 8.2 성능 저하 리스크
- **리스크**: 구조 변경으로 인한 성능 저하
- **대응**: 각 단계별 성능 테스트 및 최적화

### 8.3 복잡성 증가 리스크
- **리스크**: 과도한 추상화로 인한 복잡성 증가
- **대응**: 실용적인 수준의 추상화 유지 및 문서화

## 9. 결론

이 리팩토링 계획은 크로스킴프 아비트라지 코드베이스의 구조를 개선하여 유지보수성, 확장성, 테스트 용이성을 향상시키는 것을 목표로 합니다. 점진적인 접근 방식을 통해 기능 중단을 최소화하면서 코드 품질을 향상시킬 수 있습니다.

각 단계는 독립적으로 구현 및 테스트가 가능하며, 필요에 따라 일부 단계만 선택적으로 진행할 수도 있습니다. 특히 이벤트 기반 아키텍처는 선택적으로 도입할 수 있는 장기적인 개선 방안입니다.

리팩토링 과정에서 발생할 수 있는 리스크를 최소화하기 위해 철저한 테스트와 모니터링이 필요하며, 문제 발생 시 신속하게 대응할 수 있는 롤백 계획도 마련되어 있습니다.
