# 이벤트 핸들러 시스템 설명

## 개요
이 프로젝트의 이벤트 처리 시스템은 세 가지 주요 구성 요소로 되어 있습니다:

1. **EventBus** - 이벤트를 발행하고 구독하는 중앙 채널
2. **EventHandler** - 거래소 관련 이벤트를 처리하는 핸들러
3. **MetricsManager** - 시스템 메트릭을 관리하는 중앙 매니저

## 데이터 흐름

1. **거래소 커넥터(BaseWebsocketConnector 및 자식 클래스)** 
   - 웹소켓 연결 상태 변경, 메시지 수신, 오류 등의 이벤트가 발생
   - 각 이벤트를 EventHandler의 적절한 메서드로 전달

2. **EventHandler**
   - 전달받은 이벤트를 처리 (로깅, 알림 등)
   - 필요한 경우 MetricsManager를 통해 메트릭 업데이트
   - EventBus를 통해 다른 컴포넌트로 이벤트 전파

3. **MetricsManager**
   - 거래소별 메트릭과 상태 정보를 중앙에서 관리
   - 이벤트 데이터를 저장하고 주기적으로 요약 정보 생성

## 주요 이벤트 유형

- **연결 상태 이벤트** - 거래소 웹소켓 연결/해제 및 재연결
- **메시지 수신 이벤트** - 스냅샷, 델타 등 데이터 수신
- **오류 이벤트** - 다양한 오류 상황
- **메트릭 업데이트** - 성능 지표 업데이트

## 사용 예시

```python
# BaseWebsocketConnector 클래스에서 연결 상태 변경 시
self.is_connected = True  # 상태 변경

# 이벤트 핸들러로 상태 변경을 알림
await self.event_handler.handle_connection_status(
    status="connected",
    message="웹소켓 연결 성공"
)

# 메시지 수신 처리
await self.event_handler.handle_message_received(
    message_type="snapshot",
    size=len(message_data)
)

# 오류 처리
await self.event_handler.handle_error(
    error_type="connection_error",
    message="연결 실패",
    severity="error"
)
```

## 상세 구현

### 1. BaseWebsocketConnector

거래소 웹소켓 연결을 담당하는 기본 클래스로, 모든 거래소 커넥터의 부모 클래스입니다.

```python
# 초기화 단계에서 이벤트 핸들러를 가져옴
self.event_handler = EventHandler.get_instance(self.exchange_code, self.settings)
self.event_bus = EventBus.get_instance()
self.metrics_manager = MetricsManager.get_instance()

# 연결 상태 변경 시
self.is_connected = True/False  # 이 setter가 이벤트 발생시킴

# 로깅은 이벤트 핸들러를 통해 수행
self.log_info("메시지")  # 내부적으로 self.event_handler.log_info() 호출

# 이벤트 처리도 이벤트 핸들러에 위임
await self.handle_error("오류 타입", "오류 메시지")  # 내부적으로 event_handler.handle_error() 호출
```

### 2. EventHandler

각 거래소별로 하나의 인스턴스가 생성되며, 이벤트 처리 로직을 중앙화합니다.

```python
# 이벤트 처리 예시
async def handle_connection_status(self, status, message, **kwargs):
    # 로깅
    self.log_info(message)
    
    # 텔레그램 알림 전송
    await self.send_telegram_message(event_type, formatted_message)
    
    # 이벤트 버스로 이벤트 발행
    await self.event_bus.publish(EVENT_TYPES["CONNECTION_STATUS"], event_data)
```

### 3. MetricsManager

시스템 전체의 메트릭을 관리하는 싱글톤 클래스입니다.

```python
# 거래소 초기화
def initialize_exchange(self, exchange_code):
    # 거래소 메트릭 초기화
    if exchange_code not in self.metrics:
        self.metrics[exchange_code] = {
            "message_count": 0,
            "error_count": 0,
            # 기타 초기 메트릭...
        }

# 메트릭 업데이트
def update_metrics(self, exchange_code, metric_name, value=1.0, **kwargs):
    # 메트릭 업데이트
    self._update_metric(exchange_code, metric_name, value)
    
    # 이벤트 발행
    asyncio.create_task(
        self.publish_metric_event(exchange_code, metric_name, value, **kwargs)
    )

# 메트릭 조회
def get_metrics(self, exchange_code=None):
    # 특정 거래소 또는 모든 거래소의 메트릭 반환
    if exchange_code:
        return self.metrics.get(exchange_code, {})
    else:
        return self.metrics
```

## 구현 이점

1. **관심사 분리**: 각 컴포넌트가 자신의 역할에만 집중
   - 커넥터: 네트워크 연결 관리
   - 이벤트 핸들러: 이벤트 처리 및 외부 알림
   - 메트릭 매니저: 메트릭 관리 및 모니터링

2. **중앙화된 이벤트 처리**: 모든 이벤트가 일관된 방식으로 처리됨
   - 동일한 타입의 이벤트는 항상 같은 방식으로 처리
   - 로깅, 알림, 메트릭 기록 등이 표준화됨

3. **유연한 확장성**: 새로운 거래소 추가가 용이함
   - 새 거래소는 BaseWebsocketConnector를 상속하고 필요한 메서드만 구현
   - 기존 이벤트 처리 인프라를 그대로 활용 가능