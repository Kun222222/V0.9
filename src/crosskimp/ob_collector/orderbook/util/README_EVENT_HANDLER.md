# 이벤트 핸들러 시스템 설명

## 개요
이 프로젝트의 이벤트 처리 시스템은 세 가지 주요 구성 요소로 되어 있습니다:

1. **EventBus** - 이벤트를 발행하고 구독하는 중앙 채널
2. **EventHandler** - 거래소 관련 이벤트를 처리하는 핸들러
3. **SystemEventManager** - 시스템 상태 및 메트릭을 관리하는 매니저

## 데이터 흐름

1. **거래소 커넥터(BaseWebsocketConnector 및 자식 클래스)** 
   - 웹소켓 연결 상태 변경, 메시지 수신, 오류 등의 이벤트가 발생
   - 각 이벤트를 EventHandler의 적절한 메서드로 전달

2. **EventHandler**
   - 전달받은 이벤트를 처리 (로깅, 알림 등)
   - 필요한 경우 SystemEventManager를 통해 메트릭 업데이트
   - EventBus를 통해 다른 컴포넌트로 이벤트 전파

3. **SystemEventManager**
   - 거래소별 메트릭과 상태 정보를 관리
   - 이벤트 데이터를 저장하고 필요시 요약 정보 생성

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
self.event_handler = EventHandlerFactory.get_handler(self.exchangename, self.settings)
self.event_bus = self.event_handler.event_bus
self.system_event_manager = self.event_handler.system_event_manager

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
    
    # 시스템 이벤트 발행
    await self.publish_system_event(
        EVENT_TYPES["CONNECTION_STATUS"],
        status=status,
        message=message,
        **kwargs
    )
    
    # 텔레그램 알림 전송
    await self.send_telegram_message(event_type, formatted_message)
    
    # 이벤트 버스로 이벤트 발행
    await self.event_bus.publish("connection_status_changed", event_data)
```

### 3. SystemEventManager

시스템 전체의 상태와 메트릭을 관리하는 싱글톤 클래스입니다.

```python
# 메트릭 기록
def record_metric(self, exchange_code, metric_name, **data):
    # 거래소가 초기화되지 않았으면 초기화
    if exchange_code not in self.metrics:
        self.initialize_exchange(exchange_code)
    
    # 메트릭 업데이트 로직
    self._update_metric(exchange_code, metric_name, value)

# 이벤트 발행
async def publish_system_event(self, event_type, exchange_code=None, **data):
    # 이벤트 발행 로직
    await self.event_bus.publish("system_event", event_data)
```

## 구현 이점

1. **관심사 분리**: 각 컴포넌트가 자신의 역할에만 집중
   - 커넥터: 네트워크 연결 관리
   - 이벤트 핸들러: 이벤트 처리 및 외부 알림
   - 시스템 이벤트 매니저: 메트릭 관리 및 모니터링

2. **중앙화된 이벤트 처리**: 모든 이벤트가 일관된 방식으로 처리됨
   - 동일한 타입의 이벤트는 항상 같은 방식으로 처리
   - 로깅, 알림, 메트릭 기록 등이 표준화됨

3. **유연한 확장성**: 새로운 거래소 추가가 용이함
   - 새 거래소는 BaseWebsocketConnector를 상속하고 필요한 메서드만 구현
   - 기존 이벤트 처리 인프라를 그대로 활용 가능