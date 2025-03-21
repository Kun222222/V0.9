# SystemEventManager에서 EventHandler로 이동하기

이 문서는 `SystemEventManager`를 `EventHandler`로 대체하는 작업 가이드입니다.

## 개요

`SystemEventManager` 클래스는 거래소 이벤트 처리와 메트릭 관리를 담당했지만, 이제 이 책임은 `EventHandler` 클래스로 이동되었습니다. 모든 새로운 코드는 `EventHandler`를 사용해야 합니다.

## 주요 변경 사항

1. `SystemEventManager`는 전체 시스템의 모든 거래소를 한 인스턴스에서 관리했지만, `EventHandler`는 거래소별로 별도의 인스턴스를 가집니다.
2. `SystemEventManager`는 중앙 집중식 관리 방식을 사용했지만, `EventHandler`는 분산 관리 방식을 사용하여 결합도를 낮추었습니다.
3. `SystemEventManager`는 싱글톤 패턴을 사용했지만, `EventHandler`는 팩토리 패턴과 유사한 거래소별 싱글톤을 사용합니다.

## 코드 마이그레이션 가이드

### 1. 인스턴스 가져오기

**기존 코드:**
```python
from crosskimp.ob_collector.orderbook.util.event_manager import SystemEventManager

# 인스턴스 가져오기
system_event_manager = SystemEventManager.get_instance()
```

**새로운 코드:**
```python
from crosskimp.ob_collector.orderbook.util.event_handler import EventHandler

# 인스턴스 가져오기 (거래소별로 다른 인스턴스 사용)
event_handler = EventHandler.get_instance(exchange_code, settings)
```

### 2. 이벤트 발행

**기존 코드:**
```python
await system_event_manager.publish_system_event(
    EVENT_TYPES["ERROR_EVENT"],
    exchange_code="upbit",
    error_type="connection_error",
    message="연결 실패"
)
```

**새로운 코드:**
```python
await event_handler.publish_system_event(
    EVENT_TYPES["ERROR_EVENT"],
    error_type="connection_error",
    message="연결 실패"
)
```

또는 특정 이벤트 유형에 맞는 메서드 사용:
```python
await event_handler.handle_error(
    error_type="connection_error",
    message="연결 실패",
    severity="error"
)
```

### 3. 메트릭 및 상태 조회

**기존 코드:**
```python
metrics = system_event_manager.get_metrics(exchange_code)
status = system_event_manager.get_status(exchange_code)
errors = system_event_manager.get_errors(exchange_code)
```

**새로운 코드:**
```python
metrics = event_handler.get_metrics()
status = event_handler.get_status()
errors = event_handler.get_errors()
```

## 마이그레이션 계획

1. 모든 새로운 코드에서는 바로 `EventHandler`를 사용합니다.
2. 기존 코드는 단계적으로 `EventHandler`로 마이그레이션합니다.
3. 마이그레이션 완료 후 `SystemEventManager` 클래스와 `event_manager.py` 파일은 완전히 제거되었습니다.

## 이점

1. **결합도 감소**: 각 거래소별로 독립적인 이벤트 처리로 결합도 감소
2. **명확한 책임**: 거래소별 EventHandler를 통해 책임 분산
3. **중복 코드 제거**: 여러 클래스에 분산된 이벤트 처리 로직을 중앙화
4. **일관된 이벤트 처리**: 모든 컴포넌트에서 동일한 이벤트 처리 패턴 사용 