# 오더북 이벤트 시스템 마이그레이션 가이드

이 문서는 오더북 컴포넌트의 이벤트 시스템을 기존 독립적인 `EventBus`에서 통합 이벤트 버스로 마이그레이션하는 방법을 설명합니다.

## 마이그레이션 단계

### 1. 오더북 전용 이벤트 타입 정의

오더북 컴포넌트에서 사용하는 이벤트 타입을 `crosskimp/common/events/domains/orderbook.py` 파일에 정의했습니다. 이 파일에는 다음이 포함됩니다:

- `OrderbookEventTypes`: 오더북 관련 이벤트 타입 상수
- `EVENT_TYPE_MAPPING`: 기존 이벤트 타입에서 새 이벤트 타입으로의 매핑

### 2. 이벤트 어댑터 구현

`crosskimp/ob_collector/orderbook/util/event_adapter.py` 파일에 이벤트 어댑터를 구현했습니다. 이 어댑터는:

- 기존 `EventBus`와 동일한 인터페이스 제공
- 내부적으로는 새 통합 이벤트 시스템 사용
- 이벤트 타입 매핑 및 변환 기능 포함
- 정적 `get_event_adapter()` 함수를 통해 접근 가능

### 3. 마이그레이션 단계별 적용 방법

1. **기존 코드에서 이벤트 버스 임포트 변경**:
   ```python
   # 기존 코드
   from crosskimp.ob_collector.orderbook.util.event_bus import get_event_bus
   
   # 변경 후
   from crosskimp.ob_collector.orderbook.util.event_adapter import get_event_adapter
   ```

2. **함수 호출 부분 변경**:
   ```python
   # 기존 코드
   event_bus = get_event_bus()
   
   # 변경 후
   event_bus = get_event_adapter()
   ```

3. **이벤트 타입 사용 방법**:
   ```python
   # 기존 코드
   from crosskimp.ob_collector.orderbook.util.event_bus import EVENT_TYPES
   
   # 새로운 방식 (권장)
   from crosskimp.common.events.domains.orderbook import OrderbookEventTypes
   
   # 새로운 방식으로 이벤트 발행
   await event_bus.publish(OrderbookEventTypes.CONNECTION_STATUS, data)
   ```

## 어댑터 사용 예시

```python
from crosskimp.ob_collector.orderbook.util.event_adapter import get_event_adapter
from crosskimp.common.events.domains.orderbook import OrderbookEventTypes

# 어댑터 가져오기
adapter = get_event_adapter()

# 이벤트 구독
async def my_callback(data):
    print(f"이벤트 수신: {data}")

await adapter.subscribe(OrderbookEventTypes.CONNECTION_STATUS, my_callback)

# 이벤트 발행
await adapter.publish(OrderbookEventTypes.CONNECTION_STATUS, {
    "exchange": "binance",
    "status": "connected",
    "timestamp": time.time()
})

# 구독 해제
adapter.unsubscribe(OrderbookEventTypes.CONNECTION_STATUS, my_callback)
```

## 테스트 방법

이벤트 어댑터가 제대로 작동하는지 확인하려면 테스트 코드를 실행하세요:

```bash
python -m src.crosskimp.ob_collector.orderbook.util.event_adapter_test
```

## 주의 사항

1. 이벤트 버스는 컴포넌트 시작 시 자동으로 초기화됩니다.
2. 이벤트 콜백은 비동기 함수(async def)로 정의해야 합니다.
3. 기존 EVENT_TYPES는 더 이상 사용하지 말고 OrderbookEventTypes 클래스의 상수를 사용하세요.

## 마이그레이션 이점

- 컴포넌트 간 이벤트 통신 일관성 확보
- 중복 코드 제거 및 유지보수성 향상
- 시스템 전체 이벤트 모니터링 개선
- 이벤트 표준화 및 구조화 

## 마이그레이션 진행 상황 (2023-05-26)

현재까지 수정된 파일:
- `src/crosskimp/common/events/domains/orderbook.py` (새로 생성)
- `src/crosskimp/common/events/domains/status.py` (새로 생성)
- `src/crosskimp/common/events/domains/telegram.py` (새로 생성)
- `src/crosskimp/ob_collector/orderbook/util/event_adapter.py` (새로 생성)
- `src/crosskimp/ob_collector/orderbook/order_manager.py`
- `src/crosskimp/ob_collector/orderbook/util/event_handler.py`
- `src/crosskimp/ob_collector/orderbook/util/event_adapter_test.py`
- `src/crosskimp/system_manager/error_manager.py`
- `src/crosskimp/system_manager/metric_manager.py`
- `src/crosskimp/system_manager/status_manager.py` (2023-05-26 수정)
- `src/crosskimp/telegrambot/bot_manager.py` (2023-05-26 수정)
- `src/crosskimp/telegrambot/notification.py` (2023-05-26 수정)
- `src/crosskimp/telegrambot/monitoring.py` (2023-05-26 수정)
- 연결 관련 파일 (`binance_s_cn.py`, `bithumb_s_cn.py` 등)

마이그레이션 완료된 부분:
- 오더북 수집기의 이벤트 시스템 → 공통 이벤트 시스템 통합
- 상태 관리자의 이벤트 시스템 → 공통 이벤트 시스템 통합 
- 오류 관리자의 이벤트 시스템 → 공통 이벤트 시스템 통합
- 메트릭 관리자의 이벤트 시스템 → 공통 이벤트 시스템 통합
- 텔레그램 봇의 이벤트 시스템 → 공통 이벤트 시스템 통합
- 이벤트 타입 상수화 및 네이밍 표준화

다음 단계:
1. 시스템 통합 테스트 실행
2. 문제점 확인 및 수정
3. 레거시 이벤트 버스 파일(event_bus.py) 제거

추가 마이그레이션 작업은 시스템 부하가 적은 시간에 진행하는 것이 좋습니다. 

## 마이그레이션 체크리스트

1. [X] 중앙 이벤트 시스템 구현
2. [X] 이벤트 타입 상수 모듈 구현
3. [X] 어댑터 레이어 구현
4. [X] 오더북 수집기 이벤트 시스템 마이그레이션
5. [X] 상태 관리자 이벤트 시스템 마이그레이션
6. [X] 오류 관리자 이벤트 시스템 마이그레이션
7. [X] 메트릭 관리자 이벤트 시스템 마이그레이션
8. [X] 텔레그램 봇 이벤트 시스템 마이그레이션
9. [ ] 기존 이벤트 버스 파일 제거 (마이그레이션 완료 후)
10. [ ] 최종 테스트 및 문서화 