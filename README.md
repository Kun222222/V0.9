# CrossKimpArbitrage

크로스킴프 차익거래 봇 프로젝트

## 로깅 시스템

로깅 시스템은 다음과 같은 기능을 제공합니다:

1. **통합 로거 (Unified Logger)**
   - 일반적인 애플리케이션 로그를 기록
   - `get_unified_logger()` 함수로 접근

2. **큐 로거 (Queue Logger)**
   - 데이터 큐 관련 로그를 기록
   - `get_queue_logger()` 함수로 접근

3. **Raw 로거 (Raw Logger)**
   - 거래소별 원시 데이터 로깅
   - `get_raw_logger(exchange_name)` 함수로 접근

4. **로그 파일 관리**
   - 자동 로그 로테이션 (파일 크기 제한)
   - 오래된 로그 파일 자동 압축 및 정리
   - 에러 로그 분리 저장

5. **로깅 시스템 초기화 및 종료**
   - `initialize_logging()`: 로깅 시스템 초기화
   - `shutdown_logging()`: 로깅 시스템 안전 종료

### 사용 예시

```python
from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, get_raw_logger

# 통합 로거 사용
logger = get_unified_logger()
logger.info("정보 메시지")
logger.error("에러 메시지")

# Raw 로거 사용 (거래소별)
raw_logger = get_raw_logger("binance")
raw_logger.debug("raw: 바이낸스 원시 데이터")
```

## 설정 관리 