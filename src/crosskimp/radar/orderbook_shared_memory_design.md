# 오더북 데이터 공유 시스템 설계안 (FlatBuffers + 공유 메모리)

## 현재 시스템 분석

현재 시스템은 다음과 같은 구조로 되어 있습니다:

1. **오더북 데이터 수집**: 여러 거래소(바이낸스, 바이빗, 업비트, 빗썸 등)의 웹소켓을 통해 오더북 데이터를 수집
2. **델타 업데이트 처리**: 각 거래소별 특화된 오더북 매니저가 델타 업데이트를 처리하여 오더북 상태 유지
3. **큐 기반 데이터 전달**: 업데이트된 오더북 데이터는 asyncio.Queue를 통해 다른 컴포넌트로 전달
4. **메트릭 수집**: 다양한 메트릭을 수집하여 시스템 상태 모니터링

## 설계 목표

1. 파이썬에서 처리된 오더북 데이터를 FlatBuffers로 직렬화
2. 직렬화된 데이터를 공유 메모리에 저장
3. C++ 프로그램에서 공유 메모리의 데이터를 읽어 계산 수행
4. 기존 시스템의 안정성과 성능을 유지하면서 확장

## 시스템 구성 요소

### 1. 파이썬 측 (데이터 생산자)

#### 1.1 FlatBuffers 스키마 정의 (`orderbook.fbs`)
```
namespace OrderBookData;

table PriceLevel {
  price:double;
  quantity:double;
}

table OrderBook {
  exchange_name:string;
  symbol:string;
  timestamp:long;
  sequence:long;
  bids:[PriceLevel];
  asks:[PriceLevel];
}

root_type OrderBook;
```

#### 1.2 공유 메모리 관리자 (`shared_memory_manager.py`)
- 공유 메모리 세그먼트 생성 및 관리
- 메모리 매핑 헤더 관리 (데이터 크기, 타임스탬프, 상태 플래그)
- 세마포어를 통한 동기화 처리
- 더블 버퍼링 구현 (읽기/쓰기 충돌 방지)

#### 1.3 FlatBuffers 직렬화 모듈 (`flatbuffers_serializer.py`)
- 오더북 데이터를 FlatBuffers 형식으로 변환
- 최적화된 메모리 레이아웃 구성
- 제로 카피 전략 적용

#### 1.4 C++ 인터페이스 (`cpp_interface.py`)
- 파이썬과 C++ 간의 인터페이스 정의
- 공유 메모리 설정 및 초기화
- 동기화 메커니즘 구현

### 2. C++ 측 (데이터 소비자)

#### 2.1 공유 메모리 접근 모듈 (`shared_memory_reader.cpp`)
- 공유 메모리 세그먼트 접근
- 메모리 매핑 헤더 해석
- 세마포어를 통한 동기화 처리
- 더블 버퍼링 처리

#### 2.2 FlatBuffers 역직렬화 모듈 (`orderbook_data.cpp`)
- FlatBuffers 데이터를 C++ 객체로 변환
- 제로 카피 접근 구현
- 데이터 무결성 검증

#### 2.3 계산 엔진 (`calculation_engine.cpp`)
- 오더북 데이터 기반 계산 수행
- 최적화된 알고리즘 구현
- 결과 저장 및 출력

#### 2.4 메인 프로그램 (`main.cpp`)
- 프로그램 초기화 및 설정
- 이벤트 루프 관리
- 오류 처리 및 복구

## 구현 계획

### 1. 파이썬 측 구현

#### 1.1 FlatBuffers 스키마 및 코드 생성
- FlatBuffers 스키마 정의
- flatc 컴파일러를 사용하여 파이썬 및 C++ 코드 생성

#### 1.2 공유 메모리 관리자 구현
- `shared_memory` 라이브러리 활용
- 메모리 세그먼트 생성 및 관리 로직 구현
- 동기화 메커니즘 구현

#### 1.3 오더북 데이터 직렬화 모듈 구현
- 기존 오더북 클래스에 FlatBuffers 직렬화 메서드 추가
- 최적화된 직렬화 로직 구현

#### 1.4 기존 코드 수정
- `OrderBookV2.send_to_queue` 메서드 수정하여 공유 메모리에도 데이터 저장
- 메트릭 수집 로직에 공유 메모리 관련 메트릭 추가

### 2. C++ 측 구현

#### 2.1 공유 메모리 접근 모듈 구현
- 공유 메모리 세그먼트 접근 로직 구현
- 동기화 메커니즘 구현

#### 2.2 오더북 데이터 처리 모듈 구현
- FlatBuffers 데이터 접근 및 처리 로직 구현
- 데이터 무결성 검증 로직 구현

#### 2.3 계산 엔진 구현
- 오더북 데이터 기반 계산 알고리즘 구현
- 최적화된 계산 로직 구현

## 파일 구조

### 파이썬 측 (src/crosskimp/ob_collector/cpp)
```
cpp/
├── flatbuffers/
│   ├── orderbook.fbs
│   ├── orderbook_generated.py
│   └── README.md
├── shared_memory_manager.py
├── flatbuffers_serializer.py
├── cpp_interface.py
└── __init__.py
```

### C++ 측 (src/crosskimp/radar)
```
radar/
├── include/
│   ├── flatbuffers/
│   │   └── orderbook_generated.h
│   ├── shared_memory_reader.h
│   ├── orderbook_data.h
│   └── calculation_engine.h
├── src/
│   ├── shared_memory_reader.cpp
│   ├── orderbook_data.cpp
│   ├── calculation_engine.cpp
│   └── main.cpp
├── CMakeLists.txt
└── README.md
```

## 데이터 흐름

1. 웹소켓을 통해 오더북 델타 업데이트 수신
2. 각 거래소별 오더북 매니저가 델타 업데이트 처리
3. 업데이트된 오더북 데이터를 FlatBuffers로 직렬화
4. 직렬화된 데이터를 공유 메모리에 기록
5. 세마포어를 통해 C++ 프로그램에 데이터 가용성 알림
6. C++ 프로그램이 공유 메모리에서 데이터 읽기
7. FlatBuffers 데이터를 C++ 객체로 변환 (제로 카피)
8. 변환된 데이터로 계산 수행
9. 계산 결과 저장 또는 출력

## 동기화 메커니즘

1. **세마포어**: 공유 메모리 접근 제어
2. **메모리 매핑 헤더**: 데이터 크기, 타임스탬프, 상태 플래그 등 메타데이터 저장
3. **더블 버퍼링**: 읽기/쓰기 충돌 방지를 위한 두 개의 버퍼 교대 사용
4. **원자적 업데이트**: 데이터 일관성을 위한 원자적 업데이트 구현

## 오류 처리 및 복구 전략

1. **데이터 검증**: 공유 메모리 데이터의 무결성 검증
2. **타임아웃 처리**: 동기화 작업 타임아웃 감지 및 복구
3. **재연결 메커니즘**: 공유 메모리 연결 끊김 시 자동 재연결
4. **로깅 및 모니터링**: 시스템 상태 및 오류 추적

## 성능 최적화 전략

1. **제로 카피 전략**: FlatBuffers의 제로 카피 특성 활용
2. **메모리 레이아웃 최적화**: 캐시 효율성을 위한 데이터 구조 설계
3. **배치 처리**: 여러 업데이트를 묶어서 처리하여 오버헤드 감소
4. **비동기 처리**: 데이터 생산과 소비의 병렬 처리

## 구현 단계

1. **FlatBuffers 스키마 정의 및 코드 생성**
2. **공유 메모리 관리자 구현**
3. **파이썬 측 직렬화 모듈 구현**
4. **C++ 측 공유 메모리 접근 모듈 구현**
5. **C++ 측 데이터 처리 및 계산 엔진 구현**
6. **통합 테스트 및 성능 최적화**
7. **문서화 및 배포**

## 오더북 데이터 형식

현재 시스템에서 오더북 데이터는 다음과 같은 형태로 큐에 저장되고 있습니다:

```python
def to_dict(self) -> dict:
    """오더북 데이터를 딕셔너리로 변환"""
    return {
        "exchangename": self.exchangename,
        "symbol": self.symbol,
        "bids": self.bids[:10],  # 상위 10개만
        "asks": self.asks[:10],  # 상위 10개만
        "timestamp": self.last_update_time or int(time.time() * 1000),
        "sequence": self.last_update_id
    }
```

이 데이터 구조를 기반으로 FlatBuffers 스키마를 설계하고, 공유 메모리 구현을 할 수 있습니다. 