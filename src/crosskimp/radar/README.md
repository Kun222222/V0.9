# CrossKimp Radar

오더북 데이터를 기반으로 차익거래 기회를 계산하는 C++ 프로그램입니다.

## 개요

이 프로그램은 파이썬에서 처리된 오더북 데이터를 FlatBuffers와 공유 메모리를 통해 전달받아 차익거래 기회를 계산합니다.

## 기능

- 공유 메모리를 통한 오더북 데이터 수신
- FlatBuffers를 사용한 효율적인 데이터 처리
- 거래소 간 차익거래 기회 계산
- 실시간 메트릭 모니터링

## 빌드 방법

### 필수 요구사항

- CMake 3.10 이상
- C++17 지원 컴파일러
- FlatBuffers 라이브러리
- POSIX 공유 메모리 및 세마포어 지원

### 빌드 명령어

```bash
# 빌드 디렉토리 생성
mkdir -p build
cd build

# CMake 구성
cmake ..

# 빌드
make

# 설치 (선택 사항)
make install
```

## 실행 방법

```bash
# 기본 실행
./radar

# 도움말 표시
./radar --help
```

## 구조

- `include/`: 헤더 파일
  - `shared_memory_reader.h`: 공유 메모리 접근 모듈
  - `orderbook_data.h`: 오더북 데이터 처리 모듈
  - `calculation_engine.h`: 계산 엔진 모듈
  - `flatbuffers/`: FlatBuffers 생성 헤더

- `src/`: 소스 파일
  - `main.cpp`: 메인 프로그램
  - `shared_memory_reader.cpp`: 공유 메모리 접근 모듈 구현
  - `orderbook_data.cpp`: 오더북 데이터 처리 모듈 구현
  - `calculation_engine.cpp`: 계산 엔진 모듈 구현

## 사용 예시

1. 파이썬 오더북 수집기 실행
2. C++ 레이더 프로그램 실행
3. 차익거래 기회 모니터링

## 라이선스

이 프로젝트는 내부 사용 목적으로 개발되었습니다. 