cmake_minimum_required(VERSION 3.10)
project(CrossKimpRadar VERSION 0.1.0 LANGUAGES CXX)

# C++17 표준 사용
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 빌드 타입 설정
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# 컴파일러 옵션 설정
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

# 소스 파일 목록
set(SOURCES
    src/main.cpp
    src/shared_memory_reader.cpp
    src/orderbook_data.cpp
    src/calculation_engine.cpp
)

# 헤더 파일 디렉토리
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# 실행 파일 생성
add_executable(radar ${SOURCES})

# 스레드 라이브러리 링크
find_package(Threads REQUIRED)
target_link_libraries(radar PRIVATE Threads::Threads)

# rt 라이브러리 링크 (공유 메모리 및 세마포어 사용)
if(UNIX AND NOT APPLE)
    target_link_libraries(radar PRIVATE rt)
endif()

# 설치 설정
install(TARGETS radar DESTINATION bin)

# 빌드 정보 출력
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "C++ flags: ${CMAKE_CXX_FLAGS}")
message(STATUS "C++ flags (${CMAKE_BUILD_TYPE}): ${CMAKE_CXX_FLAGS_${CMAKE_BUILD_TYPE}}")

# FlatBuffers 라이브러리 경로 설정
set(FLATBUFFERS_DIR "${CMAKE_SOURCE_DIR}/../../..")
include_directories(${FLATBUFFERS_DIR})

# 헤더 파일 설정
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/include/flatbuffers
) 