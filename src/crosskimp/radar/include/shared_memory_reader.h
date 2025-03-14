/**
 * @file shared_memory_reader.h
 * @brief 공유 메모리 접근 모듈 헤더
 */

#ifndef SHARED_MEMORY_READER_H
#define SHARED_MEMORY_READER_H

#include <string>
#include <memory>
#include <functional>
#include <chrono>
#include <vector>
#include <atomic>
#include <mutex>
#include <thread>
#include <semaphore.h>

// 공유 메모리 관련 상수
constexpr const char* SHARED_MEMORY_NAME = "orderbook_shared_memory";  // macOS에서는 앞에 슬래시(/)를 제거
constexpr const char* SEMAPHORE_NAME = "orderbook_semaphore";  // macOS에서는 앞에 슬래시(/)를 제거
constexpr size_t HEADER_SIZE = 64;  // 헤더 크기 (바이트)
constexpr size_t MAX_BUFFER_SIZE = 100 * 1024 * 1024;  // 100MB
constexpr int DOUBLE_BUFFER_COUNT = 2;  // 더블 버퍼링을 위한 버퍼 개수

// 상태 플래그 정의
constexpr uint32_t STATUS_READY = 0x01;  // 데이터 준비됨
constexpr uint32_t STATUS_PROCESSING = 0x02;  // 데이터 처리 중
constexpr uint32_t STATUS_ERROR = 0x04;  // 오류 발생

// 헤더 구조체
struct SharedMemoryHeader {
    char magic[4];          // 매직 넘버 ('SMHR')
    uint32_t buffer_index;  // 현재 활성 버퍼 인덱스
    uint32_t data_size;     // 데이터 크기
    uint64_t timestamp;     // 타임스탬프
    uint32_t status_flags;  // 상태 플래그
    char reserved[40];      // 예약 영역
};

// 콜백 함수 타입 정의
using DataCallback = std::function<bool(const uint8_t*, size_t)>;

// 메트릭 구조체
struct SharedMemoryMetrics {
    uint64_t read_count = 0;        // 읽기 횟수
    uint64_t read_errors = 0;       // 읽기 오류 횟수
    uint64_t total_bytes_read = 0;  // 총 읽은 바이트 수
    uint64_t last_read_time = 0;    // 마지막 읽기 시간
    uint64_t max_data_size = 0;     // 최대 데이터 크기
    double avg_read_time_ms = 0;    // 평균 읽기 시간 (ms)
};

/**
 * @class SharedMemoryReader
 * @brief 공유 메모리 접근 클래스
 * 
 * 이 클래스는 공유 메모리에서 데이터를 읽는 기능을 제공합니다.
 */
class SharedMemoryReader {
public:
    /**
     * @brief 생성자
     * @param name 공유 메모리 이름
     * @param callback 데이터 콜백 함수
     */
    SharedMemoryReader(const std::string& name = SHARED_MEMORY_NAME, 
                      DataCallback callback = nullptr);
    
    /**
     * @brief 소멸자
     */
    ~SharedMemoryReader();
    
    /**
     * @brief 초기화
     * @return 초기화 성공 여부
     */
    bool initialize();
    
    /**
     * @brief 데이터 콜백 설정
     * @param callback 데이터 콜백 함수
     */
    void setCallback(DataCallback callback);
    
    /**
     * @brief 데이터 폴링 시작
     * @param interval_ms 폴링 간격 (밀리초)
     */
    void startPolling(int interval_ms = 10);
    
    /**
     * @brief 데이터 폴링 중지
     */
    void stopPolling();
    
    /**
     * @brief 데이터 읽기 (수동)
     * @return 읽기 성공 여부
     */
    bool readData();
    
    /**
     * @brief 메트릭 정보 출력
     */
    void printMetrics() const;
    
private:
    std::string name_;                  // 공유 메모리 이름
    DataCallback callback_;             // 데이터 콜백 함수
    int shm_fd_;                        // 공유 메모리 파일 디스크립터
    sem_t* sem_fd_;                     // 세마포어 포인터
    uint8_t* memory_ptr_;               // 메모리 매핑 포인터
    size_t total_size_;                 // 전체 메모리 크기
    size_t buffer_size_;                // 버퍼 크기
    uint32_t last_buffer_index_;        // 마지막으로 읽은 버퍼 인덱스
    uint64_t last_timestamp_;          // 마지막 타임스탬프
    
    std::atomic<bool> is_polling_;      // 폴링 중 여부
    std::thread polling_thread_;        // 폴링 스레드
    std::mutex mutex_;                  // 뮤텍스
    
    // 메트릭
    SharedMemoryMetrics metrics_;        // 메트릭 정보
    double total_read_time_ms_;         // 총 읽기 시간 (밀리초)
    
    /**
     * @brief 폴링 스레드 함수
     * @param interval_ms 폴링 간격 (밀리초)
     */
    void pollingThread(int interval_ms);
    
    /**
     * @brief 헤더 읽기
     * @param header 헤더 구조체 참조
     * @return 읽기 성공 여부
     */
    bool readHeader(SharedMemoryHeader& header);
    
    /**
     * @brief 버퍼 데이터 읽기
     * @param buffer_index 버퍼 인덱스
     * @param data_size 데이터 크기
     * @return 읽기 성공 여부
     */
    bool readBuffer(uint32_t buffer_index, uint32_t data_size);
    
    /**
     * @brief 리소스 정리
     */
    void cleanup();
};

#endif // SHARED_MEMORY_READER_H 