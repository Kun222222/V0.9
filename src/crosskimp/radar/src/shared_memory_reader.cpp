/**
 * @file shared_memory_reader.cpp
 * @brief 공유 메모리 접근 모듈 구현
 */

#include "../include/shared_memory_reader.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>

// 헤더 매직 넘버 정의
const char* HEADER_MAGIC = "SMHR";

SharedMemoryReader::SharedMemoryReader(const std::string& name, DataCallback callback)
    : name_(name),
      callback_(callback),
      shm_fd_(-1),
      sem_fd_(nullptr),
      memory_ptr_(nullptr),
      total_size_(HEADER_SIZE + (MAX_BUFFER_SIZE * DOUBLE_BUFFER_COUNT)),
      buffer_size_(MAX_BUFFER_SIZE),
      last_buffer_index_(0),
      is_polling_(false),
      total_read_time_ms_(0) {
}

SharedMemoryReader::~SharedMemoryReader() {
    stopPolling();
    cleanup();
}

bool SharedMemoryReader::initialize() {
    try {
        // 공유 메모리 열기
        std::string shm_path = std::string("/") + name_;  // macOS에서는 앞에 슬래시(/)를 추가
        shm_fd_ = shm_open(shm_path.c_str(), O_RDONLY, 0666);
        if (shm_fd_ == -1) {
            std::cerr << "공유 메모리 열기 실패: " << strerror(errno) << std::endl;
            return false;
        }
        
        // 메모리 매핑
        memory_ptr_ = static_cast<uint8_t*>(mmap(
            nullptr, total_size_, PROT_READ, MAP_SHARED, shm_fd_, 0
        ));
        if (memory_ptr_ == MAP_FAILED) {
            std::cerr << "메모리 매핑 실패: " << strerror(errno) << std::endl;
            close(shm_fd_);
            shm_fd_ = -1;
            return false;
        }
        
        // 세마포어 열기
        std::string sem_path = std::string("/") + SEMAPHORE_NAME;  // macOS에서는 앞에 슬래시(/)를 추가
        sem_fd_ = sem_open(sem_path.c_str(), 0);
        if (sem_fd_ == SEM_FAILED) {
            std::cerr << "세마포어 열기 실패: " << strerror(errno) << std::endl;
            munmap(memory_ptr_, total_size_);
            memory_ptr_ = nullptr;
            close(shm_fd_);
            shm_fd_ = -1;
            return false;
        }
        
        std::cout << "공유 메모리 초기화 완료: " << name_ << " (크기: " << total_size_ << " 바이트)" << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "공유 메모리 초기화 실패: " << e.what() << std::endl;
        cleanup();
        return false;
    }
}

void SharedMemoryReader::setCallback(DataCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    callback_ = callback;
}

void SharedMemoryReader::startPolling(int interval_ms) {
    if (is_polling_.exchange(true)) {
        return;  // 이미 폴링 중
    }
    
    // 폴링 스레드 시작
    polling_thread_ = std::thread(&SharedMemoryReader::pollingThread, this, interval_ms);
    std::cout << "데이터 폴링 시작 (간격: " << interval_ms << "ms)" << std::endl;
}

void SharedMemoryReader::stopPolling() {
    if (!is_polling_.exchange(false)) {
        return;  // 폴링 중이 아님
    }
    
    // 폴링 스레드 종료 대기
    if (polling_thread_.joinable()) {
        polling_thread_.join();
    }
    
    std::cout << "데이터 폴링 중지" << std::endl;
}

bool SharedMemoryReader::readData() {
    if (memory_ptr_ == nullptr || sem_fd_ == nullptr) {
        std::cerr << "공유 메모리가 초기화되지 않았습니다." << std::endl;
        return false;
    }
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // 세마포어 획득 시도
    if (sem_trywait(sem_fd_) == -1) {
        if (errno == EAGAIN) {
            // 세마포어를 획득할 수 없으면 다시 시도
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            if (sem_wait(sem_fd_) == -1) {
                std::cerr << "세마포어 획득 실패: " << strerror(errno) << std::endl;
                metrics_.read_errors++;
                return false;
            }
        } else {
            std::cerr << "세마포어 획득 실패: " << strerror(errno) << std::endl;
            metrics_.read_errors++;
            return false;
        }
    }
    
    try {
        // 헤더 읽기
        SharedMemoryHeader header;
        if (!readHeader(header)) {
            sem_post(sem_fd_);  // 세마포어 해제
            metrics_.read_errors++;
            return false;
        }
        
        // 버퍼 인덱스 확인
        if (header.buffer_index >= DOUBLE_BUFFER_COUNT) {
            std::cerr << "잘못된 버퍼 인덱스: " << header.buffer_index << std::endl;
            sem_post(sem_fd_);  // 세마포어 해제
            metrics_.read_errors++;
            return false;
        }
        
        // 데이터 크기 확인
        if (header.data_size == 0 || header.data_size > buffer_size_) {
            std::cerr << "잘못된 데이터 크기: " << header.data_size << std::endl;
            sem_post(sem_fd_);  // 세마포어 해제
            metrics_.read_errors++;
            return false;
        }
        
        // 이미 처리한 버퍼인지 확인
        if (header.buffer_index == last_buffer_index_ && header.timestamp <= last_timestamp_) {
            std::cerr << "이미 처리된 버퍼: 인덱스=" << header.buffer_index 
                      << ", 이전 타임스탬프=" << last_timestamp_
                      << ", 현재 타임스탬프=" << header.timestamp << std::endl;
            sem_post(sem_fd_);  // 세마포어 해제
            return true;  // 이미 처리한 버퍼
        }
        
        // 버퍼 데이터 읽기
        if (!readBuffer(header.buffer_index, header.data_size)) {
            sem_post(sem_fd_);  // 세마포어 해제
            return false;
        }
        
        // 마지막 버퍼 인덱스와 타임스탬프 업데이트
        last_buffer_index_ = header.buffer_index;
        last_timestamp_ = header.timestamp;
        
        // 세마포어 해제
        sem_post(sem_fd_);
        
        // 메트릭 업데이트
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        double read_time_ms = duration.count() / 1000.0;
        
        std::lock_guard<std::mutex> lock(mutex_);
        metrics_.read_count++;
        metrics_.total_bytes_read += header.data_size;
        metrics_.last_read_time = static_cast<uint64_t>(std::time(nullptr));
        metrics_.max_data_size = std::max(metrics_.max_data_size, static_cast<uint64_t>(header.data_size));
        
        total_read_time_ms_ += read_time_ms;
        metrics_.avg_read_time_ms = total_read_time_ms_ / metrics_.read_count;
        
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "데이터 읽기 실패: " << e.what() << std::endl;
        sem_post(sem_fd_);  // 세마포어 해제
        metrics_.read_errors++;
        return false;
    }
}

void SharedMemoryReader::printMetrics() const {
    std::cout << "=== 공유 메모리 리더 메트릭 ===" << std::endl;
    std::cout << "읽기 횟수: " << metrics_.read_count << std::endl;
    std::cout << "읽기 오류 횟수: " << metrics_.read_errors << std::endl;
    std::cout << "총 읽은 바이트 수: " << metrics_.total_bytes_read << std::endl;
    std::cout << "최대 데이터 크기: " << metrics_.max_data_size << " 바이트" << std::endl;
    std::cout << "평균 읽기 시간: " << metrics_.avg_read_time_ms << "ms" << std::endl;
    
    if (metrics_.last_read_time > 0) {
        char time_buf[64];
        time_t last_time = static_cast<time_t>(metrics_.last_read_time);
        std::strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", std::localtime(&last_time));
        std::cout << "마지막 읽기 시간: " << time_buf << std::endl;
    }
}

void SharedMemoryReader::pollingThread(int interval_ms) {
    while (is_polling_) {
        if (readData()) {
            // 데이터 읽기 성공
        }
        
        // 지정된 간격만큼 대기
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }
}

bool SharedMemoryReader::readHeader(SharedMemoryHeader& header) {
    if (memory_ptr_ == nullptr) {
        return false;
    }
    
    // 헤더 복사
    std::memcpy(&header, memory_ptr_, sizeof(SharedMemoryHeader));
    
    // 헤더 디버깅 - 첫 16바이트 출력
    std::cerr << "헤더 읽기: ";
    for (size_t i = 0; i < 16; i++) {
        printf("%02x ", memory_ptr_[i]);
    }
    std::cerr << std::endl;
    
    // 매직 넘버 확인
    if (std::memcmp(header.magic, HEADER_MAGIC, 4) != 0) {
        std::cerr << "잘못된 헤더 매직 넘버: " 
                  << static_cast<char>(header.magic[0])
                  << static_cast<char>(header.magic[1])
                  << static_cast<char>(header.magic[2])
                  << static_cast<char>(header.magic[3])
                  << " (예상: SMHR)" << std::endl;
        return false;
    }
    
    // 기타 헤더 필드 디버깅
    std::cerr << "헤더 매직: " << header.magic[0] << header.magic[1] << header.magic[2] << header.magic[3]
              << ", 버퍼 인덱스: " << header.buffer_index
              << ", 데이터 크기: " << header.data_size
              << ", 타임스탬프: " << header.timestamp
              << ", 상태 플래그: " << header.status_flags
              << std::endl;
              
    return true;
}

bool SharedMemoryReader::readBuffer(uint32_t buffer_index, uint32_t data_size) {
    if (memory_ptr_ == nullptr || buffer_index >= DOUBLE_BUFFER_COUNT) {
        return false;
    }
    
    // 버퍼 오프셋 계산
    size_t buffer_offset = HEADER_SIZE + (buffer_index * buffer_size_);
    
    // 데이터 크기 검증
    if (data_size > buffer_size_) {
        std::cerr << "데이터 크기가 버퍼 크기를 초과: " << data_size << " > " << buffer_size_ << std::endl;
        return false;
    }
    
    // 콜백 호출
    if (callback_) {
        callback_(memory_ptr_ + buffer_offset, data_size);
    }
    
    return true;
}

void SharedMemoryReader::cleanup() {
    // 메모리 매핑 해제
    if (memory_ptr_ != nullptr && memory_ptr_ != MAP_FAILED) {
        munmap(memory_ptr_, total_size_);
        memory_ptr_ = nullptr;
    }
    
    // 공유 메모리 파일 디스크립터 닫기
    if (shm_fd_ != -1) {
        close(shm_fd_);
        shm_fd_ = -1;
    }
    
    // 세마포어 닫기
    if (sem_fd_ != SEM_FAILED && sem_fd_ != nullptr) {
        sem_close(sem_fd_);
        sem_fd_ = nullptr;
    }
} 