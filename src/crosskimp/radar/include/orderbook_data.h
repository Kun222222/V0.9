/**
 * @file orderbook_data.h
 * @brief 오더북 데이터 모듈 헤더
 */

#ifndef ORDERBOOK_DATA_H
#define ORDERBOOK_DATA_H

#include <string>
#include <map>
#include <vector>
#include <mutex>
#include <memory>
#include <functional>
#include <atomic>

#include "flatbuffers/orderbook_generated.h"

/**
 * @struct OrderBookEntry
 * @brief 오더북 항목 구조체
 */
struct OrderBookEntry {
    double price;
    double quantity;
    
    OrderBookEntry() : price(0), quantity(0) {}
    OrderBookEntry(double p, double q) : price(p), quantity(q) {}
};

/**
 * @class OrderBookDataManager
 * @brief 오더북 데이터 관리 클래스
 * 
 * 이 클래스는 여러 거래소의 오더북 데이터를 관리하는 기능을 제공합니다.
 */
class OrderBookDataManager {
public:
    /**
     * @brief 생성자
     */
    OrderBookDataManager();
    
    /**
     * @brief 소멸자
     */
    ~OrderBookDataManager();
    
    /**
     * @brief 데이터 처리
     * @param data 데이터 포인터
     * @param size 데이터 크기
     * @return 처리 성공 여부
     */
    bool processData(const uint8_t* data, size_t size);
    
    /**
     * @brief 오더북 데이터 가져오기
     * @param exchange 거래소 이름
     * @param symbol 심볼
     * @return 오더북 데이터 (없으면 nullptr)
     */
    std::shared_ptr<std::pair<std::vector<OrderBookEntry>, std::vector<OrderBookEntry>>> getOrderBook(
        const std::string& exchange, const std::string& symbol) const;
    
    /**
     * @brief 거래소 목록 가져오기
     * @return 거래소 목록
     */
    std::vector<std::string> getExchanges() const;
    
    /**
     * @brief 심볼 목록 가져오기
     * @param exchange 거래소 이름
     * @return 심볼 목록
     */
    std::vector<std::string> getSymbols(const std::string& exchange) const;
    
    /**
     * @brief 메트릭 정보 출력
     */
    void printMetrics() const;
    
private:
    // 오더북 데이터 맵 (거래소 -> 심볼 -> 오더북)
    std::map<std::string, std::map<std::string, std::shared_ptr<std::pair<std::vector<OrderBookEntry>, std::vector<OrderBookEntry>>>>> orderbooks_;
    
    // 타임스탬프 맵 (거래소 -> 심볼 -> 타임스탬프)
    std::map<std::string, std::map<std::string, uint64_t>> timestamps_;
    
    // 시퀀스 맵 (거래소 -> 심볼 -> 시퀀스)
    std::map<std::string, std::map<std::string, uint64_t>> sequences_;
    
    // 동기화를 위한 뮤텍스
    mutable std::mutex mutex_;
    
    // 메트릭
    struct Metrics {
        uint64_t update_count;
        uint64_t last_update_time;
        uint64_t error_count;
        
        Metrics() : update_count(0), last_update_time(0), error_count(0) {}
    };
    
    Metrics metrics_;
};

#endif // ORDERBOOK_DATA_H 