/**
 * @file calculation_engine.h
 * @brief 계산 엔진 모듈 헤더
 */

#ifndef CALCULATION_ENGINE_H
#define CALCULATION_ENGINE_H

#include <string>
#include <vector>
#include <map>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <atomic>

#include "orderbook_data.h"

/**
 * @struct ArbitrageOpportunity
 * @brief 차익거래 기회 구조체
 */
struct ArbitrageOpportunity {
    std::string buy_exchange;   // 매수 거래소
    std::string sell_exchange;  // 매도 거래소
    std::string symbol;         // 심볼
    double buy_price;           // 매수 가격
    double sell_price;          // 매도 가격
    double profit_percentage;   // 수익률 (%)
    double max_quantity;        // 최대 거래 수량
    uint64_t timestamp;         // 타임스탬프
    
    ArbitrageOpportunity() : buy_price(0), sell_price(0), profit_percentage(0), max_quantity(0), timestamp(0) {}
};

/**
 * @class CalculationEngine
 * @brief 계산 엔진 클래스
 * 
 * 이 클래스는 오더북 데이터를 기반으로 계산을 수행하는 기능을 제공합니다.
 */
class CalculationEngine {
public:
    /**
     * @brief 생성자
     * @param orderbook_data 오더북 데이터 객체
     */
    CalculationEngine(std::shared_ptr<OrderBookDataManager> orderbook_data);
    
    /**
     * @brief 소멸자
     */
    ~CalculationEngine();
    
    /**
     * @brief 계산 시작
     * @param interval_ms 계산 간격 (밀리초)
     */
    void start(int interval_ms = 100);
    
    /**
     * @brief 계산 중지
     */
    void stop();
    
    /**
     * @brief 차익거래 기회 가져오기
     * @param min_profit_percentage 최소 수익률 (%)
     * @return 차익거래 기회 목록
     */
    std::vector<ArbitrageOpportunity> getArbitrageOpportunities(double min_profit_percentage = 0.0) const;
    
    /**
     * @brief 메트릭 정보 출력
     */
    void printMetrics() const;
    
    /**
     * @brief 차익거래 기회 콜백 설정
     * @param callback 콜백 함수
     * @param min_profit_percentage 최소 수익률 (%)
     */
    void setOpportunityCallback(std::function<void(const ArbitrageOpportunity&)> callback, double min_profit_percentage = 0.5);
    
private:
    // 오더북 데이터 객체
    std::shared_ptr<OrderBookDataManager> orderbook_data_;
    
    // 차익거래 기회 목록
    std::vector<ArbitrageOpportunity> opportunities_;
    
    // 계산 스레드
    std::thread calculation_thread_;
    std::atomic<bool> is_running_;
    
    // 동기화를 위한 뮤텍스
    mutable std::mutex mutex_;
    
    // 콜백 관련
    std::function<void(const ArbitrageOpportunity&)> opportunity_callback_;
    double callback_min_profit_percentage_;
    
    // 메트릭
    struct Metrics {
        uint64_t calculation_count;
        uint64_t opportunity_count;
        uint64_t last_calculation_time;
        double max_profit_percentage;
        double avg_calculation_time_ms;
        
        Metrics() : calculation_count(0), opportunity_count(0), last_calculation_time(0),
                   max_profit_percentage(0), avg_calculation_time_ms(0) {}
    };
    
    Metrics metrics_;
    double total_calculation_time_ms_;
    
    /**
     * @brief 계산 스레드 함수
     * @param interval_ms 계산 간격 (밀리초)
     */
    void calculationThread(int interval_ms);
    
    /**
     * @brief 차익거래 기회 계산
     */
    void calculateArbitrageOpportunities();
    
    /**
     * @brief 두 거래소 간 차익거래 기회 계산
     * @param symbol 심볼
     * @param exchange1 거래소1
     * @param exchange2 거래소2
     */
    void calculatePairOpportunity(const std::string& symbol, const std::string& exchange1, const std::string& exchange2);
};

#endif // CALCULATION_ENGINE_H 