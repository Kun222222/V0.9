/**
 * @file calculation_engine.cpp
 * @brief 계산 엔진 모듈 구현
 */

#include "../include/calculation_engine.h"

#include <iostream>
#include <chrono>
#include <ctime>
#include <algorithm>
#include <set>

CalculationEngine::CalculationEngine(std::shared_ptr<OrderBookDataManager> orderbook_data)
    : orderbook_data_(orderbook_data),
      is_running_(false),
      callback_min_profit_percentage_(0.5),
      total_calculation_time_ms_(0) {
}

CalculationEngine::~CalculationEngine() {
    stop();
}

void CalculationEngine::start(int interval_ms) {
    if (is_running_.exchange(true)) {
        return;  // 이미 실행 중
    }
    
    // 계산 스레드 시작
    calculation_thread_ = std::thread(&CalculationEngine::calculationThread, this, interval_ms);
    std::cout << "계산 엔진 시작 (간격: " << interval_ms << "ms)" << std::endl;
}

void CalculationEngine::stop() {
    if (!is_running_.exchange(false)) {
        return;  // 이미 중지됨
    }
    
    // 계산 스레드 종료 대기
    if (calculation_thread_.joinable()) {
        calculation_thread_.join();
    }
    
    std::cout << "계산 엔진 중지" << std::endl;
}

std::vector<ArbitrageOpportunity> CalculationEngine::getArbitrageOpportunities(double min_profit_percentage) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (min_profit_percentage <= 0.0) {
        return opportunities_;
    }
    
    // 최소 수익률 이상인 기회만 필터링
    std::vector<ArbitrageOpportunity> filtered;
    for (const auto& opp : opportunities_) {
        if (opp.profit_percentage >= min_profit_percentage) {
            filtered.push_back(opp);
        }
    }
    
    return filtered;
}

void CalculationEngine::printMetrics() const {
    std::cout << "=== 계산 엔진 메트릭 ===" << std::endl;
    std::cout << "계산 횟수: " << metrics_.calculation_count << std::endl;
    std::cout << "기회 발견 횟수: " << metrics_.opportunity_count << std::endl;
    std::cout << "최대 수익률: " << metrics_.max_profit_percentage << "%" << std::endl;
    std::cout << "평균 계산 시간: " << metrics_.avg_calculation_time_ms << "ms" << std::endl;
    
    if (metrics_.last_calculation_time > 0) {
        char time_buf[64];
        time_t last_time = static_cast<time_t>(metrics_.last_calculation_time);
        std::strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", std::localtime(&last_time));
        std::cout << "마지막 계산 시간: " << time_buf << std::endl;
    }
    
    // 현재 기회 목록 출력 (상위 5개만)
    std::cout << "현재 차익거래 기회 (상위 5개):" << std::endl;
    
    // 수익률 기준 내림차순 정렬
    std::vector<ArbitrageOpportunity> sorted_opportunities = opportunities_;
    std::sort(sorted_opportunities.begin(), sorted_opportunities.end(), [](const auto& a, const auto& b) {
        return a.profit_percentage > b.profit_percentage;
    });
    
    // 상위 5개만 출력
    size_t count = 0;
    for (const auto& opp : sorted_opportunities) {
        std::cout << "  " << opp.symbol << ": " << opp.buy_exchange << " -> " << opp.sell_exchange
                  << " (" << opp.profit_percentage << "%, " << opp.max_quantity << "개)" << std::endl;
        if (++count >= 5) break;
    }
    
    if (sorted_opportunities.size() > 5) {
        std::cout << "  ... 외 " << (sorted_opportunities.size() - 5) << "개" << std::endl;
    }
}

void CalculationEngine::setOpportunityCallback(std::function<void(const ArbitrageOpportunity&)> callback, double min_profit_percentage) {
    std::lock_guard<std::mutex> lock(mutex_);
    opportunity_callback_ = callback;
    callback_min_profit_percentage_ = min_profit_percentage;
}

void CalculationEngine::calculationThread(int interval_ms) {
    while (is_running_) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        // 차익거래 기회 계산
        calculateArbitrageOpportunities();
        
        // 메트릭 업데이트
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        double calculation_time_ms = duration.count() / 1000.0;
        
        {
            std::lock_guard<std::mutex> lock(mutex_);
            metrics_.calculation_count++;
            metrics_.last_calculation_time = static_cast<uint64_t>(std::time(nullptr));
            
            total_calculation_time_ms_ += calculation_time_ms;
            metrics_.avg_calculation_time_ms = total_calculation_time_ms_ / metrics_.calculation_count;
        }
        
        // 다음 계산까지 대기
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }
}

void CalculationEngine::calculateArbitrageOpportunities() {
    // 거래소 목록 가져오기
    std::vector<std::string> exchanges = orderbook_data_->getExchanges();
    if (exchanges.size() < 2) {
        return;  // 거래소가 2개 미만이면 차익거래 불가능
    }
    
    // 모든 거래소 쌍에 대해 계산
    std::vector<ArbitrageOpportunity> new_opportunities;
    
    for (size_t i = 0; i < exchanges.size(); i++) {
        for (size_t j = i + 1; j < exchanges.size(); j++) {
            const std::string& exchange1 = exchanges[i];
            const std::string& exchange2 = exchanges[j];
            
            // 거래소1의 심볼 목록 가져오기
            std::vector<std::string> symbols1 = orderbook_data_->getSymbols(exchange1);
            
            // 거래소2의 심볼 목록 가져오기
            std::vector<std::string> symbols2 = orderbook_data_->getSymbols(exchange2);
            
            // 공통 심볼 찾기
            std::set<std::string> common_symbols;
            for (const auto& symbol : symbols1) {
                if (std::find(symbols2.begin(), symbols2.end(), symbol) != symbols2.end()) {
                    common_symbols.insert(symbol);
                }
            }
            
            // 각 공통 심볼에 대해 차익거래 기회 계산
            for (const auto& symbol : common_symbols) {
                calculatePairOpportunity(symbol, exchange1, exchange2);
            }
        }
    }
    
    // 콜백 호출
    if (opportunity_callback_) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& opp : opportunities_) {
            if (opp.profit_percentage >= callback_min_profit_percentage_) {
                opportunity_callback_(opp);
            }
        }
    }
}

void CalculationEngine::calculatePairOpportunity(const std::string& symbol, const std::string& exchange1, const std::string& exchange2) {
    // 두 거래소의 오더북 데이터 가져오기
    auto orderbook1 = orderbook_data_->getOrderBook(exchange1, symbol);
    auto orderbook2 = orderbook_data_->getOrderBook(exchange2, symbol);
    
    // 데이터가 없으면 종료
    if (!orderbook1 || !orderbook2) {
        return;
    }
    
    // 매수/매도 데이터 추출
    const auto& bids1 = orderbook1->first;   // 거래소1의 매수 호가
    const auto& asks1 = orderbook1->second;  // 거래소1의 매도 호가
    const auto& bids2 = orderbook2->first;   // 거래소2의 매수 호가
    const auto& asks2 = orderbook2->second;  // 거래소2의 매도 호가
    
    // 데이터가 충분한지 확인
    if (bids1.empty() || asks1.empty() || bids2.empty() || asks2.empty()) {
        return;
    }
    
    // 거래소1에서 매수, 거래소2에서 매도하는 경우
    double buy_price1 = asks1[0].price;  // 거래소1의 최저 매도가 (우리가 살 가격)
    double sell_price2 = bids2[0].price;  // 거래소2의 최고 매수가 (우리가 팔 가격)
    
    // 수익률 계산
    if (sell_price2 > buy_price1) {
        double profit_percentage1 = (sell_price2 - buy_price1) / buy_price1 * 100.0;
        double max_quantity1 = std::min(asks1[0].quantity, bids2[0].quantity);
        
        if (profit_percentage1 > 0.0) {
            ArbitrageOpportunity opportunity;
            opportunity.buy_exchange = exchange1;
            opportunity.sell_exchange = exchange2;
            opportunity.symbol = symbol;
            opportunity.buy_price = buy_price1;
            opportunity.sell_price = sell_price2;
            opportunity.profit_percentage = profit_percentage1;
            opportunity.max_quantity = max_quantity1;
            opportunity.timestamp = static_cast<uint64_t>(std::time(nullptr));
            
            // 기회 추가
            {
                std::lock_guard<std::mutex> lock(mutex_);
                opportunities_.push_back(opportunity);
                
                // 메트릭 업데이트
                metrics_.opportunity_count++;
                metrics_.max_profit_percentage = std::max(metrics_.max_profit_percentage, profit_percentage1);
            }
        }
    }
    
    // 거래소2에서 매수, 거래소1에서 매도하는 경우
    double buy_price2 = asks2[0].price;  // 거래소2의 최저 매도가 (우리가 살 가격)
    double sell_price1 = bids1[0].price;  // 거래소1의 최고 매수가 (우리가 팔 가격)
    
    // 수익률 계산
    if (sell_price1 > buy_price2) {
        double profit_percentage2 = (sell_price1 - buy_price2) / buy_price2 * 100.0;
        double max_quantity2 = std::min(asks2[0].quantity, bids1[0].quantity);
        
        if (profit_percentage2 > 0.0) {
            ArbitrageOpportunity opportunity;
            opportunity.buy_exchange = exchange2;
            opportunity.sell_exchange = exchange1;
            opportunity.symbol = symbol;
            opportunity.buy_price = buy_price2;
            opportunity.sell_price = sell_price1;
            opportunity.profit_percentage = profit_percentage2;
            opportunity.max_quantity = max_quantity2;
            opportunity.timestamp = static_cast<uint64_t>(std::time(nullptr));
            
            // 기회 추가
            {
                std::lock_guard<std::mutex> lock(mutex_);
                opportunities_.push_back(opportunity);
                
                // 메트릭 업데이트
                metrics_.opportunity_count++;
                metrics_.max_profit_percentage = std::max(metrics_.max_profit_percentage, profit_percentage2);
            }
        }
    }
} 