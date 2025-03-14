/**
 * @file main.cpp
 * @brief 메인 프로그램
 */

#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>

#include "../include/shared_memory_reader.h"
#include "../include/orderbook_data.h"
#include "../include/calculation_engine.h"

// 전역 변수
std::atomic<bool> g_running(true);

// 시그널 핸들러
void signalHandler(int signal) {
    std::cout << "시그널 수신: " << signal << std::endl;
    g_running = false;
}

// 차익거래 기회 콜백 함수
void opportunityCallback(const ArbitrageOpportunity& opportunity) {
    std::cout << "차익거래 기회 발견: " << opportunity.symbol
              << " " << opportunity.buy_exchange << " -> " << opportunity.sell_exchange
              << " (" << opportunity.profit_percentage << "%, " << opportunity.max_quantity << "개)"
              << std::endl;
}

int main(int argc, char* argv[]) {
    try {
        std::cout << "오더북 데이터 처리 프로그램 시작" << std::endl;
        
        // 시그널 핸들러 등록
        std::signal(SIGINT, signalHandler);
        std::signal(SIGTERM, signalHandler);
        
        // 오더북 데이터 객체 생성
        auto orderbook_data = std::make_shared<OrderBookDataManager>();
        
        // 계산 엔진 생성
        auto calculation_engine = std::make_shared<CalculationEngine>(orderbook_data);
        
        // 차익거래 기회 콜백 설정
        calculation_engine->setOpportunityCallback(opportunityCallback, 0.5);
        
        // 공유 메모리 리더 생성
        SharedMemoryReader shared_memory_reader(SHARED_MEMORY_NAME, [orderbook_data](const uint8_t* data, size_t size) -> bool {
            return orderbook_data->processData(data, size);
        });
        
        // 공유 메모리 초기화
        if (!shared_memory_reader.initialize()) {
            std::cerr << "공유 메모리 초기화 실패" << std::endl;
            return 1;
        }
        
        // 데이터 폴링 시작
        shared_memory_reader.startPolling(10);  // 10ms 간격으로 폴링
        
        // 계산 엔진 시작
        calculation_engine->start(100);  // 100ms 간격으로 계산
        
        std::cout << "프로그램이 실행 중입니다. 종료하려면 Ctrl+C를 누르세요." << std::endl;
        
        // 메인 루프
        while (g_running) {
            // 1초마다 메트릭 출력
            static int count = 0;
            if (++count % 10 == 0) {
                shared_memory_reader.printMetrics();
                orderbook_data->printMetrics();
                calculation_engine->printMetrics();
            }
            
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        std::cout << "프로그램 종료 중..." << std::endl;
        
        // 계산 엔진 중지
        calculation_engine->stop();
        
        // 데이터 폴링 중지
        shared_memory_reader.stopPolling();
        
        std::cout << "프로그램 종료 완료" << std::endl;
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "오류 발생: " << e.what() << std::endl;
        return 1;
    }
} 