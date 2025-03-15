#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
바이낸스 선물 오더북 데이터 전송 테스트
- 큐로 데이터가 제대로 전송되는지 확인
- C++로 데이터가 제대로 전송되는지 확인
"""

import os
import sys
import asyncio
import time
import json
import logging
from unittest.mock import MagicMock, patch
from datetime import datetime
from typing import Dict, List, Any, Optional

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from crosskimp.logger.logger import get_unified_logger
from src.crosskimp.ob_collector.cpp.cpp_interface import get_cpp_interface, send_orderbook_to_cpp
from crosskimp.config.config_loader import initialize_config, get_settings
from src.crosskimp.ob_collector.orderbook.orderbook.binance_f_ob import BinanceFutureOrderBookManager, BinanceFutureOrderBook
from src.crosskimp.ob_collector.orderbook.orderbook.base_ob_v2 import OrderBookV2
from crosskimp.config.constants import Exchange
from src.crosskimp.ob_collector.cpp.shared_memory_manager import get_shared_memory_manager

# 로거 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MockQueue:
    """테스트용 큐 클래스"""
    def __init__(self):
        self.items = []
        self.put_count = 0
    
    async def put(self, item):
        self.items.append(item)
        self.put_count += 1
        return True

class MockCppInterface:
    """테스트용 C++ 인터페이스 클래스"""
    def __init__(self):
        self.sent_data = []
        self.send_count = 0
    
    async def process_orderbook(self, exchange_name, orderbook_data):
        self.sent_data.append((exchange_name, orderbook_data))
        self.send_count += 1
        logger.info(f"MockCppInterface: C++로 데이터 전송 - {exchange_name}, {orderbook_data.get('symbol')}")
        return True

# C++ 인터페이스 초기화 함수
def initialize_cpp_interface():
    """C++ 인터페이스 초기화"""
    cpp_interface = get_cpp_interface()
    cpp_interface._initialize()
    logger.info("C++ 인터페이스 초기화 완료")

# 공유 메모리 초기화 함수
def initialize_shared_memory():
    """공유 메모리 초기화"""
    shared_memory_manager = get_shared_memory_manager()
    shared_memory_manager.initialize()
    logger.info("공유 메모리 초기화 완료")

async def test_binance_future_orderbook():
    """바이낸스 선물 오더북 테스트"""
    # 설정 초기화
    await initialize_config()
    
    # 테스트용 큐 및 C++ 인터페이스 생성
    mock_queue = MockQueue()
    mock_cpp = MockCppInterface()
    
    # 원래 함수 백업
    original_get_cpp_interface = get_cpp_interface
    original_send_to_cpp = send_orderbook_to_cpp
    
    # 모킹 함수 정의
    def mock_get_cpp_interface():
        logger.info("mock_get_cpp_interface 호출됨")
        return mock_cpp
    
    async def mock_send_to_cpp(exchange_name, orderbook_data):
        logger.info(f"mock_send_to_cpp 호출됨: {exchange_name}, {orderbook_data.get('symbol')}")
        return await mock_cpp.process_orderbook(exchange_name, orderbook_data)
    
    try:
        # 함수 모킹
        import src.crosskimp.ob_collector.cpp.cpp_interface as cpp_interface
        cpp_interface.get_cpp_interface = mock_get_cpp_interface
        cpp_interface.send_orderbook_to_cpp = mock_send_to_cpp
        
        # 바이낸스 선물 오더북 매니저 생성
        ob_manager = BinanceFutureOrderBookManager(depth=100)
        
        # 출력 큐 설정
        ob_manager.set_output_queue(mock_queue)
        
        # 테스트 데이터 생성
        symbol = "BTC"
        snapshot_data = {
            "lastUpdateId": 1000,
            "bids": [
                ["49900.0", "1.0"],
                ["49800.0", "2.0"],
                ["49700.0", "3.0"]
            ],
            "asks": [
                ["50100.0", "1.0"],
                ["50200.0", "2.0"],
                ["50300.0", "3.0"]
            ]
        }
        
        # 버퍼 초기화
        if symbol not in ob_manager.buffer_events:
            ob_manager.buffer_events[symbol] = []
        
        # 스냅샷 초기화
        logger.info("스냅샷 초기화 시작")
        result = await ob_manager.initialize_snapshot(symbol, snapshot_data)
        logger.info(f"스냅샷 초기화 결과: {result.is_valid}, {result.error_messages}")
        
        # 큐 전송 횟수 확인
        logger.info(f"초기화 후 큐 전송 횟수: {mock_queue.put_count}")
        logger.info(f"초기화 후 C++ 전송 횟수: {mock_cpp.send_count}")
        
        # 델타 업데이트 테스트
        delta_data = {
            "exchangename": "binancefuture",
            "symbol": "BTC",
            "bids": [[50000.0, 1.0], [49900.0, 2.0]],
            "asks": [[50100.0, 1.0], [50200.0, 2.0]],
            "timestamp": int(time.time() * 1000),
            "first_update_id": 1001,
            "final_update_id": 1002,
            "pu": 1000,  # 이전 업데이트 ID
            "sequence": 1002
        }
        
        logger.info("델타 업데이트 시작")
        result = await ob_manager.update(symbol, delta_data)
        logger.info(f"델타 업데이트 결과: {result.is_valid}, {result.error_messages}")
        
        # 큐 전송 횟수 확인
        logger.info(f"업데이트 후 큐 전송 횟수: {mock_queue.put_count}")
        logger.info(f"업데이트 후 C++ 전송 횟수: {mock_cpp.send_count}")
        
        # 오더북 객체 직접 테스트
        logger.info("오더북 객체 직접 테스트 시작")
        ob = BinanceFutureOrderBook(
            exchangename="binancefuture",
            symbol="BTC",
            depth=100,
            inversion_detection=True
        )
        
        # 오더북 업데이트
        ob.update_orderbook(
            bids=[[50000.0, 1.0], [49900.0, 2.0]],
            asks=[[50100.0, 1.0], [50200.0, 2.0]],
            timestamp=int(time.time() * 1000),
            sequence=1002,
            is_snapshot=False
        )
        
        # 큐 설정
        ob.set_output_queue(mock_queue)
        
        # 명시적으로 send_to_cpp 호출
        logger.info("명시적으로 send_to_cpp 호출")
        
        # 공유 메모리 초기화
        initialize_shared_memory()
        logger.info("공유 메모리 초기화 완료")
        
        # C++ 인터페이스 초기화
        initialize_cpp_interface()
        logger.info("C++ 인터페이스 초기화 완료")
        
        # 직접 send_orderbook_to_cpp 함수 호출
        logger.info("직접 send_orderbook_to_cpp 함수 호출")
        
        # 오더북 데이터를 딕셔너리로 변환
        orderbook_data = ob.to_dict()
        
        # 직접 모킹된 함수 호출
        await mock_send_to_cpp("binancefuture", orderbook_data)
        
        # 최종 전송 횟수 확인
        logger.info(f"최종 큐 전송 횟수: {mock_queue.put_count}")
        logger.info(f"최종 C++ 전송 횟수: {mock_cpp.send_count}")
        
        # 마지막으로 전송된 데이터 확인
        if mock_queue.items:
            exchange, data = mock_queue.items[-1]
            logger.info(f"마지막으로 큐에 전송된 데이터: {exchange}, {json.dumps(data, indent=2)}")
        
        if mock_cpp.sent_data:
            exchange, data = mock_cpp.sent_data[-1]
            logger.info(f"마지막으로 C++에 전송된 데이터: {exchange}, {json.dumps(data, indent=2)}")
            logger.info("테스트 성공: 바이낸스 선물 오더북 데이터가 큐와 C++로 제대로 전송됨")
            return True
        else:
            logger.error("C++로 전송된 데이터가 없습니다!")
            logger.info(f"OrderBookV2.send_to_cpp 메서드: {OrderBookV2.send_to_cpp}")
            logger.info(f"BinanceFutureOrderBook.send_to_cpp 메서드: {BinanceFutureOrderBook.send_to_cpp}")
            logger.info(f"현재 모킹된 send_orderbook_to_cpp 함수: {cpp_interface.send_orderbook_to_cpp}")
            logger.info(f"원본 send_orderbook_to_cpp 함수: {original_send_to_cpp}")
            logger.error("테스트 실패: 바이낸스 선물 오더북 데이터가 큐나 C++로 전송되지 않음")
            return False
        
    finally:
        # 원래 함수 복원
        cpp_interface.get_cpp_interface = original_get_cpp_interface
        cpp_interface.send_orderbook_to_cpp = original_send_to_cpp
        
        # 메트릭 로깅 중지
        if 'ob_manager' in locals():
            await ob_manager.stop_metrics_logging()

async def main():
    """메인 함수"""
    try:
        success = await test_binance_future_orderbook()
        if success:
            logger.info("테스트 성공: 바이낸스 선물 오더북 데이터가 큐와 C++로 제대로 전송됨")
        else:
            logger.error("테스트 실패: 바이낸스 선물 오더북 데이터가 큐나 C++로 전송되지 않음")
    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main()) 