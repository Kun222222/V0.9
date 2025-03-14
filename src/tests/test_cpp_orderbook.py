#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
오더북 데이터가 C++로 정상적으로 전송되는지 테스트하는 스크립트
"""

import os
import sys
import asyncio
import time
import json
from datetime import datetime
from typing import Dict, List, Any, Optional

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from src.crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from src.crosskimp.ob_collector.cpp.cpp_interface import get_cpp_interface, send_orderbook_to_cpp
from src.crosskimp.ob_collector.utils.config.config_loader import initialize_config, get_settings
from src.crosskimp.ob_collector.orderbook.websocket.base_ws_manager import WebsocketManager
from src.crosskimp.ob_collector.core.aggregator import Aggregator

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# 테스트 결과를 저장할 디렉토리
RESULTS_DIR = os.path.join(os.path.dirname(__file__), 'src', 'logs', 'cpp_test_results')
os.makedirs(RESULTS_DIR, exist_ok=True)

class OrderBookCppTester:
    """
    오더북 데이터가 C++로 정상적으로 전송되는지 테스트하는 클래스
    """
    
    def __init__(self):
        """초기화"""
        self.cpp_interface = get_cpp_interface()
        self.ws_manager = None
        self.settings = None
        self.test_results = {
            "start_time": time.time(),
            "exchanges": {},
            "total_messages": 0,
            "successful_messages": 0,
            "failed_messages": 0
        }
        
        # 테스트 결과 파일 경로
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.result_file = os.path.join(RESULTS_DIR, f"cpp_test_{timestamp}.json")
        
    async def initialize(self):
        """시스템 초기화"""
        try:
            # 설정 초기화 및 로드
            await initialize_config()
            self.settings = get_settings()
            if not self.settings:
                logger.error("설정 로드 실패")
                return False
                
            # 웹소켓 매니저 초기화
            logger.info("웹소켓 매니저 초기화 시작")
            self.ws_manager = WebsocketManager(self.settings)
            self.ws_manager.register_callback(self.websocket_callback)
            logger.info("웹소켓 매니저 초기화 완료")
            
            # Aggregator 초기화
            logger.info("Aggregator 초기화 시작")
            aggregator = Aggregator(self.settings)
            logger.info("Aggregator 초기화 완료")
            
            # 심볼 필터링
            logger.info("심볼 필터링 시작")
            filtered_data = await aggregator.run_filtering()
            logger.info(f"심볼 필터링 완료: {len(filtered_data)} 심볼")
            
            # 웹소켓 시작
            logger.info("웹소켓 연결 시작")
            await self.ws_manager.start_all_websockets(filtered_data)
            logger.info("웹소켓 연결 완료")
            
            return True
            
        except Exception as e:
            logger.error(f"초기화 중 오류 발생: {str(e)}", exc_info=True)
            return False
    
    async def websocket_callback(self, exchange_name: str, data: dict):
        """웹소켓 메시지 수신 콜백"""
        try:
            # 거래소별 통계 초기화
            if exchange_name not in self.test_results["exchanges"]:
                self.test_results["exchanges"][exchange_name] = {
                    "total_messages": 0,
                    "successful_messages": 0,
                    "failed_messages": 0,
                    "symbols": {}
                }
            
            # 심볼 정보 가져오기
            symbol = data.get("symbol", "unknown")
            
            # 심볼별 통계 초기화
            if symbol not in self.test_results["exchanges"][exchange_name]["symbols"]:
                self.test_results["exchanges"][exchange_name]["symbols"][symbol] = {
                    "total_messages": 0,
                    "successful_messages": 0,
                    "failed_messages": 0,
                    "last_message_time": 0
                }
            
            # 전체 메시지 카운트 증가
            self.test_results["total_messages"] += 1
            self.test_results["exchanges"][exchange_name]["total_messages"] += 1
            self.test_results["exchanges"][exchange_name]["symbols"][symbol]["total_messages"] += 1
            
            # C++로 데이터 전송
            start_time = time.time()
            success = await send_orderbook_to_cpp(exchange_name, data)
            end_time = time.time()
            
            # 전송 결과 기록
            if success:
                self.test_results["successful_messages"] += 1
                self.test_results["exchanges"][exchange_name]["successful_messages"] += 1
                self.test_results["exchanges"][exchange_name]["symbols"][symbol]["successful_messages"] += 1
                logger.info(f"[{exchange_name}] {symbol} 오더북 데이터 C++ 전송 성공 ({(end_time - start_time) * 1000:.2f}ms)")
            else:
                self.test_results["failed_messages"] += 1
                self.test_results["exchanges"][exchange_name]["failed_messages"] += 1
                self.test_results["exchanges"][exchange_name]["symbols"][symbol]["failed_messages"] += 1
                logger.error(f"[{exchange_name}] {symbol} 오더북 데이터 C++ 전송 실패")
            
            # 마지막 메시지 시간 업데이트
            self.test_results["exchanges"][exchange_name]["symbols"][symbol]["last_message_time"] = time.time()
            
            # 주기적으로 결과 저장
            if self.test_results["total_messages"] % 100 == 0:
                self.save_results()
                
        except Exception as e:
            logger.error(f"콜백 처리 중 오류 발생: {str(e)}", exc_info=True)
    
    def save_results(self):
        """테스트 결과 저장"""
        try:
            # 종료 시간 업데이트
            self.test_results["end_time"] = time.time()
            self.test_results["duration"] = self.test_results["end_time"] - self.test_results["start_time"]
            
            # 성공률 계산
            total = self.test_results["total_messages"]
            if total > 0:
                self.test_results["success_rate"] = (self.test_results["successful_messages"] / total) * 100
            else:
                self.test_results["success_rate"] = 0
            
            # 거래소별 성공률 계산
            for exchange_name, exchange_data in self.test_results["exchanges"].items():
                total = exchange_data["total_messages"]
                if total > 0:
                    exchange_data["success_rate"] = (exchange_data["successful_messages"] / total) * 100
                else:
                    exchange_data["success_rate"] = 0
            
            # 결과 파일 저장
            with open(self.result_file, 'w', encoding='utf-8') as f:
                json.dump(self.test_results, f, indent=2, ensure_ascii=False)
                
            logger.info(f"테스트 결과 저장 완료: {self.result_file}")
            
        except Exception as e:
            logger.error(f"결과 저장 중 오류 발생: {str(e)}", exc_info=True)
    
    async def run_test(self, duration: int = 60):
        """
        지정된 시간 동안 테스트 실행
        
        Args:
            duration: 테스트 실행 시간 (초)
        """
        try:
            # 시스템 초기화
            if not await self.initialize():
                logger.error("시스템 초기화 실패")
                return
            
            logger.info(f"테스트 시작: {duration}초 동안 실행")
            
            # 지정된 시간 동안 실행
            start_time = time.time()
            while time.time() - start_time < duration:
                # 1초마다 상태 출력
                if int(time.time() - start_time) % 10 == 0:
                    total = self.test_results["total_messages"]
                    success = self.test_results["successful_messages"]
                    failed = self.test_results["failed_messages"]
                    success_rate = (success / total) * 100 if total > 0 else 0
                    
                    logger.info(f"테스트 진행 중: {int(time.time() - start_time)}초 경과, "
                               f"총 {total}개 메시지, 성공 {success}개 ({success_rate:.2f}%), 실패 {failed}개")
                
                await asyncio.sleep(1)
            
            # 최종 결과 저장
            self.save_results()
            
            # 결과 출력
            total = self.test_results["total_messages"]
            success = self.test_results["successful_messages"]
            failed = self.test_results["failed_messages"]
            success_rate = (success / total) * 100 if total > 0 else 0
            
            logger.info("=" * 50)
            logger.info(f"테스트 완료: 총 {total}개 메시지, 성공 {success}개 ({success_rate:.2f}%), 실패 {failed}개")
            logger.info(f"테스트 결과 파일: {self.result_file}")
            logger.info("=" * 50)
            
            # 거래소별 결과 출력
            logger.info("거래소별 결과:")
            for exchange_name, exchange_data in self.test_results["exchanges"].items():
                total = exchange_data["total_messages"]
                success = exchange_data["successful_messages"]
                failed = exchange_data["failed_messages"]
                success_rate = (success / total) * 100 if total > 0 else 0
                
                logger.info(f"  [{exchange_name}] 총 {total}개 메시지, 성공 {success}개 ({success_rate:.2f}%), 실패 {failed}개")
            
        except Exception as e:
            logger.error(f"테스트 실행 중 오류 발생: {str(e)}", exc_info=True)
        finally:
            # 웹소켓 종료
            if self.ws_manager:
                await self.ws_manager.shutdown()
                logger.info("웹소켓 종료 완료")

async def main():
    """메인 함수"""
    try:
        # 테스트 시간 설정 (기본 60초)
        duration = 60
        if len(sys.argv) > 1:
            try:
                duration = int(sys.argv[1])
            except ValueError:
                logger.error(f"유효하지 않은 테스트 시간: {sys.argv[1]}, 기본값 60초로 설정")
        
        # 테스터 생성 및 실행
        tester = OrderBookCppTester()
        await tester.run_test(duration)
        
    except KeyboardInterrupt:
        logger.info("Ctrl+C 감지됨, 프로그램 종료 중...")
    except Exception as e:
        logger.error(f"메인 함수 오류: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main()) 