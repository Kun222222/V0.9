#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
웹소켓 연결 상태 및 재연결 테스트

이 테스트는 다음 항목을 확인합니다:
1. 모든 거래소 연결이 정상적으로 이루어지는지
2. 연결 상태가 일관되게 ObCollector와 공유되는지
3. 연결이 끊어지면 자동으로 재연결되는지
"""

import asyncio
import sys
import os
import time
import random
from typing import Dict, List

# 프로젝트 루트 경로 추가 (실행 환경에 따라 조정)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

# 프로젝트 모듈 임포트
from crosskimp.common.logger.logger import get_unified_logger, create_raw_logger
from crosskimp.common.config.app_config import get_config
from crosskimp.common.config.common_constants import Exchange, SystemComponent

# ObCollector 및 관련 모듈 임포트
from crosskimp.ob_collector.ob_collector import ObCollector

# 로거 설정
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

class WebSocketConnectionTest:
    """웹소켓 연결 테스트 클래스"""
    
    def __init__(self):
        """테스트 초기화"""
        self.logger = logger
        self.ob_collector = ObCollector()
        
        # 테스트 결과 저장용 변수
        self.connection_results = {}
        self.reconnection_results = {}
        
        # 테스트 대상 거래소 (필요에 따라 조정)
        self.test_exchanges = [
            Exchange.UPBIT.value,
            Exchange.BYBIT.value,
            Exchange.BITHUMB.value,
            Exchange.BINANCE.value,
            # 필요에 따라 더 추가
        ]
        
    async def setup(self):
        """테스트 준비"""
        self.logger.info("===== 웹소켓 연결 테스트 시작 =====")
        
        # ObCollector 초기화
        await self.ob_collector.initialize()
        
        # 모든 거래소 연결 테스트 준비
        if not self.ob_collector.is_initialized():
            self.logger.error("ObCollector 초기화 실패")
            return False
            
        return True
        
    async def test_exchange_connections(self):
        """모든 거래소 연결 테스트"""
        self.logger.info("--- 거래소 연결 테스트 ---")
        
        for exchange in self.test_exchanges:
            try:
                # 거래소 연결 시도
                self.logger.info(f"[{exchange}] 연결 테스트 시작")
                
                # ObCollector를 통한 거래소 연결
                result = await self.ob_collector._connect_and_subscribe(exchange)
                
                # 결과 저장
                self.connection_results[exchange] = {
                    "success": result,
                    "status": self.ob_collector.exchange_status.get(exchange, False),
                    "connector": self.ob_collector.connectors.get(exchange)
                }
                
                self.logger.info(f"[{exchange}] 연결 테스트 결과: {'성공' if result else '실패'}")
                
                # 연결된 경우 1초 대기 후 상태 재확인
                if result:
                    await asyncio.sleep(1)
                    connector = self.ob_collector.connectors.get(exchange)
                    if connector:
                        status_match = connector.is_connected == self.ob_collector.exchange_status.get(exchange, False)
                        self.logger.info(f"[{exchange}] 연결 상태 일치 여부: {status_match}")
                        self.connection_results[exchange]["status_match"] = status_match
                
            except Exception as e:
                self.logger.error(f"[{exchange}] 연결 테스트 중 오류: {str(e)}")
                self.connection_results[exchange] = {
                    "success": False,
                    "error": str(e)
                }
                
            # 다음 거래소 테스트 전 잠시 대기
            await asyncio.sleep(1)
            
        return self.connection_results
        
    async def test_reconnection(self):
        """연결 끊김 및 재연결 테스트"""
        self.logger.info("--- 재연결 테스트 ---")
        
        for exchange, result in self.connection_results.items():
            if not result.get("success", False):
                self.logger.info(f"[{exchange}] 초기 연결 실패로 재연결 테스트 건너뜀")
                continue
                
            connector = result.get("connector")
            if not connector:
                self.logger.warning(f"[{exchange}] 연결 객체가 없어 재연결 테스트 불가")
                continue
                
            try:
                # 현재 연결 상태 확인
                initial_status = connector.is_connected
                self.logger.info(f"[{exchange}] 현재 연결 상태: {initial_status}")
                
                if not initial_status:
                    self.logger.warning(f"[{exchange}] 이미 연결이 끊어져 있어 재연결 테스트 건너뜀")
                    continue
                
                # 웹소켓 강제 종료하여 연결 끊김 시뮬레이션
                self.logger.info(f"[{exchange}] 연결 강제 종료 시뮬레이션")
                await connector.disconnect()
                
                # 연결 끊김 확인
                await asyncio.sleep(1)
                after_disconnect = connector.is_connected
                self.logger.info(f"[{exchange}] 강제 종료 후 연결 상태: {after_disconnect}")
                
                # OB 컬렉터의 연결 상태도 확인
                ob_status = self.ob_collector.exchange_status.get(exchange, False)
                self.logger.info(f"[{exchange}] OB 컬렉터 연결 상태: {ob_status}")
                
                # 상태 일치 여부 확인
                status_match_after_disconnect = after_disconnect == ob_status
                self.logger.info(f"[{exchange}] 연결 종료 후 상태 일치 여부: {status_match_after_disconnect}")
                
                # 자동 재연결 테스트 (3번 연결 상태 확인)
                reconnected = False
                for i in range(3):
                    self.logger.info(f"[{exchange}] 재연결 상태 확인 {i+1}/3...")
                    await asyncio.sleep(2)  # 재연결 시간 확보
                    
                    # 재연결 상태 확인
                    current_status = connector.is_connected
                    ob_status = self.ob_collector.exchange_status.get(exchange, False)
                    status_match = current_status == ob_status
                    
                    self.logger.info(f"[{exchange}] 현재 연결 상태: {current_status}, OB 상태: {ob_status}, 일치: {status_match}")
                    
                    if current_status and status_match:
                        reconnected = True
                        self.logger.info(f"[{exchange}] 자동 재연결 확인됨!")
                        break
                
                # 결과 저장
                self.reconnection_results[exchange] = {
                    "initial_status": initial_status,
                    "disconnect_success": not after_disconnect,
                    "status_match_after_disconnect": status_match_after_disconnect,
                    "reconnected": reconnected,
                    "final_status": connector.is_connected,
                    "final_status_match": connector.is_connected == self.ob_collector.exchange_status.get(exchange, False)
                }
                
            except Exception as e:
                self.logger.error(f"[{exchange}] 재연결 테스트 중 오류: {str(e)}")
                self.reconnection_results[exchange] = {
                    "error": str(e)
                }
                
            # 다음 거래소 테스트 전 잠시 대기
            await asyncio.sleep(2)
            
        return self.reconnection_results
        
    async def test_connection_callback(self):
        """연결 콜백 테스트"""
        self.logger.info("--- 연결 콜백 테스트 ---")
        
        # 테스트 결과를 저장할 딕셔너리
        callback_results = {}
        
        # 성공적으로 연결된 거래소 중 하나 선택
        connected_exchanges = [ex for ex, res in self.connection_results.items() 
                              if res.get("success", False)]
        
        if not connected_exchanges:
            self.logger.warning("연결된 거래소가 없어 콜백 테스트 건너뜀")
            return {}
            
        test_exchange = random.choice(connected_exchanges)
        connector = self.ob_collector.connectors.get(test_exchange)
        
        if not connector:
            self.logger.warning(f"[{test_exchange}] 연결 객체가 없어 콜백 테스트 불가")
            return {}
            
        try:
            self.logger.info(f"[{test_exchange}] 연결 콜백 테스트 시작")
            
            # 현재 상태 저장
            initial_status = connector.is_connected
            
            # 연결 강제 종료
            await connector.disconnect()
            await asyncio.sleep(1)
            
            # 재연결 - 이 과정에서 콜백이 호출되는지 확인
            self.logger.info(f"[{test_exchange}] 수동 재연결 시도")
            reconnect_result = await connector.reconnect()
            
            # 약간의 시간 여유
            await asyncio.sleep(1)
            
            # 결과 확인
            final_status = connector.is_connected
            ob_status = self.ob_collector.exchange_status.get(test_exchange, False)
            status_match = final_status == ob_status
            
            self.logger.info(f"[{test_exchange}] 재연결 결과: {reconnect_result}, 연결 상태: {final_status}, OB 상태: {ob_status}, 일치: {status_match}")
            
            # 결과 저장
            callback_results[test_exchange] = {
                "initial_status": initial_status,
                "reconnect_result": reconnect_result, 
                "final_status": final_status,
                "ob_status": ob_status,
                "status_match": status_match
            }
            
        except Exception as e:
            self.logger.error(f"[{test_exchange}] 콜백 테스트 중 오류: {str(e)}")
            callback_results[test_exchange] = {
                "error": str(e)
            }
            
        return callback_results
        
    def print_results(self):
        """테스트 결과 출력"""
        self.logger.info("\n===== 웹소켓 연결 테스트 결과 =====")
        
        # 연결 테스트 결과
        self.logger.info("\n--- 거래소 연결 테스트 결과 ---")
        for exchange, result in self.connection_results.items():
            success = result.get("success", False)
            status = "성공" if success else "실패"
            status_match = result.get("status_match", False) if success else "N/A"
            
            self.logger.info(f"[{exchange}] 연결: {status}, 상태 일치: {status_match}")
            
        # 재연결 테스트 결과
        self.logger.info("\n--- 재연결 테스트 결과 ---")
        for exchange, result in self.reconnection_results.items():
            if "error" in result:
                self.logger.info(f"[{exchange}] 테스트 오류: {result['error']}")
                continue
                
            reconnected = result.get("reconnected", False)
            status = "성공" if reconnected else "실패"
            final_match = result.get("final_status_match", False)
            
            self.logger.info(f"[{exchange}] 재연결: {status}, 최종 상태 일치: {final_match}")
            
        # 종합 결과
        self.logger.info("\n--- 종합 결과 ---")
        connection_success = sum(1 for r in self.connection_results.values() if r.get("success", False))
        reconnection_success = sum(1 for r in self.reconnection_results.values() if r.get("reconnected", False))
        
        self.logger.info(f"총 거래소 수: {len(self.test_exchanges)}")
        self.logger.info(f"연결 성공: {connection_success}/{len(self.test_exchanges)}")
        self.logger.info(f"재연결 성공: {reconnection_success}/{len(self.reconnection_results)}")
        
        # 전체 테스트 통과 여부
        all_passed = (connection_success == len(self.test_exchanges) and 
                     reconnection_success == len(self.reconnection_results))
                     
        self.logger.info(f"\n전체 테스트 결과: {'✅ 통과' if all_passed else '❌ 실패'}\n")
            
    async def teardown(self):
        """테스트 정리"""
        self.logger.info("테스트 정리 중...")
        
        # ObCollector 종료
        await self.ob_collector.stop_collection()
        
        self.logger.info("===== 웹소켓 연결 테스트 종료 =====")
        
    async def run_all_tests(self):
        """모든 테스트 실행"""
        # 테스트 준비
        setup_success = await self.setup()
        if not setup_success:
            self.logger.error("테스트 준비 실패")
            return False
            
        try:
            # 연결 테스트
            await self.test_exchange_connections()
            
            # 약간의 시간 여유
            await asyncio.sleep(2)
            
            # 재연결 테스트
            await self.test_reconnection()
            
            # 연결 콜백 테스트
            await self.test_connection_callback()
            
            # 결과 출력
            self.print_results()
            
        except Exception as e:
            self.logger.error(f"테스트 실행 중 오류: {str(e)}")
            return False
        finally:
            # 테스트 정리
            await self.teardown()
            
        return True
            
async def main():
    """메인 함수"""
    # 테스트 실행
    test = WebSocketConnectionTest()
    await test.run_all_tests()
    
if __name__ == "__main__":
    # 비동기 루프 실행
    asyncio.run(main()) 