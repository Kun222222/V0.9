import asyncio
import time
from datetime import datetime
from typing import List, Dict

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.websocket.bybit_spot_websocket import BybitSpotWebsocket
from crosskimp.ob_collector.orderbook.websocket.bybit_future_websocket import BybitFutureWebsocket

logger = get_unified_logger()

class ConnectionTester:
    def __init__(self, test_count: int = 20):
        self.test_count = test_count
        self.settings = {
            "websocket": {
                "orderbook_depth": 10,
                "depth_level": 50
            },
            "connection": {
                "websocket": {
                    "depth_level": 50
                }
            }
        }
        
        # 테스트 결과 저장
        self.results = {
            "spot": {
                "success": 0,
                "fail": 0,
                "total_time": 0,
                "attempts": [],
                "consecutive_fails": 0,  # 연속 실패 횟수 추가
                "max_consecutive_fails": 0  # 최대 연속 실패 횟수 추가
            },
            "future": {
                "success": 0,
                "fail": 0,
                "total_time": 0,
                "attempts": [],
                "consecutive_fails": 0,  # 연속 실패 횟수 추가
                "max_consecutive_fails": 0  # 최대 연속 실패 횟수 추가
            }
        }
        
        # 테스트 시작 시간 저장
        self.start_time = None

    def _update_progress_stats(self, market_type: str, success: bool):
        """진행 상황 통계 업데이트"""
        if success:
            self.results[market_type]["consecutive_fails"] = 0
        else:
            self.results[market_type]["consecutive_fails"] += 1
            self.results[market_type]["max_consecutive_fails"] = max(
                self.results[market_type]["max_consecutive_fails"],
                self.results[market_type]["consecutive_fails"]
            )

    def _print_progress(self, current_test: int):
        """현재 진행 상황 출력"""
        elapsed = time.time() - self.start_time
        tests_done = current_test + 1
        tests_remaining = self.test_count - tests_done
        
        # 예상 남은 시간 계산 (초당 평균 처리량 기준)
        avg_time_per_test = elapsed / tests_done if tests_done > 0 else 0
        estimated_remaining = avg_time_per_test * tests_remaining
        
        spot = self.results["spot"]
        future = self.results["future"]
        
        logger.info("\n=== 테스트 진행 상황 ===")
        logger.info(f"진행률: {tests_done}/{self.test_count} ({tests_done/self.test_count*100:.1f}%)")
        logger.info(f"경과 시간: {elapsed/60:.1f}분")
        logger.info(f"예상 남은 시간: {estimated_remaining/60:.1f}분")
        
        logger.info("\n[현물]")
        logger.info(f"성공률: {spot['success']}/{tests_done} ({spot['success']/tests_done*100:.1f}% 성공)")
        if spot['success'] > 0:
            logger.info(f"평균 연결 시간: {spot['total_time']/spot['success']:.2f}초")
        logger.info(f"현재 연속 실패: {spot['consecutive_fails']}회")
        logger.info(f"최대 연속 실패: {spot['max_consecutive_fails']}회")
        
        logger.info("\n[선물]")
        logger.info(f"성공률: {future['success']}/{tests_done} ({future['success']/tests_done*100:.1f}% 성공)")
        if future['success'] > 0:
            logger.info(f"평균 연결 시간: {future['total_time']/future['success']:.2f}초")
        logger.info(f"현재 연속 실패: {future['consecutive_fails']}회")
        logger.info(f"최대 연속 실패: {future['max_consecutive_fails']}회")
        logger.info("\n" + "="*50 + "\n")

    async def test_spot_connection(self, attempt: int):
        """현물 웹소켓 연결 테스트"""
        try:
            start_time = time.time()
            ws = BybitSpotWebsocket(self.settings)
            
            logger.info(f"[테스트] 바이빗 현물 연결 테스트 #{attempt + 1} 시작")
            await ws.connect()
            
            elapsed = time.time() - start_time
            self.results["spot"]["success"] += 1
            self.results["spot"]["total_time"] += elapsed
            self.results["spot"]["attempts"].append({
                "attempt": attempt + 1,
                "success": True,
                "time": elapsed,
                "retries": ws.current_retry
            })
            
            logger.info(
                f"[테스트] 바이빗 현물 연결 테스트 #{attempt + 1} 성공 | "
                f"소요시간={elapsed:.2f}초, 재시도={ws.current_retry}회"
            )
            
            self._update_progress_stats("spot", True)
            await ws.stop()
            
        except Exception as e:
            elapsed = time.time() - start_time
            self.results["spot"]["fail"] += 1
            self.results["spot"]["attempts"].append({
                "attempt": attempt + 1,
                "success": False,
                "time": elapsed,
                "error": str(e)
            })
            logger.error(
                f"[테스트] 바이빗 현물 연결 테스트 #{attempt + 1} 실패 | "
                f"error={str(e)}"
            )
            self._update_progress_stats("spot", False)

    async def test_future_connection(self, attempt: int):
        """선물 웹소켓 연결 테스트"""
        try:
            start_time = time.time()
            ws = BybitFutureWebsocket(self.settings)
            
            logger.info(f"[테스트] 바이빗 선물 연결 테스트 #{attempt + 1} 시작")
            await ws.connect()
            
            elapsed = time.time() - start_time
            self.results["future"]["success"] += 1
            self.results["future"]["total_time"] += elapsed
            self.results["future"]["attempts"].append({
                "attempt": attempt + 1,
                "success": True,
                "time": elapsed,
                "retries": ws.current_retry
            })
            
            logger.info(
                f"[테스트] 바이빗 선물 연결 테스트 #{attempt + 1} 성공 | "
                f"소요시간={elapsed:.2f}초, 재시도={ws.current_retry}회"
            )
            
            self._update_progress_stats("future", True)
            await ws.stop()
            
        except Exception as e:
            elapsed = time.time() - start_time
            self.results["future"]["fail"] += 1
            self.results["future"]["attempts"].append({
                "attempt": attempt + 1,
                "success": False,
                "time": elapsed,
                "error": str(e)
            })
            logger.error(
                f"[테스트] 바이빗 선물 연결 테스트 #{attempt + 1} 실패 | "
                f"error={str(e)}"
            )
            self._update_progress_stats("future", False)

    def print_results(self):
        """테스트 결과 출력"""
        total_time = time.time() - self.start_time
        
        logger.info("\n=== 바이빗 웹소켓 연결 테스트 최종 결과 ===")
        logger.info(f"총 소요시간: {total_time/60:.1f}분")
        
        # 현물 결과
        spot = self.results["spot"]
        logger.info("\n[현물]")
        logger.info(f"성공: {spot['success']}/{self.test_count} ({spot['success']/self.test_count*100:.1f}%)")
        if spot['success'] > 0:
            avg_time = spot['total_time'] / spot['success']
            logger.info(f"평균 연결 시간: {avg_time:.2f}초")
            
            # 재시도 통계
            retries = [a['retries'] for a in spot['attempts'] if 'retries' in a]
            if retries:
                avg_retries = sum(retries) / len(retries)
                max_retries = max(retries)
                logger.info(f"평균 재시도 횟수: {avg_retries:.1f}")
                logger.info(f"최대 재시도 횟수: {max_retries}")
        logger.info(f"최대 연속 실패: {spot['max_consecutive_fails']}회")
        
        # 선물 결과
        future = self.results["future"]
        logger.info("\n[선물]")
        logger.info(f"성공: {future['success']}/{self.test_count} ({future['success']/self.test_count*100:.1f}%)")
        if future['success'] > 0:
            avg_time = future['total_time'] / future['success']
            logger.info(f"평균 연결 시간: {avg_time:.2f}초")
            
            # 재시도 통계
            retries = [a['retries'] for a in future['attempts'] if 'retries' in a]
            if retries:
                avg_retries = sum(retries) / len(retries)
                max_retries = max(retries)
                logger.info(f"평균 재시도 횟수: {avg_retries:.1f}")
                logger.info(f"최대 재시도 횟수: {max_retries}")
        logger.info(f"최대 연속 실패: {future['max_consecutive_fails']}회")

    async def run_tests(self):
        """테스트 실행"""
        self.start_time = time.time()
        logger.info(f"\n[테스트] 바이빗 웹소켓 연결 테스트 시작 | {self.test_count}회 반복")
        
        for i in range(self.test_count):
            # 현물 테스트
            await self.test_spot_connection(i)
            await asyncio.sleep(1)  # 테스트 간 간격
            
            # 선물 테스트
            await self.test_future_connection(i)
            await asyncio.sleep(1)  # 테스트 간 간격
            
            # 10회마다 상세 진행 상황 출력
            if (i + 1) % 10 == 0:
                self._print_progress(i)
        
        self.print_results()

async def main():
    """메인 함수"""
    try:
        tester = ConnectionTester(test_count=100)
        await tester.run_tests()
    except KeyboardInterrupt:
        logger.info("\n테스트가 사용자에 의해 중단되었습니다.")
    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main()) 