"""
배치 프로세서

오더북 데이터를 배치로 처리하는 클래스입니다.
"""

import time
import asyncio
import multiprocessing as mp
from typing import Dict, Any, List, Set, Optional, Callable
import logging
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import pickle

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.config.app_config import get_config
from crosskimp.radar.caching.orderbook_cache import OrderbookCache

# 프로세스 생성 방식을 'spawn'으로 설정 (fork 대신)
# fork는 부모 프로세스의 모든 자원을 복사하지만 spawn은 새로운 인터프리터를 시작하므로 더 안전
mp_context = mp.get_context('spawn')

class BatchProcessor:
    """
    배치 프로세서
    
    오더북 데이터를 효율적으로 배치 처리합니다.
    멀티프로세싱을 활용하여 계산 성능을 최적화합니다.
    """
    
    def __init__(self, max_workers: int = None):
        """
        초기화
        
        Args:
            max_workers: 최대 작업자 수 (기본값: CPU 코어 수)
        """
        self.logger = get_unified_logger(SystemComponent.RADAR.value)
        self.logger.info("배치 프로세서 초기화 중...")
        
        # 설정 로드
        self.config = get_config()
        
        # 캐시 접근
        self.orderbook_cache = OrderbookCache()
        
        # 처리 콜백
        self.result_callbacks = []
        
        # 쓰레드 풀 설정 - RLock 객체 직렬화 문제로 인해 ProcessPool 대신 ThreadPool 사용
        self.max_workers = max_workers
        self.executor = None
        
        # 상태 변수
        self.is_running = False
        self.stats = {
            'batches_received': 0,
            'items_processed': 0,
            'processing_time': 0,
            'avg_processing_time': 0,
            'start_time': 0,
            'last_report_time': 0
        }
        
        self.logger.info(f"배치 프로세서 초기화 완료 (최대 작업자: {max_workers or '자동'})")
    
    async def start(self) -> bool:
        """
        배치 프로세서 시작
        
        Returns:
            bool: 시작 성공 여부
        """
        try:
            self.logger.info("배치 프로세서 시작 중...")
            
            if self.is_running:
                self.logger.warning("배치 프로세서가 이미 실행 중입니다.")
                return True
            
            # 상태 초기화
            self.is_running = True
            self.stats['start_time'] = time.time()
            self.stats['last_report_time'] = time.time()
            
            # 쓰레드 풀 초기화 (ProcessPoolExecutor 대신 ThreadPoolExecutor 사용)
            self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
            
            # 통계 태스크 시작
            self.stats_task = asyncio.create_task(self._report_stats())
            
            self.logger.info("배치 프로세서 시작 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"배치 프로세서 시작 중 오류: {str(e)}", exc_info=True)
            self.is_running = False
            return False
    
    async def stop(self) -> None:
        """배치 프로세서 종료"""
        if not self.is_running:
            return
            
        self.logger.info("배치 프로세서 종료 중...")
        
        # 상태 변경
        self.is_running = False
        
        # 태스크 종료 대기
        if hasattr(self, 'stats_task'):
            try:
                await asyncio.wait_for(self.stats_task, timeout=5)
            except asyncio.TimeoutError:
                self.logger.warning("통계 태스크 종료 대기 시간 초과")
        
        # 쓰레드 풀 종료
        if self.executor:
            self.executor.shutdown(wait=True)
            self.executor = None
        
        self.logger.info("배치 프로세서 종료 완료")
    
    def add_result_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        결과 처리 콜백 등록
        
        Args:
            callback: 계산 결과를 처리할 콜백 함수
        """
        if callback not in self.result_callbacks:
            self.result_callbacks.append(callback)
            self.logger.debug(f"결과 콜백 함수 등록됨 (총 {len(self.result_callbacks)}개)")
    
    def process_batch(self, batch: List[Dict[str, Any]]) -> None:
        """
        오더북 데이터 배치 처리
        
        Args:
            batch: 오더북 데이터 배치
        """
        if not self.is_running or not batch:
            return
            
        try:
            start_time = time.time()
            
            # 배치 통계 업데이트
            self.stats['batches_received'] += 1
            self.stats['items_processed'] += len(batch)
            
            # 교환소별로 데이터 그룹화
            grouped_data = defaultdict(list)
            for item in batch:
                exchange = item.get('exchange')
                if exchange:
                    grouped_data[exchange].append(item)
            
            # 각 교환소 데이터를 스레드 풀로 처리
            for exchange, exchange_data in grouped_data.items():
                if not exchange_data:
                    continue
                    
                # 캐시 업데이트
                for item in exchange_data:
                    self.orderbook_cache.update(item)
                
                # 스레드 풀로 계산 함수 실행
                # 스레드는 메모리를 공유하므로 직렬화 문제가 없음
                future = self.executor.submit(
                    self._dummy_calculation,
                    exchange,
                    exchange_data
                )
                future.add_done_callback(self._handle_calculation_result)
            
            # 처리 시간 통계 업데이트
            elapsed = time.time() - start_time
            self.stats['processing_time'] += elapsed
            
            if self.stats['batches_received'] > 0:
                self.stats['avg_processing_time'] = (
                    self.stats['processing_time'] / self.stats['batches_received']
                )
            
        except Exception as e:
            self.logger.error(f"배치 처리 중 오류: {str(e)}", exc_info=True)
    
    def _handle_calculation_result(self, future) -> None:
        """
        계산 결과 처리
        
        Args:
            future: 완료된 작업 퓨처
        """
        try:
            result = future.result()
            
            if result:
                # 결과 콜백 호출
                for callback in self.result_callbacks:
                    try:
                        callback(result)
                    except Exception as e:
                        self.logger.error(f"결과 콜백 실행 중 오류: {str(e)}")
                        
        except (TypeError, pickle.PickleError) as e:
            # 직렬화 관련 오류 처리
            self.logger.error(f"계산 결과 직렬화 오류 (일반적으로 Lock 객체와 관련): {str(e)}")
        except Exception as e:
            self.logger.error(f"계산 결과 처리 중 오류: {str(e)}")
    
    def _dummy_calculation(self, exchange: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        더미 계산 함수 (실제 계산 로직 구현 전까지 사용)
        
        Args:
            exchange: 교환소 이름
            data: 오더북 데이터 목록
            
        Returns:
            Dict[str, Any]: 계산 결과
        """
        # 스레드 풀에서는 직접 데이터를 처리할 수 있음
        symbols = set()
        for item in data:
            if 'symbol' in item:
                symbols.add(item['symbol'])
        
        # 간단한 계산 결과 반환
        return {
            'exchange': exchange,
            'timestamp': time.time(),
            'symbols_count': len(symbols),
            'data_count': len(data),
            'symbols': list(symbols),
            'processed': True
        }
    
    async def _report_stats(self) -> None:
        """통계 보고 루프"""
        self.logger.info("통계 보고 루프 시작")
        
        while self.is_running:
            try:
                current_time = time.time()
                elapsed = current_time - self.stats['last_report_time']
                
                if elapsed >= 10:  # 10초마다 통계 보고
                    total_elapsed = current_time - self.stats['start_time']
                    rate = self.stats['items_processed'] / total_elapsed if total_elapsed > 0 else 0
                    
                    self.logger.info(
                        f"배치 처리 통계: 배치 {self.stats['batches_received']}개, "
                        f"항목 {self.stats['items_processed']}건 "
                        f"(평균 {rate:.1f}건/초), "
                        f"평균 처리 시간 {self.stats['avg_processing_time']*1000:.2f}ms"
                    )
                    
                    self.stats['last_report_time'] = current_time
                
                await asyncio.sleep(1)  # 1초 대기
                
            except Exception as e:
                self.logger.error(f"통계 보고 중 오류: {str(e)}")
                await asyncio.sleep(5)  # 오류 발생 시 5초 대기
        
        self.logger.info("통계 보고 루프 종료")


# 싱글톤 인스턴스
_batch_processor_instance = None

def get_batch_processor() -> BatchProcessor:
    """
    배치 프로세서 싱글톤 인스턴스 반환
    
    Returns:
        BatchProcessor: 배치 프로세서 인스턴스
    """
    global _batch_processor_instance
    
    if _batch_processor_instance is None:
        _batch_processor_instance = BatchProcessor()
    
    return _batch_processor_instance 