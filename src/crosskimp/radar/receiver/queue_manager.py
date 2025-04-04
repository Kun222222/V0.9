"""
큐 관리 클래스

오더북 데이터 큐를 관리하고 배치 처리를 준비합니다.
"""

import asyncio
import time
from typing import Dict, Any, List, Callable, Optional
import logging
from queue import Queue, Empty

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.config.app_config import get_config

class QueueManager:
    """
    큐 관리 클래스
    
    오더북 데이터 큐를 관리하고 배치 처리를 준비합니다.
    멀티스레딩 환경에서 안전하게 데이터를 관리합니다.
    """
    
    def __init__(self, max_queue_size: int = 10000, batch_size: int = 100):
        """
        초기화
        
        Args:
            max_queue_size: 최대 큐 크기 (기본값: 10000)
            batch_size: 배치 처리 크기 (기본값: 100)
        """
        self.logger = get_unified_logger(SystemComponent.RADAR.value)
        self.logger.info("큐 관리자 초기화 중...")
        
        # 설정 로드
        self.config = get_config()
        
        # 큐 설정
        self.max_queue_size = max_queue_size
        self.batch_size = batch_size
        
        # 큐 생성
        self.queue = Queue(maxsize=max_queue_size)
        
        # 배치 처리 콜백
        self.batch_callbacks = []
        
        # 상태 변수
        self.is_running = False
        self.stats = {
            'enqueued': 0,
            'dequeued': 0,
            'batches': 0,
            'overflow': 0,
            'start_time': 0,
            'last_report_time': 0
        }
        
        self.logger.info(f"큐 관리자 초기화 완료 (최대 큐 크기: {max_queue_size}, 배치 크기: {batch_size})")
    
    async def start(self) -> bool:
        """
        큐 관리자 시작
        
        Returns:
            bool: 시작 성공 여부
        """
        try:
            self.logger.info("큐 관리자 시작 중...")
            
            if self.is_running:
                self.logger.warning("큐 관리자가 이미 실행 중입니다.")
                return True
            
            # 상태 초기화
            self.is_running = True
            self.stats['start_time'] = time.time()
            self.stats['last_report_time'] = time.time()
            
            # 큐 처리 태스크 시작
            self.process_task = asyncio.create_task(self._process_queue())
            self.stats_task = asyncio.create_task(self._report_stats())
            
            self.logger.info("큐 관리자 시작 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"큐 관리자 시작 중 오류: {str(e)}", exc_info=True)
            self.is_running = False
            return False
    
    async def stop(self) -> None:
        """큐 관리자 종료"""
        if not self.is_running:
            return
            
        self.logger.info("큐 관리자 종료 중...")
        
        # 상태 변경
        self.is_running = False
        
        # 태스크 종료 대기
        if hasattr(self, 'process_task'):
            try:
                await asyncio.wait_for(self.process_task, timeout=5)
            except asyncio.TimeoutError:
                self.logger.warning("큐 처리 태스크 종료 대기 시간 초과")
        
        if hasattr(self, 'stats_task'):
            try:
                await asyncio.wait_for(self.stats_task, timeout=5)
            except asyncio.TimeoutError:
                self.logger.warning("통계 태스크 종료 대기 시간 초과")
        
        # 큐 정리
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except Empty:
                break
        
        self.logger.info("큐 관리자 종료 완료")
    
    def add_to_queue(self, data: Dict[str, Any]) -> bool:
        """
        데이터를 큐에 추가
        
        Args:
            data: 오더북 데이터
            
        Returns:
            bool: 추가 성공 여부
        """
        if not self.is_running:
            return False
            
        try:
            # 큐가 가득 찼을 경우 오래된 항목 제거
            if self.queue.full():
                try:
                    self.queue.get_nowait()
                    self.queue.task_done()
                    self.stats['overflow'] += 1
                except Empty:
                    pass
            
            # 큐에 데이터 추가
            self.queue.put_nowait(data)
            self.stats['enqueued'] += 1
            return True
            
        except Exception as e:
            self.logger.error(f"큐 추가 중 오류: {str(e)}")
            return False
    
    def add_batch_callback(self, callback: Callable[[List[Dict[str, Any]]], None]) -> None:
        """
        배치 처리 콜백 등록
        
        Args:
            callback: 배치 데이터를 처리할 콜백 함수
        """
        if callback not in self.batch_callbacks:
            self.batch_callbacks.append(callback)
            self.logger.debug(f"배치 콜백 함수 등록됨 (총 {len(self.batch_callbacks)}개)")
    
    async def _process_queue(self) -> None:
        """큐 처리 루프"""
        self.logger.info("큐 처리 루프 시작")
        
        while self.is_running:
            try:
                # 배치 크기 결정 (현재 큐 크기와 설정된 배치 크기 중 작은 값)
                batch_size = min(self.batch_size, self.queue.qsize())
                
                # 배치 생성
                if batch_size > 0:
                    batch = []
                    for _ in range(batch_size):
                        try:
                            item = self.queue.get_nowait()
                            batch.append(item)
                            self.queue.task_done()
                            self.stats['dequeued'] += 1
                        except Empty:
                            break
                    
                    # 배치 크기가 0보다 크면 콜백 호출
                    if len(batch) > 0:
                        self.stats['batches'] += 1
                        
                        # 콜백 호출
                        for callback in self.batch_callbacks:
                            try:
                                callback(batch)
                            except Exception as e:
                                self.logger.error(f"배치 콜백 실행 중 오류: {str(e)}")
                
                # 잠시 대기
                await asyncio.sleep(0.001)  # 1ms 대기
                
            except Exception as e:
                self.logger.error(f"큐 처리 중 오류: {str(e)}")
                await asyncio.sleep(1)  # 오류 발생 시 1초 대기
        
        self.logger.info("큐 처리 루프 종료")
    
    async def _report_stats(self) -> None:
        """통계 보고 루프"""
        self.logger.info("통계 보고 루프 시작")
        
        while self.is_running:
            try:
                current_time = time.time()
                elapsed = current_time - self.stats['last_report_time']
                
                if elapsed >= 10:  # 10초마다 통계 보고
                    total_elapsed = current_time - self.stats['start_time']
                    rate = self.stats['enqueued'] / total_elapsed if total_elapsed > 0 else 0
                    
                    self.logger.info(
                        f"큐 통계: 입력 {self.stats['enqueued']}건 "
                        f"(평균 {rate:.1f}건/초), "
                        f"출력 {self.stats['dequeued']}건, "
                        f"배치 {self.stats['batches']}개, "
                        f"오버플로우 {self.stats['overflow']}건, "
                        f"현재 큐 크기 {self.queue.qsize()}건"
                    )
                    
                    self.stats['last_report_time'] = current_time
                
                await asyncio.sleep(1)  # 1초 대기
                
            except Exception as e:
                self.logger.error(f"통계 보고 중 오류: {str(e)}")
                await asyncio.sleep(5)  # 오류 발생 시 5초 대기
        
        self.logger.info("통계 보고 루프 종료")


# 싱글톤 인스턴스
_queue_manager_instance = None

def get_queue_manager() -> QueueManager:
    """
    큐 관리자 싱글톤 인스턴스 반환
    
    Returns:
        QueueManager: 큐 관리자 인스턴스
    """
    global _queue_manager_instance
    
    if _queue_manager_instance is None:
        _queue_manager_instance = QueueManager()
    
    return _queue_manager_instance 