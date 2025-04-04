"""
레이더 프로세서

오더북 데이터를 수신하고 처리하여 차익거래 기회를 찾는 중앙 제어 클래스입니다.
"""

import asyncio
import time
from typing import Dict, Any, List, Optional, Callable

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.config.app_config import get_config
from crosskimp.radar.receiver.data_receiver import DataReceiver
from crosskimp.radar.receiver.queue_manager import QueueManager
from crosskimp.radar.receiver.batch_processor import BatchProcessor
from crosskimp.radar.caching.orderbook_cache import OrderbookCache

class RadarProcessor:
    """
    레이더 프로세서
    
    오더북 데이터를 수신하고 처리하여 차익거래 기회를 찾는 중앙 제어 클래스입니다.
    데이터 수신부와 계산부를 연결하고 시스템 라이프사이클을 관리합니다.
    """
    
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(SystemComponent.RADAR.value)
        self.logger.info("레이더 프로세서 초기화 중...")
        
        # 설정 로드
        self.config = get_config()
        
        # 컴포넌트 초기화
        self.data_receiver = DataReceiver()
        self.queue_manager = QueueManager()
        self.batch_processor = BatchProcessor()
        self.orderbook_cache = OrderbookCache()
        
        # 결과 콜백
        self.result_callbacks = []
        
        # 상태 변수
        self.is_running = False
        
        self.logger.info("레이더 프로세서 초기화 완료")
    
    async def start(self) -> bool:
        """
        레이더 프로세서 시작
        
        Returns:
            bool: 시작 성공 여부
        """
        try:
            self.logger.info("레이더 프로세서 시작 중...")
            
            if self.is_running:
                self.logger.warning("레이더 프로세서가 이미 실행 중입니다.")
                return True
            
            # 상태 초기화
            self.is_running = True
            
            # 큐 관리자 시작
            await self.queue_manager.start()
            
            # 배치 프로세서 시작
            await self.batch_processor.start()
            
            # 배치 프로세서에 큐 관리자 연결
            self.queue_manager.add_batch_callback(self.batch_processor.process_batch)
            
            # 데이터 리시버 시작 및 큐 관리자 연결
            self.data_receiver.add_callback(self.queue_manager.add_to_queue)
            await self.data_receiver.start()
            
            # 배치 프로세서 결과 콜백 등록
            for callback in self.result_callbacks:
                self.batch_processor.add_result_callback(callback)
            
            self.logger.info("레이더 프로세서 시작 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"레이더 프로세서 시작 중 오류: {str(e)}", exc_info=True)
            self.is_running = False
            await self.stop()
            return False
    
    async def stop(self) -> None:
        """레이더 프로세서 종료"""
        if not self.is_running:
            return
            
        self.logger.info("레이더 프로세서 종료 중...")
        
        # 상태 변경
        self.is_running = False
        
        # 컴포넌트 종료 (역순)
        await self.data_receiver.stop()
        await self.batch_processor.stop()
        await self.queue_manager.stop()
        
        self.logger.info("레이더 프로세서 종료 완료")
    
    def add_result_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        결과 처리 콜백 등록
        
        Args:
            callback: 계산 결과를 처리할 콜백 함수
        """
        if callback not in self.result_callbacks:
            self.result_callbacks.append(callback)
            
            # 실행 중인 경우 배치 프로세서에도 콜백 등록
            if self.is_running:
                self.batch_processor.add_result_callback(callback)
                
            self.logger.debug(f"결과 콜백 함수 등록됨 (총 {len(self.result_callbacks)}개)")
    
    def get_orderbook_data(self, exchange: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        현재 캐싱된 오더북 데이터 조회
        
        Args:
            exchange: 교환소명
            symbol: 심볼명
            
        Returns:
            Optional[Dict[str, Any]]: 오더북 데이터 또는 None
        """
        return self.orderbook_cache.get(exchange, symbol)
    
    def get_all_orderbooks(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        모든 캐싱된 오더북 데이터 조회
        
        Returns:
            Dict[str, Dict[str, Dict[str, Any]]]: 모든 오더북 데이터
        """
        return self.orderbook_cache.get_all()