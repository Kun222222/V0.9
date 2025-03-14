"""
오더북 기본 클래스 정의
"""

import asyncio
import time
from typing import Dict, List, Optional, Any

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.common.models import ValidationResult
from crosskimp.ob_collector.cpp.cpp_interface import send_orderbook_to_cpp

logger = get_unified_logger()


class OrderBook:
    """
    오더북 기본 클래스
    모든 거래소별 오더북 클래스의 부모 클래스
    """
    
    def __init__(self, exchangename: str, symbol: str, depth: int = 100):
        """
        Args:
            exchangename: 거래소 이름
            symbol: 심볼
            depth: 오더북 깊이
        """
        self.exchangename = exchangename
        self.symbol = symbol
        self.depth = depth
        
        # 오더북 데이터
        self.bids: List[List[float]] = []  # [[가격, 수량], ...]
        self.asks: List[List[float]] = []  # [[가격, 수량], ...]
        
        # 메타데이터
        self.last_update_time: int = 0
        self.last_update_id: int = 0
        self.last_sequence: Optional[int] = None
        
        # 출력 큐
        self.output_queue: Optional[asyncio.Queue] = None
        
    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """
        출력 큐 설정
        
        Args:
            queue: 출력 큐
        """
        self.output_queue = queue
        
    def update_orderbook(self, bids: List[List[float]], asks: List[List[float]], 
                        timestamp: Optional[int] = None, sequence: Optional[int] = None,
                        is_snapshot: bool = False) -> None:
        """
        오더북 데이터 업데이트
        
        Args:
            bids: 매수 호가 목록 [[가격, 수량], ...]
            asks: 매도 호가 목록 [[가격, 수량], ...]
            timestamp: 타임스탬프
            sequence: 시퀀스 번호
            is_snapshot: 스냅샷 여부
        """
        # 자식 클래스에서 구현
        raise NotImplementedError
        
    def to_dict(self) -> dict:
        """
        오더북 데이터를 딕셔너리로 변환
        
        Returns:
            오더북 데이터 딕셔너리
        """
        return {
            "exchangename": self.exchangename,
            "symbol": self.symbol,
            "bids": self.bids,
            "asks": self.asks,
            "timestamp": self.last_update_time,
            "sequence": self.last_sequence or self.last_update_id
        }
        
    async def send_to_cpp(self) -> None:
        """
        오더북 데이터를 C++로 전송
        """
        try:
            data = self.to_dict()
            await send_orderbook_to_cpp(self.exchangename, data)
        except Exception as e:
            logger.error(f"{self.exchangename} {self.symbol} C++ 전송 실패: {str(e)}", exc_info=True)
            
    async def send_to_queue(self) -> None:
        """
        오더북 데이터를 큐로 전송
        """
        if self.output_queue:
            try:
                data = self.to_dict()
                await self.output_queue.put((self.exchangename, data))
            except Exception as e:
                logger.error(f"{self.exchangename} {self.symbol} 큐 전송 실패: {str(e)}", exc_info=True)
                
    def should_process_update(self, timestamp: Optional[int] = None, sequence: Optional[int] = None) -> bool:
        """
        업데이트 처리 여부 결정
        
        Args:
            timestamp: 타임스탬프
            sequence: 시퀀스 번호
            
        Returns:
            처리 여부
        """
        # 자식 클래스에서 구현
        return True 