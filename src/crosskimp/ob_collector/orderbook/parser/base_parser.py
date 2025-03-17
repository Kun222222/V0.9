"""
기본 파서 클래스

이 모듈은 다양한 거래소의 웹소켓 메시지를 파싱하는 기본 클래스를 제공합니다.
"""

from typing import Dict, List, Optional, Any, Tuple
import json
import time
from abc import ABC, abstractmethod

from crosskimp.logger.logger import get_unified_logger

logger = get_unified_logger()

class BaseParser(ABC):
    """
    기본 파서 클래스
    
    모든 거래소별 파서 클래스의 기본 클래스입니다.
    """
    def __init__(self, exchangename: str):
        """
        기본 파서 초기화
        
        Args:
            exchangename: 거래소 이름
        """
        self.exchangename = exchangename
        
        # 파싱 통계
        self.stats = {
            "total_parsed": 0,
            "valid_parsed": 0,
            "invalid_parsed": 0,
            "processing_errors": 0,
            "filtered_zero_bids": 0,  # 0인 매수 호가 필터링 수
            "filtered_zero_asks": 0,  # 0인 매도 호가 필터링 수
        }
    
    @abstractmethod
    def parse_message(self, raw_message: str) -> Optional[Dict[str, Any]]:
        """
        웹소켓 메시지 파싱
        
        Args:
            raw_message: 원본 웹소켓 메시지
            
        Returns:
            Optional[Dict[str, Any]]: 파싱된 오더북 데이터 또는 None
        """
        pass
    
    def filter_zero_values(self, bids: List[Dict[str, float]], asks: List[Dict[str, float]]) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
        """
        0인 값을 필터링하는 공통 메서드
        
        Args:
            bids: 매수 호가 목록 [{'price': float, 'size': float}, ...]
            asks: 매도 호가 목록 [{'price': float, 'size': float}, ...]
            
        Returns:
            Tuple[List[Dict[str, float]], List[Dict[str, float]]]: 필터링된 매수/매도 호가 목록
        """
        # 매수 호가 필터링
        filtered_bids = []
        zero_bids_count = 0
        
        for bid in bids:
            if bid['size'] > 0:
                filtered_bids.append(bid)
            else:
                zero_bids_count += 1
        
        # 매도 호가 필터링
        filtered_asks = []
        zero_asks_count = 0
        
        for ask in asks:
            if ask['size'] > 0:
                filtered_asks.append(ask)
            else:
                zero_asks_count += 1
        
        # 통계 업데이트
        self.stats["filtered_zero_bids"] += zero_bids_count
        self.stats["filtered_zero_asks"] += zero_asks_count
        
        # 필터링 로그 (많은 항목이 필터링된 경우에만)
        if zero_bids_count > 5 or zero_asks_count > 5:
            self.log_debug(
                f"0인 값 필터링 | "
                f"매수: {zero_bids_count}건, 매도: {zero_asks_count}건"
            )
        
        return filtered_bids, filtered_asks
    
    def filter_zero_values_array(self, bids: List[List[float]], asks: List[List[float]]) -> Tuple[List[List[float]], List[List[float]]]:
        """
        0인 값을 필터링하는 공통 메서드 (배열 형식용)
        
        Args:
            bids: 매수 호가 목록 [[price, size], ...]
            asks: 매도 호가 목록 [[price, size], ...]
            
        Returns:
            Tuple[List[List[float]], List[List[float]]]: 필터링된 매수/매도 호가 목록
        """
        # 매수 호가 필터링
        filtered_bids = []
        zero_bids_count = 0
        
        for bid in bids:
            if len(bid) >= 2 and bid[1] > 0:
                filtered_bids.append(bid)
            else:
                zero_bids_count += 1
        
        # 매도 호가 필터링
        filtered_asks = []
        zero_asks_count = 0
        
        for ask in asks:
            if len(ask) >= 2 and ask[1] > 0:
                filtered_asks.append(ask)
            else:
                zero_asks_count += 1
        
        # 통계 업데이트
        self.stats["filtered_zero_bids"] += zero_bids_count
        self.stats["filtered_zero_asks"] += zero_asks_count
        
        # 필터링 로그 (많은 항목이 필터링된 경우에만)
        if zero_bids_count > 5 or zero_asks_count > 5:
            self.log_debug(
                f"0인 값 필터링 | "
                f"매수: {zero_bids_count}건, 매도: {zero_asks_count}건"
            )
        
        return filtered_bids, filtered_asks
    
    def log_error(self, msg: str, exc_info: bool = True) -> None:
        """
        오류 로깅
        
        Args:
            msg: 오류 메시지
            exc_info: 예외 정보 포함 여부
        """
        logger.error(f"[{self.exchangename}] {msg}", exc_info=exc_info)
        self.stats["processing_errors"] += 1
    
    def log_info(self, msg: str) -> None:
        """
        정보 로깅
        
        Args:
            msg: 정보 메시지
        """
        logger.info(f"[{self.exchangename}] {msg}")
    
    def log_debug(self, msg: str) -> None:
        """
        디버그 로깅
        
        Args:
            msg: 디버그 메시지
        """
        logger.debug(f"[{self.exchangename}] {msg}")
    
    def log_warning(self, msg: str) -> None:
        """
        경고 로깅
        
        Args:
            msg: 경고 메시지
        """
        logger.warning(f"[{self.exchangename}] {msg}")
    
    def get_stats(self) -> Dict[str, int]:
        """
        파싱 통계 반환
        
        Returns:
            Dict[str, int]: 파싱 통계
        """
        return self.stats 