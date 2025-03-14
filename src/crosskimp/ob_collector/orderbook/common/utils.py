"""
오더북 수집기에서 사용하는 공통 유틸리티 함수 정의
"""

import asyncio
import time
import json
import logging
from typing import Dict, List, Optional, Any, Tuple, Callable

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger

logger = get_unified_logger()


class EventEmitter:
    """
    이벤트 발행자 클래스
    메시지 수신 → 파싱 → 오더북 업데이트 → 데이터 전송의 흐름을 이벤트 기반으로 구현
    """
    
    def __init__(self):
        """이벤트 리스너 딕셔너리 초기화"""
        self.listeners = {}
        
    def on(self, event: str, callback: Callable):
        """
        이벤트 리스너 등록
        
        Args:
            event: 이벤트 이름
            callback: 이벤트 발생 시 호출할 콜백 함수
        """
        if event not in self.listeners:
            self.listeners[event] = []
        self.listeners[event].append(callback)
        
    def off(self, event: str, callback: Optional[Callable] = None):
        """
        이벤트 리스너 제거
        
        Args:
            event: 이벤트 이름
            callback: 제거할 콜백 함수 (None인 경우 해당 이벤트의 모든 리스너 제거)
        """
        if event not in self.listeners:
            return
            
        if callback is None:
            self.listeners[event] = []
        else:
            self.listeners[event] = [cb for cb in self.listeners[event] if cb != callback]
        
    async def emit(self, event: str, *args, **kwargs):
        """
        이벤트 발행
        
        Args:
            event: 이벤트 이름
            *args, **kwargs: 콜백 함수에 전달할 인자
        """
        if event not in self.listeners:
            return
            
        for callback in self.listeners[event]:
            try:
                result = callback(*args, **kwargs)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"이벤트 처리 중 오류 발생: {str(e)}", exc_info=True)


class ReconnectStrategy:
    """
    재연결 전략 클래스
    지수 백오프 알고리즘을 사용하여 재연결 간격 계산
    """
    
    def __init__(self, initial_delay: float = 1.0, max_delay: float = 60.0, 
                 multiplier: float = 2.0, max_attempts: int = 0):
        """
        Args:
            initial_delay: 초기 재연결 간격 (초)
            max_delay: 최대 재연결 간격 (초)
            multiplier: 재연결 간격 증가 배수
            max_attempts: 최대 재연결 시도 횟수 (0인 경우 무제한)
        """
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.max_attempts = max_attempts
        self.current_delay = initial_delay
        self.attempts = 0
        
    def next_delay(self) -> float:
        """
        다음 재연결 간격 계산
        
        Returns:
            다음 재연결 간격 (초)
        """
        if self.max_attempts > 0 and self.attempts >= self.max_attempts:
            return -1  # 최대 시도 횟수 초과
            
        delay = min(self.current_delay, self.max_delay)
        self.current_delay = min(self.current_delay * self.multiplier, self.max_delay)
        self.attempts += 1
        return delay
        
    def reset(self):
        """재연결 전략 초기화"""
        self.current_delay = self.initial_delay
        self.attempts = 0


def format_orderbook_for_log(orderbook: Dict[str, Any], max_depth: int = 3) -> str:
    """
    로깅을 위한 오더북 포맷팅
    
    Args:
        orderbook: 오더북 데이터
        max_depth: 로깅할 최대 깊이
        
    Returns:
        포맷팅된 오더북 문자열
    """
    if not orderbook:
        return "빈 오더북"
        
    bids = orderbook.get("bids", [])
    asks = orderbook.get("asks", [])
    
    bids_str = ", ".join([f"{bid[0]}@{bid[1]}" for bid in bids[:max_depth]])
    asks_str = ", ".join([f"{ask[0]}@{ask[1]}" for ask in asks[:max_depth]])
    
    return f"매수: [{bids_str}], 매도: [{asks_str}]"


def calculate_spread(bids: List[List[float]], asks: List[List[float]]) -> Tuple[float, float]:
    """
    스프레드 계산
    
    Args:
        bids: 매수 호가 목록 [[가격, 수량], ...]
        asks: 매도 호가 목록 [[가격, 수량], ...]
        
    Returns:
        (스프레드, 스프레드 비율(%))
    """
    if not bids or not asks:
        return 0.0, 0.0
        
    best_bid = max(bid[0] for bid in bids)
    best_ask = min(ask[0] for ask in asks)
    
    spread = best_ask - best_bid
    spread_pct = (spread / best_bid) * 100 if best_bid > 0 else 0.0
    
    return spread, spread_pct


def is_price_inverted(bids: List[List[float]], asks: List[List[float]]) -> bool:
    """
    가격 역전 여부 확인
    
    Args:
        bids: 매수 호가 목록 [[가격, 수량], ...]
        asks: 매도 호가 목록 [[가격, 수량], ...]
        
    Returns:
        가격 역전 여부
    """
    if not bids or not asks:
        return False
        
    best_bid = max(bid[0] for bid in bids)
    best_ask = min(ask[0] for ask in asks)
    
    return best_bid >= best_ask 