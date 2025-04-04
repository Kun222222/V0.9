"""
오더북 캐싱 클래스

최신 오더북 데이터를 메모리에 캐싱하여 빠른 접근을 제공합니다.
"""

import asyncio
import threading
import time
from typing import Dict, Any, Optional
import logging

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent

class OrderbookCache:
    """
    오더북 캐싱 클래스
    
    최신 오더북 데이터를 메모리에 캐싱하여 빠른 접근을 제공합니다.
    스레드 안전한 접근을 위해 락을 사용합니다.
    """
    
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(SystemComponent.RADAR.value)
        self.logger.info("오더북 캐시 초기화 중...")
        
        # 캐시 데이터 구조: {exchange: {symbol: data}}
        self.cache = {}
        
        # 스레드 안전한 접근을 위한 락
        self.lock = threading.RLock()
        
        # 캐시 통계
        self.stats = {
            'updates': 0,
            'hits': 0,
            'misses': 0,
            'last_update_time': 0
        }
        
        self.logger.info("오더북 캐시 초기화 완료")
    
    def update(self, data: Dict[str, Any]) -> bool:
        """
        캐시 업데이트
        
        Args:
            data: 오더북 데이터
            
        Returns:
            bool: 업데이트 성공 여부
        """
        exchange = data.get('exchange')
        symbol = data.get('symbol')
        
        if not exchange or not symbol:
            return False
        
        with self.lock:
            # 필요한 경우 교환소 딕셔너리 생성
            if exchange not in self.cache:
                self.cache[exchange] = {}
            
            # 데이터 업데이트
            self.cache[exchange][symbol] = data
            
            # 통계 업데이트
            self.stats['updates'] += 1
            self.stats['last_update_time'] = time.time()
            
            # 주기적인 로깅 (1000번마다)
            if self.stats['updates'] % 1000 == 0:
                self.logger.debug(f"오더북 캐시 업데이트: {self.stats['updates']}건, "
                                f"캐시 크기: {len(self.cache)}개 거래소, "
                                f"총 {self._count_symbols()}개 심볼")
            
            return True
    
    def get(self, exchange: str, symbol: str) -> Optional[Dict[str, Any]]:
        """
        캐시된 데이터 조회
        
        Args:
            exchange: 거래소 코드
            symbol: 심볼명
            
        Returns:
            Optional[Dict[str, Any]]: 캐시된 오더북 데이터 또는 None
        """
        with self.lock:
            if exchange in self.cache and symbol in self.cache[exchange]:
                self.stats['hits'] += 1
                return self.cache[exchange][symbol].copy()  # 복사본 반환
            
            self.stats['misses'] += 1
            return None
    
    def get_all(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        모든 캐시된 데이터 조회
        
        Returns:
            Dict: 모든 캐시된 데이터의 복사본
        """
        with self.lock:
            # 깊은 복사 수행
            result = {}
            for exchange, symbols in self.cache.items():
                result[exchange] = {}
                for symbol, data in symbols.items():
                    result[exchange][symbol] = data.copy()
            
            return result
    
    def clear(self) -> None:
        """캐시 초기화"""
        with self.lock:
            self.cache.clear()
            self.logger.info("오더북 캐시가 초기화되었습니다.")
    
    def _count_symbols(self) -> int:
        """
        캐싱된 총 심볼 수 계산
        
        Returns:
            int: 총 심볼 수
        """
        count = 0
        for exchange in self.cache:
            count += len(self.cache[exchange])
        return count 