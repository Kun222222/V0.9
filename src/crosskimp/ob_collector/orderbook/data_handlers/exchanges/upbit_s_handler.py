"""
업비트 현물 데이터 핸들러 모듈

업비트 현물 거래소의 오더북 데이터 처리를 담당하는 클래스를 제공합니다.
"""

import json
import time
import asyncio
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR
from crosskimp.ob_collector.orderbook.data_handlers.ob_data_manager import get_orderbook_data_manager

class UpbitSpotDataHandler:
    """
    업비트 현물 데이터 핸들러 클래스
    
    업비트 현물 거래소의 데이터 처리를 담당합니다.
    """
    
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        self.exchange_code = Exchange.UPBIT.value
        self.exchange_name_kr = EXCHANGE_NAMES_KR[self.exchange_code]
        self.orderbooks = {}  # 심볼별 오더북 저장
        self.sequence_numbers = {}  # 심볼별 시퀀스 번호 (타임스탬프 값 사용)
        self.last_update_time = {}  # 심볼별 마지막 업데이트 시간
        
        # 업비트는 오더북 깊이가 15로 고정
        self.orderbook_depth = 15
        
        # 데이터 관리자 가져오기
        self.data_manager = get_orderbook_data_manager()
        
        self.logger.info(f"{self.exchange_name_kr} 데이터 핸들러 초기화 (오더북 깊이: {self.orderbook_depth}, 고정)")
        
    async def get_orderbook_snapshot(self, symbol: str) -> Dict[str, Any]:
        """
        오더북 스냅샷 가져오기 (REST API 사용)
        
        Args:
            symbol: 심볼 코드 (예: "btc")
            
        Returns:
            Dict[str, Any]: 오더북 스냅샷 데이터
        """
        # 캐시된 데이터가 있으면 반환
        if symbol in self.orderbooks:
            return self.orderbooks[symbol]
            
        # 캐시된 데이터가 없으면 빈 오더북 반환
        # 실제로는 REST API를 호출해야 하지만, 업비트는 웹소켓으로 전체 오더북이 항상 제공되므로 불필요
        empty_orderbook = {
            "exchange": Exchange.UPBIT.value,
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "bids": [],
            "asks": []
        }
        return empty_orderbook
    
    def process_orderbook_update(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        오더북 메시지 처리
        
        Args:
            message: 수신된 오더북 메시지
            
        Returns:
            Dict[str, Any]: 처리된 오더북 데이터
        """
        # 심볼 추출
        symbol = message.get("symbol", "").lower()
        if not symbol:
            self.logger.error(f"{self.exchange_name_kr} 심볼 정보 없음")
            return {}
            
        # 타임스탬프 추출
        timestamp = message.get("timestamp", int(time.time() * 1000))
        
        # 데이터 추출
        bids = message.get("bids", [])
        asks = message.get("asks", [])
        
        # 시퀀스 번호에 타임스탬프 값 저장
        self.sequence_numbers[symbol] = timestamp
        
        # 업비트는 매번 전체 오더북을 전송하므로, 바로 저장
        orderbook = {
            "exchange": Exchange.UPBIT.value,
            "symbol": symbol,
            "timestamp": timestamp,
            "sequence": self.sequence_numbers[symbol],  # 시퀀스 번호 추가
            "bids": sorted(bids, key=lambda x: float(x[0]), reverse=True),  # 매수 호가는 내림차순
            "asks": sorted(asks, key=lambda x: float(x[0]))  # 매도 호가는 오름차순
        }
        
        # 오더북 저장
        self.orderbooks[symbol] = orderbook
        self.last_update_time[symbol] = timestamp
        
        # 데이터 관리자에 오더북 로깅
        self.data_manager.log_orderbook_data(Exchange.UPBIT.value, symbol, orderbook)
        
        return orderbook
        
    def normalize_symbol(self, symbol: str) -> str:
        """
        심볼 정규화
        
        Args:
            symbol: 심볼 코드 (예: "BTC/KRW", "KRW-BTC", "BTCKRW")
            
        Returns:
            str: 정규화된 심볼 (예: "btc")
        """
        # 대문자 제거, 슬래시 제거, KRW- 접두사 제거
        symbol = symbol.lower()
        
        # "krw-" 접두사 제거
        if symbol.startswith("krw-"):
            symbol = symbol[4:]
            
        # "/krw" 접미사 제거
        if symbol.endswith("/krw"):
            symbol = symbol[:-4]
            
        # "krw" 접미사 제거
        if symbol.endswith("krw"):
            symbol = symbol[:-3]
            
        return symbol
    
    def get_orderbook(self, symbol: str) -> Optional[Dict]:
        """
        현재 오더북 상태 반환
        
        Args:
            symbol: 심볼 코드
            
        Returns:
            Optional[Dict]: 오더북 상태 또는 None (없는 경우)
        """
        normalized_symbol = self.normalize_symbol(symbol)
        return self.orderbooks.get(normalized_symbol) 