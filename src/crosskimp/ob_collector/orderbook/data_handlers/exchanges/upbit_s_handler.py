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
        self.sequence_numbers = {}  # 심볼별 시퀀스 번호
        self.last_update_time = {}  # 심볼별 마지막 업데이트 시간
        
        # 데이터 관리자 가져오기
        self.data_manager = get_orderbook_data_manager()
        
        # self.logger.info("업비트 현물 데이터 핸들러 초기화 완료")
    
    def process_message(self, message: Union[str, bytes]) -> Optional[Dict[str, Any]]:
        """
        원시 메시지를 직접 처리하는 통합 메서드
        
        Args:
            message: 웹소켓에서 수신한 원시 메시지 (문자열 또는 바이트)
            
        Returns:
            Optional[Dict[str, Any]]: 처리된 오더북 데이터 또는 None
        """
        try:
            # 메시지가 bytes인 경우 문자열로 변환
            if isinstance(message, bytes):
                message = message.decode('utf-8')
                
            # 한 번만 파싱 수행
            data = json.loads(message)
            
            # 타입 체크 (orderbook 타입만 처리)
            if data.get("type") != "orderbook":
                return None
                
            # 심볼 추출 (KRW-BTC -> BTC)
            code = data.get("code", "")
            if not code or not code.startswith("KRW-"):
                return None
                    
            symbol = code.split("-")[1].lower()
            
            # 타임스탬프 추출
            timestamp = data.get("timestamp", int(time.time() * 1000))
            
            # 오더북 데이터 추출
            orderbook_units = data.get("orderbook_units", [])
            
            # 매수/매도 호가 배열 생성
            bids = [[float(unit.get("bid_price", 0)), float(unit.get("bid_size", 0))] for unit in orderbook_units]
            asks = [[float(unit.get("ask_price", 0)), float(unit.get("ask_size", 0))] for unit in orderbook_units]
            
            # 결과 오더북 구성
            orderbook = {
                "exchange": self.exchange_code,
                "symbol": symbol,
                "timestamp": timestamp,
                "bids": sorted(bids, key=lambda x: float(x[0]), reverse=True),  # 매수 호가는 내림차순
                "asks": sorted(asks, key=lambda x: float(x[0]))  # 매도 호가는 오름차순
            }
            
            # 오더북 저장
            self.orderbooks[symbol] = orderbook
            self.last_update_time[symbol] = timestamp
            
            # 데이터 관리자에 오더북 전달 - 로깅은 관리자가 담당
            self.data_manager.process_orderbook_data(self.exchange_code, symbol, orderbook)
            
            # 처리된 오더북 반환
            return orderbook
                
        except json.JSONDecodeError as e:
            self.logger.error(f"업비트 현물 메시지 JSON 파싱 오류: {str(e)}")
            return None
            
        except Exception as e:
            self.logger.error(f"업비트 현물 메시지 처리 중 오류: {str(e)}")
            return None
    
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