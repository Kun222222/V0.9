"""
빗썸 현물 데이터 핸들러 모듈

빗썸 현물 거래소의 오더북 데이터 처리를 담당하는 클래스를 제공합니다.
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

class BithumbSpotDataHandler:
    """
    빗썸 현물 데이터 핸들러 클래스
    
    빗썸 현물 거래소의 데이터 처리를 담당합니다.
    """
    
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        self.exchange_code = Exchange.BITHUMB.value
        self.exchange_name_kr = EXCHANGE_NAMES_KR[self.exchange_code]
        self.orderbooks = {}  # 심볼별 오더북 저장
        self.sequence_numbers = {}  # 심볼별 시퀀스 번호
        self.last_update_time = {}  # 심볼별 마지막 업데이트 시간
        
        # 데이터 관리자 가져오기
        self.data_manager = get_orderbook_data_manager()
        
        # self.logger.info(f"{self.exchange_name_kr} 데이터 핸들러 초기화 완료")
        
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
        
    async def get_orderbook_snapshot(self, symbol: str) -> Dict[str, Any]:
        """
        메모리에서 현재 오더북 데이터 반환
        
        Args:
            symbol: 심볼 코드 (예: "btc")
            
        Returns:
            Dict[str, Any]: 오더북 데이터
        """
        if symbol in self.orderbooks:
            return self.orderbooks[symbol]
        else:
            # 아직 데이터가 없는 경우 빈 오더북 반환
            empty_orderbook = {
                "exchange": self.exchange_code,
                "symbol": symbol,
                "timestamp": int(time.time() * 1000),
                "bids": [],
                "asks": []
            }
            return empty_orderbook
    
    async def process_snapshot(self, symbol: str, data: Dict) -> None:
        """
        스냅샷 처리
        
        Args:
            symbol: 심볼 코드 (예: "btc")
            data: 오더북 스냅샷 데이터
        """
        # self.logger.debug(f"{self.exchange_name_kr} {symbol} 스냅샷 처리 시작")
        
        try:
            # 타임스탬프 가져오기
            timestamp = data.get("timestamp", int(time.time() * 1000))
            
            # 이전 스냅샷이 있고, 새 스냅샷이 더 오래된 경우 무시
            if symbol in self.last_update_time and timestamp < self.last_update_time.get(symbol, 0):
                self.logger.warning(f"{self.exchange_name_kr} {symbol} 오래된 스냅샷 무시 (현재: {self.last_update_time[symbol]}, 수신: {timestamp})")
                return
                
            # 데이터 추출
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            
            # 오더북 구성
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
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} {symbol} 스냅샷 처리 중 오류: {str(e)}")
    
    def process_message(self, raw_message: Union[str, bytes]) -> Dict[str, Any]:
        """
        원시 웹소켓 메시지를 직접 처리하는 통합 메서드
        
        Args:
            raw_message: 수신된 원시 웹소켓 메시지 (문자열 또는 바이트)
            
        Returns:
            Dict[str, Any]: 처리된 오더북 데이터 또는 빈 딕셔너리(처리할 수 없는 메시지)
        """
        try:
            # 바이트 타입이면 UTF-8로 디코딩
            if isinstance(raw_message, bytes):
                raw_message = raw_message.decode('utf-8')
            
            # JSON 문자열을 딕셔너리로 변환 (한 번만)
            data = json.loads(raw_message)
            
            # 메시지 타입 확인
            message_type = data.get("type")
            
            # 오더북 메시지가 아닌 경우 빈 딕셔너리 반환
            if message_type != "orderbook":
                return {}
                
            # 심볼 추출
            symbol = data.get("code", "").replace("KRW-", "").lower()
            stream_type = data.get("stream_type", "REALTIME")  # 스트림 타입 (SNAPSHOT 또는 REALTIME)
            timestamp = data.get("timestamp", int(time.time() * 1000))
            
            # 오더북 데이터 변환 (price, size 형식으로)
            bids = []
            asks = []
            orderbook_units = data.get("orderbook_units", [])
            
            # 매수/매도 호가 처리
            for unit in orderbook_units:
                bid_price = unit.get("bid_price")
                bid_size = unit.get("bid_size")
                ask_price = unit.get("ask_price")
                ask_size = unit.get("ask_size")
                
                if bid_price is not None and bid_size is not None:
                    bids.append([float(bid_price), float(bid_size)])
                
                if ask_price is not None and ask_size is not None:
                    asks.append([float(ask_price), float(ask_size)])
            
            # 결과 구성
            result = {
                "type": "orderbook",
                "subtype": stream_type.lower(),  # 스트림 타입 소문자로 변환
                "exchange": self.exchange_code,
                "symbol": symbol,
                "timestamp": timestamp,
                "bids": bids,
                "asks": asks,
                "total_bid_size": data.get("total_bid_size", 0),
                "total_ask_size": data.get("total_ask_size", 0)
            }
            
            # SNAPSHOT 타입이면 스냅샷 처리, REALTIME 타입이면 업데이트 처리
            if stream_type.lower() == "snapshot":
                asyncio.create_task(self.process_snapshot(symbol, result))
            elif stream_type.lower() == "realtime":
                asyncio.create_task(self.process_delta_update(symbol, result))
            
            return {
                "exchange": self.exchange_code,
                "symbol": symbol,
                "timestamp": timestamp,
                "bids": bids,
                "asks": asks
            }
            
        except json.JSONDecodeError as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 JSON 파싱 오류: {str(e)}")
            return {}
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 처리 중 오류: {str(e)}")
            return {}
    
    async def process_delta_update(self, symbol: str, data: Dict) -> None:
        """
        델타 업데이트 처리 - 단순화된 접근법 적용
        
        Args:
            symbol: 심볼 코드 (예: "btc")
            data: 오더북 업데이트 데이터
        """
        try:
            # 타임스탬프 가져오기
            timestamp = data.get("timestamp", int(time.time() * 1000))
            
            # 이전 업데이트가 있고, 새 업데이트가 더 오래된 경우 무시
            if symbol in self.last_update_time and timestamp < self.last_update_time.get(symbol, 0):
                self.logger.warning(f"{self.exchange_name_kr} {symbol} 오래된 업데이트 무시 (현재: {self.last_update_time[symbol]}, 수신: {timestamp})")
                return
                
            # 아직 스냅샷이 없는 경우 처리 중단
            if symbol not in self.orderbooks:
                self.logger.warning(f"{self.exchange_name_kr} {symbol} 스냅샷 없이 업데이트 수신, 무시함")
                return
                
            # 기존 오더북 가져오기
            orderbook = self.orderbooks.get(symbol, {"bids": [], "asks": []})
            
            # 현재 매수/매도 호가를 딕셔너리로 변환
            current_bids = {float(price): float(amount) for price, amount in orderbook.get("bids", [])}
            current_asks = {float(price): float(amount) for price, amount in orderbook.get("asks", [])}
            
            # 새 업데이트 데이터
            new_bids = data.get("bids", [])
            new_asks = data.get("asks", [])
            
            # 업데이트 적용 - 단순화된 접근법
            for price, amount in new_bids:
                price, amount = float(price), float(amount)
                if amount == 0:
                    # 수량이 0이면 삭제
                    current_bids.pop(price, None)
                else:
                    # 그 외에는 업데이트
                    current_bids[price] = amount
                    
            for price, amount in new_asks:
                price, amount = float(price), float(amount)
                if amount == 0:
                    # 수량이 0이면 삭제
                    current_asks.pop(price, None)
                else:
                    # 그 외에는 업데이트
                    current_asks[price] = amount
            
            # 가격 일관성 검증 및 조정
            if current_bids and current_asks:
                # 최고 매수가와 최저 매도가 계산
                highest_bid = max(current_bids.keys())
                lowest_ask = min(current_asks.keys())
                
                # 가격 역전 현상 확인 (최고 매수가 >= 최저 매도가)
                if highest_bid >= lowest_ask:
                    # self.logger.warning(f"빗썸 현물 {symbol} 가격 역전 현상 감지 및 수정 중: 최고 매수가({highest_bid}) >= 최저 매도가({lowest_ask})")
                    
                    # 방법 1: 역전된 매수가 제거
                    invalid_bids = [price for price in current_bids.keys() if price >= lowest_ask]
                    for price in invalid_bids:
                        # self.logger.debug(f"빗썸 현물 {symbol} 역전된 매수가 제거: {price}")
                        current_bids.pop(price, None)
                    
                    # 방법 2: 역전된 매도가 제거
                    if current_bids:  # 매수가 전부 제거된 경우를 대비
                        highest_bid = max(current_bids.keys())
                        invalid_asks = [price for price in current_asks.keys() if price <= highest_bid]
                        for price in invalid_asks:
                            # self.logger.debug(f"빗썸 현물 {symbol} 역전된 매도가 제거: {price}")
                            current_asks.pop(price, None)
            
            # 업데이트된 오더북 구성
            updated_orderbook = {
                "exchange": self.exchange_code,
                "symbol": symbol,
                "timestamp": timestamp,
                "bids": sorted([(price, amount) for price, amount in current_bids.items()], key=lambda x: float(x[0]), reverse=True),
                "asks": sorted([(price, amount) for price, amount in current_asks.items()], key=lambda x: float(x[0]))
            }
            
            # 오더북 저장
            self.orderbooks[symbol] = updated_orderbook
            self.last_update_time[symbol] = timestamp
            
            # 데이터 관리자에 오더북 전달 - 로깅은 관리자가 담당
            self.data_manager.process_orderbook_data(self.exchange_code, symbol, updated_orderbook)
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} {symbol} 델타 업데이트 처리 중 오류: {str(e)}")
    
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