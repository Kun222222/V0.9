"""
바이빗 현물 데이터 핸들러 모듈

바이빗 현물 거래소의 오더북 데이터 처리 로직을 제공합니다.
"""

import json
import time
import asyncio
import aiohttp
from typing import Dict, List, Set, Any, Optional, Union
import logging
from collections import defaultdict
import copy

from crosskimp.common.config.app_config import get_config
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR
from crosskimp.ob_collector.orderbook.data_handlers.ob_data_manager import get_orderbook_data_manager

class BybitSpotDataHandler:
    """
    바이빗 현물 데이터 핸들러 클래스
    
    바이빗 현물 거래소의 데이터 처리를 담당합니다.
    """
    
    # REST API 상수 (거래소 정보 조회용으로 유지)
    REST_API_ENDPOINT = "https://api.bybit.com/v5"
    
    def __init__(self):
        """초기화"""
        # 로깅 설정
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        
        # 설정 로드
        self.config = get_config()
        
        # 거래소 코드 설정
        self.exchange_code = Exchange.BYBIT_SPOT.value
        self.exchange_name_kr = EXCHANGE_NAMES_KR[self.exchange_code]
        
        # 중앙 데이터 관리자 가져오기
        self.data_manager = get_orderbook_data_manager()
        
        # 오더북 상태 저장용 변수
        self.orderbooks = {}  # symbol -> orderbook
        self.snapshot_timestamps = {}  # symbol -> last_snapshot_timestamp
        self.last_update_ids = {}  # symbol -> last_update_id
        
        # HTTP 세션
        self.session = None
        
        # self.logger.info(f"{self.exchange_name_kr} 데이터 핸들러 초기화 완료")
        
    async def process_snapshot(self, symbol: str, data: Dict) -> None:
        """
        오더북 스냅샷 데이터 처리
        
        Args:
            symbol: 심볼
            data: 스냅샷 데이터
        """
        try:
            # 필요한 데이터 추출
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            sequence = data.get("sequence", 0)
            update_id = data.get("update_id", 0)
            timestamp = data.get("timestamp", int(time.time() * 1000))
            
            # 오더북 생성
            orderbook = {
                "bids": {},
                "asks": {},
                "timestamp": timestamp,
                "sequence": sequence,
                "update_id": update_id,
                "symbol": symbol,
                "exchange": self.exchange_code
            }
            
            # 매수 호가 변환 및 저장 (가격 -> 수량)
            for bid in bids:
                price = float(bid[0])
                amount = float(bid[1])
                if amount > 0:
                    orderbook["bids"][price] = amount
                    
            # 매도 호가 변환 및 저장 (가격 -> 수량)
            for ask in asks:
                price = float(ask[0])
                amount = float(ask[1])
                if amount > 0:
                    orderbook["asks"][price] = amount
                    
            # 저장
            self.orderbooks[symbol] = orderbook
            self.last_update_ids[symbol] = update_id
            self.snapshot_timestamps[symbol] = timestamp
            
            # 로깅용 복사본 생성 (최대 10개 항목)
            log_orderbook = {
                "bids": sorted(list(orderbook["bids"].items()), key=lambda x: x[0], reverse=True)[:10],
                "asks": sorted(list(orderbook["asks"].items()), key=lambda x: x[0])[:10],
                "timestamp": timestamp,
                "sequence": sequence,
                "update_id": update_id,
                "symbol": symbol,
                "exchange": self.exchange_code
            }
            
            # 배열 형태로 변환 [[price, amount], ...]
            formatted_bids = [[price, amount] for price, amount in log_orderbook["bids"]]
            formatted_asks = [[price, amount] for price, amount in log_orderbook["asks"]]
            
            # 중앙 데이터 관리자에 로깅
            self.data_manager.log_orderbook_data(
                self.exchange_code,
                symbol,
                {
                    "bids": formatted_bids,
                    "asks": formatted_asks,
                    "timestamp": timestamp,
                    "sequence": sequence
                }
            )
            
            # self.logger.info(f"[{symbol}] {self.exchange_name_kr} 오더북 초기화 완료")
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 스냅샷 처리 중 오류: {str(e)}")
    
    async def get_exchange_info(self) -> Dict[str, Any]:
        """
        거래소 정보 조회
        
        Returns:
            Dict[str, Any]: 거래소 정보
        """
        # 세션이 없으면 생성
        if self.session is None:
            self.session = aiohttp.ClientSession()
            
        url = f"{self.REST_API_ENDPOINT}/market/instruments-info"
        params = {
            "category": "spot"
        }
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status != 200:
                    self.logger.error(f"{self.exchange_name_kr} 거래소 정보 조회 실패: {response.status}")
                    return {}
                    
                data = await response.json()
                
                if data["retCode"] != 0:
                    self.logger.error(f"{self.exchange_name_kr} 거래소 정보 응답 오류: {data}")
                    return {}
                    
                return data["result"]
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 거래소 정보 조회 중 오류: {str(e)}")
            return {}
            
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
                
            # JSON 문자열을 딕셔너리로 변환 (한 번만 파싱)
            data = json.loads(message)
            
            # 핑퐁 응답 메시지인 경우
            if "op" in data and data["op"] == "pong":
                return {
                    "type": "pong",
                    "data": data
                }
                
            # 구독 응답 메시지인 경우
            if "success" in data and "op" in data and data["op"] == "subscribe":
                return {
                    "type": "subscription_response",
                    "data": data
                }

            # 오더북 메시지인 경우
            if "topic" in data and "data" in data and "topic" in data and "orderbook" in data["topic"]:
                # 심볼 추출 (예: orderbook.50.BTCUSDT)
                topic_parts = data["topic"].split(".")
                if len(topic_parts) >= 3:
                    symbol = topic_parts[-1].lower()
                    
                    # 원본 type을 그대로 유지 (snapshot 또는 delta)
                    original_type = data.get("type", "delta")
                    
                    # 데이터 추출
                    orderbook_data = data.get("data", {})
                    
                    # 결과 구성
                    result = {
                        "type": original_type,
                        "exchange": self.exchange_code,
                        "symbol": symbol,
                        "event_time": data.get("ts", 0),
                        "bids": orderbook_data.get("b", []),
                        "asks": orderbook_data.get("a", []),
                        "sequence": orderbook_data.get("seq", 0),
                        "update_id": orderbook_data.get("u", 0),
                        "cross_seq": orderbook_data.get("seq", 0),
                        "timestamp": orderbook_data.get("cts", 0)
                    }
                    
                    # 중앙 데이터 관리자에 원본 메시지 로깅
                    self.data_manager.log_raw_message(self.exchange_code, message)
                    
                    # 메시지 타입에 따라 처리
                    if original_type == "snapshot":
                        # 스냅샷 데이터 변환
                        snapshot = {
                            "exchange": self.exchange_code,
                            "symbol": symbol.replace("usdt", "").lower(),
                            "timestamp": result.get("event_time", 0),
                            "sequence": result.get("sequence", 0),
                            "update_id": result.get("update_id", 0),
                            "bids": result.get("bids", []),
                            "asks": result.get("asks", []),
                            "type": "snapshot"
                        }
                        
                        # 스냅샷 처리 - 비동기 태스크로 처리
                        asyncio.create_task(self.process_snapshot(symbol.replace("usdt", "").lower(), snapshot))
                        return result
                        
                    elif original_type == "delta":
                        # 델타 데이터 변환
                        delta = {
                            "exchange": self.exchange_code,
                            "symbol": symbol.replace("usdt", "").lower(),
                            "timestamp": result.get("event_time", 0),
                            "sequence": result.get("sequence", 0),
                            "update_id": result.get("update_id", 0),
                            "bids": result.get("bids", []),
                            "asks": result.get("asks", []),
                            "type": "delta"
                        }
                        
                        # 오더북이 없으면 처리 불가
                        symbol_clean = symbol.replace("usdt", "").lower()
                        if symbol_clean not in self.orderbooks:
                            # self.logger.warning(f"[{symbol_clean}] 스냅샷 없이 델타 수신, 무시")
                            return result
                        
                        # 델타 업데이트 처리
                        asyncio.create_task(self.process_delta_update(symbol_clean, delta))
                        return result
                    
                    return result
                
            # 기타 메시지는 그대로 반환
            return {
                "type": "unknown",
                "data": data,
                "raw": message
            }
            
        except json.JSONDecodeError as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 JSON 파싱 오류: {str(e)}")
            return {
                "type": "error",
                "error": "json_decode_error",
                "message": str(e),
                "raw": message
            }
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 처리 중 오류: {str(e)}")
            return {
                "type": "error",
                "error": "processing_error",
                "message": str(e),
                "raw": message
            }
            
    async def process_delta_update(self, symbol: str, data: Dict) -> None:
        """
        델타 업데이트 처리
        
        Args:
            symbol: 심볼
            data: 델타 데이터
        """
        try:
            # 필요한 데이터 추출
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            sequence = data.get("sequence", 0)
            update_id = data.get("update_id", 0)
            timestamp = data.get("timestamp", int(time.time() * 1000))
            
            # 오더북이 없으면 처리 불가
            if symbol not in self.orderbooks:
                # self.logger.warning(f"[{symbol}] 스냅샷 없이 델타 수신, 무시")
                return
                
            # 시퀀스 검증
            if "sequence" in data:
                last_sequence = self.orderbooks[symbol].get("sequence", 0)
                if sequence <= last_sequence:
                    self.logger.warning(f"[{symbol}] 이전 시퀀스의 델타 메시지 수신, 무시 ({sequence} <= {last_sequence})")
                    return
            
            # 업데이트 ID 검증
            if "update_id" in data:
                last_update_id = self.orderbooks[symbol].get("update_id", 0)
                if update_id <= last_update_id:
                    self.logger.warning(f"[{symbol}] 이전 업데이트 ID의 델타 메시지 수신, 무시 ({update_id} <= {last_update_id})")
                    return
                
            # 오더북 가져오기
            orderbook = self.orderbooks[symbol]
            
            # 매수 호가 업데이트
            for bid in bids:
                price = float(bid[0])
                amount = float(bid[1])
                
                if amount == 0:
                    # 수량이 0이면 항목 삭제
                    if price in orderbook["bids"]:
                        del orderbook["bids"][price]
                else:
                    # 수량이 있으면 항목 추가/업데이트
                    orderbook["bids"][price] = amount
                    
            # 매도 호가 업데이트
            for ask in asks:
                price = float(ask[0])
                amount = float(ask[1])
                
                if amount == 0:
                    # 수량이 0이면 항목 삭제
                    if price in orderbook["asks"]:
                        del orderbook["asks"][price]
                else:
                    # 수량이 있으면 항목 추가/업데이트
                    orderbook["asks"][price] = amount
                    
            # 가격 일관성 검증 및 조정
            if orderbook["bids"] and orderbook["asks"]:
                # 최고 매수가와 최저 매도가 계산
                highest_bid = max(orderbook["bids"].keys())
                lowest_ask = min(orderbook["asks"].keys())
                
                # 가격 역전 현상 확인 (최고 매수가 >= 최저 매도가)
                if highest_bid >= lowest_ask:
                    # self.logger.warning(f"바이빗 현물 {symbol} 가격 역전 현상 감지 및 수정 중: 최고 매수가({highest_bid}) >= 최저 매도가({lowest_ask})")
                    
                    # 방법 1: 역전된 매수가 제거
                    invalid_bids = [price for price in orderbook["bids"].keys() if price >= lowest_ask]
                    for price in invalid_bids:
                        # self.logger.debug(f"바이빗 현물 {symbol} 역전된 매수가 제거: {price}")
                        del orderbook["bids"][price]
                    
                    # 방법 2: 역전된 매도가 제거 (필요 시)
                    if orderbook["bids"]:  # 매수가 전부 제거된 경우를 대비
                        highest_bid = max(orderbook["bids"].keys())
                        invalid_asks = [price for price in orderbook["asks"].keys() if price <= highest_bid]
                        for price in invalid_asks:
                            # self.logger.debug(f"바이빗 현물 {symbol} 역전된 매도가 제거: {price}")
                            del orderbook["asks"][price]
            
            # 메타데이터 업데이트
            orderbook["timestamp"] = timestamp
            orderbook["sequence"] = sequence
            orderbook["update_id"] = update_id
            
            # 로깅용 복사본 생성 (최대 10개 항목)
            log_bids = sorted(list(orderbook["bids"].items()), key=lambda x: x[0], reverse=True)[:10]
            log_asks = sorted(list(orderbook["asks"].items()), key=lambda x: x[0])[:10]
            
            # 배열 형태로 변환 [[price, amount], ...]
            formatted_bids = [[price, amount] for price, amount in log_bids]
            formatted_asks = [[price, amount] for price, amount in log_asks]
            
            # 중앙 데이터 관리자에 로깅
            self.data_manager.log_orderbook_data(
                self.exchange_code,
                symbol,
                {
                    "bids": formatted_bids,
                    "asks": formatted_asks,
                    "timestamp": timestamp,
                    "sequence": sequence
                }
            )
            
        except Exception as e:
            self.logger.error(f"바이빗 현물 델타 업데이트 처리 중 오류: {str(e)}")
            
    def create_subscription_message(self, symbols: List[str]) -> Dict[str, Any]:
        """
        구독 메시지 생성
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            Dict[str, Any]: 구독 메시지
        """
        # 심볼 형식 변환
        formatted_symbols = [self.normalize_symbol(s) for s in symbols]
        
        # args 배열 구성
        args = [f"orderbook.50.{symbol.upper()}" for symbol in formatted_symbols]
        
        # 구독 메시지 생성
        message = {
            "op": "subscribe",
            "args": args
        }
        
        return message
        
    def normalize_symbol(self, symbol: str) -> str:
        """
        심볼명 정규화 (API 요청용)
        
        Args:
            symbol: 정규화할 심볼
            
        Returns:
            str: 정규화된 심볼
        """
        # 소문자로 변환
        symbol = symbol.lower()
        
        # USDT 추가
        if not symbol.endswith('usdt'):
            symbol = f"{symbol}usdt"
            
        return symbol
        
    def get_orderbook(self, symbol: str) -> Optional[Dict]:
        """
        오더북 데이터 반환
        
        Args:
            symbol: 심볼
            
        Returns:
            Optional[Dict]: 오더북 데이터 또는 None
        """
        # 심볼 정규화
        symbol = symbol.lower().replace("usdt", "")
        
        if symbol not in self.orderbooks:
            return None
            
        orderbook = self.orderbooks[symbol]
        
        # 배열 형태로 변환
        bids = sorted(list(orderbook["bids"].items()), key=lambda x: x[0], reverse=True)
        asks = sorted(list(orderbook["asks"].items()), key=lambda x: x[0])
        
        # 배열 형태로 변환 [[price, amount], ...]
        formatted_bids = [[price, amount] for price, amount in bids]
        formatted_asks = [[price, amount] for price, amount in asks]
        
        # 포맷팅된 오더북 반환
        return {
            "exchange": self.exchange_code,
            "symbol": symbol,
            "timestamp": orderbook.get("timestamp", 0),
            "sequence": orderbook.get("sequence", 0),
            "update_id": orderbook.get("update_id", 0),
            "bids": formatted_bids,
            "asks": formatted_asks
        } 