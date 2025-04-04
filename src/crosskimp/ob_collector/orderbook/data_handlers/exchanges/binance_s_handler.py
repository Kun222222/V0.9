"""
바이낸스 현물 데이터 핸들러 모듈

바이낸스 현물 거래소의 데이터 처리를 담당합니다.
오더북 스냅샷 조회, 메시지 처리 등을 제공합니다.
"""

import json
import time
import aiohttp
import asyncio
from typing import Dict, List, Any, Optional, Union, Tuple
import logging
import datetime

from crosskimp.common.config.app_config import get_config
from crosskimp.common.logger.logger import get_unified_logger, create_raw_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR
from crosskimp.ob_collector.orderbook.data_handlers.validators import BaseOrderBookValidator
from crosskimp.ob_collector.orderbook.data_handlers.ob_data_manager import get_orderbook_data_manager

class BinanceSpotDataHandler:
    """
    바이낸스 현물 데이터 핸들러 클래스
    
    바이낸스 현물 거래소의 데이터 처리를 담당합니다.
    """
    
    # 상수 정의
    REST_API_ENDPOINT = "https://api.binance.com"
    ORDERBOOK_SNAPSHOT_ENDPOINT = "/api/v3/depth"
    DEFAULT_LIMIT = 50  # 오더북 스냅샷 요청 시 기본 깊이 (100에서 50으로 변경)
    
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        self.exchange_code = Exchange.BINANCE_SPOT.value
        self.exchange_name_kr = EXCHANGE_NAMES_KR[self.exchange_code]
        # 데이터 관리자 가져오기
        self.data_manager = get_orderbook_data_manager()
        self.data_manager.register_exchange(self.exchange_code)
        
        # 기존 raw_logger는 유지하되 사용은 최소화
        self.raw_logger = create_raw_logger(self.exchange_code)
        
        self.config = get_config()
        self.symbol_infos = {}  # 심볼별 정보 캐시
        self.last_snapshot_time = {}  # 마지막 스냅샷 요청 시간 (심볼별)
        
        # 오더북 데이터 저장소
        self.orderbooks = {}  # symbol -> orderbook_data
        self.last_update_ids = {}  # symbol -> lastUpdateId
        
        # 버퍼된 메시지 저장
        self.buffered_messages = {}  # symbol -> [messages]
        
        # 검증기 초기화
        self.validator = BaseOrderBookValidator(self.exchange_code)
        
        # API 요청 제한 관리
        self.last_api_call_time = 0  # 마지막 API 호출 시간 (전체)
        self.snapshot_request_count = 0  # 누적 스냅샷 요청 횟수
        self.snapshot_request_time = time.time()  # 스냅샷 요청 시간 측정 시작
        self.max_requests_per_minute = 1000  # 분당 최대 요청 횟수 (Binance 제한은 1200이지만 안전하게 설정)
        
        # 시퀀스 갭 관련 설정
        self.gap_threshold = 50000  # 시퀀스 갭 임계값
        self.snapshot_cooldown = {}  # 심볼별 스냅샷 재요청 쿨다운 시간
        self.min_snapshot_interval = 10.0  # 각 심볼별 스냅샷 요청 최소 간격 (초)
        
        # self.logger.info("바이낸스 현물 데이터 핸들러 초기화")
        
    async def get_orderbook_snapshot(self, symbol: str, limit: int = None) -> Dict[str, Any]:
        """
        오더북 스냅샷 가져오기 (REST API)
        
        Args:
            symbol: 심볼 (예: "BTC")
            limit: 오더북 깊이 (최대 5000)
            
        Returns:
            Dict[str, Any]: 오더북 스냅샷 데이터
        """
        if limit is None:
            limit = self.DEFAULT_LIMIT
            
        # 심볼 정규화
        normalized_symbol = self.normalize_symbol(symbol)
            
        # 심볼 형식 변환 (REST API용)
        symbol_fmt = symbol.upper() + "USDT" if not symbol.upper().endswith("USDT") else symbol.upper()
        
        # 속도 제한 관리
        now = time.time()
        
        # 1. 글로벌 API 요청 제한 관리
        elapsed_since_last_call = now - self.last_api_call_time
        if elapsed_since_last_call < 0.1:  # 전체 요청에 대해 최소 100ms 간격 유지
            await asyncio.sleep(0.1 - elapsed_since_last_call)
        
        # 2. 분당 요청 횟수 제한 관리
        self.snapshot_request_count += 1
        elapsed_minute = now - self.snapshot_request_time
        if elapsed_minute >= 60:
            # 1분이 지나면 카운터 초기화
            self.snapshot_request_count = 1
            self.snapshot_request_time = now
        elif self.snapshot_request_count > self.max_requests_per_minute:
            # 분당 최대 요청 횟수 초과 시 대기
            self.logger.warning(f"분당 최대 요청 횟수({self.max_requests_per_minute})를 초과했습니다. 다음 분까지 대기합니다.")
            await asyncio.sleep(60 - elapsed_minute + 1)  # 다음 분까지 대기 + 1초 여유
            self.snapshot_request_count = 1
            self.snapshot_request_time = time.time()
        
        # 3. 심볼별 요청 간격 관리
        if normalized_symbol in self.last_snapshot_time:
            elapsed = now - self.last_snapshot_time[normalized_symbol]
            if elapsed < 1.0:  # 각 심볼별 최소 1초 간격
                await asyncio.sleep(1.0 - elapsed)
                
        # 현재 시간 업데이트
        self.last_snapshot_time[normalized_symbol] = time.time()
        self.last_api_call_time = time.time()
        
        # REST API URL 생성
        url = f"{self.REST_API_ENDPOINT}{self.ORDERBOOK_SNAPSHOT_ENDPOINT}"
        params = {
            "symbol": symbol_fmt,
            "limit": limit
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # 로우 데이터 로깅을 중앙 관리자를 통해 처리
                        self.data_manager.log_raw_message(self.exchange_code, f"depth raw: {json.dumps(data)}")
                        
                        # 응답 처리
                        result = {
                            "exchange": self.exchange_code,
                            "symbol": normalized_symbol,
                            "timestamp": int(time.time() * 1000),
                            "last_update_id": data.get("lastUpdateId", 0),
                            "bids": data.get("bids", []),
                            "asks": data.get("asks", [])
                        }
                        
                        # 스냅샷 데이터 처리
                        await self.process_snapshot(normalized_symbol, result)
                        
                        self.logger.debug(f"{self.exchange_name_kr} 오더북 스냅샷 수신: {normalized_symbol} ({len(result['bids'])} bids, {len(result['asks'])} asks)")
                        return result
                    else:
                        error_data = await response.text()
                        self.logger.error(f"{self.exchange_name_kr} 오더북 스냅샷 요청 실패: {response.status} - {error_data}")
                        return {
                            "exchange": self.exchange_code,
                            "symbol": normalized_symbol,
                            "error": True,
                            "status_code": response.status,
                            "message": error_data
                        }
                        
        except asyncio.TimeoutError:
            self.logger.error(f"{self.exchange_name_kr} 오더북 스냅샷 요청 타임아웃: {normalized_symbol}")
            return {
                "exchange": self.exchange_code,
                "symbol": normalized_symbol,
                "error": True,
                "message": "Request timeout"
            }
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 오더북 스냅샷 요청 중 오류: {str(e)}")
            return {
                "exchange": self.exchange_code,
                "symbol": normalized_symbol,
                "error": True,
                "message": str(e)
            }
            
    async def process_snapshot(self, symbol: str, data: Dict) -> None:
        """
        스냅샷 데이터 처리
        
        Args:
            symbol: 심볼 이름 (소문자)
            data: 스냅샷 데이터
        """
        try:
            # 타임스탬프 및 시퀀스 추출
            timestamp = data.get("timestamp", int(time.time() * 1000))
            last_update_id = data.get("last_update_id", 0)
            
            if not last_update_id:
                self.logger.error(f"{self.exchange_name_kr} {symbol} 스냅샷 데이터에 시퀀스 번호 없음")
                return
                
            # 매수/매도 호가 추출
            bids_data = data.get("bids", [])
            asks_data = data.get("asks", [])
            
            # 검증기를 통한 스냅샷 초기화
            validation_result = await self.validator.initialize_orderbook(
                symbol=symbol,
                data={
                    "bids": [[float(bid[0]), float(bid[1])] for bid in bids_data],
                    "asks": [[float(ask[0]), float(ask[1])] for ask in asks_data],
                    "timestamp": timestamp,
                    "sequence": last_update_id
                }
            )
            
            if not validation_result.is_valid:
                # self.logger.warning(f"{self.exchange_name_kr} {symbol} 스냅샷 검증 실패: {validation_result.errors}")
                pass
            
            # 마지막 업데이트 ID 저장
            self.last_update_ids[symbol] = last_update_id
            
            # 오더북 내용 로깅
            orderbook = self.validator.get_orderbook(symbol)
            if orderbook:
                bids_count = len(orderbook.get("bids", []))
                asks_count = len(orderbook.get("asks", []))
                # self.logger.info(f"바이낸스 현물 {symbol} 스냅샷 초기화 완료 | lastUpdateId: {last_update_id}, 매수: {bids_count}건, 매도: {asks_count}건")
                
                # 최상위 호가 정보 로깅 (디버그용)
                if bids_count > 0 and asks_count > 0:
                    best_bid = orderbook["bids"][0]
                    best_ask = orderbook["asks"][0]
                    # self.logger.debug(f"바이낸스 현물 {symbol} 최상위 호가 - 매수: {best_bid[0]} / 매도: {best_ask[0]} (스프레드: {best_ask[0] - best_bid[0]})")
                    
                    # 전체 오더북 구조 출력 (디버그 레벨로)
                    # self.logger.debug(f"바이낸스 현물 {symbol} 전체 오더북: 매수 {bids_count}건, 매도 {asks_count}건")
            
            # 버퍼된 메시지 처리
            await self._process_buffered_messages(symbol)
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} {symbol} 스냅샷 처리 중 오류: {str(e)}")
            
    async def _process_buffered_messages(self, symbol: str) -> None:
        """
        버퍼된 메시지 처리
        
        Args:
            symbol: 심볼 이름 (소문자)
        """
        if symbol not in self.buffered_messages or not self.buffered_messages[symbol]:
            return
            
        buffered = self.buffered_messages[symbol]
        # self.logger.info(f"바이낸스 현물 {symbol} 버퍼된 메시지 처리 시작: {len(buffered)}개")
        
        # 버퍼 클리어
        self.buffered_messages[symbol] = []
        
        # 시퀀스로 정렬
        sorted_messages = sorted(buffered, key=lambda x: x.get("final_update_id", 0))
        
        # 버퍼된 메시지 순차 처리
        for message in sorted_messages:
            await self.process_delta_update(symbol, message)
            
        # self.logger.info(f"바이낸스 현물 {symbol} 버퍼된 메시지 처리 완료")
        
        # 업데이트 후 오더북 상태 로깅
        orderbook = self.validator.get_orderbook(symbol)
        if orderbook:
            bids_count = len(orderbook.get("bids", []))
            asks_count = len(orderbook.get("asks", []))
            # self.logger.debug(f"바이낸스 현물 {symbol} 버퍼 처리 후 오더북 상태 - 매수: {bids_count}건, 매도: {asks_count}건")
            
    async def get_exchange_info(self) -> Dict[str, Any]:
        """
        거래소 정보 가져오기 (REST API)
        
        Returns:
            Dict[str, Any]: 거래소 정보
        """
        url = f"{self.REST_API_ENDPOINT}/api/v3/exchangeInfo"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # 심볼 정보 추출 및 캐싱
                        for symbol_info in data.get("symbols", []):
                            symbol = symbol_info.get("symbol", "").lower()
                            self.symbol_infos[symbol] = symbol_info
                            
                        # self.logger.info(f"{self.exchange_name_kr} 거래소 정보 수신: {len(self.symbol_infos)}개 심볼")
                        return data
                    else:
                        error_data = await response.text()
                        self.logger.error(f"{self.exchange_name_kr} 거래소 정보 요청 실패: {response.status} - {error_data}")
                        return {
                            "error": True,
                            "status_code": response.status,
                            "message": error_data
                        }
                        
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 거래소 정보 요청 중 오류: {str(e)}")
            return {
                "error": True,
                "message": str(e)
            }
            
    def process_message(self, message: Union[str, bytes]) -> Dict[str, Any]:
        """
        수신된 웹소켓 메시지 처리 (통합 처리 방식)
        
        Args:
            message: 웹소켓으로부터 수신된 원본 메시지 (문자열 또는 바이트)
            
        Returns:
            Dict[str, Any]: 처리된 메시지
        """
        try:
            # 바이트인 경우 문자열로 디코딩
            if isinstance(message, bytes):
                message = message.decode('utf-8')
                
            # JSON 문자열을 딕셔너리로 변환
            data = json.loads(message)
            
            # 로우 데이터 로깅을 중앙 관리자를 통해 처리
            # 메시지 타입에 따라 로깅 형식 조정
            if isinstance(data, dict) and "e" in data and data["e"] == "depthUpdate":
                self.data_manager.log_raw_message(self.exchange_code, f"depthUpdate raw: {message}")
            
            # PONG 응답 처리
            if isinstance(data, dict) and "id" in data and "result" in data and data["result"] is None:
                return {
                    "type": "pong",
                    "data": data
                }
            
            # 구독 응답 메시지인 경우
            if isinstance(data, dict) and "result" in data:
                return {
                    "type": "subscription_response",
                    "data": data
                }
                
            # 일반 이벤트 메시지인 경우 (depth 업데이트)
            if isinstance(data, dict) and "e" in data and data["e"] == "depthUpdate":
                # 심볼 추출 및 정규화
                original_symbol = data.get("s", "").lower()
                symbol = self.normalize_symbol(original_symbol)
                
                # 데이터 형식 변환
                result = {
                    "type": "depth_update",
                    "exchange": self.exchange_code,
                    "symbol": symbol,
                    "event_time": data.get("E", 0),
                    "transaction_time": data.get("T", 0) if "T" in data else 0,
                    "first_update_id": data.get("U", 0),
                    "final_update_id": data.get("u", 0),
                    "bids": [[float(price), float(amount)] for price, amount in data.get("b", [])],
                    "asks": [[float(price), float(amount)] for price, amount in data.get("a", [])]
                }
                
                # 현재 알려진 마지막 업데이트 ID를 시퀀스로 포함
                if symbol in self.last_update_ids:
                    result["sequence"] = self.last_update_ids[symbol]
                else:
                    result["sequence"] = result.get("final_update_id", 0)
                
                # 현재 오더북 상태를 조회하여 로깅
                current_orderbook = self.get_orderbook(symbol)
                if current_orderbook:
                    # 중앙 관리자를 통한 오더북 데이터 로깅
                    self.data_manager.log_orderbook_data(
                        self.exchange_code,
                        symbol,
                        current_orderbook
                    )
                
                # 델타 업데이트 처리를 비동기 태스크로 생성
                loop = asyncio.get_event_loop()
                loop.create_task(self.process_delta_update(symbol, result))
                
                # 현재 오더북 상태를 조회하여 결과에 추가
                orderbook = self.get_orderbook(symbol)
                if orderbook:
                    # 처리된 오더북 상태를 결과에 포함
                    result["current_orderbook"] = orderbook
                
                return result
            
            # 그 외 응답은 원본 반환
            return {
                "type": "unknown",
                "data": data
            }
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 처리 중 오류: {str(e)}")
            return {
                "type": "error",
                "error": str(e),
                "message": message if isinstance(message, str) else str(message)
            }
        
    async def process_delta_update(self, symbol: str, data: Dict) -> None:
        """
        오더북 델타 업데이트 처리
        
        Args:
            symbol: 심볼 이름 (정규화된 심볼)
            data: 델타 업데이트 데이터
        """
        try:
            # 타임스탬프 및 시퀀스 추출
            timestamp = data.get("event_time", int(time.time() * 1000))
            first_update_id = data.get("first_update_id", 0)
            final_update_id = data.get("final_update_id", 0)
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            
            # 검증기에 오더북이 없거나 last_update_ids에 심볼이 없으면 스냅샷 필요
            if symbol not in self.last_update_ids:
                # self.logger.debug(f"{self.exchange_name_kr} {symbol} 오더북 초기화 필요, 버퍼에 메시지 저장")
                if symbol not in self.buffered_messages:
                    self.buffered_messages[symbol] = []
                self.buffered_messages[symbol].append(data)
                
                # 스냅샷 요청 쿨다운 확인
                now = time.time()
                if symbol not in self.snapshot_cooldown or (now - self.snapshot_cooldown.get(symbol, 0)) >= self.min_snapshot_interval:
                    # 스냅샷이 존재하지 않으면 스냅샷 요청
                    self.snapshot_cooldown[symbol] = now
                    await self.get_orderbook_snapshot(symbol)
                else:
                    # self.logger.debug(f"{self.exchange_name_kr} {symbol} 스냅샷 요청 쿨다운 중, 요청 보류")
                    pass
                return
            
            # 바이낸스 현물 데이터 검증 로직 적용 (현물 정합성 규칙)
            last_update_id = self.last_update_ids.get(symbol, 0)
            
            # 시퀀스 역전 확인(final_update_id가 last_update_id보다 작은 경우)만 검증
            if final_update_id <= last_update_id:
                # 역전된 경우 로깅하고 무시
                # self.logger.debug(f"{self.exchange_name_kr} {symbol} 시퀀스 역전 감지 (무시됨): final_id={final_update_id}, last_id={last_update_id}")
                return
                
            # 검증기를 통한 델타 업데이트
            validation_result = await self.validator.update(
                symbol=symbol,
                data={
                    "bids": [[float(bid[0]), float(bid[1])] for bid in bids],
                    "asks": [[float(ask[0]), float(ask[1])] for ask in asks],
                    "timestamp": timestamp,
                    "sequence": final_update_id
                }
            )
            
            if not validation_result.is_valid:
                # self.logger.warning(f"{self.exchange_name_kr} {symbol} 델타 검증 실패: {validation_result.errors}")
                return
            
            # 마지막 업데이트 ID 업데이트
            self.last_update_ids[symbol] = final_update_id
            
            # 업데이트 성공 시 주기적으로 오더북 상태 로깅 (100번에 1번 정도)
            if final_update_id % 100 == 0:
                orderbook = self.validator.get_orderbook(symbol)
                if orderbook:
                    bids_count = len(orderbook.get("bids", []))
                    asks_count = len(orderbook.get("asks", []))
                    
                    # 최상위 호가 정보 로깅
                    if bids_count > 0 and asks_count > 0:
                        best_bid = orderbook["bids"][0]
                        best_ask = orderbook["asks"][0]
                        # self.logger.debug(f"{self.exchange_name_kr} {symbol} 델타 업데이트 후 오더북 상태 - 매수: {bids_count}건, 매도: {asks_count}건, 최상위 매수: {best_bid[0]}, 최상위 매도: {best_ask[0]}")
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} {symbol} 델타 처리 중 오류: {str(e)}")
        
    def create_subscription_message(self, symbols: List[str]) -> Dict[str, Any]:
        """
        웹소켓 구독 메시지 생성
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            Dict[str, Any]: 구독 메시지
        """
        # 심볼 형식 변환 (모두 소문자로 + usdt 추가)
        formatted_symbols = [s.lower() + 'usdt' if not s.lower().endswith('usdt') else s.lower() for s in symbols]
        
        # 바이낸스 현물 구독 메시지 형식
        params = [f"{symbol}@depth@100ms" for symbol in formatted_symbols]
        
        return {
            "method": "SUBSCRIBE",
            "params": params,
            "id": int(time.time() * 1000)  # 고유 ID 생성
        }
        
    def normalize_symbol(self, symbol: str) -> str:
        """
        심볼 형식 정규화
        
        Args:
            symbol: 원본 심볼
            
        Returns:
            str: 정규화된 심볼
        """
        # 모두 소문자로 변환
        symbol = symbol.lower()
        
        # USDT 접미사 제거
        if symbol.endswith('usdt'):
            symbol = symbol[:-4]
            
        return symbol
        
    def get_orderbook(self, symbol: str) -> Optional[Dict]:
        """
        심볼의 오더북 데이터 반환
        
        Args:
            symbol: 심볼 이름 (소문자)
            
        Returns:
            Optional[Dict]: 오더북 데이터 (없으면 None)
        """
        if not self.validator:
            return None
            
        orderbook = self.validator.get_orderbook(symbol)
        
        # 오더북이 존재하는 경우, last_update_ids 정보 추가
        if orderbook and symbol in self.last_update_ids:
            # 오더북에 시퀀스 번호가 없거나 0인 경우, last_update_ids 값 사용
            if not orderbook.get("sequence"):
                orderbook["sequence"] = self.last_update_ids[symbol]
                
        return orderbook