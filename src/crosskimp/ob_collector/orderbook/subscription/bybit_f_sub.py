"""
바이비트 선물 구독 모듈

바이비트 선물 웹소켓 구독 및 메시지 처리 클래스를 제공합니다.
"""

import asyncio
import json
import time
import datetime
import websockets
from typing import Dict, List, Optional, Union, Any, Set

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import Exchange, EXCHANGE_NAMES_KR

from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector

# 웹소켓 설정
WS_URL = "wss://stream.bybit.com/v5/public/linear"  # 웹소켓 URL 
MAX_SYMBOLS_PER_SUBSCRIPTION = 10  # 구독당 최대 심볼 수
DEFAULT_DEPTH = 50  # 기본 오더북 깊이

# 로깅 설정
ENABLE_RAW_LOGGING = True  # raw 데이터 로깅 활성화 여부
ENABLE_ORDERBOOK_LOGGING = True  # 오더북 데이터 로깅 활성화 여부

# 로거 설정
logger = get_unified_logger()

class BybitFutureSubscription(BaseSubscription):
    """
    바이비트 선물 구독 클래스
    
    바이비트 선물 거래소의 웹소켓 연결을 통해 오더북 데이터를 구독하는 클래스입니다.
    
    특징:
    - 메시지 형식: 스냅샷 및 델타 업데이트 지원
    - 웹소켓을 통한 스냅샷 및 델타 업데이트 수신
    """
    
    # 1. 초기화 단계
    def __init__(self, connection, exchange_code: str = None, on_data_received=None):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
            exchange_code: 거래소 코드 (None이면 connection에서 가져옴)
            on_data_received: 데이터 수신 시 호출될 콜백 함수
        """
        # 부모 클래스 초기화
        super().__init__(connection, exchange_code, on_data_received)
        
        # 구독 설정
        self.depth_level = DEFAULT_DEPTH
        self.max_symbols_per_subscription = MAX_SYMBOLS_PER_SUBSCRIPTION
        
        # 로깅 설정
        self.raw_logging_enabled = ENABLE_RAW_LOGGING
        self.orderbook_logging_enabled = ENABLE_ORDERBOOK_LOGGING
        
        # 각 심볼별 오더북 상태 저장용
        self.orderbooks = {}  # symbol -> {"bids": {...}, "asks": {...}, "timestamp": ..., "sequence": ...}

    async def _ensure_websocket(self) -> bool:
        """
        웹소켓 연결 확보 - 바이비트 선물용 버전
        
        부모 클래스의 _ensure_websocket 메서드를 사용하여 연결을 확보합니다.
        연결 관리는 Connector 클래스에서 전담합니다.
        """
        self.log_info("바이비트 선물 웹소켓 연결 확보 시도")
        
        # 부모 클래스의 _ensure_websocket 메서드 호출
        success = await super()._ensure_websocket()
        
        if success:
            self.log_info("바이비트 선물 웹소켓 연결 확보 완료")
        else:
            self.log_error("바이비트 선물 웹소켓 연결 확보 실패")
                   
        return success

    # 3. 구독 처리 단계
    async def create_subscribe_message(self, symbols: List[str]) -> Dict:
        """
        구독 메시지 생성
        
        Args:
            symbols: 구독할 심볼 또는 심볼 목록
            
        Returns:
            Dict: 구독 메시지
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        
        topics = []
        for symbol in symbols:
            market = f"{symbol}USDT"
            topics.append(f"orderbook.{self.depth_level}.{market}")
        
        return {
            "op": "subscribe",
            "args": topics
        }

    async def _send_subscription_requests(self, symbols: List[str]) -> bool:
        """
        구독 요청 전송
        
        Args:
            symbols: 구독할 심볼 리스트
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            # 구독 메시지 생성
            msg = await self.create_subscribe_message(symbols)
            json_msg = json.dumps(msg)
            
            # 메시지 전송
            success = await self.send_message(json_msg)
            
            if not success:
                self.log_error(f"구독 메시지 전송 실패: {symbols[:3]}...")
                return False
            
            self.log_info(f"구독 메시지 전송 성공: {symbols}")
            return True
        except Exception as e:
            self.log_error(f"구독 메시지 전송 중 오류: {str(e)}")
            return False
    
    async def subscribe(self, symbol):
        """
        심볼 구독
        
        단일 심볼 또는 심볼 리스트를 구독합니다.
        
        Args:
            symbol: 구독할 심볼 또는 심볼 리스트
            
        Returns:
            bool: 구독 성공 여부
        """
        try:
            # 심볼 전처리
            symbols = await self._preprocess_symbols(symbol)
            if not symbols:
                return False
                
            # 구독 상태 업데이트
            for sym in symbols:
                self.subscribed_symbols[sym] = True
            
            # 웹소켓 연결 확보
            if not await self._ensure_websocket():
                self.log_error("웹소켓 연결이 없어 구독 실패")
                return False
            
            # 구독 요청 전송
            if not await self._send_subscription_requests(symbols):
                raise Exception("구독 메시지 전송 실패")
            
            # 메시지 수신 루프 시작
            if not self.message_loop_task or self.message_loop_task.done():
                self.log_info("메시지 수신 루프 시작")
                self.stop_event.clear()
                self.message_loop_task = asyncio.create_task(self.message_loop())
            
            return True
            
        except Exception as e:
            self.log_error(f"구독 중 오류 발생: {str(e)}")
            self._update_metrics("error_count", 1, op="increment")
            return False
    
    # 4. 메시지 수신 및 처리 단계
    def is_snapshot_message(self, message: str) -> bool:
        """
        스냅샷 메시지 여부 확인
        
        Args:
            message: 검사할 메시지
            
        Returns:
            bool: 스냅샷 메시지 여부
        """
        try:
            data = json.loads(message)
            return data.get("type") == "snapshot"
        except:
            return False
    
    def is_delta_message(self, message: str) -> bool:
        """
        메시지가 델타인지 확인
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 델타 메시지인 경우 True
        """
        try:
            data = self._parse_json_message(message)
            if not data:
                return False
            
            # 토픽이 orderbook으로 시작하는지 확인
            if "topic" not in data:
                return False
                
            topic = data.get("topic", "")
            if not topic.startswith("orderbook"):
                return False
                
            # 메시지 타입이 delta인지 확인
            type_field = data.get("type", "").lower()
            if type_field == "delta":
                return True
            
            return False
            
        except Exception as e:
            self.log_error(f"델타 메시지 확인 중 오류: {str(e)}")
            return False

    def _parse_message(self, message: str) -> Optional[Dict]:
        """
        메시지 파싱 (JSON 문자열 -> 파이썬 딕셔너리)
        
        Args:
            message: 수신된 메시지
            
        Returns:
            Optional[Dict]: 파싱된 메시지 데이터 또는 None(파싱 실패시)
        """
        try:
            # JSON 문자열 파싱
            data = json.loads(message)
            result = {}
            
            # 메시지 형식 판별
            if "topic" in data and "data" in data:
                topic = data.get("topic", "")
                if "orderbook" in topic:
                    # 타입 확인
                    msg_type = data.get("type", "delta").lower()  # "snapshot" or "delta"
                    
                    # 심볼 추출 (topic 형식: orderbook.50.{symbol})
                    parts = topic.split(".")
                    if len(parts) >= 3:
                        symbol = parts[-1].replace("USDT", "")
                    else:
                        return None
                    
                    # 데이터 추출
                    inner_data = data.get("data", {})
                    
                    # 매수/매도 데이터
                    bids = inner_data.get("b", [])  # "b" for bids
                    asks = inner_data.get("a", [])  # "a" for asks
                    
                    # 시퀀스 및 타임스탬프
                    timestamp = inner_data.get("ts")  # 타임스탬프 (밀리초 단위)
                    sequence = inner_data.get("seq") or inner_data.get("u")  # 시퀀스 번호
                    
                    # 결과 구성
                    result = {
                        "symbol": symbol,
                        "type": msg_type,
                        "bids": bids,
                        "asks": asks,
                        "timestamp": timestamp,
                        "sequence": sequence
                    }
                    
                    return result
            
            return None
            
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)} | 원본: {message[:100]}...")
            return None

    def log_raw_message(self, message: str) -> None:
        """
        원시 메시지 로깅
        
        Args:
            message: 원시 메시지
        """
        # 부모 클래스의 로깅 메서드 사용
        super().log_raw_message(message)
    
    # 5. 콜백 호출 단계
    # 부모 클래스의 _call_callback 메서드를 사용함

    async def _on_message(self, message: str) -> None:
        """메시지 수신 콜백"""
        try:
            # 메시지 처리 시작 시간 측정
            start_time = time.time()
            
            # 원시 메시지 로깅 및 메시지 카운트 증가 (부모 클래스의 _on_message 사용)
            await super()._on_message(message)
            
            # JSON 파싱 (일부 메시지는 일반 텍스트일 수 있음)
            try:
                parsed_message = json.loads(message)
            except json.JSONDecodeError:
                self.log_error(f"JSON 파싱 실패: {message[:100]}...")
                return
            
            # 구독 응답 처리
            if parsed_message.get("op") == "subscribe":
                return
                
            # 오더북 메시지 처리
            if "topic" in parsed_message and "data" in parsed_message:
                topic = parsed_message.get("topic", "")
                if "orderbook" in topic:
                    try:
                        parts = topic.split(".")
                        if len(parts) >= 3:
                            symbol = parts[-1].replace("USDT", "")
                            msg_type = parsed_message.get("type", "delta").lower()  # "snapshot" or "delta"
                        
                            # 스냅샷/델타 여부에 따라 다르게 처리
                            if msg_type == "snapshot":
                                # 스냅샷 처리
                                orderbook = {}
                                orderbook["bids"] = {}
                                orderbook["asks"] = {}
                                
                                # 데이터 추출
                                data = parsed_message.get("data", {})
                                bids = data.get("b", [])  # 매수 호가
                                asks = data.get("a", [])  # 매도 호가
                                
                                # 타임스탬프와 시퀀스 추출
                                timestamp = data.get("ts")
                                sequence = data.get("seq") or data.get("u")
                                
                                # 오더북 구성
                                for item in bids:
                                    price, size = float(item[0]), float(item[1])
                                    if size > 0:
                                        orderbook["bids"][price] = size
                                
                                for item in asks:
                                    price, size = float(item[0]), float(item[1])
                                    if size > 0:
                                        orderbook["asks"][price] = size
                                
                                # 정렬된 전체 오더북 구성
                                sorted_bids = sorted(orderbook["bids"].items(), key=lambda x: x[0], reverse=True)
                                sorted_asks = sorted(orderbook["asks"].items(), key=lambda x: x[0])
                                
                                # 배열 형태로 변환 [price, size]
                                bids_array = [[price, size] for price, size in sorted_bids]
                                asks_array = [[price, size] for price, size in sorted_asks]
                                
                                # 완전한 오더북 데이터 구성
                                orderbook_data = {
                                    "bids": bids_array,
                                    "asks": asks_array,
                                    "timestamp": timestamp,
                                    "sequence": sequence,
                                    "type": "snapshot"
                                }
                                
                                # 오더북 저장
                                self.orderbooks[symbol] = {
                                    "bids": orderbook["bids"],
                                    "asks": orderbook["asks"],
                                    "timestamp": timestamp,
                                    "sequence": sequence
                                }
                                
                                # 오더북 데이터 로깅 (부모 클래스의 메서드 사용)
                                self.log_orderbook_data(symbol, orderbook_data)
                                
                                # 성능 측정 종료
                                processing_time_ms = (time.time() - start_time) * 1000
                                self.log_debug(f"[{symbol}] 스냅샷 처리 완료 (처리 시간: {processing_time_ms:.2f}ms)")
                            
                            elif msg_type == "delta":
                                # 델타 처리 (기존 오더북 업데이트)
                                if symbol in self.orderbooks:
                                    # 델타 데이터 추출
                                    data = parsed_message.get("data", {})
                                    bids = data.get("b", [])  # 매수 호가
                                    asks = data.get("a", [])  # 매도 호가
                                    
                                    # 타임스탬프와 시퀀스 추출
                                    timestamp = data.get("ts")
                                    sequence = data.get("seq") or data.get("u")
                                    
                                    # 저장된 시퀀스와 비교
                                    stored_sequence = self.orderbooks[symbol].get("sequence")
                                    if sequence <= stored_sequence:
                                        self.log_warning(f"{symbol} 이전 시퀀스의 델타 메시지 수신, 무시 ({sequence} <= {stored_sequence})")
                                        return
                                    
                                    # 기존 오더북 가져오기
                                    orderbook = self.orderbooks[symbol]
                                    
                                    # 매수 호가 업데이트
                                    for bid in bids:
                                        price = float(bid[0])
                                        size = float(bid[1])
                                        if size == 0:
                                            orderbook["bids"].pop(price, None)
                                        else:
                                            orderbook["bids"][price] = size
                                    
                                    # 매도 호가 업데이트
                                    for ask in asks:
                                        price = float(ask[0])
                                        size = float(ask[1])
                                        if size == 0:
                                            orderbook["asks"].pop(price, None)
                                        else:
                                            orderbook["asks"][price] = size
                                    
                                    # 타임스탬프와 시퀀스 업데이트
                                    orderbook["timestamp"] = timestamp
                                    orderbook["sequence"] = sequence
                                    
                                    # 정렬된 전체 오더북 구성
                                    sorted_bids = sorted(orderbook["bids"].items(), key=lambda x: x[0], reverse=True)
                                    sorted_asks = sorted(orderbook["asks"].items(), key=lambda x: x[0])
                                    
                                    # 배열 형태로 변환 [price, size]
                                    bids_array = [[price, size] for price, size in sorted_bids]
                                    asks_array = [[price, size] for price, size in sorted_asks]
                                    
                                    # 완전한 오더북 데이터 구성
                                    orderbook_data = {
                                        "bids": bids_array,
                                        "asks": asks_array,
                                        "timestamp": timestamp,
                                        "sequence": sequence,
                                        "type": "delta"
                                    }
                                    
                                    # 오더북 데이터 로깅 (부모 클래스의 메서드 사용)
                                    self.log_orderbook_data(symbol, orderbook_data)
                                    
                                    # 성능 측정 종료
                                    processing_time_ms = (time.time() - start_time) * 1000
                                    self.log_debug(f"[{symbol}] 델타 처리 완료 (처리 시간: {processing_time_ms:.2f}ms)")
                                else:
                                    # 스냅샷 없이 델타 수신 로그는 중요하므로 유지
                                    self.log_warning(f"{symbol} 스냅샷 없이 델타 수신, 무시")
                    except Exception as e:
                        self.log_error(f"오더북 처리 중 오류: {str(e)}")
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {str(e)}")
    
    async def create_unsubscribe_message(self, symbol: str) -> Dict:
        """
        구독 해제 메시지 생성
        
        Args:
            symbol: 구독 해제할 심볼
            
        Returns:
            Dict: 구독 해제 메시지
        """
        market = f"{symbol}USDT"
        
        # 구독 해제 메시지 생성
        return {
            "op": "unsubscribe",
            "args": [f"orderbook.{self.depth_level}.{market}"]
        }

    async def unsubscribe(self, symbol: Optional[str] = None) -> bool:
        """구독 취소"""
        try:
            return await super().unsubscribe(symbol)
        except Exception as e:
            self.log_error(f"구독 취소 중 오류: {e}")
            return False 

    async def _on_disconnection(self, msg: str) -> None:
        """웹소켓 연결 해제 콜백"""
        try:
            await self.reconnect()
        except Exception as e:
            self.log_error(f"연결 해제 처리 중 오류: {str(e)}") 