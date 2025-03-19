"""
바이빗 선물 구독 클래스

이 모듈은 바이빗 선물 거래소의 웹소켓 연결을 통해 오더북 데이터를 구독하는 클래스를 제공합니다.
"""

import asyncio
import json
import time
import datetime
from typing import Dict, List, Optional, Union, Any

from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
from crosskimp.config.constants_v3 import Exchange

# 웹소켓 설정
WS_URL = "wss://stream.bybit.com/v5/public/linear"  # 웹소켓 URL 
MAX_SYMBOLS_PER_SUBSCRIPTION = 10  # 구독당 최대 심볼 수
DEFAULT_DEPTH = 50  # 기본 오더북 깊이

class BybitFutureSubscription(BaseSubscription):
    """
    바이빗 선물 구독 클래스
    
    바이빗 선물 거래소의 웹소켓 연결을 통해 오더북 데이터를 구독하는 클래스입니다.
    
    특징:
    - 메시지 형식: 스냅샷 및 델타 업데이트 지원
    - 웹소켓을 통한 스냅샷 및 델타 업데이트 수신
    """
    
    # 1. 초기화 단계
    def __init__(self, connection):
        """
        바이빗 선물 구독 초기화
        
        Args:
            connection: 바이빗 선물 웹소켓 연결 객체
        """
        # 부모 클래스 초기화 (exchange_code 전달)
        super().__init__(connection, Exchange.BYBIT_FUTURE.value)
        
        # 구독 관련 설정
        self.max_symbols_per_subscription = MAX_SYMBOLS_PER_SUBSCRIPTION
        self.depth_level = DEFAULT_DEPTH
        
        # 로깅 설정 - 활성화
        self.log_orderbook_enabled = True
        
        # 바이빗 오더북 검증기 초기화
        self.validator = BaseOrderBookValidator(Exchange.BYBIT_FUTURE.value)
        
        # 각 심볼별 전체 오더북 상태 저장용
        self.orderbooks = {}  # symbol -> {"bids": {...}, "asks": {...}, "timestamp": ..., "sequence": ...}

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
            success = await self.connection.send_message(json_msg)
            
            if not success:
                self.log_error(f"구독 메시지 전송 실패: {symbols[:3]}...")
                return False
            
            self.log_info(f"구독 메시지 전송 성공: {symbols}")
            return True
        except Exception as e:
            self.log_error(f"구독 메시지 전송 중 오류: {str(e)}")
            return False
    
    async def subscribe(self, symbol, on_snapshot=None, on_delta=None, on_error=None):
        """
        심볼 구독
        
        단일 심볼 또는 심볼 리스트를 구독합니다.
        
        Args:
            symbol: 구독할 심볼 또는 심볼 리스트
            on_snapshot: 스냅샷 수신 시 호출할 콜백 함수
            on_delta: 델타 수신 시 호출할 콜백 함수
            on_error: 에러 발생 시 호출할 콜백 함수
            
        Returns:
            bool: 구독 성공 여부
        """
        try:
            # 심볼 전처리
            symbols = await self._preprocess_symbols(symbol)
            if not symbols:
                return False
                
            # 콜백 등록
            await self._register_callbacks(symbols, on_snapshot, on_delta, on_error)
            
            # 연결 확인 및 연결
            if not await self._ensure_connection():
                raise Exception("웹소켓 연결 실패")
            
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
            self.metrics_manager.record_error(self.exchange_code)
            
            if on_error is not None:
                if isinstance(symbol, list):
                    for sym in symbol:
                        await on_error(sym, str(e))
                else:
                    await on_error(symbol, str(e))
            return False
    
    # 4. 메시지 수신 및 처리 단계
    def is_snapshot_message(self, message: str) -> bool:
        """
        메시지가 스냅샷인지 확인
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 스냅샷 메시지인 경우 True
        """
        # _parse_message 결과를 활용하여 타입 판별
        parsed_data = self._parse_message(message)
        return parsed_data is not None and parsed_data.get("type") == "snapshot"
    
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
                    if timestamp is None:
                        timestamp = int(time.time() * 1000)  # 현재 시간을 밀리초로 사용
                        
                    sequence = inner_data.get("seq") or inner_data.get("u")  # 시퀀스 번호
                    
                    # 결과 구성
                    result = {
                        "symbol": symbol,
                        "type": msg_type,
                        "bids": bids,
                        "asks": asks,
                        "timestamp": timestamp,  # 타임스탬프 추출
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
        # 로그 출력 없이 메서드만 유지 (호환성을 위해)
        pass
    
    # 5. 콜백 호출 단계
    # 부모 클래스의 _call_callback 메서드를 사용함

    async def _on_message(self, message: str) -> None:
        """
        메시지 수신 및 처리
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        try:
            # 원시 메시지 로깅
            self.log_raw_message(message)
            
            # JSON 파싱
            data = json.loads(message)
            
            # 구독 응답 처리
            if data.get("op") == "subscribe":
                # self.log_debug(f"구독 응답 수신: {data}")
                return
                
            # 오더북 메시지 처리
            if "topic" in data and "data" in data:
                topic = data.get("topic", "")
                if "orderbook" in topic:
                    parts = topic.split(".")
                    if len(parts) >= 3:
                        symbol = parts[-1].replace("USDT", "")
                        msg_type = data.get("type", "delta").lower()  # "snapshot" or "delta"
                        
                        # 메시지 파싱
                        parsed_data = self._parse_message(message)
                        if not parsed_data:
                            return
                            
                        # 메시지 타입에 따른 처리
                        if msg_type == "snapshot":
                            # 현물과 같은 방식으로 오더북 초기화
                            bids = parsed_data.get("bids", [])
                            asks = parsed_data.get("asks", [])
                            timestamp = parsed_data.get("timestamp")
                            sequence = parsed_data.get("sequence")
                            
                            # 스냅샷인 경우 오더북 초기화
                            self.orderbooks[symbol] = {
                                "bids": {float(bid[0]): float(bid[1]) for bid in bids},
                                "asks": {float(ask[0]): float(ask[1]) for ask in asks},
                                "timestamp": timestamp,
                                "sequence": sequence
                            }
                            
                            # 오더북 초기화 후 콜백 호출
                            sorted_bids = sorted(self.orderbooks[symbol]["bids"].items(), key=lambda x: x[0], reverse=True)
                            sorted_asks = sorted(self.orderbooks[symbol]["asks"].items(), key=lambda x: x[0])
                            
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
                            
                            # 오더북 데이터 로깅
                            self.log_orderbook_data(symbol, orderbook_data)
                            
                            # 스냅샷 또는 델타 메시지 각각에 맞는 처리
                            if symbol in self.snapshot_callbacks:
                                await self._call_callback(symbol, orderbook_data, is_snapshot=True)
                                # 메트릭 업데이트
                                self.metrics_manager.update_message_stats(self.exchange_code, "snapshot")
                            elif symbol in self.delta_callbacks:
                                await self._call_callback(symbol, orderbook_data, is_snapshot=False)
                                # 메트릭 업데이트
                                self.metrics_manager.update_message_stats(self.exchange_code, "delta")
                        
                        elif msg_type == "delta":
                            # 델타 처리 - symbol이 orderbooks에 있을 때만 처리
                            if symbol in self.orderbooks:
                                # 델타 데이터 추출
                                bids = parsed_data.get("bids", [])
                                asks = parsed_data.get("asks", [])
                                timestamp = parsed_data.get("timestamp")
                                sequence = parsed_data.get("sequence")
                                
                                # 시퀀스 확인
                                if sequence <= self.orderbooks[symbol]["sequence"]:
                                    self.log_warning(f"{symbol} 이전 시퀀스의 델타 메시지 수신, 무시 ({sequence} <= {self.orderbooks[symbol]['sequence']})")
                                    return
                                
                                # 기존 오더북 가져오기
                                orderbook = self.orderbooks[symbol]
                                
                                # 매수 호가 업데이트
                                for bid in bids:
                                    price = float(bid[0])
                                    size = float(bid[1])
                                    if size == 0:
                                        # 수량이 0이면 해당 가격의 호가 삭제
                                        orderbook["bids"].pop(price, None)
                                    else:
                                        # 그렇지 않으면 추가 또는 업데이트
                                        orderbook["bids"][price] = size
                                
                                # 매도 호가 업데이트
                                for ask in asks:
                                    price = float(ask[0])
                                    size = float(ask[1])
                                    if size == 0:
                                        # 수량이 0이면 해당 가격의 호가 삭제
                                        orderbook["asks"].pop(price, None)
                                    else:
                                        # 그렇지 않으면 추가 또는 업데이트
                                        orderbook["asks"][price] = size
                                
                                # 타임스탬프와 시퀀스 업데이트
                                orderbook["timestamp"] = timestamp
                                orderbook["sequence"] = sequence
                                
                                # 정렬된 전체 오더북 구성
                                # 매수(높은 가격 -> 낮은 가격), 매도(낮은 가격 -> 높은 가격)
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
                                    "type": "snapshot"  # 델타도 스냅샷으로 전달
                                }
                                
                                # 오더북 데이터 로깅
                                self.log_orderbook_data(symbol, orderbook_data)
                                
                                # 델타 콜백 호출
                                await self._call_callback(symbol, orderbook_data, is_snapshot=False)
                                # 메트릭 업데이트 추가
                                self.metrics_manager.update_message_stats(self.exchange_code, "delta")
                            else:
                                self.log_warning(f"{symbol} 스냅샷 없이 델타 수신, 무시")
        except json.JSONDecodeError:
            self.log_error(f"JSON 파싱 실패: {message[:100]}...")
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