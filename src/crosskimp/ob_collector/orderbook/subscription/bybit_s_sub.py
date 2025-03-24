"""
바이비트 현물 구독 모듈

바이비트 현물 웹소켓 구독 및 메시지 처리 클래스를 제공합니다.
"""

import asyncio
import time
import json
import datetime
import websockets
from typing import Dict, List, Any, Optional, Union, Tuple, Set

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import Exchange, EXCHANGE_NAMES_KR

from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator

# 웹소켓 설정
WS_URL = "wss://stream.bybit.com/v5/public/spot"  # 웹소켓 URL
MAX_SYMBOLS_PER_SUBSCRIPTION = 10  # 구독당 최대 심볼 수
DEFAULT_DEPTH = 50  # 기본 오더북 깊이

# 로깅 설정
ENABLE_RAW_LOGGING = True  # raw 데이터 로깅 활성화 여부
ENABLE_ORDERBOOK_LOGGING = True  # 오더북 데이터 로깅 활성화 여부

# 로거 설정
logger = get_unified_logger()

class BybitSubscription(BaseSubscription):
    """
    바이빗 현물 구독 클래스
    
    바이빗 현물 거래소의 웹소켓 구독을 담당하는 클래스입니다.
    
    특징:
    - 웹소켓을 통한 스냅샷 수신 후 델타 업데이트 처리
    - 시퀀스 번호 기반 데이터 정합성 검증
    """
    
    # 1. 초기화 단계
    def __init__(self, connection: BaseWebsocketConnector, exchange_code: str = None, on_data_received=None):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
            exchange_code: 거래소 코드 (None이면 connection에서 가져옴)
            on_data_received: 데이터 수신 시 호출될 콜백 함수
        """
        # 부모 클래스 초기화
        super().__init__(connection, exchange_code, on_data_received)
        
        # 구독 관련 설정
        self.max_symbols_per_subscription = MAX_SYMBOLS_PER_SUBSCRIPTION
        self.depth_level = DEFAULT_DEPTH
        
        # 로깅 설정
        self.raw_logging_enabled = True  # raw 데이터 로깅 활성화
        self.orderbook_logging_enabled = ENABLE_ORDERBOOK_LOGGING  # 오더북 데이터 로깅 활성화
        
        # 각 심볼별 전체 오더북 상태 저장용
        self.orderbooks = {}  # symbol -> {"bids": {...}, "asks": {...}, "timestamp": ..., "sequence": ...}
    
    async def _ensure_websocket(self) -> bool:
        """
        웹소켓 연결 확보 - 바이빗용 버전
        
        부모 클래스의 _ensure_websocket 메서드를 사용하여 연결을 확보합니다.
        연결 관리는 Connector 클래스에서 전담합니다.
        """
        self.log_info("바이빗 웹소켓 연결 확보 시도")
        
        # 부모 클래스의 _ensure_websocket 메서드 호출
        success = await super()._ensure_websocket()
        
        if success:
            self.log_info("바이빗 웹소켓 연결 확보 완료")
        else:
            self.log_error("바이빗 웹소켓 연결 확보 실패")
                    
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
        # 단일 심볼을 리스트로 변환
        if isinstance(symbols, str):
            symbols = [symbols]
            
        # 심볼 형식 변환 ({symbol}USDT)
        args = []
        for sym in symbols:
            market = f"{sym}USDT"
            args.append(f"orderbook.{self.depth_level}.{market}")
        
        # 구독 메시지 생성
        return {
            "op": "subscribe",
            "args": args
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
            # 배치 단위로 구독 처리
            total_batches = (len(symbols) + self.max_symbols_per_subscription - 1) // self.max_symbols_per_subscription
            self.log_info(f"구독 시작 | 총 {len(symbols)}개 심볼, {total_batches}개 배치로 나눔")
            
            for i in range(0, len(symbols), self.max_symbols_per_subscription):
                batch_symbols = symbols[i:i + self.max_symbols_per_subscription]
                batch_num = (i // self.max_symbols_per_subscription) + 1
                
                # 구독 메시지 생성 및 전송
                subscribe_message = await self.create_subscribe_message(batch_symbols)
                if not await self.send_message(json.dumps(subscribe_message)):
                    raise Exception(f"배치 {batch_num}/{total_batches} 구독 메시지 전송 실패")
                
                self.log_info(f"구독 요청 전송 | 배치 {batch_num}/{total_batches}, {len(batch_symbols)}개 심볼")
                
                # 요청 간 짧은 딜레이 추가
                await asyncio.sleep(0.1)
                
            return True
        except Exception as e:
            self.log_error(f"구독 메시지 전송 중 오류: {str(e)}")
            return False
            
    
    async def subscribe(self, symbol):
        """
        심볼 또는 심볼 리스트 구독
        
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
    def _parse_json_message(self, message: str) -> Optional[Dict]:
        """
        메시지를 JSON으로 파싱하는 공통 헬퍼 메소드
        
        Args:
            message: 원시 메시지 (bytes 또는 str)
            
        Returns:
            Optional[Dict]: 파싱된 JSON 또는 None (파싱 실패시)
        """
        try:
            # 메시지가 bytes인 경우 문자열로 변환
            if isinstance(message, bytes):
                message = message.decode('utf-8')
                
            # 메시지가 문자열인 경우 JSON으로 파싱
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
                
            return data
        except Exception as e:
            self.log_error(f"JSON 파싱 실패: {str(e)}")
            return None
    
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
        내부 메시지 파싱 메소드
        
        외부 파서 없이 직접 메시지를 파싱하는 기능
        
        Args:
            message: 수신된 원시 메시지
            
        Returns:
            Optional[Dict]: 파싱된 오더북 데이터 또는 None (파싱 실패시)
        """
        try:
            # 공통 JSON 파싱 메소드 사용
            data = self._parse_json_message(message)
            if data is None:
                return None
                
            # 바이빗 메시지 구조 체크
            if "topic" not in data or "data" not in data:
                return None
            
            # 토픽이 orderbook으로 시작하는지 확인
            topic = data.get("topic", "")
            if not topic.startswith("orderbook."):
                return None
            
            # 메시지 타입 확인 (snapshot 또는 delta)
            data_type = data.get("type", "").lower()
            if data_type not in ["snapshot", "delta"]:
                return None
            
            # 심볼 추출 (orderbook.50.BTCUSDT -> BTC)
            topic_parts = topic.split(".")
            if len(topic_parts) < 3:
                return None
            
            market = topic_parts[2]  # BTCUSDT
            if not market.endswith("USDT"):
                return None
            
            symbol = market[:-4]  # BTCUSDT -> BTC
            
            # 데이터 추출
            orderbook_data = data.get("data", {})
            
            # 타임스탬프 추출 (메시지 자체의 ts 필드 사용)
            timestamp = data.get("ts")
            
            # 시퀀스 추출 (u: updateId)
            sequence = orderbook_data.get("u", 0)
            
            # 매수 호가 추출
            bids_data = orderbook_data.get("b", [])
            bids = [[float(price), float(size)] for price, size in bids_data]
            
            # 매도 호가 추출
            asks_data = orderbook_data.get("a", [])
            asks = [[float(price), float(size)] for price, size in asks_data]
            
            # 파싱된 데이터 반환
            return {
                "symbol": symbol,
                "timestamp": timestamp,
                "sequence": sequence,
                "bids": bids,
                "asks": asks,
                "type": data_type  # snapshot 또는 delta
            }
            
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}")
            return None
    
    # 5. 콜백 호출 단계
    # 부모 클래스의 _call_callback 메서드를 사용함
    
    async def _on_message(self, message: str) -> None:
        """
        메시지 수신 처리
        
        Args:
            message: 수신된 원시 메시지
        """
        try:
            # 메시지 처리 시작 시간 측정
            start_time = time.time()
            
            # 상위 클래스 _on_message 호출하여 raw 로깅 및 메시지 카운트 증가
            await super()._on_message(message)
            
            # 내부적으로 메시지 파싱
            parsed_data = self._parse_message(message)
                
            if not parsed_data:
                return
                
            symbol = parsed_data.get("symbol")
            if not symbol or symbol not in self.subscribed_symbols:
                return
            
            # 메시지 타입 확인
            is_snapshot = parsed_data.get("type") == "snapshot"
            
            # 오더북 데이터와 시퀀스 추출
            bids = parsed_data.get("bids", [])
            asks = parsed_data.get("asks", [])
            timestamp = parsed_data.get("timestamp")
            sequence = parsed_data.get("sequence")
            
            # 오더북 업데이트
            if is_snapshot:
                # 스냅샷인 경우 오더북 초기화
                self.orderbooks[symbol] = {
                    "bids": {float(bid[0]): float(bid[1]) for bid in bids},
                    "asks": {float(ask[0]): float(ask[1]) for ask in asks},
                    "timestamp": timestamp,
                    "sequence": sequence
                }
            else:
                # 델타인 경우 기존 오더북에 업데이트 적용
                if symbol not in self.orderbooks:
                    # 스냅샷 없이 델타가 먼저 도착한 경우 처리
                    self.log_warning(f"{symbol} 스냅샷 없이 델타 메시지 수신, 무시")
                    return
                
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
            sorted_bids = sorted(self.orderbooks[symbol]["bids"].items(), key=lambda x: x[0], reverse=True)
            sorted_asks = sorted(self.orderbooks[symbol]["asks"].items(), key=lambda x: x[0])
            
            # 배열 형태로 변환 [price, size]
            bids_array = [[price, size] for price, size in sorted_bids]
            asks_array = [[price, size] for price, size in sorted_asks]
            
            # 메시지 처리 - 완전한 오더북 데이터로 콜백 호출
            orderbook_data = {
                "bids": bids_array,
                "asks": asks_array,
                "timestamp": self.orderbooks[symbol]["timestamp"],
                "sequence": self.orderbooks[symbol]["sequence"],
                "type": "snapshot"  # 항상 완전한 스냅샷 형태로 전달
            }

            # 오더북 데이터 로깅 
            self.log_orderbook_data(symbol, orderbook_data)
                    
        except Exception as e:
            self.log_error(f"메시지 처리 실패: {str(e)}")
            # 내부 이벤트 발행 코드 삭제
    
    # 6. 구독 취소 단계
    async def create_unsubscribe_message(self, symbol: str) -> Dict:
        """
        구독 취소 메시지 생성
        
        Args:
            symbol: 심볼
        
        Returns:
            Dict: 구독 취소 메시지
        """
        market = f"{symbol}USDT"
        
        return {
            "op": "unsubscribe",
            "args": [f"orderbook.{self.depth_level}.{market}"]
        }
        
    # async def unsubscribe 메서드 제거 - 부모 클래스의 메서드를 상속받아 사용 

    async def process_error_message(self, message: str) -> None:
        """에러 메시지 처리"""
        self.log_error(f"에러 메시지 수신: {message}")
        
        # 에러 이벤트 발행 코드 삭제
 