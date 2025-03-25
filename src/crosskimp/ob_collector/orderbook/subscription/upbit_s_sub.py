"""
업비트 구독 클래스

이 모듈은 업비트 거래소의 웹소켓 구독을 담당하는 클래스를 제공합니다.
"""

import json
import asyncio
import time
import datetime
import uuid
from typing import Dict, List, Union, Optional, Any, Set, Tuple

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import Exchange, EXCHANGE_NAMES_KR

from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector

# 웹소켓 설정
WS_URL = "wss://api.upbit.com/websocket/v1"  # 웹소켓 URL
MAX_SYMBOLS_PER_SUBSCRIPTION = 100  # 구독당 최대 심볼 수 (업데이트: 업비트는 대량의 심볼 구독 지원)

# 로깅 설정
ENABLE_RAW_LOGGING = True  # raw 데이터 로깅 활성화 여부
ENABLE_ORDERBOOK_LOGGING = True  # 오더북 데이터 로깅 활성화 여부

class UpbitSubscription(BaseSubscription):
    """
    업비트 구독 클래스
    
    업비트 거래소의 웹소켓 구독을 담당하는 클래스입니다.
    
    책임:
    - 구독 관리 (구독, 구독 취소)
    - 메시지 처리 및 파싱
    - 이벤트 발행
    - 원시 데이터 로깅
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
        
        # 로깅 설정
        self.raw_logging_enabled = ENABLE_RAW_LOGGING
        self.orderbook_logging_enabled = ENABLE_ORDERBOOK_LOGGING

    # 3. 구독 처리 단계
    async def create_subscribe_message(self, symbol: Union[str, List[str]]) -> Dict:
        """
        구독 메시지 생성
        
        단일 심볼 또는 심볼 리스트에 대한 구독 메시지를 생성합니다.
        
        Args:
            symbol: 구독할 심볼 또는 심볼 리스트 (예: "BTC" 또는 ["BTC", "ETH"])
            
        Returns:
            Dict: 구독 메시지
        """
        # 심볼이 리스트인지 확인하고 처리
        if isinstance(symbol, list):
            symbols = symbol
        else:
            symbols = [symbol]
            
        # 심볼 형식 변환 (KRW-{symbol})
        market_codes = [f"KRW-{s.upper()}" for s in symbols]
        
        # 구독 메시지 생성 - level:0으로 모든 오더북 변경사항 수신
        # 고유한 ticket 이름 생성
        current_timestamp = int(time.time() * 1000)
        first_symbols = symbols[:3] if len(symbols) > 3 else symbols
        ticket_name = f"upbit_orderbook_{current_timestamp}_{','.join(first_symbols)}"
        
        message = [
            {"ticket": ticket_name},
            {
                "type": "orderbook",
                "codes": market_codes,
                "is_only_realtime": False,  # 스냅샷 포함 요청
                "level": 0  # 모든 변경사항 실시간 수신
            },
            {"format": "DEFAULT"}
        ]
        
        symbols_str = ", ".join(symbols) if len(symbols) <= 5 else f"{len(symbols)}개 심볼"
        self.log_debug(f"[구독 메시지] {symbols_str} 구독 메시지 생성 (level:0)")
        return message
    
    async def _send_subscription_requests(self, symbols: List[str]) -> bool:
        """
        구독 요청 전송
        
        Args:
            symbols: 구독할 심볼 리스트
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            # 한 번에 모든 심볼 구독
            self.log_info(f"구독 시작 | 총 {len(symbols)}개 심볼을 한 번에 구독")
            
            # 구독 메시지 생성 및 전송
            subscribe_message = await self.create_subscribe_message(symbols)
            if not await self.send_message(json.dumps(subscribe_message)):
                raise Exception(f"구독 메시지 전송 실패")
            
            self.log_info(f"구독 요청 전송 | {len(symbols)}개 심볼")
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
            # 웹소켓 연결 확보
            if not await self._ensure_websocket():
                self.log_error("웹소켓 연결이 없어 구독 실패")
                return False
            
            # 연결 후 안정화를 위한 대기
            # 웹소켓 연결이 완전히 설정될 시간 확보
            self.log_info("웹소켓 연결 안정화를 위해 1초 대기")
            await asyncio.sleep(1.0)
            
            # 연결 상태 다시 확인
            if not self.connection.is_connected or not await self.connection.get_websocket():
                self.log_error("웹소켓 연결이 유지되지 않아 구독 실패")
                return False
                
            # 심볼 전처리
            symbols = await self._preprocess_symbols(symbol)
            if not symbols:
                return False
            
            # 구독 상태 업데이트
            for sym in symbols:
                self.subscribed_symbols[sym] = True
            
            # 메시지 수신 루프 시작 (이미 시작되지 않은 경우)
            if not self.message_loop_task or self.message_loop_task.done():
                self.log_info("메시지 수신 루프 시작")
                self.stop_event.clear()
                self.message_loop_task = asyncio.create_task(self.message_loop())
            
            # 구독 요청 전송 - 최대 3번 재시도
            max_retries = 3
            for attempt in range(1, max_retries + 1):
                try:
                    if await self._send_subscription_requests(symbols):
                        break
                    else:
                        if attempt < max_retries:
                            self.log_warning(f"구독 시도 {attempt}/{max_retries} 실패, 재시도...")
                            await asyncio.sleep(0.5)
                        else:
                            raise Exception(f"구독 메시지 전송 최대 시도 횟수({max_retries}회) 초과")
                except Exception as e:
                    if attempt < max_retries:
                        self.log_warning(f"구독 시도 {attempt}/{max_retries} 중 오류: {e}, 재시도...")
                        await asyncio.sleep(0.5)
                    else:
                        raise
            
            return True
            
        except Exception as e:
            self.log_error(f"구독 중 오류 발생: {str(e)}")
            self._update_metrics("error_count", 1, op="increment")
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
        try:
            # JSON 파싱
            if isinstance(message, bytes):
                message = message.decode('utf-8')
                
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
                
            # 업비트는 stream_type 필드로 구분
            stream_type = data.get("stream_type", "")
            return stream_type.upper() == "SNAPSHOT" if stream_type else False
            
        except Exception as e:
            self.log_error(f"스냅샷 메시지 확인 중 오류: {e}")
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
            if isinstance(message, bytes):
                message = message.decode('utf-8')
                
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
                
            # 업비트는 type과 stream_type으로 구분
            message_type = data.get("type")
            stream_type = data.get("stream_type", "")
            
            return message_type == "orderbook" and stream_type.upper() == "REALTIME"
            
        except Exception as e:
            self.log_error(f"델타 메시지 확인 중 오류: {e}")
            return False
    
    def _parse_message(self, message: str) -> dict:
        """
        내부 메시지 파싱 메소드
        
        외부 파서 없이 직접 메시지를 파싱하는 기능
        
        Args:
            message: 수신된 원시 메시지
            
        Returns:
            dict: 파싱된 오더북 데이터
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
                
            # 타입 체크 (orderbook 타입만 처리)
            if data.get("type") != "orderbook":
                return None
                
            # 심볼 추출 (KRW-BTC -> BTC)
            code = data.get("code", "")
            if not code or not code.startswith("KRW-"):
                return None
                
            symbol = code.split("-")[1]
            
            # 타임스탬프 및 시퀀스 추출
            timestamp = data.get("timestamp")
            sequence = timestamp  # 업비트는 별도 시퀀스가 없으므로 타임스탬프 사용
            
            # 호가 데이터 추출 및 가공
            orderbook_units = data.get("orderbook_units", [])
            
            # 매수/매도 호가 배열 생성
            bids = []  # 매수 호가 [가격, 수량]
            asks = []  # 매도 호가 [가격, 수량]
            
            for unit in orderbook_units:
                # 매수 호가 추가
                bid_price = unit.get("bid_price")
                bid_size = unit.get("bid_size")
                if bid_price is not None and bid_size is not None:
                    bids.append([float(bid_price), float(bid_size)])
                    
                # 매도 호가 추가
                ask_price = unit.get("ask_price")
                ask_size = unit.get("ask_size")
                if ask_price is not None and ask_size is not None:
                    asks.append([float(ask_price), float(ask_size)])
            
            # 가격 기준 내림차순 정렬 (매수 호가)
            bids.sort(key=lambda x: x[0], reverse=True)
            
            # 가격 기준 오름차순 정렬 (매도 호가)
            asks.sort(key=lambda x: x[0])
            
            # 스트림 타입 확인 (스냅샷 vs 실시간)
            stream_type = data.get("stream_type", "")
            is_snapshot = stream_type.upper() == "SNAPSHOT"
            
            # 파싱된 데이터 반환
            return {
                "symbol": symbol,
                "timestamp": timestamp,
                "sequence": sequence,
                "bids": bids,
                "asks": asks,
                "type": "snapshot" if is_snapshot else "delta"
            }
            
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}")
            return None
    
    def _validate_timestamp(self, symbol: str, timestamp: int) -> bool:
        """
        타임스탬프 기반 간단한 시퀀스 검증
        
        Args:
            symbol: 심볼
            timestamp: 타임스탬프
            
        Returns:
            bool: 타임스탬프가 유효하면 True, 아니면 False
        """
        # 타임스탬프가 없으면 유효하지 않음
        if timestamp is None:
            return False
            
        # 심볼의 마지막 타임스탬프와 비교
        if symbol in self.last_timestamps:
            last_ts = self.last_timestamps[symbol]
            if timestamp <= last_ts:
                # 타임스탬프 역전 감지
                return False
        
        # 마지막 타임스탬프 업데이트
        self.last_timestamps[symbol] = timestamp
        return True
    
    # 5. 콜백 호출 단계
    async def _on_message(self, message: str) -> None:
        """메시지 수신 처리"""
        try:
            # 메시지 처리 시작 시간 측정
            start_time = time.time()
            
            # 상위 클래스 _on_message 호출하여 메시지 카운트 증가 등 공통 처리
            await super()._on_message(message)
            
            # 메시지 파싱
            parsed_data = self._parse_message(message)
            if not parsed_data:
                return
                
            symbol = parsed_data.get("symbol")
            if not symbol or symbol not in self.subscribed_symbols:
                return
                
            # 간단한 시퀀스 검증 (타임스탬프 기반)
            timestamp = parsed_data.get("timestamp")
            if not self._validate_timestamp(symbol, timestamp):
                self.log_warning(f"{symbol} 타임스탬프 역전 감지: skip")
                return
            
            try:
                if self.validator:
                    # 메시지 타입 확인 (스냅샷 또는 델타)
                    is_snapshot = parsed_data.get("type") == "snapshot"
                    
                    # 검증 및 처리
                    if is_snapshot:
                        result = await self.validator.initialize_orderbook(symbol, parsed_data)
                        # 메트릭 업데이트
                        if result.is_valid:
                            processing_time_ms = (time.time() - start_time) * 1000
                            self.log_debug(f"[{symbol}] 스냅샷 처리 완료 (처리 시간: {processing_time_ms:.2f}ms)")
                    else:
                        result = await self.validator.update(symbol, parsed_data)
                        # 메트릭 업데이트
                        if result.is_valid:
                            processing_time_ms = (time.time() - start_time) * 1000
                            self.log_debug(f"[{symbol}] 델타 처리 완료 (처리 시간: {processing_time_ms:.2f}ms)")
                    
                    if result.is_valid:
                        # 유효한 오더북 데이터 가져오기
                        orderbook_data = self.validator.get_orderbook(symbol)
                        
                        # 오더북 데이터 로깅 (플래그 확인)
                        if self.orderbook_logging_enabled:
                            self.log_orderbook_data(symbol, orderbook_data)
                    else:
                        self.log_error(f"{symbol} 오더북 검증 실패: {result.errors}")
            except Exception as e:
                self.log_error(f"{symbol} 오더북 검증 중 오류: {str(e)}")
                
        except Exception as e:
            self.log_error(f"메시지 처리 실패: {str(e)}")
    
    # 6. 구독 취소 단계
    async def create_unsubscribe_message(self, symbol: str) -> Dict:
        """
        구독 취소 메시지 생성
        
        업비트는 별도의 구독 취소 메시지가 없으므로, 
        빈 구독 메시지를 반환하여 기존 구독을 대체합니다.
        
        Args:
            symbol: 구독 취소할 심볼 (예: "BTC")
            
        Returns:
            Dict: 구독 취소 메시지 (빈 메시지)
        """
        # 업비트는 구독 취소 메시지가 없으므로 빈 배열 반환
        return [
            {"ticket": f"upbit_unsubscribe_{int(time.time())}"},
            {"type": "orderbook", "codes": []},
            {"format": "DEFAULT"}
        ]
    
    async def unsubscribe(self, symbol: Optional[str] = None) -> bool:
        """구독 취소"""
        try:
            return await super().unsubscribe(symbol)
        except Exception as e:
            self.log_error(f"구독 취소 중 오류: {e}")
            return False
    
    # REST API 관련 메서드