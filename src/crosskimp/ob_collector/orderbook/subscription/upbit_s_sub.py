"""
업비트 구독 클래스

이 모듈은 업비트 거래소의 웹소켓 구독을 담당하는 클래스를 제공합니다.
"""

import json
import asyncio
import datetime
import time
from typing import Dict, List, Optional, Union

from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
from crosskimp.config.constants_v3 import Exchange

# 웹소켓 설정
WS_URL = "wss://api.upbit.com/websocket/v1"  # 웹소켓 URL
MAX_SYMBOLS_PER_SUBSCRIPTION = 10  # 구독당 최대 심볼 수

class UpbitSubscription(BaseSubscription):
    """
    업비트 구독 클래스
    
    업비트 거래소의 웹소켓 구독을 담당하는 클래스입니다.
    
    책임:
    - 구독 관리 (구독, 구독 취소)
    - 메시지 처리 및 파싱
    - 콜백 호출
    - 원시 데이터 로깅
    """
    
    # 1. 초기화 단계
    def __init__(self, connection):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
        """
        # 부모 클래스 초기화 (exchange_code 전달)
        super().__init__(connection, Exchange.UPBIT.value)
        
        # 구독 관련 설정
        self.max_symbols_per_subscription = MAX_SYMBOLS_PER_SUBSCRIPTION
        
        # 로깅 설정 - 활성화
        self.log_orderbook_enabled = True
        
        # 업비트 오더북 검증기 초기화
        self.validator = BaseOrderBookValidator(Exchange.UPBIT.value)

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
        
        # 구독 메시지 생성
        message = [
            {"ticket": f"upbit_orderbook_{int(time.time())}"},
            {
                "type": "orderbook",
                "codes": market_codes,
                "is_only_realtime": False  # 스냅샷 포함 요청
            },
            {"format": "DEFAULT"}
        ]
        
        symbols_str = ", ".join(symbols) if len(symbols) <= 5 else f"{len(symbols)}개 심볼"
        self.log_debug(f"[구독 메시지] {symbols_str} 구독 메시지 생성")
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
            # 배치 단위로 구독 처리
            total_batches = (len(symbols) + self.max_symbols_per_subscription - 1) // self.max_symbols_per_subscription
            self.log_info(f"구독 시작 | 총 {len(symbols)}개 심볼, {total_batches}개 배치로 나눔")
            
            for i in range(0, len(symbols), self.max_symbols_per_subscription):
                batch_symbols = symbols[i:i + self.max_symbols_per_subscription]
                batch_num = (i // self.max_symbols_per_subscription) + 1
                
                # 구독 메시지 생성 및 전송
                subscribe_message = await self.create_subscribe_message(batch_symbols)
                if not await self.connection.send_message(json.dumps(subscribe_message)):
                    raise Exception(f"배치 {batch_num}/{total_batches} 구독 메시지 전송 실패")
                
                self.log_info(f"구독 요청 전송 | 배치 {batch_num}/{total_batches}, {len(batch_symbols)}개 심볼")
                
                # 요청 간 짧은 딜레이 추가
                await asyncio.sleep(0.1)
                
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
        
        업비트는 모든 오더북 메시지가 스냅샷 형태이므로,
        메시지 타입이 orderbook인지 확인합니다.
        
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
                
            # orderbook 타입 확인
            is_orderbook = data.get("type") == "orderbook"
            
            return is_orderbook
            
        except Exception as e:
            self.log_error(f"스냅샷 메시지 확인 중 오류: {e}")
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
            
            # 파싱된 데이터 반환
            return {
                "symbol": symbol,
                "timestamp": timestamp,
                "sequence": sequence,
                "bids": bids,
                "asks": asks
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
        """
        메시지 수신 처리
        
        Args:
            message: 수신된 원시 메시지
        """
        try:
            # 내부적으로 메시지 파싱
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
            
            # 유효한 데이터만 사용
            orderbook_data = {
                "bids": parsed_data.get("bids", []),
                "asks": parsed_data.get("asks", []),
                "timestamp": timestamp,
                "sequence": parsed_data.get("sequence")
            }
            
            # 검증된 오더북 데이터 로깅
            self.log_orderbook_data(symbol, orderbook_data)
            
            # 콜백 호출
            if symbol in self.snapshot_callbacks:
                await self.snapshot_callbacks[symbol](symbol, orderbook_data)
                
        except Exception as e:
            self.log_error(f"메시지 처리 실패: {str(e)}")
            self.metrics_manager.record_error(self.exchange_code)
    
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
    
    # REST API 관련 메서드