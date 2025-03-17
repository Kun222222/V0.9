"""
업비트 구독 클래스

이 모듈은 업비트 거래소의 웹소켓 구독을 담당하는 클래스를 제공합니다.
"""

import json
import time
import asyncio
import os
import datetime
from typing import Dict, List, Any, Optional, Callable, Union
import websockets.exceptions
from enum import Enum

from crosskimp.ob_collector.orderbook.subscription.base_subscription import (
    BaseSubscription, 
    SnapshotMethod, 
    DeltaMethod,
    SubscriptionStatus
)
from crosskimp.ob_collector.orderbook.connection.base_ws_connector import BaseWebsocketConnector
from crosskimp.logger.logger import create_raw_logger
from crosskimp.config.paths import LOG_SUBDIRS
from crosskimp.ob_collector.orderbook.validator.validators import UpbitOrderBookValidator

# ============================
# 업비트 구독 관련 상수
# ============================
# 거래소 코드
EXCHANGE_CODE = "upbit"

# 웹소켓 설정
REST_URL = "https://api.upbit.com/v1/orderbook"  # https://docs.upbit.com/reference/호가-정보-조회
WS_URL = "wss://api.upbit.com/websocket/v1"     # https://docs.upbit.com/reference/websocket-orderbook

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
    
    def __init__(self, connection: BaseWebsocketConnector, parser=None):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
            parser: 메시지 파싱 객체 (선택적)
        """
        # 부모 클래스 초기화 (exchange_code 전달)
        super().__init__(connection, EXCHANGE_CODE, parser)
        
        # 로깅 설정
        self.log_raw_data = True
        self.raw_logger = None
        self._setup_raw_logging()
        
        # 업비트 전용 검증기 초기화
        self.validator = UpbitOrderBookValidator(EXCHANGE_CODE)
        
        # 오더북 데이터 저장소
        self.orderbooks = {}  # symbol -> orderbook_data
    
    def _setup_raw_logging(self):
        """Raw 데이터 로깅 설정"""
        try:
            # 로그 디렉토리 설정
            raw_data_dir = LOG_SUBDIRS['raw_data']
            log_dir = raw_data_dir / EXCHANGE_CODE
            log_dir.mkdir(exist_ok=True, parents=True)
            
            # 로그 파일 경로 설정
            current_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_file_path = log_dir / f"{EXCHANGE_CODE}_raw_{current_datetime}.log"
            
            # 로거 설정
            self.raw_logger = create_raw_logger(EXCHANGE_CODE)
            self.logger.info(f"{EXCHANGE_CODE} raw 로거 초기화 완료")
        except Exception as e:
            self.logger.error(f"Raw 로깅 설정 실패: {str(e)}", exc_info=True)
            self.log_raw_data = False
    
    def _get_snapshot_method(self) -> SnapshotMethod:
        """
        스냅샷 수신 방법 반환
        
        업비트는 웹소켓을 통해 스냅샷을 수신합니다.
        
        Returns:
            SnapshotMethod: WEBSOCKET
        """
        return SnapshotMethod.WEBSOCKET
    
    def _get_delta_method(self) -> DeltaMethod:
        """
        델타 수신 방법 반환
        
        업비트는 델타를 사용하지 않고 항상 전체 스냅샷을 수신합니다.
        
        Returns:
            DeltaMethod: NONE
        """
        return DeltaMethod.NONE
    
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
        self.logger.debug(f"[구독 메시지] {symbols_str} 구독 메시지 생성")
        return message
    
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
            self.logger.error(f"[{self.exchange_code}] 스냅샷 메시지 확인 중 오류: {e}")
            return False
    
    def log_raw_message(self, message: str) -> None:
        """
        원본 메시지 로깅 (raw_data 디렉토리)
        
        Args:
            message: 원본 메시지
        """
        try:
            # raw_logger가 있는 경우에만 로깅
            if self.log_raw_data and self.raw_logger:
                # 메시지 타입과 심볼 추출 시도
                try:
                    if isinstance(message, bytes):
                        message_str = message.decode('utf-8')
                    else:
                        message_str = message
                        
                    data = json.loads(message_str)
                    msg_type = data.get("type", "unknown")
                    code = data.get("code", "unknown")
                    symbol = code.replace("KRW-", "") if code.startswith("KRW-") else code
                except:
                    msg_type = "unknown"
                    symbol = "unknown"
                
                # 타임스탬프 추가
                timestamp = int(time.time() * 1000)
                log_entry = f"{timestamp}|{msg_type}|{symbol}|{message}"
                
                # 로깅
                self.raw_logger.debug(log_entry)
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] 원본 메시지 로깅 실패: {str(e)}")
    
    async def _on_message(self, message: str) -> None:
        """
        메시지 수신 처리
        
        Args:
            message: 수신된 원시 메시지
        """
        try:
            # 원본 메시지 로깅
            self.log_raw_message(message)
            
            # 메트릭 업데이트
            self.metrics_manager.record_message(self.exchange_code)
            
            # 메시지 크기 기록 (바이트)
            if isinstance(message, str):
                message_size = len(message.encode('utf-8'))
            elif isinstance(message, bytes):
                message_size = len(message)
            else:
                message_size = 0
            self.metrics_manager.record_bytes(self.exchange_code, message_size)
            
            # 메시지 파싱
            parsed_data = None
            if self.parser:
                parsed_data = self.parser.parse_message(message)
                
            if not parsed_data:
                return
                
            symbol = parsed_data.get("symbol")
            if not symbol or symbol not in self.subscribed_symbols:
                return
                
            # 파싱된 데이터 로깅 (raw_logger 사용)
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            log_data = {
                "exchangename": self.exchange_code,
                "symbol": symbol,
                "bids": parsed_data.get("bids", []),
                "asks": parsed_data.get("asks", []),
                "timestamp": parsed_data.get("timestamp"),
                "sequence": parsed_data.get("sequence")
            }
            if self.raw_logger:
                self.raw_logger.debug(f"[{current_time}] {json.dumps(log_data)}")
                
            # 검증
            validation = self.validator.validate_orderbook(
                symbol=symbol,
                bids=parsed_data["bids"],
                asks=parsed_data["asks"],
                timestamp=parsed_data.get("timestamp"),
                sequence=parsed_data.get("sequence")
            )
            
            if not validation.is_valid:
                self.logger.warning(f"[{self.exchange_code}] {symbol} 검증 실패: {validation.errors}")
                return
                
            # 오더북 업데이트
            self.orderbooks[symbol] = {
                "bids": parsed_data["bids"][:10],  # 상위 10개만
                "asks": parsed_data["asks"][:10],  # 상위 10개만
                "timestamp": parsed_data["timestamp"],
                "sequence": parsed_data["sequence"]
            }
            
            # 검증된 오더북 데이터 로깅 (raw_logger 사용)
            if self.raw_logger:
                validated_data = {
                    "exchangename": self.exchange_code,
                    "symbol": symbol,
                    "bids": self.orderbooks[symbol]["bids"],
                    "asks": self.orderbooks[symbol]["asks"],
                    "timestamp": self.orderbooks[symbol]["timestamp"],
                    "sequence": self.orderbooks[symbol]["sequence"]
                }
                self.raw_logger.debug(f"[{current_time}] {json.dumps(validated_data)}")
            
            # 콜백 호출
            if symbol in self.snapshot_callbacks:
                await self.snapshot_callbacks[symbol](symbol, self.orderbooks[symbol])
                
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] 메시지 처리 실패: {str(e)}")
            self.metrics_manager.record_error(self.exchange_code)
    
    async def subscribe(self, symbol, on_snapshot=None, on_delta=None, on_error=None):
        """
        심볼 구독
        
        단일 심볼 또는 심볼 리스트를 구독합니다.
        업비트는 한 번의 요청으로 여러 심볼을 구독할 수 있습니다.
        
        업비트는 항상 스냅샷만 제공하므로 델타 콜백은 무시됩니다.
        
        Args:
            symbol: 구독할 심볼 또는 심볼 리스트
            on_snapshot: 스냅샷 수신 시 호출할 콜백 함수
            on_delta: 델타 수신 시 호출할 콜백 함수 (업비트는 무시됨)
            on_error: 에러 발생 시 호출할 콜백 함수
            
        Returns:
            bool: 구독 성공 여부
        """
        try:
            # 심볼이 리스트인지 확인하고 처리
            if isinstance(symbol, list):
                symbols = symbol
            else:
                symbols = [symbol]
                
            if not symbols:
                self.logger.warning(f"[{self.exchange_code}] 구독할 심볼이 없습니다.")
                return False
                
            # 이미 구독 중인 심볼 필터링
            new_symbols = [s for s in symbols if s not in self.subscribed_symbols]
            if not new_symbols:
                self.logger.info(f"[{self.exchange_code}] 모든 심볼이 이미 구독 중입니다.")
                return True
                
            # 콜백 함수 등록
            for sym in new_symbols:
                if on_snapshot is not None:
                    self.snapshot_callbacks[sym] = on_snapshot
                # 업비트는 델타를 사용하지 않으므로 델타 콜백은 등록하지 않음
                if on_error is not None:
                    self.error_callbacks[sym] = on_error
            
            # 연결 확인 및 연결
            if not hasattr(self.connection, 'is_connected') or not self.connection.is_connected:
                self.logger.info(f"[{self.exchange_code}] 웹소켓 연결 시작")
                if not await self.connection.connect():
                    raise Exception("웹소켓 연결 실패")
                
                # 연결 상태 메트릭 업데이트
                self.metrics_manager.update_connection_state(self.exchange_code, "connected")
            
            # 구독 메시지 생성 및 전송
            subscribe_message = await self.create_subscribe_message(new_symbols)
            if not await self.connection.send_message(json.dumps(subscribe_message)):
                raise Exception("구독 메시지 전송 실패")
            
            # 구독 상태 업데이트
            for sym in new_symbols:
                self.subscribed_symbols[sym] = SubscriptionStatus.SUBSCRIBED
            
            self.logger.info(f"[{self.exchange_code}] 구독 성공: {len(new_symbols)}개 심볼")
            
            # 첫 번째 구독 시 메시지 수신 루프 시작
            if not self.message_loop_task or self.message_loop_task.done():
                self.logger.info(f"[{self.exchange_code}] 첫 번째 구독으로 메시지 수신 루프 시작")
                self.message_loop_task = asyncio.create_task(self.message_loop())
            
            return True
            
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] 구독 중 오류 발생: {str(e)}")
            # 에러 메트릭 업데이트
            self.metrics_manager.record_error(self.exchange_code)
            
            if on_error is not None:
                if isinstance(symbol, list):
                    for sym in symbol:
                        on_error(sym, str(e))
                else:
                    on_error(symbol, str(e))
            return False
    
    async def unsubscribe(self, symbol: str) -> bool:
        """
        구독 취소
        
        업비트는 구독 취소 메시지를 별도로 보내지 않고, 연결을 끊거나 새로운 구독 메시지를 보내는 방식으로 처리합니다.
        따라서 이 메서드는 내부적으로 구독 상태만 업데이트합니다.
        
        Args:
            symbol: 구독 취소할 심볼
            
        Returns:
            bool: 구독 취소 성공 여부
        """
        try:
            if symbol not in self.subscribed_symbols:
                self.logger.warning(f"[{self.exchange_code}] {symbol} 구독 중이 아닙니다.")
                return True
            
            # 상태 업데이트
            del self.subscribed_symbols[symbol]
            self.logger.info(f"[{self.exchange_code}] {symbol} 구독 취소 완료")
            
            # 모든 구독이 취소된 경우 상태 업데이트
            if not self.subscribed_symbols:
                self.status = SubscriptionStatus.IDLE
                
            return True
            
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] {symbol} 구독 취소 중 오류 발생: {e}")
            # 에러 메트릭 업데이트
            self.metrics_manager.record_error(self.exchange_code)
            return False