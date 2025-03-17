"""
바이빗 현물 구독 클래스

이 모듈은 바이빗 현물 거래소의 웹소켓 구독을 담당하는 클래스를 제공합니다.
"""

import json
import time
import asyncio
import os
import datetime
from typing import Dict, List, Any, Optional, Callable, Union
import websockets.exceptions

from crosskimp.ob_collector.orderbook.subscription.base_subscription import (
    BaseSubscription, 
    SnapshotMethod, 
    DeltaMethod
)
from crosskimp.logger.logger import get_unified_logger, create_raw_logger
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator

# ============================
# 바이빗 현물 구독 관련 상수
# ============================
# 거래소 코드
EXCHANGE_CODE = "bybit"  # 거래소 코드 (소문자로 통일)

# 웹소켓 설정
REST_URL = "https://api.bybit.com/v5/market/orderbook"  # REST API URL
WS_URL = "wss://stream.bybit.com/v5/public/spot"  # 웹소켓 URL
MAX_SYMBOLS_PER_SUBSCRIPTION = 10  # 구독당 최대 심볼 수
DEFAULT_DEPTH = 50  # 기본 오더북 깊이

class BybitSubscription(BaseSubscription):
    """
    바이빗 현물 구독 클래스
    
    바이빗 현물 거래소의 웹소켓 구독을 담당하는 클래스입니다.
    
    특징:
    - 웹소켓을 통한 스냅샷 수신 후 델타 업데이트 처리
    - 시퀀스 번호 기반 데이터 정합성 검증
    """
    
    def __init__(self, connection, parser=None):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
            parser: 메시지 파싱 객체 (선택적)
        """
        super().__init__(connection, EXCHANGE_CODE, parser)
        
        # 구독 관련 설정
        self.max_symbols_per_subscription = MAX_SYMBOLS_PER_SUBSCRIPTION
        
        # 로깅 설정
        self.logger = get_unified_logger()
        self.raw_logger = create_raw_logger(EXCHANGE_CODE)
        
        # 원시 데이터 로깅 비활성화 (필요시에만 활성화)
        self.log_raw_data = False
        
        # 바이빗 오더북 검증기 초기화
        self.validator = BaseOrderBookValidator(EXCHANGE_CODE)
    
    def _get_snapshot_method(self) -> SnapshotMethod:
        """
        스냅샷 수신 방법 반환
        
        바이빗은 웹소켓을 통해 스냅샷을 수신합니다.
        
        Returns:
            SnapshotMethod: 스냅샷 수신 방법
        """
        return SnapshotMethod.WEBSOCKET
    
    def _get_delta_method(self) -> DeltaMethod:
        """
        델타 수신 방법 반환
        
        바이빗은 웹소켓을 통해 델타를 수신합니다.
        
        Returns:
            DeltaMethod: 델타 수신 방법
        """
        return DeltaMethod.WEBSOCKET
    
    async def create_subscribe_message(self, symbol: Union[str, List[str]]) -> Dict:
        """
        구독 메시지 생성
        
        Args:
            symbol: 심볼 또는 심볼 리스트
            
        Returns:
            Dict: 구독 메시지
        """
        # 단일 심볼을 리스트로 변환
        if isinstance(symbol, str):
            symbols = [symbol]
        else:
            symbols = symbol
            
        # 심볼 형식 변환 ({symbol}USDT)
        args = []
        for sym in symbols:
            market = f"{sym}USDT"
            args.append(f"orderbook.{DEFAULT_DEPTH}.{market}")
        
        # 구독 메시지 생성
        return {
            "op": "subscribe",
            "args": args
        }
    
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
            "args": [f"orderbook.{DEFAULT_DEPTH}.{market}"]
        }
    
    def is_snapshot_message(self, message: str) -> bool:
        """
        메시지가 스냅샷인지 확인
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 스냅샷 메시지인 경우 True
        """
        try:
            data = json.loads(message)
            return data.get("type") == "snapshot"
        except (json.JSONDecodeError, KeyError):
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
            data = json.loads(message)
            return data.get("type") == "delta"
        except (json.JSONDecodeError, KeyError):
            return False
    
    def log_raw_message(self, message: str) -> None:
        """
        원시 메시지 로깅
        
        Args:
            message: 원시 메시지
        """
        # log_raw_data가 True인 경우에만 로깅
        if self.log_raw_data and self.raw_logger:
            try:
                # 바이트 메시지 디코딩
                if isinstance(message, bytes):
                    message = message.decode('utf-8')
                
                # JSON 형태로 파싱하여 필요한 정보만 로깅
                data = json.loads(message)
                topic = data.get('topic', 'unknown')
                
                # 타임스탬프 추가하여 간소화된 형태로 로깅
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                self.raw_logger.debug(f"[{timestamp}] Raw message received for topic: {topic}")
            except Exception as e:
                self.logger.error(f"원본 메시지 로깅 실패: {str(e)}")
        # 로깅 비활성화 상태면 아무것도 하지 않음
    
    async def get_rest_snapshot(self, symbol: str) -> Dict:
        """
        REST API를 통해 스냅샷 요청 (REST 방식인 경우)
        
        바이빗은 웹소켓을 통해 스냅샷을 수신하므로 이 메서드는 사용되지 않습니다.
        
        Args:
            symbol: 심볼
            
        Returns:
            Dict: 스냅샷 데이터
        """
        # 바이빗은 웹소켓을 통해 스냅샷을 수신하므로 이 메서드는 사용되지 않습니다.
        self.logger.warning(f"바이빗은 REST API 스냅샷을 지원하지 않습니다. 웹소켓을 통해 스냅샷을 수신하세요.")
        return {}
    
    async def _on_message(self, message: str) -> None:
        """
        메시지 수신 처리
        
        Args:
            message: 수신된 원시 메시지
        """
        try:
            # 메시지 파싱
            parsed_data = None
            if self.parser:
                parsed_data = self.parser.parse_message(message)
                
            if not parsed_data:
                return
            
            # 파싱된 데이터에서 필요한 정보 추출
            symbol = parsed_data.get("symbol")
            if not symbol or symbol not in self.subscribed_symbols:
                return
            
            # 스냅샷/델타 구분
            is_snapshot = self.is_snapshot_message(message)
            is_delta = self.is_delta_message(message)
            
            # 검증 및 업데이트
            validation_result = None
            
            if is_snapshot:
                # 스냅샷 검증 및 초기화
                validation_result = await self.validator.initialize_orderbook(
                    symbol=symbol,
                    data=parsed_data
                )
                
                # 스냅샷 콜백 호출
                if validation_result.is_valid and symbol in self.snapshot_callbacks:
                    # 검증된 데이터로 콜백 호출
                    validated_data = {
                        "bids": validation_result.limited_bids,
                        "asks": validation_result.limited_asks,
                        "timestamp": parsed_data.get("timestamp"),
                        "sequence": parsed_data.get("sequence")
                    }
                    await self.snapshot_callbacks[symbol](symbol, validated_data)
            
            elif is_delta:
                # 델타 검증 및 업데이트
                validation_result = await self.validator.update(
                    symbol=symbol,
                    data=parsed_data
                )
                
                # 델타 콜백 호출
                if validation_result.is_valid and symbol in self.delta_callbacks:
                    # 검증된 데이터로 콜백 호출
                    validated_data = {
                        "bids": validation_result.limited_bids,
                        "asks": validation_result.limited_asks,
                        "timestamp": parsed_data.get("timestamp"),
                        "sequence": parsed_data.get("sequence")
                    }
                    await self.delta_callbacks[symbol](symbol, validated_data)
            
            # 검증 결과 로깅
            if validation_result and not validation_result.is_valid:
                self.logger.warning(f"[{self.exchange_code}] {symbol} 검증 실패: {validation_result.errors}")
            
            # 검증된 오더북 데이터 로깅 (상위 10개만)
            if self.raw_logger and validation_result and validation_result.is_valid:
                # 현재 시간 기록
                current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                
                # 검증기에서 관리하는 전체 오더북 가져오기
                full_orderbook = self.validator.get_orderbook(symbol)
                
                if full_orderbook:
                    # 매수/매도 주문 개수 계산
                    bid_count = len(full_orderbook["bids"])
                    ask_count = len(full_orderbook["asks"])
                    
                    # 상위 10개만 제한하여 로깅
                    limited_bids = full_orderbook["bids"][:10] if full_orderbook["bids"] else []
                    limited_asks = full_orderbook["asks"][:10] if full_orderbook["asks"] else []
                    
                    log_data = {
                        "exchangename": self.exchange_code,  # 소문자로 통일된 exchange_code 사용
                        "symbol": symbol,
                        "bids": limited_bids,
                        "asks": limited_asks,
                        "timestamp": parsed_data.get("timestamp"),
                        "sequence": parsed_data.get("sequence")
                    }
                    self.raw_logger.debug(f"[{current_time}] 매수 ({bid_count}) / 매도 ({ask_count}) {json.dumps(log_data)}")
                    
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] 메시지 처리 중 오류 발생: {str(e)}")
    
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
            # 단일 심볼을 리스트로 변환
            if isinstance(symbol, str):
                symbols = [symbol]
            else:
                symbols = symbol
            
            # 연결 상태 확인
            if not self.connection or not self.connection.is_connected:
                self.logger.error(f"웹소켓이 연결되지 않았습니다. 연결을 먼저 시도해주세요.")
                return False
                
            if not self.connection.ws:
                self.logger.error(f"웹소켓 객체가 없습니다. 연결을 다시 시도해주세요.")
                return False
            
            # 콜백 함수 등록
            for sym in symbols:
                if on_snapshot:
                    self.snapshot_callbacks[sym] = on_snapshot
                if on_delta:
                    self.delta_callbacks[sym] = on_delta
                if on_error:
                    self.error_callbacks[sym] = on_error
                
                # 구독된 심볼에 추가
                self.subscribed_symbols[sym] = True
            
            # 메시지 루프가 실행 중이 아니면 시작
            if self.message_loop_task is None or self.message_loop_task.done():
                self.stop_event.clear()
                self.message_loop_task = asyncio.create_task(self.message_loop())
            
            # 구독 메시지 생성 및 전송
            total_batches = (len(symbols) + self.max_symbols_per_subscription - 1) // self.max_symbols_per_subscription
            self.logger.info(f"구독 시작 | 총 {len(symbols)}개 심볼, {total_batches}개 배치로 나눔")
            
            for i in range(0, len(symbols), self.max_symbols_per_subscription):
                batch_symbols = symbols[i:i + self.max_symbols_per_subscription]
                batch_num = (i // self.max_symbols_per_subscription) + 1
                
                # 구독 메시지 생성
                msg = await self.create_subscribe_message(batch_symbols)
                
                # 구독 메시지 전송
                await self.connection.ws.send(json.dumps(msg))
                self.logger.info(f"구독 요청 전송 | 배치 {batch_num}/{total_batches}, symbols={batch_symbols}")
                await asyncio.sleep(0.1)
            
            # 상태 업데이트
            self.logger.info(f"전체 구독 요청 완료 | 총 {len(symbols)}개 심볼")
            
            return True
            
        except Exception as e:
            self.logger.error(f"구독 요청 실패: {e}")
            
            # 에러 콜백 호출
            if isinstance(symbol, str) and symbol in self.error_callbacks and self.error_callbacks[symbol]:
                await self.error_callbacks[symbol](symbol, str(e))
            
            return False
    
    async def unsubscribe(self, symbol: str) -> bool:
        """
        구독 취소
        
        Args:
            symbol: 구독 취소할 심볼
            
        Returns:
            bool: 구독 취소 성공 여부
        """
        try:
            # 연결 확인 - 이미 연결이 끊어진 경우에는 메시지 전송 없이 바로 정리
            if not self.connection or not self.connection.is_connected or not self.connection.ws:
                self.logger.info(f"[{self.exchange_code}] {symbol} 연결 없음, 구독 상태 정리만 진행")
                # 콜백 정리 및 상태 제거는 부모 클래스의 _cleanup_subscription 메서드 호출로 대체
                self._cleanup_subscription(symbol)
                return True
            
            # 구독 취소 메시지 생성
            msg = await self.create_unsubscribe_message(symbol)
            
            # 구독 취소 메시지 전송
            await self.connection.ws.send(json.dumps(msg))
            self.logger.info(f"[{self.exchange_code}] {symbol} 구독 취소 요청 전송")
            
            # 콜백 정리 및 상태 제거는 부모 클래스의 _cleanup_subscription 메서드 호출로 대체
            self._cleanup_subscription(symbol)
            
            return True
            
        except websockets.exceptions.ConnectionClosed:
            # 연결이 이미 닫혀있는 경우 - 성공으로 처리하고 정리만 진행
            self.logger.info(f"[{self.exchange_code}] {symbol} 연결이 닫혀있음, 구독 상태 정리만 진행")
            # 콜백 정리 및 상태 제거는 부모 클래스의 _cleanup_subscription 메서드 호출로 대체
            self._cleanup_subscription(symbol)
            return True
            
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] {symbol} 구독 취소 실패: {e}")
            return False
    
    async def close(self) -> None:
        """
        모든 구독 취소 및 자원 정리
        """
        try:
            self.logger.info(f"바이빗 구독 종료 시작")
            
            # 기본 클래스의 close 메서드 호출
            await super().close()
            
            # 추가 정리 작업이 필요한 경우 여기에 구현
            
            self.logger.info(f"바이빗 구독 종료 완료")
            
        except Exception as e:
            self.logger.error(f"바이빗 구독 종료 중 오류 발생: {e}") 