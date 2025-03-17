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
    DeltaMethod,
    SubscriptionStatus
)
from crosskimp.logger.logger import get_unified_logger, create_raw_logger
from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG

# ============================
# 바이빗 현물 구독 관련 상수
# ============================
# 거래소 코드
EXCHANGE_CODE = Exchange.BYBIT.value  # 거래소 코드
BYBIT_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이빗 설정

# 웹소켓 설정
REST_URL = BYBIT_CONFIG.get("api_urls", {}).get("depth", "")  # REST API URL
WS_URL = BYBIT_CONFIG["ws_url"]  # 웹소켓 URL
MAX_SYMBOLS_PER_SUBSCRIPTION = BYBIT_CONFIG["max_symbols_per_subscription"]  # 구독당 최대 심볼 수

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
        super().__init__(connection, parser)
        
        # 구독 관련 설정
        self.max_symbols_per_subscription = MAX_SYMBOLS_PER_SUBSCRIPTION
        self.subscribed_symbols = []
        
        # 메시지 처리 통계
        self.message_stats = {
            "total_received": 0,
            "snapshot": 0,
            "delta": 0,
            "errors": 0
        }
        
        # 로깅 설정
        self.logger = get_unified_logger()
        self.raw_logger = create_raw_logger(EXCHANGE_CODE)
        
        # 메시지 루프 태스크
        self.message_loop_task = None
        self.stop_event = asyncio.Event()
        
        # 콜백 함수
        self.on_snapshot_callbacks = {}  # symbol -> callback
        self.on_delta_callbacks = {}     # symbol -> callback
        self.on_error_callbacks = {}     # symbol -> callback
    
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
            args.append(f"orderbook.{BYBIT_CONFIG.get('default_depth', 50)}.{market}")
        
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
            "args": [f"orderbook.{BYBIT_CONFIG.get('default_depth', 50)}.{market}"]
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
    
    async def _run_message_loop(self):
        """
        메시지 수신 루프 실행
        
        웹소켓으로부터 메시지를 수신하고 처리합니다.
        """
        try:
            self.logger.info(f"바이빗 메시지 수신 루프 시작")
            
            while not self.stop_event.is_set():
                try:
                    # 메시지 수신
                    message = await self.connection.receive_raw()
                    
                    if message is None:
                        # 연결이 끊어진 경우
                        self.logger.warning(f"바이빗 웹소켓 연결이 끊어졌습니다. 메시지 루프를 종료합니다.")
                        break
                    
                    # 메시지 처리
                    await self._process_message(message)
                    
                except websockets.exceptions.ConnectionClosed as e:
                    self.logger.error(f"바이빗 웹소켓 연결이 닫혔습니다: {e}")
                    break
                    
                except asyncio.CancelledError:
                    self.logger.info(f"바이빗 메시지 루프가 취소되었습니다.")
                    break
                    
                except Exception as e:
                    self.logger.error(f"바이빗 메시지 처리 중 오류 발생: {e}")
                    self.message_stats["errors"] += 1
                    continue
                    
        except Exception as e:
            self.logger.error(f"바이빗 메시지 루프 실행 중 오류 발생: {e}")
            
        finally:
            self.logger.info(f"바이빗 메시지 수신 루프 종료")
            self.status = SubscriptionStatus.CLOSED
    
    async def _process_message(self, message):
        """
        메시지 처리
        
        Args:
            message: 수신된 메시지
        """
        try:
            # 메시지 통계 업데이트
            self.message_stats["total_received"] += 1
            
            # 바이트 메시지 디코딩
            raw_message = message
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            
            # raw 로거를 사용하여 원본 메시지 로깅
            try:
                # 타임스탬프 추가
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                self.raw_logger.debug(f"[{timestamp}] {message}")
                self.logger.debug(f"원본 메시지 raw_data 디렉토리에 로깅 완료")
            except Exception as e:
                self.logger.error(f"원본 메시지 로깅 실패: {str(e)}")
            
            # JSON 파싱
            try:
                data = json.loads(message)
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON 파싱 실패: {e}")
                self.message_stats["errors"] += 1
                return
            
            # 구독 응답 처리
            if data.get("op") == "subscribe":
                self.logger.info(f"구독 응답 수신: {data}")
                return
                
            # PONG 응답 처리
            if data.get("op") == "pong" or (data.get("ret_msg") == "pong" and data.get("op") == "ping"):
                self.logger.debug(f"PONG 응답 수신")
                return
            
            # 오더북 메시지 처리
            if "topic" in data and "data" in data:
                topic = data.get("topic", "")
                if "orderbook" in topic:
                    # 심볼 추출
                    parts = topic.split(".")
                    if len(parts) >= 3:
                        symbol = parts[-1].replace("USDT", "")
                        msg_type = data.get("type", "delta")
                        
                        # 메시지 타입에 따라 처리
                        if msg_type == "snapshot":
                            self.message_stats["snapshot"] += 1
                            self.logger.info(f"스냅샷 메시지 수신: {symbol}")
                            
                            # 스냅샷 콜백 호출
                            if symbol in self.on_snapshot_callbacks and self.on_snapshot_callbacks[symbol]:
                                await self.on_snapshot_callbacks[symbol](symbol, data)
                                
                        elif msg_type == "delta":
                            self.message_stats["delta"] += 1
                            
                            # 델타 콜백 호출
                            if symbol in self.on_delta_callbacks and self.on_delta_callbacks[symbol]:
                                await self.on_delta_callbacks[symbol](symbol, data)
                        
                        # 상태 업데이트
                        if symbol in self.subscribed_symbols:
                            if msg_type == "snapshot":
                                self.subscribed_symbols[symbol] = SubscriptionStatus.SNAPSHOT_RECEIVED
                            elif msg_type == "delta":
                                self.subscribed_symbols[symbol] = SubscriptionStatus.DELTA_RECEIVING
            
        except Exception as e:
            self.logger.error(f"메시지 처리 중 오류 발생: {e}")
            self.message_stats["errors"] += 1
    
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
            
            # 연결 확인
            if not self.connection.is_connected:
                self.logger.error(f"웹소켓이 연결되지 않았습니다.")
                return False
            
            # 상태 업데이트
            self.status = SubscriptionStatus.SUBSCRIBING
            
            # 콜백 함수 등록
            for sym in symbols:
                if on_snapshot:
                    self.on_snapshot_callbacks[sym] = on_snapshot
                if on_delta:
                    self.on_delta_callbacks[sym] = on_delta
                if on_error:
                    self.on_error_callbacks[sym] = on_error
                
                # 구독 상태 초기화
                self.subscribed_symbols[sym] = SubscriptionStatus.SUBSCRIBING
            
            # 메시지 루프가 실행 중이 아니면 시작
            if self.message_loop_task is None or self.message_loop_task.done():
                self.stop_event.clear()
                self.message_loop_task = asyncio.create_task(self._run_message_loop())
            
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
            self.status = SubscriptionStatus.SUBSCRIBED
            self.logger.info(f"전체 구독 요청 완료 | 총 {len(symbols)}개 심볼")
            
            return True
            
        except Exception as e:
            self.logger.error(f"구독 요청 실패: {e}")
            self.status = SubscriptionStatus.ERROR
            
            # 에러 콜백 호출
            if isinstance(symbol, str) and symbol in self.on_error_callbacks and self.on_error_callbacks[symbol]:
                await self.on_error_callbacks[symbol](symbol, str(e))
            
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
            # 연결 확인
            if not self.connection.is_connected:
                self.logger.error(f"웹소켓이 연결되지 않았습니다.")
                return False
            
            # 구독 취소 메시지 생성
            msg = await self.create_unsubscribe_message(symbol)
            
            # 구독 취소 메시지 전송
            await self.connection.ws.send(json.dumps(msg))
            self.logger.info(f"{symbol} 구독 취소 요청 전송")
            
            # 콜백 함수 제거
            self.on_snapshot_callbacks.pop(symbol, None)
            self.on_delta_callbacks.pop(symbol, None)
            self.on_error_callbacks.pop(symbol, None)
            
            # 구독 상태 제거
            self.subscribed_symbols.pop(symbol, None)
            
            return True
            
        except Exception as e:
            self.logger.error(f"{symbol} 구독 취소 실패: {e}")
            return False
    
    async def close(self) -> None:
        """
        모든 구독 취소 및 자원 정리
        """
        try:
            self.logger.info(f"바이빗 구독 종료 시작")
            
            # 메시지 루프 중지
            self.stop_event.set()
            
            if self.message_loop_task and not self.message_loop_task.done():
                self.message_loop_task.cancel()
                try:
                    await self.message_loop_task
                except asyncio.CancelledError:
                    pass
            
            # 모든 구독 취소
            symbols = list(self.subscribed_symbols.keys())
            for symbol in symbols:
                await self.unsubscribe(symbol)
            
            # 상태 업데이트
            self.status = SubscriptionStatus.CLOSED
            self.logger.info(f"바이빗 구독 종료 완료")
            
        except Exception as e:
            self.logger.error(f"바이빗 구독 종료 중 오류 발생: {e}") 