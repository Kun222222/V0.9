import asyncio
import json
import aiohttp
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Optional, List, Any, Callable, Union, Tuple

# 필요한 로거 임포트 - 실제 구현에 맞게 수정 필요
from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.connection.base_ws_connector import BaseWebsocketConnector
# 메트릭 매니저 임포트 추가
from crosskimp.ob_collector.orderbook.metric.metrics_manager import WebsocketMetricsManager


class SnapshotMethod(Enum):
    """스냅샷 수신 방법"""
    WEBSOCKET = "websocket"  # 웹소켓을 통해 스냅샷 수신 (업비트, 바이빗)
    REST = "rest"            # REST API를 통해 스냅샷 수신 (빗섬, 바이낸스)
    NONE = "none"            # 스냅샷 필요 없음


class DeltaMethod(Enum):
    """델타 수신 방법"""
    WEBSOCKET = "websocket"  # 웹소켓을 통해 델타 수신
    NONE = "none"            # 델타 수신 안함 (업비트처럼 항상 전체 스냅샷)


class SubscriptionStatus(Enum):
    """구독 상태"""
    IDLE = "idle"                # 초기 상태
    CONNECTING = "connecting"    # 연결 중
    SUBSCRIBING = "subscribing"  # 구독 중
    SUBSCRIBED = "subscribed"    # 구독 완료
    SNAPSHOT_RECEIVED = "snapshot_received"  # 스냅샷 수신 완료
    DELTA_RECEIVING = "delta_receiving"      # 델타 수신 중
    ERROR = "error"              # 에러 상태
    CLOSED = "closed"            # 종료됨


class BaseSubscription(ABC):
    """
    오더북 구독 기본 클래스
    
    각 거래소별 구독 패턴을 처리할 수 있는 추상 클래스입니다.
    - 업비트: 웹소켓을 통해 스냅샷 형태로 계속 수신
    - 빗섬: REST 스냅샷 > 웹소켓 델타
    - 바이빗(현물, 선물): 웹소켓 스냅샷 > 웹소켓 델타
    - 바이낸스(현물, 선물): REST 스냅샷 > 웹소켓 델타
    
    책임:
    - 구독 관리 (구독, 구독 취소)
    - 메시지 처리 및 파싱
    - 콜백 호출
    - 원시 데이터 로깅
    - 메트릭 관리
    """
    
    def __init__(self, connection: BaseWebsocketConnector, exchange_code: str, parser=None):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
            exchange_code: 거래소 코드
            parser: 메시지 파싱 객체 (선택적)
        """
        self.connection = connection
        self.parser = parser
        self.logger = get_unified_logger()
        self.exchange_code = exchange_code
        
        # 구독 상태 관리
        self.status = SubscriptionStatus.IDLE
        self.subscribed_symbols = {}  # symbol: status
        
        # 스냅샷과 델타 메소드 (거래소별로 달라짐)
        self.snapshot_method = self._get_snapshot_method()
        self.delta_method = self._get_delta_method()
        
        # 콜백 함수 저장
        self.snapshot_callbacks = {}  # symbol: callback
        self.delta_callbacks = {}     # symbol: callback
        self.error_callbacks = {}     # symbol: callback
        
        # REST API 세션 (필요한 경우)
        self.rest_session = None
        if self.snapshot_method == SnapshotMethod.REST:
            self._init_rest_session()
        
        # 비동기 태스크 관리
        self.tasks = {}
        
        # 메시지 수신 루프 태스크
        self.message_loop_task = None
        
        # 종료 이벤트
        self.stop_event = asyncio.Event()
        
        # 메트릭 매니저 초기화
        self.metrics_manager = WebsocketMetricsManager.get_instance()
        self.metrics_manager.initialize_exchange(self.exchange_code)
    
    def _init_rest_session(self):
        """REST API 세션 초기화 (REST 스냅샷 방식인 경우)"""
        self.rest_session = aiohttp.ClientSession()
        self.logger.info(f"[{self.exchange_code}] REST API 세션 초기화 완료")
    
    @abstractmethod
    def _get_snapshot_method(self) -> SnapshotMethod:
        """스냅샷 수신 방법 반환"""
        pass
    
    @abstractmethod
    def _get_delta_method(self) -> DeltaMethod:
        """델타 수신 방법 반환"""
        pass
    
    @abstractmethod
    async def create_subscribe_message(self, symbol: Union[str, List[str]]) -> Dict:
        """
        구독 메시지 생성
        
        Args:
            symbol: 심볼 또는 심볼 리스트
            
        Returns:
            Dict: 구독 메시지
        """
        pass
    
    async def create_unsubscribe_message(self, symbol: str) -> Dict:
        """
        구독 취소 메시지 생성
        
        기본 구현은 빈 딕셔너리를 반환합니다.
        대부분의 거래소는 구독 취소 메시지가 없거나 단순하므로 필요한 경우만 오버라이드합니다.
        
        Args:
            symbol: 심볼
            
        Returns:
            Dict: 구독 취소 메시지
        """
        # 기본 구현 - 빈 딕셔너리 반환
        self.logger.debug(f"[{self.exchange_code}] 구독 취소 메시지 생성 (기본 구현)")
        return {}
    
    @abstractmethod
    def is_snapshot_message(self, message: str) -> bool:
        """
        메시지가 스냅샷인지 확인
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 스냅샷 메시지인 경우 True
        """
        pass
    
    def is_delta_message(self, message: str) -> bool:
        """
        메시지가 델타인지 확인
        
        기본 구현은 False를 반환합니다.
        델타를 사용하지 않는 거래소는 이 메서드를 오버라이드할 필요가 없습니다.
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 델타 메시지인 경우 True
        """
        # 기본 구현 - 델타 메시지 없음
        return False
    
    async def get_rest_snapshot(self, symbol: str) -> Dict:
        """
        REST API를 통해 스냅샷 요청 (REST 방식인 경우)
        
        기본 구현은 빈 딕셔너리를 반환합니다.
        REST 스냅샷을 사용하지 않는 거래소는 이 메서드를 오버라이드할 필요가 없습니다.
        
        Args:
            symbol: 심볼
            
        Returns:
            Dict: 스냅샷 데이터
        """
        # 기본 구현 - REST 스냅샷 지원 안함
        self.logger.warning(f"[{self.exchange_code}] REST API 스냅샷을 지원하지 않음: {symbol}")
        return {}
    
    @abstractmethod
    def log_raw_message(self, message: str) -> None:
        """
        원시 메시지 로깅
        
        Args:
            message: 원시 메시지
        """
        pass
    
    async def _on_message(self, message: str) -> None:
        """
        메시지 수신 콜백 기본 구현
        
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
            
            # 스냅샷 메시지 확인
            if self.is_snapshot_message(message):
                # 메트릭 업데이트
                self.metrics_manager.record_orderbook(self.exchange_code)
                
                # 스냅샷 메시지 파싱 및 처리
                parsed_data = None
                if self.parser:
                    parsed_data = self.parser.parse_message(message)
                
                if parsed_data:
                    symbol = parsed_data.get("symbol")
                    if symbol and symbol in self.subscribed_symbols and symbol in self.snapshot_callbacks:
                        # 스냅샷 콜백 호출
                        callback = self.snapshot_callbacks[symbol]
                        if callback:
                            await callback(symbol, parsed_data)
            
            # 델타 메시지 확인
            elif self.is_delta_message(message):
                # 메트릭 업데이트
                self.metrics_manager.record_orderbook(self.exchange_code)
                
                # 델타 메시지 파싱 및 처리
                parsed_data = None
                if self.parser:
                    parsed_data = self.parser.parse_message(message)
                
                if parsed_data:
                    symbol = parsed_data.get("symbol")
                    if symbol and symbol in self.subscribed_symbols and symbol in self.delta_callbacks:
                        # 델타 콜백 호출
                        callback = self.delta_callbacks[symbol]
                        if callback:
                            await callback(symbol, parsed_data)
            
        except Exception as e:
            # 에러 메트릭 업데이트
            self.metrics_manager.record_error(self.exchange_code)
            self.logger.error(f"메시지 처리 중 오류 발생: {str(e)}")
            
            # 에러 콜백 호출
            for symbol, callback in self.error_callbacks.items():
                if callback:
                    try:
                        await callback(symbol, str(e))
                    except Exception as cb_error:
                        self.logger.error(f"에러 콜백 호출 중 오류: {str(cb_error)}")
    
    async def message_loop(self) -> None:
        """
        메시지 수신 루프 기본 구현
        
        웹소켓 연결에서 메시지를 지속적으로 수신하고 처리합니다.
        """
        self.logger.info(f"[{self.exchange_code}] 메시지 수신 루프 시작")
        self.metrics_manager.update_connection_state(self.exchange_code, "connected")
        
        while not self.stop_event.is_set():
            try:
                # 연결 상태 확인
                if not hasattr(self.connection, 'is_connected') or not self.connection.is_connected:
                    self.logger.error(f"[{self.exchange_code}] 웹소켓 연결이 끊어짐")
                    self.metrics_manager.update_connection_state(self.exchange_code, "disconnected")
                    await asyncio.sleep(1)  # 잠시 대기 후 재시도
                    continue
                
                # 원시 메시지 수신
                message = await self.connection.receive_raw()
                
                if not message:
                    await asyncio.sleep(0.01)  # 짧은 대기로 CPU 사용량 감소
                    continue
                
                # 메시지 처리 시작 시간
                start_time = asyncio.get_event_loop().time()
                
                # 메시지 처리
                await self._on_message(message)
                
                # 메시지 처리 시간 계산 및 기록 (밀리초 단위)
                processing_time = (asyncio.get_event_loop().time() - start_time) * 1000
                self.metrics_manager.record_processing_time(self.exchange_code, processing_time)
                
            except asyncio.CancelledError:
                self.logger.info(f"[{self.exchange_code}] 메시지 수신 루프 취소됨")
                break
                
            except Exception as e:
                self.logger.error(f"[{self.exchange_code}] 메시지 수신 중 오류: {e}")
                self.metrics_manager.record_error(self.exchange_code)
                await asyncio.sleep(0.1)  # 오류 발생시 약간 더 긴 대기
        
        self.metrics_manager.update_connection_state(self.exchange_code, "disconnected")
        self.logger.info(f"[{self.exchange_code}] 메시지 수신 루프 종료")
    
    @abstractmethod
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
        pass
    
    @abstractmethod
    async def unsubscribe(self, symbol: str) -> bool:
        """
        구독 취소
        
        Args:
            symbol: 구독 취소할 심볼
            
        Returns:
            bool: 구독 취소 성공 여부
        """
        pass
    
    async def close(self) -> None:
        """
        모든 구독 취소 및 자원 정리 기본 구현
        """
        try:
            # 종료 이벤트 설정
            self.stop_event.set()
            
            # 모든 구독 취소
            symbols = list(self.subscribed_symbols.keys())
            for symbol in symbols:
                await self.unsubscribe(symbol)
            
            # 메시지 수신 루프 취소
            if self.message_loop_task and not self.message_loop_task.done():
                self.message_loop_task.cancel()
                try:
                    await self.message_loop_task
                except asyncio.CancelledError:
                    pass
            
            # 연결 종료
            if hasattr(self.connection, 'disconnect'):
                await self.connection.disconnect()
            
            # 태스크 취소
            for task in self.tasks.values():
                task.cancel()
            self.tasks.clear()
            
            # REST 세션 종료
            if self.rest_session:
                await self.rest_session.close()
                self.rest_session = None
            
            self.status = SubscriptionStatus.CLOSED
            self.metrics_manager.update_connection_state(self.exchange_code, "disconnected")
            self.logger.info(f"[{self.exchange_code}] 모든 구독 취소 및 자원 정리 완료")
            
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] 자원 정리 중 오류 발생: {e}")
            self.metrics_manager.record_error(self.exchange_code)
            self.status = SubscriptionStatus.ERROR

