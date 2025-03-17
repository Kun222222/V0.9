import asyncio
import json
import aiohttp
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Optional, List, Any, Callable, Union, Tuple

# 필요한 로거 임포트 - 실제 구현에 맞게 수정 필요
from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
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
        
        # 메트릭 매니저 인스턴스 획득
        self.metrics_manager = WebsocketMetricsManager.get_instance()
        self.metrics_manager.initialize_exchange(self.exchange_code)
        
        # 구독 상태 관리
        self.subscribed_symbols = {}  # symbol: True/False
        
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
        
        # 원시 데이터 로깅 설정 - 기본적으로 비활성화
        self.log_raw_data = False
    
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
            # 원본 메시지 로깅 (활성화된 경우만)
            if self.log_raw_data:
                self.log_raw_message(message)
            
            # 메시지 파싱
            parsed_data = None
            if self.parser:
                parsed_data = self.parser.parse_message(message)
            
            if not parsed_data:
                return
                
            # 심볼 확인
            symbol = parsed_data.get("symbol")
            if not symbol or symbol not in self.subscribed_symbols:
                return
                
            # 메시지 타입에 따라 콜백 분기 처리
            if self.is_snapshot_message(message) and symbol in self.snapshot_callbacks:
                await self._call_snapshot_callback(symbol, parsed_data)
            elif self.is_delta_message(message) and symbol in self.delta_callbacks:
                await self._call_delta_callback(symbol, parsed_data)
                
        except Exception as e:
            # 에러 로깅
            self.logger.error(f"메시지 처리 오류: {str(e)}")
            
            # 에러 콜백 호출
            await self._call_error_callbacks(str(e))
    
    async def _call_snapshot_callback(self, symbol: str, data: Dict) -> None:
        """스냅샷 콜백 호출"""
        callback = self.snapshot_callbacks.get(symbol)
        if callback:
            await callback(symbol, data)
    
    async def _call_delta_callback(self, symbol: str, data: Dict) -> None:
        """델타 콜백 호출"""
        callback = self.delta_callbacks.get(symbol)
        if callback:
            await callback(symbol, data)
    
    async def _call_error_callbacks(self, error_msg: str) -> None:
        """모든 등록된 에러 콜백 호출"""
        for symbol, callback in self.error_callbacks.items():
            if callback:
                try:
                    await callback(symbol, error_msg)
                except Exception as cb_error:
                    self.logger.error(f"에러 콜백 호출 중 오류: {str(cb_error)}")
    
    @property
    def is_connected(self) -> bool:
        """연결 상태 확인 (메트릭 매니저 사용)"""
        return self.metrics_manager.is_connected(self.exchange_code)
    
    async def message_loop(self) -> None:
        """
        메시지 수신 루프 기본 구현
        
        웹소켓 연결에서 메시지를 지속적으로 수신하고 처리합니다.
        """
        self.logger.info(f"[{self.exchange_code}] 메시지 수신 루프 시작")
        
        while not self.stop_event.is_set():
            try:
                # 연결 상태 확인
                if not self.connection.is_connected:
                    self.logger.warning(f"[{self.exchange_code}] 웹소켓 연결 끊김, 재연결 대기 중")
                    await asyncio.sleep(1)  # 잠시 대기 후 재시도
                    continue
                
                # 원시 메시지 수신
                message = await self.connection.receive_raw()
                
                if not message:
                    await asyncio.sleep(0.01)  # 짧은 대기로 CPU 사용량 감소
                    continue
                
                # 메시지 처리
                await self._on_message(message)
                
            except asyncio.CancelledError:
                self.logger.info(f"[{self.exchange_code}] 메시지 수신 루프 취소됨")
                break
                
            except Exception as e:
                self.logger.error(f"[{self.exchange_code}] 메시지 수신 중 오류: {e}")
                await asyncio.sleep(0.1)  # 오류 발생시 약간 더 긴 대기
        
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
    
    def _cleanup_subscription(self, symbol: str) -> None:
        """
        구독 관련 콜백 및 상태 정리
        
        Args:
            symbol: 정리할 심볼
        """
        # 콜백 함수 제거
        self.snapshot_callbacks.pop(symbol, None)
        self.delta_callbacks.pop(symbol, None)
        self.error_callbacks.pop(symbol, None)
        
        # 구독 상태 제거
        self.subscribed_symbols.pop(symbol, None)
    
    async def close(self) -> None:
        """
        모든 구독 취소 및 자원 정리 기본 구현
        """
        try:
            # 종료 이벤트 설정
            self.stop_event.set()
            
            # 메시지 수신 루프 취소
            if self.message_loop_task and not self.message_loop_task.done():
                self.message_loop_task.cancel()
                try:
                    await self.message_loop_task
                except asyncio.CancelledError:
                    pass
            
            # 모든 구독 취소 (타임아웃 설정)
            symbols = list(self.subscribed_symbols.keys())
            for symbol in symbols:
                try:
                    # 각 구독 취소에 최대 1초의 타임아웃 설정
                    await asyncio.wait_for(self.unsubscribe(symbol), timeout=1.0)
                except asyncio.TimeoutError:
                    self.logger.warning(f"[{self.exchange_code}] {symbol} 구독 취소 타임아웃")
                    # 타임아웃 발생해도 콜백은 정리
                    self._cleanup_subscription(symbol)
                except Exception as e:
                    self.logger.error(f"[{self.exchange_code}] {symbol} 구독 취소 오류: {str(e)}")
                    # 오류 발생해도 콜백은 정리
                    self._cleanup_subscription(symbol)
            
            # 연결 종료
            if hasattr(self.connection, 'disconnect'):
                try:
                    # 연결 종료에도 타임아웃 설정
                    await asyncio.wait_for(self.connection.disconnect(), timeout=2.0)
                except asyncio.TimeoutError:
                    self.logger.warning(f"[{self.exchange_code}] 연결 종료 타임아웃")
                except Exception as e:
                    self.logger.error(f"[{self.exchange_code}] 연결 종료 오류: {str(e)}")
            
            # 태스크 취소
            for task in self.tasks.values():
                task.cancel()
            self.tasks.clear()
            
            # REST 세션 종료
            if self.rest_session:
                await self.rest_session.close()
                self.rest_session = None
            
            self.metrics_manager.update_connection_state(self.exchange_code, "disconnected")
            self.logger.info(f"[{self.exchange_code}] 모든 구독 취소 및 자원 정리 완료")
            
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] 자원 정리 중 오류 발생: {e}")
            self.metrics_manager.record_error(self.exchange_code)

