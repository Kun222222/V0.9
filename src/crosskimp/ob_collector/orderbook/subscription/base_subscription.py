import asyncio
import json
import aiohttp
import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, Optional, List, Any, Callable, Union, Tuple, Set
import datetime
import websockets

# 필요한 로거 임포트 - 실제 구현에 맞게 수정 필요
from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
# 메트릭 매니저 임포트 추가
from crosskimp.ob_collector.orderbook.metric.metrics_manager import WebsocketMetricsManager
from crosskimp.config.paths import LOG_SUBDIRS

# 한글 거래소 이름 매핑
EXCHANGE_NAMES_KR = {
    "UPBIT": "[업비트]",
    "BYBIT": "[바이빗]",
    "BINANCE": "[바이낸스]",
    "BITHUMB": "[빗썸]",
    "BINANCE_FUTURE": "[바이낸스 선물]",
    "BYBIT_FUTURE": "[바이빗 선물]",
}


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
    
    def __init__(self, connection: BaseWebsocketConnector, exchange_code: str):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
            exchange_code: 거래소 코드
        """
        self.connection = connection
        self.logger = get_unified_logger()
        self.exchange_code = exchange_code
        
        # 메트릭 매니저 싱글톤 인스턴스 사용
        self.metrics_manager = WebsocketMetricsManager.get_instance()
        self.metrics_manager.initialize_exchange(self.exchange_code)
        
        # 구독 상태 관리
        self.subscribed_symbols = {}  # symbol: True/False
        
        # 콜백 함수 저장
        self.snapshot_callbacks = {}  # symbol: callback
        self.delta_callbacks = {}     # symbol: callback
        self.error_callbacks = {}     # symbol: callback
        
        # 비동기 태스크 관리
        self.tasks = {}
        
        # 메시지 수신 루프 태스크
        self.message_loop_task = None
        
        # 종료 이벤트
        self.stop_event = asyncio.Event()
        
        # 원시 데이터 로깅 설정 - 기본적으로 비활성화
        self.log_raw_data = False
        
        # 검증기 및 출력 큐
        self.validator = None
        self.output_queue = None
    
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
            
            # 메시지 파싱 - 각 서브클래스에서 구현해야 함
            parsed_data = None
            
            # 메시지 타입 확인
            if self.is_snapshot_message(message):
                # 스냅샷 메시지 처리
                symbol = self._extract_symbol_from_message(message)
                if symbol and symbol in self.subscribed_symbols:
                    parsed_data = self._parse_snapshot_message(message)
                    if parsed_data:
                        await self._handle_snapshot(symbol, parsed_data)
            
            elif self.is_delta_message(message):
                # 델타 메시지 처리
                symbol = self._extract_symbol_from_message(message)
                if symbol and symbol in self.subscribed_symbols:
                    parsed_data = self._parse_delta_message(message)
                    if parsed_data:
                        await self._handle_delta(symbol, parsed_data)
                
        except Exception as e:
            # 에러 로깅
            self.logger.error(f"메시지 처리 오류: {str(e)}")
            
            # 에러 처리
            symbol = self._extract_symbol_from_message(message)
            if symbol:
                self._handle_error(symbol, str(e))
            else:
                self._handle_error("unknown", str(e))
    
    def _extract_symbol_from_message(self, message: str) -> Optional[str]:
        """
        메시지에서 심볼 추출 (기본 구현)
        
        각 서브클래스에서 거래소별 형식에 맞게 오버라이드해야 함
        
        Args:
            message: 수신된 메시지
            
        Returns:
            Optional[str]: 추출된 심볼 또는 None
        """
        # 기본 구현은 None 반환 - 각 거래소별로 구현 필요
        return None
        
    def _parse_snapshot_message(self, message: str) -> Optional[Dict]:
        """
        스냅샷 메시지 파싱 (기본 구현)
        
        각 서브클래스에서 거래소별 형식에 맞게 오버라이드해야 함
        
        Args:
            message: 수신된 스냅샷 메시지
            
        Returns:
            Optional[Dict]: 파싱된 스냅샷 데이터 또는 None
        """
        # 기본 구현은 None 반환 - 각 거래소별로 구현 필요
        return None
        
    def _parse_delta_message(self, message: str) -> Optional[Dict]:
        """
        델타 메시지 파싱 (기본 구현)
        
        각 서브클래스에서 거래소별 형식에 맞게 오버라이드해야 함
        
        Args:
            message: 수신된 델타 메시지
            
        Returns:
            Optional[Dict]: 파싱된 델타 데이터 또는 None
        """
        # 기본 구현은 None 반환 - 각 거래소별로 구현 필요
        return None
    
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
        """연결 상태 확인 (연결 객체에서 직접 참조)"""
        # 중복 상태 관리를 제거하고 연결 객체의 상태만 참조
        return self.connection.is_connected if self.connection else False
    
    async def message_loop(self) -> None:
        """
        메시지 수신 루프
        
        웹소켓에서 메시지를 계속 수신하고 처리하는 루프입니다.
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        self.logger.info("메시지 수신 루프 시작")
        
        while not self.stop_event.is_set():
            try:
                # 연결 상태 확인
                if not self.is_connected:
                    self.logger.warning("웹소켓 연결이 끊어짐, 재연결 대기")
                    await asyncio.sleep(1)
                    continue
                
                # 메시지 수신
                message = await self.connection.receive_raw()
                
                if not message:
                    await asyncio.sleep(0.01)  # 짧은 대기 시간 추가 (CPU 사용량 감소)
                    continue
                
                # 메시지 처리
                await self._on_message(message)
                
            except asyncio.CancelledError:
                self.logger.info("메시지 수신 루프 취소됨")
                break
                
            except websockets.exceptions.ConnectionClosed as e:
                self.logger.warning(f"웹소켓 연결 끊김: {e}")
                # 연결 상태 업데이트
                self.metrics_manager.update_connection_state(self.exchange_code, "disconnected")
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"메시지 수신 루프 오류: {str(e)}")
                self.metrics_manager.record_error(self.exchange_code)
                await asyncio.sleep(0.1)
                
        self.logger.info("메시지 수신 루프 종료")
    
    @abstractmethod
    async def subscribe(self, symbol, on_snapshot=None, on_delta=None, on_error=None):
        """
        심볼 구독
        
        단일 심볼 또는 심볼 리스트를 구독합니다.
        각 거래소별 구현에서는 자체적인 배치 처리 로직을 포함해야 합니다.
        
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
    async def unsubscribe(self, symbol: Optional[str] = None) -> bool:
        """
        구독 취소
        
        Args:
            symbol: 구독 취소할 심볼. None인 경우 모든 심볼 구독 취소 및 자원 정리
            
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
    
    def set_validator(self, validator) -> None:
        """
        검증기 설정
        
        Args:
            validator: 검증기 객체
        """
        self.validator = validator
        
    def set_output_queue(self, queue) -> None:
        """
        출력 큐 설정
        
        Args:
            queue: 출력 큐
        """
        self.output_queue = queue
    
    async def _handle_snapshot(self, symbol: str, data: Dict) -> None:
        """
        스냅샷 처리
        
        Args:
            symbol: 심볼
            data: 스냅샷 데이터
        """
        try:
            # 처리 시작 시간 기록
            start_time = time.time()
            
            # 검증기가 없으면 처리 불가
            if not self.validator:
                self.logger.error(f"[{self.exchange_code}] 검증기가 설정되지 않았습니다")
                return
                
            # 데이터 검증
            result = await self.validator.initialize_orderbook(symbol, data)
            is_valid = result.is_valid
            orderbook = self.validator.get_orderbook(symbol) if is_valid else None
            
            # 출력 큐에 전송
            self._send_to_output_queue(symbol, orderbook, is_valid)
            
            # 메트릭 및 통계 업데이트
            self._update_metrics(self.exchange_code, start_time, "snapshot", data)
            
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] {symbol} 스냅샷 처리 실패: {str(e)}")
            self.metrics_manager.record_error(self.exchange_code)
            
    async def _handle_delta(self, symbol: str, data: Dict) -> None:
        """
        델타 처리
        
        Args:
            symbol: 심볼
            data: 델타 데이터
        """
        try:
            # 처리 시작 시간 기록
            start_time = time.time()
            
            # 검증기가 없으면 처리 불가
            if not self.validator:
                self.logger.error(f"[{self.exchange_code}] 검증기가 설정되지 않았습니다")
                return
                
            # 오더북 업데이트 (세부 구현은 validator에 위임)
            result = await self.validator.update(symbol, data)
            
            # 검증 결과 처리
            is_valid = result.is_valid
            orderbook = self.validator.get_orderbook(symbol) if is_valid else None
            
            # 출력 큐에 전송
            self._send_to_output_queue(symbol, orderbook, is_valid)
            
            # 메트릭 및 통계 업데이트
            self._update_metrics(self.exchange_code, start_time, "delta", data)
            
        except Exception as e:
            self.logger.error(f"[{self.exchange_code}] {symbol} 델타 처리 실패: {str(e)}")
            self.metrics_manager.record_error(self.exchange_code)
            
    def _handle_error(self, symbol: str, error: str) -> None:
        """
        에러 콜백
        
        Args:
            symbol: 심볼
            error: 오류 메시지
        """
        self.logger.error(f"[{self.exchange_code}] {symbol} 오류 발생: {error}")
        self.metrics_manager.record_error(self.exchange_code)

    def _update_metrics(self, exchange_code: str, start_time: float, message_type: str, data: Dict) -> None:
        """
        메트릭 및 통계 업데이트
        
        Args:
            exchange_code: 거래소 코드
            start_time: 처리 시작 시간
            message_type: 메시지 타입 ("snapshot" 또는 "delta")
            data: 메시지 데이터
        """
        try:
            # 처리 시간 계산 (밀리초)
            processing_time_ms = (time.time() - start_time) * 1000
            
            # 추정 데이터 크기 계산 (바이트)
            data_size = self._estimate_data_size(data)
            
            # 메트릭 매니저에 모든 지표 업데이트 위임
            self.metrics_manager.update_metric(exchange_code, "processing_time", time_ms=processing_time_ms)
            self.metrics_manager.update_metric(exchange_code, "bytes", size=data_size)
            self.metrics_manager.update_metric(exchange_code, message_type)
            self.metrics_manager.update_metric(exchange_code, "connected")
        except Exception as e:
            self.logger.error(f"[{exchange_code}] 메트릭 업데이트 실패: {str(e)}")
            
    def _estimate_data_size(self, data: Dict) -> int:
        """
        데이터 크기 추정 (효율적인 방식)
        
        Args:
            data: 데이터 객체
            
        Returns:
            int: 추정된 바이트 크기
        """
        # 빈 데이터 처리
        if not data:
            return 0
            
        size = 0
        
        # bids와 asks가 있는 경우 (오더북 데이터)
        if "bids" in data and isinstance(data["bids"], list):
            # 각 호가 항목의 대략적인 크기 (숫자 + 콤마 + 대괄호)
            size += len(data["bids"]) * 20  # 호가당 평균 20바이트로 추정
            
        if "asks" in data and isinstance(data["asks"], list):
            size += len(data["asks"]) * 20
        
        # 기타 메타데이터의 대략적인 크기
        size += 100  # 타임스탬프, 시퀀스 번호 등 (고정 100바이트로 추정)
        
        return size
        
    def _send_to_output_queue(self, symbol: str, orderbook: Any, is_valid: bool) -> None:
        """
        검증된 오더북 데이터를 출력 큐에 전송
        
        Args:
            symbol: 심볼
            orderbook: 오더북 데이터
            is_valid: 데이터 유효성 여부
        """
        if is_valid and self.output_queue and orderbook:
            # 오더북 데이터가 객체일 경우 dict로 변환
            data = orderbook.to_dict() if hasattr(orderbook, 'to_dict') else orderbook
            
            self.output_queue.put_nowait({
                "exchange": self.exchange_code,
                "symbol": symbol,
                "timestamp": time.time(),
                "data": data
            })
            
    def log_error(self, msg: str, exc_info: bool = False) -> None:
        """
        오류 로깅
        
        Args:
            msg: 오류 메시지
            exc_info: 예외 정보 포함 여부
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        self.logger.error(f"{exchange_kr} {msg}", exc_info=exc_info)
        self.metrics_manager.record_error(self.exchange_code)

    def log_warning(self, msg: str) -> None:
        """
        경고 로깅
        
        Args:
            msg: 경고 메시지
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        self.logger.warning(f"{exchange_kr} {msg}")

    def log_info(self, msg: str) -> None:
        """
        정보 로깅
        
        Args:
            msg: 정보 메시지
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        self.logger.info(f"{exchange_kr} {msg}")

    def log_debug(self, msg: str) -> None:
        """
        디버그 로깅
        
        Args:
            msg: 디버그 메시지
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        self.logger.debug(f"{exchange_kr} {msg}")

