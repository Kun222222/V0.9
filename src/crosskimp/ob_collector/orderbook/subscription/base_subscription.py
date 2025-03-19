import asyncio
import time
from abc import ABC, abstractmethod
from typing import Dict, Optional, List, Any, Union
import websockets
import json
import datetime

from crosskimp.logger.logger import get_unified_logger, create_raw_logger
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.metric.metrics_manager import WebsocketMetricsManager
from crosskimp.config.constants_v3 import EXCHANGE_NAMES_KR, LOG_SUBDIRS

class BaseSubscription(ABC):
    
    # 1. 초기화 단계
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
        
        # 오더북 데이터 로깅 설정 - 기본적으로 비활성화
        self.log_orderbook_enabled = False
        
        # 검증기 및 출력 큐
        self.validator = None
        self.output_queue = None
        
        # 로깅 설정
        self.raw_logger = None
        self._setup_raw_logging()
        
        # 메시지 처리 상태 추적
        self.last_message_time = {}  # symbol -> timestamp
        self.message_count = {}  # symbol -> count
        self.last_sequence = {}  # symbol -> sequence
        
        # 메시지 처리 시간 추적
        self.processing_times = []  # 최근 처리 시간 기록
        
        # 오더북 데이터 저장소
        self.orderbooks = {}  # symbol -> orderbook_data
        
        # 타임스탬프 추적
        self.last_timestamps = {}  # symbol -> {"timestamp": timestamp, "counter": count}
        
        # 출력용 오더북 뎁스 설정
        self.output_depth = 10  # 로깅에 사용할 오더북 뎁스
    
    def _setup_raw_logging(self):
        """Raw 데이터 로깅 설정"""
        try:
            raw_data_dir = LOG_SUBDIRS['raw_data']
            self.log_file_path = raw_data_dir / f"{self.exchange_code}_raw_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            self.raw_logger = create_raw_logger(self.exchange_code)
            self.log_info("raw 로거 초기화 완료")
        except Exception as e:
            self.log_error(f"Raw 로깅 설정 실패: {str(e)}")
            self.log_orderbook_enabled = False
    
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
    
    # 2. 연결 관리 단계
    @property
    def is_connected(self) -> bool:
        """연결 상태 확인 (연결 객체에서 직접 참조)"""
        # 중복 상태 관리를 제거하고 연결 객체의 상태만 참조
        return self.connection.is_connected if self.connection else False
    
    def _is_connected(self) -> bool:
        """연결 상태 확인 - 자식 클래스에서 사용할 내부 메서드"""
        return hasattr(self.connection, 'is_connected') and self.connection.is_connected
    
    async def _ensure_connection(self) -> bool:
        """
        연결 확보 및 상태 관리
        
        웹소켓 연결이 되어 있지 않으면 연결을 시도합니다.
        
        Returns:
            bool: 연결 성공 여부
        """
        if self._is_connected():
            return True
            
        self.log_info("웹소켓 연결 시작")
        try:
            if not await self.connection.connect():
                self.log_error("웹소켓 연결 실패")
                return False
                
            self.metrics_manager.update_connection_state(self.exchange_code, "connected")
            return True
        except Exception as e:
            self.log_error(f"연결 중 오류: {str(e)}")
            return False
    
    # 3. 구독 처리 단계
    async def _preprocess_symbols(self, symbol: Union[str, List[str]]) -> List[str]:
        """
        심볼 전처리
        
        Args:
            symbol: 심볼 또는 심볼 리스트
            
        Returns:
            List[str]: 전처리된 심볼 리스트
        """
        if isinstance(symbol, list):
            symbols = symbol
        else:
            symbols = [symbol]
            
        if not symbols:
            self.log_warning("구독할 심볼이 없습니다.")
            return []
            
        return symbols
        
    async def _register_callbacks(self, symbols: List[str], on_snapshot=None, on_delta=None, on_error=None) -> None:
        """
        콜백 함수 등록
        
        Args:
            symbols: 심볼 리스트
            on_snapshot: 스냅샷 콜백
            on_delta: 델타 콜백
            on_error: 에러 콜백
        """
        for sym in symbols:
            if on_snapshot is not None:
                self.snapshot_callbacks[sym] = on_snapshot
            if on_delta is not None:
                self.delta_callbacks[sym] = on_delta
            elif on_snapshot is not None:
                self.delta_callbacks[sym] = on_snapshot
            if on_error is not None:
                self.error_callbacks[sym] = on_error
            
            self.subscribed_symbols[sym] = True
    
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
    
    # 4. 메시지 수신 및 처리 단계
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
    
    def log_raw_message(self, message: str) -> None:
        """
        원시 메시지 로깅
        
        Args:
            message: 원시 메시지
        """
        if self.log_orderbook_enabled and self.raw_logger:
            try:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                # 메시지가 너무 길 수 있으므로 일부만 로깅
                message_preview = message[:500] + "..." if len(message) > 500 else message
                self.raw_logger.debug(f"[{timestamp}] RAW: {message_preview}")
            except Exception as e:
                self.log_error(f"원시 메시지 로깅 실패: {str(e)}")
    
    def log_orderbook_data(self, symbol: str, orderbook: Dict) -> None:
        """
        처리된 오더북 데이터 로깅
        
        Args:
            symbol: 심볼
            orderbook: 오더북 데이터
        """
        if self.log_orderbook_enabled and self.raw_logger:
            try:
                current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                
                bids = orderbook.get('bids', [])[:self.output_depth]
                asks = orderbook.get('asks', [])[:self.output_depth]
                
                log_data = {
                    "exchangename": self.exchange_code,
                    "symbol": symbol,
                    "bids": bids,
                    "asks": asks,
                    "timestamp": orderbook.get('timestamp'),
                    "sequence": orderbook.get('sequence')
                }
                
                self.raw_logger.debug(f"[{current_time}] 매수 ({len(bids)}) / 매도 ({len(asks)}) {json.dumps(log_data)}")
            except Exception as e:
                self.log_error(f"오더북 데이터 로깅 실패: {str(e)}")
    
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
    
    async def _on_message(self, message: str) -> None:
        """
        메시지 수신 콜백 기본 구현
        
        Args:
            message: 수신된 원시 메시지
        """
        try:
            # 원본 메시지 로깅 (활성화된 경우만)
            if self.log_orderbook_enabled:
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
            exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
            self.logger.error(f"{exchange_kr} 메시지 처리 오류: {str(e)}")
            
            # 에러 처리
            symbol = self._extract_symbol_from_message(message)
            if symbol:
                self._handle_error(symbol, str(e))
            else:
                self._handle_error("unknown", str(e))
    
    async def message_loop(self) -> None:
        """
        메시지 수신 루프
        
        웹소켓에서 메시지를 계속 수신하고 처리하는 루프입니다.
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        self.logger.info(f"{exchange_kr} 메시지 수신 루프 시작")
        
        while not self.stop_event.is_set():
            try:
                # 연결 상태 확인
                if not self.is_connected:
                    self.logger.warning(f"{exchange_kr} 웹소켓 연결이 끊어짐, 재연결 대기")
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
                self.logger.info(f"{exchange_kr} 메시지 수신 루프 취소됨")
                break
                
            except websockets.exceptions.ConnectionClosed as e:
                self.logger.warning(f"{exchange_kr} 웹소켓 연결 끊김: {e}")
                # 연결 상태 업데이트
                self.metrics_manager.update_connection_state(self.exchange_code, "disconnected")
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"{exchange_kr} 메시지 수신 루프 오류: {str(e)}")
                self.metrics_manager.record_error(self.exchange_code)
                await asyncio.sleep(0.1)
                
        self.logger.info(f"{exchange_kr} 메시지 수신 루프 종료")

    # 5. 콜백 호출 단계
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
            
            # 검증기가 없으면 콜백으로 직접 전달
            if not self.validator:
                exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
                self.logger.debug(f"{exchange_kr} 검증기 없음, 원본 데이터를 콜백으로 직접 전달")
                
                # 콜백 직접 호출
                if symbol in self.snapshot_callbacks:
                    await self.snapshot_callbacks[symbol](symbol, data)
                return
                
            # 데이터 검증
            result = await self.validator.initialize_orderbook(symbol, data)
            is_valid = result.is_valid
            orderbook = self.validator.get_orderbook(symbol) if is_valid else None
            
            # 스냅샷 콜백으로 전달
            if is_valid and symbol in self.snapshot_callbacks:
                await self.snapshot_callbacks[symbol](symbol, orderbook if orderbook else data)
            
            # 메트릭 및 통계 업데이트
            self._update_metrics(self.exchange_code, start_time, "snapshot", data)
            
        except Exception as e:
            exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
            self.logger.error(f"{exchange_kr} {symbol} 스냅샷 처리 실패: {str(e)}")
            self.metrics_manager.record_error(self.exchange_code)
            
            # 에러 콜백 호출
            if symbol in self.error_callbacks:
                await self.error_callbacks[symbol](symbol, str(e))
            
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
            
            # 검증기가 없으면 콜백으로 직접 전달
            if not self.validator:
                exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
                self.logger.debug(f"{exchange_kr} 검증기 없음, 원본 데이터를 콜백으로 직접 전달")
                
                # 콜백 직접 호출
                if symbol in self.delta_callbacks:
                    await self.delta_callbacks[symbol](symbol, data)
                # 델타 콜백이 없으면 스냅샷 콜백으로 전달 (선택적)
                elif symbol in self.snapshot_callbacks:
                    await self.snapshot_callbacks[symbol](symbol, data)
                return
                
            # 오더북 업데이트 (세부 구현은 validator에 위임)
            result = await self.validator.update(symbol, data)
            
            # 검증 결과 처리
            is_valid = result.is_valid
            orderbook = self.validator.get_orderbook(symbol) if is_valid else None
            
            # 델타 콜백으로 전달
            if is_valid and symbol in self.delta_callbacks:
                await self.delta_callbacks[symbol](symbol, orderbook if orderbook else data)
            # 델타 콜백이 없으면 스냅샷 콜백으로 전달 (선택적)
            elif is_valid and symbol in self.snapshot_callbacks:
                await self.snapshot_callbacks[symbol](symbol, orderbook if orderbook else data)
            
            # 메트릭 및 통계 업데이트
            self._update_metrics(self.exchange_code, start_time, "delta", data)
            
        except Exception as e:
            exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
            self.logger.error(f"{exchange_kr} {symbol} 델타 처리 실패: {str(e)}")
            self.metrics_manager.record_error(self.exchange_code)
            
            # 에러 콜백 호출
            if symbol in self.error_callbacks:
                await self.error_callbacks[symbol](symbol, str(e))
    
    def _handle_error(self, symbol: str, error: str) -> None:
        """
        에러 콜백
        
        Args:
            symbol: 심볼
            error: 오류 메시지
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        self.logger.error(f"{exchange_kr} {symbol} 오류 발생: {error}")
        self.metrics_manager.record_error(self.exchange_code)
        
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
        """
        모든 에러 콜백 호출

        Args:
            error_msg: 에러 메시지
        """
        for symbol in self.error_callbacks:
            await self.error_callbacks[symbol](symbol, error_msg)

    async def _call_callback(self, symbol: str, data: Dict, is_snapshot: bool = True) -> None:
        """
        콜백 메서드 (스냅샷 또는 델타)
        
        Args:
            symbol: 심볼
            data: 오더북 데이터
            is_snapshot: 스냅샷 여부 (True: 스냅샷 콜백, False: 델타 콜백)
        """
        try:
            callback = self.snapshot_callbacks.get(symbol) if is_snapshot else self.delta_callbacks.get(symbol)
            if callback:
                await callback(symbol, data)
        except Exception as e:
            callback_type = "스냅샷" if is_snapshot else "델타"
            self.log_error(f"{symbol} {callback_type} 콜백 호출 실패: {str(e)}")
            
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
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, f"[{exchange_code}]")
            self.logger.error(f"{exchange_kr} 메트릭 업데이트 실패: {str(e)}")
    
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
        
        Warning: 이 메서드는 더 이상 사용되지 않으며 콜백을 통한 처리로 대체됩니다.
        콜백이 제공되지 않은 경우에만 사용됩니다.
        
        Args:
            symbol: 심볼
            orderbook: 오더북 데이터
            is_valid: 데이터 유효성 여부
        """
        # 더 이상 사용하지 않는 메서드이지만 하위 호환성을 위해 유지
        if is_valid and self.output_queue and orderbook:
            # 오더북 데이터가 객체일 경우 dict로 변환
            data = orderbook.to_dict() if hasattr(orderbook, 'to_dict') else orderbook
            
            self.output_queue.put_nowait({
                "exchange": self.exchange_code,
                "symbol": symbol,
                "timestamp": time.time(),
                "data": data
            })
    
    # 6. 구독 취소 단계
    async def _cancel_tasks(self) -> None:
        """모든 태스크 취소"""
        for task in self.tasks.values():
            task.cancel()
        self.tasks.clear()
    
    async def _cancel_message_loop(self) -> None:
        """메시지 수신 루프 취소"""
        if self.message_loop_task and not self.message_loop_task.done():
            self.message_loop_task.cancel()
            try:
                await self.message_loop_task
            except asyncio.CancelledError:
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
    
    async def unsubscribe(self, symbol: Optional[str] = None) -> bool:
        """
        구독 취소
        
        이 메서드를 사용하면 모든 심볼 구독을 취소하고 자원을 정리합니다.
        symbol 파라미터는 하위 호환성을 위해 유지되지만 무시됩니다.
        모든 구현은 전체 구독 취소만 지원해야 합니다.
        
        Args:
            symbol: 무시됨. 과거 버전과의 호환성을 위해 유지됨
            
        Returns:
            bool: 구독 취소 성공 여부
        """
        try:
            # 종료 이벤트 설정
            self.stop_event.set()
            
            # 메시지 수신 루프 취소
            await self._cancel_message_loop()
            
            # 모든 심볼 목록 저장
            symbols = list(self.subscribed_symbols.keys())
            
            # 각 심볼별로 구독 취소 메시지 전송
            for sym in symbols:
                # 구독 취소 메시지 생성 및 전송
                unsubscribe_message = await self.create_unsubscribe_message(sym)
                try:
                    if self.connection and self._is_connected():
                        await self.connection.send_message(json.dumps(unsubscribe_message))
                except Exception as e:
                    self.log_warning(f"{sym} 구독 취소 메시지 전송 실패: {e}")
                
                # 내부적으로 구독 상태 정리
                self._cleanup_subscription(sym)
            
            self.log_info("모든 심볼 구독 취소 완료")
            
            # 연결 종료 처리
            if hasattr(self.connection, 'disconnect'):
                try:
                    await asyncio.wait_for(self.connection.disconnect(), timeout=2.0)
                except (asyncio.TimeoutError, Exception) as e:
                    self.log_warning(f"연결 종료 중 오류: {str(e)}")
            
            # 태스크 정리
            await self._cancel_tasks()
            
            self.log_info("모든 자원 정리 완료")
            return True
            
        except Exception as e:
            self.log_error(f"구독 취소 중 오류 발생: {e}")
            self.metrics_manager.record_error(self.exchange_code)
            return False
    
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
    
    # 7. 로깅 유틸리티
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

