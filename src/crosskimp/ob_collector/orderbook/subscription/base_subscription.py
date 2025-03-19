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
        
        # 거래소 한글 이름 미리 계산하여 저장
        self.exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        
        # 웹소켓 객체 직접 저장
        self.ws = None
        
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
    
    # 2. 연결 관리 및 메시지 송수신 단계
    @property
    def is_connected(self) -> bool:
        """연결 상태 확인 (연결 객체에서 직접 참조)"""
        # 웹소켓 객체와 연결 객체 모두 확인
        return self.ws is not None and self.connection.is_connected
    
    def _is_connected(self) -> bool:
        """연결 상태 확인 - 자식 클래스에서 사용할 내부 메서드"""
        return self.ws is not None and hasattr(self.connection, 'is_connected') and self.connection.is_connected
    
    async def _ensure_websocket(self) -> bool:
        """
        웹소켓 연결 확보
        
        웹소켓 객체가 없거나 연결이 끊어진 경우 재연결을 시도하고
        웹소켓 객체를 얻습니다.
        
        Returns:
            bool: 웹소켓 객체 확보 성공 여부
        """
        # 이미 연결되어 있고 웹소켓 객체가 있으면 성공
        if self._is_connected() and self.ws:
            return True
            
        # 연결 시도
        self.log_info("웹소켓 연결 확보 시도")
        try:
            # 연결 관리자를 통해 연결
            if not await self.connection.connect():
                self.log_error("웹소켓 연결 실패")
                return False
            
            # 웹소켓 객체 가져오기
            self.ws = await self.connection.get_websocket()
            if not self.ws:
                self.log_error("웹소켓 객체 가져오기 실패")
                return False
                
            self.log_info("웹소켓 연결 확보 성공")
            return True
        except Exception as e:
            self.log_error(f"웹소켓 연결 확보 중 오류: {str(e)}")
            return False
    
    async def send_message(self, message: str) -> bool:
        """
        웹소켓을 통해 메시지 전송
        
        Args:
            message: 전송할 메시지
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            # 웹소켓 연결 확보
            if not await self._ensure_websocket():
                self.log_error("웹소켓 연결이 없어 메시지 전송 실패")
                return False
                
            # 메시지 전송
            await self.ws.send(message)
            return True
        except Exception as e:
            self.log_error(f"메시지 전송 실패: {str(e)}")
            # 연결 끊김 처리
            self.ws = None
            return False
    
    async def receive_message(self) -> Optional[str]:
        """
        웹소켓에서 메시지 수신
        
        Returns:
            Optional[str]: 수신된 메시지 또는 None
        """
        try:
            # 웹소켓 연결 확보
            if not await self._ensure_websocket():
                return None
                
            # 메시지 수신
            message = await self.ws.recv()
            
            # 기본적인 메트릭만 유지
            if message:
                self.metrics_manager.record_message(self.exchange_code)
            
            return message
        except websockets.exceptions.ConnectionClosed as e:
            self.log_warning(f"웹소켓 연결 끊김: {e}")
            # 연결 끊김 처리
            self.ws = None
            # 연결 상태 업데이트
            self.metrics_manager.update_connection_state(self.exchange_code, "disconnected")
            return None
        except Exception as e:
            self.log_error(f"메시지 수신 실패: {str(e)}")
            self.metrics_manager.record_error(self.exchange_code)
            return None
    
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
    def log_raw_message(self, message: str) -> None:
        """
        원시 메시지 로깅
        
        Args:
            message: 원시 메시지
        """
        if self.log_orderbook_enabled and self.raw_logger:
            try:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                
                if isinstance(message, bytes):
                    message = message.decode('utf-8')
                
                # 메시지 트림 (너무 길 경우)
                if len(message) > 1000:
                    log_message = message[:1000] + "... (truncated)"
                else:
                    log_message = message
                
                # 파일에 로깅
                self.raw_logger.debug(f"[{timestamp}] {log_message}")
                
            except Exception as e:
                self.logger.error(f"{self.exchange_kr} 원시 메시지 로깅 실패: {str(e)}")
    
    def log_orderbook_data(self, symbol: str, orderbook: Dict) -> None:
        """
        오더북 데이터 로깅
        
        Args:
            symbol: 심볼
            orderbook: 오더북 데이터
        """
        if not self.log_orderbook_enabled:
            return
        
        try:
            current_time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
            
            # 오더북 데이터 준비
            bids = orderbook.get("bids", [])[:10]  # 상위 10개만
            asks = orderbook.get("asks", [])[:10]  # 상위 10개만
            
            log_data = {
                "ts": orderbook.get("timestamp", int(time.time() * 1000)),
                "seq": orderbook.get("sequence", 0),
                "bids": bids,
                "asks": asks
            }
                
            self.raw_logger.debug(f"[{current_time}] 매수 ({len(bids)}) / 매도 ({len(asks)}) {json.dumps(log_data)}")
        except Exception as e:
            self.log_error(f"오더북 데이터 로깅 실패: {str(e)}")
    
    async def _on_message(self, message: str) -> None:
        """
        메시지 수신 콜백 기본 구현
        
        이 메서드는 모든 자식 클래스에서 오버라이드해야 합니다.
        각 거래소의 메시지 형식에 맞게 처리 로직을 구현해야 합니다.
        
        Args:
            message: 수신된 원시 메시지
        """
        try:
            # 원본 메시지 로깅 (활성화된 경우만)
            if self.log_orderbook_enabled:
                self.log_raw_message(message)
            
            # 기본 구현은 로깅만 수행 - 자식 클래스에서 오버라이드해야 함
            self.log_warning(f"_on_message 메서드가 구현되지 않았습니다. 자식 클래스에서 구현해야 합니다.")
            
        except Exception as e:
            # 에러 로깅
            self.logger.error(f"{self.exchange_kr} 메시지 처리 오류: {str(e)}")
            
            # 에러 처리
            self._handle_error("unknown", str(e))
    
    async def message_loop(self) -> None:
        """
        메시지 수신 루프
        
        웹소켓에서 메시지를 계속 수신하고 처리하는 루프입니다.
        """
        self.logger.info(f"{self.exchange_kr} 메시지 수신 루프 시작")
        
        while not self.stop_event.is_set():
            try:
                # 연결 확보 시도
                if not await self._ensure_websocket():
                    self.logger.warning(f"{self.exchange_kr} 웹소켓 연결 확보 실패, 재시도 예정")
                    await asyncio.sleep(1)
                    continue
                
                # 메시지 수신
                message = await self.receive_message()
                
                if not message:
                    await asyncio.sleep(0.01)  # 짧은 대기 시간 추가 (CPU 사용량 감소)
                    continue
                
                # 메시지 처리
                await self._on_message(message)
                
            except asyncio.CancelledError:
                self.logger.info(f"{self.exchange_kr} 메시지 수신 루프 취소됨")
                break
                
            except websockets.exceptions.ConnectionClosed:
                # receive_message에서 이미 처리됨
                self.ws = None
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"{self.exchange_kr} 메시지 수신 루프 오류: {str(e)}")
                self.metrics_manager.record_error(self.exchange_code)
                await asyncio.sleep(0.1)
                
        self.logger.info(f"{self.exchange_kr} 메시지 수신 루프 종료")

    # 5. 콜백 호출 단계
    def _handle_error(self, symbol: str, error: str) -> None:
        """오류 메시지 기록 및 에러 메트릭 업데이트"""
        self.logger.error(f"{self.exchange_kr} {symbol} 오류 발생: {error}")
        self.metrics_manager.record_error(self.exchange_code)
        
        # 에러 콜백 호출
        asyncio.create_task(self._call_callback(symbol, error, callback_type="error"))
    
    async def _call_callback(self, symbol: str, data: Any, callback_type: str = "snapshot") -> None:
        """
        콜백 함수 호출
        
        Args:
            symbol: 심볼
            data: 전달할 데이터
            callback_type: 콜백 유형 ("snapshot", "delta", "error" 중 하나)
        """
        try:
            callbacks = None
            if callback_type == "snapshot":
                callbacks = self.snapshot_callbacks
            elif callback_type == "delta":
                callbacks = self.delta_callbacks
            elif callback_type == "error":
                callbacks = self.error_callbacks
            else:
                self.log_error(f"알 수 없는 콜백 유형: {callback_type}")
                return
            
            if symbol in callbacks:
                await callbacks[symbol](symbol, data)
            elif callback_type == "error" and self.error_callbacks:
                # 에러는 모든 등록된 에러 콜백에 전송
                for sym, callback in self.error_callbacks.items():
                    await callback(sym, data)
        except Exception as e:
            self.log_error(f"{symbol} {callback_type} 콜백 호출 실패: {str(e)}")
    
    def _update_metrics(self, exchange_code: str, start_time: float, message_type: str, data: Dict) -> None:
        """
        메트릭 업데이트: 기본 메시지 카운트와 에러 카운트만 유지
        """
        try:
            # 메시지 수신 기록만 간단하게 유지
            self.metrics_manager.record_message(self.exchange_code)
        except Exception as e:
            self.logger.error(f"{self.exchange_kr} 메트릭 업데이트 실패: {str(e)}")
    
    def _estimate_data_size(self, data: Dict) -> int:
        """
        오더북 데이터 크기 추정 (바이트 단위) - 미사용 메서드이므로 간소화
        """
        return 0
    
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
                    if self.ws:
                        await self.send_message(json.dumps(unsubscribe_message))
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
            
            # 웹소켓 객체 초기화
            self.ws = None
            
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
    """
    로깅 유틸리티 메서드들
    
    아래 메서드들은 거래소 이름이 포함된 일관된 형식으로 로그를 기록합니다.
    - log_error: 오류 로깅 (에러 메트릭도 함께 기록)
    - log_warning: 경고 로깅
    - log_info: 정보 로깅
    - log_debug: 디버그 로깅
    """
    
    def log_error(self, msg: str, exc_info: bool = False) -> None:
        """오류 메시지 로깅 (exc_info: 예외 스택 트레이스 포함 여부)"""
        self.logger.error(f"{self.exchange_kr} {msg}", exc_info=exc_info)
        self.metrics_manager.record_error(self.exchange_code)

    def log_warning(self, msg: str) -> None:
        """경고 메시지 로깅"""
        self.logger.warning(f"{self.exchange_kr} {msg}")

    def log_info(self, msg: str) -> None:
        """정보 메시지 로깅"""
        self.logger.info(f"{self.exchange_kr} {msg}")

    def log_debug(self, msg: str) -> None:
        """디버그 메시지 로깅"""
        self.logger.debug(f"{self.exchange_kr} {msg}")

