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
    def __init__(self, connection: BaseWebsocketConnector, exchange_code: str = None):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
            exchange_code: 거래소 코드 (기본값: connection 객체에서 가져옴)
        """
        self.connection = connection
        self.logger = get_unified_logger()
        
        # 거래소 코드가 없으면 connection에서 가져옴
        self.exchange_code = exchange_code or self.connection.exchangename
        
        # 거래소 한글 이름 미리 계산하여 저장
        self.exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        
        # 웹소켓 객체 직접 저장
        self.ws = None
        
        # 메트릭 매니저 싱글톤 인스턴스 사용
        self.metrics_manager = WebsocketMetricsManager.get_instance()
        self.metrics_manager.initialize_exchange(self.exchange_code)
        
        # 이벤트 버스 초기화
        from crosskimp.ob_collector.orderbook.event_bus import EventBus
        self.event_bus = EventBus.get_instance()
        
        # 구독 상태 관리
        self.subscribed_symbols = {}  # symbol: True/False
        
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
        
    # 2. 연결 관리 및 메시지 송수신 단계
    @property
    def is_connected(self) -> bool:
        """연결 상태 확인 (연결 객체에서 직접 참조)"""
        # BaseWebsocketConnector의 is_connected 속성만 참조
        return self.connection.is_connected
    
    async def _ensure_websocket(self) -> bool:
        """
        웹소켓 연결 확보
        
        웹소켓 객체가 없거나 연결이 끊어진 경우, 연결 관리자의 
        get_websocket() 메서드를 통해 자동 연결을 시도합니다.
        get_websocket()은 이제 필요시 내부적으로 connect()를 호출합니다.
        
        Returns:
            bool: 웹소켓 객체 확보 성공 여부
        """
        # 연결 시도
        if not self.is_connected:
            self.log_info("웹소켓 연결 확보 시도")
        
        try:
            # 연결 관리자의 get_websocket()을 통해 자동 연결 시도
            # 이 메서드는 필요시 내부적으로 connect()를 호출합니다
            self.ws = await self.connection.get_websocket()
            
            if not self.ws:
                self.log_error("웹소켓 객체 가져오기 실패")
                return False
                
            if not self.is_connected:
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
                self.metrics_manager.record_metric(self.exchange_code, "message")
            
            return message
        except websockets.exceptions.ConnectionClosed as e:
            self.log_warning(f"웹소켓 연결 끊김: {e}")
            # 연결 끊김 처리 - 웹소켓 객체만 정리 (연결 상태는 connector에서 관리)
            self.ws = None
            return None
        except Exception as e:
            self.log_error(f"메시지 수신 실패: {str(e)}")
            self.metrics_manager.record_metric(self.exchange_code, "error")
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
    async def subscribe(self, symbol):
        """
        심볼 구독
        
        단일 심볼 또는 심볼 리스트를 구독합니다.
        각 거래소별 구현에서는 자체적인 배치 처리 로직을 포함해야 합니다.
        
        이벤트 버스를 통한 구독 방식을 사용합니다:
        
        ```python
        event_bus = EventBus.get_instance()
        event_bus.subscribe("orderbook_snapshot", snapshot_handler)
        event_bus.subscribe("orderbook_delta", delta_handler)
        event_bus.subscribe("orderbook_error", error_handler)
        ```
        
        Args:
            symbol: 구독할 심볼 또는 심볼 리스트
            
        Returns:
            bool: 구독 성공 여부
        """
        pass
        
        """
        # 자식 클래스 구현 예시
        # 1. 심볼 전처리
        # 2. 구독 메시지 생성 및 전송
        # 3. 구독 상태 업데이트
        # 4. 메시지 수신 루프 시작
        # 5. 구독 이벤트 발행
        """
    
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
                "symbol": symbol,
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
        메시지 수신 처리 기본 구현
        
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
            
        """
        # 자식 클래스 구현 예시 - 이벤트 버스 패턴 사용
        # 1. 메시지 파싱
        # 2. 메시지 타입과 심볼 식별
        # 3. 스냅샷 또는 델타 처리
        # 4. 검증기로 유효성 검사
        # 5. 이벤트 발행: self.publish_event(symbol, data, event_type)
        """
    
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
                self.metrics_manager.record_metric(self.exchange_code, "error")
                await asyncio.sleep(0.1)
                
        self.logger.info(f"{self.exchange_kr} 메시지 수신 루프 종료")

    # 5. 이벤트 발행 단계 (수정됨)
    def _handle_error(self, symbol: str, error: str) -> None:
        """
        오류 메시지 기록 및 에러 메트릭 업데이트
        
        Args:
            symbol: 심볼
            error: 오류 메시지
        """
        self.logger.error(f"{self.exchange_kr} {symbol} 오류 발생: {error}")
        self.metrics_manager.record_metric(self.exchange_code, "error")
        
        # 이벤트 버스를 통해 오류 이벤트 발행
        self.publish_event(symbol, error, "error")
    
    def _update_metrics(self, exchange_code: str, start_time: float, message_type: str, data: Dict) -> None:
        """
        메트릭 업데이트: 이벤트 버스를 통해 메트릭 이벤트 발행
        
        Args:
            exchange_code: 거래소 코드
            start_time: 메시지 처리 시작 시간
            message_type: 메시지 타입 ("snapshot" 또는 "delta")
            data: 메시지 데이터
        """
        try:
            # 처리 시간 계산
            processing_time_ms = (time.time() - start_time) * 1000
            
            # 메트릭 이벤트 데이터 준비
            metric_event = {
                "exchange_code": exchange_code,
                "event_type": message_type,
                "processing_time_ms": processing_time_ms,
                "timestamp": time.time()
            }
            
            # 이벤트 버스를 통해 메트릭 이벤트 발행
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self.event_bus.publish("metric_event", metric_event))
            else:
                self.event_bus.publish_sync("metric_event", metric_event)
        except Exception as e:
            self.logger.error(f"{self.exchange_kr} 메트릭 업데이트 실패: {str(e)}")
    
    def publish_event(self, symbol: str, data: Any, event_type: str) -> None:
        """
        이벤트 버스를 통해 오더북 이벤트 발행
        
        Args:
            symbol: 심볼
            data: 이벤트 데이터
            event_type: 이벤트 타입 ("snapshot", "delta", "error")
        """
        try:
            # 이벤트 데이터 준비
            event_data = {
                "exchange_code": self.exchange_code,
                "symbol": symbol,
                "data": data,
                "timestamp": time.time(),
                "type": event_type
            }
            
            # 이벤트 타입에 따라 다른 이벤트 채널 사용
            channel = f"orderbook_{event_type}"
            
            # 이벤트 발행
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self.event_bus.publish(channel, event_data))
            else:
                self.event_bus.publish_sync(channel, event_data)
                
            # 메트릭 이벤트도 추가로 발행 (통계 및 모니터링용)
            metric_event = {
                "exchange_code": self.exchange_code,
                "event_type": event_type,  # snapshot, delta, error 등을 그대로 사용
                "timestamp": time.time(),
                "source": "subscription"
            }
            
            if loop.is_running():
                asyncio.create_task(self.event_bus.publish("metric_event", metric_event))
            else:
                self.event_bus.publish_sync("metric_event", metric_event)
                
        except Exception as e:
            self.log_warning(f"이벤트 버스 발행 실패: {str(e)}")
    
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
            
            # 구독 취소 이벤트 발행
            self._publish_subscription_status_event(symbols, "unsubscribed")
            
            self.log_info("모든 심볼 구독 취소 완료")
            
            # 웹소켓 객체 초기화
            self.ws = None
            
            # 태스크 정리
            await self._cancel_tasks()
            
            self.log_info("모든 자원 정리 완료")
            return True
            
        except Exception as e:
            self.log_error(f"구독 취소 중 오류 발생: {e}")
            self.metrics_manager.record_metric(self.exchange_code, "error")
            return False
    
    def _cleanup_subscription(self, symbol: str) -> None:
        """
        구독 관련 상태 정리
        
        Args:
            symbol: 정리할 심볼
        """
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
        self.metrics_manager.record_metric(self.exchange_code, "error")

    def log_warning(self, msg: str) -> None:
        """경고 메시지 로깅"""
        self.logger.warning(f"{self.exchange_kr} {msg}")

    def log_info(self, msg: str) -> None:
        """정보 메시지 로깅"""
        self.logger.info(f"{self.exchange_kr} {msg}")

    def log_debug(self, msg: str) -> None:
        """디버그 메시지 로깅"""
        self.logger.debug(f"{self.exchange_kr} {msg}")

    def _publish_subscription_status_event(self, symbols: List[str], status: str) -> None:
        """
        구독 상태 변경 이벤트 발행
        
        Args:
            symbols: 심볼 목록
            status: 상태 (subscribed, unsubscribed 등)
        """
        try:
            # 이벤트 데이터 준비
            event_data = {
                "exchange_code": self.exchange_code,
                "symbols": symbols,
                "timestamp": time.time(),
                "status": status
            }
            
            # 이벤트 발행
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self.event_bus.publish("subscription_status", event_data))
            else:
                self.event_bus.publish_sync("subscription_status", event_data)
                
        except Exception as e:
            self.log_warning(f"구독 상태 이벤트 발행 실패: {str(e)}")

