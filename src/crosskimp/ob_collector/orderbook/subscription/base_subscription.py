import asyncio
import time
from abc import ABC, abstractmethod
from typing import Dict, Optional, List, Any, Union
import websockets
import json
import datetime
import os
import traceback

from crosskimp.logger.logger import get_unified_logger, create_raw_logger
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
from crosskimp.config.constants_v3 import EXCHANGE_NAMES_KR, LOG_SUBDIRS, Exchange, normalize_exchange_code

# 이벤트 타입 정의 추가
EVENT_TYPES = {
    "CONNECTION_STATUS": "connection_status",  # 연결 상태 변경
    "METRIC_UPDATE": "metric_update",          # 메트릭 업데이트
    "ERROR_EVENT": "error_event",              # 오류 이벤트
    "SUBSCRIPTION_STATUS": "subscription_status",  # 구독 상태 변경
    "ORDERBOOK_UPDATE": "orderbook_update",     # 오더북 업데이트
    "DATA_SNAPSHOT": "data_snapshot",            # 데이터 스냅샷
    "DATA_DELTA": "data_delta",                # 데이터 델타
    "DATA_UPDATED": "data_updated"              # 데이터 업데이트
}

class BaseSubscription(ABC):
    
    # 1. 초기화 단계
    def __init__(self, connection: BaseWebsocketConnector, exchange_code: str = None):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
            exchange_code: 거래소 코드 (예: "upbit")
        """
        self.connection = connection
        self.logger = get_unified_logger()
        
        # 거래소 코드가 없으면 connection에서 가져옴
        self.exchange_code = normalize_exchange_code(exchange_code or self.connection.exchangename)
        
        # 거래소 한글 이름 미리 계산하여 저장
        self.exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        
        # 로깅 디렉토리 생성
        raw_data_dir = os.path.join(LOG_SUBDIRS['raw_data'], self.exchange_code)
        os.makedirs(raw_data_dir, exist_ok=True)
        
        # 웹소켓 객체 직접 저장
        self.ws = None
        
        # SystemEventManager 설정
        try:
            from crosskimp.ob_collector.orderbook.util.event_manager import SystemEventManager
            self.system_event_manager = SystemEventManager.get_instance()
            self.system_event_manager.initialize_exchange(self.exchange_code)
            
            # 현재 거래소 코드 설정
            self.system_event_manager.set_current_exchange(self.exchange_code)
            
        except ImportError:
            self.logger.error("SystemEventManager를 가져올 수 없습니다. 일부 기능이 작동하지 않을 수 있습니다.")
        
        # 이벤트 버스 초기화
        from crosskimp.ob_collector.orderbook.util.event_bus import EventBus
        self.event_bus = EventBus.get_instance()
        
        # 구독 상태 관리
        self.subscribed_symbols = {}  # symbol: True/False
        
        # 비동기 태스크 관리
        self.tasks = {}
        
        # 메시지 수신 루프 태스크
        self.message_loop_task = None
        
        # 종료 이벤트
        self.stop_event = asyncio.Event()
        
        # 메시지 카운트 관련 변수 추가 (message_counter.py 대체)
        self._message_count = 0  # 총 메시지 수
        self._last_count_update = time.time()  # 마지막 카운트 업데이트 시간
        self._prev_message_count = 0  # 이전 메시지 카운트 (속도 계산용)
        self._count_update_interval = 1.0  # 메트릭 업데이트 간격
        
        # 메트릭 업데이트를 위한 태스크 시작
        self._start_metric_update_task()
        
        # 오더북 데이터 로깅 설정 - 기본적으로 비활성화
        self.raw_logging_enabled = False  # raw 데이터 로깅 활성화 여부
        self.orderbook_logging_enabled = False  # 오더북 데이터 로깅 활성화 여부
        self.raw_logger = None
        
        # 검증기 및 출력 큐
        self.validator = None
        
        # 로깅 설정
        self._setup_raw_logging()
        
        # 기본 검증기 초기화
        self.initialize_validator()
        
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
            # 로거 초기화 (로깅 활성화 여부와 관계없이)
            self.raw_logger = create_raw_logger(self.exchange_code)
            self.log_info("raw 로거 초기화 완료")
        except Exception as e:
            self.log_error(f"Raw 로깅 설정 실패: {str(e)}")
            self.raw_logging_enabled = False
            self.orderbook_logging_enabled = False
    
    def initialize_validator(self) -> None:
        """
        거래소 코드에 맞는 검증기 초기화
        """
        try:
            from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
            self.validator = BaseOrderBookValidator(self.exchange_code)
            self.log_debug(f"{self.exchange_kr} 검증기 초기화 완료")
        except ImportError:
            self.log_error("BaseOrderBookValidator를 가져올 수 없습니다. 검증 기능이 비활성화됩니다.")
            self.validator = None
    
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
        
        웹소켓 연결 객체로부터 메시지를 비동기로 수신합니다.
        연결 실패나 수신 오류 처리를 포함합니다.
        
        Returns:
            Optional[str]: 수신된 메시지 또는 None (오류 발생 시)
        """
        try:
            if not self.is_connected:
                self.log_error("메시지 수신 실패: 웹소켓이 연결되지 않음")
                return None
                
            # 웹소켓 객체 확인
            ws = await self.connection.get_websocket()
            if not ws:
                self.log_error("메시지 수신 실패: 웹소켓 객체가 없음")
                return None
                
            # 메시지 수신
            message = await ws.recv()
            
            # 메시지 수신 시 메트릭 증가 (자체 구현 메서드 사용)
            self.increment_message_count()
            
            return message
            
        except asyncio.CancelledError:
            # 태스크 취소는 오류가 아님
            raise
            
        except Exception as e:
            # 모든 다른 예외는 오류로 처리
            self.log_error(f"메시지 수신 중 오류: {str(e)}")
            
            # 오류 이벤트 발행
            asyncio.create_task(self.publish_system_event_sync(
                EVENT_TYPES["ERROR_EVENT"],
                error_type="message_error",
                message=str(e),
                severity="error"
            ))
            
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
            message: 로깅할 원시 메시지
        """
        if not self.raw_logging_enabled or not self.raw_logger:
            return
            
        try:
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            self.raw_logger.debug(f"[{current_time}] {message}")
        except Exception as e:
            self.log_error(f"원시 메시지 로깅 실패: {str(e)}")
    
    def log_orderbook_data(self, symbol: str, orderbook: Dict) -> None:
        """
        오더북 데이터 로깅
        
        Args:
            symbol: 심볼
            orderbook: 오더북 데이터
        """
        try:
            if not self.orderbook_logging_enabled or not self.raw_logger:
                return
                
            current_time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
            bids = orderbook.get("bids", [])[:10]  # 상위 10개만
            asks = orderbook.get("asks", [])[:10]  # 상위 10개만
            
            log_data = {
                "symbol": symbol,
                "ts": orderbook.get("timestamp"),
                "seq": orderbook.get("sequence"),
                "bids": bids,
                "asks": asks
            }
                
            self.raw_logger.debug(f"[{current_time}] 매수 ({len(bids)}) / 매도 ({len(asks)}) {json.dumps(log_data)}")
        except Exception as e:
            self.log_error(f"오더북 데이터 로깅 실패: {str(e)}")
    
    async def _on_message(self, message: str) -> None:
        """
        메시지 수신 콜백 (자식 클래스에서 재정의 가능)
        
        메시지를 수신하면 호출되는 콜백 메서드입니다.
        메시지 파싱, 검증, 처리를 담당합니다.
        
        Args:
            message: 수신된 메시지
        """
        try:
            # 메시지 처리 시작 시간 측정
            start_time = time.time()
            
            # 원시 메시지 로깅 (모든 거래소에서 동일하게 동작)
            self.log_raw_message(message)
            
            # 처리 시간 계산 및 메트릭 발행
            processing_time_ms = (time.time() - start_time) * 1000
            
            # 시스템 이벤트 발행 - 처리 시간
            asyncio.create_task(self.publish_system_event_sync(
                EVENT_TYPES["METRIC_UPDATE"],
                metric_name="processing_time",
                value=processing_time_ms
            ))
            
        except asyncio.CancelledError:
            # 태스크 취소는 오류가 아님
            raise
            
        except Exception as e:
            # 모든 다른 예외는 오류로 처리
            self.log_error(f"메시지 처리 중 오류: {str(e)}")
            
            # 오류 이벤트 발행
            asyncio.create_task(self.publish_system_event_sync(
                EVENT_TYPES["ERROR_EVENT"],
                error_type="message_processing_error",
                message=str(e),
                severity="error"
            ))
    
    async def message_loop(self) -> None:
        """
        메시지 수신 루프
        
        웹소켓으로부터 메시지를 지속적으로 수신하고 처리합니다.
        """
        self.log_info("메시지 수신 루프 시작")
        
        try:
            # 웹소켓 연결 확인
            if not await self._ensure_websocket():
                self.log_error("메시지 루프 시작 실패: 웹소켓 연결 없음")
                return
                
            # 메시지 수신 루프
            while not self.stop_event.is_set():
                try:
                    # 메시지 수신
                    message = await self.receive_message()
                    if not message:
                        # 메시지 수신 실패 시 짧은 대기 후 재시도
                        await asyncio.sleep(0.1)
                        continue
                    
                    # 메시지 처리 (자식 클래스의 _on_message 호출)
                    await self._on_message(message)
                    
                except asyncio.CancelledError:
                    # 태스크 취소는 에러가 아니므로 정상 종료
                    self.log_debug("메시지 루프 취소됨")
                    break
                    
                except Exception as e:
                    # 모든 다른 예외는 오류로 처리하되 루프는 계속
                    self.log_error(f"메시지 처리 중 예외 발생: {str(e)}")
                    
                    # 오류 이벤트 발행
                    asyncio.create_task(self.publish_system_event_sync(
                        EVENT_TYPES["ERROR_EVENT"],
                        error_type="message_loop_error",
                        message=str(e),
                        severity="error"
                    ))
                    
                    # 잠시 대기 후 계속
                    await asyncio.sleep(0.1)
            
            self.log_info("메시지 수신 루프 종료")
            
        except Exception as e:
            # 메시지 루프 자체에서 발생한 예외
            self.log_error(f"메시지 루프 실행 중 오류: {str(e)}")
            
            # 오류 이벤트 발행
            asyncio.create_task(self.publish_system_event_sync(
                EVENT_TYPES["ERROR_EVENT"],
                error_type="message_loop_fatal_error",
                message=str(e),
                severity="critical"
            ))

    # 5. 이벤트 발행 단계 (수정됨)
    def _handle_error(self, symbol: str, error: str) -> None:
        """
        오류 처리
        
        Args:
            symbol: 관련 심볼
            error: 오류 메시지
        """
        self.log_error(f"{symbol} 오류: {error}")
            
        # 오류 이벤트 발행
        asyncio.create_task(self.publish_system_event_sync(
            EVENT_TYPES["ERROR_EVENT"],
            symbol=symbol,
            error_type="subscription_error",
            message=error,
            severity="error"
        ))
    
    def _update_metrics(self, exchange_code: str, start_time: float, message_type: str, data: Any) -> None:
        """
        메트릭 업데이트
        
        Args:
            exchange_code: 거래소 코드
            start_time: 메시지 처리 시작 시간
            message_type: 메시지 타입 ('snapshot' 또는 'delta')
            data: 메시지 데이터
        """
        try:
            # 처리 시간 계산 (밀리초)
            processing_time = (time.time() - start_time) * 1000
            
            # 크기 추정
            data_size = self._estimate_data_size(data)
            
            # 시스템 이벤트 발행 - 처리 시간
            asyncio.create_task(self.publish_system_event_sync(
                EVENT_TYPES["METRIC_UPDATE"],
                metric_name="processing_time",
                value=processing_time,
                message_type=message_type
            ))
            
            # 시스템 이벤트 발행 - 데이터 크기
            asyncio.create_task(self.publish_system_event_sync(
                EVENT_TYPES["METRIC_UPDATE"],
                metric_name="data_size",
                value=data_size,
                message_type=message_type
            ))
            
            # 메시지 타입별 카운트 이벤트 발행
            metric_name = f"{message_type}_count" if message_type in ["snapshot", "delta"] else "message_count"
            asyncio.create_task(self.publish_system_event_sync(
                EVENT_TYPES["METRIC_UPDATE"],
                metric_name=metric_name,
                value=1
            ))
                
        except Exception as e:
            self.log_error(f"메트릭 업데이트 중 오류: {str(e)}")
    
    def publish_event(self, symbol: str, data: Any, event_type: str) -> None:
        """
        오더북 이벤트 발행
        
        Args:
            symbol: 심볼
            data: 이벤트 데이터
            event_type: 이벤트 타입 ('snapshot', 'delta', 'error')
        """
        try:
            # 이벤트 타입에 따라 적절한 이벤트 버스 이벤트 타입 선택
            if event_type == "snapshot":
                bus_event_type = EVENT_TYPES["DATA_SNAPSHOT"]
            elif event_type == "delta":
                bus_event_type = EVENT_TYPES["DATA_DELTA"]
            elif event_type == "error":
                bus_event_type = EVENT_TYPES["ERROR_EVENT"]
            else:
                bus_event_type = EVENT_TYPES["DATA_UPDATED"]
            
            # 이벤트 버스를 통해 이벤트 발행 (동기식)
            self.event_bus.publish_sync(
                bus_event_type,
                symbol=symbol,
                exchange_code=self.exchange_code,
                data=data,
                timestamp=time.time()
            )
                
        except Exception as e:
            self.log_error(f"이벤트 발행 중 오류: {str(e)}")
    
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
            await self._publish_subscription_status_event(symbols, "unsubscribed")
            
            self.log_info("모든 심볼 구독 취소 완료")
            
            # 웹소켓 객체 초기화
            self.ws = None
            
            # 태스크 정리
            await self._cancel_tasks()
            
            self.log_info("모든 자원 정리 완료")
            return True
            
        except Exception as e:
            self.log_error(f"구독 취소 중 오류 발생: {e}")
            # 비동기 함수인 publish_system_event_sync를 asyncio.create_task로 감싸서 호출
            asyncio.create_task(self.publish_system_event_sync(
                EVENT_TYPES["ERROR_EVENT"],
                message=f"구독 취소 중 오류 발생: {e}"
            ))
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
    
    def log_error(self, msg: str) -> None:
        """오류 메시지 로깅"""
        self.logger.error(f"{self.exchange_kr} {msg}")
        
        # 오류 이벤트 발행 (간소화된 방식)
        try:
            self.system_event_manager.record_metric(self.exchange_code, "error_count")
        except Exception:
            pass  # 이벤트 발행 실패는 무시

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
        구독 상태 이벤트 발행
        
        Args:
            symbols: 심볼 리스트
            status: 구독 상태 ('subscribed' 또는 'unsubscribed')
        """
        try:
            # asyncio.create_task를 사용하여 비동기 함수를 안전하게 호출
            asyncio.create_task(self.publish_system_event_sync(
                EVENT_TYPES["SUBSCRIPTION_STATUS"],
                status=status,
                symbols=symbols,
                count=len(symbols)
            ))
                
        except Exception as e:
            self.log_error(f"구독 상태 이벤트 발행 중 오류: {str(e)}")

    async def publish_system_event(self, event_type: str, **data) -> None:
        """
        시스템 이벤트 발행 (비동기)
        
        Args:
            event_type: 이벤트 타입 (EVENT_TYPES 상수 사용)
            **data: 이벤트 데이터
        """
        # exchange_code 필드가 없으면 추가
        if "exchange_code" not in data:
            data["exchange_code"] = self.exchange_code
            
        # system_event_manager의 비동기 publish_system_event 메서드 호출
        await self.system_event_manager.publish_system_event(event_type, **data)
    
    async def publish_system_event_sync(self, event_type: str, **data) -> None:
        """
        시스템 이벤트 발행 (동기식)
        
        과거에는 비동기 컨텍스트 외부에서도 사용할 수 있었지만,
        이제는 비동기 함수로 변경되었습니다. 이름은 호환성을 위해 유지합니다.
        
        Args:
            event_type: 이벤트 타입 (EVENT_TYPES 상수 사용)
            **data: 이벤트 데이터
        """
        # exchange_code 필드가 없으면 추가
        if "exchange_code" not in data:
            data["exchange_code"] = self.exchange_code
            
        # 이벤트 버스를 통해 비동기로 이벤트 발행 (이전에는 동기식이었음)
        await self.event_bus.publish_sync(event_type, **data)

    def _start_metric_update_task(self) -> None:
        """메트릭 업데이트 태스크 시작"""
        try:
            # 비동기 태스크 생성
            async def update_metrics():
                try:
                    while not self.stop_event.is_set():
                        await asyncio.sleep(self._count_update_interval)
                        self._update_message_metrics()
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    self.log_error(f"메트릭 업데이트 태스크 오류: {str(e)}")
            
            # 태스크 생성 및 저장
            self.tasks["metric_update"] = asyncio.create_task(update_metrics())
            
        except RuntimeError:
            # 이벤트 루프가 없는 경우 무시
            pass
    
    def _update_message_metrics(self) -> None:
        """메시지 메트릭 업데이트 (차이와 속도 계산)"""
        try:
            current_time = time.time()
            time_diff = current_time - self._last_count_update
            
            if time_diff == 0:
                return
                
            # 메시지 수 변화량 계산
            count_diff = self._message_count - self._prev_message_count
            
            # 초당 메시지 속도 계산
            messages_per_second = count_diff / time_diff
            
            # 메트릭 업데이트 - 전체 카운트와 속도를 직접 설정
            metrics = self.system_event_manager.metrics[self.exchange_code]
            
            # 총 메시지 수 업데이트
            metrics["message_count"] = self._message_count
            
            # 메시지 속도 업데이트
            metrics["message_rate"] = messages_per_second
            
            # 첫 메시지 시간이 없는 경우 설정 (첫 메시지가 있을 때)
            if metrics.get("first_message_time") is None and self._message_count > 0:
                metrics["first_message_time"] = self._last_count_update
            
            # 이전 카운트 업데이트
            self._prev_message_count = self._message_count
            self._last_count_update = current_time
        
        except Exception as e:
            self.log_error(f"메트릭 업데이트 중 오류: {str(e)}")
    
    def increment_message_count(self, n: int = 1) -> None:
        """
        메시지 카운트 증가
        
        Args:
            n: 증가시킬 값 (기본값: 1)
        """
        # 메시지 카운트 증가
        self._message_count += n

