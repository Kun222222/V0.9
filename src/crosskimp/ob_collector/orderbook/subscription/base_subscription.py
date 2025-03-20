import asyncio
import time
from abc import ABC, abstractmethod
from typing import Dict, Optional, List, Any, Union
import websockets
import json
import datetime
import os

from crosskimp.logger.logger import get_unified_logger, create_raw_logger
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
from crosskimp.config.constants_v3 import EXCHANGE_NAMES_KR, LOG_SUBDIRS, normalize_exchange_code
from crosskimp.ob_collector.orderbook.util.event_bus import EventBus, EVENT_TYPES
from crosskimp.ob_collector.orderbook.util.event_handler import EventHandler, LoggingMixin
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator

class BaseSubscription(ABC, LoggingMixin):
    def __init__(self, connection: BaseWebsocketConnector, exchange_code: str = None):
        """기본 구독 클래스 초기화"""
        self.connection = connection
        self.exchange_code = normalize_exchange_code(exchange_code or self.connection.exchangename)
        self.setup_logger(self.exchange_code)
        self.exchange_kr = self.exchange_name_kr  # 이전 코드와 호환성 유지
        
        # 로깅 디렉토리 생성
        raw_data_dir = os.path.join(LOG_SUBDIRS['raw_data'], self.exchange_code)
        os.makedirs(raw_data_dir, exist_ok=True)
        
        # 웹소켓 및 이벤트 관련 초기화
        self.ws = None
        self.event_handler = EventHandler.get_instance(self.exchange_code, connection.settings)
        self.event_bus = self.event_handler.event_bus
        
        # 구독 및 태스크 관리
        self.subscribed_symbols = {}
        self.tasks = {}
        self.message_loop_task = None
        self.stop_event = asyncio.Event()
        
        # 메시지 카운트 관련 변수
        self._message_count = 0
        self._last_count_update = time.time()
        self._prev_message_count = 0
        self._count_update_interval = 1.0
        self._start_metric_update_task()
        
        # 로깅 및 검증 관련 설정
        self.raw_logging_enabled = False
        self.orderbook_logging_enabled = False
        self.raw_logger = None
        self.validator = None
        self._setup_raw_logging()
        self.initialize_validator()
        
        # 상태 추적 변수들
        self.last_message_time = {}
        self.message_count = {}
        self.last_sequence = {}
        self.processing_times = []
        self.orderbooks = {}
        self.last_timestamps = {}
        self.output_depth = 10
        
    def _setup_raw_logging(self):
        """Raw 데이터 로깅 설정"""
        try:
            self.raw_logger = create_raw_logger(self.exchange_code)
            self.log_info("raw 로거 초기화 완료")
        except Exception as e:
            self.log_error(f"Raw 로깅 설정 실패: {str(e)}")
            self.raw_logging_enabled = False
            self.orderbook_logging_enabled = False
    
    def initialize_validator(self) -> None:
        """거래소 코드에 맞는 검증기 초기화"""
        try:
            self.validator = BaseOrderBookValidator(self.exchange_code)
            self.log_debug(f"{self.exchange_kr} 검증기 초기화 완료")
        except ImportError:
            self.log_error("BaseOrderBookValidator를 가져올 수 없습니다. 검증 기능이 비활성화됩니다.")
            self.validator = None
    
    def set_validator(self, validator) -> None:
        """검증기 설정"""
        self.validator = validator
        
    @property
    def is_connected(self) -> bool:
        """연결 상태 확인"""
        return self.connection.is_connected
    
    async def _ensure_websocket(self) -> bool:
        """웹소켓 연결 확보"""
        if not self.is_connected:
            self.log_info("웹소켓 연결 확보 시도")
        
        try:
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
        """웹소켓으로 메시지 전송"""
        try:
            if not await self._ensure_websocket():
                self.log_error("웹소켓 연결이 없어 메시지 전송 실패")
                return False
                
            await self.ws.send(message)
            return True
        except Exception as e:
            self.log_error(f"메시지 전송 실패: {str(e)}")
            self.ws = None
            return False
    
    async def receive_message(self) -> Optional[str]:
        """웹소켓에서 메시지 수신"""
        try:
            if not self.is_connected:
                self.log_error("메시지 수신 실패: 웹소켓이 연결되지 않음")
                return None
                
            ws = await self.connection.get_websocket()
            if not ws:
                self.log_error("메시지 수신 실패: 웹소켓 객체가 없음")
                return None
                
            message = await ws.recv()
            self.increment_message_count()
            
            return message
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log_error(f"메시지 수신 중 오류: {str(e)}")
            
            asyncio.create_task(self.event_handler.handle_error(
                error_type="message_error",
                message=str(e),
                severity="error"
            ))
            
            return None
    
    async def _preprocess_symbols(self, symbol: Union[str, List[str]]) -> List[str]:
        """심볼 전처리"""
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
        """구독 메시지 생성 (자식 클래스에서 구현)"""
        pass
    
    @abstractmethod
    async def subscribe(self, symbol):
        """심볼 구독 (자식 클래스에서 구현)"""
        pass
    
    def log_raw_message(self, message: str) -> None:
        """원시 메시지 로깅"""
        if not self.raw_logging_enabled or not self.raw_logger:
            return
            
        try:
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            self.raw_logger.debug(f"[{current_time}] {message}")
        except Exception as e:
            self.log_error(f"원시 메시지 로깅 실패: {str(e)}")
    
    def log_orderbook_data(self, symbol: str, orderbook: Dict) -> None:
        """오더북 데이터 로깅"""
        try:
            if not self.orderbook_logging_enabled or not self.raw_logger:
                return
                
            current_time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
            bids = orderbook.get("bids", [])[:10]
            asks = orderbook.get("asks", [])[:10]
            
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
        """메시지 수신 처리"""
        try:
            start_time = time.time()
            self.log_raw_message(message)
            
            processing_time_ms = (time.time() - start_time) * 1000
            
            asyncio.create_task(self.event_handler.handle_metric_update(
                metric_name="processing_time",
                value=processing_time_ms
            ))
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {str(e)}")
            
            asyncio.create_task(self.event_handler.handle_error(
                error_type="message_processing_error",
                message=str(e),
                severity="error"
            ))
    
    async def message_loop(self) -> None:
        """메시지 수신 루프"""
        self.log_info("메시지 수신 루프 시작")
        
        try:
            if not await self._ensure_websocket():
                self.log_error("메시지 루프 시작 실패: 웹소켓 연결 없음")
                return
                
            while not self.stop_event.is_set():
                try:
                    message = await self.receive_message()
                    if not message:
                        await asyncio.sleep(0.1)
                        continue
                    
                    await self._on_message(message)
                    
                except asyncio.CancelledError:
                    self.log_debug("메시지 루프 취소됨")
                    break
                except Exception as e:
                    self.log_error(f"메시지 처리 중 예외 발생: {str(e)}")
                    
                    asyncio.create_task(self.event_handler.handle_error(
                        error_type="message_loop_error",
                        message=str(e),
                        severity="error"
                    ))
                    
                    await asyncio.sleep(0.1)
            
            self.log_info("메시지 수신 루프 종료")
            
        except Exception as e:
            self.log_error(f"메시지 루프 실행 중 오류: {str(e)}")
            
            asyncio.create_task(self.event_handler.handle_error(
                error_type="message_loop_fatal_error",
                message=str(e),
                severity="critical"
            ))

    def _handle_error(self, symbol: str, error: str) -> None:
        """오류 처리"""
        self.log_error(f"{symbol} 오류: {error}")
            
        asyncio.create_task(self.event_handler.handle_error(
            error_type="subscription_error",
            message=error,
            severity="error"
        ))
    
    def _update_metrics(self, exchange_code: str, start_time: float, message_type: str, data: Any) -> None:
        """
        메트릭 업데이트 처리
        
        Args:
            exchange_code: 거래소 코드
            start_time: 시작 시간
            message_type: 메시지 타입 ('snapshot', 'delta' 등)
            data: 메시지 데이터
        """
        try:
            # 메시지 카운트 증가
            self.event_handler.increment_message_count()
            
            # 처리 시간 계산 (밀리초)
            processing_time = (time.time() - start_time) * 1000
            
            # 데이터 크기 추정
            data_size = self._estimate_data_size(data)
            
            # 이벤트 핸들러로 메트릭 업데이트
            asyncio.create_task(
                self.event_handler.handle_metric_update(
                    metric_name="processing_time",
                    value=processing_time,
                    data={"message_type": message_type}
                )
            )
            
            asyncio.create_task(
                self.event_handler.handle_metric_update(
                    metric_name="data_size",
                    value=data_size,
                    data={"message_type": message_type}
                )
            )
            
            # 메시지 타입별 카운트 업데이트
            metric_name = f"{message_type}_count" if message_type in ["snapshot", "delta"] else "message_count"
            asyncio.create_task(
                self.event_handler.handle_metric_update(
                    metric_name=metric_name,
                    value=1
                )
            )
            
        except Exception as e:
            self.log_error(f"메트릭 업데이트 중 오류: {str(e)}")
    
    def publish_event(self, symbol: str, data: Any, event_type: str) -> None:
        """오더북 이벤트 발행"""
        try:
            if event_type == "snapshot":
                bus_event_type = EVENT_TYPES["DATA_SNAPSHOT"]
            elif event_type == "delta":
                bus_event_type = EVENT_TYPES["DATA_DELTA"]
            elif event_type == "error":
                bus_event_type = EVENT_TYPES["ERROR_EVENT"]
            else:
                bus_event_type = EVENT_TYPES["DATA_UPDATED"]
            
            if event_type == "error":
                asyncio.create_task(
                    self.event_handler.handle_error(
                        error_type="orderbook_error",
                        message=str(data),
                        symbol=symbol
                    )
                )
            else:
                asyncio.create_task(
                    self.event_handler.handle_data_event(
                        event_type=bus_event_type,
                        symbol=symbol,
                        data=data
                    )
                )
                
        except Exception as e:
            self.log_error(f"이벤트 발행 중 오류: {str(e)}")
    
    def _estimate_data_size(self, data: Dict) -> int:
        """데이터 크기 추정"""
        return 0
    
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
        """구독 취소 메시지 생성"""
        self.log_info(f"[{self.exchange_code}] 구독 취소 메시지 생성 (기본 구현)")
        return {}
    
    async def unsubscribe(self, symbol: Optional[str] = None) -> bool:
        """모든 심볼 구독 취소"""
        try:
            self.stop_event.set()
            await self._cancel_message_loop()
            
            symbols = list(self.subscribed_symbols.keys())
            
            for sym in symbols:
                unsubscribe_message = await self.create_unsubscribe_message(sym)
                try:
                    if self.ws:
                        await self.send_message(json.dumps(unsubscribe_message))
                except Exception as e:
                    self.log_warning(f"{sym} 구독 취소 메시지 전송 실패: {e}")
                
                self._cleanup_subscription(sym)
            
            if symbols:
                try:
                    await self._publish_subscription_status_event(symbols, "unsubscribed")
                except Exception as e:
                    self.log_warning(f"구독 취소 이벤트 발행 실패: {e}")
            
            self.log_info("모든 심볼 구독 취소 완료")
            
            self.ws = None
            await self._cancel_tasks()
            
            self.log_info("모든 자원 정리 완료")
            return True
            
        except Exception as e:
            self.log_error(f"구독 취소 중 오류 발생: {e}")
            if hasattr(self, 'event_handler') and self.event_handler is not None:
                try:
                    asyncio.create_task(self.event_handler.handle_error(
                        error_type="subscription_error",
                        message=f"구독 취소 중 오류 발생: {e}",
                        severity="error"
                    ))
                except Exception:
                    pass
            return False
    
    def _cleanup_subscription(self, symbol: str) -> None:
        """구독 관련 상태 정리"""
        self.subscribed_symbols.pop(symbol, None)
    
    async def _publish_subscription_status_event(self, symbols: List[str], status: str) -> None:
        """
        구독 상태 이벤트 발행 (이전 코드와의 호환성을 위한 메서드)
        
        Args:
            symbols: 심볼 목록
            status: 구독 상태 (subscribed/unsubscribed)
        """
        # 이벤트 핸들러의 handle_subscription_status 메서드 호출
        try:
            await self.event_handler.handle_subscription_status(status, symbols)
        except Exception as e:
            self.log_warning(f"구독 상태 이벤트 발행 실패: {e}")
    
    async def publish_system_event(self, event_type: str, **data) -> None:
        """시스템 이벤트 발행"""
        try:
            if "exchange_code" not in data:
                data["exchange_code"] = self.exchange_code
                
            # 이벤트 타입에 따라 적절한 핸들러 메서드 호출
            if event_type == EVENT_TYPES["ERROR_EVENT"]:
                await self.event_handler.handle_error(
                    data.get("error_type", "unknown"),
                    data.get("message", "알 수 없는 오류"),
                    data.get("severity", "error")
                )
            elif event_type == EVENT_TYPES["CONNECTION_STATUS"]:
                await self.event_handler.handle_connection_status(
                    data.get("status", "unknown"),
                    data.get("message", None)
                )
            elif event_type == EVENT_TYPES["METRIC_UPDATE"]:
                self.event_handler.update_metrics(
                    data.get("metric_name", "unknown"),
                    data.get("value", 1)
                )
        except Exception as e:
            self.log_warning(f"이벤트({event_type}) 발행 중 오류: {str(e)}")

    def _start_metric_update_task(self) -> None:
        """메트릭 업데이트 태스크 시작"""
        try:
            async def update_metrics():
                try:
                    while not self.stop_event.is_set():
                        await asyncio.sleep(self._count_update_interval)
                        self._update_message_metrics()
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    self.log_error(f"메트릭 업데이트 태스크 오류: {str(e)}")
            
            self.tasks["metric_update"] = asyncio.create_task(update_metrics())
            
        except RuntimeError:
            pass
    
    def _update_message_metrics(self) -> None:
        """메시지 메트릭 업데이트"""
        try:
            current_time = time.time()
            time_diff = current_time - self._last_count_update
            
            if time_diff == 0:
                return
                
            count_diff = self._message_count - self._prev_message_count
            messages_per_second = count_diff / time_diff
            
            try:
                # 메시지 속도 업데이트
                self.event_handler.update_metrics("message_rate", messages_per_second)
                self._prev_message_count = self._message_count
                self._last_count_update = current_time
            except Exception as e:
                self.log_error(f"메트릭 업데이트 중 오류: {str(e)}")
        
        except Exception as e:
            self.log_error(f"메시지 메트릭 업데이트 중 오류: {str(e)}")
    
    def increment_message_count(self, n: int = 1) -> None:
        """메시지 카운트 증가"""
        try:
            self._message_count += n
            self.event_handler.increment_message_count(n)
        except Exception as e:
            self._message_count += n

