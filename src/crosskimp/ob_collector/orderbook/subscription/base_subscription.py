"""
거래소 구독 관리 모듈의 기본 클래스
"""

import asyncio
import time
import random
import json
import os
from typing import Dict, List, Any, Optional, Set, Tuple, Union
from abc import ABC, abstractmethod
import websockets
import datetime

from crosskimp.common.logger.logger import get_unified_logger, create_raw_logger
from crosskimp.common.config.common_constants import Exchange, EXCHANGE_NAMES_KR, normalize_exchange_code, SystemComponent
from crosskimp.common.config.app_config import AppConfig, get_config
from crosskimp.common.events.system_eventbus import SystemEventBus, EventType

from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator

# 로거 설정
logger = get_unified_logger(component=SystemComponent.ORDERBOOK.value)

class BaseSubscription(ABC):
    def __init__(self, connection: BaseWebsocketConnector, exchange_code: str = None, on_data_received=None):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
            exchange_code: 거래소 코드 (None이면 connection에서 가져옴)
            on_data_received: 데이터 수신 시 호출될 콜백 함수
        """
        self.connection = connection
        self.exchange_code = normalize_exchange_code(exchange_code or self.connection.exchangename)
        self.logger = get_unified_logger(component=SystemComponent.ORDERBOOK.value)
        self.exchange_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, self.exchange_code)  # 한글 거래소명 가져오기
        
        # 거래소별 하위 폴더 생성하지 않음
        
        # 웹소켓 및 이벤트 관련 초기화
        self.ws = None
        
        # 시스템 이벤트 버스 가져오기 (메트릭 발행용으로 유지)
        self.sys_event_bus = self._get_sys_event_bus()
        
        # 구독 및 태스크 관리
        self.subscribed_symbols = {}
        self.subscription_status = {}  # 구독 상태 저장용 딕셔너리 추가
        self.tasks = {}
        self.message_loop_task = None
        self.stop_event = asyncio.Event()
        
        # 메시지 카운트 관련 변수
        self._message_count = 0
        self._last_count_update = time.time()
        self._prev_message_count = 0
        self._count_update_interval = 1.0
        self._start_metric_update_task()
        
        # 메시지 로깅 및 검증 관련 설정
        self.raw_logging_enabled = False
        self.orderbook_logging_enabled = False
        self.raw_logger = None
        self.validator = None
        self._setup_raw_logging()
        self.initialize_validator()
        
        # 상태 추적 변수들
        self.message_count = {}
        self.last_sequence = {}
        self.orderbooks = {}
        self.last_timestamps = {}
        self.output_depth = 10
        
        # 메시지 로깅 활성화 여부
        self.message_logging_enabled = os.environ.get('DEBUG_MESSAGE_LOGGING', '0') == '1'
        
        # 데이터 수신 콜백 함수
        self.on_data_received = on_data_received

    # 로깅 메서드
    def log_debug(self, message: str) -> None:
        """디버그 로그 출력"""
        # self.logger.debug(f"{self.exchange_kr} {message}")

    def log_info(self, message: str) -> None:
        """정보 로그 출력"""
        self.logger.info(f"{self.exchange_kr} {message}")

    def log_warning(self, message: str) -> None:
        """경고 로그 출력"""
        self.logger.warning(f"{self.exchange_kr} {message}")

    def log_error(self, message: str, exc_info: bool = False) -> None:
        """오류 로그 출력"""
        self.logger.error(f"{self.exchange_kr} {message}", exc_info=exc_info)

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
            self.log_debug(f"검증기 초기화 완료")
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
        # 이미 연결되어 있는지 확인
        if self.is_connected:
            # 웹소켓 객체도 확인
            self.ws = await self.connection.get_websocket()
            if self.ws:
                return True
            else:
                # 연결 상태와 웹소켓 객체 불일치 - 연결 상태 초기화
                self.log_warning("연결 상태와 웹소켓 객체 불일치, 연결 재시도")
                # self.connection.is_connected = False  # 이 부분은 손대지 않기로 함
        
        # 연결이 아직 안 되었거나 오류가 있는 경우
        self.log_info("웹소켓 연결 확보 시도")
        
        # 웹소켓 연결 시도
        try:
            # 연결 요청. 타임아웃은 2초로 설정 (더 넉넉하게)
            connection_started = await self.connection.connect_with_timeout(timeout=2.0)
            
            # 연결 완료 대기
            retry_count = 0
            max_retries = 0  # 0으로 설정하면 무제한 대기
            while not self.is_connected and (max_retries == 0 or retry_count < max_retries):
                retry_count += 1
                await asyncio.sleep(0.5)  # 0.5초씩 대기
                
                # 로그 메시지는 4번째마다 출력 (2초마다)
                if retry_count % 4 == 0:
                    self.log_debug(f"웹소켓 연결 대기 중... ({retry_count}회)")
            
            # 재시도 후에도 연결 안 됨 (max_retries가 0이 아닌 경우에만)
            if max_retries > 0 and not self.is_connected and retry_count >= max_retries:
                self.log_error(f"웹소켓 연결 타임아웃 ({max_retries * 0.5}초)")
                return False
                
        except Exception as e:
            self.log_error(f"웹소켓 연결 시도 중 오류: {str(e)}")
            return False
        
        # 웹소켓 객체 가져오기 시도
        try:
            # 연결 확인 후 웹소켓 객체 가져오기
            self.ws = await self.connection.get_websocket()
            
            if not self.ws:
                self.log_error("웹소켓 객체를 가져올 수 없음 (연결은 완료됨)")
                return False
            
            self.log_info("웹소켓 연결 및 객체 확보 완료")
            return True
            
        except Exception as e:
            self.log_error(f"웹소켓 연결 확보 중 오류: {str(e)}")
            return False
            
    async def send_message(self, message: str) -> bool:
        """웹소켓으로 메시지 전송"""
        try:
            # 웹소켓 연결 상태 확인 및 확보
            if not await self._ensure_websocket():
                self.log_error("웹소켓 연결이 없어 메시지 전송 실패")
                return False
            
            # 메시지 전송 직전에 웹소켓 객체 재확인 (방어적 코딩)
            if not self.ws:
                self.log_error("웹소켓 객체가 없어 메시지 전송 실패")
                return False
                
            # 메시지 전송
            await self.ws.send(message)
            msg_preview = message[:100] + "..." if len(message) > 100 else message
            self.log_debug(f"메시지 전송 성공: {msg_preview}")
            return True
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # 간결한 오류 메시지로 로깅
            self.log_error(f"메시지 전송 실패: {str(e)}")
            
            # 웹소켓 객체 초기화 및 연결 상태 업데이트
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
    
    async def subscribe(self, symbol):
        """
        기본 구독 로직 제공 (자식 클래스에서 재정의 가능)
        """
        try:
            # 웹소켓 연결 확보 (최대 15초 대기)
            if not await self._ensure_websocket():
                self.log_error("웹소켓 연결이 없어 구독 실패")
                return False
                
            # 심볼 목록 전처리
            symbols = await self._preprocess_symbols(symbol)
            if not symbols:
                self.log_warning("구독할 심볼이 없습니다.")
                return False
                
            # 구독 메시지 생성
            subscribe_message = await self.create_subscribe_message(symbols)
            if not subscribe_message:
                self.log_error("구독 메시지 생성 실패")
                return False
                
            # 구독 메시지 전송
            message_str = json.dumps(subscribe_message)
            success = await self.send_message(message_str)
            
            if not success:
                self.log_error(f"구독 메시지 전송 실패: {symbols}")
                return False
                
            # 구독 상태 업데이트
            for sym in symbols:
                self.subscribed_symbols[sym] = True
                self.subscription_status[sym] = "subscribed"
                
            self.log_info(f"심볼 구독 성공: {symbols}")
            return True
            
        except Exception as e:
            self.log_error(f"구독 중 오류 발생: {str(e)}")
            return False
    
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
        """
        메시지 수신 기본 처리
        
        Args:
            message: 수신된 메시지
        """
        try:
            # 메시지 처리 시작 시간
            start_time = time.time()
            
            # 로깅 (디버그 모드이고 메시지 로깅이 활성화된 경우에만)
            if self.message_logging_enabled:
                self.log_debug(f"메시지 수신: {message[:100]}...")
            
            # 로우 메시지 로깅 (설정된 경우)
            self.log_raw_message(message)
            
            # 메시지 수신 정보를 connector에 전달하여 last_message_time을 일관되게 유지
            if self.connection:
                await self.connection.handle_message("websocket", len(message))
            
            # 메시지 카운트 증가 및 메트릭 업데이트
            self._message_count += 1  # 로컬 카운트 증가 (메트릭 태스크에서 사용)
            self._update_message_metrics(1)  # 중앙 메트릭 관리자 업데이트
            
            # 메시지 디코딩 및 처리
            # 이 부분은 각 구독 클래스에서 구현해야 함
            
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {str(e)}")
    
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
                    await asyncio.sleep(0.1)
            
            self.log_info("메시지 수신 루프 종료")
            
        except Exception as e:
            self.log_error(f"메시지 루프 실행 중 오류: {str(e)}")
            
            # 시스템 이벤트 발행으로 변경
            asyncio.create_task(self.sys_event_bus.publish(
                EventType.SYSTEM_EVENT,
                {
                    "exchange_code": self.exchange_code,
                    "event_type": "message_loop_fatal_error",
                    "message": str(e),
                    "severity": "critical"
                }
            ))

    def _handle_error(self, symbol: str, error: str) -> None:
        """오류 처리"""
        self.log_error(f"심볼 {symbol} 오류: {error}")
        
        # 오류 카운트 메트릭 업데이트
        self._update_metrics("error_count", 1, op="increment", error_type="subscription_error", message=error)
            
        # 시스템 이벤트 발행으로 변경
        asyncio.create_task(self.sys_event_bus.publish(
            EventType.SYSTEM_EVENT,
            {
                "exchange_code": self.exchange_code,
                "event_type": "subscription_error",
                "message": error,
                "severity": "error"
            }
        ))
    
    async def handle_metric_update(self, metric_name: str, value: float = 1.0, data: Dict = None) -> None:
        """
        메트릭 업데이트 처리 (event_handler.handle_metric_update 대체)
        
        Args:
            metric_name: 메트릭 이름
            value: 메트릭 값
            data: 추가 데이터 (딕셔너리)
        """
        # 허용된 메트릭 목록
        allowed_metrics = [
            "message_count", 
            "error_count", 
            "subscription_count", 
            "last_message_time", 
            "message_rate"
        ]
        
        try:
            # 허용된 메트릭만 업데이트
            if metric_name in allowed_metrics:
                metric_data = {
                    "exchange_code": self.exchange_code,
                    "metric_name": metric_name,
                    "value": value
                }
                
                # 추가 데이터가 있으면 병합
                if data:
                    metric_data.update(data)
                
                # 시스템 이벤트 버스로 메트릭 업데이트 이벤트 발행
                await self.sys_event_bus.publish(
                    EventType.STATUS_UPDATE,
                    metric_data
                )
        except Exception as e:
            self.log_error(f"메트릭 업데이트 처리 중 오류: {str(e)}")

    def _update_metrics(self, metric_name: str, value: float = 1.0, **kwargs) -> None:
        """
        메트릭 업데이트 (이벤트 버스 사용)
        
        허용된 메트릭:
        - message_count: 수신한 총 메시지 수
        - error_count: 발생한 오류 수
        - subscription_count: 구독 중인 심볼 수
        - last_message_time: 마지막 메시지 수신 시간
        - message_rate: 초당 메시지 수
        
        Args:
            metric_name: 메트릭 이름
            value: 메트릭 값
            **kwargs: 추가 데이터
        """
        # 허용된 메트릭 목록
        allowed_metrics = [
            "message_count", 
            "error_count", 
            "subscription_count", 
            "last_message_time", 
            "message_rate"
        ]
        
        try:
            # 허용된 메트릭이면 업데이트
            if metric_name in allowed_metrics:
                # 비동기 함수가 아니므로 새 태스크로 처리
                metric_data = {
                    "exchange_code": self.exchange_code,
                    "metric_name": metric_name,
                    "value": value
                }
                
                # 추가 데이터가 있으면 병합
                if kwargs:
                    metric_data.update(kwargs)
                
                # 새 태스크로 전송 (비동기 함수를 동기 함수에서 호출)
                asyncio.create_task(self.sys_event_bus.publish(
                    EventType.STATUS_UPDATE,
                    metric_data
                ))
        except Exception as e:
            self.log_error(f"메트릭 업데이트 중 오류: {str(e)}")
    
    def _update_message_metrics(self, msg_count: int = 1) -> None:
        """
        메시지 관련 메트릭 업데이트
        
        이벤트 버스를 통해 메트릭 업데이트
        
        Args:
            msg_count: 메시지 개수
        """
        try:
            # 메시지 카운트 증가
            self._update_metrics("message_count", msg_count, op="increment")
            
            # 마지막 메시지 시간 업데이트
            self._update_metrics("last_message_time", time.time())
        except Exception as e:
            self.log_error(f"메시지 메트릭 업데이트 중 오류: {str(e)}")
    
    def increment_message_count(self) -> None:
        """메시지 카운트 증가"""
        try:
            self._message_count += 1
            self._update_message_metrics(1)
        except Exception as e:
            self.log_error(f"메시지 카운트 증가 중 오류: {str(e)}")
    
    def publish_event(self, symbol: str, data: Any, event_type: str) -> None:
        """
        이벤트 발행
        
        Args:
            symbol: 심볼명
            data: 이벤트 데이터
            event_type: 이벤트 타입
        """
        # 이벤트 발행 (event_bus 사용)
        if hasattr(self, 'sys_event_bus') and self.sys_event_bus:
            # 비동기 컨텍스트에서 호출되므로 create_task 사용
            asyncio.create_task(self.sys_event_bus.publish(
                event_type, 
                {
                    "exchange_code": self.exchange_code,
                    "symbol": symbol,
                    "data": data,
                    "timestamp": time.time()
                }
            ))
        
        # 데이터 수신 콜백 호출 (추가)
        if self.on_data_received:
            asyncio.create_task(self.on_data_received(symbol, data, event_type))
    
    async def publish_error_event(self, error_message: str, error_type: str) -> None:
        """오류 이벤트 발행"""
        try:
            await self.sys_event_bus.publish(
                EventType.SYSTEM_EVENT,
                {
                    "exchange_code": self.exchange_code,
                    "event_type": error_type,
                    "message": error_message,
                    "severity": "error"
                }
            )
        except Exception as e:
            self.log_warning(f"오류 이벤트 발행 실패: {str(e)}")
    
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
        self.log_info(f"심볼 {symbol} 구독 취소 메시지 생성")
        return {}
    
    async def unsubscribe(self, symbol: Optional[str] = None) -> bool:
        """모든 심볼 구독 취소"""
        try:
            self.stop_event.set()
            await self._cancel_message_loop()
            
            symbols = list(self.subscribed_symbols.keys()) if symbol is None else [symbol]
            
            for sym in symbols:
                unsubscribe_message = await self.create_unsubscribe_message(sym)
                try:
                    if self.ws:
                        await self.send_message(json.dumps(unsubscribe_message))
                except Exception as e:
                    self.log_warning(f"심볼 {sym} 구독 취소 메시지 전송 실패: {e}")
                
                self._cleanup_subscription(sym)
                
                # 구독 상태 업데이트 - subscription_status가 있는 경우에만 실행
                if hasattr(self, 'subscription_status'):
                    self.subscription_status[sym] = "unsubscribed"
            
            if symbols:
                try:
                    await self._publish_subscription_status_event(symbols, "unsubscribed")
                except Exception as e:
                    self.log_warning(f"구독 취소 이벤트 발행 실패: {e}")
            
            self.log_info(f"{len(symbols)}개 심볼 구독 취소 완료")
            
            # 모든 심볼 구독 취소인 경우에만 전체 정리
            if symbol is None:
                self.ws = None
                await self._cancel_tasks()
                self.log_info("모든 자원 정리 완료")
                
            return True
            
        except Exception as e:
            self.log_error(f"구독 취소 중 오류 발생: {e}")
            
            # 오류 메트릭 업데이트
            self._update_metrics("error_count", 1, op="increment", error_type="unsubscribe_error", error_message=str(e))
            
            return False
    
    def _cleanup_subscription(self, symbol: str) -> None:
        """구독 관련 상태 정보 정리"""
        self.log_debug(f"심볼 {symbol} 구독 상태 정보 정리")
        # 관련 상태 변수 초기화
        self.message_count.pop(symbol, None)
        self.last_sequence.pop(symbol, None)
        self.last_timestamps.pop(symbol, None)
        self.orderbooks.pop(symbol, None)
        
        # 메트릭 초기화 - 심볼별 메트릭 초기화
        self._update_metrics("subscription_count", -1, op="decrement")
    
    async def _publish_subscription_status_event(self, symbols: List[str], status: str) -> None:
        """
        구독 상태 이벤트 발행 (이전 코드와의 호환성을 위한 메서드)
        
        Args:
            symbols: 심볼 목록
            status: 구독 상태 (subscribed/unsubscribed)
        """
        # 시스템 이벤트 발행으로 변경
        try:
            await self.sys_event_bus.publish(
                EventType.SYSTEM_EVENT,
                {
                    "exchange_code": self.exchange_code,
                    "event_type": "subscription_status",
                    "status": status,
                    "symbols": symbols
                }
            )
        except Exception as e:
            self.log_warning(f"구독 상태 이벤트 발행 실패: {e}")
    
    async def publish_system_event(self, event_type: str, **data) -> None:
        """시스템 이벤트 발행"""
        try:
            if "exchange_code" not in data:
                data["exchange_code"] = self.exchange_code
                
            # 시스템 이벤트 발행으로 변경
            await self.sys_event_bus.publish(
                EventType.SYSTEM_EVENT,
                {
                    "exchange_code": self.exchange_code,
                    "event_type": event_type,
                    **data
                }
            )
        except Exception as e:
            self.log_warning(f"이벤트({event_type}) 발행 중 오류: {str(e)}")

    def _start_metric_update_task(self) -> None:
        """
        메트릭 업데이트 태스크 시작
        
        주기적으로 메시지 속도 메트릭을 업데이트합니다.
        """
        try:
            async def update_metrics():
                # 메트릭 업데이트 초기값 설정
                self._message_count = 0
                self._prev_message_count = 0
                self._last_count_update = time.time()
                
                while True:
                    try:
                        await asyncio.sleep(1.0)  # 1초마다 업데이트
                        
                        # 현재 시점의 메트릭 정보 가져오기
                        current_time = time.time()
                        time_diff = current_time - self._last_count_update
                        
                        # 메시지 카운트 차이 및 속도 계산
                        count_diff = self._message_count - self._prev_message_count
                        
                        if time_diff > 0:
                            messages_per_second = count_diff / time_diff
                            
                            # 메시지 속도 메트릭 업데이트
                            if count_diff > 0:
                                self._update_metrics(
                                    "message_rate",
                                    messages_per_second
                                )
                            
                            # 구독 중인 심볼 수 메트릭 추가
                            self._update_metrics(
                                "subscription_count",
                                len(self.subscribed_symbols)
                            )
                            
                            # 상태 업데이트
                            self._prev_message_count = self._message_count
                            self._last_count_update = current_time
                            
                    except asyncio.CancelledError:
                        self.log_debug("메트릭 업데이트 태스크 취소됨")
                        break
                    except Exception as e:
                        self.log_error(f"메트릭 업데이트 태스크 오류: {str(e)}")
            
            # 이벤트 루프에 태스크 추가
            self.metrics_task = asyncio.create_task(update_metrics())
            self.metrics_task.set_name(f"metrics_task_{self.exchange_code}")
            
        except RuntimeError:
            # 이벤트 루프가 실행 중이 아닌 경우
            self.log_debug("이벤트 루프가 실행 중이 아니므로 메트릭 업데이트 태스크를 시작할 수 없습니다.")

    def _get_sys_event_bus(self) -> SystemEventBus:
        """
        시스템 이벤트 버스 인스턴스 반환
        
        Note:
            크로스킴프 시스템 전체에서 사용하는 간소화된 이벤트 버스 반환
        
        Returns:
            SystemEventBus: 글로벌 이벤트 버스 인스턴스
        """
        from crosskimp.common.events import get_event_bus
        return get_event_bus()

