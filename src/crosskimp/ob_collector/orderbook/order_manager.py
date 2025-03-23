"""
오더북 관리자 모듈

이 모듈은 거래소별 웹소켓 연결, 구독, 파싱, 오더북 처리의 전체 흐름을 관리합니다.
베이스 클래스들을 활용하여 중앙에서 컨트롤 타워 역할을 합니다.

흐름: connection → subscription → parser
"""

import asyncio
import time
from typing import Dict, List, Any, Optional

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.constants_v3 import Exchange, EXCHANGE_NAMES_KR
from crosskimp.common.events.sys_event_bus import SimpleEventBus, EventType

from crosskimp.ob_collector.eventbus.types import EventTypes
from crosskimp.ob_collector.eventbus.handler import get_orderbook_event_bus, EventHandler, LoggingMixin, get_event_handler
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
from crosskimp.ob_collector.orderbook.util.component_factory import create_connector, create_subscription, create_validator

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class OrderManager(LoggingMixin):
    """
    통합 오더북 관리자 클래스
    
    베이스 클래스들을 활용하여 중앙에서 컨트롤 타워 역할을 합니다.
    거래소별 특화 로직은 설정과 콜백으로 처리합니다.
    
    [v0.6 업데이트]
    중앙 메트릭 관리자와 통합되어 메트릭 데이터 관리가 일원화됩니다.
    """
    
    def __init__(self, settings: dict, exchange_code: str):
        """
        OrderManager 초기화

        Args:
            settings: 설정 정보
            exchange_code: 거래소 코드
        """
        # 설정 저장
        self.settings = settings
        
        # 거래소 정보
        self.exchange_code = exchange_code
        self.exchange_name_kr = EXCHANGE_NAMES_KR[exchange_code]
        
        # 로거 설정 (LoggingMixin 메서드 사용)
        self.setup_logger(self.exchange_code)
        
        # 이벤트 핸들러 초기화
        self.event_handler = get_event_handler(self.exchange_code, self.settings)
        
        # 중앙 메트릭 관리자 가져오기
        self.sys_event_bus = SimpleEventBus()
        
        # 컴포넌트들
        self.connector = None       # 웹소켓 연결 객체
        self.subscription = None     # 구독 객체
        self.validator = None        # 검증 객체
        
        # 구독 심볼 관리
        self.symbols = set()
        
        # 상태 관리
        self.is_running = False
        
        # 시작 시간 기록
        self.start_time = time.time()
        
        self.log_info(f"{self.exchange_name_kr} 오더북 관리자 초기화")
    
    async def initialize(self) -> bool:
        """
        초기화 - 연결 객체 및 구독 객체 생성
        
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            # 시스템 이벤트 버스 시작
            if self.sys_event_bus and hasattr(self.sys_event_bus, 'start'):
                await self.sys_event_bus.start()
                self.log_info(f"시스템 이벤트 버스 시작됨")
            
            # 초기화 이미 완료된 경우
            if self.is_running:
                self.log_info("이미 초기화되었습니다.")
                return True
                
            # 이벤트 버스 및 핸들러 초기화
            self.event_bus = get_orderbook_event_bus()
            self.event_handler = get_event_handler(self.exchange_code, self.settings)
            
            if not self.event_handler:
                self.log_error(f"이벤트 핸들러 초기화 실패")
                return False
                
            # 웹소켓 연결 객체 생성
            self.connector = create_connector(self.exchange_code, self.settings)
            if not self.connector:
                self.log_error(f"{self.exchange_name_kr} 연결 객체 생성 실패")
                return False
                
            # 구독 객체 생성
            self.subscription = create_subscription(self.exchange_code, self.connector)
            if not self.subscription:
                self.log_error(f"{self.exchange_name_kr} 구독 객체 생성 실패")
                return False
                
            # 검증기 생성 및 설정
            self.validator = create_validator(self.exchange_code)
            if self.validator and self.subscription:
                self.subscription.set_validator(self.validator)
                self.log_debug(f"{self.exchange_name_kr} 검증기 설정 완료")
            else:
                self.log_warning(f"{self.exchange_name_kr} 검증기 생성 실패, 검증 없이 진행")
            
            # 이벤트 핸들러 등록
            if self.event_bus:
                # 데이터 이벤트 핸들러 등록
                await self.event_bus.subscribe(EventTypes.ORDERBOOK_UPDATED, 
                                              self.handle_data_event)
                await self.event_bus.subscribe(EventTypes.SNAPSHOT_RECEIVED, 
                                              self.handle_data_event)
                await self.event_bus.subscribe(EventTypes.DELTA_RECEIVED, 
                                              self.handle_data_event)
                
                # 연결 상태 이벤트 핸들러 등록
                await self.event_bus.subscribe(EventTypes.CONNECTION_STATUS, 
                                              self.handle_connector_event)
                                              
                # 구독 상태 이벤트 핸들러 등록
                await self.event_bus.subscribe(EventTypes.SUBSCRIPTION_STATUS, 
                                              self.handle_subscription_event)
                                              
                # 오류 이벤트 핸들러 등록
                await self.event_bus.subscribe(EventTypes.ERROR_EVENT, 
                                              self.handle_error_event)
            
            # 초기화 완료 상태 설정
            self.is_running = True
            self.log_info(f"{self.exchange_name_kr} OrderManager 초기화 완료")
            
            return True
            
        except Exception as e:
            self.log_error(f"{self.exchange_name_kr} 초기화 실패: {str(e)}")
            return False
    
    async def start(self, symbols: List[str]) -> bool:
        """
        오더북 수집 시작
        
        Args:
            symbols: 수집할 심볼 목록
            
        Returns:
            bool: 시작 성공 여부
        """
        try:
            if not symbols:
                self.log_warning(f"{self.exchange_name_kr} 심볼이 없어 오더북 수집을 시작하지 않습니다")
                return False
                
            # 필수 컴포넌트 검증
            if not self.connector:
                self.log_error(f"{self.exchange_name_kr} 연결 객체가 초기화되지 않았습니다. 먼저 initialize()를 호출해야 합니다.")
                return False
                
            if not self.subscription:
                self.log_error(f"{self.exchange_name_kr} 구독 객체가 초기화되지 않았습니다. 먼저 initialize()를 호출해야 합니다.")
                return False
            
            # 심볼 저장 (현재 관리 중인 심볼 목록 업데이트)
            if self.is_running:
                # 새로운 심볼만 추가 (이 부분은 OrderManager의 역할)
                new_symbols = [s for s in symbols if s not in self.symbols]
                if new_symbols:
                    self.log_info(f"{self.exchange_name_kr} 추가 심볼 구독: {len(new_symbols)}개")
                    self.symbols.update(new_symbols)
                    symbols_to_subscribe = new_symbols
                else:
                    return True  # 추가할 심볼이 없으면 성공으로 간주
            else:
                # 처음 시작하는 경우
                self.symbols = set(symbols)
                symbols_to_subscribe = symbols
                self.is_running = True
            
            # 검증기 설정 (있는 경우)
            if self.validator and self.subscription:
                self.subscription.set_validator(self.validator)
            
            # 구독과 웹소켓 연결을 병렬로 처리
            # 구독 시작 태스크 실행 (백그라운드로 실행)
            subscription_task = asyncio.create_task(self.subscription.subscribe(symbols_to_subscribe))
            
            # 구독 결과 확인
            subscription_success = await subscription_task
            
            if subscription_success:
                self.log_info(f"{self.exchange_name_kr} 오더북 수집 시작 - 심볼 {len(self.symbols)}개")
                return True
            else:
                if not self.is_running:
                    self.is_running = False
                self.log_error(f"{self.exchange_name_kr} 구독 실패")
                return False
            
        except Exception as e:
            self.is_running = False
            self.log_error(f"{self.exchange_name_kr} 오더북 수집 시작 실패: {str(e)}")
            return False
    
    async def stop(self) -> None:
        """오더북 수집 중지"""
        try:
            if not self.is_running:
                return
                
            # 구독 취소는 완전히 subscription에 위임
            if self.subscription:
                unsubscribe_success = await self.subscription.unsubscribe(None)
                if not unsubscribe_success:
                    self.log_warning(f"{self.exchange_name_kr} 구독 취소 중 일부 오류 발생")
            
            # 연결 객체 정리 (웹소켓 종료)
            if self.connector:
                await self.connector.disconnect()
                self.log_info(f"{self.exchange_name_kr} 웹소켓 연결 종료됨")
            
            # 오더북 관리자 정리 (OrderManager의 역할)
            if self.validator:
                self.validator.clear_all()
            
            # 상태 관리 (OrderManager의 역할)
            self.is_running = False
            self.symbols.clear()
            
            self.log_info(f"{self.exchange_name_kr} 오더북 수집 중지 완료")
            
        except Exception as e:
            self.log_error(f"{self.exchange_name_kr} 중지 중 오류 발생: {str(e)}")
            self.is_running = False  # 오류가 발생해도 실행 중 상태 해제
    
    @property
    def is_connected(self) -> bool:
        """
        현재 연결 상태 확인 - BaseConnector에 위임
        
        Returns:
            bool: 현재 연결 상태
        """
        # 연결 객체가 있으면 그 상태를 사용 (단일 소스 오브 트루스)
        return self.connector.is_connected if self.connector else False

    async def get_status(self) -> Dict[str, Any]:
        """
        현재 상태 정보 반환
        
        중앙 메트릭 관리자로부터 메트릭 데이터를 가져옵니다.
        
        Returns:
            Dict: 상태 정보
        """
        now = time.time()
        
        # 연결 상태 확인
        connected = self.connector.is_connected if self.connector else False
        
        # 구독 상태 확인
        subscribed_symbols = []
        if self.subscription:
            try:
                subscription_status = self.subscription.subscriptions
                if isinstance(subscription_status, dict) and "symbols" in subscription_status:
                    subscribed_symbols = subscription_status.get("symbols", [])
            except Exception as e:
                self.log_error(f"구독 상태 확인 오류: {str(e)}")
        
        # 중앙 메트릭 관리자에서 메트릭 가져오기
        metrics = self.sys_event_bus.get_metrics(self.exchange_code)
        
        # 데이터 수신 여부는 connected 상태로 대체 (connection이 살아있으면 receiving으로 간주)
        receiving = connected
            
        # 마지막 오류 메시지 가져오기
        last_error = "없음"
        errors = self.sys_event_bus.get_errors(self.exchange_code, 1)
        if errors and len(errors) > 0:
            error = errors[0]
            last_error = f"{error.get('type', 'unknown')}: {error.get('message', '?')}"
            # 너무 긴 오류 메시지는 축약
            if len(last_error) > 50:
                last_error = last_error[:47] + "..."
        
        # 상태 데이터 구성
        status_data = {
            "exchange": self.exchange_code,
            "exchange_name_kr": self.exchange_name_kr,
            "connected": connected,
            "receiving": receiving,
            "subscribed_symbols": subscribed_symbols,
            "subscribed_count": len(subscribed_symbols),
            "message_count": metrics.get("message_count", 0),
            "error_count": metrics.get("error_count", 0),
            "last_error": last_error,
            "message_rate": metrics.get("message_rate", 0)
        }
        
        return status_data
    
    async def handle_connector_event(self, event_data: dict) -> None:
        """
        컨넥터 이벤트 처리
        
        중앙 메트릭 관리자를 활용하여 이벤트 데이터를 처리합니다.
        
        Args:
            event_data: 이벤트 데이터
        """
        try:
            exchange_code = event_data.get("exchange_code")
            
            # 다른 거래소의 이벤트는 무시
            if exchange_code != self.exchange_code:
                return
                
            # 연결 상태 이벤트 처리
            status = event_data.get("status")
            timestamp = event_data.get("timestamp", time.time())
            
            # 중앙 메트릭 관리자에 상태 업데이트
            self.sys_event_bus.update_metric(
                self.exchange_code, 
                "connection_status", 
                status,
                timestamp=timestamp
            )
            
            if status == "connected":
                self.log_info(f"웹소켓 연결 성공: {self.exchange_name_kr}")
                # 연결 시간 기록
                self.sys_event_bus.update_metric(
                    self.exchange_code,
                    "connection_time",
                    timestamp
                )
                    
            elif status == "disconnected":
                self.log_warning(f"웹소켓 연결 종료: {self.exchange_name_kr}")
                # 연결 종료 시간 기록
                self.sys_event_bus.update_metric(
                    self.exchange_code,
                    "disconnection_time",
                    timestamp
                )
                    
                # 재연결 시도 횟수 증가
                self.sys_event_bus.update_metric(
                    self.exchange_code,
                    "reconnect_count",
                    1,
                    op="increment"
                )
                
            elif status == "reconnecting":
                self.log_warning(f"웹소켓 재연결 시도 중: {self.exchange_name_kr}")
              
        except Exception as e:
            self.log_error(f"컨넥터 이벤트 처리 중 오류: {str(e)}")
            # 오류 카운트 증가
            self.sys_event_bus.update_metric(
                self.exchange_code,
                "error_count",
                1,
                op="increment",
                error_type="event_handler_error",
                error_message=str(e)
            )

    async def handle_subscription_event(self, event_data: dict) -> None:
        """
        구독 이벤트 처리
        
        중앙 메트릭 관리자를 활용하여 구독 상태를 추적합니다.
        
        Args:
            event_data: 구독 이벤트 데이터
        """
        try:
            # 이벤트 데이터 확인
            exchange_code = event_data.get("exchange_code")
            status = event_data.get("status")
            symbols = event_data.get("symbols", [])
            
            # 다른 거래소 이벤트는 무시
            if exchange_code != self.exchange_code:
                return
                
            # 구독 상태 변경 로깅
            if status == "subscribed":
                symbol_text = ", ".join(symbols) if len(symbols) <= 3 else f"{len(symbols)}개 심볼"
                self.log_info(f"구독 완료: {symbol_text}")
            elif status == "unsubscribed":
                symbol_text = ", ".join(symbols) if len(symbols) <= 3 else f"{len(symbols)}개 심볼"
                self.log_info(f"구독 해제: {symbol_text}")
            
            # 중앙 메트릭 관리자에 상태 업데이트
            self.sys_event_bus.update_metric(
                self.exchange_code,
                "subscription_status",
                status == "subscribed" and 1.0 or 0.0,
                status=status,
                symbols_count=len(symbols)
            )
            
            # 구독 심볼 수 메트릭 업데이트
            self.sys_event_bus.update_metric(
                self.exchange_code,
                "subscribed_symbols_count",
                float(len(symbols))
            )
            
        except Exception as e:
            self.log_error(f"구독 이벤트 처리 중 오류: {str(e)}")

    async def handle_data_event(self, event_data: dict, event_type: str = None) -> None:
        """
        데이터 이벤트 처리 (스냅샷 또는 델타)
        
        중앙 메트릭 관리자를 활용하여 데이터 이벤트를 처리합니다.
        
        Args:
            event_data: 이벤트 데이터
            event_type: 이벤트 타입 (snapshot 또는 delta), None인 경우 event_data에서 가져옴
        """
        try:
            exchange_code = event_data.get("exchange_code")
            symbol = event_data.get("symbol")
            timestamp = event_data.get("timestamp", time.time())
            
            # event_type이 전달되지 않은 경우 event_data에서 가져옴
            if event_type is None:
                event_type = event_data.get("event_type", "unknown")
            
            # 다른 거래소의 이벤트는 무시
            if exchange_code != self.exchange_code:
                return
            
            # 이벤트 타입별 메트릭 업데이트
            if event_type == "snapshot":
                self.sys_event_bus.update_metric(
                    self.exchange_code,
                    "snapshot_count",
                    1,
                    op="increment",
                    symbol=symbol
                )
            elif event_type == "delta":
                self.sys_event_bus.update_metric(
                    self.exchange_code,
                    "delta_count",
                    1,
                    op="increment",
                    symbol=symbol
                )
                
            # 메시지 수신 메트릭 업데이트
            self.sys_event_bus.update_metric(
                self.exchange_code,
                "message_count",
                1,  # 명시적으로 정수 타입 사용
                op="increment",  # 증가 연산임을 명시
                symbol=symbol,
                message_type=event_type
            )
            
        except Exception as e:
            self.log_error(f"데이터 이벤트 처리 중 오류: {str(e)}")
            # 오류 메트릭 업데이트
            self.sys_event_bus.update_metric(
                self.exchange_code,
                "error_count",
                1,
                op="increment",
                error_type="data_event_error",
                error_message=str(e)
            )

    async def handle_error_event(self, event_data: dict) -> None:
        """
        오류 이벤트 처리
        
        중앙 메트릭 관리자를 활용하여 오류 이벤트를 처리합니다.
        
        Args:
            event_data: 오류 이벤트 데이터
        """
        try:
            error_type = event_data.get("error_type", "unknown")
            message = event_data.get("message", "알 수 없는 오류")
            exchange_code = event_data.get("exchange_code", self.exchange_code)
            severity = event_data.get("severity", "error")
            timestamp = event_data.get("timestamp", time.time())
            symbol = event_data.get("symbol", None)
            
            # 다른 거래소의 이벤트는 무시
            if exchange_code != self.exchange_code:
                return
                
            # 오류 심각도에 따른 로깅
            if severity == "critical":
                self.log_critical(f"{error_type} 오류: {message}")
            elif severity == "warning":
                self.log_warning(f"{error_type} 경고: {message}")
            else:
                self.log_error(f"{error_type} 오류: {message}")
                
            # 중앙 메트릭 관리자에 오류 기록
            self.sys_event_bus.update_metric(
                self.exchange_code,
                "error_count",
                1,  # 명시적으로 정수 타입 사용
                op="increment",  # 증가 연산임을 명시
                error_type=error_type,
                error_message=message,
                error_severity=severity,
                error_timestamp=timestamp,
                symbol=symbol
            )
            
            # 마지막 오류 정보 업데이트
            self.sys_event_bus.update_metric(
                self.exchange_code,
                "last_error_time",
                timestamp
            )
            
            self.sys_event_bus.update_metric(
                self.exchange_code,
                "last_error_type",
                error_type
            )
            
            self.sys_event_bus.update_metric(
                self.exchange_code,
                "last_error_message",
                message
            )
            
        except Exception as e:
            self.log_error(f"오류 이벤트 처리 중 오류: {str(e)}")
            # 이 오류마저 처리하지 못하면 마지막 시도로 로깅만 수행

# OrderManager 팩토리 함수
def create_order_manager(exchange: str, settings: dict) -> Optional[OrderManager]:
    """
    거래소별 OrderManager 인스턴스 생성
    
    Args:
        exchange: 거래소 코드
        settings: 설정 딕셔너리
        
    Returns:
        Optional[OrderManager]: OrderManager 인스턴스 또는 None
    """
    try:
        # 지원하는 거래소 목록
        supported_exchanges = [
            Exchange.UPBIT.value, 
            Exchange.BYBIT.value, 
            Exchange.BYBIT_FUTURE.value,
            Exchange.BITHUMB.value,
            Exchange.BINANCE.value,
            Exchange.BINANCE_FUTURE.value,
        ]
        
        # 지원하는 거래소인지 확인
        if exchange not in supported_exchanges:
            logger.warning(f"{EXCHANGE_NAMES_KR[exchange]} 지원되지 않는 거래소")
            return None
        
        # OrderManager 인스턴스 생성
        manager = OrderManager(settings, exchange)
        return manager
        
    except Exception as e:
        logger.error(f"{EXCHANGE_NAMES_KR[exchange]} OrderManager 생성 실패: {str(e)}")
        return None


# WebsocketManager와 통합하기 위한 함수
async def integrate_with_websocket_manager(ws_manager, settings, filtered_data):
    """
    OrderManager를 WebsocketManager와 통합합니다.
    모든 거래소를 OrderManager를 통해 처리합니다.
    
    Args:
        ws_manager: WebsocketManager 인스턴스
        settings: 설정 딕셔너리
        filtered_data: 필터링된 심볼 데이터 (exchange -> symbols)
        
    Returns:
        성공 여부
    """
    try:
        # 이벤트 버스 인스턴스 가져오기
        event_bus = get_orderbook_event_bus()
        
        # WebsocketManager가 연결 상태 변경 이벤트를 직접 구독하도록 설정
        event_bus.subscribe(
            "connection_status_changed", 
            lambda event_data: ws_manager.update_connection_status(
                event_data["exchange_code"], 
                event_data["status"]
            )
        )
        
        # WebsocketManager에 OrderManager 저장 공간 생성
        if not hasattr(ws_manager, "order_managers"):
            ws_manager.order_managers = {}
        
        # 각 거래소별 처리
        for exchange, symbols in filtered_data.items():
            if not symbols:
                logger.info(f"{EXCHANGE_NAMES_KR[exchange]} 심볼이 없어 OrderManager를 초기화하지 않습니다.")
                continue
                
            # OrderManager 생성
            manager = create_order_manager(exchange, settings)
            if not manager:
                logger.error(f"{EXCHANGE_NAMES_KR[exchange]} OrderManager 생성 실패")
                continue
            
            # 초기화 수행
            init_success = await manager.initialize()
            if not init_success:
                logger.error(f"{EXCHANGE_NAMES_KR[exchange]} 초기화 실패, 해당 거래소는 건너뜁니다.")
                continue
            
            # 시작 수행 - OrderManager의 시작 메서드 사용
            start_success = await manager.start(symbols)
            if not start_success:
                logger.error(f"{EXCHANGE_NAMES_KR[exchange]} 시작 실패, 해당 거래소는 건너뜁니다.")
                continue
            
            # WebsocketManager에 OrderManager 저장
            ws_manager.order_managers[exchange] = manager
            
            logger.info(f"{EXCHANGE_NAMES_KR[exchange]} OrderManager가 WebsocketManager와 통합되었습니다.")
        
        return True
        
    except Exception as e:
        logger.error(f"OrderManager 통합 중 오류 발생: {str(e)}")
        return False
