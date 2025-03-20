"""
오더북 관리자 모듈

이 모듈은 거래소별 웹소켓 연결, 구독, 파싱, 오더북 처리의 전체 흐름을 관리합니다.
베이스 클래스들을 활용하여 중앙에서 컨트롤 타워 역할을 합니다.

흐름: connection → subscription → parser
"""

import asyncio
import time
import importlib
from typing import Dict, List, Any, Optional
import traceback

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import Exchange, EXCHANGE_NAMES_KR
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
from crosskimp.ob_collector.orderbook.util.component_factory import create_connector, create_subscription, create_validator
from crosskimp.ob_collector.orderbook.util.event_bus import EventBus

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class OrderManager:
    """
    통합 오더북 관리자 클래스
    
    베이스 클래스들을 활용하여 중앙에서 컨트롤 타워 역할을 합니다.
    거래소별 특화 로직은 설정과 콜백으로 처리합니다.
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
        
        # 로거 설정
        self.logger = logger
        
        # SystemEventManager 설정
        from crosskimp.ob_collector.orderbook.util.event_manager import SystemEventManager
        self.system_event_manager = SystemEventManager.get_instance()
        self.system_event_manager.initialize_exchange(self.exchange_code)
        
        # 이벤트 버스 인스턴스 가져오기
        self.event_bus = EventBus.get_instance()
        
        # 컴포넌트들
        self.connection = None       # 웹소켓 연결 객체
        self.subscription = None     # 구독 객체
        self.validator = None        # 검증 객체
        
        # 구독 심볼 관리
        self.symbols = set()
        
        # 상태 관리
        self.is_running = False
        
        # 시작 시간 기록
        self.start_time = time.time()
        
        logger.info(f"{self.exchange_name_kr} 오더북 관리자 초기화")
    
    async def initialize(self) -> bool:
        """
        모든 컴포넌트 초기화
        
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            # 팩토리를 사용하여 컴포넌트 생성
            self.connection = create_connector(self.exchange_code, self.settings)
            if not self.connection:
                logger.error(f"{self.exchange_name_kr} 연결 객체를 생성할 수 없습니다")
                return False
            
            self.subscription = create_subscription(self.exchange_code, self.connection)
            if not self.subscription:
                logger.error(f"{self.exchange_name_kr} 구독 객체를 생성할 수 없습니다")
                return False
                
            self.validator = create_validator(self.exchange_code)
            if self.validator and self.subscription:
                self.subscription.set_validator(self.validator)
            
            # 이벤트 구독 설정
            # 1. 커넥터의 연결 상태 변경 이벤트 구독 (비동기 람다로 변경)
            self.event_bus.subscribe(
                "connection_status", 
                lambda event_data: asyncio.create_task(self.handle_connector_event(event_data))
            )
            
            # 2. 구독 상태 변경 이벤트 구독 (비동기 람다로 변경)
            self.event_bus.subscribe(
                "subscription_status",
                lambda event_data: asyncio.create_task(self.handle_subscription_event(event_data))
            )
            
            # 3. 데이터 스냅샷 이벤트 구독
            self.event_bus.subscribe(
                "data_snapshot",
                lambda event_data: asyncio.create_task(self.handle_data_event(event_data, "snapshot"))
            )
            
            # 4. 데이터 델타 이벤트 구독
            self.event_bus.subscribe(
                "data_delta",
                lambda event_data: asyncio.create_task(self.handle_data_event(event_data, "delta"))
            )
            
            # 5. 오류 이벤트 구독
            self.event_bus.subscribe(
                "error_event",
                lambda event_data: asyncio.create_task(self.handle_error_event(event_data))
            )
            
            logger.info(f"{self.exchange_name_kr} 컴포넌트 초기화 및 이벤트 구독 완료")
            return True
            
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 초기화 실패: {str(e)}", exc_info=True)
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
                logger.warning(f"{self.exchange_name_kr} 심볼이 없어 오더북 수집을 시작하지 않습니다")
                return False
                
            # 필수 컴포넌트 검증
            if not self.connection:
                logger.error(f"{self.exchange_name_kr} 연결 객체가 초기화되지 않았습니다. 먼저 initialize()를 호출해야 합니다.")
                return False
                
            if not self.subscription:
                logger.error(f"{self.exchange_name_kr} 구독 객체가 초기화되지 않았습니다. 먼저 initialize()를 호출해야 합니다.")
                return False
            
            # 심볼 저장 (현재 관리 중인 심볼 목록 업데이트)
            if self.is_running:
                # 새로운 심볼만 추가 (이 부분은 OrderManager의 역할)
                new_symbols = [s for s in symbols if s not in self.symbols]
                if new_symbols:
                    logger.info(f"{self.exchange_name_kr} 추가 심볼 구독: {len(new_symbols)}개")
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
            
            # 구독 시작 - 이벤트 버스 기반 구독
            subscription_success = await self.subscription.subscribe(symbols_to_subscribe)
            
            if subscription_success:
                logger.info(f"{self.exchange_name_kr} 오더북 수집 시작 - 심볼 {len(self.symbols)}개")
                return True
            else:
                if not self.is_running:
                    self.is_running = False
                logger.error(f"{self.exchange_name_kr} 구독 실패")
                return False
            
        except Exception as e:
            self.is_running = False
            logger.error(f"{self.exchange_name_kr} 오더북 수집 시작 실패: {str(e)}", exc_info=True)
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
                    logger.warning(f"{self.exchange_name_kr} 구독 취소 중 일부 오류 발생")
            
            # 연결 객체 정리 (웹소켓 종료)
            if self.connection:
                await self.connection.disconnect()
                logger.info(f"{self.exchange_name_kr} 웹소켓 연결 종료됨")
            
            # 오더북 관리자 정리 (OrderManager의 역할)
            if self.validator:
                self.validator.clear_all()
            
            # 상태 관리 (OrderManager의 역할)
            self.is_running = False
            self.symbols.clear()
            
            logger.info(f"{self.exchange_name_kr} 오더북 수집 중지 완료")
            
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 중지 중 오류 발생: {str(e)}")
            self.is_running = False  # 오류가 발생해도 실행 중 상태 해제
    
    @property
    def is_connected(self) -> bool:
        """
        현재 연결 상태 확인 - BaseConnector에 위임
        
        Returns:
            bool: 현재 연결 상태
        """
        # 연결 객체가 있으면 그 상태를 사용 (단일 소스 오브 트루스)
        return self.connection.is_connected if self.connection else False

    async def get_status(self) -> Dict[str, Any]:
        """
        현재 상태 정보 가져오기
        
        Returns:
            Dict: 상태 정보
        """
        # 가동 시간 계산
        uptime = time.time() - self.start_time
        
        # 메트릭 정보 가져오기
        metrics = self.system_event_manager.get_status(self.exchange_code)
        
        # 연결 정보
        connection_info = {}
        if self.connection:
            connection_info = {
                "is_connected": self.connection.is_connected,
                "reconnect_count": self.connection.stats.reconnect_count if hasattr(self.connection, 'stats') else 0
            }
        
        # 종합 상태 정보
        status = {
            "exchange": self.exchange_code,
            "exchange_kr": self.exchange_name_kr,
            "is_running": self.is_running,
            "uptime_seconds": uptime,
            "uptime_formatted": f"{int(uptime // 3600)}시간 {int((uptime % 3600) // 60)}분 {int(uptime % 60)}초",
            "symbols_count": len(self.symbols),
            "connection": connection_info,
            "metrics": metrics
        }
        
        return status
    
    async def handle_connector_event(self, event_data: dict) -> None:
        """
        커넥터 이벤트 처리 (이벤트 버스에서 호출됨)
        
        Args:
            event_data: 연결 상태 이벤트 데이터
        """
        try:
            # 이벤트가 현재 관리 중인 거래소와 관련된 것인지 확인
            if event_data.get("exchange_code") != self.exchange_code:
                return
                
            # 연결 상태 정보 추출 및 로깅만 수행
            status = event_data.get("status", "unknown")
            event_text = "연결됨" if status == "connected" else "연결 끊김"
            logger.info(f"{self.exchange_name_kr} 상태 변경: {event_text}")
                
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 커넥터 이벤트 처리 중 오류: {str(e)}")

    async def handle_subscription_event(self, event_data: dict) -> None:
        """
        구독 상태 이벤트 처리 (이벤트 버스에서 호출됨)
        
        Args:
            event_data: 구독 이벤트 데이터
        """
        try:
            # 이벤트가 현재 관리 중인 거래소와 관련된 것인지 확인
            if event_data.get("exchange_code") != self.exchange_code:
                return
                
            # 구독 상태 정보 로깅
            status = event_data.get("status", "unknown")
            symbols_count = len(event_data.get("symbols", []))
            
            if status == "subscribed":
                logger.info(f"{self.exchange_name_kr} {symbols_count}개 심볼 구독 성공")
            elif status == "unsubscribed":
                logger.info(f"{self.exchange_name_kr} {symbols_count}개 심볼 구독 취소됨")
                
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 구독 이벤트 처리 중 오류: {str(e)}")

    async def handle_data_event(self, event_data: dict, event_type: str) -> None:
        """
        데이터 이벤트 처리 (이벤트 버스에서 호출됨)
        
        Args:
            event_data: 데이터 이벤트 정보
            event_type: 이벤트 타입 ('snapshot' 또는 'delta')
        """
        try:
            # 이벤트가 현재 관리 중인 거래소와 관련된 것인지 확인
            if event_data.get("exchange_code") != self.exchange_code:
                return
                
            # 심볼 및 데이터 추출
            symbol = event_data.get("symbol")
            data = event_data.get("data")
            
            if not symbol or not data:
                return
                
            # 로그 출력 제거 - 과도한 로그 발생 방지
            # 메트릭만 업데이트하고 로그는 출력하지 않음
                
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 데이터 이벤트 처리 중 오류: {str(e)}")
            
    async def handle_error_event(self, event_data: dict) -> None:
        """
        오류 이벤트 처리 (이벤트 버스에서 호출됨)
        
        Args:
            event_data: 오류 이벤트 데이터
        """
        try:
            # 이벤트가 현재 관리 중인 거래소와 관련된 것인지 확인
            if event_data.get("exchange_code") != self.exchange_code:
                return
                
            # 오류 정보 추출 및 로깅
            error_type = event_data.get("error_type", "unknown")
            message = event_data.get("message", "알 수 없는 오류")
            severity = event_data.get("severity", "error")
            
            if severity == "critical":
                logger.critical(f"{self.exchange_name_kr} 심각한 오류: [{error_type}] {message}")
            else:
                logger.error(f"{self.exchange_name_kr} 오류: [{error_type}] {message}")
                
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 오류 이벤트 처리 중 오류: {str(e)}")

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
            # Exchange.BINANCE.value,
            # Exchange.BINANCE_FUTURE.value,
        ]
        
        # 지원하는 거래소인지 확인
        if exchange not in supported_exchanges:
            logger.warning(f"{EXCHANGE_NAMES_KR[exchange]} 지원되지 않는 거래소")
            return None
        
        # OrderManager 인스턴스 생성
        manager = OrderManager(settings, exchange)
        return manager
        
    except Exception as e:
        logger.error(f"{EXCHANGE_NAMES_KR[exchange]} OrderManager 생성 실패: {str(e)}", exc_info=True)
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
        event_bus = EventBus.get_instance()
        
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
        logger.error(f"OrderManager 통합 중 오류 발생: {str(e)}", exc_info=True)
        return False
