"""
오더북 관리자 모듈

이 모듈은 거래소별 웹소켓 연결, 구독, 파싱, 오더북 처리의 전체 흐름을 관리합니다.
베이스 클래스들을 활용하여 중앙에서 컨트롤 타워 역할을 합니다.

흐름: connection → subscription → parser
"""

import asyncio
import time
from typing import Dict, List, Any, Optional, Set, Callable
from datetime import datetime

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import Exchange, EXCHANGE_NAMES_KR
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
from crosskimp.ob_collector.orderbook.metric.metrics_manager import WebsocketMetricsManager

# 모든 거래소 컴포넌트 직접 임포트
# 연결 컴포넌트
from crosskimp.ob_collector.orderbook.connection.upbit_s_cn import UpbitWebSocketConnector
from crosskimp.ob_collector.orderbook.connection.bybit_s_cn import BybitWebSocketConnector
from crosskimp.ob_collector.orderbook.connection.bybit_f_cn import BybitFutureWebSocketConnector
from crosskimp.ob_collector.orderbook.connection.bithumb_s_cn import BithumbWebSocketConnector
# from crosskimp.ob_collector.orderbook.connection.binance_s_cn import BinanceWebSocketConnector
# from crosskimp.ob_collector.orderbook.connection.binance_f_cn import BinanceFutureWebSocketConnector

# 구독 컴포넌트
from crosskimp.ob_collector.orderbook.subscription.upbit_s_sub import UpbitSubscription
from crosskimp.ob_collector.orderbook.subscription.bybit_s_sub import BybitSubscription
from crosskimp.ob_collector.orderbook.subscription.bybit_f_sub import BybitFutureSubscription
from crosskimp.ob_collector.orderbook.subscription.bithumb_s_sub import BithumbSubscription
# from crosskimp.ob_collector.orderbook.subscription.binance_s_sub import BinanceSubscription
# from crosskimp.ob_collector.orderbook.subscription.binance_f_sub import BinanceFutureSubscription

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# 컴포넌트 클래스 매핑
EXCHANGE_CONNECTORS = {
    Exchange.UPBIT.value: UpbitWebSocketConnector,
    Exchange.BYBIT.value: BybitWebSocketConnector,
    Exchange.BYBIT_FUTURE.value: BybitFutureWebSocketConnector,
    Exchange.BITHUMB.value: BithumbWebSocketConnector
    # Exchange.BINANCE.value: BinanceWebSocketConnector,
    # Exchange.BINANCE_FUTURE.value: BinanceFutureWebSocketConnector,
}

EXCHANGE_SUBSCRIPTIONS = {
    Exchange.UPBIT.value: UpbitSubscription,
    Exchange.BYBIT.value: BybitSubscription,
    Exchange.BYBIT_FUTURE.value: BybitFutureSubscription,
    Exchange.BITHUMB.value: BithumbSubscription
    # Exchange.BINANCE.value: BinanceSubscription,
    # Exchange.BINANCE_FUTURE.value: BinanceFutureSubscription,
}

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
        
        # 출력 큐
        self.output_queue = None
        
        # 구독 관리자
        self.subscription = None
        
        # 연결 상태 콜백
        self.connection_status_callback = None
        
        # 로거 설정
        self.logger = logger
        
        # 메트릭 매니저 싱글톤 인스턴스 사용
        self.metrics_manager = WebsocketMetricsManager.get_instance()
        self.metrics_manager.initialize_exchange(self.exchange_code)
        
        # 컴포넌트들
        self.connection = None       # 웹소켓 연결 객체
        self.validator = None        # 검증 객체
        
        # 구독 심볼 관리
        self.symbols = set()
        
        # 상태 관리
        self.is_running = False
        self.tasks = {}
        
        # 외부 콜백
        self.start_time = time.time()
        
        logger.info(f"{self.exchange_name_kr} 오더북 관리자 초기화")
    
    async def initialize(self) -> bool:
        """
        모든 컴포넌트 초기화
        
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            # 직접 컴포넌트 클래스 가져오기
            conn_class = EXCHANGE_CONNECTORS.get(self.exchange_code)
            sub_class = EXCHANGE_SUBSCRIPTIONS.get(self.exchange_code)
            
            # 필요한 컴포넌트가 없는 경우
            if not conn_class or not sub_class:
                logger.error(f"{self.exchange_name_kr} 필요한 컴포넌트 클래스를 찾을 수 없습니다")
                return False
            
            # 검증기 클래스 가져오기 (선택적)
            validator_class = BaseOrderBookValidator
            
            # 연결 객체 생성
            try:
                self.connection = conn_class(self.settings)
            except Exception as e:
                logger.error(f"{self.exchange_name_kr} 연결 객체 생성 실패: {str(e)}")
                return False
            
            # 구독 객체 생성 (파서 사용하지 않음)
            try:
                self.subscription = sub_class(self.connection)
            except Exception as e:
                logger.error(f"{self.exchange_name_kr} 구독 객체 생성 실패: {str(e)}")
                self.connection = None  # 생성된 객체 정리
                return False
            
            # 검증기 객체 생성 (선택적)
            try:
                self.validator = validator_class(self.exchange_code)
                if self.subscription:
                    self.subscription.set_validator(self.validator)
            except Exception as e:
                logger.warning(f"{self.exchange_name_kr} 검증기 객체 생성 실패: {str(e)}")
                # 검증기 없이도 계속 진행
            
            # 리소스 연결
            if self.output_queue:
                self.set_output_queue(self.output_queue)
            
            # 컨넥션에 콜백 설정
            if self.connection and self.connection_status_callback:
                self.connection.set_connection_status_callback(
                    lambda status: self.update_connection_status(self.exchange_code, status)
                )
            
            logger.info(f"{self.exchange_name_kr} 컴포넌트 초기화 완료")
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
            
            # 이미 실행 중인 경우 처리
            if self.is_running:
                # 새로운 심볼만 추가
                new_symbols = [s for s in symbols if s not in self.symbols]
                if new_symbols:
                    logger.info(f"{self.exchange_name_kr} 추가 심볼 구독: {len(new_symbols)}개")
                    self.symbols.update(new_symbols)
                    await self.subscription.subscribe(
                        new_symbols,
                        on_snapshot=self._handle_snapshot,
                        on_delta=self._handle_delta,
                        on_error=self._handle_error
                    )
                return True
            
            # 심볼 저장
            self.symbols = set(symbols)
            
            # 시작 상태 설정
            self.is_running = True
            
            try:
                # 연결 및 구독 시작
                await self.connection.connect()
                
                # 구독 시작 (심볼에 따른)
                subscription_task = asyncio.create_task(
                    self.subscription.subscribe(
                        list(self.symbols),
                        on_snapshot=self._handle_snapshot,
                        on_delta=self._handle_delta,
                        on_error=self._handle_error
                    )
                )
                self.tasks["subscription"] = subscription_task
                
                # 심볼 개수 로깅
                logger.info(f"{self.exchange_name_kr} 오더북 수집 시작 - 심볼 {len(self.symbols)}개")
                
                # 메트릭 업데이트 태스크 시작
                self._start_metric_tasks()
                
                return True
            except Exception as e:
                self.is_running = False
                logger.error(f"{self.exchange_name_kr} 연결 및 구독 중 오류: {str(e)}", exc_info=True)
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
                
            # 구독 중인 모든 심볼 구독 취소
            if self.subscription:
                await self.subscription.unsubscribe(None)
            
            # 연결 종료 (이미 unsubscribe에서 처리했으므로 별도 처리 불필요)
            
            # 오더북 관리자 정리
            if self.validator:
                self.validator.clear_all()
            
            # 태스크 취소
            for task in self.tasks.values():
                task.cancel()
            
            self.is_running = False
            self.symbols.clear()
            
            logger.info(f"{self.exchange_name_kr} 오더북 수집 중지 완료")
            
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 중지 중 오류 발생: {str(e)}")
    
    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """
        출력 큐 설정
        
        Args:
            queue: 출력 큐
        """
        # 큐 설정 및 하위 컴포넌트에 전달
        self.output_queue = queue
        
        # 컴포넌트에 출력 큐 설정
        if self.validator:
            self.validator.set_output_queue(queue)
        if self.subscription:
            self.subscription.set_output_queue(queue)
            
        logger.debug(f"{self.exchange_name_kr} 출력 큐 설정 완료")
    
    @property
    def is_connected(self) -> bool:
        """
        현재 연결 상태 확인
        
        Returns:
            bool: 현재 연결 상태
        """
        # 연결 객체가 있으면 그 상태를 직접 사용
        if hasattr(self, 'connection') and self.connection:
            return self.connection.is_connected
        # 구독 객체가 있으면 그 상태를 사용
        elif hasattr(self, 'subscription') and self.subscription:
            return self.subscription.is_connected
        # 아무 것도 없으면 메트릭 매니저에서 상태 확인
        else:
            return self.metrics_manager.is_connected(self.exchange_code)
    
    def update_connection_status(self, exchange_code=None, status=None):
        """
        연결 상태 업데이트 (외부 이벤트에 의한 업데이트)
        
        Args:
            exchange_code: 거래소 코드 (기본값: self.exchange_code)
            status: 상태 ('connected' 또는 'disconnected')
        """
        # 거래소 코드가 없으면 기본값 사용
        exchange = exchange_code or self.exchange_code
        
        # 상태가 있는 경우에만 메트릭 매니저에 위임
        if status is not None:
            self.metrics_manager.update_connection_state(exchange, status)
        
        # 콜백이 설정되어 있으면 호출
        if self.connection_status_callback:
            self.connection_status_callback(exchange, status)
    
    def set_connection_status_callback(self, callback: Callable) -> None:
        """
        연결 상태 콜백 설정
        
        Args:
            callback: 콜백 함수 (exchange, status) -> None
        """
        self.connection_status_callback = callback
        
        # 하위 구성 요소에도 콜백 적용
        if self.connection:
            # 컨넥터 콜백 설정 (래핑된 콜백)
            self.connection.set_connection_status_callback(
                lambda status: self.update_connection_status(self.exchange_code, status)
            )
        
        logger.debug(f"{self.exchange_name_kr} 연결 상태 콜백 설정 완료")

    def _start_metric_tasks(self) -> None:
        """
        메트릭 업데이트 태스크 시작
        """
        # 연결 상태 검사 태스크
        self.tasks["connection_check"] = asyncio.create_task(
            self._check_connection_task()
        )
        
        # 다른 메트릭 태스크가 필요하면 여기에 추가
        logger.debug(f"{self.exchange_name_kr} 메트릭 태스크 시작")

    async def _check_connection_task(self) -> None:
        """
        연결 상태 검사 태스크 (주기적으로 연결 상태 체크)
        """
        try:
            # 체크 간격(초)
            check_interval = self.settings.get("connection_check_interval", 5)
            
            while self.is_running:
                # 현재 상태 체크
                is_connected = self.is_connected
                
                # 메트릭 매니저에 상태 업데이트
                self.metrics_manager.update_connection_state(
                    self.exchange_code,
                    "connected" if is_connected else "disconnected"
                )
                
                # 연결이 끊어졌을 때 처리
                if not is_connected and self.is_running:
                    # 재연결 시도
                    logger.warning(f"{self.exchange_name_kr} 연결이 끊어짐, 재연결 시도 중")
                    
                    # 재연결 및 구독 시도
                    try:
                        await self.connection.reconnect()
                        if self.symbols:
                            await self.subscription.subscribe(list(self.symbols))
                    except Exception as e:
                        logger.error(f"{self.exchange_name_kr} 재연결 실패: {str(e)}")
                
                # 지정된 간격만큼 대기
                await asyncio.sleep(check_interval)
                
        except asyncio.CancelledError:
            logger.debug(f"{self.exchange_name_kr} 연결 상태 검사 태스크 취소됨")
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 연결 상태 검사 중 오류: {str(e)}")

    async def get_status(self) -> Dict[str, Any]:
        """
        현재 상태 정보 가져오기
        
        Returns:
            Dict: 상태 정보
        """
        # 가동 시간 계산
        uptime = time.time() - self.start_time
        
        # 메트릭 정보 가져오기
        metrics = self.metrics_manager.get_exchange_metrics(self.exchange_code)
        
        # 연결 정보
        connection_info = {}
        if self.connection:
            connection_info = {
                "is_connected": self.connection.is_connected,
                "reconnect_count": self.connection.reconnect_count if hasattr(self.connection, 'reconnect_count') else 0
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

    # 콜백 메서드 추가
    async def _handle_snapshot(self, symbol: str, data: Dict) -> None:
        """
        스냅샷 데이터 처리 콜백
        
        Args:
            symbol: 심볼
            data: 스냅샷 데이터
        """
        if self.output_queue:
            self.output_queue.put_nowait({
                "exchange": self.exchange_code,
                "symbol": symbol,
                "timestamp": time.time(),
                "data": data,
                "type": "snapshot"
            })
            
            # 메트릭 업데이트
            self.metrics_manager.update_metric(self.exchange_code, "snapshot")
    
    async def _handle_delta(self, symbol: str, data: Dict) -> None:
        """
        델타 데이터 처리 콜백
        
        Args:
            symbol: 심볼
            data: 델타 데이터
        """
        if self.output_queue:
            self.output_queue.put_nowait({
                "exchange": self.exchange_code,
                "symbol": symbol,
                "timestamp": time.time(),
                "data": data,
                "type": "delta"
            })
            
            # 메트릭 업데이트
            self.metrics_manager.update_metric(self.exchange_code, "delta")
    
    async def _handle_error(self, symbol: str, error: str) -> None:
        """
        에러 처리 콜백
        
        Args:
            symbol: 심볼
            error: 오류 메시지
        """
        logger.error(f"{self.exchange_name_kr} {symbol} 에러: {error}")
        self.metrics_manager.record_error(self.exchange_code)

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
        return OrderManager(settings, exchange)
        
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
                
            # 출력 큐 공유
            manager.set_output_queue(ws_manager.output_queue)
            
            # 연결 상태 콜백 공유
            manager.set_connection_status_callback(
                lambda ex, status: ws_manager.update_connection_status(ex, status)
            )
            
            # 초기화 수행
            init_success = await manager.initialize()
            if not init_success:
                logger.error(f"{EXCHANGE_NAMES_KR[exchange]} 초기화 실패, 해당 거래소는 건너뜁니다.")
                continue
            
            # 시작 수행
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
