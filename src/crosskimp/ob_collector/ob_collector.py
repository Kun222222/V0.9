import asyncio
import time
from typing import Dict, List, Optional, Any

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.ob_collector.core.aggregator import Aggregator
from crosskimp.ob_collector.core.ws_usdtkrw import WsUsdtKrwMonitor
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.common.config.app_config import get_config
from crosskimp.common.events.system_types import EventPaths
from crosskimp.common.config.common_constants import SystemComponent, EXCHANGE_NAMES_KR, Exchange
from crosskimp.ob_collector.metric.metric_manager import ObcMetricManager
from crosskimp.ob_collector.connection_manager import ConnectionManager

# 모든 거래소 컴포넌트 임포트
# 연결 컴포넌트
from crosskimp.ob_collector.orderbook.connection.upbit_s_cn import UpbitWebSocketConnector
from crosskimp.ob_collector.orderbook.connection.bybit_s_cn import BybitWebSocketConnector
from crosskimp.ob_collector.orderbook.connection.bybit_f_cn import BybitFutureWebSocketConnector
from crosskimp.ob_collector.orderbook.connection.bithumb_s_cn import BithumbWebSocketConnector
from crosskimp.ob_collector.orderbook.connection.binance_s_cn import BinanceWebSocketConnector
from crosskimp.ob_collector.orderbook.connection.binance_f_cn import BinanceFutureWebSocketConnector

# 구독 컴포넌트
from crosskimp.ob_collector.orderbook.subscription.upbit_s_sub import UpbitSubscription
from crosskimp.ob_collector.orderbook.subscription.bybit_s_sub import BybitSubscription
from crosskimp.ob_collector.orderbook.subscription.bybit_f_sub import BybitFutureSubscription
from crosskimp.ob_collector.orderbook.subscription.bithumb_s_sub import BithumbSubscription
from crosskimp.ob_collector.orderbook.subscription.binance_s_sub import BinanceSubscription
from crosskimp.ob_collector.orderbook.subscription.binance_f_sub import BinanceFutureSubscription

logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

# 컴포넌트 클래스 매핑
EXCHANGE_CONNECTORS = {
    Exchange.UPBIT.value: UpbitWebSocketConnector,
    Exchange.BYBIT.value: BybitWebSocketConnector,
    Exchange.BYBIT_FUTURE.value: BybitFutureWebSocketConnector,
    Exchange.BITHUMB.value: BithumbWebSocketConnector,
    Exchange.BINANCE.value: BinanceWebSocketConnector,
    Exchange.BINANCE_FUTURE.value: BinanceFutureWebSocketConnector
}

EXCHANGE_SUBSCRIPTIONS = {
    Exchange.UPBIT.value: UpbitSubscription,
    Exchange.BYBIT.value: BybitSubscription,
    Exchange.BYBIT_FUTURE.value: BybitFutureSubscription,
    Exchange.BITHUMB.value: BithumbSubscription,
    Exchange.BINANCE.value: BinanceSubscription,
    Exchange.BINANCE_FUTURE.value: BinanceFutureSubscription
}

class ObCollector:
    """
    오더북 데이터 수집기
    
    거래소 데이터를 실시간으로 수집하는 기술적 컴포넌트입니다.
    주요 역할:
    1. 초기화 및 설정 관리
    2. 수집 프로세스 시작/중지 관리
    3. 메트릭 수집 및 보고
    """

    def __init__(self):
        """초기화"""
        # 로거 설정
        self.logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)
        
        # 설정 로드
        self.settings = get_config()
        
        # 데이터 수집 관련 변수
        self._initialized = False
        
        # 단순화된 상태 관리
        self.status = "stopped"  # "stopped", "starting", "running" 세 가지 상태로 단순화
        
        # 거래소 관련 변수
        self.subscriptions = {}
        self.filtered_symbols = {}
        
        # 오류 로그
        self.error_logs = []
        
        # USDT/KRW 모니터
        self.usdt_monitor = None
        
        # 메트릭 관리자 초기화
        self.metric_manager = ObcMetricManager()
        
        # 연결 관리자 생성 - 메트릭 관리자 전달
        self.connection_manager = ConnectionManager(metrics_manager=self.metric_manager)
        
        # 로깅 추가 - 객체 생성 완료
        self.logger.debug("ObCollector 객체 생성 완료")

    def is_initialized(self) -> bool:
        """초기화 여부 확인"""
        return self._initialized and bool(self.connection_manager.connectors)
        
    async def setup(self):
        """
        초기 설정 메서드
        """
        self.logger.info("오더북 수집기 설정 시작")
        # 로깅 추가
        self.logger.debug("ObCollector.setup() 호출됨")
        self.logger.info("오더북 수집기 설정 완료")
        return True
        
    async def initialize(self) -> bool:
        """
        시스템 초기화: 심볼 필터링 및 USDT/KRW 모니터 설정
        
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            self.logger.info("오더북 수집 시스템 초기화 시작")
            # 로깅 추가
            self.logger.debug("ObCollector.initialize() 호출됨")
            
            # 메트릭 초기화
            self.metric_manager.initialize()
            self.metric_manager.update_component_status("aggregator", "running")
            
            # 1. 심볼 필터링
            aggregator = Aggregator(self.settings)
            self.filtered_symbols = await aggregator.run_filtering()
            
            if not self.filtered_symbols:
                self.logger.error("심볼 필터링 실패")
                return False
                
            self.logger.debug(f"필터링된 심볼: {self.filtered_symbols}")
            
            # 2. USDT/KRW 모니터 설정 - 단순화
            try:
                self.usdt_monitor = WsUsdtKrwMonitor()
                self.logger.debug("USDT/KRW 모니터 생성 완료")
            except Exception as e:
                self.logger.warning(f"USDT/KRW 모니터 초기화 실패, 무시하고 계속: {str(e)}")
            
            # 3. 거래소별 웹소켓 연결 객체 및 구독 객체 생성
            if not await self._prepare_exchange_connections():
                self.logger.error("거래소 연결 준비 실패")
                return False
                
            # 성공 로깅 및 상태 설정
            self._initialized = True
            self.logger.info(f"오더북 수집 시스템 초기화 완료, {self.connection_manager.get_total_exchanges_count()}개 거래소 준비됨")
            
            return True
            
        except Exception as e:
            self.logger.error(f"시스템 초기화 중 오류 발생: {str(e)}", exc_info=True)
            self._log_error("system_init_error", str(e))
            return False

    async def start_collection(self) -> bool:
        """
        오더북 수집 시작 - 핵심 초기화 작업 완료 후 성공 반환
        
        Returns:
            bool: 시작 요청 성공 여부
        """
        try:
            self.logger.info("----------------")
            self.logger.info("오더북 수집 시작 요청")
            self.logger.info("----------------")
            # 로깅 추가
            self.logger.debug("ObCollector.start_collection() 호출됨")
            
            # 실행 즉시 starting 상태로 변경
            self.status = "starting"
            
            # 상태 로깅
            self.logger.info("오더북 수집기가 starting 상태로 전환되었습니다")
            
            # 1. 초기화가 안되어 있으면 초기화 먼저 실행
            if not self.is_initialized():
                self.logger.info("오더북 수집기가 초기화되지 않았습니다. 초기화를 시작합니다.")
                init_success = await self.initialize()
                if not init_success:
                    self.logger.error("오더북 수집 초기화 실패! 수집을 시작할 수 없습니다.")
                    return False
                self.logger.info("오더북 수집기 초기화 완료")
            else:
                self.logger.info("오더북 수집기가 이미 초기화되어 있습니다.")
            
            # 3. USDT/KRW 모니터 시작 - 단순화 (백그라운드 관리 및 초기화 대기 제거)
            if self.usdt_monitor:
                try:
                    # 단순히 시작만 하고 백그라운드에서 실행 (대기 없음)
                    asyncio.create_task(self.usdt_monitor.start())
                    self.logger.info("USDT/KRW 모니터 시작")
                except Exception as e:
                    self.logger.warning(f"USDT/KRW 모니터 시작 중 오류, 무시하고 계속: {str(e)}")
            
            # 커넥터 목록 로깅
            self.logger.info(f"등록된 거래소 커넥터: {list(self.connection_manager.connectors.keys())}")
            
            # 모든 거래소 연결 및 구독을 백그라운드에서 시작
            connect_all_task = asyncio.create_task(self._connect_all_exchanges())
            
            # 주기적 연결 상태 확인 태스크 시작 - ConnectionManager의 모니터링 시작
            self.connection_manager.start_monitoring(interval=30)
            
            # 메트릭 및 상태 업데이트
            self.metric_manager.update_component_status("websocket", "initializing")
            
            # 초기화 작업 완료 후 성공 반환
            self.logger.info("----------------")
            self.logger.info("오더북 수집 초기화 완료 - 모든 거래소 연결은 백그라운드에서 계속됩니다")
            self.logger.info("----------------")
            return True
            
        except Exception as e:
            self.logger.error(f"시스템 시작 중 오류 발생: {str(e)}", exc_info=True)
            self._log_error("system_start_error", str(e))
            return False
            
    async def _connect_all_exchanges(self):
        """모든 거래소를 백그라운드에서 동시에 연결"""
        try:
            # 로깅 추가
            self.logger.debug("ObCollector._connect_all_exchanges() 호출됨")
            
            # 연결할 거래소 목록
            exchanges_to_connect = list(self.connection_manager.connectors.keys())
            
            if not exchanges_to_connect:
                self.logger.debug("연결할 거래소가 없습니다")
                # 연결할 거래소가 없으면 이미 모든 거래소가 연결된 것으로 간주
                self.status = "running"
                self.logger.info("🟢 연결할 거래소가 없어 오더북 수집기가 running 상태로 전환되었습니다")
                return
                
            self.logger.debug(f"백그라운드에서 {len(exchanges_to_connect)}개 거래소 연결 시작")
            
            # 각 거래소별 웹소켓 연결 및 구독 시작
            connect_tasks = []
            for exchange in exchanges_to_connect:
                # 연결 태스크 생성
                connect_task = asyncio.create_task(self._connect_and_subscribe(exchange))
                connect_tasks.append(connect_task)
                
            # 모든 연결 태스크 병렬 실행
            await asyncio.gather(*connect_tasks)
            
            # 모든 거래소 연결 완료 후 running 상태로 변경
            self.status = "running"
            self.logger.info("🟢 모든 거래소 연결 및 구독이 완료되어 오더북 수집기가 running 상태로 전환되었습니다")
            
        except Exception as e:
            self.logger.error(f"거래소 연결 중 오류 발생: {str(e)}", exc_info=True)
            self._log_error("exchange_connection_error", str(e))
            
            # 오류가 발생했지만 일부 거래소는 연결되었을 수 있으므로 
            # 연결된 거래소가 전체 거래소 수와 같으면 running으로 간주
            if self.connection_manager.get_connected_exchanges_count() >= self.connection_manager.get_total_exchanges_count():
                self.status = "running"
                self.logger.info("🟢 일부 오류가 있지만 모든 거래소 연결 시도가 완료되어 running 상태로 전환되었습니다")

    async def stop_collection(self) -> bool:
        """오더북 수집 중지"""
        try:
            # 로깅 추가
            self.logger.debug("ObCollector.stop_collection() 호출됨")
            
            # 상태 업데이트
            old_status = self.status
            self.status = "stopped"
            
            self.logger.info(f"오더북 수집기 중지 요청 (현재 상태: {old_status})")
            
            # USDT/KRW 모니터 종료
            if self.usdt_monitor:
                await self.usdt_monitor.stop()
                self.logger.info("USDT/KRW 모니터가 중지되었습니다.")
                self.usdt_monitor = None
                
            # 연결 관리자 모니터링 중지
            self.connection_manager.stop_monitoring()
            
            # 모든 거래소 연결 종료
            try:
                await self.connection_manager.close_all_connections()
            except Exception as e:
                self.logger.error(f"모든 거래소 연결 종료 중 오류: {str(e)}")
            
            # 구독 정리
            # 모든 구독 객체 종료
            for exchange_code, subscription in list(self.subscriptions.items()):
                exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
                try:
                    if subscription:
                        await subscription.unsubscribe()
                        self.logger.info(f"{exchange_kr} 구독이 취소되었습니다")
                except Exception as e:
                    self.logger.error(f"{exchange_kr} 종료 중 오류: {str(e)}")
            
            self.subscriptions = {}
            
            # 상태 메트릭 업데이트
            self.metric_manager.update_component_status("websocket", "stopped")
            
            self.logger.info("오더북 수집기가 정상적으로 중지되었습니다.")
            return True
            
        except Exception as e:
            self.logger.error(f"오더북 수집기 중지 중 오류 발생: {e}", exc_info=True)
            return False

    # 상태 조회 메서드 추가
    # ======================================================
    
    def get_status(self):
        """현재 오더북 수집기의 상태 반환"""
        return self.status

    # 이벤트 발행 대신 내부 상태 관리를 위한 메서드들
    # ======================================================
    
    def _log_error(self, error_type: str, error_message: str) -> None:
        """오류 정보 기록"""
        self.error_logs.append({
                "error_type": error_type,
                "message": error_message,
                "timestamp": time.time()
            })
        # 로그 크기 제한 (최근 100개만 유지)
        if len(self.error_logs) > 100:
            self.error_logs = self.error_logs[-100:]

    # 기존 메트릭 데이터 관리 메서드들
    # ======================================================
    
    def init_metrics_for_exchange(self, exchange):
        """특정 거래소에 대한 메트릭 트래커 초기화"""
        self.metric_manager.init_metrics_for_exchange(exchange)
            
    def update_message_counter(self, exchange, count=1):
        """메시지 카운터 업데이트"""
        self.metric_manager.update_message_counter(exchange, count)
        
    def update_error_counter(self, exchange, error_type, count=1):
        """오류 카운터 업데이트"""
        self.metric_manager.update_error_counter(exchange, error_type, count)
        
    def increment_reconnect_counter(self, exchange):
        """재연결 카운터 증가"""
        self.metric_manager.increment_reconnect_counter(exchange)
        
    def update_symbol_timestamp(self, exchange, symbol, update_type="data"):
        """심볼별 타임스탬프 업데이트"""
        self.metric_manager.update_symbol_timestamp(exchange, symbol, update_type)
        
    # 통합 메트릭 제공 메서드
    # ======================================================
    
    def get_metrics(self):
        """
        시스템 메트릭 가져오기
        """
        # 현재 시스템 상태를 메트릭 매니저에 전달
        self.metric_manager.set_system_state(
            is_starting=self.status == "starting",
            is_fully_started=self.status == "running"
        )
        
        # 메트릭 반환
        return self.metric_manager.get_metrics()

    def _create_connector(self, exchange_code: str, settings: Dict[str, Any]) -> Optional[BaseWebsocketConnector]:
        """
        거래소별 웹소켓 연결 객체 생성 (ConnectionManager 위임)
        
        Args:
            exchange_code: 거래소 코드
            settings: 설정 딕셔너리
            
        Returns:
            BaseWebsocketConnector: 웹소켓 연결 객체 또는 None (실패 시)
        """
        connector_class = EXCHANGE_CONNECTORS.get(exchange_code)
        if not connector_class:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
            self.logger.warning(f"{exchange_kr} 해당 거래소의 연결 클래스를 찾을 수 없습니다")
            return None
        
        # ConnectionManager에 위임
        return self.connection_manager.create_connector(exchange_code, settings, connector_class)

    async def _connect_and_subscribe(self, exchange_code: str) -> bool:
        """
        특정 거래소에 웹소켓 연결 및 구독 수행 - 개선된 버전
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            bool: 성공 여부
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        connector = self.connection_manager.get_connector(exchange_code)
        
        if not connector:
            self.logger.warning(f"{exchange_kr} 연결 객체가 없습니다")
            return False
            
        try:
            # 웹소켓 연결 시도
            self.logger.info(f"{exchange_kr} 웹소켓 연결 시도")
            connected = await connector.connect()
            
            # 연결 실패 처리
            if not connected and not connector.is_connected:
                self.logger.error(f"{exchange_kr} 웹소켓 연결 실패")
                self.update_error_counter(exchange_code, "connection_errors")
                self.increment_reconnect_counter(exchange_code)
                return False
                
            self.logger.info(f"{exchange_kr} 웹소켓 연결 성공")
            
            # 구독 수행
            subscription = self.subscriptions.get(exchange_code)
            if not subscription:
                self.logger.warning(f"{exchange_kr} 구독 객체가 없습니다")
                return False
                
            # 필터링된 심볼 목록 가져오기
            symbols = self.filtered_symbols.get(exchange_code, [])
            if not symbols:
                self.logger.warning(f"{exchange_kr} 구독할 심볼이 없습니다")
                return False
                
            # 구독 시작
            self.logger.info(f"{exchange_kr} 구독 시작 ({len(symbols)}개 심볼)")
            subscribe_result = await subscription.subscribe(symbols)
            
            # 구독 결과 확인
            if not subscribe_result:
                self.logger.warning(f"{exchange_kr} 구독 실패 또는 부분 성공")
                self.update_error_counter(exchange_code, "subscription_errors")
            else:
                self.logger.info(f"{exchange_kr} 구독 성공")
            
            # 구독 상태 업데이트
            subscription_active = bool(subscribe_result)
            self.metric_manager.update_subscription_status(
                exchange_code, 
                active=subscription_active, 
                symbol_count=len(symbols),
                symbols=symbols
            )
            
            # 모든 심볼에 대해 타임스탬프 업데이트
            for symbol in symbols:
                self.update_symbol_timestamp(exchange_code, symbol, "subscribe")
                
            # 최종 연결 및 구독 상태 로깅
            self.logger.info(f"{exchange_kr} 연결 및 구독 처리 완료 (연결: {connector.is_connected}, 구독: {subscription_active})")
            return True
            
        except Exception as e:
            self.logger.error(f"{exchange_kr} 연결 및 구독 중 오류 발생: {str(e)}", exc_info=True)
            self.update_error_counter(exchange_code, "connection_errors")
            
            # 오류 기록
            self._log_error(f"{exchange_code}_connection_error", str(e))
            
            return False

    async def _prepare_exchange_connections(self) -> bool:
        """거래소별 연결 및 구독 객체 준비"""
        try:
            self.logger.info("================")
            self.logger.info("거래소 연결 및 구독 객체 준비 시작")
            self.logger.info("================")
            
            # 필터링된 심볼 재확인
            if not self.filtered_symbols:
                self.logger.error("필터링된 심볼이 없습니다. 거래소 연결을 준비할 수 없습니다.")
                return False
                
            self.logger.info(f"총 {len(self.filtered_symbols)} 개의 거래소에 대한 연결을 준비합니다")
            
            # 각 거래소별 커넥터 및 구독 객체 초기화
            success_count = 0
            for exchange, symbols in self.filtered_symbols.items():
                if not symbols:
                    self.logger.warning(f"거래소 '{exchange}'에 구독할 심볼이 없습니다. 건너뜁니다.")
                    continue
                
                # 한글 거래소명 가져오기
                exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
                self.logger.info(f"{exchange_kr} 컴포넌트 초기화 시작 ({len(symbols)}개 심볼)")
                
                # 커넥터 생성
                connector = self._create_connector(exchange, self.settings)
                if not connector:
                    self.logger.error(f"{exchange_kr} 커넥터 생성 실패, 해당 거래소 건너뜀")
                    continue
                    
                # 구독 객체 생성
                subscription = self._create_subscription(connector)
                if not subscription:
                    self.logger.error(f"{exchange_kr} 구독 객체 생성 실패, 해당 거래소 건너뜀")
                    continue
                    
                self.subscriptions[exchange] = subscription
                
                # 상태 초기화 - ConnectionManager 활용
                self.connection_manager.update_exchange_status(exchange, False)
                
                # 메트릭 트래커 초기화
                self.init_metrics_for_exchange(exchange)
                
                # 구독 상태 초기화
                self.metric_manager.update_subscription_status(
                    exchange, 
                    active=False, 
                    symbol_count=len(symbols),
                    symbols=symbols
                )
                
                # 심볼별 타임스탬프 초기화
                for symbol in symbols:
                    self.update_symbol_timestamp(exchange, symbol, "subscribe")
                
                success_count += 1
                self.logger.info(f"{exchange_kr} 컴포넌트 초기화 완료 ({len(symbols)}개 심볼)")
                
            # 결과 확인
            if success_count == 0:
                self.logger.error("어떤 거래소도 초기화되지 않았습니다. 오더북 수집기를 시작할 수 없습니다.")
                return False
                
            self.logger.info(f"거래소 연결 및 구독 객체 준비 완료 ({success_count}/{len(self.filtered_symbols)} 성공)")
            self.logger.info("================")
            return True
            
        except Exception as e:
            self.logger.error(f"거래소 연결 준비 중 오류 발생: {str(e)}", exc_info=True)
            self._log_error("exchange_preparation_error", str(e))
            return False

    def _create_subscription(self, connector: BaseWebsocketConnector) -> Optional[BaseSubscription]:
        """
        거래소별 구독 객체 생성
        
        Args:
            connector: 웹소켓 연결 객체
            
        Returns:
            BaseSubscription: 구독 객체 또는 None (실패 시)
        """
        exchange_code = connector.exchange_code
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        
        try:
            # 거래소 코드에 해당하는 구독 클래스 찾기
            subscription_class = EXCHANGE_SUBSCRIPTIONS.get(exchange_code)
            if not subscription_class:
                self.logger.warning(f"{exchange_kr} 해당 거래소의 구독 클래스를 찾을 수 없습니다")
                return None
                
            # 구독 객체 생성 (데이터 수신 콜백은 아직 설정하지 않음)
            self.logger.debug(f"{exchange_kr} 구독 객체 생성 시도 (클래스: {subscription_class.__name__})")
            subscription = subscription_class(connector, exchange_code, collector=self)
            self.logger.info(f"{exchange_kr} 구독 객체 생성 성공")
            return subscription
            
        except Exception as e:
            self.logger.error(f"{exchange_kr} 구독 객체 생성 실패: {str(e)}", exc_info=True)
            self._log_error(f"{exchange_code}_subscription_creation_error", str(e))
            return None

    async def _reconnect_exchange(self, exchange_code: str, reason: str):
        """
        특정 거래소에 재연결 시도 (ConnectionManager 위임 + 추가 처리)
        
        Args:
            exchange_code: 거래소 코드
            reason: 재연결 이유
            
        Returns:
            bool: 성공 여부
        """
        # ConnectionManager에 재연결 위임
        reconnect_success = await self.connection_manager.reconnect_exchange(exchange_code, reason)
        
        if not reconnect_success:
            return False
            
        # 재연결 후 재구독 처리 (이 부분은 ObCollector에서 담당)
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        
        # 구독 객체 확인
        subscription = self.subscriptions.get(exchange_code)
        if not subscription:
            self.logger.warning(f"{exchange_kr} 구독 객체가 없습니다")
            return True  # 재연결은 성공했으므로 True 반환
            
        # 심볼 목록 가져오기
        symbols = self.filtered_symbols.get(exchange_code, [])
        if not symbols:
            self.logger.warning(f"{exchange_kr} 구독할 심볼이 없습니다")
            return True
            
        # 재구독 수행
        self.logger.info(f"{exchange_kr} 재구독 시작 ({len(symbols)}개 심볼)")
        subscribe_result = await subscription.subscribe(symbols)
        
        # 구독 결과 확인
        if not subscribe_result:
            self.logger.warning(f"{exchange_kr} 재구독 실패 또는 부분 성공")
            self.update_error_counter(exchange_code, "subscription_errors")
        else:
            self.logger.info(f"✅ [{exchange_kr}] 재구독 성공")
            
            # 메시지 카운터 초기화
            self.metric_manager.reset_message_counter(exchange_code)
            
        # 구독 상태 업데이트
        self.metric_manager.update_subscription_status(
            exchange_code, 
            active=bool(subscribe_result), 
            symbol_count=len(symbols),
            symbols=symbols
        )
        
        return True
