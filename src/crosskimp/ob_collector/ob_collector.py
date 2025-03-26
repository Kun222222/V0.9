import asyncio
import time
from typing import Dict, List, Optional, Any
import logging

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.ob_collector.core.aggregator import Aggregator
from crosskimp.ob_collector.core.ws_usdtkrw import WsUsdtKrwMonitor
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.common.config.app_config import get_config
from crosskimp.common.events.system_types import EventPaths
from crosskimp.common.config.common_constants import SystemComponent, EXCHANGE_NAMES_KR, Exchange
from crosskimp.ob_collector.metric.metric_manager import ObcMetricManager

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
    상태 관리는 하지 않으며 순수한 데이터 수집 기능만 제공합니다.
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
        self.connectors = {}
        self.subscriptions = {}
        self.filtered_symbols = {}
        
        # 상태 추적 (기술적 목적)
        self.exchange_status = {}
        
        # 오류 로그
        self.error_logs = []
        
        # USDT/KRW 모니터
        self.usdt_monitor = None
        
        # 연결된 거래소 수 추적 
        self.connected_exchanges_count = 0
        self.total_exchanges_count = 0
        
        # 메트릭 관리자 초기화
        self.metric_manager = ObcMetricManager()

    def is_initialized(self) -> bool:
        """초기화 여부 확인"""
        return self._initialized and bool(self.connectors)
        
    async def setup(self):
        """
        초기 설정 메서드
        """
        self.logger.info("오더북 수집기 설정 시작")
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
            self.total_exchanges_count = len(self.connectors)
            self.logger.info(f"오더북 수집 시스템 초기화 완료, {self.total_exchanges_count}개 거래소 준비됨")
            
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
            self.logger.info(f"등록된 거래소 커넥터: {list(self.connectors.keys())}")
            
            # 모든 거래소 연결 및 구독을 백그라운드에서 시작
            connect_all_task = asyncio.create_task(self._connect_all_exchanges())
            
            # 주기적 연결 상태 확인 태스크 시작
            connection_health_task = asyncio.create_task(self._monitor_connection_health())
            
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
            # 연결할 거래소 목록
            exchanges_to_connect = list(self.connectors.keys())
            
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
            if self.connected_exchanges_count >= self.total_exchanges_count:
                self.status = "running"
                self.logger.info("🟢 일부 오류가 있지만 모든 거래소 연결 시도가 완료되어 running 상태로 전환되었습니다")
            
    async def _connect_and_subscribe(self, exchange_code: str) -> bool:
        """
        특정 거래소에 웹소켓 연결 및 구독 수행
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            bool: 성공 여부
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        connector = self.connectors.get(exchange_code)
        
        if not connector:
            self.logger.warning(f"{exchange_kr} 연결 객체가 없습니다")
            return False
            
        try:
            # 웹소켓 연결 시도 - 중요: 연결 결과 확인 
            self.logger.info(f"{exchange_kr} 웹소켓 연결 시도")
            connected = await connector.connect()
            
            # 연결 상태 확인
            if not connected and not connector.is_connected():
                self.logger.error(f"{exchange_kr} 웹소켓 연결 실패")
                
                # 오류 메트릭 업데이트
                self.update_error_counter(exchange_code, "connection_errors")
                self.increment_reconnect_counter(exchange_code)
                
                # 연결 상태 업데이트
                self.exchange_status[exchange_code] = False
                self.metric_manager.update_exchange_status(exchange_code, False)
                return False
                
            self.logger.info(f"{exchange_kr} 웹소켓 연결 성공")
            
            # 연결 성공 시 연결 시작 시간 기록 및 연결된 거래소 수 증가
            self.exchange_status[exchange_code] = True
            self.metric_manager.update_exchange_status(exchange_code, True)
            self.metric_manager.update_component_status("websocket", "running")
            
            # 연결된 거래소 수 업데이트
            self.connected_exchanges_count += 1
            
            # 모든 거래소가 연결되었는지 확인
            if self.connected_exchanges_count >= self.total_exchanges_count:
                old_status = self.status
                self.status = "running"
                self.logger.info(f"🟢 모든 거래소가 연결되어 오더북 수집기가 {old_status} → running 상태로 전환되었습니다 (연결됨: {self.connected_exchanges_count}/{self.total_exchanges_count})")
            
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
                # 구독 실패 시 오류 메트릭 업데이트
                self.update_error_counter(exchange_code, "subscription_errors")
            else:
                self.logger.info(f"{exchange_kr} 구독 성공")
            
            # 구독 상태 업데이트 (결과와 상관없이 상태는 업데이트)
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
            self.logger.info(f"{exchange_kr} 연결 및 구독 처리 완료 (연결: {self.exchange_status[exchange_code]}, 구독: {subscription_active})")
            return True
            
        except Exception as e:
            self.logger.error(f"{exchange_kr} 연결 및 구독 중 오류 발생: {str(e)}", exc_info=True)
            self.update_error_counter(exchange_code, "connection_errors")
            
            # 오류 기록
            self._log_error(f"{exchange_code}_connection_error", str(e))
            
            # 연결 상태 업데이트
            self.exchange_status[exchange_code] = False
            self.metric_manager.update_exchange_status(exchange_code, False)
            
            # 실패 반환
            return False

    async def stop_collection(self) -> bool:
        """
        오더북 수집 중지
        
        Returns:
            bool: 중지 성공 여부
        """
        try:
            self.logger.info("오더북 수집 중지 중")
            
            # 상태 플래그 초기화
            self.status = "stopped"
            self.connected_exchanges_count = 0
            
            # USDT/KRW 모니터 중지
            if self.usdt_monitor:
                await self.usdt_monitor.stop()
                
            # 각 거래소별 구독 해제 및 연결 종료
            for exchange, subscription in self.subscriptions.items():
                try:
                    # 한글 거래소명 가져오기
                    exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
                    
                    # 구독 해제
                    await subscription.unsubscribe()
                    self.logger.info(f"{exchange_kr} 구독 해제 완료")
                    
                    # 상태 업데이트
                    self.metric_manager.update_subscription_status(exchange, active=False, symbol_count=0)
                    
                    # 연결 종료
                    connector = self.connectors.get(exchange)
                    if connector:
                        await connector.disconnect()
                        self.logger.info(f"{exchange_kr} 연결 종료 완료")
                        
                        # 상태 업데이트
                        self.exchange_status[exchange] = False
                        self.metric_manager.update_exchange_status(exchange, False)
                        
                except Exception as e:
                    exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
                    self.logger.error(f"{exchange_kr} 종료 중 오류: {str(e)}")
            
            self.connectors = {}
            self.subscriptions = {}
            self._initialized = False
            
            self.logger.info("오더북 수집 중지 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"시스템 중지 중 오류 발생: {str(e)}", exc_info=True)
            self._log_error("system_stop_error", str(e))
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
        모든 메트릭 데이터를 수집하여 반환
        
        Returns:
            Dict: 메트릭 데이터 딕셔너리
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
        거래소별 웹소켓 연결 객체 생성
        
        Args:
            exchange_code: 거래소 코드
            settings: 설정 딕셔너리
            
        Returns:
            BaseWebsocketConnector: 웹소켓 연결 객체 또는 None (실패 시)
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, f"{exchange_code}")
        
        try:
            # 거래소 코드에 해당하는 클래스 찾기
            connector_class = EXCHANGE_CONNECTORS.get(exchange_code)
            if not connector_class:
                self.logger.warning(f"{exchange_kr} 해당 거래소의 연결 클래스를 찾을 수 없습니다")
                return None
                
            # 연결 객체 생성
            self.logger.debug(f"{exchange_kr} 연결 객체 생성 시도 (클래스: {connector_class.__name__})")
            connector = connector_class(settings, exchange_code)
            self.logger.info(f"{exchange_kr} 연결 객체 생성 성공")
            return connector
            
        except Exception as e:
            self.logger.error(f"{exchange_kr} 연결 객체 생성 실패: {str(e)}", exc_info=True)
            self._log_error(f"{exchange_code}_connector_creation_error", str(e))
            return None

    def _create_subscription(
        self,
        connector: BaseWebsocketConnector
    ) -> Optional[BaseSubscription]:
        """
        거래소별 구독 객체 생성
        
        Args:
            connector: 웹소켓 연결 객체
            
        Returns:
            BaseSubscription: 구독 객체 또는 None (실패 시)
        """
        exchange_code = connector.exchange_code
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, f"{exchange_code}")
        
        try:
            # 거래소 코드에 해당하는 클래스 찾기
            subscription_class = EXCHANGE_SUBSCRIPTIONS.get(exchange_code)
            if not subscription_class:
                self.logger.warning(f"{exchange_kr} 해당 거래소의 구독 클래스를 찾을 수 없습니다")
                return None
                
            # 구독 객체 생성 (collector 객체 전달)
            self.logger.debug(f"{exchange_kr} 구독 객체 생성 시도 (클래스: {subscription_class.__name__})")
            subscription = subscription_class(connector, exchange_code, collector=self)
            self.logger.info(f"{exchange_kr} 구독 객체 생성 성공")
            return subscription
            
        except Exception as e:
            self.logger.error(f"{exchange_kr} 구독 객체 생성 실패: {str(e)}", exc_info=True)
            self._log_error(f"{exchange_code}_subscription_creation_error", str(e))
            return None

    async def _prepare_exchange_connections(self) -> bool:
        """거래소별 연결 및 구독 객체 준비

        Returns:
            bool: 성공 여부
        """
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
                self.logger.info(f"[{exchange_kr}] 컴포넌트 초기화 시작 ({len(symbols)}개 심볼)")
                
                # 커넥터 생성
                connector = self._create_connector(exchange, self.settings)
                if not connector:
                    self.logger.error(f"[{exchange_kr}] 커넥터 생성 실패, 해당 거래소 건너뜀")
                    continue
                    
                self.connectors[exchange] = connector
                
                # 구독 객체 생성
                subscription = self._create_subscription(connector)
                if not subscription:
                    self.logger.error(f"[{exchange_kr}] 구독 객체 생성 실패, 해당 거래소 건너뜀")
                    continue
                    
                self.subscriptions[exchange] = subscription
                
                # 상태 초기화
                self.exchange_status[exchange] = False
                
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
                self.logger.info(f"[{exchange_kr}] 컴포넌트 초기화 완료 ({len(symbols)}개 심볼)")
                
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

    async def _monitor_connection_health(self):
        """
        주기적으로 모든 거래소 연결 상태를 확인하고 필요시 재연결하는 태스크
        """
        check_interval = 30  # 30초마다 상태 확인
        self.logger.info(f"연결 상태 모니터링 시작 (점검 간격: {check_interval}초)")
        
        while self.status != "stopped":
            try:
                # 시스템이 시작 중이거나 실행 중인 경우에만 확인
                if self.status in ["starting", "running"]:
                    await self._check_all_connections()
                
                # 지정된 간격만큼 대기
                await asyncio.sleep(check_interval)
                
            except asyncio.CancelledError:
                self.logger.debug("연결 상태 모니터링 태스크 취소됨")
                break
            except Exception as e:
                self.logger.error(f"연결 상태 모니터링 중 오류: {str(e)}")
                await asyncio.sleep(check_interval)  # 오류 발생 시에도 계속 진행
                
        self.logger.debug("연결 상태 모니터링 종료")
    
    async def _check_all_connections(self):
        """
        모든 거래소의 연결 상태를 확인하고 필요시 재연결 시도
        """
        for exchange_code, connector in self.connectors.items():
            # 실제 상태와 내부 상태가 일치하는지 확인
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
            
            try:
                # 연결되어 있다고 표시되었지만 실제로 연결이 끊어진 경우
                if connector.is_connected:
                    # 웹소켓 객체 확인
                    ws = await connector.get_websocket()
                    if not ws:
                        self.logger.warning(f"{exchange_kr} 웹소켓 연결이 불일치: 상태는 연결됨이지만 웹소켓 객체가 없음")
                        # 연결 복구 시도
                        await self._reconnect_exchange(exchange_code, "connection_inconsistent")
                elif self.exchange_status.get(exchange_code, False):
                    # 내부 상태는 연결됨으로 표시되어 있지만 실제로는 끊어진 경우
                    self.logger.warning(f"{exchange_kr} 연결 상태가 불일치: ObCollector는 연결됨, 커넥터는 끊어짐")
                    # 연결 복구 시도
                    await self._reconnect_exchange(exchange_code, "status_inconsistent")
            except Exception as e:
                self.logger.error(f"{exchange_kr} 연결 상태 확인 중 오류: {str(e)}")
                # 오류 발생 시 연결 복구 시도
                await self._reconnect_exchange(exchange_code, f"check_error: {str(e)}")
    
    async def _reconnect_exchange(self, exchange_code: str, reason: str):
        """
        특정 거래소에 재연결 시도
        
        Args:
            exchange_code: 거래소 코드
            reason: 재연결 이유
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        
        # 재연결 카운터 증가
        self.increment_reconnect_counter(exchange_code)
        reconnect_count = self.reconnect_counts.get(exchange_code, 0)
        
        self.logger.info(f"🔄 [{exchange_kr}] 재연결 시도 #{reconnect_count} (이유: {reason})")
        
        try:
            # 1. 연결 객체 확인
            connector = self.connectors.get(exchange_code)
            if not connector:
                self.logger.error(f"[{exchange_kr}] 연결 객체가 없습니다")
                return False
                
            # 2. 연결 상태 업데이트
            self.exchange_status[exchange_code] = False
            self.metric_manager.update_exchange_status(exchange_code, False)
            
            # 3. 재연결 시도
            reconnect_success = await connector.reconnect()
            
            # 4. 재연결 실패 처리
            if not reconnect_success:
                self.logger.error(f"❌ [{exchange_kr}] 재연결 #{reconnect_count} 실패")
                self.update_error_counter(exchange_code, "reconnect_errors")
                return False
                
            # 5. 재연결 성공 처리
            self.logger.info(f"✅ [{exchange_kr}] 재연결 #{reconnect_count} 성공")
            self.exchange_status[exchange_code] = True
            self.metric_manager.update_exchange_status(exchange_code, True)
            
            # 6. 다시 구독 필요 - 구독 객체 확인
            subscription = self.subscriptions.get(exchange_code)
            if not subscription:
                self.logger.warning(f"[{exchange_kr}] 구독 객체가 없습니다")
                return True  # 재연결은 성공했으므로 True 반환
                
            # 7. 필터링된 심볼 목록 가져오기
            symbols = self.filtered_symbols.get(exchange_code, [])
            if not symbols:
                self.logger.warning(f"[{exchange_kr}] 구독할 심볼이 없습니다")
                return True  # 재연결은 성공했으므로 True 반환
                
            # 8. 재구독 수행
            self.logger.info(f"[{exchange_kr}] 재구독 시작 ({len(symbols)}개 심볼)")
            subscribe_result = await subscription.subscribe(symbols)
            
            # 9. 구독 결과 확인
            if not subscribe_result:
                self.logger.warning(f"[{exchange_kr}] 재구독 실패 또는 부분 성공")
                self.update_error_counter(exchange_code, "subscription_errors")
            else:
                self.logger.info(f"✅ [{exchange_kr}] 재구독 성공")
                
                # 10. 재구독 성공 시 메트릭 초기화
                # 메시지 카운터 명시적 초기화 (재시작을 위해)
                self.metric_manager.reset_message_counter(exchange_code)
                
                # 각 심볼의 타임스탬프 초기화
                for symbol in symbols:
                    self.update_symbol_timestamp(exchange_code, symbol, "reset")
                
            # 11. 구독 상태 업데이트
            self.metric_manager.update_subscription_status(
                exchange_code, 
                active=bool(subscribe_result), 
                symbol_count=len(symbols),
                symbols=symbols
            )
            
            # 12. 재연결/재구독 후 수신 확인 메시지 로깅
            self.logger.info(f"📊 [{exchange_kr}] 재연결 및 재구독 완료 - 데이터 수신이 곧 재개됩니다.")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ [{exchange_kr}] 재연결 #{reconnect_count} 처리 중 오류: {str(e)}", exc_info=True)
            self.update_error_counter(exchange_code, "reconnect_errors")
            return False
