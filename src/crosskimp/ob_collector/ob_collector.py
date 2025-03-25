import asyncio
import time
from typing import Dict, List, Optional, Any, Type
import logging

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.domains.process_component import ProcessComponent
from crosskimp.ob_collector.core.aggregator import Aggregator
from crosskimp.ob_collector.core.ws_usdtkrw import WsUsdtKrwMonitor
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.common.config.app_config import get_config
from crosskimp.common.config.common_constants import SystemComponent, EXCHANGE_NAMES_KR, normalize_exchange_code, Exchange, EventType, ProcessStatus

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

class ObCollector(ProcessComponent):
    """
    오더북 수집기 클래스
    """
    def __init__(self):
        """초기화"""
        super().__init__(process_name="ob_collector")
        self.settings = get_config()
        self.connectors = {}  # 거래소별 커넥터
        self.subscriptions = {}  # 거래소별 구독 객체
        self.filtered_symbols = {}  # 필터링된 심볼 정보
        self.usdt_monitor = None
        
        # 거래소 상태 관리
        self.exchange_status = {}  # 거래소별 연결 상태 추적
        self.subscription_status = {}  # 거래소별 구독 상태 추적
        self.last_status_update = {}  # 마지막 상태 업데이트 시간
        
        # 상태 보고 태스크
        self.status_reporter_task = None
        self.stop_event = asyncio.Event()
        
    async def setup(self):
        """
        ProcessComponent에서 상속받은 setup 메서드 재정의
        이벤트 버스 핸들러 등록 및 초기 설정 수행
        """
        self.logger.info("오더북 수집기 설정 시작")
        await super().setup()  # 부모 클래스의 setup 메서드 호출
        self.logger.info("오더북 수집기 설정 완료")
        return True
        
    async def initialize(self) -> bool:
        """시스템 초기화: 심볼 필터링 및 USDT/KRW 모니터 설정"""
        try:
            self.logger.info("오더북 수집 시스템 초기화 시작")
            
            # 1. 심볼 필터링
            aggregator = Aggregator(self.settings)
            self.filtered_symbols = await aggregator.run_filtering()
            
            if not self.filtered_symbols:
                self.logger.error("필터링된 심볼이 없습니다")
                return False
                
            # 2. USDT/KRW 모니터 초기화
            self.usdt_monitor = WsUsdtKrwMonitor()
            
            # 3. 각 거래소별 커넥터 및 구독 객체 초기화
            for exchange, symbols in self.filtered_symbols.items():
                if not symbols:
                    continue
                
                # 한글 거래소명 가져오기
                exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
                    
                # 커넥터 생성
                connector = self._create_connector(exchange, self.settings)
                self.connectors[exchange] = connector
                
                # 구독 객체 생성 및 커넥터 연결
                subscription = self._create_subscription(connector)
                self.subscriptions[exchange] = subscription
                
                # 상태 초기화
                self.exchange_status[exchange] = False  # 연결 상태
                self.subscription_status[exchange] = {
                    "active": False,
                    "symbol_count": len(symbols)
                }
                self.last_status_update[exchange] = time.time()
                
                self.logger.info(f"{exchange_kr} 컴포넌트 초기화 완료")
            
            self.logger.info("오더북 수집 시스템 초기화 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"초기화 중 오류 발생: {str(e)}", exc_info=True)
            return False
            
    async def start(self) -> bool:
        """
        ProcessComponent에서 상속받은 start 메서드 구현
        이 메서드는 시스템 이벤트 버스의 PROCESS_CONTROL 이벤트에 의해 호출됨
        """
        try:
            self.logger.info("오더북 수집 시작")
            self.stop_event.clear()
            
            # 1. 초기화가 안되어 있으면 초기화 먼저 실행
            if not self.connectors:
                init_success = await self.initialize()
                if not init_success:
                    self.logger.error("오더북 수집 초기화 실패")
                    return False
            
            # 2. USDT/KRW 모니터 시작 (비동기로 백그라운드에서 실행)
            if self.usdt_monitor:
                asyncio.create_task(self.usdt_monitor.start())
                
            # 3. 시스템 시작 이벤트 발행
            system_details = {
                "exchange_count": len(self.connectors),
                "total_symbols": sum(len(symbols) for symbols in self.filtered_symbols.values())
            }
            await self._publish_status_update("system", None, {
                "status": "starting",
                "details": system_details
            })
                
            # 4. 각 거래소별 웹소켓 연결 및 구독 시작
            connect_tasks = []
            for exchange, connector in self.connectors.items():
                # 연결 태스크 생성
                connect_task = asyncio.create_task(self._connect_and_subscribe(exchange))
                connect_tasks.append(connect_task)
                
            # 모든 연결 태스크 병렬 실행
            await asyncio.gather(*connect_tasks)
            
            # 5. 정기적인 상태 보고 태스크 시작
            self.status_reporter_task = asyncio.create_task(self._start_status_reporter())
            
            # 6. 시스템 시작 완료 이벤트 발행
            system_details = {
                "exchange_count": len(self.connectors),
                "connected_exchanges": sum(1 for status in self.exchange_status.values() if status),
                "total_symbols": sum(len(symbols) for symbols in self.filtered_symbols.values()),
                "active_subscriptions": sum(status["symbol_count"] for status in self.subscription_status.values() if status["active"])
            }
            await self._publish_status_update("system", None, {
                "status": "running",
                "details": system_details
            })
            
            # 7. 프로세스 상태 업데이트
            await self._publish_status(ProcessStatus.RUNNING)
            
            self.logger.info("모든 거래소 연결 및 구독 시작 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"시스템 시작 중 오류 발생: {str(e)}", exc_info=True)
            # 오류 이벤트 발행
            await self._publish_error("system_start_error", str(e))
            return False
            
    async def stop(self) -> bool:
        """
        ProcessComponent에서 상속받은 stop 메서드 구현
        이 메서드는 시스템 이벤트 버스의 PROCESS_CONTROL 이벤트에 의해 호출됨
        """
        try:
            self.logger.info("오더북 수집 중지 중")
            
            # 0. 상태 보고 태스크 종료 신호 설정
            self.stop_event.set()
            
            # 1. 상태 보고 태스크 종료 대기
            if self.status_reporter_task and not self.status_reporter_task.done():
                try:
                    await asyncio.wait_for(self.status_reporter_task, timeout=2.0)
                except asyncio.TimeoutError:
                    self.status_reporter_task.cancel()
                    try:
                        await self.status_reporter_task
                    except asyncio.CancelledError:
                        pass
                        
            # 2. 시스템 종료 시작 이벤트 발행
            system_details = {
                "exchange_count": len(self.connectors),
                "connected_exchanges": sum(1 for status in self.exchange_status.values() if status)
            }
            await self._publish_status_update("system", None, {
                "status": "stopping",
                "details": system_details
            })
            
            # 3. USDT/KRW 모니터 중지
            if self.usdt_monitor:
                await self.usdt_monitor.stop()
                
            # 4. 각 거래소별 구독 해제 및 연결 종료
            for exchange, subscription in self.subscriptions.items():
                try:
                    # 한글 거래소명 가져오기
                    exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
                    
                    # 구독 해제
                    await subscription.unsubscribe()
                    self.logger.info(f"{exchange_kr} 구독 해제 완료")
                    
                    # 상태 업데이트
                    self.subscription_status[exchange]["active"] = False
                    self.last_status_update[exchange] = time.time()
                    
                    # 구독 해제 이벤트 발행
                    await self._publish_status_update("subscription", exchange, {
                        "active": False,
                        "symbol_count": 0
                    })
                    
                    # 연결 종료
                    connector = self.connectors.get(exchange)
                    if connector:
                        await connector.disconnect()
                        self.logger.info(f"{exchange_kr} 연결 종료 완료")
                        
                        # 상태 업데이트
                        self.exchange_status[exchange] = False
                        
                        # 연결 종료 이벤트 발행
                        await self._publish_status_update("connection", exchange, {
                            "connected": False,
                            "last_update": time.time()
                        })
                except Exception as e:
                    exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
                    self.logger.error(f"{exchange_kr} 종료 중 오류: {str(e)}")
            
            self.connectors = {}
            self.subscriptions = {}
            
            # 5. 시스템 종료 완료 이벤트 발행
            await self._publish_status_update("system", None, {
                "status": "stopped",
                "details": {}
            })
            
            # 6. 프로세스 상태 업데이트
            await self._publish_status(ProcessStatus.STOPPED)
            
            self.logger.info("오더북 수집 중지 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"시스템 중지 중 오류 발생: {str(e)}", exc_info=True)
            # 오류 이벤트 발행
            await self._publish_error("system_stop_error", str(e))
            return False
            
    async def _connect_and_subscribe(self, exchange: str) -> bool:
        """특정 거래소 연결 및 구독 처리"""
        try:
            connector = self.connectors.get(exchange)
            subscription = self.subscriptions.get(exchange)
            symbols = self.filtered_symbols.get(exchange, [])
            
            # 한글 거래소명 가져오기
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            
            if not connector or not subscription or not symbols:
                self.logger.warning(f"{exchange_kr} 컴포넌트 또는 심볼이 없음")
                return False
                
            # 1. 웹소켓 연결
            self.logger.info(f"{exchange_kr} 웹소켓 연결 시도")
            connected = await connector.connect()
            
            # 2. 연결 상태 업데이트 및 이벤트 발행
            self.exchange_status[exchange] = connected
            self.last_status_update[exchange] = time.time()
            
            # 연결 상태 이벤트 발행
            await self._publish_status_update("connection", exchange, {
                "connected": connected,
                "last_update": time.time()
            })
            
            if not connected:
                self.logger.error(f"{exchange_kr} 웹소켓 연결 실패")
                return False
                
            self.logger.info(f"{exchange_kr} 웹소켓 연결 성공")
            
            # 3. 심볼 구독
            self.logger.info(f"{exchange_kr} 구독 시작 ({len(symbols)}개 심볼)")
            subscribe_result = await subscription.subscribe(symbols)
            
            # 4. 구독 상태 업데이트
            subscription_active = bool(subscribe_result)
            self.subscription_status[exchange]["active"] = subscription_active
            self.subscription_status[exchange]["symbol_count"] = len(symbols)
            self.last_status_update[exchange] = time.time()
            
            # 구독 상태 이벤트 발행
            await self._publish_status_update("subscription", exchange, {
                "active": subscription_active,
                "symbol_count": len(symbols)
            })
            
            if subscribe_result:
                self.logger.info(f"{exchange_kr} 구독 성공")
            else:
                self.logger.warning(f"{exchange_kr} 구독 실패 또는 부분 성공")
                
            return True
            
        except Exception as e:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            self.logger.error(f"{exchange_kr} 연결 및 구독 중 오류 발생: {str(e)}", exc_info=True)
            
            # 오류 발생 이벤트 발행
            await self._publish_error(f"{exchange}_connection_error", str(e))
            return False

    # 이벤트 발행 관련 메서드 (통합 접근법)
    # ======================================================
    
    async def _publish_status_update(self, update_type: str, exchange: Optional[str], status_data: Dict[str, Any]) -> None:
        """
        통합된 상태 업데이트 이벤트 발행
        
        Args:
            update_type: 업데이트 유형 ('system', 'connection', 'subscription')
            exchange: 거래소 코드 (system 유형일 경우 None)
            status_data: 상태 데이터
        """
        try:
            event_data = {
                "process_name": self.process_name,
                "event_type": "status_update",
                "update_type": update_type,
                "timestamp": time.time()
            }
            
            if exchange:
                event_data["exchange"] = exchange
                
            event_data["status"] = status_data
            
            await self.event_bus.publish(EventType.PROCESS_STATUS, event_data)
        except Exception as e:
            self.logger.error(f"상태 업데이트 이벤트 발행 실패: {str(e)}")
    
    async def _publish_error(self, error_type: str, error_message: str) -> None:
        """오류 이벤트 발행 (중요 오류만 선별적으로 전송)"""
        try:
            await self.event_bus.publish(EventType.ERROR, {
                "process_name": self.process_name,
                "error_type": error_type,
                "message": error_message,
                "timestamp": time.time()
            })
        except Exception as e:
            self.logger.error(f"오류 이벤트 발행 실패: {str(e)}")
    
    async def _start_status_reporter(self) -> None:
        """
        정기적인 상태 보고 태스크
        
        5초마다 각 거래소별 연결 및 구독 상태를 이벤트로 발행합니다.
        """
        try:
            self.logger.info("상태 보고 태스크 시작")
            
            while not self.stop_event.is_set():
                try:
                    # 각 거래소별 상태 전송
                    await self._publish_exchange_statuses()
                    
                    # 5초 대기 (또는 중지 이벤트 발생 시까지)
                    try:
                        await asyncio.wait_for(self.stop_event.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        # 타임아웃은 정상적인 상황 (5초 경과)
                        pass
                        
                except Exception as e:
                    self.logger.error(f"상태 정보 발행 중 오류: {str(e)}")
                    await asyncio.sleep(1.0)  # 오류 발생 시 짧게 대기 후 재시도
                    
            self.logger.info("상태 보고 태스크 종료")
            
        except asyncio.CancelledError:
            self.logger.info("상태 보고 태스크 취소됨")
            raise
        
        except Exception as e:
            self.logger.error(f"상태 보고 태스크 예외 발생: {str(e)}")
    
    async def _publish_exchange_statuses(self) -> None:
        """각 거래소별 연결 및 구독 상태 개별 발행"""
        try:
            # 각 거래소별로 상태 발행
            for exchange in self.exchange_status:
                # 1. 연결 상태 전송
                connected = self.exchange_status.get(exchange, False)
                await self._publish_status_update("connection", exchange, {
                    "connected": connected,
                    "last_update": self.last_status_update.get(exchange, time.time())
                })
                
                # 2. 구독 상태 전송
                subscription_status = self.subscription_status.get(exchange, {"active": False, "symbol_count": 0})
                await self._publish_status_update("subscription", exchange, {
                    "active": subscription_status["active"],
                    "symbol_count": subscription_status["symbol_count"]
                })
                
        except Exception as e:
            self.logger.error(f"거래소 상태 발행 중 오류: {str(e)}")

    def _create_connector(self, exchange_code: str, settings: Dict[str, Any], on_status_change=None) -> Optional[BaseWebsocketConnector]:
        """
        거래소별 웹소켓 연결 객체 생성
        
        Args:
            exchange_code: 거래소 코드
            settings: 설정 딕셔너리
            on_status_change: 연결 상태 변경 시 호출될 콜백 함수
            
        Returns:
            BaseWebsocketConnector: 웹소켓 연결 객체 또는 None (실패 시)
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, f"[{exchange_code}]")
        
        try:
            # 거래소 코드에 해당하는 클래스 찾기
            connector_class = EXCHANGE_CONNECTORS.get(exchange_code)
            if not connector_class:
                self.logger.warning(f"{exchange_kr} 해당 거래소의 연결 클래스를 찾을 수 없습니다")
                return None
                
            # 연결 객체 생성 (콜백 전달)
            connector = connector_class(settings, exchange_code, on_status_change)
            self.logger.debug(f"{exchange_kr} 연결 객체 생성됨")
            return connector
            
        except Exception as e:
            self.logger.error(f"{exchange_kr} 연결 객체 생성 실패: {str(e)}")
            return None

    def _create_subscription(
        self,
        connector: BaseWebsocketConnector,
        on_data_received=None
    ) -> Optional[BaseSubscription]:
        """
        거래소별 구독 객체 생성
        
        Args:
            connector: 웹소켓 연결 객체
            on_data_received: 데이터 수신 시 호출될 콜백 함수
            
        Returns:
            BaseSubscription: 구독 객체 또는 None (실패 시)
        """
        exchange_code = connector.exchange_code
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, f"[{exchange_code}]")
        
        try:
            # 거래소 코드에 해당하는 클래스 찾기
            subscription_class = EXCHANGE_SUBSCRIPTIONS.get(exchange_code)
            if not subscription_class:
                self.logger.warning(f"{exchange_kr} 해당 거래소의 구독 클래스를 찾을 수 없습니다")
                return None
                
            # 구독 객체 생성 (콜백 전달)
            subscription = subscription_class(connector, exchange_code, on_data_received)
            self.logger.debug(f"{exchange_kr} 구독 객체 생성됨")
            return subscription
            
        except Exception as e:
            self.logger.error(f"{exchange_kr} 구독 객체 생성 실패: {str(e)}")
            return None
