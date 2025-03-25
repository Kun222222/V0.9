import asyncio
import time
import random
from typing import Dict, List, Optional, Any
import logging

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.domains.process_component import ProcessComponent
from crosskimp.ob_collector.core.aggregator import Aggregator
from crosskimp.ob_collector.core.ws_usdtkrw import WsUsdtKrwMonitor
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.common.config.app_config import get_config
from crosskimp.common.events.system_types import EventCategory, SystemEventType, ObCollectorEventType
from crosskimp.common.config.common_constants import SystemComponent, EXCHANGE_NAMES_KR, Exchange, ProcessStatus

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

# 효율적인 메시지 카운팅을 위한 클래스
class HybridMessageCounter:
    """시간 윈도우와 샘플링을 결합한 메시지 카운터"""
    
    def __init__(self, window_size=60, sample_rate=0.1):
        """
        초기화
        
        Args:
            window_size: 유지할 시간 윈도우 크기(초)
            sample_rate: 샘플링 비율 (0.0~1.0)
        """
        self.window_size = window_size
        self.sample_rate = sample_rate
        self.windows = {}  # 시간별 샘플링된 카운트
        self.last_cleanup = time.time()
        self.total_estimated = 0  # 예상 총 메시지 수
        
    def update(self, count=1):
        """메시지 수신 업데이트"""
        # 샘플링 적용
        if random.random() >= self.sample_rate:
            # 총 예상치는 업데이트
            self.total_estimated += count
            return
            
        current_time = int(time.time())
        self.windows.setdefault(current_time, 0)
        self.windows[current_time] += count
        self.total_estimated += count
        
        # 주기적 정리 (매 100번째 샘플링된 업데이트마다)
        if sum(self.windows.values()) % 100 == 0:
            self._cleanup_old_windows(current_time)
            
    def _cleanup_old_windows(self, current_time=None):
        """오래된 윈도우 제거"""
        if current_time is None:
            current_time = int(time.time())
            
        self.windows = {
            t: c for t, c in self.windows.items()
            if current_time - t < self.window_size
        }
        
    def get_metrics(self):
        """현재 메트릭 조회"""
        current_time = int(time.time())
        
        # 필요시 오래된 윈도우 정리
        self._cleanup_old_windows(current_time)
        
        recent_windows = {
            t: c for t, c in self.windows.items()
            if current_time - t < self.window_size
        }
        
        if not recent_windows:
            return {"rate": 0, "total_estimated": self.total_estimated}
            
        # 시간 범위 계산
        time_range = min(self.window_size, 
                          current_time - min(recent_windows.keys()) + 1)
        
        sampled_count = sum(recent_windows.values())
        estimated_window_count = sampled_count / self.sample_rate
        rate = estimated_window_count / time_range
        
        return {
            "rate": rate,  # 추정 초당 메시지 수
            "total_estimated": self.total_estimated  # 추정 총 메시지 수
        }

# 오류 카운터 클래스
class ErrorCounter:
    """오류 추적 카운터"""
    
    def __init__(self):
        """오류 카운터 초기화"""
        self.error_types = {
            "connection_errors": 0,
            "parsing_errors": 0, 
            "sequence_errors": 0,
            "timeout_errors": 0,
            "other_errors": 0
        }
        self.last_error_time = 0  # 마지막 오류 시간
        self.total_errors = 0  # 총 오류 수
        
    def update(self, error_type, count=1):
        """오류 발생 기록"""
        if error_type in self.error_types:
            self.error_types[error_type] += count
        else:
            self.error_types["other_errors"] += count
            
        self.total_errors += count
        self.last_error_time = time.time()
        
    def get_metrics(self):
        """오류 메트릭 조회"""
        return {
            "total": self.total_errors,
            "types": self.error_types,
            "last_error_time": self.last_error_time
        }

class ObCollector:
    """
    오더북 수집기 클래스
    """
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)
        self.settings = get_config()
        self.connectors = {}  # 거래소별 커넥터
        self.subscriptions = {}  # 거래소별 구독 객체
        self.filtered_symbols = {}  # 필터링된 심볼 정보
        self.usdt_monitor = None
        
        # 거래소 상태 관리
        self.exchange_status = {}  # 거래소별 연결 상태 추적
        self.subscription_status = {}  # 거래소별 구독 상태 추적
        self.last_status_update = {}  # 마지막 상태 업데이트 시간
        self.connection_start_time = {}  # 각 거래소별 연결 시작 시간
        
        # 메트릭 추적을 위한 카운터 
        self.message_counters = {}  # 거래소별 메시지 카운터
        self.error_counters = {}    # 거래소별 오류 카운터
        self.reconnect_counts = {}  # 거래소별 재연결 횟수
        
        # 심볼별 업데이트 추적
        self.symbol_updates = {}    # 거래소+심볼별 마지막 업데이트 시간
        self.symbol_subscribe_times = {}  # 거래소+심볼별 구독 시작 시간
        
        # 시스템 시작 시간
        self.start_time = None
        self.component_status = {
            "websocket": "stopped",
            "aggregator": "stopped",
            "usdtkrw_monitor": "stopped"
        }
        
        # 상태 관리용 변수들
        self.system_status = {"status": "stopped", "details": {}}
        self.error_logs = []
        
    async def setup(self):
        """
        초기 설정 메서드
        이벤트 수신 및 필요한 설정을 수행합니다
        """
        self.logger.info("오더북 수집기 설정 시작")
        self.logger.info("오더북 수집기 설정 완료")
        return True
        
    async def initialize(self) -> bool:
        """시스템 초기화: 심볼 필터링 및 USDT/KRW 모니터 설정"""
        try:
            self.logger.info("오더북 수집 시스템 초기화 시작")
            
            # 메트릭 초기화
            self.start_time = time.time()
            self.component_status["aggregator"] = "running"
            
            # 1. 심볼 필터링
            aggregator = Aggregator(self.settings)
            self.filtered_symbols = await aggregator.run_filtering()
            
            if not self.filtered_symbols:
                self.logger.error("필터링된 심볼이 없습니다")
                self.component_status["aggregator"] = "error"
                return False
                
            # 2. USDT/KRW 모니터 초기화
            self.usdt_monitor = WsUsdtKrwMonitor()
            self.component_status["usdtkrw_monitor"] = "ready"
            
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
                
                # 메트릭 트래커 초기화
                self.init_metrics_for_exchange(exchange)
                
                # 심볼별 타임스탬프 초기화
                for symbol in symbols:
                    self.update_symbol_timestamp(exchange, symbol, "subscribe")
                
                self.logger.info(f"{exchange_kr} 컴포넌트 초기화 완료")
            
            self.logger.info("오더북 수집 시스템 초기화 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"초기화 중 오류 발생: {str(e)}", exc_info=True)
            self.component_status["aggregator"] = "error"
            # 오류 메트릭 업데이트
            for exchange in self.filtered_symbols.keys():
                self.update_error_counter(exchange, "connection_errors")
            return False
            
    async def start_collection(self) -> bool:
        """
        오더북 수집 시작
        실제 수집 작업을 시작합니다
        """
        try:
            self.logger.info("오더북 수집 시작")
            
            # 1. 초기화가 안되어 있으면 초기화 먼저 실행
            if not self.connectors:
                init_success = await self.initialize()
                if not init_success:
                    self.logger.error("오더북 수집 초기화 실패")
                    return False
            
            # 2. USDT/KRW 모니터 시작 (비동기로 백그라운드에서 실행)
            if self.usdt_monitor:
                asyncio.create_task(self.usdt_monitor.start())
                
            # 3. 시스템 시작 상태 업데이트
            system_details = {
                "exchange_count": len(self.connectors),
                "total_symbols": sum(len(symbols) for symbols in self.filtered_symbols.values())
            }
            self.system_status = {"status": "starting", "details": system_details}
                
            # 4. 각 거래소별 웹소켓 연결 및 구독 시작
            connect_tasks = []
            for exchange, connector in self.connectors.items():
                # 연결 태스크 생성
                connect_task = asyncio.create_task(self._connect_and_subscribe(exchange))
                connect_tasks.append(connect_task)
                
            # 모든 연결 태스크 병렬 실행
            await asyncio.gather(*connect_tasks)
            
            # 5. 시스템 시작 완료 상태 업데이트
            system_details = {
                "exchange_count": len(self.connectors),
                "connected_exchanges": sum(1 for status in self.exchange_status.values() if status),
                "total_symbols": sum(len(symbols) for symbols in self.filtered_symbols.values()),
                "active_subscriptions": sum(status["symbol_count"] for status in self.subscription_status.values() if status["active"])
            }
            self.system_status = {"status": "running", "details": system_details}
            
            # 6. 프로세스 상태 업데이트
            self.logger.debug(ProcessStatus.RUNNING)
            
            self.logger.info("모든 거래소 연결 및 구독 시작 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"시스템 시작 중 오류 발생: {str(e)}", exc_info=True)
            # 오류 기록
            self._log_error("system_start_error", str(e))
            return False
            
    async def stop_collection(self) -> bool:
        """
        오더북 수집 중지
        실행 중인 수집 작업을 중지합니다
        """
        try:
            self.logger.info("오더북 수집 중지 중")
            
            # 시스템 종료 시작 상태 업데이트
            system_details = {
                "exchange_count": len(self.connectors),
                "connected_exchanges": sum(1 for status in self.exchange_status.values() if status)
            }
            self.system_status = {"status": "stopping", "details": system_details}
            
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
                    self.subscription_status[exchange]["active"] = False
                    self.subscription_status[exchange]["symbol_count"] = 0
                    self.last_status_update[exchange] = time.time()
                    
                    # 연결 종료
                    connector = self.connectors.get(exchange)
                    if connector:
                        await connector.disconnect()
                        self.logger.info(f"{exchange_kr} 연결 종료 완료")
                        
                        # 상태 업데이트
                        self.exchange_status[exchange] = False
                        
                        # 연결 시작 시간 초기화
                        if exchange in self.connection_start_time:
                            del self.connection_start_time[exchange]
                        
                except Exception as e:
                    exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
                    self.logger.error(f"{exchange_kr} 종료 중 오류: {str(e)}")
            
            self.connectors = {}
            self.subscriptions = {}
            
            # 시스템 종료 완료 상태 업데이트
            self.system_status = {"status": "stopped", "details": {}}
            
            # 프로세스 상태 업데이트
            self.logger.debug(ProcessStatus.STOPPED)
            
            self.logger.info("오더북 수집 중지 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"시스템 중지 중 오류 발생: {str(e)}", exc_info=True)
            # 오류 기록
            self._log_error("system_stop_error", str(e))
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
            
            # 2. 연결 상태 업데이트
            self.exchange_status[exchange] = connected
            self.last_status_update[exchange] = time.time()
            
            # 연결 성공 시 연결 시작 시간 기록
            if connected:
                self.connection_start_time[exchange] = time.time()
                self.component_status["websocket"] = "running"
            else:
                # 연결 실패 시 오류 메트릭 업데이트
                self.update_error_counter(exchange, "connection_errors")
                self.increment_reconnect_counter(exchange)
            
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
            
            # 구독 시 심볼별 구독 시간 업데이트
            if subscription_active:
                for symbol in symbols:
                    self.update_symbol_timestamp(exchange, symbol, "subscribe")
            
            if subscribe_result:
                self.logger.info(f"{exchange_kr} 구독 성공")
            else:
                self.logger.warning(f"{exchange_kr} 구독 실패 또는 부분 성공")
                # 구독 실패 시 오류 메트릭 업데이트
                self.update_error_counter(exchange, "subscription_errors")
                
            return True
            
        except Exception as e:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            self.logger.error(f"{exchange_kr} 연결 및 구독 중 오류 발생: {str(e)}", exc_info=True)
            
            # 오류 기록
            self._log_error(f"{exchange}_connection_error", str(e))
            
            # 오류 메트릭 업데이트
            self.update_error_counter(exchange, "connection_errors")
            return False

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
        if exchange not in self.message_counters:
            self.message_counters[exchange] = HybridMessageCounter()
            
        if exchange not in self.error_counters:
            self.error_counters[exchange] = ErrorCounter()
            
        if exchange not in self.reconnect_counts:
            self.reconnect_counts[exchange] = 0
            
    def update_message_counter(self, exchange, count=1):
        """메시지 카운터 업데이트"""
        if exchange not in self.message_counters:
            self.init_metrics_for_exchange(exchange)
            
        self.message_counters[exchange].update(count)
        
    def update_error_counter(self, exchange, error_type, count=1):
        """오류 카운터 업데이트"""
        if exchange not in self.error_counters:
            self.init_metrics_for_exchange(exchange)
            
        self.error_counters[exchange].update(error_type, count)
        
    def increment_reconnect_counter(self, exchange):
        """재연결 카운터 증가"""
        if exchange not in self.reconnect_counts:
            self.reconnect_counts[exchange] = 0
            
        self.reconnect_counts[exchange] += 1
        
    def update_symbol_timestamp(self, exchange, symbol, update_type="data"):
        """심볼별 타임스탬프 업데이트"""
        key = f"{exchange}:{symbol}"
        current_time = time.time()
        
        if update_type == "subscribe":
            if key not in self.symbol_subscribe_times:
                self.symbol_subscribe_times[key] = current_time
        else:  # data update
            self.symbol_updates[key] = current_time
            
    # 통합 메트릭 제공 메서드
    # ======================================================
    
    def get_metrics(self):
        """
        모든 메트릭 데이터를 수집하여 반환
        
        Returns:
            Dict: 메트릭 데이터 딕셔너리
        """
        return {
            "connection": self._get_connection_metrics(),
            "message": self._get_message_metrics(),
            "subscription": self._get_subscription_metrics(),
            "error": self._get_error_metrics(),
            "system": self._get_system_metrics()
        }
        
    def _get_connection_metrics(self):
        """연결 관련 메트릭 수집"""
        current_time = time.time()
        result = {}
        
        for exchange in self.exchange_status:
            connected = self.exchange_status.get(exchange, False)
            
            # 업타임 계산
            uptime = 0
            if connected and exchange in self.connection_start_time:
                uptime = current_time - self.connection_start_time[exchange]
            
            result[exchange] = {
                "connected": connected,
                "uptime": uptime,
                "reconnect_count": self.reconnect_counts.get(exchange, 0),
                "last_connected": self.connection_start_time.get(exchange, 0)
            }
            
        return result
        
    def _get_message_metrics(self):
        """메시지 관련 메트릭 수집"""
        result = {}
        
        for exchange, counter in self.message_counters.items():
            metrics = counter.get_metrics()
            result[exchange] = {
                "total_count": metrics["total_estimated"],
                "rate": metrics["rate"],
                # 최근 메시지 시간은 symbol_updates에서 최신값을 찾아서 사용
                "last_message_time": self._get_latest_symbol_update(exchange)
            }
            
        return result
        
    def _get_latest_symbol_update(self, exchange):
        """특정 거래소에 대한 가장 최근 심볼 업데이트 시간 조회"""
        latest_time = 0
        prefix = f"{exchange}:"
        
        for key, timestamp in self.symbol_updates.items():
            if key.startswith(prefix) and timestamp > latest_time:
                latest_time = timestamp
                
        return latest_time
        
    def _get_subscription_metrics(self):
        """구독 관련 메트릭 수집"""
        result = {}
        
        for exchange, status in self.subscription_status.items():
            symbols_data = {}
            prefix = f"{exchange}:"
            
            # 구독 중인 심볼들의 상태 정보 수집
            for symbol in self.filtered_symbols.get(exchange, []):
                key = f"{exchange}:{symbol}"
                symbols_data[symbol] = {
                    "active": status["active"],
                    "last_update": self.symbol_updates.get(key, 0),
                    "subscribe_time": self.symbol_subscribe_times.get(key, 0)
                }
                
            result[exchange] = {
                "active": status["active"],
                "total_symbols": status["symbol_count"],
                "symbols": symbols_data
            }
            
        return result
        
    def _get_error_metrics(self):
        """오류 관련 메트릭 수집"""
        result = {}
        
        for exchange, counter in self.error_counters.items():
            metrics = counter.get_metrics()
            
            # 오류율 계산 (메시지 수 대비)
            error_rate = 0
            if exchange in self.message_counters:
                msg_metrics = self.message_counters[exchange].get_metrics()
                total_messages = msg_metrics["total_estimated"]
                if total_messages > 0:
                    error_rate = metrics["total"] / total_messages
                    
            result[exchange] = {
                "total": metrics["total"],
                "types": metrics["types"],
                "last_error_time": metrics["last_error_time"],
                "rate": error_rate
            }
            
        return result
        
    def _get_system_metrics(self):
        """시스템 상태 메트릭 수집"""
        current_time = time.time()
        uptime = 0
        
        if self.start_time:
            uptime = current_time - self.start_time
            
        return {
            "status": ProcessStatus.RUNNING if self.start_time else ProcessStatus.STOPPED,
            "start_time": self.start_time or 0,
            "uptime": uptime,
            "components": self.component_status
        }

    def _create_connector(self, exchange_code: str, settings: Dict[str, Any]) -> Optional[BaseWebsocketConnector]:
        """
        거래소별 웹소켓 연결 객체 생성
        
        Args:
            exchange_code: 거래소 코드
            settings: 설정 딕셔너리
            
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
                
            # 연결 객체 생성
            connector = connector_class(settings, exchange_code)
            self.logger.debug(f"{exchange_kr} 연결 객체 생성됨")
            return connector
            
        except Exception as e:
            self.logger.error(f"{exchange_kr} 연결 객체 생성 실패: {str(e)}")
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
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, f"[{exchange_code}]")
        
        try:
            # 거래소 코드에 해당하는 클래스 찾기
            subscription_class = EXCHANGE_SUBSCRIPTIONS.get(exchange_code)
            if not subscription_class:
                self.logger.warning(f"{exchange_kr} 해당 거래소의 구독 클래스를 찾을 수 없습니다")
                return None
                
            # 구독 객체 생성 (collector 객체 전달)
            subscription = subscription_class(connector, exchange_code, collector=self)
            self.logger.debug(f"{exchange_kr} 구독 객체 생성됨")
            return subscription
            
        except Exception as e:
            self.logger.error(f"{exchange_kr} 구독 객체 생성 실패: {str(e)}")
            return None
