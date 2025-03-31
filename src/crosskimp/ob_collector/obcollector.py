"""
오더북 수집 시스템 관리자 모듈

이 모듈은 오더북 수집 시스템의 전체 기능을 조율하는 메인 진입점입니다.
심볼 관리, 웹소켓 연결, 오더북 수집을 종합적으로 관리합니다.
"""

import asyncio
import time
import datetime
import json
from typing import Dict, List, Any, Optional, Callable, Set

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import Exchange, SystemComponent, EXCHANGE_NAMES_KR
from crosskimp.common.config.app_config import get_config

# 메트릭 모듈 임포트 제거

# 기존 컴포넌트 임포트
from crosskimp.ob_collector.core.aggregator import Aggregator, fetch_upbit_symbols_and_volume
from crosskimp.ob_collector.core.ws_usdtkrw import WsUsdtKrwMonitor
from crosskimp.ob_collector.orderbook.connection.connector_manager import ConnectionManager
from crosskimp.ob_collector.orderbook.connection.factory import get_factory, ExchangeConnectorFactory
from crosskimp.ob_collector.orderbook.connection.connector_interface import ExchangeConnectorInterface
from crosskimp.ob_collector.orderbook.data_handlers.ob_data_manager import get_orderbook_data_manager

class OrderbookCollectorManager:
    """
    오더북 수집 시스템 관리자
    
    이 클래스는 다음 기능을 통합합니다:
    1. 심볼 필터링 (Aggregator)
    2. USDT/KRW 모니터링 (WsUsdtKrwMonitor)
    3. 웹소켓 연결 관리 (ConnectionManager)
    4. 거래소 커넥터 생성 및 관리 (ExchangeConnectorFactory)
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        초기화
        
        Args:
            config_path: 설정 파일 경로 (선택 사항)
        """
        # 로거 설정
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        self.logger.info("오더북 수집 시스템 관리자 초기화 중...")
        
        # 설정 로드
        self.config = get_config()
        
        # ConnectionManager 먼저 초기화 (다른 컴포넌트의 의존성)
        self.connection_manager = ConnectionManager()
        
        # 메트릭 관리자 삭제 - 외부에서 주입받도록 변경됨
        
        # 서브 컴포넌트 초기화
        self.factory = get_factory()
        self.aggregator = Aggregator(self.config.exchange_settings)
        self.usdtkrw_monitor = WsUsdtKrwMonitor()
        
        # 내부 상태 변수
        self.filtered_symbols = {}  # 필터링된 심볼 목록
        self.symbol_prices = {}     # 심볼별 가격 정보 (추가됨)
        self.connectors = {}  # 생성된 커넥터 객체
        self.subscriptions = {}  # 구독 정보
        
        # 콜백 저장
        self.orderbook_callbacks = []
        
        # 실행 상태
        self.is_running = False
        self.initialization_complete = False
        
        # 데이터 관리자 가져오기
        self.data_manager = get_orderbook_data_manager()
        
        # 거래소 커넥터 팩토리 생성
        self.connector_factory = ExchangeConnectorFactory()
        
        # 통계 출력 태스크
        self.stats_task = None
        
        self.logger.info("오더북 수집 시스템 관리자 초기화 완료")
        
    async def initialize(self) -> bool:
        """
        시스템 초기화
        
        설정 로드, 심볼 필터링, 컴포넌트 초기화 등을 수행합니다.
        
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            self.logger.info("오더북 수집 시스템 초기화 시작...")
            
            # 1. 심볼 필터링 실행
            self.logger.info("심볼 필터링 시작...")
            
            # Aggregator를 통해 모든 거래소의 심볼 필터링 수행
            self.filtered_symbols = await self.aggregator.run_filtering()
            
            if not self.filtered_symbols:
                self.logger.error("심볼 필터링 실패 또는 필터링된 심볼이 없습니다.")
                return False
            
            # 업비트 가격 정보 가져오기 (필터링된 심볼에 대해서만)
            if Exchange.UPBIT.value in self.filtered_symbols:
                # 필터링된 심볼 목록 가져오기
                filtered_upbit_symbols = self.filtered_symbols[Exchange.UPBIT.value]
                
                # 모든 가격 정보 가져오기 (한 번만 호출)
                _, all_upbit_prices = await fetch_upbit_symbols_and_volume(
                    self.config.get_value_from_settings("trading.settings.min_volume_krw", 5000000000)
                )
                
                # 필터링된 심볼에 대한 가격 정보만 저장
                filtered_upbit_prices = {sym: all_upbit_prices.get(sym, 0) for sym in filtered_upbit_symbols}
                self.symbol_prices[Exchange.UPBIT.value] = filtered_upbit_prices
                
                self.logger.info(f"업비트 필터링된 심볼: {len(filtered_upbit_symbols)}개, 가격 정보 수집 완료")
            
            self.logger.info(f"심볼 필터링 완료: {len(self.filtered_symbols)}개 거래소")
            
            # 1.5 ConnectionManager에 재연결 콜백 등록
            self.connection_manager.set_reconnect_callback(self._connect_and_subscribe)
            self.logger.info("재연결 콜백 등록 완료")
            
            # 2. 거래소 커넥터 생성 및 등록
            supported_exchanges = self.factory.get_supported_exchanges()
            for exchange_code in supported_exchanges:
                if exchange_code in self.filtered_symbols and self.filtered_symbols[exchange_code]:
                    symbols = self.filtered_symbols[exchange_code]
                    self.logger.info(f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 초기화: {len(symbols)}개 심볼")
                    
                    try:
                        # Factory를 통해 커넥터 생성 (ConnectionManager 전달)
                        connector = self.factory.create_connector(exchange_code, self.connection_manager)
                        
                        # ConnectionManager에 등록
                        self.connection_manager.register_connector(exchange_code, connector)
                        
                        # 생성된 커넥터 참조 저장
                        self.connectors[exchange_code] = connector
                        self.logger.info(f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 커넥터 등록 성공")
                    except Exception as e:
                        self.logger.error(f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 커넥터 생성 및 등록 실패: {str(e)}")
            
            self.initialization_complete = True
            self.logger.info("오더북 수집 시스템 초기화 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"오더북 수집 시스템 초기화 중 오류: {str(e)}", exc_info=True)
            return False
            
    async def start(self) -> bool:
        """
        오더북 수집 시스템 시작
        
        USDT/KRW 모니터링 시작, 웹소켓 연결 및 구독을 수행합니다.
        
        Returns:
            bool: 시작 성공 여부
        """
        if not self.initialization_complete:
            self.logger.error("초기화가 완료되지 않았습니다. initialize() 메서드를 먼저 호출하세요.")
            return False
            
        try:
            self.logger.info("오더북 수집 시스템 시작 중...")
            self.is_running = True
            
            # 0. USDT/KRW 모니터를 OrderbookDataManager에 연결
            self.data_manager.set_usdt_monitor(self.usdtkrw_monitor)
            self.logger.info("USDT/KRW 모니터가 오더북 데이터 관리자에 연결되었습니다.")
            
            # 1. USDT/KRW 모니터링 시작 (별도 태스크)
            usdtkrw_task = asyncio.create_task(self.usdtkrw_monitor.start())
            self.logger.info("USDT/KRW 모니터링 시작됨")
            
            # 2. 거래소 연결 및 구독 (모니터링 전에 먼저 연결)
            await self._connect_and_subscribe_all()
            
            # 3. 연결이 완료된 후 모니터링 시작
            self.connection_manager.start_monitoring()
            
            # 통계 출력 태스크 시작
            self.stats_task = asyncio.create_task(self._print_stats_periodically())
            
            self.logger.info("오더북 수집 시스템 시작 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"오더북 수집 시스템 시작 중 오류: {str(e)}", exc_info=True)
            self.is_running = False
            return False
            
    async def _connect_and_subscribe_all(self) -> None:
        """모든 거래소 연결 및 심볼 구독"""
        """모든 거래소 연결 및 구독"""
        connect_tasks = []
        
        for exchange_code in self.connectors:
            # 현재 연결 상태 확인 - 이미 연결된 경우 스킵
            if self.connection_manager.is_exchange_connected(exchange_code):
                self.logger.info(f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 이미 연결되어 있습니다. 연결 시도 스킵")
                continue
                
            # 연결 태스크 추가
            connect_tasks.append(self._connect_and_subscribe(exchange_code))
            
        # 병렬로 모든 거래소 연결 시도
        if connect_tasks:
            results = await asyncio.gather(*connect_tasks, return_exceptions=True)
            
            # 결과 처리 - 예외를 포함할 수 있음
            success_count = 0
            for i, res in enumerate(results):
                if isinstance(res, Exception):
                    self.logger.error(f"거래소 연결 중 오류 발생: {str(res)}")
                elif res is True:
                    success_count += 1
                    
            self.logger.info(f"거래소 연결 결과: {success_count}/{len(connect_tasks)}개 성공")
        else:
            self.logger.warning("연결할 거래소가 없거나 모두 이미 연결되어 있습니다.")
            
    async def _connect_and_subscribe(self, exchange_code: str) -> bool:
        """
        특정 거래소 연결 및 심볼 구독
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            bool: 성공 여부
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        symbols = self.filtered_symbols.get(exchange_code, [])
        connector = self.connectors.get(exchange_code)
        
        if not connector:
            self.logger.error(f"{exchange_kr} 커넥터를 찾을 수 없습니다.")
            return False
        
        if not symbols:
            self.logger.warning(f"{exchange_kr}의 구독할 심볼이 없습니다.")
            return False
        
        try:
            # 1. 오더북 콜백 설정
            connector.add_orderbook_callback(self._on_orderbook_update)
            
            # 2. 추가 콜백 설정
            for callback in self.orderbook_callbacks:
                connector.add_orderbook_callback(callback)
            
            # 3. 연결 시도
            self.logger.info(f"{exchange_kr} 웹소켓 연결 시작")
            connected = await connector.connect()
            
            if not connected:
                self.logger.error(f"{exchange_kr} 웹소켓 연결 실패")
                return False
            
            self.logger.info(f"{exchange_kr} 웹소켓 연결 성공")
            
            # 4. 심볼 구독
            self.logger.info(f"{exchange_kr} 구독 시작 ({len(symbols)}개 심볼)")
            self.logger.debug(f"{exchange_kr} 구독 시작: {sorted(symbols)}")
            
            # 업비트인 경우 가격 정보와 함께 전달
            if exchange_code == Exchange.UPBIT.value and hasattr(connector, 'connection_strategy'):
                # 업비트 가격 정보 가져오기
                prices = self.symbol_prices.get(Exchange.UPBIT.value, {})
                
                # 로그 추가
                self.logger.debug(f"구독 요청 - 필터링된 심볼 목록: {len(symbols)}개, 가격 정보: {len(prices)}개")
                if len(symbols) != len(prices):
                    self.logger.warning(f"심볼 수({len(symbols)})와 가격 정보 수({len(prices)})가 일치하지 않습니다")
                
                # 커넥터의 connection_strategy에 직접 구독 요청 (가격 정보 포함)
                # 필터링된 심볼에 대한 가격 정보만 전달
                filtered_prices = {sym: prices.get(sym, 0) for sym in symbols}
                
                subscribed = await connector.connection_strategy.subscribe(
                    connector.ws, 
                    symbols, 
                    filtered_prices
                )
            else:
                # 그 외 거래소는 일반 구독
                subscribed = await connector.subscribe(symbols)
            
            if not subscribed:
                self.logger.error(f"{exchange_kr} 심볼 구독 실패")
                return False
            
            # 5. ConnectionManager에 구독 상태 업데이트
            self.connection_manager.update_subscription_status(
                exchange_code, 
                active=True, 
                symbols=symbols, 
                symbol_count=len(symbols)
            )
            
            self.logger.info(f"{exchange_kr} 구독 완료: {len(symbols)}개 심볼")
            return True
            
        except Exception as e:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
            self.logger.error(f"{exchange_kr} 연결 및 구독 중 오류: {str(e)}", exc_info=True)
            return False

    def add_orderbook_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        오더북 업데이트 콜백 등록
        
        Args:
            callback: 오더북 업데이트 콜백 함수
        """
        self.orderbook_callbacks.append(callback)
        
        # 이미 생성된 커넥터에도 콜백 등록
        for exchange_code, connector in self.connectors.items():
            if connector:
                connector.add_orderbook_callback(callback)
                
        self.logger.debug(f"오더북 콜백 등록 완료 (총 {len(self.orderbook_callbacks)}개)")
        
    def get_exchange_status(self) -> Dict[str, bool]:
        """
        거래소 연결 상태 조회
        
        Returns:
            Dict[str, bool]: 거래소별 연결 상태
        """
        return self.connection_manager.get_connection_status()
        
    def get_usdtkrw_price(self, exchange: Optional[str] = None) -> float:
        """
        USDT/KRW 가격 조회
        
        Args:
            exchange: 거래소 코드 (None이면 평균 가격)
            
        Returns:
            float: USDT/KRW 가격
        """
        if exchange:
            return self.usdtkrw_monitor.get_price(exchange)
        else:
            # 모든 가격의 평균 (0 제외)
            prices = self.usdtkrw_monitor.get_all_prices()
            valid_prices = [p for p in prices.values() if p > 0]
            if valid_prices:
                return sum(valid_prices) / len(valid_prices)
            return 0.0
            
    async def stop(self) -> bool:
        """
        오더북 수집 시스템 종료
        
        모든 웹소켓 연결 종료, 태스크 취소 등을 수행합니다.
        
        Returns:
            bool: 종료 성공 여부
        """
        try:
            self.logger.info("오더북 수집 시스템 종료 중...")
            self.is_running = False
            
            # 1. 모니터링 중지
            self.connection_manager.stop_monitoring()
            
            # 2. USDT/KRW 모니터링 중지
            await self.usdtkrw_monitor.stop()
            
            # 3. 모든 거래소 연결 종료
            success = await self.connection_manager.close_all_connections()
            
            # 통계 태스크 종료
            if self.stats_task and not self.stats_task.done():
                self.stats_task.cancel()
                try:
                    await self.stats_task
                except asyncio.CancelledError:
                    pass
            
            self.logger.info("오더북 수집 시스템 종료 완료")
            return success
            
        except Exception as e:
            self.logger.error(f"오더북 수집 시스템 종료 중 오류: {str(e)}", exc_info=True)
            return False

    def _on_orderbook_update(self, data):
        """
        오더북 업데이트 처리
        
        Args:
            data: 오더북 업데이트 데이터
        """
        try:
            # 심볼 정보 추출 및 타임스탬프 업데이트
            exchange = data.get("exchange", "unknown")
            symbol = data.get("symbol", "unknown")
            self.connection_manager.update_symbol_timestamp(exchange, symbol)
            
            # 콜백 실행
            for callback in self.orderbook_callbacks:
                try:
                    callback(data)
                except Exception as e:
                    # 오류 카운트 제거 - data_manager로 이관
                    self.logger.error(f"오더북 콜백 실행 중 오류: {str(e)}")
                    # 대신 data_manager에 오류 기록
                    self.data_manager.log_error(exchange)
                    
        except Exception as e:
            exchange = data.get("exchange", "unknown") if isinstance(data, dict) else "unknown"
            # 오류 카운트 제거 - data_manager로 이관
            self.logger.error(f"오더북 업데이트 처리 중 오류: {str(e)}")
            # 대신 data_manager에 오류 기록
            self.data_manager.log_error(exchange)
    
    async def _print_stats_periodically(self):
        """주기적으로 데이터 관리자 통계 출력"""
        try:
            # 데이터 관리자에서 통계 출력 주기 가져오기
            stats_interval = self.data_manager.stats_interval
            
            while self.is_running:
                # 지정된 간격마다 통계 출력
                await asyncio.sleep(stats_interval)
                
                if self.is_running:
                    self.data_manager.print_stats()
                    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.error(f"통계 출력 태스크 오류: {str(e)}")

    def get_raw_metrics_data(self):
        """
        메트릭 계산에 필요한 원시 데이터 제공
        
        Returns:
            Dict: 원시 메트릭 데이터 (USDT/KRW 가격만)
        """
        try:
            # 기본 메트릭 데이터 단순화 - USDT/KRW 가격만 반환
            return {
                "usdtkrw_prices": self.usdtkrw_monitor.get_all_prices()
            }
        except Exception as e:
            self.logger.error(f"원시 메트릭 데이터 수집 중 오류: {str(e)}", exc_info=True)
            return {}

# 사용 예시
async def main():
    # 관리자 객체 생성
    manager = OrderbookCollectorManager()
    
    # 오더북 콜백 등록
    manager.add_orderbook_callback(on_orderbook_update)
    
    # 시스템 초기화
    if not await manager.initialize():
        print("초기화 실패")
        return
        
    # 시스템 시작
    if not await manager.start():
        print("시작 실패")
        return
        
    try:
        # 시스템 실행 중 대기
        while True:
            await asyncio.sleep(10)
            # 상태 확인, 모니터링 등
    finally:
        # 종료
        await manager.stop()

def on_orderbook_update(data):
    # 오더북 업데이트 처리 예시
    symbol = data.get("symbol", "")
    exchange = data.get("exchange", "")
    
    # 로거 객체 가져오기
    logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
    
if __name__ == "__main__":
    asyncio.run(main()) 