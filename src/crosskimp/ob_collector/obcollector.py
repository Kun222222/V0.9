"""
오더북 수집 시스템 관리자 모듈

이 모듈은 오더북 수집 시스템의 전체 기능을 조율하는 메인 진입점입니다.
심볼 관리, 웹소켓 연결, 오더북 수집을 종합적으로 관리합니다.
"""

import asyncio
from typing import Dict, List, Any, Optional

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import Exchange, SystemComponent, EXCHANGE_NAMES_KR
from crosskimp.common.config.app_config import get_config

# 기존 aggregator 대신 symbol_filter 모듈 사용
from crosskimp.ob_collector.symbol_filter.filter_manager import Aggregator
from crosskimp.ob_collector.usdtkrw.ws_usdtkrw import WsUsdtKrwMonitor
from crosskimp.ob_collector.orderbook.connection.connector_manager import ConnectionManager
from crosskimp.ob_collector.orderbook.connection.factory import get_factory
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
        
        # 서브 컴포넌트 초기화
        self.factory = get_factory()
        self.aggregator = Aggregator(self.config.exchange_settings)
        self.usdtkrw_monitor = WsUsdtKrwMonitor()
        
        # 내부 상태 변수
        self.filtered_symbols = {}  # 필터링된 심볼 목록
        self.connectors = {}  # 생성된 커넥터 객체
        
        # 실행 상태
        self.is_running = False
        self.initialization_complete = False
        
        # 데이터 관리자 가져오기
        self.data_manager = get_orderbook_data_manager()
        
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
            filter_result = await self.aggregator.run_filtering()
            
            # 새로운 필터 관리자는 {'symbols': {...}, 'prices': {...}} 형식으로 반환
            self.filtered_symbols = filter_result.get('symbols', {})
            
            if not self.filtered_symbols:
                self.logger.error("심볼 필터링 실패 또는 필터링된 심볼이 없습니다.")
                return False
                
            self.logger.info(f"심볼 필터링 완료: {len(self.filtered_symbols)}개 거래소")
            
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
            
            # 1. USDT/KRW 모니터링 시작 (별도 태스크)
            usdtkrw_task = asyncio.create_task(self.usdtkrw_monitor.start())
            self.logger.info("USDT/KRW 모니터링 시작됨")
            
            # 2. 거래소 연결 및 구독 (모니터링 전에 먼저 연결)
            await self._connect_and_subscribe_all()
            
            # 3. 연결이 완료된 후 모니터링 시작
            self.connection_manager.start_monitoring()
            
            # 4. 데이터 관리자의 통계 타이머 시작 (20초 간격으로 설정)
            self.data_manager.start_stats_timer(interval=20)
            self.logger.info("데이터 관리자 통계 타이머 시작됨 (간격: 20초)")
            
            self.logger.info("오더북 수집 시스템 시작 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"오더북 수집 시스템 시작 중 오류: {str(e)}", exc_info=True)
            self.is_running = False
            return False
            
    async def _connect_and_subscribe_all(self) -> None:
        """모든 거래소 연결 및 심볼 구독"""
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
            # 연결 시도
            self.logger.info(f"{exchange_kr} 웹소켓 연결 시작")
            connected = await connector.connect()
            
            if not connected:
                self.logger.error(f"{exchange_kr} 웹소켓 연결 실패")
                return False
            
            self.logger.info(f"{exchange_kr} 웹소켓 연결 성공")
            
            # 심볼 구독 - subscribe_symbols를 subscribe로 변경
            self.logger.info(f"{exchange_kr} 구독 시작 ({len(symbols)}개 심볼)")
            subscribed = await connector.subscribe(symbols)
            
            if not subscribed:
                self.logger.error(f"{exchange_kr} 심볼 구독 실패")
                return False
            
            # ConnectionManager에 구독 상태 업데이트 (추가)
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
            
            # 3. 데이터 관리자 통계 타이머 중지
            self.data_manager.stop_stats_timer()
            self.logger.info("데이터 관리자 통계 타이머 중지됨")
            
            # 4. 모든 거래소 연결 종료
            success = await self.connection_manager.close_all_connections()
            
            self.logger.info("오더북 수집 시스템 종료 완료")
            return success
            
        except Exception as e:
            self.logger.error(f"오더북 수집 시스템 종료 중 오류: {str(e)}", exc_info=True)
            return False

if __name__ == "__main__":
    print("이 모듈은 직접 실행할 수 없습니다. 다른 모듈에서 임포트하여 사용하세요.") 