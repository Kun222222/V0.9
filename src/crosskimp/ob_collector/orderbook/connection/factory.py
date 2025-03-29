"""
거래소 커넥터 팩토리 모듈

지원하는 거래소별 커넥터 객체를 생성하는 팩토리 클래스를 제공합니다.
"""

from typing import Dict, Any, Optional
import logging

from crosskimp.common.config.app_config import get_config
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, normalize_exchange_code, EXCHANGE_NAMES_KR

from crosskimp.ob_collector.orderbook.connection.exchanges.binance_f_connector import BinanceFutureConnector
from crosskimp.ob_collector.orderbook.connection.exchanges.binance_s_connector import BinanceSpotConnector
from crosskimp.ob_collector.orderbook.connection.exchanges.bybit_f_connector import BybitFutureConnector
from crosskimp.ob_collector.orderbook.connection.exchanges.bybit_s_connector import BybitSpotConnector
from crosskimp.ob_collector.orderbook.connection.exchanges.bithumb_s_connector import BithumbSpotConnector
from crosskimp.ob_collector.orderbook.connection.exchanges.upbit_s_connector import UpbitSpotConnector
from crosskimp.ob_collector.orderbook.connection.connector_interface import ExchangeConnectorInterface

class ExchangeConnectorFactory:
    """
    거래소 커넥터 팩토리 클래스
    
    거래소 식별자에 따라 적절한 커넥터 객체를 생성합니다.
    모든 커넥터는 ExchangeConnectorInterface를 구현합니다.
    """
    
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        self.config = get_config()
        self.connectors = {}  # 캐시된 커넥터 인스턴스
        
    def create_connector(self, exchange_code: str, connection_manager=None) -> Optional[ExchangeConnectorInterface]:
        """
        거래소 코드에 맞는 커넥터 객체 생성
        
        Args:
            exchange_code: 거래소 코드
            connection_manager: 연결 관리자 객체 (선택 사항)
            
        Returns:
            ExchangeConnectorInterface: 생성된 커넥터 객체
        """
        # 거래소 코드 정규화
        exchange_code = normalize_exchange_code(exchange_code)
        
        # 캐시된 인스턴스가 있으면 반환
        if exchange_code in self.connectors:
            return self.connectors[exchange_code]
            
        # 거래소별 커넥터 클래스 생성
        connector = None
        
        if exchange_code == Exchange.BINANCE_SPOT.value:
            connector = BinanceSpotConnector(connection_manager)
            self.logger.info(f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 커넥터 생성됨")
            
        elif exchange_code == Exchange.BINANCE_FUTURE.value:
            connector = BinanceFutureConnector(connection_manager)
            self.logger.info(f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 커넥터 생성됨")
            
        elif exchange_code == Exchange.BYBIT_FUTURE.value:
            connector = BybitFutureConnector(connection_manager)
            self.logger.info(f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 커넥터 생성됨")
            
        elif exchange_code == Exchange.BYBIT_SPOT.value:
            connector = BybitSpotConnector(connection_manager)
            self.logger.info(f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 커넥터 생성됨")
            
        elif exchange_code == Exchange.BITHUMB.value:
            connector = BithumbSpotConnector(connection_manager)
            self.logger.info(f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 커넥터 생성됨")
            
        elif exchange_code == Exchange.UPBIT.value:
            connector = UpbitSpotConnector(connection_manager)
            self.logger.info(f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 커넥터 생성됨")
            
        # TODO: 추가 거래소 지원 확장
            
        # 생성된 커넥터 캐싱
        if connector:
            self.connectors[exchange_code] = connector
            
        return connector
        
    def get_connector(self, exchange_code: str) -> Optional[ExchangeConnectorInterface]:
        """
        캐시된 거래소 커넥터 가져오기
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            Optional[ExchangeConnectorInterface]: 거래소 커넥터 또는 None (없는 경우)
        """
        return self.connectors.get(exchange_code)
        
    def get_supported_exchanges(self) -> list:
        """
        지원하는 거래소 코드 목록 반환
        
        Returns:
            list: 지원하는 거래소 코드 목록
        """
        return [
            Exchange.BINANCE_FUTURE.value, 
            Exchange.BINANCE_SPOT.value, 
            Exchange.BYBIT_FUTURE.value,
            Exchange.BYBIT_SPOT.value,
            Exchange.BITHUMB.value,
            Exchange.UPBIT.value
        ]
        
    def cleanup(self) -> None:
        """모든 커넥터 정리"""
        self.connectors.clear()
        self.logger.info("모든 거래소 커넥터 정리 완료")
        
# 싱글톤 인스턴스
_factory_instance = None

def get_factory() -> ExchangeConnectorFactory:
    """
    거래소 커넥터 팩토리 싱글톤 인스턴스 반환
    
    Returns:
        ExchangeConnectorFactory: 팩토리 싱글톤 인스턴스
    """
    global _factory_instance
    if _factory_instance is None:
        _factory_instance = ExchangeConnectorFactory()
    return _factory_instance 