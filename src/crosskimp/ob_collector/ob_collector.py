import asyncio
from typing import Dict, List, Optional
import logging

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.domains.process_component import ProcessComponent
from crosskimp.ob_collector.core.aggregator import Aggregator
from crosskimp.ob_collector.core.ws_usdtkrw import WsUsdtKrwMonitor
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.ob_collector.orderbook.util.component_factory import create_connector, create_subscription
from crosskimp.common.config.app_config import get_config
from crosskimp.common.config.common_constants import SystemComponent, EXCHANGE_NAMES_KR, normalize_exchange_code

logger = get_unified_logger(component=SystemComponent.ORDERBOOK.value)

class OrderbookCollector(ProcessComponent):
    def __init__(self):
        super().__init__(process_name="orderbook_collector")
        self.settings = get_config()
        self.connectors = {}  # 거래소별 커넥터
        self.subscriptions = {}  # 거래소별 구독 객체
        self.filtered_symbols = {}  # 필터링된 심볼 정보
        self.usdt_monitor = None
        
    async def setup(self):
        """
        ProcessComponent에서 상속받은 setup 메서드 재정의
        이벤트 버스 핸들러 등록 및 초기 설정 수행
        """
        logger.info("오더북 수집기 설정 시작")
        await super().setup()  # 부모 클래스의 setup 메서드 호출
        logger.info("오더북 수집기 설정 완료")
        return True
        
    async def initialize(self) -> bool:
        """시스템 초기화: 심볼 필터링 및 USDT/KRW 모니터 설정"""
        try:
            logger.info("오더북 수집 시스템 초기화 시작")
            
            # 1. 심볼 필터링
            aggregator = Aggregator(self.settings)
            self.filtered_symbols = await aggregator.run_filtering()
            
            if not self.filtered_symbols:
                logger.error("필터링된 심볼이 없습니다")
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
                connector = create_connector(exchange, self.settings)
                self.connectors[exchange] = connector
                
                # 구독 객체 생성 및 커넥터 연결
                subscription = create_subscription(connector)
                self.subscriptions[exchange] = subscription
                
                logger.info(f"{exchange_kr} 컴포넌트 초기화 완료")
            
            logger.info("오더북 수집 시스템 초기화 완료")
            return True
            
        except Exception as e:
            logger.error(f"초기화 중 오류 발생: {str(e)}", exc_info=True)
            return False
            
    async def start(self) -> bool:
        """
        ProcessComponent에서 상속받은 start 메서드 구현
        이 메서드는 시스템 이벤트 버스의 PROCESS_CONTROL 이벤트에 의해 호출됨
        """
        try:
            logger.info("오더북 수집 시작")
            
            # 1. 초기화가 안되어 있으면 초기화 먼저 실행
            if not self.connectors:
                init_success = await self.initialize()
                if not init_success:
                    logger.error("오더북 수집 초기화 실패")
                    return False
            
            # 2. USDT/KRW 모니터 시작 (비동기로 백그라운드에서 실행)
            if self.usdt_monitor:
                asyncio.create_task(self.usdt_monitor.start())
                
            # 3. 각 거래소별 웹소켓 연결 및 구독 시작
            connect_tasks = []
            for exchange, connector in self.connectors.items():
                # 연결 태스크 생성
                connect_task = asyncio.create_task(self._connect_and_subscribe(exchange))
                connect_tasks.append(connect_task)
                
            # 모든 연결 태스크 병렬 실행
            await asyncio.gather(*connect_tasks)
            
            logger.info("모든 거래소 연결 및 구독 시작 완료")
            return True
            
        except Exception as e:
            logger.error(f"시스템 시작 중 오류 발생: {str(e)}", exc_info=True)
            return False
            
    async def stop(self) -> bool:
        """
        ProcessComponent에서 상속받은 stop 메서드 구현
        이 메서드는 시스템 이벤트 버스의 PROCESS_CONTROL 이벤트에 의해 호출됨
        """
        try:
            logger.info("오더북 수집 중지 중")
            
            # 1. USDT/KRW 모니터 중지
            if self.usdt_monitor:
                await self.usdt_monitor.stop()
                
            # 2. 각 거래소별 구독 해제 및 연결 종료
            for exchange, subscription in self.subscriptions.items():
                try:
                    # 한글 거래소명 가져오기
                    exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
                    
                    # 구독 해제
                    await subscription.unsubscribe()
                    logger.info(f"{exchange_kr} 구독 해제 완료")
                    
                    # 연결 종료
                    connector = self.connectors.get(exchange)
                    if connector:
                        await connector.disconnect()
                        logger.info(f"{exchange_kr} 연결 종료 완료")
                except Exception as e:
                    exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
                    logger.error(f"{exchange_kr} 종료 중 오류: {str(e)}")
            
            self.connectors = {}
            self.subscriptions = {}
            
            logger.info("오더북 수집 중지 완료")
            return True
            
        except Exception as e:
            logger.error(f"시스템 중지 중 오류 발생: {str(e)}", exc_info=True)
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
                logger.warning(f"{exchange_kr} 컴포넌트 또는 심볼이 없음")
                return False
                
            # 1. 웹소켓 연결
            logger.info(f"{exchange_kr} 웹소켓 연결 시도")
            connected = await connector.connect()
            
            if not connected:
                logger.error(f"{exchange_kr} 웹소켓 연결 실패")
                return False
                
            logger.info(f"{exchange_kr} 웹소켓 연결 성공")
            
            # 2. 심볼 구독
            logger.info(f"{exchange_kr} 구독 시작 ({len(symbols)}개 심볼)")
            subscribe_result = await subscription.subscribe(symbols)
            
            if subscribe_result:
                logger.info(f"{exchange_kr} 구독 성공")
            else:
                logger.warning(f"{exchange_kr} 구독 실패 또는 부분 성공")
                
            return True
            
        except Exception as e:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            logger.error(f"{exchange_kr} 연결 및 구독 중 오류 발생: {str(e)}", exc_info=True)
            return False
