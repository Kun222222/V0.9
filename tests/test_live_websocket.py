#!/usr/bin/env python
"""
실제 거래소 웹소켓 연결 및 구독 테스트
"""

import asyncio
import sys
import os
import time
from typing import Dict, List, Any
import signal
import logging

# 모듈 경로 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from crosskimp.common.config.app_config import get_config
from crosskimp.common.config.common_constants import Exchange, EXCHANGE_NAMES_KR
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.ob_collector.core.aggregator import Aggregator

# 모든 거래소 컴포넌트 임포트
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

# 로거 설정
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

# 테스트할 거래소 목록
TEST_EXCHANGES = [
    Exchange.UPBIT.value,
    Exchange.BITHUMB.value,
    Exchange.BINANCE.value,
    Exchange.BINANCE_FUTURE.value,
    Exchange.BYBIT.value,
    Exchange.BYBIT_FUTURE.value,
]

# 거래소 컴포넌트 매핑
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

# 메시지 수신 콜백 함수
async def on_data_received(symbol: str, data: Any, event_type: str):
    """데이터 수신 콜백"""
    if isinstance(data, dict) and 'timestamp' in data:
        logger.info(f"데이터 수신: {symbol} - 타임스탬프: {data['timestamp']}")
    else:
        logger.info(f"데이터 수신: {symbol} - 이벤트: {event_type}")

class WebSocketTester:
    """웹소켓 연결 및 구독 테스트 클래스"""
    
    def __init__(self):
        """초기화"""
        self.settings = get_config()
        self.connectors = {}
        self.subscriptions = {}
        self.filtered_symbols = {}
        self.messages_received = {}
        self.stop_event = asyncio.Event()
        
        # SIGINT 핸들러 설정
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, sig, frame):
        """시그널 핸들러"""
        logger.info("종료 신호 수신, 정리 중...")
        if not self.stop_event.is_set():
            self.stop_event.set()
    
    async def setup(self):
        """테스트 환경 설정"""
        logger.info("테스트 환경 설정 시작")
        
        # 심볼 필터링
        aggregator = Aggregator(self.settings)
        self.filtered_symbols = await aggregator.run_filtering()
        
        if not self.filtered_symbols:
            logger.error("필터링된 심볼이 없습니다")
            return False
        
        # 각 거래소별 최대 3개의 심볼만 테스트에 사용
        for exchange in self.filtered_symbols:
            if self.filtered_symbols[exchange]:
                self.filtered_symbols[exchange] = self.filtered_symbols[exchange][:3]
        
        # 테스트할 거래소만 필터링
        self.filtered_symbols = {k: v for k, v in self.filtered_symbols.items() if k in TEST_EXCHANGES}
        
        logger.info(f"테스트 대상 거래소: {list(self.filtered_symbols.keys())}")
        for exchange, symbols in self.filtered_symbols.items():
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            logger.info(f"{exchange_kr} 테스트 심볼: {symbols}")
        
        return True
    
    async def connect_exchange(self, exchange_code: str):
        """거래소 연결 및 구독"""
        try:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
            symbols = self.filtered_symbols.get(exchange_code, [])
            
            if not symbols:
                logger.warning(f"{exchange_kr} 구독할 심볼이 없습니다")
                return False
            
            # 커넥터 생성
            if exchange_code not in self.connectors:
                connector_class = EXCHANGE_CONNECTORS.get(exchange_code)
                if not connector_class:
                    logger.error(f"{exchange_kr} 커넥터 클래스를 찾을 수 없습니다")
                    return False
                
                connector = connector_class(self.settings, exchange_code)
                self.connectors[exchange_code] = connector
            else:
                connector = self.connectors[exchange_code]
            
            # 구독 객체 생성
            if exchange_code not in self.subscriptions:
                subscription_class = EXCHANGE_SUBSCRIPTIONS.get(exchange_code)
                if not subscription_class:
                    logger.error(f"{exchange_kr} 구독 클래스를 찾을 수 없습니다")
                    return False
                
                subscription = subscription_class(connector, exchange_code, on_data_received)
                self.subscriptions[exchange_code] = subscription
            else:
                subscription = self.subscriptions[exchange_code]
            
            # 연결 시도
            logger.info(f"{exchange_kr} 웹소켓 연결 시작")
            connected = await connector.connect()
            if not connected:
                logger.error(f"{exchange_kr} 웹소켓 연결 실패")
                return False
            
            logger.info(f"{exchange_kr} 웹소켓 연결 성공")
            
            # 메시지 수신 루프 시작
            subscription.message_loop_task = asyncio.create_task(subscription.message_loop())
            
            # 구독 시도
            logger.info(f"{exchange_kr} 심볼 구독 시작: {symbols}")
            success = await subscription.subscribe(symbols)
            
            if success:
                logger.info(f"{exchange_kr} 심볼 구독 성공")
                self.messages_received[exchange_code] = 0
                return True
            else:
                logger.error(f"{exchange_kr} 심볼 구독 실패")
                return False
            
        except Exception as e:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
            logger.error(f"{exchange_kr} 연결 및 구독 중 오류: {str(e)}")
            return False
    
    async def simulate_disconnect(self, exchange_code: str):
        """연결 끊김 시뮬레이션"""
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        logger.info(f"{exchange_kr} 연결 끊김 시뮬레이션")
        
        connector = self.connectors.get(exchange_code)
        if connector and connector.is_connected:
            await connector.disconnect()
            logger.info(f"{exchange_kr} 연결 끊김 성공")
            return True
        return False
    
    async def test_reconnect(self, exchange_code: str, wait_time: int = 5):
        """재연결 테스트"""
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        
        # 연결 끊김 시뮬레이션
        await self.simulate_disconnect(exchange_code)
        
        # 잠시 대기
        logger.info(f"{wait_time}초 대기 후 재연결 시도...")
        await asyncio.sleep(wait_time)
        
        # 재연결 시도
        logger.info(f"{exchange_kr} 재연결 시도")
        success = await self.connect_exchange(exchange_code)
        
        if success:
            logger.info(f"{exchange_kr} 재연결 및 재구독 성공")
            return True
        else:
            logger.error(f"{exchange_kr} 재연결 또는 재구독 실패")
            return False
    
    async def count_messages(self, duration: int = 10):
        """일정 시간 동안 수신된 메시지 카운트"""
        logger.info(f"{duration}초 동안 메시지 수신 확인...")
        
        # 모든 거래소의 초기 메시지 카운트 저장
        initial_counts = {}
        for exchange_code, subscription in self.subscriptions.items():
            if hasattr(subscription, '_message_count'):
                initial_counts[exchange_code] = subscription._message_count
        
        # 지정된 시간 동안 대기
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=duration)
        except asyncio.TimeoutError:
            pass
        
        # 최종 메시지 카운트 확인
        results = {}
        for exchange_code, subscription in self.subscriptions.items():
            if hasattr(subscription, '_message_count'):
                current_count = subscription._message_count
                initial = initial_counts.get(exchange_code, 0)
                count = current_count - initial
                exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
                results[exchange_code] = {
                    'exchange': exchange_kr,
                    'messages': count,
                    'per_second': count / duration if duration > 0 else 0
                }
        
        return results
    
    async def cleanup(self):
        """자원 정리"""
        logger.info("모든 연결 종료 및 자원 정리 중...")
        
        for exchange_code, subscription in self.subscriptions.items():
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
            try:
                logger.info(f"{exchange_kr} 구독 취소 중...")
                await subscription.unsubscribe()
                
                # 메시지 루프 태스크 취소
                if hasattr(subscription, 'message_loop_task') and subscription.message_loop_task:
                    subscription.message_loop_task.cancel()
                    try:
                        await subscription.message_loop_task
                    except asyncio.CancelledError:
                        pass
                
                logger.info(f"{exchange_kr} 구독 취소 완료")
            except Exception as e:
                logger.error(f"{exchange_kr} 구독 취소 중 오류: {str(e)}")
        
        for exchange_code, connector in self.connectors.items():
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
            try:
                logger.info(f"{exchange_kr} 연결 종료 중...")
                await connector.disconnect()
                logger.info(f"{exchange_kr} 연결 종료 완료")
            except Exception as e:
                logger.error(f"{exchange_kr} 연결 종료 중 오류: {str(e)}")
        
        self.connectors = {}
        self.subscriptions = {}
    
    async def run_test(self):
        """전체 테스트 실행"""
        if not await self.setup():
            logger.error("테스트 환경 설정 실패")
            return False
        
        try:
            # 1. 모든 거래소 연결 및 구독
            connection_results = {}
            for exchange_code in self.filtered_symbols:
                exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
                logger.info(f"=== {exchange_kr} 연결 및 구독 테스트 시작 ===")
                success = await self.connect_exchange(exchange_code)
                connection_results[exchange_code] = success
                logger.info(f"=== {exchange_kr} 연결 및 구독 테스트 결과: {'성공' if success else '실패'} ===")
            
            # 연결 실패한 거래소가 있는지 확인
            failed_exchanges = [k for k, v in connection_results.items() if not v]
            if failed_exchanges:
                failed_names = [EXCHANGE_NAMES_KR.get(ex, ex) for ex in failed_exchanges]
                logger.warning(f"연결 실패한 거래소: {failed_names}")
            
            # 2. 메시지 수신 확인 (10초)
            logger.info("=== 메시지 수신 확인 테스트 시작 ===")
            message_counts = await self.count_messages(10)
            
            logger.info("=== 메시지 수신 결과 ===")
            for exchange_code, result in message_counts.items():
                logger.info(f"{result['exchange']}: {result['messages']}개 메시지 ({result['per_second']:.1f}/초)")
            
            # 3. 연결 끊김 및 재연결 테스트 (거래소 하나만)
            if connection_results.get(Exchange.UPBIT.value, False):
                logger.info("=== 업비트 연결 끊김 및 재연결 테스트 시작 ===")
                reconnect_success = await self.test_reconnect(Exchange.UPBIT.value)
                logger.info(f"=== 업비트 재연결 테스트 결과: {'성공' if reconnect_success else '실패'} ===")
                
                # 재연결 후 메시지 수신 확인 (5초)
                if reconnect_success:
                    logger.info("=== 재연결 후 메시지 수신 확인 ===")
                    reconnect_counts = await self.count_messages(5)
                    
                    for exchange_code, result in reconnect_counts.items():
                        if exchange_code == Exchange.UPBIT.value:
                            logger.info(f"{result['exchange']} 재연결 후: {result['messages']}개 메시지 ({result['per_second']:.1f}/초)")
            
            logger.info("=== 모든 테스트 완료 ===")
            return True
            
        except Exception as e:
            logger.error(f"테스트 실행 중 오류: {str(e)}")
            return False
        finally:
            await self.cleanup()

async def main():
    """메인 함수"""
    try:
        logger.info("=== 실제 거래소 웹소켓 연결 테스트 시작 ===")
        tester = WebSocketTester()
        await tester.run_test()
        logger.info("=== 테스트 종료 ===")
    except KeyboardInterrupt:
        logger.info("사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        logger.error(f"테스트 중 예외 발생: {str(e)}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n프로그램이 종료되었습니다.") 