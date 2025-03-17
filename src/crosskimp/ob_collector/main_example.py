"""
오더북 수집기 메인 모듈 예제

이 모듈은 OrderManager와 WebsocketManager를 함께 사용하는 예제를 제공합니다.
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timedelta

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import LOG_SYSTEM

# 기존 WebsocketManager 임포트
from crosskimp.ob_collector.orderbook.websocket.base_ws_manager import WebsocketManager

# 새로운 OrderManager 임포트
from crosskimp.ob_collector.orderbook.order_manager import integrate_with_websocket_manager

# 심볼 필터링을 위한 Aggregator 임포트
from crosskimp.ob_collector.core.aggregator import Aggregator

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class OrderbookCollector:
    """
    오더북 수집기 클래스
    
    OrderManager와 WebsocketManager를 함께 사용하여 오더북 데이터를 수집합니다.
    """
    
    def __init__(self, settings: dict):
        """
        초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        self.settings = settings
        self.ws_manager = None
        self.aggregator = None
        self.stop_event = asyncio.Event()
        
        # 시그널 핸들러 설정
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"{LOG_SYSTEM} 오더북 수집기 초기화 완료")
    
    def _signal_handler(self, sig, frame):
        """시그널 핸들러"""
        logger.info(f"{LOG_SYSTEM} 종료 시그널 수신: {sig}")
        if not self.stop_event.is_set():
            self.stop_event.set()
        else:
            logger.info(f"{LOG_SYSTEM} 강제 종료")
            sys.exit(1)
    
    async def initialize(self):
        """초기화"""
        try:
            # Aggregator 초기화
            self.aggregator = Aggregator(self.settings)
            
            # WebsocketManager 초기화
            self.ws_manager = WebsocketManager(self.settings)
            
            logger.info(f"{LOG_SYSTEM} 오더북 수집기 초기화 완료")
            return True
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 초기화 실패: {str(e)}")
            return False
    
    async def start(self):
        """시작"""
        try:
            # 심볼 필터링
            filtered_data = await self.aggregator.run_filtering()
            if not filtered_data:
                logger.error(f"{LOG_SYSTEM} 필터링된 심볼이 없습니다.")
                return False
            
            # 업비트를 제외한 데이터 (기존 WebsocketManager용)
            non_upbit_data = {k: v for k, v in filtered_data.items() if k != "upbit"}
            
            # 기존 WebsocketManager 시작 (업비트 제외)
            await self.ws_manager.start_all_websockets(non_upbit_data)
            
            # OrderManager 통합 (업비트용)
            await integrate_with_websocket_manager(self.ws_manager, self.settings, filtered_data)
            
            logger.info(f"{LOG_SYSTEM} 오더북 수집기 시작 완료")
            return True
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 시작 실패: {str(e)}")
            return False
    
    async def stop(self):
        """중지"""
        try:
            # WebsocketManager 중지
            if self.ws_manager:
                await self.ws_manager.shutdown()
            
            # OrderManager 중지 (WebsocketManager에 저장된 경우)
            if hasattr(self.ws_manager, "order_managers"):
                for manager in self.ws_manager.order_managers.values():
                    await manager.stop()
            
            logger.info(f"{LOG_SYSTEM} 오더북 수집기 중지 완료")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 중지 실패: {str(e)}")
    
    async def run(self):
        """실행"""
        try:
            # 초기화
            if not await self.initialize():
                return
            
            # 시작
            if not await self.start():
                await self.stop()
                return
            
            # 종료 이벤트 대기
            logger.info(f"{LOG_SYSTEM} 프로그램을 종료하려면 Ctrl+C를 누르세요")
            await self.stop_event.wait()
            
            # 중지
            await self.stop()
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 실행 중 오류 발생: {str(e)}")
            await self.stop()


async def main():
    """메인 함수"""
    try:
        # 설정 로드 (실제 구현에서는 파일에서 로드)
        settings = {
            "websocket": {
                "orderbook_depth": 15,
                "ping_interval": 60,
                "ping_timeout": 10
            },
            "trading": {
                "general": {
                    "min_volume_krw": 100_000_000  # 1억원
                },
                "excluded_symbols": {
                    "list": ["LUNA", "UST"]  # 제외할 심볼
                }
            }
        }
        
        # 오더북 수집기 생성 및 실행
        collector = OrderbookCollector(settings)
        await collector.run()
        
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 메인 함수 오류: {str(e)}")


if __name__ == "__main__":
    # 비동기 이벤트 루프 실행
    asyncio.run(main()) 