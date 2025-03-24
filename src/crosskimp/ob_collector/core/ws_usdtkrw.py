"""
USDT/KRW 시세 수집 모듈

이 모듈은 국내 거래소(업비트, 빗썸)의 USDT/KRW 시세를 실시간으로 수집하는 기능을 제공합니다.
"""

import asyncio
import json
import time
import uuid
import websockets
from typing import Dict, Callable, List, Any, Optional, Set
from threading import Lock

from crosskimp.common.logger.logger import get_unified_logger
# constants_v3 대신 새로운 모듈에서 가져오기
from crosskimp.common.config.common_constants import Exchange, EXCHANGE_NAMES_KR, SystemComponent

# 웹소켓 URL 정의
WEBSOCKET_URLS = {
    Exchange.UPBIT.value: "wss://api.upbit.com/websocket/v1",
    Exchange.BITHUMB.value: "wss://pubwss.bithumb.com/pub/ws"
}

# 웹소켓 설정
WEBSOCKET_CONFIG = {
    "upbit": {
        "ping_interval": 30,
        "ping_timeout": 10
    },
    "bithumb": {
        "ping_interval": 30,
        "ping_timeout": 10
    }
}

# 로거 인스턴스 가져오기
logger = get_unified_logger(component=SystemComponent.ORDERBOOK.value)

class WsUsdtKrwMonitor:
    """
    업비트와 빗썸의 USDT/KRW 가격을 실시간으로 모니터링 (WebSocket 기반)
    - 웹소켓을 통한 실시간 가격 업데이트
    - 각 거래소별 가격 및 통합 가격 제공
    """
    def __init__(self):
        self.prices = {
            Exchange.UPBIT.value: 0.0,
            Exchange.BITHUMB.value: 0.0,
            "last_update": 0
        }
        self.connections = {}
        self.stop_event = asyncio.Event()
        
        # 웹소켓 설정
        self.ws_config = {
            Exchange.UPBIT.value: WEBSOCKET_CONFIG["upbit"],
            Exchange.BITHUMB.value: WEBSOCKET_CONFIG["bithumb"]
        }
        
        # 재시도 설정
        self.max_retries = 3
        self.retry_delay = 1.0
        
        # 가격 변경 콜백 리스트
        self._price_callbacks = []
        
        # 연결 상태
        self.connection_status = {
            Exchange.UPBIT.value: False,
            Exchange.BITHUMB.value: False
        }
        
        logger.info("USDT/KRW 웹소켓 모니터가 초기화되었습니다.")
    
    def add_price_callback(self, callback: Callable[[float], None]):
        """가격 변경 콜백 등록"""
        if callback not in self._price_callbacks:
            self._price_callbacks.append(callback)
            logger.debug("USDT/KRW 가격 콜백이 등록되었습니다.")
    
    def remove_price_callback(self, callback: Callable[[float], None]):
        """가격 변경 콜백 제거"""
        if callback in self._price_callbacks:
            self._price_callbacks.remove(callback)
            logger.debug("USDT/KRW 가격 콜백이 제거되었습니다.")

    async def _notify_price_change(self, exchange: str):
        """가격 변경 알림"""
        price = self.prices[exchange]
        for callback in self._price_callbacks:
            try:
                callback(price)
            except Exception as e:
                logger.error(f"USDT/KRW 가격 콜백 실행 중 오류가 발생했습니다: {str(e)}")

    async def start(self):
        """모니터링 시작"""
        try:
            logger.info("USDT/KRW 웹소켓 모니터링을 시작합니다.")
            
            # 웹소켓 연결 및 모니터링 태스크 생성
            tasks = [
                asyncio.create_task(self._connect_upbit()),
                asyncio.create_task(self._connect_bithumb()),
                asyncio.create_task(self._status_monitor())
            ]
            
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"USDT/KRW 웹소켓 모니터 시작 중 오류가 발생했습니다: {str(e)}", exc_info=True)
    
    async def _status_monitor(self):
        """연결 상태 및 가격 모니터링"""
        last_log_time = 0  # 마지막 로깅 시간 추적
        log_interval = 10  # 10초마다 로깅
        
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                status_msg = "USDT/KRW 현재 가격 | "
                
                for exchange in [Exchange.UPBIT.value, Exchange.BITHUMB.value]:
                    # 연결 상태 이모지
                    status_emoji = "🟢" if self.connection_status[exchange] else "🔴"
                    
                    if self.prices[exchange] > 0:
                        status_msg += f"{status_emoji} {EXCHANGE_NAMES_KR[exchange]}: {self.prices[exchange]:,.2f} KRW | "
                    else:
                        status_msg += f"{status_emoji} {EXCHANGE_NAMES_KR[exchange]}: 가격 없음 | "
                
                # 10초마다 가격 정보 로깅
                if current_time - last_log_time >= log_interval:
                    logger.info(status_msg.rstrip(" | "))
                    last_log_time = current_time
                
                await asyncio.sleep(1.0)  # 1초마다 상태 확인
            except Exception as e:
                logger.error(f"USDT/KRW 상태 모니터링 중 오류가 발생했습니다: {str(e)}")
                await asyncio.sleep(1.0)
    
    async def _connect_upbit(self):
        """업비트 웹소켓 연결 및 구독"""
        subscribe_fmt = [
            {"ticket": "USDT_KRW_MONITOR"},
            {
                "type": "ticker",
                "codes": ["KRW-USDT"],
                "isOnlyRealtime": True
            }
        ]
        
        while not self.stop_event.is_set():
            try:
                async with websockets.connect(WEBSOCKET_URLS[Exchange.UPBIT.value]) as websocket:
                    self.connections[Exchange.UPBIT.value] = websocket
                    self.connection_status[Exchange.UPBIT.value] = True
                    await websocket.send(json.dumps(subscribe_fmt))
                    logger.info(f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]} 웹소켓 연결에 성공했습니다.")
                    
                    while not self.stop_event.is_set():
                        try:
                            data = await websocket.recv()
                            await self._handle_upbit_message(json.loads(data))
                        except Exception as e:
                            if not self.stop_event.is_set():
                                logger.error(f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]} 메시지 처리 중 오류가 발생했습니다: {str(e)}")
                            break
            except Exception as e:
                if not self.stop_event.is_set():
                    logger.error(f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]} 웹소켓 연결 중 오류가 발생했습니다: {str(e)}")
                    await asyncio.sleep(self.retry_delay)
            finally:
                self.connection_status[Exchange.UPBIT.value] = False
    
    async def _connect_bithumb(self):
        """빗썸 웹소켓 연결 및 구독"""
        subscribe_fmt = {
            "type": "ticker",
            "symbols": ["USDT_KRW"],
            "tickTypes": ["30M"]
        }
        
        while not self.stop_event.is_set():
            try:
                async with websockets.connect(WEBSOCKET_URLS[Exchange.BITHUMB.value]) as websocket:
                    self.connections[Exchange.BITHUMB.value] = websocket
                    self.connection_status[Exchange.BITHUMB.value] = True
                    await websocket.send(json.dumps(subscribe_fmt))
                    logger.info(f"{EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]} 웹소켓 연결에 성공했습니다.")
                    
                    while not self.stop_event.is_set():
                        try:
                            data = await websocket.recv()
                            await self._handle_bithumb_message(json.loads(data))
                        except Exception as e:
                            if not self.stop_event.is_set():
                                logger.error(f"{EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]} 메시지 처리 중 오류가 발생했습니다: {str(e)}")
                            break
            except Exception as e:
                if not self.stop_event.is_set():
                    logger.error(f"{EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]} 웹소켓 연결 중 오류가 발생했습니다: {str(e)}")
                    await asyncio.sleep(self.retry_delay)
            finally:
                self.connection_status[Exchange.BITHUMB.value] = False
    
    async def _handle_upbit_message(self, data: dict):
        """업비트 웹소켓 메시지 처리"""
        try:
            if isinstance(data, dict) and "trade_price" in data:
                price = float(data["trade_price"])
                if price > 0:
                    self.prices[Exchange.UPBIT.value] = price
                    # logger.debug(f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]} 가격이 업데이트되었습니다: {price:,.2f} KRW")
                    await self._notify_price_change(Exchange.UPBIT.value)
        except Exception as e:
            logger.error(f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]} 메시지 파싱 중 오류가 발생했습니다: {str(e)}")
    
    async def _handle_bithumb_message(self, data: dict):
        """빗썸 웹소켓 메시지 처리"""
        try:
            if isinstance(data, dict) and "content" in data:
                content = data["content"]
                if isinstance(content, dict) and "closePrice" in content:
                    price = float(content["closePrice"])
                    if price > 0:
                        self.prices[Exchange.BITHUMB.value] = price
                        # logger.debug(f"{EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]} 가격이 업데이트되었습니다: {price:,.2f} KRW")
                        await self._notify_price_change(Exchange.BITHUMB.value)
        except Exception as e:
            logger.error(f"{EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]} 메시지 파싱 중 오류가 발생했습니다: {str(e)}")
    
    def get_price(self, exchange: str) -> float:
        """특정 거래소의 현재 가격 조회"""
        return self.prices.get(exchange, 0.0)
    
    def get_all_prices(self) -> Dict[str, float]:
        """모든 거래소의 현재 가격 조회"""
        return {
            Exchange.UPBIT.value: self.prices[Exchange.UPBIT.value],
            Exchange.BITHUMB.value: self.prices[Exchange.BITHUMB.value]
        }
    
    async def stop(self):
        """모니터링 종료"""
        self.stop_event.set()
        for exchange, websocket in self.connections.items():
            try:
                await websocket.close()
                self.connection_status[exchange] = False
                logger.info(f"{EXCHANGE_NAMES_KR[exchange]} 웹소켓 연결이 종료되었습니다.")
            except Exception as e:
                logger.error(f"{EXCHANGE_NAMES_KR[exchange]} 웹소켓 종료 중 오류가 발생했습니다: {str(e)}")
        logger.info("USDT/KRW 웹소켓 모니터링이 종료되었습니다.") 