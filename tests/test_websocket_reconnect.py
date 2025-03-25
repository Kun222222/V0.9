import asyncio
import pytest
import unittest.mock as mock
import time
from typing import Dict, List, Any

from crosskimp.common.config.common_constants import Exchange
from crosskimp.ob_collector.ob_collector import ObCollector
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription

# Mock 클래스 정의
class MockWebSocket:
    """테스트용 웹소켓 Mock 클래스"""
    def __init__(self):
        self.closed = False
        self.sent_messages = []
    
    async def send(self, message):
        """메시지 전송 Mock"""
        self.sent_messages.append(message)
        return True
    
    async def recv(self):
        """메시지 수신 Mock"""
        if self.closed:
            raise ConnectionError("연결이 끊어졌습니다")
        return '{"type": "heartbeat"}'  # 기본 응답
    
    async def close(self):
        """연결 종료 Mock"""
        self.closed = True

class MockConnector(BaseWebsocketConnector):
    """테스트용 커넥터 Mock 클래스"""
    def __init__(self, settings, exchange_code):
        super().__init__(settings, exchange_code)
        self.mock_ws = MockWebSocket()
        self.ws_url = "wss://mock.test/ws"
        self.connection_count = 0
        self.disconnect_count = 0
    
    async def connect(self):
        """웹소켓 연결 Mock"""
        self.connection_count += 1
        self.mock_ws = MockWebSocket()  # 새 웹소켓 생성
        self.ws = self.mock_ws
        self.is_connected = True
        return True
    
    async def disconnect(self):
        """웹소켓 연결 종료 Mock"""
        self.disconnect_count += 1
        if self.ws:
            await self.ws.close()
            self.ws = None
        self.is_connected = False
        return True

class MockSubscription(BaseSubscription):
    """테스트용 구독 Mock 클래스"""
    def __init__(self, connection, exchange_code):
        super().__init__(connection, exchange_code)
        self.subscribe_count = 0
        self.unsubscribe_count = 0
        self.last_subscribed_symbols = []
    
    async def create_subscribe_message(self, symbols):
        """구독 메시지 생성 Mock"""
        return {"type": "subscribe", "symbols": symbols}
    
    async def create_unsubscribe_message(self, symbol):
        """구독 취소 메시지 생성 Mock"""
        return {"type": "unsubscribe", "symbol": symbol}
    
    async def subscribe(self, symbols):
        """심볼 구독 Mock"""
        self.subscribe_count += 1
        self.last_subscribed_symbols = symbols if isinstance(symbols, list) else [symbols]
        for sym in self.last_subscribed_symbols:
            self.subscribed_symbols[sym] = True
            self.subscription_status[sym] = "subscribed"
        
        # 구독 메시지 전송
        message = await self.create_subscribe_message(self.last_subscribed_symbols)
        await self.send_message(str(message))
        
        return True
    
    async def _on_message(self, message):
        """메시지 처리 Mock"""
        await super()._on_message(message)

# 실제 테스트 클래스
@pytest.mark.asyncio
class TestWebSocketReconnect:
    """웹소켓 재연결 및 재구독 테스트"""
    
    async def setup_test_environment(self):
        """테스트 환경 설정"""
        # 설정 Mock 생성
        settings = {
            "exchanges": {
                "upbit": {"enabled": True},
                "binance": {"enabled": True}
            }
        }
        
        # ObCollector 인스턴스 생성 (Mock으로 교체 예정)
        self.collector = ObCollector()
        self.collector.settings = settings
        
        # Mock 객체로 교체
        self.exchange_code = Exchange.UPBIT.value
        self.connector = MockConnector(settings, self.exchange_code)
        self.subscription = MockSubscription(self.connector, self.exchange_code)
        
        # Collector에 Mock 객체 설정
        self.collector.connectors = {self.exchange_code: self.connector}
        self.collector.subscriptions = {self.exchange_code: self.subscription}
        self.collector.filtered_symbols = {self.exchange_code: ["BTC-USDT", "ETH-USDT"]}
        
        # 구독 상태 설정
        self.collector.exchange_status = {self.exchange_code: True}
        self.collector.subscription_status = {
            self.exchange_code: {"active": True, "symbol_count": 2}
        }
        
        return True
    
    async def test_reconnect_and_resubscribe(self):
        """재연결 및 재구독 테스트"""
        await self.setup_test_environment()
        
        # 1. 초기 구독 설정
        await self.collector._connect_and_subscribe(self.exchange_code)
        
        # 구독 상태 확인
        assert self.subscription.subscribe_count == 1
        assert len(self.subscription.subscribed_symbols) == 2
        assert "BTC-USDT" in self.subscription.subscribed_symbols
        assert "ETH-USDT" in self.subscription.subscribed_symbols
        assert self.connector.connection_count == 1
        
        # 2. 연결 끊김 시뮬레이션
        self.connector.mock_ws.closed = True
        self.connector.is_connected = False
        
        # 3. 재연결 시도
        await self.collector._connect_and_subscribe(self.exchange_code)
        
        # 4. 재연결 및 재구독 확인
        assert self.connector.connection_count == 2  # 연결 시도 횟수 증가
        assert self.subscription.subscribe_count == 2  # 구독 시도 횟수 증가
        assert len(self.subscription.subscribed_symbols) == 2
        assert "BTC-USDT" in self.subscription.subscribed_symbols
        assert "ETH-USDT" in self.subscription.subscribed_symbols
        
        # 5. 메시지 확인 (구독 메시지가 전송되었는지)
        assert len(self.connector.mock_ws.sent_messages) > 0
        
        print("웹소켓 재연결 및 재구독 테스트 성공!")

@pytest.mark.asyncio
async def test_main():
    """메인 테스트 함수"""
    test = TestWebSocketReconnect()
    await test.test_reconnect_and_resubscribe()

if __name__ == "__main__":
    asyncio.run(test_main()) 