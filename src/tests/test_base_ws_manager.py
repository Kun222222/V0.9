import pytest
import asyncio
from unittest.mock import Mock, patch
from crosskimp.ob_collector.orderbook.websocket.base_ws_manager import WebsocketManager

@pytest.fixture
def ws_manager():
    settings = {
        "websocket": {
            "delay_threshold_ms": 1000
        }
    }
    return WebsocketManager(settings)

@pytest.mark.asyncio
async def test_update_connection_status_disconnect_counting(ws_manager):
    # 초기 상태 확인
    exchange = "binance"
    assert ws_manager.metrics[exchange].reconnect_count == 0
    assert ws_manager.metrics[exchange].connection_status == False

    # 연결 상태로 변경
    ws_manager.update_connection_status(exchange, "connect")
    assert ws_manager.metrics[exchange].connection_status == True
    assert ws_manager.metrics[exchange].reconnect_count == 0

    # 연결 끊김 상태로 변경
    ws_manager.update_connection_status(exchange, "disconnect")
    assert ws_manager.metrics[exchange].connection_status == False
    assert ws_manager.metrics[exchange].reconnect_count == 1

    # 다시 연결
    ws_manager.update_connection_status(exchange, "connect")
    assert ws_manager.metrics[exchange].connection_status == True
    assert ws_manager.metrics[exchange].reconnect_count == 1

    # 다시 연결 끊김
    ws_manager.update_connection_status(exchange, "disconnect")
    assert ws_manager.metrics[exchange].connection_status == False
    assert ws_manager.metrics[exchange].reconnect_count == 2

@pytest.mark.asyncio
async def test_check_message_delays_disconnect_counting(ws_manager):
    exchange = "binance"
    
    # 초기 연결 설정
    ws_manager.update_connection_status(exchange, "connect")
    assert ws_manager.metrics[exchange].connection_status == True
    assert ws_manager.metrics[exchange].reconnect_count == 0
    
    # 메시지 지연 시뮬레이션 (2초 지연)
    ws_manager.metrics[exchange].last_message_time = asyncio.get_event_loop().time() - 2
    
    # check_message_delays 실행
    await ws_manager.check_message_delays()
    
    # 연결이 끊겼는지 확인
    assert ws_manager.metrics[exchange].connection_status == False
    assert ws_manager.metrics[exchange].reconnect_count == 1

@pytest.mark.asyncio
async def test_check_alerts_disconnect_counting(ws_manager):
    exchange = "binance"
    
    # 초기 연결 설정
    ws_manager.update_connection_status(exchange, "connect")
    assert ws_manager.metrics[exchange].connection_status == True
    assert ws_manager.metrics[exchange].reconnect_count == 0
    
    # 마지막 메시지 시간을 61초 전으로 설정
    ws_manager.metrics[exchange].last_message_time = asyncio.get_event_loop().time() - 61
    
    # check_alerts 실행
    await ws_manager.check_alerts()
    
    # 연결이 끊겼는지 확인
    assert ws_manager.metrics[exchange].connection_status == False
    assert ws_manager.metrics[exchange].reconnect_count == 1 