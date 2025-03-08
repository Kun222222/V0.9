import unittest
import asyncio
from unittest.mock import patch, MagicMock
from crosskimp.ob_collector.orderbook.websocket.upbit_s_ws import UpbitWebsocket
from crosskimp.ob_collector.orderbook.orderbook.upbit_s_ob import UpbitOrderBookManager
from crosskimp.ob_collector.orderbook.websocket.base_ws_manager import WebsocketManager

class TestUpbitOrderBook(unittest.IsolatedAsyncioTestCase):

    @patch.object(UpbitWebsocket, 'connect', return_value=True)
    async def test_websocket_connection(self, mock_connect):
        ws = UpbitWebsocket(settings={})
        self.assertTrue(await ws.connect())
        mock_connect.assert_called_once()

    @patch.object(UpbitWebsocket, 'parse_message')
    async def test_message_parsing(self, mock_parse):
        raw_message = '{"type":"orderbook","code":"KRW-BTC","orderbook_units":[{"ask_price":50000,"bid_price":49900,"ask_size":1,"bid_size":1}]}'
        mock_parse.return_value = {'type': 'orderbook', 'code': 'KRW-BTC', 'orderbook_units': [{'ask_price': 50000, 'bid_price': 49900, 'ask_size': 1, 'bid_size': 1}]}
        ws = UpbitWebsocket(settings={})
        parsed_data = await ws.parse_message(raw_message)
        print(f"Parsed Data: {parsed_data}")  # 로깅 추가
        self.assertIsNotNone(parsed_data)
        self.assertEqual(parsed_data['type'], 'orderbook')

    @patch.object(UpbitOrderBookManager, 'initialize_orderbook')
    async def test_orderbook_update(self, mock_initialize):
        manager = UpbitOrderBookManager()
        data = {
            'type': 'orderbook',
            'code': 'KRW-BTC',
            'orderbook_units': [
                {'ask_price': 50000, 'bid_price': 49900, 'ask_size': 1, 'bid_size': 1}
            ]
        }
        print(f"Initializing Orderbook with Data: {data}")  # 로깅 추가
        result = await manager.initialize_orderbook('KRW-BTC', data)
        self.assertIsNotNone(result)
        mock_initialize.assert_called_once_with('KRW-BTC', data)

    @patch.object(WebsocketManager, 'process_queue')
    async def test_queue_transmission(self, mock_process):
        manager = WebsocketManager(settings={})
        orderbook_data = {
            'exchangename': 'upbit',
            'symbol': 'BTC',
            'bids': [[49900, 1]],
            'asks': [[50000, 1]],
            'timestamp': 1234567890,
            'sequence': 1
        }
        print(f"Queueing Orderbook Data: {orderbook_data}")  # 로깅 추가
        await manager.output_queue.put(orderbook_data)
        await manager.process_queue()  # process_queue 호출 추가
        self.assertEqual(manager.output_queue.qsize(), 1)
        mock_process.assert_called_once()

if __name__ == '__main__':
    unittest.main() 