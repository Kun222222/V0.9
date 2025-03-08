# file: orderbook/websocket/binance_spot_websocket.py

import asyncio
import json
import time
import aiohttp
from websockets import connect
from typing import Dict, List, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, get_raw_logger
from crosskimp.ob_collector.orderbook.websocket.base_ws import BaseWebsocket
from crosskimp.ob_collector.orderbook.orderbook.binance_spot_orderbook_manager import BinanceSpotOrderBookManager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

def parse_binance_depth_update(msg_data: dict) -> Optional[dict]:
    if msg_data.get("e") != "depthUpdate":
        return None
    symbol_raw = msg_data.get("s", "")
    symbol = symbol_raw.replace("USDT", "").upper()

    b_data = msg_data.get("b", [])
    a_data = msg_data.get("a", [])
    event_time = msg_data.get("E", 0)
    first_id = msg_data.get("U", 0)
    final_id = msg_data.get("u", 0)

    bids, asks = [], []
    for b in b_data:
        px, qty = float(b[0]), float(b[1])
        if px > 0 and qty != 0:
            bids.append([px, qty])
    for a in a_data:
        px, qty = float(a[0]), float(a[1])
        if px > 0 and qty != 0:
            asks.append([px, qty])

    return {
        "exchangename": "binance",
        "symbol": symbol,
        "bids": bids,
        "asks": asks,
        "timestamp": event_time,
        "first_update_id": first_id,
        "final_update_id": final_id,
        "sequence": final_id,
        "type": "delta"
    }

class BinanceSpotWebsocket(BaseWebsocket):
    """
    바이낸스 현물 웹소켓
    - depth=500으로 사용하려면: settings["depth"] = 500
    - subscribe -> snapshot -> orderbook_manager
    - gap 발생 시 재스냅샷 X
    """
    def __init__(self, settings: dict):
        super().__init__(settings, "binance")
        self.manager = BinanceSpotOrderBookManager(depth=settings.get("depth", 500))
        self.subscribed_symbols = set()
        self.ws = None
        self.session = None
        self.ws_url = "wss://stream.binance.com:9443/ws"
        self.logger = logger
        
        # raw 로거 초기화
        self.raw_logger = get_raw_logger("binance_spot")

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        super().set_output_queue(queue)
        self.manager.output_queue = queue
        # self.logger.debug(f"[{self.exchangename}] output_queue set")

    async def connect(self):
        try:
            self.logger.info(f"[{self.exchangename}] connect attempt")
            self.session = aiohttp.ClientSession()
            self.ws = await connect(
                self.ws_url,
                ping_interval=self.ping_interval,  # 150초
                ping_timeout=self.ping_timeout,    # 10초
                compression=None
            )
            self.is_connected = True
            self.stats.connection_start_time = time.time()
            self.logger.info(f"[{self.exchangename}] WebSocket connected")
        except Exception as e:
            self.log_error(f"connect() error: {e}")
            raise

    async def subscribe(self, symbols: List[str]):
        if not symbols:
            self.logger.warning(f"[{self.exchangename}] no symbols to subscribe")
            return

        chunk_size = 10
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i+chunk_size]
            sub_params = [f"{sym.lower()}usdt@depth@100ms" for sym in chunk]
            msg = {
                "method": "SUBSCRIBE",
                "params": sub_params,
                "id": int(time.time() * 1000)
            }
            await self.ws.send(json.dumps(msg))
            self.logger.info(f"[{self.exchangename}] SUBSCRIBE {msg}")
            await asyncio.sleep(1)

        # 스냅샷
        for sym in symbols:
            snapshot = await self.manager.fetch_snapshot(sym)
            if snapshot:
                init_res = await self.manager.initialize_orderbook(sym, snapshot)
                if not init_res.is_valid:
                    self.logger.error(f"[{self.exchangename}] {sym} snap init fail: {init_res.error_messages}")
            else:
                self.logger.error(f"[{self.exchangename}] {sym} snapshot fail")

    async def parse_message(self, message: str) -> Optional[dict]:
        try:
            data = json.loads(message)
            if "result" in data and "id" in data:
                self.logger.info(f"[{self.exchangename}] subscribe resp: {data}")
                return None
            if data.get("e") == "depthUpdate":
                symbol = data["s"].replace("USDT","").upper()
                # Raw 메시지 그대로 로깅 (raw 로그 파일에만 기록)
                self.log_raw_message("depthUpdate", message, symbol)
                return data
            return None
        except Exception as e:
            self.log_error(f"parse_message error: {e}")
            return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        try:
            evt = parse_binance_depth_update(parsed)
            if evt:
                symbol = evt["symbol"]
                res = await self.manager.update(symbol, evt)
                if not res.is_valid:
                    self.logger.error(f"[{self.exchangename}] {symbol} update fail: {res.error_messages}")
        except Exception as e:
            self.log_error(f"handle_parsed_message error: {e}")

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        exchange_symbols = symbols_by_exchange.get("binance", [])
        if not exchange_symbols:
            self.logger.warning(f"[{self.exchangename}] no symbols to start")
            return

        while not self.stop_event.is_set():
            try:
                await self.connect()
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "connect")
                self.logger.info(f"[{self.exchangename}] connected")

                await self.subscribe(exchange_symbols)

                while not self.stop_event.is_set():
                    try:
                        msg = await asyncio.wait_for(
                            self.ws.recv(), timeout=self.health_check_interval
                        )
                        self.stats.last_message_time = time.time()
                        self.stats.message_count += 1

                        parsed = await self.parse_message(msg)
                        if parsed and self.connection_status_callback:
                            self.connection_status_callback(self.exchangename, "message")
                        if parsed:
                            await self.handle_parsed_message(parsed)

                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        self.log_error(f"recv fail: {e}")
                        break

            except Exception as conn_e:
                delay = self.reconnect_strategy.next_delay()
                self.log_error(f"connect fail: {conn_e}, wait {delay}s", exc_info=False)
                await asyncio.sleep(delay)
            finally:
                if self.ws:
                    try:
                        await self.ws.close()
                    except:
                        pass
                self.is_connected = False
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "disconnect")
                self.logger.info(f"[{self.exchangename}] disconnected")

    async def stop(self) -> None:
        self.logger.info(f"[{self.exchangename}] stop called")
        self.stop_event.set()
        if self.ws:
            await self.ws.close()
        self.is_connected = False
        self.logger.info(f"[{self.exchangename}] stopped")

    def log_raw_message(self, msg_type: str, message: str, symbol: str) -> None:
        """
        Raw 메시지 로깅
        Args:
            msg_type: 메시지 타입 (snapshot/depthUpdate)
            message: raw 메시지
            symbol: 심볼명
        """
        try:
            self.raw_logger.info(f"{msg_type}|{symbol}|{message}")
        except Exception as e:
            logger.error(f"[{self.exchangename}] Raw 로깅 실패: {str(e)}")