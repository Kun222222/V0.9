# file: orderbook/websocket/binance_future_websocket.py

import asyncio
import json
import time
import aiohttp
import websockets
from websockets import connect
from typing import Dict, List, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, get_raw_logger
from crosskimp.ob_collector.orderbook.websocket.base_ws_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.orderbook.binance_f_ob import BinanceFutureOrderBookManager, parse_binance_future_depth_update

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class BinanceFutureWebsocket(BaseWebsocketConnector):
    """
    바이낸스 선물 웹소켓
    - 바이낸스 현물과 유사한 흐름 + testnet 지원
    - orderbook_manager를 통해 오더북 업데이트 & output_queue 전달
    """

    def __init__(self, settings: dict, testnet: bool = False):
        super().__init__(settings, "binancefuture")
        self.testnet = testnet
        if self.testnet:
            self.ws_base = "wss://stream.binancefuture.com/stream?streams="
            self.snapshot_base = "https://testnet.binancefuture.com/fapi/v1/depth"
        else:
            self.ws_base = "wss://fstream.binance.com/stream?streams="
            self.snapshot_base = "https://fapi.binance.com/fapi/v1/depth"

        self.update_speed = "100ms"
        self.snapshot_depth = settings.get("depth", 100)
        self.orderbook_manager = BinanceFutureOrderBookManager(self.snapshot_depth)

        self.subscribed_symbols: set = set()
        self.instance_key: Optional[str] = None

        # 실시간 url
        self.wsurl = ""
        self.is_connected = False
        # Ping/Pong 설정 추가
        self.ping_interval = 150
        self.ping_timeout = 10
        
        # raw 로거 초기화
        self.raw_logger = get_raw_logger("binance_future")

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """
        BaseWebsocket에도 저장 + orderbook_manager에도 동일하게 저장
        """
        super().set_output_queue(queue)
        if self.orderbook_manager:
            self.orderbook_manager.output_queue = queue

    async def connect(self):
        """
        바이낸스 현물과 유사한 로직
        """
        try:
            if not self.wsurl:
                raise ValueError("WebSocket URL is not set. Call subscribe() first.")
                
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect_attempt")
                
            self.session = aiohttp.ClientSession()
            self.ws = await connect(
                self.wsurl,
                ping_interval=self.ping_interval,  # 150초
                ping_timeout=self.ping_timeout,    # 10초
                compression=None
            )
            self.is_connected = True
            self.stats.connection_start_time = time.time()
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect")
        except Exception as e:
            self.log_error(f"connect() 예외: {e}", exc_info=True)
            raise

    async def subscribe(self, symbols: List[str]):
        if not symbols:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "warning")
            return

        streams = []
        for sym in symbols:
            sym_lower = sym.lower()
            if not sym_lower.endswith("usdt"):
                sym_lower += "usdt"
            streams.append(f"{sym_lower}@depth@{self.update_speed}")
            self.subscribed_symbols.add(sym.upper())

        combined = "/".join(streams)
        self.wsurl = self.ws_base + combined
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "subscribe")

        await self.connect()

        # 스냅샷
        for sym in symbols:
            snapshot = await self.request_snapshot(sym)
            if snapshot:
                res = await self.orderbook_manager.initialize_orderbook(sym, snapshot)
                if not res.is_valid:
                    self.log_error(f"{sym} 스냅샷 적용 실패: {res.error_messages}")
            else:
                self.log_error(f"{sym} 스냅샷 요청 실패")

    async def request_snapshot(self, symbol: str) -> Optional[dict]:
        """
        바이낸스 현물과 유사한 스냅샷 요청
        """
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                url = f"{self.snapshot_base}?symbol={symbol.upper()}USDT&limit={self.snapshot_depth}"
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "snapshot_request")
                
                # TCP 커넥터에 DNS 캐시 설정 추가
                connector = aiohttp.TCPConnector(
                    ttl_dns_cache=300,  # DNS 캐시 5분
                    ssl=False  # SSL 검증 비활성화 (필요한 경우)
                )
                
                async with aiohttp.ClientSession(connector=connector) as sess:
                    async with sess.get(url, timeout=10) as resp:
                        if resp.status == 200:
                            raw_data = await resp.json()
                            return self.parse_snapshot(raw_data, symbol)
                        else:
                            self.log_error(f"{symbol} 스냅샷 status={resp.status}")
                            
            except Exception as e:
                self.log_error(f"스냅샷 요청 실패: {e}", exc_info=True)
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                
        self.log_error(f"{symbol} ME 스냅샷 요청 최대 재시도 횟수 초과")
        return None

    def parse_snapshot(self, data: dict, symbol: str) -> Optional[dict]:
        """
        바이낸스 현물 parse_snapshot과 유사
        """
        try:
            if "lastUpdateId" not in data:
                self.log_error(f"{symbol} 'lastUpdateId' 없음")
                return None

            last_id = data["lastUpdateId"]
            bids = [[float(b[0]), float(b[1])] for b in data.get("bids", []) if float(b[0])>0 and float(b[1])>0]
            asks = [[float(a[0]), float(a[1])] for a in data.get("asks", []) if float(a[0])>0 and float(a[1])>0]

            snapshot = {
                "exchangename": "binancefuture",
                "symbol": symbol,
                "bids": bids,
                "asks": asks,
                "timestamp": int(time.time()*1000),
                "sequence": last_id,
                "type": "snapshot"
            }
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "snapshot_parsed")
            return snapshot
        except Exception as e:
            self.log_error(f"parse_snapshot() 예외: {e}", exc_info=True)
            return None

    async def parse_message(self, message: str) -> Optional[dict]:
        try:
            data = json.loads(message)
            if "stream" in data and "data" in data:
                data = data["data"]

            # 구독 응답
            if "result" in data and "id" in data:
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe_response")
                return None

            # depthUpdate
            if data.get("e") == "depthUpdate":
                symbol = data["s"].replace("USDT","").upper()
                # Raw 메시지 그대로 로깅 (raw 로그 파일에만 기록)
                self.log_raw_message("depthUpdate", message, symbol)
                return data

            return None
        except json.JSONDecodeError as je:
            self.log_error(f"JSON 파싱실패: {je}, message={message[:200]}", exc_info=False)
        except Exception as e:
            self.log_error(f"parse_message() 예외: {e}", exc_info=True)
        return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        try:
            symbol = parsed.get("s", "").replace("USDT","").upper()
            evt = parse_binance_future_depth_update(parsed)
            if not evt:
                return
            
            res = await self.orderbook_manager.update(symbol, evt)
            if not res.is_valid:
                self.log_error(f"{symbol} 오더북 업데이트 실패: {res.error_messages}")
        except Exception as e:
            self.log_error(f"handle_parsed_message() 예외: {e}", exc_info=True)

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        exchange_symbols = symbols_by_exchange.get(self.exchangename.lower(), [])
        if not exchange_symbols:
            self.log_error("구독할 심볼 없음.")
            return

        # 공통 로깅을 위한 부모 클래스 start 호출
        await super().start(symbols_by_exchange)

        while not self.stop_event.is_set():
            try:
                # 단일 연결로 모든 심볼 구독
                await self.subscribe(exchange_symbols)
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "connect")

                while not self.stop_event.is_set():
                    try:
                        message = await asyncio.wait_for(
                            self.ws.recv(),
                            timeout=self.health_check_interval
                        )
                        self.stats.last_message_time = time.time()
                        self.stats.message_count += 1
                        parsed = await self.parse_message(message)
                        if parsed and self.connection_status_callback:
                            self.connection_status_callback(self.exchangename, "message")

                        if parsed:
                            await self.handle_parsed_message(parsed)

                    except asyncio.TimeoutError:
                        # 헬스체크 timeout
                        continue
                    except Exception as e:
                        self.log_error(f"메시지 수신 실패 | error={str(e)}")
                        break

            except Exception as conn_e:
                # 연결 실패 시 백오프
                delay = self.reconnect_strategy.next_delay()
                self.log_error(f"연결 실패: {conn_e}, 재연결 {delay}s 후 재시도", exc_info=False)
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

    async def stop(self) -> None:
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "stop")
        self.stop_event.set()
        if self.ws:
            await self.ws.close()
        self.is_connected = False
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "disconnect")

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
            self.log_error(f"Raw 로깅 실패: {str(e)}")