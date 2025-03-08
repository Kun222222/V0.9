# file: orderbook/websocket/upbit_websocket.py

import asyncio
import json
import time
import aiohttp
from websockets import connect
from typing import Dict, List, Optional

from crosskimp.ob_collector.utils.logging.logger import get_raw_logger
from crosskimp.ob_collector.orderbook.websocket.base_ws import BaseWebsocket
from crosskimp.ob_collector.orderbook.orderbook.upbit_s_ob import UpbitOrderBookManager

class UpbitWebsocket(BaseWebsocket):
    """
    Upbit WebSocket 클라이언트
    - URL: wss://api.upbit.com/websocket/v1
    - 구독: orderbook (심볼별 "KRW-{symbol}")
    """
    def __init__(self, settings: dict):
        super().__init__(settings, "upbit")
        self.ws_url = "wss://api.upbit.com/websocket/v1"
        depth = settings.get("websocket", {}).get("orderbook_depth", 10)
        self.orderbook_manager = UpbitOrderBookManager(depth=depth)
        self.session: Optional[aiohttp.ClientSession] = None
        self.is_connected = False
        reconnect_cfg = settings.get("websocket", {}).get("reconnect", {})
        self.max_retries = reconnect_cfg.get("max_retries", 5)
        self.retry_delay = reconnect_cfg.get("retry_delay", 5)
        self.current_retry = 0
        self.last_request_time = 0.0
        self.request_interval = 0.125
        self.initialized_symbols = set()  # 초기화된 심볼 추적
        self.last_ping_time = 0
        self.ping_interval = 60  # 60초
        
        # raw 로거 초기화
        self.raw_logger = get_raw_logger("upbit")

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

    async def connect(self) -> bool:
        try:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect_attempt")
                
            self.ws = await connect(
                self.ws_url,
                ping_interval=None,
                compression=None
            )
            self.is_connected = True
            self.current_retry = 0
            self.stats.connection_start_time = time.time()
            
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect")
            return True
            
        except Exception as e:
            self.log_error(f"웹소켓 연결 실패: {str(e)}", exc_info=True)
            return False

    async def subscribe(self, symbols: List[str]) -> None:
        try:
            if not self.ws:
                self.log_error("웹소켓이 연결되지 않음")
                return
            
            markets = [f"KRW-{s.upper()}" for s in symbols]
            
            # 웹소켓 구독 메시지 전송
            sub_message = [
                {"ticket": f"upbit_orderbook_{int(time.time())}"},
                {
                    "type": "orderbook",
                    "codes": markets,
                    "isOnlyRealtime": False  # 스냅샷 포함 요청
                }
            ]
            
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe")
            await self.ws.send(json.dumps(sub_message))
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe_complete")
            
        except Exception as e:
            self.log_error(f"구독 처리 중 오류 발생: {str(e)}")
            raise

    async def parse_message(self, message: str) -> Optional[dict]:
        try:
            if isinstance(message, bytes):
                message = message.decode("utf-8")
            
            # PING 응답 처리
            if message == '{"status":"UP"}':
                return None
            
            data = json.loads(message)
            if data.get("type") != "orderbook":
                return None

            symbol = data.get("code", "").replace("KRW-","")
            if not symbol:
                self.log_error("심볼 정보 누락")
                return None

            # raw 메시지 로깅
            self.log_raw_message("depthUpdate", message, symbol)

            ts = data.get("timestamp", int(time.time()*1000))
            msg_type = "delta"  # 업비트 웹소켓은 실시간 델타 업데이트
            units = data.get("orderbook_units", [])
            bids = []
            asks = []
            for unit in units:
                bid_price = float(unit.get("bid_price",0))
                bid_size = float(unit.get("bid_size",0))
                ask_price = float(unit.get("ask_price",0))
                ask_size = float(unit.get("ask_size",0))
                if bid_price > 0:
                    bids.append([bid_price, bid_size])
                if ask_price > 0:
                    asks.append([ask_price, ask_size])

            return {
                "exchangename": "upbit",
                "symbol": symbol.upper(),
                "bids": bids,
                "asks": asks,
                "timestamp": ts,
                "sequence": ts,
                "type": msg_type
            }
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}, raw={message[:200]}...", exc_info=True)
            return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        try:
            symbol = parsed["symbol"]
            
            # 업비트는 매 메시지가 전체 오더북이므로, 항상 스냅샷으로 처리
            result = await self.orderbook_manager.initialize_orderbook(symbol, parsed)
            if result.is_valid:
                self.orderbook_manager.update_sequence(symbol, parsed["timestamp"])
                ob_dict = self.orderbook_manager.get_orderbook(symbol).to_dict()
                if self.output_queue:
                    await self.output_queue.put((self.exchangename, ob_dict))
            else:
                self.log_error(f"{symbol} 오더북 업데이트 실패")

        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {e}")

    async def request_snapshot(self, symbol: str) -> Optional[dict]:
        try:
            now = time.time()
            elapsed = now - self.last_request_time
            if elapsed < self.request_interval:
                await asyncio.sleep(self.request_interval - elapsed)
            self.last_request_time = time.time()

            if not self.session:
                self.session = aiohttp.ClientSession()

            market_str = f"KRW-{symbol}"
            url = "https://api.upbit.com/v1/orderbook"
            params = {"markets": market_str}
            headers = {"Accept": "application/json"}

            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "snapshot_request")

            async with self.session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if not data or not isinstance(data, list):
                        self.log_error(f"{symbol} 스냅샷 응답 형식 오류")
                        return None

                    snapshot_data = data[0]
                    snapshot_data["stream_type"] = "SNAPSHOT"
                    # raw 메시지 로깅
                    self.log_raw_message("snapshot", json.dumps(snapshot_data), symbol)

                    parsed = await self._parse_snapshot(snapshot_data)
                    if parsed:
                        if self.connection_status_callback:
                            self.connection_status_callback(self.exchangename, "snapshot_received")
                        return parsed
                    return None
                else:
                    self.log_error(f"{symbol} 스냅샷 요청 실패: status={resp.status}")
        except Exception as e:
            self.log_error(f"{symbol} 스냅샷 요청 중 오류 발생: {str(e)}", exc_info=True)
        return None

    async def _parse_snapshot(self, data: dict) -> Optional[dict]:
        try:
            symbol = data.get("market", "").replace("KRW-","")
            if not symbol:
                symbol = data.get("code", "").replace("KRW-","")
            if not symbol:
                self.log_error("심볼 정보 누락")
                return None

            ts = data.get("timestamp", int(time.time()*1000))
            units = data.get("orderbook_units", [])
            bids = []
            asks = []
            for unit in units:
                bid_price = float(unit.get("bid_price",0))
                bid_size = float(unit.get("bid_size",0))
                ask_price = float(unit.get("ask_price",0))
                ask_size = float(unit.get("ask_size",0))
                if bid_price > 0:
                    bids.append([bid_price, bid_size])
                if ask_price > 0:
                    asks.append([ask_price, ask_size])

            return {
                "exchangename": "upbit",
                "symbol": symbol.upper(),
                "bids": bids,
                "asks": asks,
                "timestamp": ts,
                "sequence": ts,
                "type": "snapshot"
            }
        except Exception as e:
            self.log_error(f"{symbol} 스냅샷 파싱 실패: {str(e)}")
            return None

    async def _send_ping(self):
        try:
            if self.ws and self.is_connected:
                await self.ws.ping()
                self.last_ping_time = time.time()
        except Exception as e:
            self.log_error(f"Ping 전송 실패: {str(e)}")

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        while not self.stop_event.is_set():
            try:
                symbols = symbols_by_exchange.get(self.exchangename.lower(), [])
                if not symbols:
                    self.log_error("구독할 심볼 없음")
                    return

                # 공통 로깅을 위한 부모 클래스 start 호출
                await super().start(symbols_by_exchange)

                # 연결
                if not await self.connect():
                    await asyncio.sleep(self.retry_delay)
                    continue

                # 구독
                await self.subscribe(symbols)

                # 메시지 처리 루프
                while not self.stop_event.is_set():
                    try:
                        # Ping 전송
                        now = time.time()
                        if now - self.last_ping_time >= self.ping_interval:
                            await self._send_ping()

                        message = await asyncio.wait_for(
                            self.ws.recv(),
                            timeout=self.ping_interval
                        )
                        self.stats.last_message_time = time.time()
                        self.stats.message_count += 1

                        parsed = await self.parse_message(message)
                        if parsed:
                            await self.handle_parsed_message(parsed)
                            if self.connection_status_callback:
                                self.connection_status_callback(self.exchangename, "message")

                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        self.log_error(f"메시지 처리 실패: {str(e)}")
                        break

            except Exception as e:
                self.current_retry += 1
                self.log_error(f"연결 실패: {str(e)}, retry={self.current_retry}")
                
                if self.current_retry > self.max_retries:
                    self.log_error("최대 재시도 횟수 초과")
                    break
                    
                await asyncio.sleep(self.retry_delay)
                continue
                
            finally:
                if self.ws:
                    await self.ws.close()
                self.is_connected = False
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "disconnect")

    async def stop(self) -> None:
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "stop")
        self.stop_event.set()
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                self.log_error(f"웹소켓 종료 실패: {e}")

        self.is_connected = False
        self.orderbook_manager.clear_all()
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "disconnect")