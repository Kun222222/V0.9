# file: orderbook/websocket/upbit_websocket.py

import asyncio
import json
import time
import aiohttp
from websockets import connect
from typing import Dict, List, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, get_raw_logger
from crosskimp.ob_collector.orderbook.websocket.base_websocket import BaseWebsocket
from crosskimp.ob_collector.orderbook.orderbook.base_orderbook import ValidationResult
from crosskimp.ob_collector.orderbook.orderbook.upbit_orderbook_manager import UpbitOrderBookManager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

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

    async def connect(self) -> bool:
        try:
            self.ws = await connect(
                self.ws_url,
                ping_interval=None,
                compression=None
            )
            self.is_connected = True
            self.current_retry = 0
            logger.info(
                f"[{self.exchangename}] WebSocket 연결 성공 | "
                f"url={self.ws_url}"
            )
            return True
        except Exception as e:
            logger.error(
                f"[{self.exchangename}] WebSocket 연결 실패 | "
                f"error={str(e)}",
                exc_info=True
            )
            return False

    async def subscribe(self, symbols: List[str]) -> None:
        try:
            logger.info(f"[{self.exchangename}] 구독 시작 | symbols={symbols}")
            
            if not self.ws:
                logger.error(f"[{self.exchangename}] WebSocket이 연결되지 않음")
                return
            
            # 아직 초기화되지 않은 심볼만 처리
            new_symbols = [s for s in symbols if s.upper() not in self.initialized_symbols]
            if not new_symbols:
                logger.info("모든 심볼이 이미 초기화됨")
                return

            # 초기화 락 추가
            initialization_lock = asyncio.Lock()
            markets = []

            async with initialization_lock:
                for symbol in new_symbols:
                    if symbol.upper() in self.initialized_symbols:
                        continue  # 이중 체크로 동시성 문제 방지
                        
                    market = f"KRW-{symbol.upper()}"
                    logger.info(f"- {symbol} 스냅샷 요청 시작")
                    
                    snap = await self.request_snapshot(symbol.upper())
                    if snap:
                        init_res = await self.orderbook_manager.initialize_orderbook(symbol.upper(), snap)
                        if not init_res.is_valid:
                            logger.error(f"- {symbol} 스냅샷 초기화 실패 | error={init_res.error_messages}")
                            continue
                        
                        self.initialized_symbols.add(symbol.upper())
                        logger.info(f"- {symbol} 스냅샷 초기화 완료")
                        markets.append(market)
                        
                        # 레이트 리밋 준수를 위한 짧은 대기
                        await asyncio.sleep(0.1)
                    else:
                        logger.error(f"- {symbol} 스냅샷 요청 실패")
                        continue

            if markets:
                sub_message = [
                    {"ticket": f"upbit_orderbook_{int(time.time())}"},
                    {
                        "type": "orderbook",
                        "codes": markets,
                        "isOnlyRealtime": True
                    }
                ]
                await self.ws.send(json.dumps(sub_message))
                logger.info(f"[{self.exchangename}] 구독 메시지 전송 | markets={markets}")
        except Exception as e:
            logger.error(f"[{self.exchangename}] 구독 처리 중 오류 발생 | error={str(e)}")
            raise

    async def parse_message(self, message: str) -> Optional[dict]:
        try:
            if isinstance(message, bytes):
                message = message.decode("utf-8")
            
            # PING 응답 처리 추가
            if message == '{"status":"UP"}':
                logger.debug("[Upbit] Received PONG (UP status)")
                return None
            
            data = json.loads(message)
            if data.get("type") != "orderbook":
                logger.debug(
                    f"[{self.exchangename}] 처리되지 않는 메시지 타입 | "
                    f"type={data.get('type', 'unknown')}"
                )
                return None

            symbol = data.get("code", "").replace("KRW-","")
            if not symbol:
                logger.warning(
                    f"[{self.exchangename}] 심볼 정보 누락 | "
                    f"data={data}"
                )
                return None

            # raw 메시지 로깅 추가
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

            # self.logger.debug(
            #     f"[{self.exchangename}] {symbol} 델타 메시지 파싱 완료 | "
            #     f"bids={len(bids)}, asks={len(asks)}, "
            #     f"timestamp={ts}"
            # )

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
            logger.error(
                f"[{self.exchangename}] 메시지 파싱 실패 | "
                f"error={str(e)}, "
                f"raw={message[:200]}...",
                exc_info=True
            )
            return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        try:
            symbol = parsed["symbol"]
            
            # 이미 초기화된 심볼만 처리
            if symbol not in self.initialized_symbols:
                logger.debug(f"[{self.exchangename}] {symbol} 아직 초기화되지 않음, 메시지 스킵")
                return

            if not self.orderbook_manager.is_initialized(symbol):
                logger.warning(f"[{self.exchangename}] {symbol} 오더북 초기화 상태 불일치")
                self.initialized_symbols.remove(symbol)  # 상태 불일치 시 재초기화 필요
                return

            orderbook = self.orderbook_manager.get_orderbook(symbol)
            if not orderbook:
                logger.warning(f"[{self.exchangename}] {symbol} 오더북 객체 없음")
                self.initialized_symbols.remove(symbol)
                return

            result = await orderbook.update(parsed)
            if result.is_valid:
                self.orderbook_manager.update_sequence(symbol, parsed["sequence"])
                ob_dict = orderbook.to_dict()
                # self.logger.debug(f"[{self.exchangename}] {symbol} 오더북 업데이트 성공")
                if self.output_queue:
                    await self.output_queue.put(("upbit", ob_dict))
                    # self.logger.debug(f"[{self.exchangename}] {symbol} 오더북 큐 전송 완료")
            else:
                logger.error(f"[{self.exchangename}] {symbol} 오더북 업데이트 실패 | error={result.error_messages}")
                self.initialized_symbols.remove(symbol)  # 업데이트 실패 시 재초기화 필요
                await self._request_snapshot_and_reinit(symbol)

        except Exception as e:
            logger.error(f"[{self.exchangename}] 메시지 처리 중 오류: {e}")

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

            logger.info(
                f"[{self.exchangename}] {symbol} 스냅샷 요청 | "
                f"url={url}, market={market_str}"
            )

            async with self.session.get(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if not data or not isinstance(data, list):
                        logger.error(
                            f"[{self.exchangename}] {symbol} 스냅샷 응답 형식 오류 | "
                            f"response={data}"
                        )
                        return None

                    snapshot_data = data[0]
                    snapshot_data["stream_type"] = "SNAPSHOT"
                    # raw 메시지 로깅 추가
                    self.log_raw_message("snapshot", json.dumps(snapshot_data), symbol)

                    parsed = await self._parse_snapshot(snapshot_data)
                    if parsed:
                        logger.info(f"[{self.exchangename}] {symbol} 스냅샷 파싱 완료")
                        return parsed
                    return None
                else:
                    logger.error(
                        f"[{self.exchangename}] {symbol} 스냅샷 요청 실패 | "
                        f"status={resp.status}"
                    )
        except Exception as e:
            logger.error(
                f"[{self.exchangename}] {symbol} 스냅샷 요청 중 오류 발생 | "
                f"error={str(e)}",
                exc_info=True
            )
        return None

    async def _parse_snapshot(self, data: dict) -> Optional[dict]:
        try:
            symbol = data.get("market", "").replace("KRW-","")
            if not symbol:
                symbol = data.get("code", "").replace("KRW-","")
            if not symbol:
                logger.warning(
                    f"[{self.exchangename}] 스냅샷 심볼 정보 누락 | "
                    f"data={data}"
                )
                return None

            ts = data.get("timestamp", int(time.time()*1000))
            msg_type = "snapshot"
            bids = []
            asks = []
            for unit in data.get("orderbook_units", []):
                bp = float(unit.get("bid_price",0))
                bs = float(unit.get("bid_size",0))
                ap = float(unit.get("ask_price",0))
                as_ = float(unit.get("ask_size",0))
                if bp > 0:
                    bids.append([bp, bs])
                if ap > 0:
                    asks.append([ap, as_])

            logger.info(
                f"[{self.exchangename}] - {symbol} 스냅샷 파싱 완료 | "
                f"bids={len(bids)}, asks={len(asks)}, "
                f"timestamp={ts}"
            )

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
            logger.error(
                f"[{self.exchangename}] - {symbol} 스냅샷 파싱 실패 | "
                f"error={str(e)}",
                exc_info=True
            )
            return None

    async def _send_ping(self):
        """
        업비트 전용 ping 메시지 전송
        - 10초마다 PING 메시지 전송
        - 응답으로 {"status":"UP"} 수신
        """
        if self.ws and time.time() - self.last_ping_time > 10:  # 10초로 변경
            try:
                await self.ws.send("PING")  # 단순 "PING" 텍스트 전송
                self.last_ping_time = time.time()
                logger.debug("[Upbit] PING 전송")
            except Exception as e:
                logger.error(f"[Upbit] PING 전송 실패: {e}")

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        try:
            symbols = symbols_by_exchange.get(self.exchangename, [])
            if not symbols:
                logger.warning("[Upbit] 구독할 심볼이 없음")
                return

            logger.info(f"[Upbit] WebSocket 시작 | symbols={symbols}")

            while not self.stop_event.is_set():
                try:
                    await self.connect()
                    if hasattr(self, "connection_status_callback") and callable(self.connection_status_callback):
                        self.connection_status_callback(self.exchangename, "connect")
                        logger.info("[Upbit] 연결 상태 콜백 'connect' 호출")

                    # 재연결 시 초기화된 심볼 초기화
                    self.initialized_symbols.clear()
                    await self.subscribe(symbols)

                    while not self.stop_event.is_set():
                        try:
                            await self._send_ping()  # ping 전송
                            message = await self.ws.recv()
                            self.last_message_time = time.time()
                            parsed = await self.parse_message(message)
                            if parsed:
                                await self.handle_parsed_message(parsed)
                        except Exception as e:
                            logger.error(f"[Upbit] 메시지 수신 중 오류 발생 | error={str(e)}", exc_info=True)
                            break

                except Exception as e:
                    logger.error(f"[Upbit] WebSocket 처리 중 오류 발생 | error={str(e)}", exc_info=True)
                    await asyncio.sleep(5)
                finally:
                    if self.ws:
                        await self.ws.close()
                    self.is_connected = False
                    if hasattr(self, "connection_status_callback") and callable(self.connection_status_callback):
                        self.connection_status_callback(self.exchangename, "disconnect")
                        logger.debug("[Upbit] 연결 상태 콜백 'disconnect' 호출")
                    await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"[Upbit] 시작 처리 중 오류 발생 | error={str(e)}", exc_info=True)

    async def stop(self) -> None:
        try:
            self.stop_event.set()
            if self.ws:
                try:
                    await self.ws.close()
                    logger.info("[Upbit] WebSocket 연결 종료")
                except Exception as e:
                    logger.warning(
                        f"[Upbit] WebSocket 종료 중 오류 발생 | "
                        f"error={str(e)}"
                    )
            if self.session:
                await self.session.close()
                self.session = None
            self.orderbook_manager.clear_all()
            logger.info("[Upbit] 오더북 및 버퍼 초기화 완료")
        except Exception as e:
            logger.error(
                f"[Upbit] 종료 처리 중 오류 발생 | "
                f"error={str(e)}",
                exc_info=True
            )

    def _log_connection_status(self, status: str):
        """연결 상태 로깅"""
        logger.debug(f"[{self.exchangename}] 연결 상태 콜백 '{status}' 호출")

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