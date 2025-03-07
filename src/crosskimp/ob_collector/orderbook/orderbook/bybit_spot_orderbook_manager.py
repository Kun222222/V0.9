# file: orderbook/bybit_spot_orderbook_manager.py

import asyncio
import time
from typing import Dict, Optional, List
from aiohttp import ClientSession, ClientTimeout

from utils.logging.logger import bybit_logger
from orderbook.orderbook.base_orderbook_manager import BaseOrderBookManager
from orderbook.orderbook.base_orderbook import OrderBook, ValidationResult

SNAPSHOT_SEMAPHORE = asyncio.Semaphore(5)

GLOBAL_AIOHTTP_SESSION: Optional[ClientSession] = None
GLOBAL_SESSION_LOCK = asyncio.Lock()

async def get_global_aiohttp_session() -> ClientSession:
    global GLOBAL_AIOHTTP_SESSION
    async with GLOBAL_SESSION_LOCK:
        if GLOBAL_AIOHTTP_SESSION is None or GLOBAL_AIOHTTP_SESSION.closed:
            bybit_logger.info("[BybitSpot] Creating global aiohttp session")
            GLOBAL_AIOHTTP_SESSION = ClientSession(timeout=ClientTimeout(total=None))
        return GLOBAL_AIOHTTP_SESSION

class BybitSpotOrderBookManager(BaseOrderBookManager):
    """
    Bybit 현물 오더북 매니저
    """
    def __init__(self, depth: int = 500):
        super().__init__(depth)
        self.exchangename = "bybit"
        # 바이낸스와 유사하게 snapshot_url 로 명명
        self.snapshot_url = "https://api.bybit.com/v5/market/orderbook"
        self.logger = bybit_logger
        
        # 기존 초기화 코드
        self.orderbooks: Dict[str, OrderBook] = {}
        self.sequence_states: Dict[str, Dict] = {}
        self.buffer_events: Dict[str, List[dict]] = {}
        self.snapshot_retries: Dict[str, int] = {}
        
        # 로깅 제어를 위한 상태 추가
        self.last_queue_log_time: Dict[str, float] = {}
        self.queue_log_interval = 5.0  # 5초마다 로깅

    async def fetch_snapshot(self, symbol: str) -> Optional[dict]:
        await SNAPSHOT_SEMAPHORE.acquire()
        try:
            return await self._fetch_impl(symbol)
        finally:
            SNAPSHOT_SEMAPHORE.release()

    async def _fetch_impl(self, symbol: str) -> Optional[dict]:
        max_retries = 3
        delay = 1.0
        url = self.snapshot_url
        params = {
            "category": "spot",
            "symbol": f"{symbol.upper()}USDT",
            "limit": self.depth
        }

        for attempt in range(max_retries):
            try:
                session = await get_global_aiohttp_session()
                bybit_logger.info(
                    f"[BybitSpot] fetch_snapshot({symbol}) attempt={attempt+1}/{max_retries}, url={url}, params={params}"
                )
                async with session.get(url, params=params, timeout=10) as resp:
                    bybit_logger.info(f"[BybitSpot] {symbol} snapshot response status={resp.status}")
                    if resp.status == 200:
                        data = await resp.json()
                        bybit_logger.debug(f"[BybitSpot] {symbol} snapshot raw: {data}")
                        if data.get("retCode") == 0:
                            result = data.get("result", {})
                            parsed = self.parse_snapshot(result, symbol)
                            if parsed:
                                bybit_logger.info(f"[BybitSpot] {symbol} snapshot parse success: seq={parsed.get('sequence')}")
                                return parsed
                            else:
                                bybit_logger.error(f"[BybitSpot] {symbol} parse_snapshot returned None")
                        else:
                            bybit_logger.error(
                                f"[BybitSpot] {symbol} snapshot fail retCode={data.get('retCode')}, retMsg={data.get('retMsg')}"
                            )
                    else:
                        bybit_logger.error(f"[BybitSpot] {symbol} snapshot HTTP error status={resp.status}")
            except asyncio.TimeoutError:
                bybit_logger.error(f"[BybitSpot] {symbol} snapshot timeout (attempt={attempt+1})")
            except Exception as e:
                bybit_logger.error(f"[BybitSpot] {symbol} snapshot exception: {e}", exc_info=True)

            if attempt < max_retries - 1:
                await asyncio.sleep(delay)
                delay *= 2

        bybit_logger.error(f"[BybitSpot] {symbol} all snapshot attempts failed.")
        return None

    def parse_snapshot(self, data: dict, symbol: str) -> Optional[dict]:
        try:
            seq = data.get("seq", 0)
            ts = data.get("ts", int(time.time()*1000))
            b_raw = data.get("b", [])
            a_raw = data.get("a", [])

            bids = []
            for b in b_raw:
                px, qty = float(b[0]), float(b[1])
                if px > 0 and qty > 0:
                    bids.append([px, qty])

            asks = []
            for a in a_raw:
                px, qty = float(a[0]), float(a[1])
                if px > 0 and qty > 0:
                    asks.append([px, qty])

            bybit_logger.info(
                f"[BybitSpot] parse_snapshot({symbol}) seq={seq}, ts={ts}, "
                f"bids_len={len(bids)}, asks_len={len(asks)}"
            )
            return {
                "exchangename": self.exchangename,
                "symbol": symbol,
                "bids": bids,
                "asks": asks,
                "timestamp": ts,
                "sequence": seq,
                "type": "snapshot"
            }
        except Exception as e:
            bybit_logger.error(f"[BybitSpot] parse_snapshot error: {e}", exc_info=True)
            return None

    async def initialize_orderbook(self, symbol: str, snapshot: dict) -> ValidationResult:
        """
        스냅샷으로 오더북 초기화
        """
        try:
            seq = snapshot.get("sequence")
            if seq is None:
                err_msg = f"[BybitSpot] {symbol} snapshot has no sequence"
                bybit_logger.error(err_msg)
                return ValidationResult(False, [err_msg])

            self.sequence_states[symbol] = {
                "initialized": True,
                "last_seq": seq,
                "first_delta_applied": False
            }
            bybit_logger.info(f"[BybitSpot] initialize_orderbook({symbol}), seq={seq}")

            # 오더북 생성
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = OrderBook(
                    exchangename=self.exchangename,
                    symbol=symbol,
                    depth=self.depth,
                    logger=self.logger
                )

            ob = self.orderbooks[symbol]
            snap_res = await ob.update(snapshot)
            if not snap_res.is_valid:
                bybit_logger.error(
                    f"[BybitSpot] {symbol} snapshot apply fail: {snap_res.error_messages}"
                )
                return snap_res

            bybit_logger.info(f"[BybitSpot] {symbol} orderbook initialized with snapshot seq={seq}")
            await self._apply_buffered_events(symbol)
            return snap_res

        except Exception as e:
            bybit_logger.error(f"[BybitSpot] initialize_orderbook({symbol}) error: {e}", exc_info=True)
            return ValidationResult(False, [str(e)])

    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """
        델타 이벤트 추가 -> 버퍼에 쌓고, 만약 초기화된 상태면 바로 적용 시도
        """
        # 버퍼링(공통)
        if symbol not in self.buffer_events:
            self.buffer_events[symbol] = []
        if len(self.buffer_events[symbol]) >= self.max_buffer_size:
            removed = self.buffer_events[symbol].pop(0)
            bybit_logger.warning(f"[BybitSpot] {symbol} buffer overflow, removed oldest event seq={removed.get('sequence')}")

        self.buffer_events[symbol].append(data)
        bybit_logger.debug(f"[BybitSpot] {symbol} new event buffered seq={data.get('sequence')}")

        st = self.sequence_states.get(symbol, {})
        if not st.get("initialized"):
            bybit_logger.debug(f"[BybitSpot] {symbol} not initialized yet -> only buffering.")
            return ValidationResult(True, [])

        # 이미 초기화된 상태라면 버퍼 적용
        await self._apply_buffered_events(symbol)
        return ValidationResult(True, [])

    async def _apply_buffered_events(self, symbol: str):
        """
        버퍼 내 이벤트를 오름차순으로 적용
        적용 후 마지막 상태를 큐에 put
        """
        st = self.sequence_states.get(symbol)
        if not st or not st["initialized"]:
            bybit_logger.debug(f"[BybitSpot] _apply_buffered_events: {symbol} not inited -> return.")
            return

        last_seq = st["last_seq"]
        events = self.buffer_events.get(symbol, [])
        sorted_events = sorted(events, key=lambda x: x.get("sequence", 0))

        ob = self.orderbooks[symbol]
        applied_count = 0

        for evt in sorted_events:
            seq = evt.get("sequence", 0)
            
            # 시퀀스 번호 체계가 다른 경우를 위한 처리
            if last_seq > 1000000000 and seq < 1000000000:  # 스냅샷과 델타의 시퀀스 체계가 다른 경우
                bybit_logger.info(f"[BybitSpot] {symbol} sequence system changed: last_seq={last_seq} -> new_seq={seq}")
                last_seq = seq - 1  # 새로운 시퀀스 체계로 전환
                st["last_seq"] = last_seq
            
            if seq <= last_seq:
                bybit_logger.debug(f"[BybitSpot] _apply_buffered_events: skip old seq={seq}, last_seq={last_seq}")
                continue

            # gap 확인
            if seq > last_seq + 1:
                gap_size = seq - (last_seq + 1)
                bybit_logger.warning(
                    f"[BybitSpot] {symbol} seq gap detected: last_seq={last_seq}, new_seq={seq}, gap={gap_size}"
                )

            # 델타 적용
            res = await ob.update(evt)
            if not res.is_valid:
                bybit_logger.error(
                    f"[BybitSpot] {symbol} delta apply fail seq={seq}, err={res.error_messages}"
                )
                continue

            if not st["first_delta_applied"]:
                ob.enable_cross_detection()
                st["first_delta_applied"] = True
                bybit_logger.info(f"[BybitSpot] {symbol} first delta applied -> enable cross detection")

            last_seq = seq
            applied_count += 1

        # 버퍼 소진
        if applied_count > 0:
            bybit_logger.debug(
                f"[BybitSpot] {symbol} applied_count={applied_count} new_last_seq={last_seq}"
            )

        self.buffer_events[symbol] = []
        st["last_seq"] = last_seq

        # 최종 오더북을 큐에 넣기
        if applied_count > 0:
            final_ob = ob.to_dict()
            if self._output_queue:
                try:
                    await self._output_queue.put(("bybit", final_ob))
                    
                    # 로깅 주기 체크
                    current_time = time.time()
                    last_log_time = self.last_queue_log_time.get(symbol, 0)
                    if current_time - last_log_time >= self.queue_log_interval:
                        bybit_logger.debug(f"[BybitSpot] {symbol} final OB queued")
                        self.last_queue_log_time[symbol] = current_time
                        
                except Exception as e:
                    bybit_logger.error(f"[BybitSpot] queue fail: {e}", exc_info=True)

    def clear_symbol(self, symbol: str):
        self.orderbooks.pop(symbol, None)
        self.buffer_events.pop(symbol, None)
        self.sequence_states.pop(symbol, None)
        self.snapshot_retries.pop(symbol, None)
        bybit_logger.info(f"[BybitSpot] {symbol} data cleared")

    def clear_all(self):
        syms = list(self.orderbooks.keys())
        self.orderbooks.clear()
        self.buffer_events.clear()
        self.sequence_states.clear()
        self.snapshot_retries.clear()
        bybit_logger.info(f"[BybitSpot] all data cleared. syms={syms}")