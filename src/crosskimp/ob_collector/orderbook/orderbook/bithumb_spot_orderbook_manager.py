# file: core/websocket/exchanges/bithumb_spot_orderbook_manager.py

import asyncio
import time
from typing import Dict, List, Optional
from aiohttp import ClientSession, ClientTimeout

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.orderbook.base_orderbook import OrderBook, ValidationResult
from crosskimp.ob_collector.orderbook.orderbook.base_orderbook_manager import BaseOrderBookManager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

############################################
# 전역 aiohttp 세션 및 세마포어 (바이낸스 스타일)
############################################
GLOBAL_AIOHTTP_SESSION: Optional[ClientSession] = None
GLOBAL_SESSION_LOCK = asyncio.Lock()
SNAPSHOT_SEMAPHORE = asyncio.Semaphore(5)  # 동시에 최대 5개 스냅샷 요청

async def get_global_aiohttp_session() -> ClientSession:
    global GLOBAL_AIOHTTP_SESSION
    async with GLOBAL_SESSION_LOCK:
        if GLOBAL_AIOHTTP_SESSION is None or GLOBAL_AIOHTTP_SESSION.closed:
            logger.info("[BithumbSpot] 전역 aiohttp 세션 생성")
            GLOBAL_AIOHTTP_SESSION = ClientSession(timeout=ClientTimeout(total=None))
        return GLOBAL_AIOHTTP_SESSION


############################################
# BithumbSpotOrderBook (Binance의 OrderBook과 동일한 인터페이스)
############################################
class BithumbSpotOrderBook(OrderBook):
    """
    빗썸 현물 오더북 (호가 역전 감지 포함)
    내부 depth는 생성자에서 전달한 값(depth)을 사용하며, to_dict()에서는 상위 10개 레벨만 반환함.
    """
    def __init__(self, exchangename: str, symbol: str, depth: int = 500, logger=None):
        super().__init__(exchangename, symbol, depth=depth, logger=logger)
        # 빗썸 특화: 역전 감지 기본값은 끄지 않고 활성화 (바이낸스와 유사하게 처리할 수 있음)
        self.ignore_cross_detection = False
        self.cross_count = 0
        self.last_cross_time = 0.0
        self.cross_cooldown_sec = 0.5
        self.cross_threshold = 3

    async def update(self, data: dict) -> ValidationResult:
        # 기본 OrderBook.update() 호출 (내부 depth 유지, 정렬, 검증)
        result = await super().update(data)
        return result


############################################
# BithumbSpotOrderBookManager (BinanceSpotOrderBookManager과 유사)
############################################
class BithumbSpotOrderBookManager(BaseOrderBookManager):
    """
    빗썸 현물 오더북 매니저  
    - 스냅샷 요청, 파싱, 초기화, 델타 이벤트 버퍼링 및 순차 적용  
    - 타임스탬프를 시퀀스로 사용하는 빗썸 특화 로직 포함
    """
    def __init__(self, depth: int = 500):
        super().__init__(depth)
        self.exchangename = "bithumb"
        self.snapshot_url = "https://api.bithumb.com/public/orderbook"
        self.logger = get_unified_logger()
        self.orderbooks: Dict[str, BithumbSpotOrderBook] = {}
        self.sequence_states: Dict[str, Dict] = {}  # { symbol: { "initialized": bool, "last_update_id": int, "first_delta_applied": bool } }
        self.buffer_events: Dict[str, List[dict]] = {}
        self.max_buffer_size = 1000

        # output_queue는 외부에서 주입 (WebsocketManager에서 설정)
        self.output_queue = None

        # 로깅 제어를 위한 상태 추가
        self.last_queue_log_time: Dict[str, float] = {}
        self.queue_log_interval = 5.0  # 5초마다 로깅

    async def fetch_snapshot(self, symbol: str) -> Optional[dict]:
        """
        스냅샷 요청: GET /public/orderbook/{symbol}_KRW?count={depth}
        바이낸스 현물과 동일하게 전역 세마포어 및 재시도/백오프 적용
        """
        await SNAPSHOT_SEMAPHORE.acquire()
        try:
            return await self._fetch_snapshot_impl(symbol)
        finally:
            SNAPSHOT_SEMAPHORE.release()

    async def _fetch_snapshot_impl(self, symbol: str) -> Optional[dict]:
        max_retries = 3
        delay = 1.0
        url = f"{self.snapshot_url}/{symbol.upper()}_KRW?count={self.depth}"

        for attempt in range(max_retries):
            try:
                session = await get_global_aiohttp_session()
                self.logger.info(
                    f"[BithumbSpot] {symbol} 스냅샷 요청 (attempt={attempt+1}/{max_retries}, count={self.depth})"
                )
                async with session.get(url, timeout=30) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return self.parse_snapshot(data, symbol)
                    else:
                        self.logger.error(
                            f"[BithumbSpot] {symbol} 스냅샷 응답 에러 status={resp.status}"
                        )
            except asyncio.TimeoutError:
                self.logger.error(f"[BithumbSpot] {symbol} 스냅샷 타임아웃 (attempt={attempt+1})")
            except Exception as e:
                self.logger.error(
                    f"[BithumbSpot] {symbol} 스냅샷 요청 실패: {e}",
                    exc_info=True
                )
            if attempt < max_retries - 1:
                await asyncio.sleep(delay)
                delay *= 2
        return None

    def parse_snapshot(self, raw_data: dict, symbol: str) -> Optional[dict]:
        """빗썸 특화: 타임스탬프를 시퀀스로 사용하는 스냅샷 파싱"""
        if "data" not in raw_data:
            self.logger.error(f"[BithumbSpot] {symbol} 스냅샷에 data 필드 없음")
            return None

        data = raw_data["data"]
        bids_raw = data.get("bids", [])
        asks_raw = data.get("asks", [])
        try:
            timestamp = int(data.get("timestamp", str(int(time.time() * 1000))))
        except ValueError:
            timestamp = int(time.time() * 1000)

        bids = []
        asks = []
        for b in bids_raw:
            try:
                price = float(b["price"])
                qty = float(b["quantity"])
                if price > 0 and qty > 0:
                    bids.append([price, qty])
            except Exception:
                continue
        for a in asks_raw:
            try:
                price = float(a["price"])
                qty = float(a["quantity"])
                if price > 0 and qty > 0:
                    asks.append([price, qty])
            except Exception:
                continue

        bids.sort(key=lambda x: x[0], reverse=True)
        asks.sort(key=lambda x: x[0])
        parsed = {
            "exchangename": "bithumb",
            "symbol": symbol.upper(),
            "bids": bids,
            "asks": asks,
            "timestamp": timestamp,
            "sequence": timestamp,  # 빗썸 특화: 타임스탬프를 시퀀스로 사용
            "type": "snapshot"
        }
        self.logger.info(f"[BithumbSpot] {symbol} Snapshot Parsed: {parsed}")
        return parsed

    async def initialize_orderbook(self, symbol: str, snapshot: dict) -> ValidationResult:
        """빗썸 특화: 타임스탬프 기반 시퀀스 관리"""
        try:
            seq = snapshot.get("sequence")
            if seq is None:
                return ValidationResult(False, [f"{symbol} 스냅샷에 sequence 없음"])

            self.sequence_states[symbol] = {
                "initialized": True,
                "last_update_id": seq,
                "first_delta_applied": False
            }
            ob = BithumbSpotOrderBook("bithumb", symbol, depth=self.depth, logger=self.logger)
            self.orderbooks[symbol] = ob
            self.buffer_events.setdefault(symbol, [])

            res = await ob.update(snapshot)
            if not res.is_valid:
                return res

            await self._apply_buffered_events(symbol)
            return res
        except Exception as e:
            self.logger.error(f"[BithumbSpot] {symbol} 초기화 오류: {e}", exc_info=True)
            return ValidationResult(False, [str(e)])

    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """빗썸 특화: 타임스탬프 기반 시퀀스 관리"""
        if symbol not in self.buffer_events:
            self.buffer_events[symbol] = []
        if len(self.buffer_events[symbol]) >= self.max_buffer_size:
            self.buffer_events[symbol].pop(0)
        self.buffer_events[symbol].append(data)
        st = self.sequence_states.get(symbol)
        if not st or not st["initialized"]:
            self.logger.debug(f"[BithumbSpot] {symbol} 오더북 미초기화 -> 버퍼링만.")
            return ValidationResult(True, [])
        await self._apply_buffered_events(symbol)
        return ValidationResult(True, [])

    async def _apply_buffered_events(self, symbol: str):
        """빗썸 특화: 타임스탬프 기반 시퀀스 관리"""
        st = self.sequence_states.get(symbol)
        if not st or not st["initialized"]:
            return
        last_id = st["last_update_id"]
        events = self.buffer_events[symbol]
        sorted_evts = sorted(events, key=lambda x: x.get("timestamp", 0))
        ob = self.orderbooks[symbol]
        applied_count = 0

        for evt in sorted_evts:
            evt_seq = evt.get("sequence", evt.get("timestamp", 0))
            if evt_seq <= last_id:
                self.logger.debug(f"[BithumbSpot] {symbol} 이미 처리된 이벤트 스킵 | evt_seq={evt_seq}, last_id={last_id}")
                continue

            first_seq = evt.get("sequence", 0)
            if first_seq > last_id + 1:
                # self.logger.warning(f"[BithumbSpot] {symbol} 시퀀스 gap 발생 -> last_id={last_id}, first_seq={first_seq}")

                res = await ob.update(evt)
            if res.is_valid:
                if not st["first_delta_applied"]:
                    st["first_delta_applied"] = True
                    ob.enable_cross_detection()
                    self.logger.info(f"[BithumbSpot] {symbol} 첫 델타 적용 완료, 역전 감지 활성화")
                last_id = evt_seq
                applied_count += 1
            else:
                self.logger.error(f"[BithumbSpot] {symbol} 델타 적용 실패: {res.error_messages}")

        self.buffer_events[symbol] = []
        st["last_update_id"] = last_id

        if applied_count > 0 and self.output_queue:
            final_ob = ob.to_dict()
            try:
                await self.output_queue.put(("bithumb", final_ob))
                
                # 로깅 주기 체크
                current_time = time.time()
                last_log_time = self.last_queue_log_time.get(symbol, 0)
                if current_time - last_log_time >= self.queue_log_interval:
                    # self.logger.debug(f"[BithumbSpot] {symbol} 오더북 큐 전송 완료 | seq={last_id}")
                    self.last_queue_log_time[symbol] = current_time
                    
            except Exception as e:
                self.logger.error(f"[BithumbSpot] 큐 전송 실패: {e}", exc_info=True)

    def is_initialized(self, symbol: str) -> bool:
        st = self.sequence_states.get(symbol)
        return bool(st and st.get("initialized"))

    def get_orderbook(self, symbol: str) -> Optional[BithumbSpotOrderBook]:
        return self.orderbooks.get(symbol)

    def clear_symbol(self, symbol: str) -> None:
        self.orderbooks.pop(symbol, None)
        self.sequence_states.pop(symbol, None)
        self.buffer_events.pop(symbol, None)
        self.logger.info(f"[BithumbSpot] {symbol} 심볼 데이터 제거 완료")

    def clear_all(self) -> None:
        syms = list(self.orderbooks.keys())
        self.orderbooks.clear()
        self.sequence_states.clear()
        self.buffer_events.clear()
        self.logger.info(f"[BithumbSpot] 전체 심볼 데이터 제거 완료: {syms}")