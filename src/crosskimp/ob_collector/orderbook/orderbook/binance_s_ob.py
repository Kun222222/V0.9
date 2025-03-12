# file: orderbook/binance_spot_orderbook_manager.py

import asyncio
import time
from typing import Dict, Optional, List
from aiohttp import ClientSession, ClientTimeout
import aiohttp

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.orderbook.base_ob import OrderBook, ValidationResult
from crosskimp.ob_collector.orderbook.orderbook.base_ob_manager import BaseOrderBookManager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

GLOBAL_AIOHTTP_SESSION: Optional[ClientSession] = None
GLOBAL_SESSION_LOCK = asyncio.Lock()
SNAPSHOT_SEMAPHORE = asyncio.Semaphore(5)  # 동시 5개까지

async def get_global_aiohttp_session() -> ClientSession:
    """글로벌 세션 관리"""
    global GLOBAL_AIOHTTP_SESSION
    async with GLOBAL_SESSION_LOCK:
        if GLOBAL_AIOHTTP_SESSION is None or GLOBAL_AIOHTTP_SESSION.closed:
            GLOBAL_AIOHTTP_SESSION = ClientSession(
                timeout=ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=100)  # 동시 연결 제한
            )
            logger.info("[binance] Created new global aiohttp session")
        return GLOBAL_AIOHTTP_SESSION

class BinanceSpotOrderBookManager(BaseOrderBookManager):
    """
    바이낸스 현물 오더북 매니저
    - depth=500
    - gap 시 재스냅샷 대신 경고만
    """

    def __init__(self, depth: int = 500):
        super().__init__(depth)
        self.exchangename = "binance"
        self.snapshot_url = "https://api.binance.com/api/v3/depth"
        
        # 버퍼 및 시퀀스 관리 초기화
        self.buffer_events: Dict[str, List[dict]] = {}
        self.sequence_states: Dict[str, Dict] = {}
        self.initialization_locks: Dict[str, asyncio.Lock] = {}
        
        # 로깅 제어를 위한 상태 추가
        self.last_queue_log_time: Dict[str, float] = {}
        self.queue_log_interval = 5.0  # 5초마다 로깅

    async def _get_symbol_lock(self, symbol: str) -> asyncio.Lock:
        """심볼별 초기화 락 관리"""
        if symbol not in self.initialization_locks:
            self.initialization_locks[symbol] = asyncio.Lock()
        return self.initialization_locks[symbol]

    async def initialize_orderbook(self, symbol: str, snapshot: dict) -> ValidationResult:
        """
        오더북 초기화 (스냅샷 적용)
        
        Args:
            symbol: 심볼
            snapshot: 스냅샷 데이터
            
        Returns:
            ValidationResult: 검증 결과
        """
        try:
            # 심볼 락 획득
            lock = await self._get_symbol_lock(symbol)
            async with lock:
                # 스냅샷 데이터 검증
                if "lastUpdateId" not in snapshot:
                    logger.error(f"[{self.exchangename}] {symbol} snapshot missing lastUpdateId")
                    return ValidationResult(False, ["스냅샷에 lastUpdateId가 없습니다."])
                
                # 오더북 생성 또는 초기화
                if symbol not in self.orderbooks:
                    self.orderbooks[symbol] = OrderBook(self.exchangename, symbol, self.depth)
                
                # 스냅샷 데이터 형식 변환
                formatted_snapshot = {
                    "type": "snapshot",
                    "bids": [(float(price), float(qty)) for price, qty in snapshot.get("bids", [])],
                    "asks": [(float(price), float(qty)) for price, qty in snapshot.get("asks", [])],
                }
                
                # 스냅샷 적용
                result = await self.orderbooks[symbol].update(formatted_snapshot)
                
                if result.is_valid:
                    # 시퀀스 상태 초기화
                    self.sequence_states[symbol] = {
                        "last_update_id": snapshot["lastUpdateId"],
                        "initialized": True
                    }
                    
                    # 버퍼 이벤트 적용
                    await self._apply_buffered_events(symbol)
                    
                    # 스냅샷 수신 콜백 호출 (추가)
                    if hasattr(self, 'connection_status_callback') and self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "snapshot")
                    
                    # 출력 큐에 스냅샷 메시지 전송 - 스냅샷은 큐로 전송하지 않도록 주석 처리
                    """
                    if self._output_queue:
                        await self._output_queue.put((
                            self.exchangename,
                            {
                                "type": "snapshot",
                                "symbol": symbol,
                                "timestamp": time.time(),
                                "data": {
                                    "bids": self.orderbooks[symbol].get_bids(10),
                                    "asks": self.orderbooks[symbol].get_asks(10)
                                }
                            }
                        ))
                    """
                    
                    logger.info(f"[BaseOrderBook][{self.exchangename}] - {symbol} 스냅샷 init")
                    return result
                else:
                    logger.error(f"[{self.exchangename}] {symbol} 스냅샷 적용 실패: {result.error_messages}")
                    return result
                    
        except Exception as e:
            logger.error(f"[{self.exchangename}] {symbol} 스냅샷 초기화 오류: {str(e)}")
            return ValidationResult(False, [f"스냅샷 초기화 오류: {str(e)}"])

    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """
        델타 업데이트 처리
        - 초기화 전: 버퍼링
        - 초기화 후: 시퀀스 검증 후 적용
        """
        # 버퍼 초기화
        if symbol not in self.buffer_events:
            self.buffer_events[symbol] = []
            
        # 버퍼 크기 제한
        if len(self.buffer_events[symbol]) >= self.max_buffer_size:
            self.buffer_events[symbol].pop(0)
            
        # 이벤트 버퍼링
        self.buffer_events[symbol].append(data)

        # 초기화 상태 확인
        st = self.sequence_states.get(symbol)
        if not st or not st["initialized"]:
            logger.debug(f"[{self.exchangename}] {symbol} 초기화 전 버퍼링")
            return ValidationResult(True, [])

        # 초기화 완료 후 이벤트 적용
        await self._apply_buffered_events(symbol)
        return ValidationResult(True, [])

    async def fetch_snapshot(self, symbol: str) -> Optional[dict]:
        """REST API로 스냅샷 요청"""
        await SNAPSHOT_SEMAPHORE.acquire()
        try:
            return await self._fetch_impl(symbol)
        finally:
            SNAPSHOT_SEMAPHORE.release()

    async def _fetch_impl(self, symbol: str) -> Optional[dict]:
        max_retries = 3
        delay = 1.0
        params = {"symbol": f"{symbol.upper()}USDT", "limit": self.depth}

        for attempt in range(max_retries):
            try:
                session = await get_global_aiohttp_session()
                logger.info(
                    f"[{self.exchangename}] {symbol} snapshot request attempt={attempt+1}/{max_retries}, depth={self.depth}"
                )
                async with session.get(self.snapshot_url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return self.parse_snapshot(data, symbol)
                    else:
                        logger.error(
                            f"[{self.exchangename}] {symbol} snapshot response error status={resp.status}"
                        )
            except asyncio.TimeoutError:
                logger.error(
                    f"[{self.exchangename}] {symbol} snapshot timeout (attempt={attempt+1})"
                )
            except Exception as e:
                logger.error(
                    f"[{self.exchangename}] {symbol} snapshot fail: {e}",
                    exc_info=True
                )
                if attempt < max_retries - 1:
                    # 세션 재생성 시도
                    global GLOBAL_AIOHTTP_SESSION
                    async with GLOBAL_SESSION_LOCK:
                        if GLOBAL_AIOHTTP_SESSION:
                            await GLOBAL_AIOHTTP_SESSION.close()
                            GLOBAL_AIOHTTP_SESSION = None
            
            if attempt < max_retries - 1:
                await asyncio.sleep(delay)
                delay *= 2

        return None

    def parse_snapshot(self, data: dict, symbol: str) -> Optional[dict]:
        """스냅샷 데이터 파싱"""
        try:
            # 스냅샷 데이터 로깅
            logger.info(f"[{self.exchangename}] {symbol} 스냅샷 데이터 키: {list(data.keys())}")
            
            if "lastUpdateId" not in data:
                logger.error(f"[{self.exchangename}] {symbol} snapshot missing lastUpdateId")
                # 테스트를 위해 lastUpdateId 추가
                data["lastUpdateId"] = int(time.time() * 1000)
                logger.info(f"[{self.exchangename}] {symbol} 테스트를 위해 lastUpdateId 추가: {data['lastUpdateId']}")

            last_id = data["lastUpdateId"]
            bids, asks = [], []
            for b in data.get("bids", []):
                try:
                    px, qty = float(b[0]), float(b[1])
                    if px > 0 and qty > 0:
                        bids.append([px, qty])
                except Exception as e:
                    logger.error(f"[{self.exchangename}] {symbol} 매수 호가 파싱 오류: {e}, 데이터: {b}")
                    
            for a in data.get("asks", []):
                try:
                    px, qty = float(a[0]), float(a[1])
                    if px > 0 and qty > 0:
                        asks.append([px, qty])
                except Exception as e:
                    logger.error(f"[{self.exchangename}] {symbol} 매도 호가 파싱 오류: {e}, 데이터: {a}")

            if not bids or not asks:
                logger.error(f"[{self.exchangename}] {symbol} snapshot empty bids or asks")
                return None

            return {
                "lastUpdateId": last_id,
                "bids": bids,
                "asks": asks
            }
        except Exception as e:
            logger.error(f"[{self.exchangename}] {symbol} 스냅샷 파싱 오류: {e}")
            return None

    async def _apply_buffered_events(self, symbol: str) -> None:
        """버퍼링된 이벤트 적용"""
        if symbol not in self.sequence_states:
            logger.debug(f"[{self.exchangename}] {symbol} 초기화 전 버퍼링")
            return

        st = self.sequence_states[symbol]
        if not st.get("initialized", False):
            logger.debug(f"[{self.exchangename}] {symbol} 초기화 전 버퍼링")
            return

        # first_delta_applied 키가 없으면 기본값 설정
        if "first_delta_applied" not in st:
            st["first_delta_applied"] = False

        ob = self.orderbooks.get(symbol)
        if not ob:
            logger.error(f"[{self.exchangename}] {symbol} 오더북 없음")
            return

        if symbol not in self.buffer_events:
            return

        events = self.buffer_events[symbol]
        if not events:
            return

        last_id = st.get("last_update_id", 0)
        applied_count = 0

        for evt in sorted(events, key=lambda x: x.get("final_update_id", 0)):
            final_id = evt.get("final_update_id", 0)
            if final_id <= last_id:
                continue

            first_id = evt.get("first_update_id", 0)
            if first_id > last_id + 1:
                logger.warning(
                    f"[{self.exchangename}] {symbol} gap detected U={first_id}, last_id={last_id} -> skip resync"
                )

            res = await ob.update(evt)
            if res.is_valid:
                if not st["first_delta_applied"]:
                    st["first_delta_applied"] = True
                    ob.enable_cross_detection()
                last_id = final_id
                applied_count += 1
            else:
                logger.error(
                    f"[{self.exchangename}] {symbol} delta apply fail: {res.error_messages}"
                )

        self.buffer_events[symbol] = []
        st["last_update_id"] = last_id

        if applied_count > 0 and self._output_queue:
            final_ob = ob.to_dict()
            try:
                await self._output_queue.put((self.exchangename, final_ob))
                
                # 로깅 주기 체크
                current_time = time.time()
                last_log_time = self.last_queue_log_time.get(symbol, 0)
                if current_time - last_log_time >= self.queue_log_interval:
                    # self.logger.debug(f"[{self.exchangename}] {symbol} final OB queued")
                    self.last_queue_log_time[symbol] = current_time
                    
            except Exception as e:
                logger.error(
                    f"[{self.exchangename}] queue fail: {e}",
                    exc_info=True
                )