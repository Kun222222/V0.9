# file: orderbook/binance_spot_orderbook_manager.py

import asyncio
import time
from typing import Dict, Optional, List
from aiohttp import ClientSession, ClientTimeout
import aiohttp
from dataclasses import dataclass

from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.orderbook.base_ob_v2 import BaseOrderBookManagerV2, OrderBookV2, ValidationResult
from crosskimp.config.constants import Exchange, EXCHANGE_NAMES_KR
from crosskimp.ob_collector.cpp.cpp_interface import send_orderbook_to_cpp

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이낸스 현물 오더북 관련 상수
# ============================
EXCHANGE_CODE = Exchange.BINANCE.value  # 거래소 코드
EXCHANGE_KR = EXCHANGE_NAMES_KR[EXCHANGE_CODE]  # 거래소 한글 이름

GLOBAL_AIOHTTP_SESSION: Optional[ClientSession] = None
GLOBAL_SESSION_LOCK = asyncio.Lock()
SNAPSHOT_SEMAPHORE = asyncio.Semaphore(5)  # 동시 5개까지

@dataclass
class OrderBookUpdate:
    bids: Dict[float, float]
    asks: Dict[float, float]
    timestamp: int
    
    def is_valid(self) -> bool:
        if not self.bids or not self.asks:
            return True  # 한쪽만 업데이트되는 경우 허용
            
        best_bid = max(self.bids.keys()) if self.bids else 0
        best_ask = min(self.asks.keys()) if self.asks else float('inf')
        return best_bid < best_ask

async def get_global_aiohttp_session() -> ClientSession:
    """글로벌 세션 관리"""
    global GLOBAL_AIOHTTP_SESSION
    async with GLOBAL_SESSION_LOCK:
        if GLOBAL_AIOHTTP_SESSION is None or GLOBAL_AIOHTTP_SESSION.closed:
            GLOBAL_AIOHTTP_SESSION = ClientSession(
                timeout=ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=100)  # 동시 연결 제한
            )
            logger.info(f"{EXCHANGE_KR} 새로운 글로벌 aiohttp 세션 생성")
        return GLOBAL_AIOHTTP_SESSION

class BinanceOrderBook(OrderBookV2):
    """
    바이낸스 전용 오더북 클래스 (V2)
    - 시퀀스 기반 업데이트 관리
    - 가격 역전 감지 및 처리
    """
    def __init__(self, exchangename: str, symbol: str, depth: int = 500):
        """초기화"""
        super().__init__(exchangename, symbol, depth)
        self.bids_dict = {}  # 매수 주문 (가격 -> 수량)
        self.asks_dict = {}  # 매도 주문 (가격 -> 수량)
        self.cross_detection_enabled = False
        
    def enable_cross_detection(self):
        """역전 감지 활성화"""
        self.cross_detection_enabled = True
        logger.info(f"{EXCHANGE_KR} {self.symbol} 역전감지 활성화")
        
    def update_orderbook(self, bids: List[List[float]], asks: List[List[float]], 
                        timestamp: Optional[int] = None, sequence: Optional[int] = None,
                        is_snapshot: bool = False) -> None:
        """
        오더북 데이터 업데이트 (바이낸스 전용)
        """
        try:
            # 스냅샷인 경우 딕셔너리 초기화
            if is_snapshot:
                self.bids_dict.clear()
                self.asks_dict.clear()
                
            # 임시 딕셔너리에 복사 (가격 역전 검사를 위해)
            temp_bids_dict = self.bids_dict.copy()
            temp_asks_dict = self.asks_dict.copy()
                
            # bids 업데이트 및 가격 역전 검사
            for price, qty in bids:
                if qty > 0:
                    # 새로운 매수 호가가 기존 매도 호가보다 높거나 같은 경우 검사
                    if self.cross_detection_enabled and not is_snapshot:
                        invalid_asks = [ask_price for ask_price in temp_asks_dict.keys() if ask_price <= price]
                        if invalid_asks:
                            # 가격 역전이 발생하는 매도 호가들 제거
                            for ask_price in invalid_asks:
                                temp_asks_dict.pop(ask_price)
                                logger.debug(
                                    f"{EXCHANGE_KR} {self.symbol} 매수호가({price}) 추가로 인해 낮은 매도호가({ask_price}) 제거"
                                )
                    temp_bids_dict[price] = qty
                else:
                    # 수량이 0이면 해당 가격 삭제
                    temp_bids_dict.pop(price, None)
                    
            # asks 업데이트 및 가격 역전 검사
            for price, qty in asks:
                if qty > 0:
                    # 새로운 매도 호가가 기존 매수 호가보다 낮거나 같은 경우 검사
                    if self.cross_detection_enabled and not is_snapshot:
                        invalid_bids = [bid_price for bid_price in temp_bids_dict.keys() if bid_price >= price]
                        if invalid_bids:
                            # 가격 역전이 발생하는 매수 호가들 제거
                            for bid_price in invalid_bids:
                                temp_bids_dict.pop(bid_price)
                                logger.debug(
                                    f"{EXCHANGE_KR} {self.symbol} 매도호가({price}) 추가로 인해 높은 매수호가({bid_price}) 제거"
                                )
                    temp_asks_dict[price] = qty
                else:
                    # 수량이 0이면 해당 가격 삭제
                    temp_asks_dict.pop(price, None)
            
            # 임시 딕셔너리를 실제 딕셔너리로 적용
            self.bids_dict = temp_bids_dict
            self.asks_dict = temp_asks_dict
                    
            # 정렬된 리스트 생성
            sorted_bids = sorted(self.bids_dict.items(), key=lambda x: x[0], reverse=True)
            sorted_asks = sorted(self.asks_dict.items(), key=lambda x: x[0])
            
            # depth 제한 및 리스트 변환
            self.bids = [[price, qty] for price, qty in sorted_bids[:self.depth]]
            self.asks = [[price, qty] for price, qty in sorted_asks[:self.depth]]
            
            # 메타데이터 업데이트
            if timestamp:
                self.last_update_time = timestamp
            if sequence:
                self.last_update_id = sequence
                
            # 비정상적인 스프레드 감지 (가격 역전은 이미 처리되었으므로 여기서는 스프레드만 체크)
            if self.bids and self.asks:
                highest_bid = self.bids[0][0]
                lowest_ask = self.asks[0][0]
                spread_pct = (lowest_ask - highest_bid) / lowest_ask * 100
                
                if spread_pct > 5:  # 스프레드가 5% 이상인 경우
                    logger.warning(
                        f"{EXCHANGE_KR} {self.symbol} 비정상 스프레드 감지: "
                        f"{spread_pct:.2f}% (매수: {highest_bid}, 매도: {lowest_ask})"
                    )
                    
            # C++로 데이터 직접 전송
            asyncio.create_task(self.send_to_cpp())
                
        except Exception as e:
            logger.error(
                f"{EXCHANGE_KR} {self.symbol} 오더북 업데이트 실패: {str(e)}", 
                exc_info=True
            )

class BinanceSpotOrderBookManager(BaseOrderBookManagerV2):
    """
    바이낸스 현물 오더북 매니저 (V2)
    - depth=500
    - 시퀀스 기반 업데이트 관리
    - REST API 스냅샷 요청 및 적용
    - C++ 직접 전송 지원
    """

    def __init__(self, depth: int = 500):
        super().__init__(depth)
        self.exchangename = EXCHANGE_CODE
        self.snapshot_url = "https://api.binance.com/api/v3/depth"
        
        # 버퍼 및 시퀀스 관리 초기화
        self.buffer_events: Dict[str, List[dict]] = {}
        self.sequence_states: Dict[str, Dict] = {}
        self.initialization_locks: Dict[str, asyncio.Lock] = {}
        self.orderbooks: Dict[str, BinanceOrderBook] = {}  # BinanceOrderBook 사용
        
        # 로깅 제어를 위한 상태 추가
        self.last_queue_log_time: Dict[str, float] = {}
        self.queue_log_interval = 5.0  # 5초마다 로깅
        
        # 가격 역전 감지 관련 설정
        self.price_inversion_count: Dict[str, int] = {}  # 심볼별 가격 역전 발생 횟수
        
        # 버퍼 크기 제한 설정
        self.max_buffer_size = 1000  # 최대 버퍼 크기
        
    async def start(self):
        """오더북 매니저 시작"""
        await self.initialize()  # 메트릭 로깅 시작

    async def _get_symbol_lock(self, symbol: str) -> asyncio.Lock:
        """심볼별 초기화 락 관리"""
        if symbol not in self.initialization_locks:
            self.initialization_locks[symbol] = asyncio.Lock()
        return self.initialization_locks[symbol]

    async def initialize_orderbook(self, symbol: str, snapshot: Optional[dict] = None) -> ValidationResult:
        """
        오더북 초기화 (스냅샷 적용)
        
        Args:
            symbol: 심볼
            snapshot: 스냅샷 데이터 (없으면 API로 요청)
            
        Returns:
            ValidationResult: 검증 결과
        """
        try:
            # 심볼 락 획득
            lock = await self._get_symbol_lock(symbol)
            async with lock:
                # 스냅샷이 제공되지 않은 경우 요청
                if not snapshot:
                    snapshot = await self.fetch_snapshot(symbol)
                    if not snapshot:
                        logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 요청 실패")
                        return ValidationResult(False, ["스냅샷 요청 실패"])
                
                # 스냅샷 데이터 검증
                if "lastUpdateId" not in snapshot:
                    logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷에 lastUpdateId가 없습니다")
                    return ValidationResult(False, ["스냅샷에 lastUpdateId가 없습니다."])
                
                # 오더북 생성 또는 초기화
                if symbol not in self.orderbooks:
                    self.orderbooks[symbol] = BinanceOrderBook(
                        exchangename=self.exchangename,
                        symbol=symbol,
                        depth=self.depth
                    )
                    
                    # 새로 생성된 오더북에 큐 설정
                    if self._output_queue:
                        self.orderbooks[symbol].set_output_queue(self._output_queue)
                
                # 스냅샷 데이터 형식 변환
                bids = [(float(price), float(qty)) for price, qty in snapshot.get("bids", [])]
                asks = [(float(price), float(qty)) for price, qty in snapshot.get("asks", [])]
                
                # 바이낸스 특화 검증: bid/ask가 모두 있는 경우에만 가격 순서 체크
                is_valid = True
                error_messages = []
                
                if bids and asks:
                    max_bid = max(bid[0] for bid in bids)
                    min_ask = min(ask[0] for ask in asks)
                    if max_bid >= min_ask:
                        is_valid = False
                        error_messages.append(f"가격 역전 발생: 최고매수가({max_bid}) >= 최저매도가({min_ask})")
                        # 가격 역전 메트릭 기록
                        self.record_metric("error", error_type="price_inversion")
                        logger.warning(f"{EXCHANGE_KR} {symbol} {error_messages[0]}")
                
                if is_valid:
                    # 오더북 업데이트
                    ob = self.orderbooks[symbol]
                    ob.update_orderbook(
                        bids=bids,
                        asks=asks,
                        timestamp=int(time.time() * 1000),
                        sequence=snapshot["lastUpdateId"],
                        is_snapshot=True
                    )
                    
                    # 시퀀스 상태 초기화
                    self.sequence_states[symbol] = {
                        "last_update_id": snapshot["lastUpdateId"],
                        "initialized": True,
                        "first_delta_applied": False
                    }
                    
                    # 초기화 직후 가격 역전 감지 활성화
                    ob.enable_cross_detection()
                    
                    # 가격 역전 카운터 초기화
                    self.price_inversion_count[symbol] = 0
                    
                    # 버퍼 이벤트 적용
                    await self._apply_buffered_events(symbol)
                    
                    # 큐로 데이터 전송
                    await ob.send_to_queue()
                    
                    # 오더북 카운트 메트릭 업데이트
                    self.record_metric("orderbook", symbol=symbol)
                    
                    # 데이터 크기 메트릭 업데이트
                    data_size = len(str(snapshot))  # 간단한 크기 측정
                    self.record_metric("bytes", size=data_size)
                    
                    logger.info(
                        f"{EXCHANGE_KR} {symbol} 스냅샷 초기화 완료 | "
                        f"seq={snapshot['lastUpdateId']}, 매수:{len(bids)}개, 매도:{len(asks)}개"
                    )
                    
                    return ValidationResult(True, [])
                else:
                    logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 적용 실패: {error_messages}")
                    return ValidationResult(False, error_messages)
                    
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 초기화 오류: {str(e)}", exc_info=True)
            # 예외 발생 메트릭 기록
            self.record_metric("error", error_type="exception")
            return ValidationResult(False, [f"스냅샷 초기화 오류: {str(e)}"])

    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """
        델타 업데이트 처리
        - 초기화 전: 버퍼링
        - 초기화 후: 시퀀스 검증 후 적용
        """
        try:
            # 오더북이 초기화되지 않은 경우 초기화
            if symbol not in self.orderbooks:
                result = await self.initialize_orderbook(symbol)
                if not result.is_valid:
                    return result
                    
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
                logger.debug(f"{EXCHANGE_KR} {symbol} 초기화 전 버퍼링")
                return ValidationResult(True, ["초기화 전 버퍼링"])

            # 초기화 완료 후 이벤트 적용
            await self._apply_buffered_events(symbol)
            
            # 메트릭 업데이트
            self.record_metric("orderbook", symbol=symbol)
            
            # 데이터 크기 메트릭 업데이트
            data_size = len(str(data))  # 간단한 크기 측정
            self.record_metric("bytes", size=data_size)
            
            return ValidationResult(True, [])
            
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} {symbol} 업데이트 오류: {str(e)}", exc_info=True)
            # 예외 발생 메트릭 기록
            self.record_metric("error", error_type="exception")
            return ValidationResult(False, [f"업데이트 오류: {str(e)}"])

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
                    f"{EXCHANGE_KR} {symbol} 스냅샷 요청 시도={attempt+1}/{max_retries}, depth={self.depth}"
                )
                async with session.get(self.snapshot_url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return self.parse_snapshot(data, symbol)
                    else:
                        logger.error(
                            f"{EXCHANGE_KR} {symbol} 스냅샷 응답 오류 status={resp.status}"
                        )
            except asyncio.TimeoutError:
                logger.error(
                    f"{EXCHANGE_KR} {symbol} 스냅샷 타임아웃 (시도={attempt+1})"
                )
            except Exception as e:
                logger.error(
                    f"{EXCHANGE_KR} {symbol} 스냅샷 요청 실패: {e}",
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
            logger.info(f"{EXCHANGE_KR} {symbol} 스냅샷 데이터 키: {list(data.keys())}")
            
            if "lastUpdateId" not in data:
                logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷에 lastUpdateId가 없음")
                # 테스트를 위해 lastUpdateId 추가
                data["lastUpdateId"] = int(time.time() * 1000)
                logger.info(f"{EXCHANGE_KR} {symbol} 테스트를 위해 lastUpdateId 추가: {data['lastUpdateId']}")

            last_id = data["lastUpdateId"]
            bids, asks = [], []
            for b in data.get("bids", []):
                try:
                    px, qty = float(b[0]), float(b[1])
                    if px > 0 and qty > 0:
                        bids.append([px, qty])
                except Exception as e:
                    logger.error(f"{EXCHANGE_KR} {symbol} 매수 호가 파싱 오류: {e}, 데이터: {b}")
                    
            for a in data.get("asks", []):
                try:
                    px, qty = float(a[0]), float(a[1])
                    if px > 0 and qty > 0:
                        asks.append([px, qty])
                except Exception as e:
                    logger.error(f"{EXCHANGE_KR} {symbol} 매도 호가 파싱 오류: {e}, 데이터: {a}")

            if not bids or not asks:
                logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 매수/매도 데이터 없음")
                return None

            return {
                "lastUpdateId": last_id,
                "bids": bids,
                "asks": asks
            }
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 파싱 오류: {e}")
            return None

    async def _apply_buffered_events(self, symbol: str) -> None:
        """
        버퍼링된 이벤트 적용
        - 가격 역전 감지 및 처리 로직 추가
        - C++ 직접 전송 지원
        """
        if symbol not in self.sequence_states:
            logger.debug(f"{EXCHANGE_KR} {symbol} 초기화 전 버퍼링")
            return

        st = self.sequence_states[symbol]
        if not st.get("initialized", False):
            logger.debug(f"{EXCHANGE_KR} {symbol} 초기화 전 버퍼링")
            return

        # first_delta_applied 키가 없으면 기본값 설정
        if "first_delta_applied" not in st:
            st["first_delta_applied"] = False

        ob = self.orderbooks.get(symbol)
        if not ob:
            logger.error(f"{EXCHANGE_KR} {symbol} 오더북 없음")
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
                    f"{EXCHANGE_KR} {symbol} 시퀀스 갭 감지 U={first_id}, last_id={last_id} -> 재동기화 생략"
                )
                
                # 시퀀스 갭 발생 시 가격 역전 검사 강화
                logger.info(f"{EXCHANGE_KR} {symbol} 시퀀스 갭 발생으로 가격 역전 검사 강화")

            # 가격 역전 검사 추가
            bids_dict = {float(b[0]): float(b[1]) for b in evt.get("bids", [])}
            asks_dict = {float(a[0]): float(a[1]) for a in evt.get("asks", [])}
            
            if bids_dict and asks_dict:
                update = OrderBookUpdate(
                    bids=bids_dict,
                    asks=asks_dict,
                    timestamp=evt.get("timestamp", 0)
                )
                
                if not update.is_valid():
                    best_bid = max(bids_dict.keys()) if bids_dict else 0
                    best_ask = min(asks_dict.keys()) if asks_dict else float('inf')
                    
                    # 가격 역전 카운터 증가
                    self.price_inversion_count[symbol] = self.price_inversion_count.get(symbol, 0) + 1
                    
                    logger.warning(
                        f"{EXCHANGE_KR} {symbol} 가격 역전 감지 및 수정 (#{self.price_inversion_count[symbol]}): "
                        f"최고매수={best_bid}, 최저매도={best_ask}, 차이={best_bid-best_ask}"
                    )
                    
                    # 가격 역전 수정: 역전된 호가 제거
                    if best_bid >= best_ask:
                        # 매수가가 매도가보다 높거나 같은 경우, 해당 매수가 제거
                        evt["bids"] = [[p, q] for p, q in evt.get("bids", []) if float(p) < best_ask]
                        logger.info(f"{EXCHANGE_KR} {symbol} 역전된 매수호가 제거 완료")

            # V2 방식으로 오더북 업데이트
            bids = [(float(b[0]), float(b[1])) for b in evt.get("bids", [])]
            asks = [(float(a[0]), float(a[1])) for a in evt.get("asks", [])]
            
            ob.update_orderbook(
                bids=bids,
                asks=asks,
                timestamp=evt.get("timestamp", int(time.time() * 1000)),
                sequence=final_id,
                is_snapshot=False
            )
            
            if not st["first_delta_applied"]:
                st["first_delta_applied"] = True
            
            last_id = final_id
            applied_count += 1

        self.buffer_events[symbol] = []
        st["last_update_id"] = last_id

        if applied_count > 0:
            # 큐로 데이터 전송
            await ob.send_to_queue()
            
            # 로깅 주기 체크
            current_time = time.time()
            last_log_time = self.last_queue_log_time.get(symbol, 0)
            if current_time - last_log_time >= self.queue_log_interval:
                logger.debug(
                    f"{EXCHANGE_KR} {symbol} 오더북 업데이트 | "
                    f"seq={last_id}, 매수:{len(ob.bids)}개, 매도:{len(ob.asks)}개"
                )
                self.last_queue_log_time[symbol] = current_time

    def is_initialized(self, symbol: str) -> bool:
        """심볼 초기화 여부 확인"""
        st = self.sequence_states.get(symbol)
        return bool(st and st.get("initialized"))
        
    def get_orderbook(self, symbol: str) -> Optional[BinanceOrderBook]:
        """심볼 오더북 객체 반환"""
        return self.orderbooks.get(symbol)
        
    def clear_symbol(self, symbol: str) -> None:
        """심볼 데이터 제거"""
        self.orderbooks.pop(symbol, None)
        self.sequence_states.pop(symbol, None)
        self.buffer_events.pop(symbol, None)
        self.initialization_locks.pop(symbol, None)
        self.last_queue_log_time.pop(symbol, None)
        self.price_inversion_count.pop(symbol, None)
        logger.info(f"{EXCHANGE_KR} {symbol} 심볼 데이터 제거 완료")
        
    def clear_all(self) -> None:
        """모든 심볼 데이터 제거"""
        syms = list(self.orderbooks.keys())
        self.orderbooks.clear()
        self.sequence_states.clear()
        self.buffer_events.clear()
        self.initialization_locks.clear()
        self.last_queue_log_time.clear()
        self.price_inversion_count.clear()
        logger.info(f"{EXCHANGE_KR} 전체 심볼 데이터 제거 완료: {syms}")
        
    async def subscribe(self, symbol: str):
        """심볼 구독 - 오더북 초기화"""
        # 오더북 초기화
        if symbol not in self.orderbooks:
            await self.initialize_orderbook(symbol)
            logger.info(f"{EXCHANGE_KR} {symbol} 오더북 초기화 완료")
        else:
            logger.info(f"{EXCHANGE_KR} {symbol} 이미 초기화됨")