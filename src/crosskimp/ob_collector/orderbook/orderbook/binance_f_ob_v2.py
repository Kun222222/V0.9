import asyncio
import time
from typing import Dict, Optional, List, Any, Tuple
from dataclasses import dataclass
import aiohttp

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.orderbook.base_ob_v2 import BaseOrderBookManagerV2, OrderBookV2, ValidationResult
from crosskimp.ob_collector.utils.config.constants import Exchange, EXCHANGE_NAMES_KR
from crosskimp.ob_collector.cpp.cpp_interface import send_orderbook_to_cpp

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이낸스 선물 오더북 관련 상수
# ============================
EXCHANGE_CODE = Exchange.BINANCE_FUTURE.value  # 거래소 코드
EXCHANGE_KR = EXCHANGE_NAMES_KR[EXCHANGE_CODE]  # 거래소 한글 이름

# REST API 설정 (스냅샷 요청용)
REST_BASE_URL = "https://fapi.binance.com/fapi/v1/depth"  # REST API 기본 URL

# 웹소켓 연결 설정 - 2024년 4월 이후 변경된 URL
WS_BASE_URL = "wss://fstream.binance.com/stream?streams="  # 웹소켓 기본 URL

# 오더북 관련 설정
UPDATE_SPEED = "100ms"  # 웹소켓 업데이트 속도 (100ms, 250ms, 500ms 중 선택)
DEFAULT_DEPTH = 500     # 기본 오더북 깊이
MAX_RETRY_COUNT = 3     # 최대 재시도 횟수
MAX_BUFFER_SIZE = 1000  # 최대 버퍼 크기

# 스냅샷 요청 설정
SNAPSHOT_RETRY_DELAY = 1  # 스냅샷 요청 재시도 초기 딜레이 (초)
SNAPSHOT_REQUEST_TIMEOUT = 10  # 스냅샷 요청 타임아웃 (초)

# DNS 캐시 설정
DNS_CACHE_TTL = 300  # DNS 캐시 TTL (초)

# 세션 관리
GLOBAL_AIOHTTP_SESSION: Optional[aiohttp.ClientSession] = None
GLOBAL_SESSION_LOCK = asyncio.Lock()
SNAPSHOT_SEMAPHORE = asyncio.Semaphore(5)  # 동시 5개까지

@dataclass
class OrderBookUpdate:
    """
    오더북 업데이트 정보
    """
    is_valid: bool = True
    error_messages: List[str] = None
    symbol: str = ""
    data: Dict = None
    bids: List[List[float]] = None
    asks: List[List[float]] = None
    timestamp: int = 0
    sequence: int = 0
    first_update_id: int = 0
    final_update_id: int = 0
    prev_final_update_id: int = 0
    transaction_time: int = 0
    buffered: bool = False
    
    def __post_init__(self):
        if self.error_messages is None:
            self.error_messages = []
        if self.bids is None:
            self.bids = []
        if self.asks is None:
            self.asks = []
        if self.data is None:
            self.data = {}

async def get_global_aiohttp_session() -> aiohttp.ClientSession:
    """글로벌 세션 관리"""
    global GLOBAL_AIOHTTP_SESSION
    async with GLOBAL_SESSION_LOCK:
        if GLOBAL_AIOHTTP_SESSION is None or GLOBAL_AIOHTTP_SESSION.closed:
            GLOBAL_AIOHTTP_SESSION = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=100, ttl_dns_cache=DNS_CACHE_TTL)  # 동시 연결 제한 및 DNS 캐시 설정
            )
            logger.info(f"{EXCHANGE_KR} 새로운 글로벌 aiohttp 세션 생성")
        return GLOBAL_AIOHTTP_SESSION

def parse_binance_future_depth_update(msg_data: dict) -> Optional[dict]:
    """
    Binance Future depthUpdate 메시지를 공통 포맷으로 변환
    
    Args:
        msg_data: 바이낸스 선물 depthUpdate 메시지
        
    Returns:
        dict: 변환된 메시지 (None: 변환 실패)
    """
    if msg_data.get("e") != "depthUpdate":
        return None
    
    symbol_raw = msg_data.get("s", "")
    symbol = symbol_raw.replace("USDT", "").upper()

    b_data = msg_data.get("b", [])
    a_data = msg_data.get("a", [])
    event_time = msg_data.get("E", 0)
    transaction_time = msg_data.get("T", 0)  # 트랜잭션 시간 추가
    first_id = msg_data.get("U", 0)
    final_id = msg_data.get("u", 0)
    prev_final_id = msg_data.get("pu", 0)  # 이전 final_update_id (바이낸스 선물 전용)

    # 숫자 변환 및 0 이상 필터링
    bids = [[float(b[0]), float(b[1])] for b in b_data if float(b[0]) > 0 and float(b[1]) != 0]
    asks = [[float(a[0]), float(a[1])] for a in a_data if float(a[0]) > 0 and float(a[1]) != 0]

    return {
        "exchangename": "binancefuture",
        "symbol": symbol,
        "bids": bids,
        "asks": asks,
        "timestamp": event_time,
        "transaction_time": transaction_time,  # 트랜잭션 시간 추가
        "first_update_id": first_id,
        "final_update_id": final_id,
        "prev_final_update_id": prev_final_id,  # 이전 final_update_id 추가
        "sequence": final_id,
        "type": "delta"
    }

class BinanceFutureOrderBookV2(OrderBookV2):
    """
    바이낸스 선물 전용 오더북 클래스 (V2)
    - 시퀀스 기반 업데이트 관리
    - 가격 역전 감지 및 처리
    - 트랜잭션 시간 추적
    """
    def __init__(self, exchangename: str, symbol: str, depth: int = 500, inversion_detection: bool = True):
        """초기화"""
        super().__init__(exchangename, symbol, depth)
        self.bids_dict = {}  # 매수 주문 (가격 -> 수량)
        self.asks_dict = {}  # 매도 주문 (가격 -> 수량)
        self.cross_detection_enabled = inversion_detection
        self.last_cpp_send_time = 0  # C++ 전송 시간 추적
        self.last_queue_send_time = 0  # 큐 전송 시간 추적
        self.cpp_send_interval = 0.1  # 0.1초 간격으로 제한 (초당 10회)
        self.queue_send_interval = 0.05  # 0.05초 간격으로 제한 (초당 20회)
        self.last_transaction_time = 0  # 마지막 트랜잭션 시간
        
        if inversion_detection:
            self.enable_cross_detection()
        
    def enable_cross_detection(self):
        """역전 감지 활성화"""
        self.cross_detection_enabled = True
        logger.info(f"{EXCHANGE_KR} {self.symbol} 역전감지 활성화")
        
    def update_orderbook(self, bids: List[List[float]], asks: List[List[float]], 
                        timestamp: Optional[int] = None, sequence: Optional[int] = None,
                        transaction_time: Optional[int] = None, is_snapshot: bool = False) -> None:
        """
        오더북 데이터 업데이트 (바이낸스 선물 전용)
        
        Args:
            bids: 매수 호가 목록 [[가격, 수량], ...]
            asks: 매도 호가 목록 [[가격, 수량], ...]
            timestamp: 타임스탬프 (밀리초)
            sequence: 시퀀스 번호
            transaction_time: 트랜잭션 시간 (밀리초)
            is_snapshot: 스냅샷 여부
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
            if transaction_time:
                self.last_transaction_time = transaction_time
                
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
            
            # 큐로 데이터 전송 (추가)
            asyncio.create_task(self.send_to_queue())
                
        except Exception as e:
            logger.error(
                f"{EXCHANGE_KR} {self.symbol} 오더북 업데이트 실패: {str(e)}", 
                exc_info=True
            )

    async def send_to_cpp(self) -> None:
        """C++로 데이터 직접 전송 (속도 제한 적용)"""
        current_time = time.time()
        if current_time - self.last_cpp_send_time < self.cpp_send_interval:
            return  # 너무 빈번한 전송 방지
            
        # 오더북 데이터를 딕셔너리로 변환
        orderbook_data = self.to_dict()
        
        # 데이터 로깅 (raw 데이터 확인용)
        if self.bids and self.asks:
            best_bid = self.bids[0][0]
            best_ask = self.asks[0][0]
            spread = best_ask - best_bid
            spread_pct = (spread / best_ask) * 100 if best_ask > 0 else 0
            
            # 상위 3개 호가만 로깅 (가독성을 위해)
            top_bids = self.bids[:3]
            top_asks = self.asks[:3]
            
            logger.info(
                f"{EXCHANGE_KR} {self.symbol} C++ 전송 데이터: "
                f"시간={orderbook_data['timestamp']}, "
                f"시퀀스={orderbook_data['sequence']}, "
                f"최우선매수={best_bid}, "
                f"최우선매도={best_ask}, "
                f"스프레드={spread:.8f} ({spread_pct:.4f}%), "
                f"상위매수={top_bids}, "
                f"상위매도={top_asks}"
            )
        
        # C++로 데이터 전송
        try:
            await send_orderbook_to_cpp(self.exchangename, orderbook_data)
            self.last_cpp_send_time = current_time
            logger.info(f"{EXCHANGE_KR} {self.symbol} C++ 전송 성공")
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} {self.symbol} C++ 전송 실패: {str(e)}", exc_info=True)

    async def send_to_queue(self) -> None:
        """큐로 데이터 전송 및 C++로 데이터 전송 (속도 제한 적용)"""
        current_time = time.time()
        if current_time - self.last_queue_send_time < self.queue_send_interval:
            return  # 너무 빈번한 전송 방지
            
        # 오더북 데이터를 딕셔너리로 변환
        orderbook_data = self.to_dict()
        
        # 큐로 데이터 전송
        if self.output_queue:
            try:
                await self.output_queue.put((self.exchangename, orderbook_data))
                self.last_queue_send_time = current_time
                logger.info(f"{EXCHANGE_KR} {self.symbol} 큐 전송 성공 (send_to_queue 메서드)")
            except Exception as e:
                logger.error(f"{EXCHANGE_KR} {self.symbol} 큐 전송 실패: {str(e)}", exc_info=True)
        else:
            logger.warning(f"{EXCHANGE_KR} {self.symbol} 큐 전송 실패: output_queue가 None입니다 (send_to_queue 메서드)")
        
        # C++로 데이터 전송 (비동기 태스크로 실행하여 블로킹하지 않도록 함)
        asyncio.create_task(self.send_to_cpp())

    def to_dict(self) -> dict:
        """오더북 데이터를 딕셔너리로 변환"""
        return {
            "exchangename": self.exchangename,
            "symbol": self.symbol,
            "bids": self.bids[:10],  # 상위 10개만
            "asks": self.asks[:10],  # 상위 10개만
            "timestamp": self.last_update_time or int(time.time() * 1000),
            "sequence": self.last_update_id,
            "transaction_time": self.last_transaction_time  # 트랜잭션 시간 추가
        }

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """
        출력 큐 설정
        
        Args:
            queue: 출력 큐
        """
        self.output_queue = queue
        logger.info(f"{EXCHANGE_KR} {self.symbol} 오더북 출력 큐 설정 완료 (큐 ID: {id(queue)})")

class BinanceFutureOrderBookManagerV2(BaseOrderBookManagerV2):
    """
    바이낸스 선물 오더북 관리자 V2
    - 스냅샷 초기화
    - 델타 업데이트 처리
    - 시퀀스 관리
    - 가격 역전 감지 및 처리
    - 트랜잭션 시간 추적
    """
    def __init__(self, depth: int = 500, cross_detection: bool = True):
        """
        초기화
        
        Args:
            depth: 오더북 깊이
            cross_detection: 가격 역전 감지 활성화 여부
        """
        # 기본 설정
        self.depth = depth
        self.cross_detection_enabled = cross_detection
        self.queue_send_interval = 0.05  # 큐 전송 간격 (초)
        self.cpp_send_interval = 0.1  # C++ 전송 간격 (초)
        
        # 스냅샷 URL
        self.snapshot_url = REST_BASE_URL
        
        # 오더북 객체 저장
        self.orderbooks: Dict[str, BinanceFutureOrderBookV2] = {}
        
        # 이벤트 버퍼
        self.event_buffers: Dict[str, List[Dict[str, Any]]] = {}
        
        # 초기화 상태
        self.initialized: Dict[str, bool] = {}
        
        # 시퀀스 관리 상태
        self.sequence_states: Dict[str, Dict[str, Any]] = {}
        
        # 버퍼 이벤트
        self.buffer_events: Dict[str, List[Dict[str, Any]]] = {}
        
        # 가격 역전 카운트
        self.price_inversion_count: Dict[str, int] = {}
        
        # 로깅 제한을 위한 타임스탬프
        self.last_gap_warning_time: Dict[str, float] = {}
        self.last_queue_log_time: Dict[str, float] = {}
        
        # 초기화 락
        self.initialization_locks: Dict[str, asyncio.Lock] = {}
        
        # 출력 큐
        self.output_queue: Optional[asyncio.Queue] = None
        
        logger.info(f"{EXCHANGE_KR} 오더북 매니저 초기화 완료 (depth={depth}, cross_detection={cross_detection})")
        
    async def initialize(self):
        """초기화 및 메트릭 로깅 시작"""
        await super().initialize()
        logger.info(f"{EXCHANGE_KR} 오더북 매니저 초기화 완료")
        
    async def start(self):
        """오더북 매니저 시작"""
        await self.initialize()  # 메트릭 로깅 시작
        logger.info(f"{EXCHANGE_KR} 오더북 매니저 시작")
        
    async def _get_symbol_lock(self, symbol: str) -> asyncio.Lock:
        """심볼별 초기화 락 관리"""
        if symbol not in self.initialization_locks:
            self.initialization_locks[symbol] = asyncio.Lock()
        return self.initialization_locks[symbol]
        
    async def initialize_orderbook(self, symbol: str, snapshot: dict) -> OrderBookUpdate:
        """
        오더북 초기화 (스냅샷 적용)
        
        Args:
            symbol: 심볼
            snapshot: 스냅샷 데이터
            
        Returns:
            OrderBookUpdate: 업데이트 결과
        """
        try:
            # 심볼 대문자 변환
            symbol = symbol.upper()
            
            # 스냅샷 데이터 검증
            if not snapshot or "bids" not in snapshot or "asks" not in snapshot:
                error_msg = f"스냅샷 데이터 형식 오류: {snapshot}"
                logger.error(f"{EXCHANGE_KR} {symbol} {error_msg}")
                return OrderBookUpdate(is_valid=False, error_messages=[error_msg])
                
            # 스냅샷 데이터에서 필요한 정보 추출
            bids = snapshot.get("bids", [])
            asks = snapshot.get("asks", [])
            timestamp = snapshot.get("timestamp", int(time.time() * 1000))
            last_update_id = snapshot.get("lastUpdateId")
            
            # 오더북 객체가 없으면 생성
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = BinanceFutureOrderBookV2(
                    symbol=symbol,
                    exchangename=EXCHANGE_CODE,
                    depth=self.depth,
                    inversion_detection=self.cross_detection_enabled
                )
                
                # 새로 생성된 오더북에 큐 설정
                if self.output_queue:
                    self.orderbooks[symbol].output_queue = self.output_queue
                    logger.info(f"{EXCHANGE_KR} {symbol} 오더북 객체 생성 및 출력 큐 설정 완료")
            
            # 스냅샷 데이터 형식 변환
            orderbook = self.orderbooks[symbol]
            
            # 오더북 업데이트 (스냅샷 적용)
            orderbook.update_orderbook(
                bids=bids,
                asks=asks,
                timestamp=timestamp,
                sequence=last_update_id,
                is_snapshot=True
            )
            
            # 이벤트 버퍼 초기화
            if symbol not in self.event_buffers:
                self.event_buffers[symbol] = []
                
            # 시퀀스 상태 초기화
            self.sequence_states[symbol] = {
                "last_update_id": last_update_id,
                "initialized": True,
                "first_processed": False
            }
                
            # 초기화 상태 업데이트
            self.initialized[symbol] = True
            
            # 초기화 성공 로그
            logger.info(f"{EXCHANGE_KR} {symbol} 오더북 초기화 성공 (lastUpdateId: {last_update_id})")
            
            # 큐로 초기 데이터 전송
            if self.output_queue:
                try:
                    data = orderbook.to_dict()
                    await self.output_queue.put((EXCHANGE_CODE, data))
                    logger.info(f"{EXCHANGE_KR} {symbol} 초기 오더북 데이터 큐 전송 성공")
                    
                    # 직접 send_to_queue 메서드 호출
                    await orderbook.send_to_queue()
                except Exception as e:
                    logger.error(f"{EXCHANGE_KR} {symbol} 초기 오더북 데이터 큐 전송 실패: {str(e)}")
            
            return OrderBookUpdate(is_valid=True)
            
        except Exception as e:
            error_msg = f"오더북 초기화 실패: {str(e)}"
            logger.error(f"{EXCHANGE_KR} {symbol} {error_msg}", exc_info=True)
            return OrderBookUpdate(is_valid=False, error_messages=[error_msg])

    async def _apply_buffered_events(self, symbol: str) -> None:
        """
        버퍼된 이벤트 적용
        
        Args:
            symbol: 심볼
        """
        if symbol not in self.buffer_events or not self.buffer_events[symbol]:
            return
            
        if symbol not in self.sequence_states or not self.sequence_states[symbol].get("initialized", False):
            return
            
        last_update_id = self.sequence_states[symbol]["last_update_id"]
        first_processed = self.sequence_states[symbol].get("first_processed", False)
        
        # 버퍼된 이벤트 정렬 (first_update_id 기준)
        sorted_events = sorted(
            self.buffer_events[symbol],
            key=lambda x: x.get("first_update_id", 0)
        )
        
        # 적용된 이벤트 수
        applied_count = 0
        
        # 적용할 이벤트 목록
        events_to_apply = []
        
        for event in sorted_events:
            first_update_id = event.get("first_update_id", 0)
            final_update_id = event.get("final_update_id", 0)
            prev_final_update_id = event.get("prev_final_update_id", 0)
            
            # 첫 번째 이벤트 처리 (바이낸스 선물 규칙)
            if not first_processed:
                # 첫 번째 이벤트는 U <= lastUpdateId+1 AND u >= lastUpdateId+1 조건을 만족해야 함
                if first_update_id <= last_update_id + 1 and final_update_id >= last_update_id + 1:
                    events_to_apply.append(event)
                    self.sequence_states[symbol]["first_processed"] = True
                    first_processed = True
                    applied_count += 1
                    continue
                else:
                    # 조건을 만족하지 않으면 스킵
                    continue
            
            # 이후 이벤트 처리 (바이낸스 선물 규칙)
            # 이전 이벤트의 final_update_id와 현재 이벤트의 prev_final_update_id가 일치해야 함
            if prev_final_update_id == last_update_id:
                events_to_apply.append(event)
                last_update_id = final_update_id
                applied_count += 1
            else:
                # 시퀀스 불일치 감지
                gap = prev_final_update_id - last_update_id if prev_final_update_id > last_update_id else last_update_id - prev_final_update_id
                
                # 갭이 1인 경우는 허용 (바이낸스 선물 특성)
                if gap == 1:
                    events_to_apply.append(event)
                    last_update_id = final_update_id
                    applied_count += 1
                    continue
                
                # 갭이 1보다 큰 경우 경고 로깅 (너무 빈번한 로깅 방지)
                current_time = time.time()
                if symbol not in self.last_gap_warning_time or current_time - self.last_gap_warning_time[symbol] > 5:
                    logger.warning(
                        f"{EXCHANGE_KR} {symbol} 시퀀스 갭 감지: "
                        f"last_update_id={last_update_id}, "
                        f"prev_final_update_id={prev_final_update_id}, "
                        f"gap={gap}"
                    )
                    self.last_gap_warning_time[symbol] = current_time
                
                # 시퀀스 불일치 시 스냅샷 다시 요청 필요
                self.sequence_states[symbol]["initialized"] = False
                self.sequence_states[symbol]["first_processed"] = False
                
                # 메트릭 기록
                self.record_metric("error", error_type="sequence_gap", symbol=symbol, gap=gap)
                
                # 버퍼 비우기
                self.buffer_events[symbol] = []
                
                # 스냅샷 다시 요청 (비동기로 실행)
                asyncio.create_task(self._reinitialize_orderbook(symbol))
                
                return
        
        # 적용할 이벤트가 있는 경우
        if events_to_apply:
            # 이벤트 적용
            for event in events_to_apply:
                await self._apply_event(symbol, event)
            
            # 시퀀스 상태 업데이트
            self.sequence_states[symbol]["last_update_id"] = last_update_id
            
            # 적용된 이벤트 제거
            self.buffer_events[symbol] = [
                event for event in self.buffer_events[symbol]
                if event.get("final_update_id", 0) > last_update_id
            ]
            
            # 버퍼 크기 로깅 (너무 빈번한 로깅 방지)
            current_time = time.time()
            if symbol not in self.last_queue_log_time or current_time - self.last_queue_log_time[symbol] > 5:
                buffer_size = len(self.buffer_events[symbol])
                if buffer_size > 100:  # 버퍼 크기가 100 이상인 경우만 로깅
                    logger.warning(f"{EXCHANGE_KR} {symbol} 버퍼 크기: {buffer_size}")
                self.last_queue_log_time[symbol] = current_time
            
            # 메트릭 기록
            self.record_metric("orderbook", symbol=symbol, operation="buffer_apply", count=applied_count)
    
    async def _apply_event(self, symbol: str, event: dict) -> None:
        """
        단일 이벤트 적용
        
        Args:
            symbol: 심볼
            event: 이벤트 데이터
        """
        if symbol not in self.orderbooks:
            return
            
        # 이벤트 데이터 추출
        bids = event.get("bids", [])
        asks = event.get("asks", [])
        timestamp = event.get("timestamp", int(time.time() * 1000))
        final_update_id = event.get("final_update_id", 0)
        transaction_time = event.get("transaction_time", 0)
        
        # 오더북 업데이트
        self.orderbooks[symbol].update_orderbook(
            bids=bids,
            asks=asks,
            timestamp=timestamp,
            sequence=final_update_id,
            transaction_time=transaction_time,
            is_snapshot=False
        )
        
        # 메트릭 기록
        self.record_metric("orderbook", symbol=symbol, operation="update")
    
    async def _reinitialize_orderbook(self, symbol: str) -> None:
        """
        오더북 재초기화 (스냅샷 다시 요청)
        
        Args:
            symbol: 심볼
        """
        try:
            # 스냅샷 요청
            snapshot = await self.fetch_snapshot(symbol)
            if not snapshot:
                logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 재요청 실패")
                return
                
            # 오더북 초기화
            result = await self.initialize_orderbook(symbol, snapshot)
            if not result.is_valid:
                logger.error(f"{EXCHANGE_KR} {symbol} 오더북 재초기화 실패: {result.error_messages}")
                return
                
            logger.info(f"{EXCHANGE_KR} {symbol} 오더북 재초기화 완료")
            
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} {symbol} 오더북 재초기화 중 오류 발생: {str(e)}", exc_info=True)
    
    async def update(self, symbol: str, data: dict) -> OrderBookUpdate:
        """
        오더북 업데이트
        
        Args:
            symbol: 심볼
            data: 업데이트 데이터
            
        Returns:
            OrderBookUpdate: 업데이트 결과
        """
        try:
            # 심볼 대문자 변환
            symbol = symbol.upper()
            
            # 오더북 객체가 없으면 초기화되지 않은 것으로 간주
            if symbol not in self.orderbooks:
                error_msg = f"오더북이 초기화되지 않았습니다"
                logger.error(f"{EXCHANGE_KR} {symbol} {error_msg}")
                return OrderBookUpdate(is_valid=False, error_messages=[error_msg])
                
            # 초기화 상태 확인
            if not self.is_initialized(symbol):
                # 초기화되지 않은 경우 이벤트 버퍼에 추가
                if symbol not in self.event_buffers:
                    self.event_buffers[symbol] = []
                    
                # 업데이트 파싱
                update = parse_binance_future_depth_update(data)
                if not update:
                    error_msg = f"업데이트 파싱 실패"
                    logger.error(f"{EXCHANGE_KR} {symbol} {error_msg}")
                    return OrderBookUpdate(is_valid=False, error_messages=[error_msg])
                    
                # 버퍼 크기 제한
                if len(self.event_buffers[symbol]) >= MAX_BUFFER_SIZE:
                    logger.warning(f"{EXCHANGE_KR} {symbol} 버퍼 크기 초과 ({len(self.event_buffers[symbol])})")
                    # 가장 오래된 이벤트 제거
                    self.event_buffers[symbol].pop(0)
                    
                # 이벤트 버퍼에 추가 (딕셔너리 형태로)
                self.event_buffers[symbol].append({
                    "first_update_id": update.get("first_update_id", 0),
                    "final_update_id": update.get("sequence", 0),
                    "prev_final_update_id": update.get("prev_final_update_id", 0),
                    "bids": update.get("bids", []),
                    "asks": update.get("asks", []),
                    "timestamp": update.get("timestamp", 0),
                    "transaction_time": update.get("transaction_time", 0)
                })
                
                # 버퍼 크기 로깅
                buffer_size = len(self.event_buffers[symbol])
                if buffer_size % 10 == 0:  # 10개 단위로 로깅
                    logger.info(f"{EXCHANGE_KR} {symbol} 버퍼 크기: {buffer_size}")
                    
                return OrderBookUpdate(is_valid=True, buffered=True)
                
            # 업데이트 파싱
            update = parse_binance_future_depth_update(data)
            if not update:
                error_msg = f"업데이트 파싱 실패"
                logger.error(f"{EXCHANGE_KR} {symbol} {error_msg}")
                return OrderBookUpdate(is_valid=False, error_messages=[error_msg])
                
            # 오더북 업데이트
            orderbook = self.orderbooks[symbol]
            
            # 업데이트 적용
            orderbook.update_orderbook(
                bids=update["bids"],
                asks=update["asks"],
                timestamp=update["timestamp"],
                sequence=update["sequence"],
                transaction_time=update["transaction_time"]
            )
            
            # 버퍼된 이벤트 적용 시도
            await self._apply_buffered_events(symbol)
            
            # 큐로 데이터 전송
            if self.output_queue:
                try:
                    # 오더북 객체에 큐가 설정되어 있는지 확인
                    if orderbook.output_queue is None:
                        orderbook.output_queue = self.output_queue
                        logger.info(f"{EXCHANGE_KR} {symbol} 오더북 객체에 출력 큐 설정 (update 메서드 내)")
                    
                    # send_to_queue 메서드 호출
                    await orderbook.send_to_queue()
                    logger.info(f"{EXCHANGE_KR} {symbol} send_to_queue 메서드 호출 성공 (update 메서드 내)")
                except Exception as e:
                    logger.error(f"{EXCHANGE_KR} {symbol} 큐 전송 실패: {str(e)}")
            
            return OrderBookUpdate(is_valid=True)
            
        except Exception as e:
            error_msg = f"오더북 업데이트 실패: {str(e)}"
            logger.error(f"{EXCHANGE_KR} {symbol} {error_msg}", exc_info=True)
            return OrderBookUpdate(is_valid=False, error_messages=[error_msg])
    
    async def fetch_snapshot(self, symbol: str) -> Optional[dict]:
        """
        REST API로 스냅샷 요청
        
        Args:
            symbol: 심볼
            
        Returns:
            dict: 스냅샷 데이터 (None: 요청 실패)
        """
        # 세마포어로 동시 요청 제한
        async with SNAPSHOT_SEMAPHORE:
            try:
                # 심볼 형식 처리
                symbol_upper = symbol.upper()
                if not symbol_upper.endswith("USDT"):
                    symbol_upper += "USDT"
                
                # URL 생성
                url = f"{self.snapshot_url}?symbol={symbol_upper}&limit={self.depth}"
                
                # 세션 가져오기
                session = await get_global_aiohttp_session()
                
                # 스냅샷 요청
                async with session.get(url, timeout=SNAPSHOT_REQUEST_TIMEOUT) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return self.parse_snapshot(data, symbol)
                    else:
                        error_text = await resp.text()
                        logger.error(
                            f"{EXCHANGE_KR} {symbol} 스냅샷 요청 실패: "
                            f"status={resp.status}, error={error_text}"
                        )
                        return None
                        
            except asyncio.TimeoutError:
                logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 요청 타임아웃")
                return None
                
            except Exception as e:
                logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 요청 중 오류 발생: {str(e)}", exc_info=True)
                return None
    
    def parse_snapshot(self, data: dict, symbol: str) -> Optional[dict]:
        """
        스냅샷 데이터 파싱
        
        Args:
            data: 스냅샷 데이터
            symbol: 심볼
            
        Returns:
            dict: 파싱된 스냅샷 데이터 (None: 파싱 실패)
        """
        try:
            if "lastUpdateId" not in data:
                logger.error(f"{EXCHANGE_KR} {symbol} 'lastUpdateId' 없음")
                return None

            last_id = data["lastUpdateId"]
            
            # 숫자 변환 및 0 이상 필터링
            bids = [[float(b[0]), float(b[1])] for b in data.get("bids", []) if float(b[0]) > 0 and float(b[1]) > 0]
            asks = [[float(a[0]), float(a[1])] for a in data.get("asks", []) if float(a[0]) > 0 and float(a[1]) > 0]

            snapshot = {
                "exchangename": "binancefuture",
                "symbol": symbol,
                "bids": bids,
                "asks": asks,
                "timestamp": int(time.time() * 1000),
                "lastUpdateId": last_id,
                "type": "snapshot"
            }
            
            return snapshot
            
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 파싱 실패: {str(e)}", exc_info=True)
            return None
    
    def is_initialized(self, symbol: str) -> bool:
        """
        심볼의 오더북 초기화 여부 확인
        
        Args:
            symbol: 심볼
            
        Returns:
            bool: 초기화 여부
        """
        return symbol in self.initialized and self.initialized[symbol]
    
    def get_orderbook(self, symbol: str) -> Optional[BinanceFutureOrderBookV2]:
        """
        심볼의 오더북 객체 반환
        
        Args:
            symbol: 심볼
            
        Returns:
            BinanceFutureOrderBookV2: 오더북 객체 (None: 없음)
        """
        return self.orderbooks.get(symbol)
    
    def clear_symbol(self, symbol: str) -> None:
        """
        심볼 데이터 제거
        
        Args:
            symbol: 심볼
        """
        self.orderbooks.pop(symbol, None)
        self.sequence_states.pop(symbol, None)
        self.buffer_events.pop(symbol, None)
        self.price_inversion_count.pop(symbol, None)
        self.last_gap_warning_time.pop(symbol, None)
        self.last_queue_log_time.pop(symbol, None)
        self.initialization_locks.pop(symbol, None)
        
        logger.info(f"{EXCHANGE_KR} {symbol} 데이터 제거 완료")
    
    def clear_all(self) -> None:
        """전체 데이터 제거"""
        self.orderbooks.clear()
        self.sequence_states.clear()
        self.buffer_events.clear()
        self.price_inversion_count.clear()
        self.last_gap_warning_time.clear()
        self.last_queue_log_time.clear()
        self.initialization_locks.clear()
        
        logger.info(f"{EXCHANGE_KR} 전체 데이터 제거 완료")
    
    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """
        출력 큐 설정
        
        Args:
            queue: 출력 큐
        """
        self.output_queue = queue
        logger.info(f"{EXCHANGE_KR} 오더북 매니저 출력 큐 설정 완료 (큐 ID: {id(queue)})")
        
        # 각 오더북 객체에도 큐 설정
        for symbol, orderbook in self.orderbooks.items():
            orderbook.output_queue = queue
            logger.info(f"{EXCHANGE_KR} {symbol} 오더북 출력 큐 설정 완료 (큐 ID: {id(queue)})")
