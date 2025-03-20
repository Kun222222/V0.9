import asyncio
import time
from typing import Dict, Optional, List, Any, Tuple
from dataclasses import dataclass
import aiohttp
import json

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import Exchange, EXCHANGE_NAMES_KR, WEBSOCKET_CONFIG

from crosskimp.ob_collector.orderbook.orderbook.base_ob import BaseOrderBookManagerV2, OrderBookV2, ValidationResult
from crosskimp.ob_collector.cpp.cpp_interface import send_orderbook_to_cpp
from crosskimp.ob_collector.orderbook.parser.binance_f_pa import BinanceFutureParser

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이낸스 선물 오더북 관련 상수
# ============================
EXCHANGE_CODE = Exchange.BINANCE_FUTURE.value  # 거래소 코드
EXCHANGE_KR = EXCHANGE_NAMES_KR[EXCHANGE_CODE]  # 거래소 한글 이름
BINANCE_FUTURE_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이낸스 선물 설정

# REST API 설정 (스냅샷 요청용)
REST_BASE_URL = BINANCE_FUTURE_CONFIG["api_urls"]["depth"]  # REST API 기본 URL

# 웹소켓 연결 설정 - 2024년 4월 이후 변경된 URL
WS_BASE_URL = BINANCE_FUTURE_CONFIG["ws_base_url"]  # 웹소켓 기본 URL

# 오더북 관련 설정
UPDATE_SPEED = BINANCE_FUTURE_CONFIG["update_speed"]  # 웹소켓 업데이트 속도 (100ms, 250ms, 500ms 중 선택)
DEFAULT_DEPTH = BINANCE_FUTURE_CONFIG["default_depth"]     # 기본 오더북 깊이
MAX_RETRY_COUNT = BINANCE_FUTURE_CONFIG["max_retry_count"]     # 최대 재시도 횟수
MAX_BUFFER_SIZE = 1000  # 최대 버퍼 크기

# 스냅샷 요청 설정
SNAPSHOT_RETRY_DELAY = BINANCE_FUTURE_CONFIG["snapshot_retry_delay"]  # 스냅샷 요청 재시도 초기 딜레이 (초)
SNAPSHOT_REQUEST_TIMEOUT = BINANCE_FUTURE_CONFIG["snapshot_request_timeout"]  # 스냅샷 요청 타임아웃 (초)

# DNS 캐시 설정
DNS_CACHE_TTL = BINANCE_FUTURE_CONFIG["dns_cache_ttl"]  # DNS 캐시 TTL (초)

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

class BinanceFutureOrderBook(OrderBookV2):
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
                
            # 스냅샷인 경우에만 C++와 큐로 데이터 전송
            # 델타 업데이트는 update 메서드에서 처리
            if is_snapshot:
                # C++로 데이터 직접 전송
                asyncio.create_task(self.send_to_cpp())
                
                # 큐로 데이터 전송 - 중복 로깅 방지를 위해 주석 처리
                # asyncio.create_task(self.send_to_queue())
                
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
        """큐로 데이터 전송 (속도 제한 적용)"""
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
        
        # C++로 데이터 전송 호출 제거 (중복 전송 방지)
        # asyncio.create_task(self.send_to_cpp())

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

    def update(self, data: dict) -> ValidationResult:
        """
        오더북 업데이트
        
        Args:
            data: 업데이트 데이터
            
        Returns:
            ValidationResult: 업데이트 결과
        """
        try:
            # 필수 필드 확인
            if "bids" not in data or "asks" not in data:
                return ValidationResult(False, ["필수 필드 누락: bids 또는 asks"])
                
            # 데이터 추출
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            timestamp = data.get("timestamp", int(time.time() * 1000))
            sequence = data.get("sequence", 0)
            transaction_time = data.get("transaction_time", 0)
            
            # 오더북 업데이트
            self.update_orderbook(
                bids=bids,
                asks=asks,
                timestamp=timestamp,
                sequence=sequence,
                transaction_time=transaction_time,
                is_snapshot=False
            )
            
            # 델타 업데이트 후 데이터 전송
            # C++로 데이터 직접 전송
            asyncio.create_task(self.send_to_cpp())
            
            # 큐로 데이터 전송
            asyncio.create_task(self.send_to_queue())
            
            return ValidationResult(True)
        except Exception as e:
            error_msg = f"오더북 업데이트 실패: {e}"
            logger.error(f"{EXCHANGE_KR} {self.symbol} {error_msg}", exc_info=True)
            return ValidationResult(False, [error_msg])

    def apply_snapshot(self, snapshot: dict) -> bool:
        """
        스냅샷 적용
        
        Args:
            snapshot: 스냅샷 데이터
            
        Returns:
            bool: 스냅샷 적용 성공 여부
        """
        try:
            # 필수 필드 확인
            if "bids" not in snapshot or "asks" not in snapshot:
                logger.error(f"{EXCHANGE_KR} {self.symbol} 스냅샷 데이터 형식 오류: 필수 필드 누락")
                return False
                
            # 데이터 추출
            bids = snapshot.get("bids", [])
            asks = snapshot.get("asks", [])
            timestamp = snapshot.get("timestamp", int(time.time() * 1000))
            last_update_id = snapshot.get("lastUpdateId", 0)
            
            # 빈 데이터 확인 (완전히 비어있는 경우만 오류 처리)
            if bids is None or asks is None:
                logger.error(f"{EXCHANGE_KR} {self.symbol} 스냅샷 데이터 오류: bids 또는 asks가 None")
                return False
            
            # 오더북 업데이트 (수량이 0인 경우도 포함하여 처리)
            self.update_orderbook(
                bids=bids,
                asks=asks,
                timestamp=timestamp,
                sequence=last_update_id,
                is_snapshot=True
            )
            
            logger.info(f"{EXCHANGE_KR} {self.symbol} 스냅샷 적용 완료 (lastUpdateId: {last_update_id}, bids: {len(bids)}, asks: {len(asks)})")
            return True
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} {self.symbol} 스냅샷 적용 실패: {e}", exc_info=True)
            return False

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
        
        # 파서 초기화
        self.parser = BinanceFutureParser()
        
        # 스냅샷 URL 설정
        self.snapshot_url = self.parser.snapshot_url
        
        # 오더북 객체 저장
        self.orderbooks: Dict[str, BinanceFutureOrderBook] = {}
        
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
                self.orderbooks[symbol] = BinanceFutureOrderBook(
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
            
            # 큐로 초기 데이터 전송 (한 번만 전송) - 중복 로깅 방지를 위해 주석 처리
            if self.output_queue:
                try:
                    # data = orderbook.to_dict()
                    # await self.output_queue.put((EXCHANGE_CODE, data))
                    # logger.info(f"{EXCHANGE_KR} {symbol} 초기 오더북 데이터 큐 전송 성공")
                    
                    # 직접 send_to_queue 메서드 호출은 제거 (중복 전송 방지)
                    # await orderbook.send_to_queue()
                    
                    # C++로 데이터 전송 (직접 호출)
                    await orderbook.send_to_cpp()
                except Exception as e:
                    logger.error(f"{EXCHANGE_KR} {symbol} C++ 데이터 전송 실패: {str(e)}")
            
            return OrderBookUpdate(is_valid=True)
            
        except Exception as e:
            error_msg = f"오더북 초기화 실패: {str(e)}"
            logger.error(f"{EXCHANGE_KR} {symbol} {error_msg}", exc_info=True)
            return OrderBookUpdate(is_valid=False, error_messages=[error_msg])

    async def apply_buffered_events(self, symbol: str) -> None:
        """
        버퍼된 이벤트 적용
        
        Args:
            symbol: 심볼 (예: BTC)
        """
        try:
            # 버퍼 확인
            if symbol not in self.event_buffers or not self.event_buffers[symbol]:
                self.log_info(f"{symbol} 버퍼된 이벤트 없음")
                return
                
            # 오더북 객체 확인
            orderbook = self.get_orderbook(symbol)
            if not orderbook:
                self.log_error(f"{symbol} 오더북 객체 없음")
                return
                
            # 버퍼된 이벤트 수
            buffer_size = len(self.event_buffers[symbol])
            self.log_info(f"{symbol} 버퍼된 이벤트 적용 시작 (개수: {buffer_size})")
            
            # 버퍼된 이벤트 적용
            applied_count = 0
            error_count = 0
            
            # 버퍼 복사 (이벤트 적용 중 버퍼가 변경될 수 있음)
            buffered_events = self.event_buffers[symbol].copy()
            self.event_buffers[symbol].clear()
            
            # 이벤트 적용
            for event in buffered_events:
                try:
                    # 이벤트 적용
                    result = orderbook.update(event)
                    
                    # 결과 처리
                    if result.is_valid:
                        applied_count += 1
                    else:
                        error_count += 1
                        self.log_error(f"{symbol} 버퍼된 이벤트 적용 실패: {result.error_messages}")
                except Exception as e:
                    error_count += 1
                    self.log_error(f"{symbol} 버퍼된 이벤트 적용 중 예외 발생: {e}")
            
            # 결과 로깅
            self.log_info(f"{symbol} 버퍼된 이벤트 적용 완료 (성공: {applied_count}, 실패: {error_count})")
        except Exception as e:
            self.log_error(f"{symbol} 버퍼된 이벤트 적용 중 예외 발생: {e}")
            # 버퍼 초기화 (오류 발생 시 버퍼 초기화)
            if symbol in self.event_buffers:
                self.event_buffers[symbol].clear()

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
    
    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """
        오더북 업데이트
        
        Args:
            symbol: 심볼
            data: 업데이트 데이터
            
        Returns:
            ValidationResult: 업데이트 결과
        """
        try:
            # 심볼 초기화 확인
            if not self.is_initialized(symbol):
                # 스냅샷 요청 여부 확인
                if not self.snapshot_requested.get(symbol, False):
                    # 스냅샷 요청
                    self.snapshot_requested[symbol] = True
                    self.snapshot_request_time[symbol] = time.time()
                    asyncio.create_task(self.request_snapshot(symbol))
                    self.log_info(f"{symbol} 스냅샷 요청 시작")
                
                # 스냅샷 타임아웃 확인
                if symbol in self.snapshot_request_time:
                    elapsed = time.time() - self.snapshot_request_time[symbol]
                    if elapsed > self.snapshot_timeout:
                        # 타임아웃 발생 - 스냅샷 재요청
                        self.log_warning(f"{symbol} 스냅샷 타임아웃 ({elapsed:.1f}초) - 재요청")
                        self.snapshot_requested[symbol] = True
                        self.snapshot_request_time[symbol] = time.time()
                        asyncio.create_task(self.request_snapshot(symbol))
                
                # 이벤트 버퍼링
                if symbol not in self.event_buffers:
                    self.event_buffers[symbol] = []
                
                # 버퍼 크기 제한
                if len(self.event_buffers[symbol]) >= self.max_buffer_size:
                    # 버퍼가 가득 찬 경우 가장 오래된 이벤트 제거
                    self.event_buffers[symbol].pop(0)
                    self.log_warning(f"{symbol} 이벤트 버퍼 가득 참 - 이벤트 제거")
                
                # 이벤트 버퍼에 추가 (이미 파싱된 데이터)
                self.event_buffers[symbol].append(data)
                self.log_debug(f"{symbol} 이벤트 버퍼링 (크기: {len(self.event_buffers[symbol])})")
                
                # 초기화 대기 중
                return ValidationResult(False, ["초기화 대기 중"])
            
            # 오더북 객체 가져오기
            orderbook = self.get_orderbook(symbol)
            if not orderbook:
                error_msg = f"오더북 객체 없음"
                self.log_error(f"{symbol} {error_msg}")
                return ValidationResult(False, [error_msg])
            
            # 오더북 업데이트 (이미 파싱된 데이터 사용)
            result = orderbook.update(data)
            
            # 업데이트 결과 로깅
            if not result.is_valid:
                self.log_error(f"{symbol} 오더북 업데이트 실패: {result.error_messages}")
            
            return result
        except Exception as e:
            error_msg = f"업데이트 중 예외 발생: {e}"
            self.log_error(f"{symbol} {error_msg}")
            return ValidationResult(False, [error_msg])
    
    async def fetch_snapshot(self, symbol: str) -> Optional[dict]:
        """
        스냅샷 데이터 가져오기
        
        Args:
            symbol: 심볼 (예: BTC)
            
        Returns:
            Optional[dict]: 스냅샷 데이터 (None: 실패)
        """
        try:
            # 심볼 형식 변환 (BTC -> BTCUSDT)
            formatted_symbol = f"{symbol.upper()}USDT"
            
            # 스냅샷 URL 생성
            url = f"{self.snapshot_url}?symbol={formatted_symbol}&limit=1000"
            self.log_info(f"{symbol} 스냅샷 요청 URL: {url}")
            
            # HTTP 세션 가져오기
            session = get_global_aiohttp_session()
            
            # 스냅샷 요청
            async with session.get(url) as response:
                if response.status != 200:
                    error_text = await response.text()
                    self.log_error(f"{symbol} 스냅샷 요청 실패 (상태 코드: {response.status}): {error_text}")
                    return None
                    
                # 응답 데이터 파싱
                data = await response.json()
                self.log_info(f"{symbol} 스냅샷 데이터 수신 성공")
                return data
        except Exception as e:
            self.log_error(f"{symbol} 스냅샷 요청 중 예외 발생: {e}")
            return None
    
    def parse_snapshot(self, data: dict, symbol: str) -> Optional[dict]:
        """
        스냅샷 데이터 파싱
        
        Args:
            data: 스냅샷 데이터
            symbol: 심볼
            
        Returns:
            Optional[dict]: 파싱된 스냅샷 데이터 또는 None (파싱 실패 시)
        """
        try:
            # 파서를 사용하여 스냅샷 데이터 파싱
            snapshot = self.parser.parse_snapshot_data(data, symbol)
            
            if not snapshot:
                logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 파싱 실패")
                return None
                
            return snapshot
            
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 파싱 실패: {e}")
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
    
    def get_orderbook(self, symbol: str) -> Optional[BinanceFutureOrderBook]:
        """
        심볼의 오더북 객체 반환
        
        Args:
            symbol: 심볼
            
        Returns:
            BinanceFutureOrderBook: 오더북 객체 (None: 없음)
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

    async def request_snapshot(self, symbol: str) -> bool:
        """
        스냅샷 요청 및 처리
        
        Args:
            symbol: 심볼 (예: BTC)
            
        Returns:
            bool: 스냅샷 처리 성공 여부
        """
        try:
            # 스냅샷 요청
            snapshot_data = await self.fetch_snapshot(symbol)
            if not snapshot_data:
                self.log_error(f"{symbol} 스냅샷 데이터 없음")
                return False
                
            # 스냅샷 파싱
            parsed_snapshot = self.parser.parse_snapshot_data(snapshot_data, symbol)
            if not parsed_snapshot:
                self.log_error(f"{symbol} 스냅샷 파싱 실패")
                return False
                
            # 오더북 객체 가져오기 또는 생성
            orderbook = self.get_orderbook(symbol)
            if not orderbook:
                # 오더북 객체 생성
                orderbook = self.create_orderbook(symbol)
                self.orderbooks[symbol] = orderbook
                self.log_info(f"{symbol} 오더북 객체 생성")
                
            # 스냅샷 적용
            if not orderbook.apply_snapshot(parsed_snapshot):
                self.log_error(f"{symbol} 스냅샷 적용 실패")
                return False
                
            # 버퍼된 이벤트 적용
            await self.apply_buffered_events(symbol)
            
            # 초기화 완료
            self.initialized[symbol] = True
            self.log_info(f"{symbol} 오더북 초기화 완료")
            
            return True
        except Exception as e:
            self.log_error(f"{symbol} 스냅샷 요청 및 처리 실패: {e}")
            return False

    # 로깅 메서드 추가
    def log_info(self, message: str) -> None:
        """
        정보 로깅
        
        Args:
            message: 로그 메시지
        """
        logger.info(f"{EXCHANGE_KR} {message}")
        
    def log_error(self, message: str) -> None:
        """
        오류 로깅
        
        Args:
            message: 로그 메시지
        """
        logger.error(f"{EXCHANGE_KR} {message}")
        
    def log_warning(self, message: str) -> None:
        """
        경고 로깅
        
        Args:
            message: 로그 메시지
        """
        logger.warning(f"{EXCHANGE_KR} {message}")
        
    def log_debug(self, message: str) -> None:
        """
        디버그 로깅
        
        Args:
            message: 로그 메시지
        """
        logger.debug(f"{EXCHANGE_KR} {message}")

    def create_orderbook(self, symbol: str) -> BinanceFutureOrderBook:
        """
        오더북 객체 생성
        
        Args:
            symbol: 심볼 (예: BTC)
            
        Returns:
            BinanceFutureOrderBook: 오더북 객체
        """
        orderbook = BinanceFutureOrderBook(
            exchangename=EXCHANGE_CODE,
            symbol=symbol,
            depth=self.depth,
            inversion_detection=self.cross_detection_enabled
        )
        
        # 출력 큐 설정
        if self.output_queue:
            orderbook.output_queue = self.output_queue
            self.log_info(f"{symbol} 오더북 객체 생성 및 출력 큐 설정 완료")
            
        return orderbook
