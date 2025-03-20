# file: core/websocket/exchanges/bithumb_spot_orderbook_manager.py

import asyncio
import time
import aiohttp
from typing import Dict, List, Set, Optional

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import Exchange, EXCHANGE_NAMES_KR, WEBSOCKET_CONFIG

from crosskimp.ob_collector.orderbook.orderbook.base_ob import BaseOrderBookManagerV2, OrderBookV2, ValidationResult
from crosskimp.ob_collector.cpp.cpp_interface import send_orderbook_to_cpp
from crosskimp.ob_collector.orderbook.parser.bithumb_s_pa import BithumbParser

# ============================
# 빗썸 현물 오더북 관련 상수
# ============================
EXCHANGE_CODE = Exchange.BITHUMB.value  # 거래소 코드
EXCHANGE_KR = EXCHANGE_NAMES_KR[EXCHANGE_CODE]  # 거래소 한글 이름
BITHUMB_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 빗썸 설정

# 로거 인스턴스 가져오기 - 전역 로거 한 번만 초기화
logger = get_unified_logger()

class BithumbSpotOrderBook(OrderBookV2):
    """
    빗썸 현물 오더북 클래스 (V2)
    - 타임스탬프 기반 시퀀스 관리
    - 가격 역전 감지 및 처리
    """
    def __init__(self, exchangename: str, symbol: str, depth: int = BITHUMB_CONFIG["default_depth"]):
        """초기화"""
        super().__init__(exchangename, symbol, depth)
        self.bids_dict = {}  # 매수 주문 (가격 -> 수량)
        self.asks_dict = {}  # 매도 주문 (가격 -> 수량)
        self.cross_detection_enabled = False
        self.last_timestamp = 0
        
    def enable_cross_detection(self):
        """역전 감지 활성화"""
        self.cross_detection_enabled = True
        logger.info(f"{EXCHANGE_KR} {self.symbol} 역전감지 활성화")
        
    def should_process_update(self, timestamp: Optional[int]) -> bool:
        """
        업데이트 처리 여부 결정
        - 타임스탬프가 이전보다 작거나 같으면 무시
        """
        if self.last_timestamp and timestamp:
            if timestamp <= self.last_timestamp:
                logger.debug(
                    f"{EXCHANGE_KR} {self.symbol} 이전 타임스탬프 무시 | "
                    f"current={timestamp}, last={self.last_timestamp}"
                )
                return False
        return True
    
    def update_orderbook(self, bids: List[List[float]], asks: List[List[float]], 
                        timestamp: Optional[int] = None, sequence: Optional[int] = None,
                        is_snapshot: bool = False) -> None:
        """
        오더북 데이터 업데이트 (빗썸 전용)
        """
        try:
            # 타임스탬프 검증
            if timestamp and not self.should_process_update(timestamp) and not is_snapshot:
                return
                
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
                self.last_timestamp = timestamp
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

class BithumbSpotOrderBookManager(BaseOrderBookManagerV2):
    """
    빗썸 현물 오더북 매니저 (V2)
    - 웹소켓으로부터 스냅샷/델타 데이터 수신
    - 타임스탬프 기반 시퀀스 관리
    - REST API 스냅샷 요청 및 적용
    """
    def __init__(self, depth: int = BITHUMB_CONFIG["default_depth"]):
        """초기화"""
        super().__init__(depth)
        self.exchangename = EXCHANGE_CODE
        self.orderbooks: Dict[str, BithumbSpotOrderBook] = {}  # BithumbSpotOrderBook 사용
        self.subscribed_symbols = set()  # 구독 중인 심볼
        self.ws = None  # 웹소켓 연결
        self.last_queue_log_time = {}  # 심볼 -> 마지막 큐 로깅 시간
        self.queue_log_interval = 5.0  # 5초마다 로깅
        self.parser = BithumbParser()  # 파서 초기화

    async def start(self):
        """오더북 매니저 시작"""
        await self.initialize()  # 메트릭 로깅 시작

    async def fetch_snapshot(self, symbol: str) -> Optional[dict]:
        """빗썸 오더북 스냅샷 요청"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                logger.debug(f"{EXCHANGE_KR} {symbol} 스냅샷 요청 시도 ({retry_count+1}/{max_retries})")
                snapshot = await self._fetch_snapshot_impl(symbol)
                if snapshot and "data" in snapshot:
                    logger.debug(f"{EXCHANGE_KR} {symbol} 스냅샷 요청 성공")
                    return snapshot["data"]
                else:
                    logger.warning(f"{EXCHANGE_KR} {symbol} 스냅샷 응답 형식 오류")
            except Exception as e:
                logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 요청 오류: {e}", exc_info=True)
            
            retry_count += 1
            await asyncio.sleep(1)  # 재시도 전 1초 대기
        
        logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 요청 최대 재시도 횟수 초과")
        return None

    async def _fetch_snapshot_impl(self, symbol: str) -> dict:
        """빗썸 오더북 스냅샷 요청 구현"""
        url = f"https://api.bithumb.com/public/orderbook/{symbol}_KRW"
        params = {"count": self.depth}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("status") == "0000":
                        # API 응답을 그대로 반환 (data 키 아래에 실제 데이터가 있음)
                        return {"data": data.get("data", {})}
                    else:
                        logger.error(f"{EXCHANGE_KR} {symbol} API 오류: {data.get('message')}")
                else:
                    logger.error(f"{EXCHANGE_KR} {symbol} HTTP 오류: {response.status}")
        
        return None

    def parse_snapshot(self, data: dict, symbol: str) -> dict:
        """스냅샷 데이터 파싱"""
        try:
            # 파서를 사용하여 스냅샷 데이터 파싱
            parsed = self.parser.parse_snapshot_data(data, symbol)
            
            # 디버깅을 위한 로깅 추가
            if parsed:
                logger.debug(f"{EXCHANGE_KR} {symbol} 스냅샷 파싱 완료: 매수 {len(parsed.get('bids', []))}개, 매도 {len(parsed.get('asks', []))}개")
            
            return parsed
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} 스냅샷 파싱 오류: {e}", exc_info=True)
            return {}

    async def initialize_orderbook(self, symbol: str, snapshot: Optional[dict] = None) -> ValidationResult:
        """오더북 초기화 - 스냅샷 요청 및 적용"""
        try:
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = BithumbSpotOrderBook(
                    exchangename=self.exchangename,
                    symbol=symbol,
                    depth=self.depth
                )
                
                # 새로 생성된 오더북에 큐 설정
                if self._output_queue:
                    self.orderbooks[symbol].set_output_queue(self._output_queue)
                    
                logger.info(f"{EXCHANGE_KR} {symbol} 오더북 초기화 시작")
            
            # 스냅샷이 제공되지 않은 경우 요청
            if not snapshot:
                snapshot = await self.fetch_snapshot(symbol)
                if not snapshot:
                    logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 요청 실패")
                    return ValidationResult(False, ["스냅샷 요청 실패"])
            
            # 스냅샷 파싱
            snapshot_data = self.parse_snapshot(snapshot, symbol)
            if not snapshot_data:
                logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 파싱 실패")
                return ValidationResult(False, ["스냅샷 파싱 실패"])
            
            # 스냅샷 시퀀스 ID 설정
            sequence = snapshot_data.get("sequence", 0)
            
            # 스냅샷 데이터 적용
            ob = self.orderbooks[symbol]
            
            # 빗썸 특화 검증: bid/ask가 모두 있는 경우에만 가격 순서 체크
            is_valid = True
            error_messages = []
            
            bids = []
            asks = []
            
            # 파서에서 반환된 형식을 OrderBook 클래스에 맞게 변환
            for bid in snapshot_data.get("bids", []):
                price = bid.get("price", 0)
                size = bid.get("size", 0)
                bids.append([price, size])
                
            for ask in snapshot_data.get("asks", []):
                price = ask.get("price", 0)
                size = ask.get("size", 0)
                asks.append([price, size])
            
            if bids and asks:
                best_bid = bids[0][0]
                best_ask = asks[0][0]
                
                # 가격 역전 검사
                if best_bid >= best_ask:
                    error_msg = f"{EXCHANGE_KR} {symbol} 스냅샷 가격 역전 감지: 최고매수가({best_bid}) >= 최저매도가({best_ask})"
                    logger.warning(error_msg)
                    is_valid = False
                    error_messages.append(error_msg)
                    
                    # 가격 역전 메트릭 기록
                    self.record_metric("error", error_type="price_inversion")
            
            # 스냅샷 적용
            if is_valid:
                # 오더북 업데이트
                ob.update_orderbook(
                    bids=bids,
                    asks=asks,
                    timestamp=snapshot_data.get("timestamp"),
                    sequence=sequence,
                    is_snapshot=True
                )
                
                # 큐로 전송
                await ob.send_to_queue()
                
                # 구독 심볼 목록에 추가
                self.subscribed_symbols.add(symbol)
                
                logger.info(f"{EXCHANGE_KR} {symbol} 스냅샷 적용 완료 | 매수:{len(bids)}건, 매도:{len(asks)}건")
                
                # 메트릭 기록
                self.record_metric(
                    event_type="snapshot",
                    symbol=symbol
                )
                
                return ValidationResult(True)
            else:
                logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 적용 실패: {error_messages}")
                return ValidationResult(False, error_messages)
                
        except Exception as e:
            error_msg = f"{EXCHANGE_KR} {symbol} 오더북 초기화 실패: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return ValidationResult(False, [error_msg])

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
            # 오더북이 없으면 초기화
            if symbol not in self.orderbooks:
                logger.info(f"{EXCHANGE_KR} {symbol} 오더북 초기화 필요")
                return ValidationResult(False, ["오더북 초기화 필요"])
            
            ob = self.orderbooks[symbol]
            
            # 타임스탬프 및 시퀀스 추출
            timestamp = data.get("timestamp", int(time.time() * 1000))
            sequence = data.get("sequence", timestamp)  # 빗썸은 타임스탬프를 시퀀스로 사용
            
            # 업데이트 처리 여부 확인
            if not ob.should_process_update(timestamp):
                return ValidationResult(True)  # 무시해도 에러는 아님
            
            # 메시지 타입 확인
            msg_type = data.get("type", "")
            is_snapshot = (msg_type == "snapshot")
            
            # 파서에서 반환된 형식을 OrderBook 클래스에 맞게 변환
            bids = []
            asks = []
            
            for bid in data.get("bids", []):
                price = bid.get("price", 0)
                size = bid.get("size", 0)
                bids.append([price, size])
                
            for ask in data.get("asks", []):
                price = ask.get("price", 0)
                size = ask.get("size", 0)
                asks.append([price, size])
            
            # 원본 데이터 뎁스 로깅
            logger.debug(
                f"{EXCHANGE_KR} {symbol} 원본 데이터 | "
                f"매수:{len(data.get('bids', []))}건, 매도:{len(data.get('asks', []))}건"
            )
            
            # 오더북 업데이트
            ob.update_orderbook(
                bids=bids,
                asks=asks,
                timestamp=timestamp,
                sequence=sequence,
                is_snapshot=is_snapshot
            )
            
            # 빗썸 특화 검증: bid/ask가 모두 있는 경우에만 가격 순서 체크
            if ob.bids and ob.asks:
                best_bid = ob.bids[0][0]
                best_ask = ob.asks[0][0]
                
                # 가격 역전 검사
                if best_bid >= best_ask:
                    logger.warning(
                        f"{EXCHANGE_KR} {symbol} 가격 역전 발생: "
                        f"최고매수가({best_bid}) >= 최저매도가({best_ask})"
                    )
                    # 가격 역전 메트릭 기록
                    self.record_metric("error", error_type="price_inversion")
            
            # 메트릭 기록
            self.record_metric(
                event_type="orderbook",
                symbol=symbol
            )
            
            # 큐로 전송
            await ob.send_to_queue()
            
            # 로깅 간격 제한 (5초마다)
            current_time = time.time()
            if symbol not in self.last_queue_log_time or (current_time - self.last_queue_log_time[symbol]) >= self.queue_log_interval:
                logger.debug(
                    f"{EXCHANGE_KR} {symbol} 오더북 업데이트 | "
                    f"매수:{len(ob.bids)}건, 매도:{len(ob.asks)}건, "
                    f"타임스탬프:{timestamp}"
                )
                self.last_queue_log_time[symbol] = current_time
            
            return ValidationResult(True)
            
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} {symbol} 오더북 업데이트 실패: {str(e)}", exc_info=True)
            return ValidationResult(False, [f"업데이트 실패: {str(e)}"])

    async def subscribe(self, symbol: str):
        """
        심볼 구독 (스냅샷 요청 및 적용)
        
        Args:
            symbol: 구독할 심볼
        """
        try:
            # 이미 구독 중인 경우 스킵
            if symbol in self.subscribed_symbols:
                logger.debug(f"{EXCHANGE_KR} {symbol} 이미 구독 중")
                return
                
            # 스냅샷 요청 및 적용
            snapshot = await self.fetch_snapshot(symbol)
            if snapshot:
                result = await self.initialize_orderbook(symbol, snapshot)
                if not result.is_valid:
                    logger.error(f"{EXCHANGE_KR} {symbol} 구독 실패: {result.error_messages}")
            else:
                logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 요청 실패")
                
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} {symbol} 구독 중 오류: {str(e)}", exc_info=True)

    def set_websocket(self, ws):
        """웹소켓 연결 설정"""
        self.ws = ws
        logger.info(f"{EXCHANGE_KR} 웹소켓 연결 설정 완료")

    def is_initialized(self, symbol: str) -> bool:
        """심볼 초기화 여부 확인"""
        return symbol in self.orderbooks

    def get_orderbook(self, symbol: str) -> Optional[BithumbSpotOrderBook]:
        """심볼 오더북 가져오기"""
        return self.orderbooks.get(symbol)

    def clear_symbol(self, symbol: str) -> None:
        """심볼 데이터 제거"""
        if symbol in self.orderbooks:
            self.orderbooks.pop(symbol)
            self.subscribed_symbols.discard(symbol)
            logger.info(f"{EXCHANGE_KR} {symbol} 데이터 제거됨")
        else:
            logger.warning(f"{EXCHANGE_KR} {symbol} 데이터가 없어 제거할 수 없음")

    def clear_all(self) -> None:
        """모든 데이터 제거"""
        symbols = list(self.orderbooks.keys())
        self.orderbooks.clear()
        self.subscribed_symbols.clear()
        logger.info(f"{EXCHANGE_KR} 전체 데이터 제거됨 | symbols={symbols}")