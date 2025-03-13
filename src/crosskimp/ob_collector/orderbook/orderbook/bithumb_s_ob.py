# file: core/websocket/exchanges/bithumb_spot_orderbook_manager.py

import asyncio
import time
import aiohttp
from typing import Dict, List, Set, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.utils.config.constants import Exchange, EXCHANGE_NAMES_KR
from .base_ob import OrderBook, ValidationResult

# ============================
# 빗썸 현물 오더북 관련 상수
# ============================
EXCHANGE_CODE = Exchange.BITHUMB.value  # 거래소 코드
EXCHANGE_KR = EXCHANGE_NAMES_KR[EXCHANGE_CODE]  # 거래소 한글 이름

# 로거 인스턴스 가져오기 - 전역 로거 한 번만 초기화
logger = get_unified_logger()

class BithumbSpotOrderBook(OrderBook):
    """빗썸 현물 오더북 클래스"""
    
    def __init__(self, symbol: str, depth: int = 100):
        """초기화"""
        super().__init__(exchangename="bithumb", symbol=symbol, depth=depth, logger=logger)
        self.bids_dict = {}  # 매수 주문 (가격 -> 수량)
        self.asks_dict = {}  # 매도 주문 (가격 -> 수량)
        self.cross_detection_enabled = False
        
    def enable_cross_detection(self):
        """역전 감지 활성화"""
        self.cross_detection_enabled = True
        
    async def update(self, data: dict) -> ValidationResult:
        """오더북 업데이트"""
        try:
            # 데이터 유효성 검사
            if not data:
                return ValidationResult(False, ["데이터가 비어있음"])
                
            # 메시지 타입 확인
            msg_type = data.get("type", "delta")
            
            # 매수/매도 주문 추출
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            
            # 스냅샷인 경우 기존 데이터 초기화
            if msg_type == "snapshot":
                self.bids_dict.clear()
                self.asks_dict.clear()
            
            # 임시 딕셔너리에 복사 (가격 역전 검사를 위해)
            temp_bids_dict = self.bids_dict.copy()
            temp_asks_dict = self.asks_dict.copy()
            
            # 매수 주문 업데이트 및 가격 역전 검사
            for price_raw, qty_raw in bids:
                price = float(price_raw)
                qty = float(qty_raw)
                
                if qty > 0:
                    # 새로운 매수 호가가 기존 매도 호가보다 높거나 같은 경우 검사
                    if self.cross_detection_enabled and temp_asks_dict:
                        invalid_asks = [ask_price for ask_price in temp_asks_dict.keys() if ask_price <= price]
                        if invalid_asks and msg_type != "snapshot":  # 스냅샷이 아닌 경우에만 검사
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
            
            # 매도 주문 업데이트 및 가격 역전 검사
            for price_raw, qty_raw in asks:
                price = float(price_raw)
                qty = float(qty_raw)
                
                if qty > 0:
                    # 새로운 매도 호가가 기존 매수 호가보다 낮거나 같은 경우 검사
                    if self.cross_detection_enabled and temp_bids_dict:
                        invalid_bids = [bid_price for bid_price in temp_bids_dict.keys() if bid_price >= price]
                        if invalid_bids and msg_type != "snapshot":  # 스냅샷이 아닌 경우에만 검사
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
            
            # 비정상적인 스프레드 감지 (가격 역전은 이미 처리되었으므로 여기서는 스프레드만 체크)
            if self.bids_dict and self.asks_dict:
                highest_bid = max(self.bids_dict.keys())
                lowest_ask = min(self.asks_dict.keys())
                spread_pct = (lowest_ask - highest_bid) / lowest_ask * 100
                
                if spread_pct > 5:  # 스프레드가 5% 이상인 경우
                    logger.warning(
                        f"{EXCHANGE_KR} {self.symbol} 비정상 스프레드 감지: "
                        f"{spread_pct:.2f}% (매수: {highest_bid}, 매도: {lowest_ask})"
                    )
            
            # 오더북 크기 로깅
            logger.debug(
                f"{EXCHANGE_KR} {self.symbol} 오더북 업데이트 완료: "
                f"매수 {len(self.bids_dict)}개, 매도 {len(self.asks_dict)}개"
            )
            
            return ValidationResult(True, [])
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} {self.symbol} 오더북 업데이트 오류: {e}", exc_info=True)
            return ValidationResult(False, [str(e)])
    
    def to_dict(self) -> dict:
        """오더북을 딕셔너리로 변환"""
        # 매수/매도 주문 정렬
        sorted_bids = sorted(self.bids_dict.items(), key=lambda x: x[0], reverse=True)
        sorted_asks = sorted(self.asks_dict.items(), key=lambda x: x[0])
        
        # 상위 10개만 추출
        top_bids = [[price, qty] for price, qty in sorted_bids[:10]]
        top_asks = [[price, qty] for price, qty in sorted_asks[:10]]
        
        return {
            "exchangename": self.exchangename,
            "symbol": self.symbol,
            "bids": top_bids,
            "asks": top_asks,
            "timestamp": int(time.time() * 1000),
            "sequence": self.last_update_id or int(time.time() * 1000)
        }
    
    def get_bids(self, n: int = 10) -> List[List[float]]:
        """상위 N개 매수 주문 반환"""
        sorted_bids = sorted(self.bids_dict.items(), key=lambda x: x[0], reverse=True)
        return [[price, qty] for price, qty in sorted_bids[:n]]
    
    def get_asks(self, n: int = 10) -> List[List[float]]:
        """상위 N개 매도 주문 반환"""
        sorted_asks = sorted(self.asks_dict.items(), key=lambda x: x[0])
        return [[price, qty] for price, qty in sorted_asks[:n]]

class BithumbSpotOrderBookManager:
    """빗썸 현물 오더북 매니저"""
    
    def __init__(self, depth: int = 100):
        """초기화"""
        self.depth = depth
        self.orderbooks = {}  # 심볼 -> 오더북 객체
        self.buffer_events = {}  # 심볼 -> 이벤트 리스트
        self.sequence_states = {}  # 심볼 -> 시퀀스 상태
        self.subscribed_symbols = set()  # 구독 중인 심볼
        self.ws = None  # 웹소켓 연결
        self.output_queue = None  # 출력 큐
        self.last_queue_log_time = {}  # 심볼 -> 마지막 큐 로깅 시간
        self.queue_log_interval = 5.0  # 5초마다 로깅

    async def fetch_snapshot(self, symbol: str) -> dict:
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
            # 데이터 추출
            bids_data = data.get("bids", [])
            asks_data = data.get("asks", [])
            
            # 유효한 가격/수량만 필터링
            bids = []
            for bid_item in bids_data:
                # 딕셔너리 형태인 경우 (API 응답)
                if isinstance(bid_item, dict):
                    price = bid_item.get("price")
                    quantity = bid_item.get("quantity")
                # 리스트/튜플 형태인 경우 (이전 코드 호환성)
                elif isinstance(bid_item, (list, tuple)) and len(bid_item) >= 2:
                    price, quantity = bid_item
                else:
                    continue
                    
                try:
                    price_float = float(price)
                    quantity_float = float(quantity)
                    if price_float > 0 and quantity_float > 0:
                        bids.append([price_float, quantity_float])
                except (ValueError, TypeError):
                    logger.warning(f"{EXCHANGE_KR} 유효하지 않은 매수 데이터: {bid_item}")
                    continue
            
            asks = []
            for ask_item in asks_data:
                # 딕셔너리 형태인 경우 (API 응답)
                if isinstance(ask_item, dict):
                    price = ask_item.get("price")
                    quantity = ask_item.get("quantity")
                # 리스트/튜플 형태인 경우 (이전 코드 호환성)
                elif isinstance(ask_item, (list, tuple)) and len(ask_item) >= 2:
                    price, quantity = ask_item
                else:
                    continue
                    
                try:
                    price_float = float(price)
                    quantity_float = float(quantity)
                    if price_float > 0 and quantity_float > 0:
                        asks.append([price_float, quantity_float])
                except (ValueError, TypeError):
                    logger.warning(f"{EXCHANGE_KR} 유효하지 않은 매도 데이터: {ask_item}")
                    continue
            
            # 타임스탬프 추출 (밀리초 단위)
            timestamp = int(data.get("timestamp", time.time() * 1000))
            
            # 결과 포맷팅
            parsed = {
                "symbol": symbol,
                "exchangename": "bithumb",
                "bids": bids,
                "asks": asks,
                "timestamp": timestamp,
                "sequence": timestamp,  # 빗썸은 타임스탬프를 시퀀스로 사용
                "type": "snapshot"
            }
            
            # 디버깅을 위한 로깅 추가
            logger.debug(f"{EXCHANGE_KR} {symbol} 스냅샷 파싱 완료: 매수 {len(bids)}개, 매도 {len(asks)}개")
            
            return parsed
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} 스냅샷 파싱 오류: {e}", exc_info=True)
            return {}

    async def initialize_orderbook(self, symbol: str):
        """오더북 초기화 - 스냅샷 요청 및 적용"""
        if symbol in self.orderbooks:
            return

        logger.info(f"{EXCHANGE_KR} {symbol} 오더북 초기화 시작")
        self.orderbooks[symbol] = BithumbSpotOrderBook(
            symbol=symbol,
            depth=self.depth
        )
        
        # 시퀀스 상태 초기화
        self.sequence_states[symbol] = {
            "initialized": False,
            "last_update_id": 0,
            "first_delta_applied": False
        }
        
        # 버퍼 초기화
        self.buffer_events[symbol] = []
        
        # 스냅샷 요청 및 적용
        snapshot = await self.fetch_snapshot(symbol)
        if not snapshot:
            logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 요청 실패")
            return
            
        # 스냅샷 적용
        ob = self.orderbooks[symbol]
        snapshot_data = self.parse_snapshot(snapshot, symbol)
        
        # 스냅샷 시퀀스 ID 설정
        sequence = snapshot_data.get("sequence", 0)
        self.sequence_states[symbol]["last_update_id"] = sequence
        self.sequence_states[symbol]["initialized"] = True
        
        # 스냅샷 데이터 적용
        await ob.update(snapshot_data)
        
        logger.info(
            f"{EXCHANGE_KR} {symbol} 오더북 초기화 완료 | "
            f"seq={sequence}, 매수:{len(ob.bids_dict)}개, 매도:{len(ob.asks_dict)}개"
        )
        
        # 큐에 스냅샷 전송
        if self.output_queue:
            final_ob = ob.to_dict()
            try:
                await self.output_queue.put(("bithumb", final_ob))
                logger.debug(f"{EXCHANGE_KR} {symbol} 스냅샷 큐 전송 완료")
                self.last_queue_log_time[symbol] = time.time()
            except Exception as e:
                logger.error(f"{EXCHANGE_KR} 큐 전송 실패: {e}", exc_info=True)

    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """웹소켓 메시지 처리"""
        if symbol not in self.orderbooks:
            logger.warning(f"{EXCHANGE_KR} {symbol} 오더북이 초기화되지 않음")
            return ValidationResult(False, ["오더북이 초기화되지 않음"])
            
        # 시퀀스 상태 확인
        st = self.sequence_states.get(symbol)
        if not st or not st["initialized"]:
            # 초기화되지 않은 경우 버퍼에 추가
            self.buffer_events.setdefault(symbol, []).append(data)
            logger.debug(f"{EXCHANGE_KR} {symbol} 초기화 전 이벤트 버퍼링 (버퍼 크기: {len(self.buffer_events[symbol])})")
            return ValidationResult(True, ["초기화 전 이벤트 버퍼링"])
            
        # 타임스탬프 추출 (시퀀스로 사용)
        timestamp = data.get("timestamp", int(time.time() * 1000))
        data["sequence"] = timestamp
        
        # 시퀀스 확인
        last_id = st["last_update_id"]
        if timestamp <= last_id:
            logger.debug(f"{EXCHANGE_KR} {symbol} 이미 처리된 이벤트 스킵 | timestamp={timestamp}, last_id={last_id}")
            return ValidationResult(True, ["이미 처리된 이벤트"])
            
        # 이벤트 버퍼링
        self.buffer_events.setdefault(symbol, []).append(data)
        
        # 버퍼 이벤트 적용
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
                logger.debug(f"{EXCHANGE_KR} {symbol} 이미 처리된 이벤트 스킵 | evt_seq={evt_seq}, last_id={last_id}")
                continue

            first_seq = evt.get("sequence", 0)
            # 빗썸은 마이크로초 단위 타임스탬프를 시퀀스로 사용하므로,
            # 5초(5,000,000 마이크로초) 이상의 큰 시간 갭이 있을 때만 경고
            if first_seq - last_id > 5_000_000:  # 5초 이상의 갭
                logger.warning(
                    f"{EXCHANGE_KR} {symbol} 큰 시간 gap 발생 -> "
                    f"이전={last_id}, 현재={first_seq}, 차이={(first_seq-last_id)/1_000_000:.2f}초"
                )

            # 이벤트 적용 전 오더북 크기 기록
            bids_count_before = len(ob.bids_dict) if hasattr(ob, 'bids_dict') else 0
            asks_count_before = len(ob.asks_dict) if hasattr(ob, 'asks_dict') else 0

            res = await ob.update(evt)
            if res.is_valid:
                if not st["first_delta_applied"]:
                    st["first_delta_applied"] = True
                    ob.enable_cross_detection()
                    logger.info(f"{EXCHANGE_KR} {symbol} 첫 델타 적용 완료, 역전 감지 활성화")
                last_id = evt_seq
                applied_count += 1

                # 이벤트 적용 후 오더북 크기 변화 로깅
                if hasattr(ob, 'bids_dict') and hasattr(ob, 'asks_dict'):
                    bids_count_after = len(ob.bids_dict)
                    asks_count_after = len(ob.asks_dict)
                    bids_diff = bids_count_after - bids_count_before
                    asks_diff = asks_count_after - asks_count_before
                    
                    if bids_diff != 0 or asks_diff != 0:
                        logger.debug(
                            f"{EXCHANGE_KR} {symbol} 오더북 크기 변화 | "
                            f"매수: {bids_count_before} → {bids_count_after} ({bids_diff:+d}), "
                            f"매도: {asks_count_before} → {asks_count_after} ({asks_diff:+d})"
                        )
            else:
                logger.error(f"{EXCHANGE_KR} {symbol} 델타 적용 실패: {res.error_messages}")

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
                    # 오더북 크기 정보 추가
                    if hasattr(ob, 'bids_dict') and hasattr(ob, 'asks_dict'):
                        logger.debug(
                            f"{EXCHANGE_KR} {symbol} 오더북 큐 전송 완료 | "
                            f"seq={last_id}, 매수:{len(ob.bids_dict)}개, 매도:{len(ob.asks_dict)}개"
                        )
                    else:
                        logger.debug(f"{EXCHANGE_KR} {symbol} 오더북 큐 전송 완료 | seq={last_id}")
                    self.last_queue_log_time[symbol] = current_time
                    
            except Exception as e:
                logger.error(f"{EXCHANGE_KR} 큐 전송 실패: {e}", exc_info=True)

    async def subscribe(self, symbol: str):
        """심볼 구독 - 오더북 초기화 및 웹소켓 구독"""
        if symbol in self.subscribed_symbols:
            return
            
        # 오더북 초기화
        await self.initialize_orderbook(symbol)
        
        # 웹소켓 구독
        if self.ws:
            await self.ws.subscribe_depth(symbol)
            self.subscribed_symbols.add(symbol)
            logger.info(f"{EXCHANGE_KR} {symbol} 웹소켓 구독 완료")
        else:
            logger.error(f"{EXCHANGE_KR} {symbol} 웹소켓 구독 실패: 웹소켓 연결 없음")

    def set_output_queue(self, queue):
        """출력 큐 설정"""
        self.output_queue = queue
        logger.info(f"{EXCHANGE_KR} 출력 큐 설정 완료")
        
    def set_websocket(self, ws):
        """웹소켓 연결 설정"""
        self.ws = ws
        logger.info(f"{EXCHANGE_KR} 웹소켓 연결 설정 완료")
        
    def is_initialized(self, symbol: str) -> bool:
        """심볼 초기화 여부 확인"""
        st = self.sequence_states.get(symbol)
        return bool(st and st.get("initialized"))
        
    def get_orderbook(self, symbol: str) -> Optional[BithumbSpotOrderBook]:
        """심볼 오더북 객체 반환"""
        return self.orderbooks.get(symbol)
        
    def clear_symbol(self, symbol: str) -> None:
        """심볼 데이터 제거"""
        self.orderbooks.pop(symbol, None)
        self.sequence_states.pop(symbol, None)
        self.buffer_events.pop(symbol, None)
        logger.info(f"{EXCHANGE_KR} {symbol} 심볼 데이터 제거 완료")
        
    def clear_all(self) -> None:
        """모든 심볼 데이터 제거"""
        syms = list(self.orderbooks.keys())
        self.orderbooks.clear()
        self.sequence_states.clear()
        self.buffer_events.clear()
        logger.info(f"{EXCHANGE_KR} 전체 심볼 데이터 제거 완료: {syms}")