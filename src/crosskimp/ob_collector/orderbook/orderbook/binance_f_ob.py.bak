# file: orderbook/binance_future_orderbook_manager.py

import asyncio
import time
from typing import Dict, Optional, List, Any
from dataclasses import dataclass

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

@dataclass
class OrderBookUpdate:
    bids: Dict[float, float]
    asks: Dict[float, float]
    timestamp: int
    first_update_id: int
    final_update_id: int
    
    def is_valid(self) -> bool:
        if not self.bids or not self.asks:
            return True  # 한쪽만 업데이트되는 경우 허용
            
        best_bid = max(self.bids.keys()) if self.bids else 0
        best_ask = min(self.asks.keys()) if self.asks else float('inf')
        return best_bid < best_ask

def parse_binance_future_depth_update(msg_data: dict) -> Optional[dict]:
    """
    Binance Future depthUpdate 메시지를 공통 포맷으로 변환
    """
    if msg_data.get("e") != "depthUpdate":
        return None
    
    symbol_raw = msg_data.get("s", "")
    symbol = symbol_raw.replace("USDT", "").upper()

    b_data = msg_data.get("b", [])
    a_data = msg_data.get("a", [])
    event_time = msg_data.get("E", 0)
    first_id = msg_data.get("U", 0)
    final_id = msg_data.get("u", 0)

    # 숫자 변환 및 0 이상 필터링
    bids = [[float(b[0]), float(b[1])] for b in b_data if float(b[0]) > 0 and float(b[1]) != 0]
    asks = [[float(a[0]), float(a[1])] for a in a_data if float(a[0]) > 0 and float(a[1]) != 0]

    return {
        "exchangename": "binancefuture",
        "symbol": symbol,
        "bids": bids,
        "asks": asks,
        "timestamp": event_time,
        "first_update_id": first_id,
        "final_update_id": final_id,
        "sequence": final_id,
        "type": "delta"
    }

class BinanceFutureOrderBook(OrderBookV2):
    """
    바이낸스 선물 전용 오더북 클래스 (V2)
    - 시퀀스 기반 업데이트 관리
    - 가격 역전 감지 및 처리
    """
    def __init__(self, exchangename: str, symbol: str, depth: int = 100, inversion_detection: bool = False):
        """초기화"""
        super().__init__(exchangename, symbol, depth)
        self.bids_dict = {}  # 매수 주문 (가격 -> 수량)
        self.asks_dict = {}  # 매도 주문 (가격 -> 수량)
        self.cross_detection_enabled = False
        self.last_cpp_send_time = 0  # C++ 전송 시간 추적
        self.last_queue_send_time = 0  # 큐 전송 시간 추적
        self.cpp_send_interval = 0.1  # 0.1초 간격으로 제한 (초당 10회)
        self.queue_send_interval = 0.1  # 0.1초 간격으로 제한 (초당 10회)
        self.inversion_detection = inversion_detection
        if inversion_detection:
            self.enable_cross_detection()
        
    def enable_cross_detection(self):
        """역전 감지 활성화"""
        self.cross_detection_enabled = True
        logger.info(f"{EXCHANGE_KR} {self.symbol} 역전감지 활성화")
        
    def update_orderbook(self, bids: List[List[float]], asks: List[List[float]], 
                        timestamp: Optional[int] = None, sequence: Optional[int] = None,
                        is_snapshot: bool = False) -> None:
        """
        오더북 데이터 업데이트 (바이낸스 선물 전용)
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
        
        # C++로 데이터 전송
        try:
            await send_orderbook_to_cpp(self.exchangename, orderbook_data)
            self.last_cpp_send_time = current_time
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
            except Exception as e:
                logger.error(f"{EXCHANGE_KR} {self.symbol} 큐 전송 실패: {str(e)}", exc_info=True)
        
        # C++로 데이터 전송 (비동기 태스크로 실행하여 블로킹하지 않도록 함)
        asyncio.create_task(self.send_to_cpp())

class BinanceFutureOrderBookManager(BaseOrderBookManagerV2):
    """
    바이낸스 선물 오더북 관리자
    - 스냅샷 초기화
    - 델타 업데이트 처리
    - 시퀀스 관리
    - 가격 역전 감지 및 처리
    """
    def __init__(self, depth: int = 100):
        """
        바이낸스 선물 오더북 관리자 초기화
        
        Args:
            depth: 오더북 깊이
        """
        super().__init__(depth)
        self.exchangename = EXCHANGE_CODE
        
        # 시퀀스 관리 상태
        self.sequence_states: Dict[str, Dict[str, Any]] = {}
        
        # 버퍼 이벤트
        self.buffer_events: Dict[str, List[Dict[str, Any]]] = {}
        
        # 가격 역전 카운터
        self.price_inversion_count: Dict[str, int] = {}
        
        # 마지막 갭 경고 시간
        self.last_gap_warning_time: Dict[str, float] = {}
        
        # 마지막 큐 로그 시간
        self.last_queue_log_time: Dict[str, float] = {}
        
        # 큐 로그 간격 (초)
        self.queue_log_interval: float = 60.0
        
        # 갭 임계값 및 경고 간격
        self.gap_threshold = 100  # 100개 이상의 갭이 발생하면 경고
        self.gap_warning_interval = 5.0  # 5초에 한 번만 경고
        
        # 버퍼 크기 제한
        self.max_buffer_size = 1000
        
        # 가격 역전 감지 활성화할 심볼 목록
        self.inversion_detection_symbols = ["BTC", "ETH", "SOL", "XRP", "LINK"]
        
        # 메트릭 로깅 시작
        asyncio.create_task(self.initialize())
        
    async def initialize(self):
        """메트릭 로깅 시작"""
        await super().initialize()

    async def start(self):
        """오더북 매니저 시작"""
        await self.initialize()  # 메트릭 로깅 시작

    async def initialize_snapshot(self, symbol: str, data: dict) -> ValidationResult:
        """
        스냅샷 초기화 처리
        - 시퀀스 상태 초기화
        - 오더북 객체 생성 및 초기화
        - 버퍼된 이벤트 적용
        """
        try:
            # 스냅샷 데이터 검증
            if not data or "lastUpdateId" not in data:
                logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 데이터 형식 오류")
                return ValidationResult(False, ["스냅샷 데이터 형식 오류"])
            
            # 시퀀스 상태 초기화
            last_update_id = data.get("lastUpdateId", 0)
            if symbol not in self.sequence_states:
                self.sequence_states[symbol] = {
                    "initialized": False,
                    "last_update_id": 0,
                    "force_jump_count": 0,
                    "first_delta_applied": False,
                    "needs_reinitialization": False,
                    "request_reinitialization": False,
                    "reinitialization_requested_at": 0,
                    "last_reinitialization_time": time.time(),
                    "reinitialization_count": 0
                }
            
            # 이미 초기화된 경우 재초기화 여부 확인
            st = self.sequence_states[symbol]
            if st["initialized"]:
                # 마지막 재초기화 시간 확인
                current_time = time.time()
                last_reinit_time = st.get("last_reinitialization_time", 0)
                
                # 재초기화 간격이 너무 짧으면 제한
                if current_time - last_reinit_time < 5.0:  # 5초 이내 재초기화 제한
                    reinit_count = st.get("reinitialization_count", 0)
                    if reinit_count > 10:  # 과도한 재초기화 제한
                        logger.warning(
                            f"{EXCHANGE_KR} {symbol} 과도한 재초기화 시도 감지 (10회 초과), "
                            f"재초기화 간격: {current_time - last_reinit_time:.2f}초"
                        )
                        return ValidationResult(False, ["과도한 재초기화 시도"])
            
            # 시퀀스 상태 업데이트
            st["initialized"] = True
            st["last_update_id"] = last_update_id
            st["force_jump_count"] = 0
            st["first_delta_applied"] = False
            st["needs_reinitialization"] = False
            st["request_reinitialization"] = False
            st["reinitialization_requested_at"] = 0
            st["last_reinitialization_time"] = time.time()
            st["reinitialization_count"] = st.get("reinitialization_count", 0) + 1
            
            # 오더북 객체 생성 또는 업데이트
            bids = [[float(b[0]), float(b[1])] for b in data.get("bids", [])]
            asks = [[float(a[0]), float(a[1])] for a in data.get("asks", [])]
            
            # 가격 역전 감지 활성화 (BTC 등 주요 심볼)
            inversion_detection = symbol in self.inversion_detection_symbols
            
            # 오더북 객체가 없으면 생성
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = BinanceFutureOrderBook(
                    exchangename=self.exchangename,
                    symbol=symbol,
                    inversion_detection=inversion_detection
                )
                
                if inversion_detection:
                    logger.info(f"{EXCHANGE_KR} {symbol} 역전감지 활성화")
            
            # 오더북 스냅샷 업데이트
            ob = self.orderbooks[symbol]
            ob.update_orderbook(
                bids=bids,
                asks=asks,
                timestamp=int(time.time() * 1000),
                sequence=last_update_id,
                is_snapshot=True
            )
            
            # 초기화 직후 가격 역전 감지 활성화 (추가 안전장치)
            if inversion_detection:
                logger.info(f"{EXCHANGE_KR} {symbol} 초기화 직후 가격 역전 감지 활성화")
            
            # 버퍼된 이벤트 적용
            await self._apply_buffered_events(symbol)
            
            # 초기화 성공 로그
            logger.info(
                f"{EXCHANGE_KR} {symbol} 스냅샷 초기화 완료 | "
                f"seq={last_update_id}, 매수:{len(ob.bids)}개, 매도:{len(ob.asks)}개"
            )
            
            # 메트릭 업데이트
            self.record_metric("snapshot", symbol=symbol)
            
            return ValidationResult(True, [])
        
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} {symbol} 스냅샷 초기화 중 오류: {e}", exc_info=True)
            # 예외 발생 메트릭 기록
            self.record_metric("error", error_type="exception")
            return ValidationResult(False, [str(e)])

    async def _apply_buffered_events(self, symbol: str) -> None:
        """
        바이낸스 선물 특화 시퀀스 관리:
        - gap 발생 시 강제 jump
        - gap 크기 모니터링
        - 과도한 갭 발생 시 경고 제한
        - 가격 역전 감지 및 처리 강화
        - C++ 직접 전송 지원
        - pu 필드 검증 추가 (바이낸스 문서 기준)
        - 재초기화 로직 개선
        """
        st = self.sequence_states.get(symbol)
        if not st or not st["initialized"]:
            return

        last_id = st["last_update_id"]
        events = self.buffer_events[symbol]
        sorted_events = sorted(events, key=lambda x: x.get("final_update_id", 0))
        ob = self.orderbooks[symbol]
        applied_count = 0
        current_time = time.time()
        
        # 이전 이벤트의 final_update_id를 추적
        previous_final_id = last_id

        for evt in sorted_events:
            first_id = evt.get("first_update_id", 0)
            final_id = evt.get("final_update_id", 0)
            previous_final_id_in_event = evt.get("pu", 0)  # 이벤트에 포함된 이전 final_update_id

            # 이미 처리된 이벤트 스킵
            if final_id <= last_id:
                continue

            # 바이낸스 문서에 따라 pu 필드 검증 (이전 이벤트의 u와 현재 이벤트의 pu가 같아야 함)
            # 첫 번째 델타 업데이트 이후에만 검증 (스냅샷 직후 첫 업데이트는 예외)
            if st.get("first_delta_applied", False) and previous_final_id_in_event > 0 and previous_final_id != previous_final_id_in_event:
                # 시퀀스 불일치 발생 시 로깅
                logger.warning(
                    f"{EXCHANGE_KR} {symbol} 시퀀스 불일치 감지: "
                    f"previous_final_id={previous_final_id}, pu={previous_final_id_in_event}, "
                    f"차이={previous_final_id_in_event - previous_final_id}"
                )
                
                # 시퀀스 불일치 메트릭 기록
                self.record_metric("error", error_type="sequence_mismatch")
                
                # 시퀀스 불일치가 심각한 경우 (차이가 큰 경우) 즉시 중단
                if abs(previous_final_id_in_event - previous_final_id) > 1000:
                    logger.error(
                        f"{EXCHANGE_KR} {symbol} 심각한 시퀀스 불일치 감지, 즉시 재초기화 필요"
                    )
                    # 시퀀스 불일치 시 스냅샷 재초기화 필요 플래그 설정
                    st["needs_reinitialization"] = True
                    break
                
                # 경미한 시퀀스 불일치는 계속 진행 (강제 점프)
                logger.info(
                    f"{EXCHANGE_KR} {symbol} 경미한 시퀀스 불일치, 강제 진행"
                )
                # 강제 점프 시 이전 ID 업데이트 (다음 이벤트에서 불일치 방지)
                previous_final_id = previous_final_id_in_event

            # 시퀀스 갭 감지
            if first_id > last_id + 1:
                gap_size = first_id - (last_id + 1)
                st["force_jump_count"] = st.get("force_jump_count", 0) + 1
                
                # 마지막 경고 시간 확인
                last_warning = self.last_gap_warning_time.get(symbol, 0)
                
                # 갭 크기가 임계값을 초과하고 경고 간격이 지났으면 경고
                if gap_size > self.gap_threshold and (current_time - last_warning) >= self.gap_warning_interval:
                    logger.warning(
                        f"{EXCHANGE_KR} {symbol} 큰 시퀀스 갭 발생: "
                        f"last_id={last_id}, first_id={first_id}, gap={gap_size}, "
                        f"force_jumps={st['force_jump_count']}, "
                        f"초당갭비율={gap_size/self.gap_warning_interval:.2f}"
                    )
                    self.last_gap_warning_time[symbol] = current_time
                    
                    # 갭이 너무 큰 경우 재초기화 필요 플래그 설정
                    if gap_size > 10000:  # 10,000 이상의 갭은 심각한 문제로 간주
                        logger.error(
                            f"{EXCHANGE_KR} {symbol} 매우 큰 시퀀스 갭 발생, 재초기화 필요"
                        )
                        st["needs_reinitialization"] = True
                        break
                elif gap_size <= self.gap_threshold:
                    logger.debug(
                        f"{EXCHANGE_KR} {symbol} 작은 시퀀스 갭: "
                        f"last_id={last_id}, first_id={first_id}, gap={gap_size}"
                    )

            # 가격 역전 검사 및 처리 개선
            bids = [[float(b[0]), float(b[1])] for b in evt.get("bids", [])]
            asks = [[float(a[0]), float(a[1])] for a in evt.get("asks", [])]
            
            # 현재 오더북 상태 가져오기
            current_bids = {bid[0]: bid[1] for bid in ob.bids}
            current_asks = {ask[0]: ask[1] for ask in ob.asks}
            
            # 업데이트 후 상태 예측
            updated_bids = current_bids.copy()
            updated_asks = current_asks.copy()
            
            # 매수 업데이트 적용
            for price, qty in bids:
                if qty > 0:
                    updated_bids[price] = qty
                else:
                    updated_bids.pop(price, None)
            
            # 매도 업데이트 적용
            for price, qty in asks:
                if qty > 0:
                    updated_asks[price] = qty
                else:
                    updated_asks.pop(price, None)
            
            # 가격 역전 검사
            if updated_bids and updated_asks:
                best_bid = max(updated_bids.keys())
                best_ask = min(updated_asks.keys())
                
                if best_bid >= best_ask:
                    # 가격 역전 카운터 증가
                    self.price_inversion_count[symbol] = self.price_inversion_count.get(symbol, 0) + 1
                    
                    logger.warning(
                        f"{EXCHANGE_KR} {symbol} 가격 역전 감지 및 수정 (#{self.price_inversion_count[symbol]}): "
                        f"최고매수={best_bid}, 최저매도={best_ask}, 차이={best_bid-best_ask}"
                    )
                    
                    # 가격 역전 수정: 역전된 매수 호가 제거
                    invalid_bids = [price for price in updated_bids.keys() if price >= best_ask]
                    for price in invalid_bids:
                        updated_bids.pop(price)
                    
                    # 이벤트에서 역전된 매수 호가 제거
                    evt["bids"] = [[price, qty] for price, qty in evt.get("bids", []) 
                                  if float(price) < best_ask]
                    
                    logger.info(f"{EXCHANGE_KR} {symbol} 역전된 매수호가 {len(invalid_bids)}개 제거 완료")

            # V2 방식으로 오더북 업데이트
            bids = [[price, qty] for price, qty in updated_bids.items()]
            asks = [[price, qty] for price, qty in updated_asks.items()]
            
            # 정렬
            bids = sorted(bids, key=lambda x: x[0], reverse=True)
            asks = sorted(asks, key=lambda x: x[0])
            
            # 오더북 업데이트 (비동기 태스크 생성 없이 직접 업데이트)
            ob.update_orderbook(
                bids=bids,
                asks=asks,
                timestamp=evt.get("timestamp", int(time.time() * 1000)),
                sequence=final_id,
                is_snapshot=False
            )
            
            if not st["first_delta_applied"]:
                st["first_delta_applied"] = True
                logger.info(f"{EXCHANGE_KR} {symbol} 첫 번째 델타 업데이트 적용 완료")
            
            # 이전 이벤트의 final_update_id 업데이트
            previous_final_id = final_id
            last_id = final_id
            applied_count += 1

        # 버퍼 비우기
        self.buffer_events[symbol] = []
        st["last_update_id"] = last_id

        # 재초기화 필요 여부 확인
        if st.get("needs_reinitialization", False):
            logger.warning(f"{EXCHANGE_KR} {symbol} 시퀀스 불일치로 인한 재초기화 필요")
            # 재초기화 플래그 초기화
            st["needs_reinitialization"] = False
            # 재초기화 요청 플래그 설정
            st["request_reinitialization"] = True
            # 재초기화 요청 시간 기록
            st["reinitialization_requested_at"] = time.time()
            return

        if applied_count > 0:
            # 큐로 데이터 전송 (한 번만 전송)
            await ob.send_to_queue()
            
            # C++로 데이터 전송 (큐와 별도로 직접 전송)
            await ob.send_to_cpp()
            
            # 로깅 주기 체크
            current_time = time.time()
            last_log_time = self.last_queue_log_time.get(symbol, 0)
            if current_time - last_log_time >= self.queue_log_interval:
                logger.debug(
                    f"{EXCHANGE_KR} {symbol} 오더북 업데이트 | "
                    f"seq={last_id}, 매수:{len(ob.bids)}개, 매도:{len(ob.asks)}개"
                )
                self.last_queue_log_time[symbol] = current_time

    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """
        델타 업데이트 처리
        - 초기화 전: 버퍼링
        - 초기화 후: 시퀀스 검증 후 적용
        - 선물 특화: 갭 발생 시 강제 진행
        - 재초기화 로직 추가
        """
        try:
            # 오더북이 초기화되지 않은 경우 초기화
            if symbol not in self.orderbooks:
                # 스냅샷 데이터가 필요하므로 초기화는 별도로 진행
                logger.debug(f"{EXCHANGE_KR} {symbol} 오더북이 초기화되지 않음")
                return ValidationResult(True, ["오더북이 초기화되지 않음"])
            
            # 재초기화 요청 확인
            st = self.sequence_states.get(symbol, {})
            if st.get("request_reinitialization", False):
                # 재초기화 요청 시간 확인
                request_time = st.get("reinitialization_requested_at", 0)
                current_time = time.time()
                
                # 재초기화 요청 후 일정 시간이 지났으면 재초기화 진행
                if current_time - request_time > 5.0:  # 5초 후 재초기화
                    logger.info(f"{EXCHANGE_KR} {symbol} 재초기화 진행")
                    
                    # 재초기화 요청 플래그 초기화
                    st["request_reinitialization"] = False
                    st["reinitialization_requested_at"] = 0
                    
                    # 시퀀스 상태 초기화
                    st["initialized"] = False
                    st["first_delta_applied"] = False
                    
                    # 버퍼 비우기
                    self.buffer_events[symbol] = []
                    
                    # 재초기화 메트릭 기록
                    self.record_metric("reinitialization", symbol=symbol)
                    
                    return ValidationResult(True, ["재초기화 진행"])
                
            # 바이낸스 선물 depthUpdate 메시지 처리
            if data.get("e") == "depthUpdate":
                # 메시지 변환
                symbol = data.get("s", "").replace("USDT", "").upper()
                first_update_id = data.get("U", 0)
                final_update_id = data.get("u", 0)
                previous_update_id = data.get("pu", 0)
                
                # 변환된 데이터 생성
                converted_data = {
                    "exchangename": self.exchangename,
                    "symbol": symbol,
                    "bids": [[float(b[0]), float(b[1])] for b in data.get("b", [])],
                    "asks": [[float(a[0]), float(a[1])] for a in data.get("a", [])],
                    "timestamp": data.get("E", int(time.time() * 1000)),
                    "first_update_id": first_update_id,
                    "final_update_id": final_update_id,
                    "pu": previous_update_id,  # pu 필드 추가
                    "sequence": final_update_id
                }
                
                # 버퍼 초기화
                if symbol not in self.buffer_events:
                    self.buffer_events[symbol] = []
                
                # 버퍼 크기 제한
                if len(self.buffer_events[symbol]) >= self.max_buffer_size:
                    self.buffer_events[symbol].pop(0)
                
                # 이벤트 버퍼링
                self.buffer_events[symbol].append(converted_data)
                
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
            
            # 기존 처리 로직 (이미 변환된 데이터)
            else:
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
            logger.error(f"{EXCHANGE_KR} {symbol} 업데이트 중 오류: {e}", exc_info=True)
            # 예외 발생 메트릭 기록
            self.record_metric("error", error_type="exception")
            return ValidationResult(False, [str(e)])

    def is_initialized(self, symbol: str) -> bool:
        """심볼 초기화 여부 확인"""
        st = self.sequence_states.get(symbol)
        return bool(st and st.get("initialized"))

    def get_orderbook(self, symbol: str) -> Optional[BinanceFutureOrderBook]:
        """심볼 오더북 객체 반환"""
        return self.orderbooks.get(symbol)

    def clear_symbol(self, symbol: str) -> None:
        """심볼 데이터 제거"""
        self.orderbooks.pop(symbol, None)
        self.sequence_states.pop(symbol, None)
        self.buffer_events.pop(symbol, None)
        self.last_gap_warning_time.pop(symbol, None)
        self.last_queue_log_time.pop(symbol, None)
        self.price_inversion_count.pop(symbol, None)
        logger.info(f"{EXCHANGE_KR} {symbol} 데이터 제거 완료")

    def clear_all(self) -> None:
        """모든 심볼 데이터 제거"""
        syms = list(self.orderbooks.keys())
        self.orderbooks.clear()
        self.sequence_states.clear()
        self.buffer_events.clear()
        self.last_gap_warning_time.clear()
        self.last_queue_log_time.clear()
        self.price_inversion_count.clear()
        logger.info(f"{EXCHANGE_KR} 전체 데이터 제거 완료: {syms}")
        
    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """출력 큐 설정 (오버라이드)"""
        self._output_queue = queue
        # 기존 오더북에 큐 설정
        for ob in self.orderbooks.values():
            ob.set_output_queue(queue)
        logger.info(f"{EXCHANGE_KR} 출력 큐 설정 완료")
        
    async def subscribe(self, symbol: str):
        """심볼 구독 - 오더북 초기화"""
        # 오더북 초기화는 스냅샷 데이터가 필요하므로 별도로 진행
        if symbol not in self.orderbooks:
            logger.info(f"{EXCHANGE_KR} {symbol} 구독 준비 완료 (스냅샷 필요)")
        else:
            logger.info(f"{EXCHANGE_KR} {symbol} 이미 초기화됨")