# file: orderbook/binance_future_orderbook_manager.py

import asyncio
import time
from typing import Dict, Optional, List
from dataclasses import dataclass

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.orderbook.base_orderbook import OrderBook, ValidationResult
from crosskimp.ob_collector.orderbook.orderbook.base_orderbook_manager import BaseOrderBookManager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

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
            
        best_bid = max(self.bids.keys())
        best_ask = min(self.asks.keys())
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

class BinanceFutureOrderBookManager(BaseOrderBookManager):
    """
    바이낸스 선물 오더북 매니저
    - 시퀀스 gap 발생 시 재동기화 대신 '강제 jump'
    - 바이낸스 현물과 다른 시퀀스 관리 정책
    """

    def __init__(self, depth: int = 100):
        super().__init__(depth)
        self.exchangename = "binancefuture"
        self.logger = logger
        
        # 선물 특화 설정
        self.force_jump = True  # 시퀀스 갭 발생 시 강제 진행
        self.gap_threshold = 1000  # 갭 크기 임계값 (1000으로 조정)
        self.gap_warning_interval = 5.0  # 갭 경고 간격 (초)
        self.last_gap_warning_time: Dict[str, float] = {}  # 심볼별 마지막 갭 경고 시간

    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """
        델타 업데이트 처리
        - 초기화 전: 버퍼링
        - 초기화 후: 시퀀스 검증 후 적용
        - 선물 특화: 갭 발생 시 강제 진행
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
            self.logger.debug(f"[{self.exchangename}] {symbol} 초기화 전 버퍼링")
            return ValidationResult(True, [])

        # 초기화 완료 후 이벤트 적용
        await self._apply_buffered_events(symbol)
        return ValidationResult(True, [])

    async def initialize_orderbook(self, symbol: str, snapshot: dict) -> ValidationResult:
        """오더북 초기화 (스냅샷 적용)"""
        try:
            last_update_id = snapshot.get("sequence")
            if last_update_id is None:
                return ValidationResult(False, [f"{symbol} snapshot에 sequence 없음"])

            # 시퀀스 상태 초기화
            self.sequence_states[symbol] = {
                "initialized": True,
                "last_update_id": last_update_id,
                "first_delta_applied": False,
                "force_jump_count": 0  # 선물 특화: 강제 진행 횟수 추적
            }

            # 오더북 생성 및 스냅샷 적용
            ob = OrderBook(self.exchangename, symbol, self.depth)
            self.orderbooks[symbol] = ob
            self.buffer_events.setdefault(symbol, [])

            res = await ob.update(snapshot)
            if not res.is_valid:
                return res

            await self._apply_buffered_events(symbol)
            return res

        except Exception as e:
            self.logger.error(
                f"[{self.exchangename}] {symbol} 초기화 중 오류: {e}",
                exc_info=True
            )
            return ValidationResult(False, [str(e)])

    async def _apply_buffered_events(self, symbol: str) -> None:
        """
        바이낸스 선물 특화 시퀀스 관리:
        - gap 발생 시 강제 jump
        - gap 크기 모니터링
        - 과도한 갭 발생 시 경고 제한
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

        for evt in sorted_events:
            first_id = evt.get("first_update_id", 0)
            final_id = evt.get("final_update_id", 0)

            if final_id <= last_id:
                continue

            if first_id > last_id + 1:
                gap_size = first_id - (last_id + 1)
                st["force_jump_count"] += 1
                
                # 마지막 경고 시간 확인
                last_warning = self.last_gap_warning_time.get(symbol, 0)
                
                # 갭 크기가 임계값을 초과하고 경고 간격이 지났으면 경고
                # if gap_size > self.gap_threshold and (current_time - last_warning) >= self.gap_warning_interval:
                    # self.logger.warning(
                    #     f"[{self.exchangename}] {symbol} 큰 시퀀스 갭 발생: "
                    #     f"last_id={last_id}, first_id={first_id}, gap={gap_size}, "
                    #     f"force_jumps={st['force_jump_count']}, "
                    #     f"초당갭비율={gap_size/self.gap_warning_interval:.2f}"
                    # )
                #     self.last_gap_warning_time[symbol] = current_time
                # elif gap_size <= self.gap_threshold:
                #     self.logger.debug(
                #         f"[{self.exchangename}] {symbol} 작은 시퀀스 갭: "
                #         f"last_id={last_id}, first_id={first_id}, gap={gap_size}"
                #     )

            res = await ob.update(evt)
            if res.is_valid:
                if not st["first_delta_applied"]:
                    st["first_delta_applied"] = True
                    ob.enable_cross_detection()
                last_id = final_id
                applied_count += 1
            else:
                self.logger.error(
                    f"[{self.exchangename}] {symbol} 델타 적용 실패: {res.error_messages}"
                )

        self.buffer_events[symbol] = []
        st["last_update_id"] = last_id

        if applied_count > 0 and self.output_queue:
            final_ob = ob.to_dict()
            try:
                await self.output_queue.put((self.exchangename, final_ob))
                # self.logger.debug(f"[{self.exchangename}] {symbol} 큐 전송 완료")
            except Exception as e:
                self.logger.error(
                    f"[{self.exchangename}] {symbol} 큐 전송 실패: {e}",
                    exc_info=True
                )

    def is_initialized(self, symbol: str) -> bool:
        st = self.sequence_states.get(symbol)
        return bool(st and st["initialized"])

    def get_orderbook(self, symbol: str) -> Optional[OrderBook]:
        return self.orderbooks.get(symbol)

    def clear_symbol(self, symbol: str) -> None:
        self.orderbooks.pop(symbol, None)
        self.sequence_states.pop(symbol, None)
        self.buffer_events.pop(symbol, None)
        self.logger.info(f"[{self.exchangename}] {symbol} 데이터 제거 완료")

    def clear_all(self) -> None:
        self.orderbooks.clear()
        self.sequence_states.clear()
        self.buffer_events.clear()
        self.logger.info(f"[{self.exchangename}] 전체 데이터 제거 완료")