# file: core/websocket/exchanges/bybit_future_orderbook_manager.py

import asyncio
import time
from typing import Dict, List, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.orderbook.base_orderbook import OrderBook, ValidationResult
from crosskimp.ob_collector.orderbook.orderbook.base_orderbook_manager import BaseOrderBookManager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class BybitFutureOrderBookManager(BaseOrderBookManager):
    """
    Bybit 선물 오더북 매니저
    - snapshot + delta 시퀀스(seq) 기반
    - 이벤트 버퍼링
    - 스냅샷 재시도 로직(snapshot_retries)
    """
    def __init__(self, depth: int = 10):
        super().__init__(depth)
        self.exchangename = "bybitfuture"
        self.logger = logger
        self.max_buffer_size = 1000
        self.max_snapshot_retries = 3
        
        # 심볼별 오더북 & 상태
        self.orderbooks: Dict[str, OrderBook] = {}
        self.sequence_states: Dict[str, Dict] = {}
        # 버퍼 (스냅샷 미적용 상태에서 들어온 delta 등을 임시 저장)
        self.buffer_events: Dict[str, List[dict]] = {}
        
        # 스냅샷 재시도
        self.snapshot_retries: Dict[str, int] = {}

    def is_initialized(self, symbol: str) -> bool:
        """스냅샷 초기화 여부"""
        st = self.sequence_states.get(symbol)
        return bool(st and st.get("initialized"))

    def get_orderbook(self, symbol: str) -> Optional[OrderBook]:
        """오더북 반환"""
        return self.orderbooks.get(symbol)

    def buffer_event(self, symbol: str, event: dict) -> None:
        """스냅샷 전후 들어온 이벤트 버퍼링"""
        if symbol not in self.buffer_events:
            self.buffer_events[symbol] = []
        if len(self.buffer_events[symbol]) >= self.max_buffer_size:
            self.logger.warning(
                f"[{self.exchangename}][{symbol}] 버퍼 크기 초과로 이벤트 제거 | "
                f"buffer_size={len(self.buffer_events[symbol])}, "
                f"max_size={self.max_buffer_size}"
            )
            self.buffer_events[symbol].pop(0)
        self.buffer_events[symbol].append(event)
        self.logger.debug(
            f"[{self.exchangename}][{symbol}] 이벤트 버퍼링 | "
            f"buffer_size={len(self.buffer_events[symbol])}, "
            f"event_type={event.get('type', 'unknown')}"
        )

    def get_valid_events(self, symbol: str) -> List[dict]:
        """
        버퍼된 이벤트 중, last_sequence보다 큰 seq만
        seq 오름차순 정렬 후 반환
        """
        if symbol not in self.buffer_events or symbol not in self.sequence_states:
            return []

        st = self.sequence_states[symbol]
        last_seq = st.get("last_sequence", 0)

        events = self.buffer_events[symbol]
        sorted_events = sorted(events, key=lambda x: x.get("sequence", 0))

        self.logger.debug(
            f"[{self.exchangename}][{symbol}] 버퍼 이벤트 처리 시작 | "
            f"events={len(events)}, last_seq={last_seq}"
        )

        valid = []
        new_seq = last_seq
        for evt in sorted_events:
            seq = evt.get("sequence", 0)
            if seq > new_seq:
                valid.append(evt)
                new_seq = seq
            else:
                self.logger.debug(
                    f"[{self.exchangename}][{symbol}] 이미 처리된 이벤트 스킵 | "
                    f"sequence={seq}, last_seq={last_seq}"
                )
        
        self.buffer_events[symbol] = []

        if valid:
            self.logger.debug(
                f"[{self.exchangename}][{symbol}] 유효 이벤트 처리 완료 | "
                f"valid_count={len(valid)}, total={len(events)}, "
                f"last_seq={new_seq}"
            )

        return valid

    async def initialize_orderbook(self, symbol: str, snapshot: dict) -> ValidationResult:
        """
        스냅샷 적용
        - OrderBook.update() 내부에서 depth/정렬 처리
        """
        try:
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = OrderBook(self.exchangename, symbol, self.depth)

            self.logger.info(
                f"[{self.exchangename}][{symbol}] 스냅샷 초기화 시작 | "
                f"sequence={snapshot.get('sequence')}, "
                f"bids={len(snapshot.get('bids', []))}, "
                f"asks={len(snapshot.get('asks', []))}"
            )

            res = await self.orderbooks[symbol].update(snapshot)
            if not res.is_valid:
                self.logger.error(
                    f"[{self.exchangename}][{symbol}] 스냅샷 적용 실패 | "
                    f"error={res.error_messages}"
                )
                self.snapshot_retries[symbol] = self.snapshot_retries.get(symbol, 0) + 1
                if self.snapshot_retries[symbol] >= self.max_snapshot_retries:
                    self.logger.error(
                        f"[{self.exchangename}][{symbol}] 스냅샷 재시도 횟수 초과 | "
                        f"retries={self.snapshot_retries[symbol]}, "
                        f"max_retries={self.max_snapshot_retries}"
                    )
                return res

            seq = snapshot.get("sequence", 0)
            self.sequence_states[symbol] = {
                "last_sequence": seq,
                "initialized": True
            }
            self.snapshot_retries[symbol] = 0

            self.logger.info(
                f"[{self.exchangename}][{symbol}] 스냅샷 초기화 완료 | "
                f"sequence={seq}"
            )
            return res

        except Exception as e:
            self.logger.error(
                f"[{self.exchangename}][{symbol}] 초기화 중 예외 발생 | "
                f"error={str(e)}",
                exc_info=True
            )
            return ValidationResult(False, [f"[{symbol}] 스냅샷 초기화 중 오류: {e}"])

    def update_sequence(self, symbol: str, seq: int) -> None:
        """시퀀스 업데이트"""
        if symbol in self.sequence_states:
            self.sequence_states[symbol]["last_sequence"] = seq
            # self.logger.debug(
            #     f"[{self.exchangename}][{symbol}] 시퀀스 갱신 | "
            #     f"new_seq={seq}"
            # )

    def clear_symbol(self, symbol: str) -> None:
        """특정 심볼 데이터 초기화"""
        self.orderbooks.pop(symbol, None)
        self.sequence_states.pop(symbol, None)
        self.buffer_events.pop(symbol, None)
        self.snapshot_retries.pop(symbol, None)
        self.logger.info(
            f"[{self.exchangename}][{symbol}] 심볼 데이터 제거 완료"
        )

    def clear_all(self) -> None:
        """전체 초기화"""
        symbols = list(self.orderbooks.keys())
        self.orderbooks.clear()
        self.sequence_states.clear()
        self.buffer_events.clear()
        self.snapshot_retries.clear()
        self.logger.info(
            f"[{self.exchangename}] 전체 데이터 제거 완료 | "
            f"symbols={symbols}"
        )