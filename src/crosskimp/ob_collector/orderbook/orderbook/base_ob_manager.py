# file: orderbook/base_orderbook_manager.py

import asyncio
import time
from typing import Dict, List, Optional

from crosskimp.ob_collector.orderbook.orderbook.base_ob import OrderBook, ValidationResult
from crosskimp.ob_collector.utils.config.constants import EXCHANGE_NAMES_KR
from crosskimp.ob_collector.utils.logging.logger import get_unified_logger

logger = get_unified_logger()

class BaseOrderBookManager:
    """
    모든 거래소 공통으로 쓸 오더북 매니저 추상 클래스.
    스냅샷/델타 파싱, 시퀀스/버퍼 관리 등 핵심 로직을 정의하고,
    구체적인 스냅샷 요청(fetch_snapshot)은 자식에서 구현.
    """
    def __init__(self, depth: int = 100):
        self.depth = depth
        self.orderbooks: Dict[str, OrderBook] = {}
        self.buffer_events: Dict[str, List[dict]] = {}
        self.sequence_states: Dict[str, Dict] = {}
        self.max_buffer_size = 5000
        self.exchangename = ""  # 자식 클래스에서 설정해야 함
        
        # 한글 거래소명 속성 추가
        self.exchange_kr = ""  # 자식 클래스에서 설정해야 함

        # 출력용 큐
        self._output_queue = None

        # 스냅샷 재시도 횟수 관리
        self.max_snapshot_retries = 3
        self.snapshot_retries: Dict[str, int] = {}

    def set_output_queue(self, queue: asyncio.Queue):
        self._output_queue = queue
        logger.info(f"{self.exchange_kr} 출력 큐 설정 완료")

    def is_initialized(self, symbol: str) -> bool:
        st = self.sequence_states.get(symbol)
        return bool(st and st.get("initialized", False))

    async def fetch_snapshot(self, symbol: str) -> Optional[dict]:
        """자식 클래스가 구현해야 함 (REST API 콜)"""
        raise NotImplementedError

    async def initialize_orderbook(self, symbol: str, snapshot: dict) -> ValidationResult:
        """스냅샷으로 오더북 초기화 (자식 클래스에서 구현)"""
        raise NotImplementedError

    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """델타 이벤트 처리 (자식 클래스에서 구현)"""
        raise NotImplementedError

    def get_orderbook(self, symbol: str) -> Optional[OrderBook]:
        return self.orderbooks.get(symbol)

    def buffer_event(self, symbol: str, event: dict):
        """
        이벤트를 버퍼에 추가. 공통 버퍼 로직
        """
        if symbol not in self.buffer_events:
            self.buffer_events[symbol] = []
        if len(self.buffer_events[symbol]) >= self.max_buffer_size:
            self.buffer_events[symbol].pop(0)
        self.buffer_events[symbol].append(event)

    async def _send_to_output_queue(self, symbol: str, ob_dict: dict, top_n: int = 10):
        """
        오더북을 외부 큐로 전달. 
        기본적으로 상위 10개만 잘라서 보냄 (BinanceSpot 예시와 동일).
        """
        if not self._output_queue:
            return
        orderbook = self.orderbooks.get(symbol)
        if not orderbook:
            return

        # 한글 거래소명 가져오기
        exchange_kr = EXCHANGE_NAMES_KR.get(self.exchangename, f"[{self.exchangename}]")

        bids_100 = sorted(ob_dict["bids"], key=lambda x: x[0], reverse=True)
        asks_100 = sorted(ob_dict["asks"], key=lambda x: x[0])

        final_bids = bids_100[:top_n]
        final_asks = asks_100[:top_n]

        final_dict = {
            "exchangename": ob_dict["exchangename"],
            "symbol": ob_dict["symbol"],
            "bids": final_bids,
            "asks": final_asks,
            "timestamp": ob_dict["timestamp"],
            "sequence": ob_dict["sequence"]
        }
        await self._output_queue.put((ob_dict["exchangename"], final_dict))