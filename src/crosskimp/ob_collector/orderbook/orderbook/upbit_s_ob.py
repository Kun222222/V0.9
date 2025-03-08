# file: orderbook/upbit_orderbook_manager.py

import asyncio
import time
from typing import Dict, Optional, List

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.orderbook.base_ob_v2 import BaseOrderBookManagerV2, OrderBookV2, ValidationResult
from crosskimp.ob_collector.utils.config.constants import EXCHANGE_NAMES_KR
from crosskimp.ob_collector.orderbook.manager.metrics_manager import WebsocketMetricsManager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class UpbitOrderBookManager(BaseOrderBookManagerV2):
    """
    업비트 오더북 관리자 V2
    - 웹소켓으로부터 스냅샷 형태의 데이터 수신
    - 타임스탬프 기반 시퀀스 관리
    - 15단계 → 10단계 변환
    """
    def __init__(self, depth: int = 15):
        super().__init__(depth)
        self.exchangename = "upbit"

    async def start(self):
        """오더북 매니저 시작"""
        await self.initialize()  # 메트릭 로깅 시작

    def is_initialized(self, symbol: str) -> bool:
        """오더북 초기화 여부 확인"""
        return symbol in self.orderbooks

    def get_sequence(self, symbol: str) -> Optional[int]:
        """현재 시퀀스(타임스탬프) 반환"""
        ob = self.orderbooks.get(symbol)
        return ob.last_update_id if ob else None

    def update_sequence(self, symbol: str, sequence: int) -> None:
        """시퀀스(타임스탬프) 업데이트"""
        if symbol in self.orderbooks:
            self.orderbooks[symbol].last_update_id = sequence

    async def initialize_orderbook(self, symbol: str, data: dict) -> ValidationResult:
        """웹소켓 데이터로 오더북 초기화"""
        try:
            start_time = time.time()
            
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = OrderBookV2(
                    exchangename=self.exchangename,
                    symbol=symbol,
                    depth=self.depth
                )

            # 데이터 변환
            converted = self._convert_upbit_format(data)
            bids = converted["bids"]
            asks = converted["asks"]
            
            # 업비트 특화 검증: bid/ask가 모두 있는 경우에만 가격 순서 체크
            is_valid = True
            error_messages = []
            
            if bids and asks and max(bid[0] for bid in bids) >= min(ask[0] for ask in asks):
                is_valid = False
                error_messages.append("Invalid price ordering")
                # 가격 역전 메트릭 기록
                self.record_metric("error", error_type="price_inversion")
            
            if is_valid:
                self.orderbooks[symbol].update_orderbook(
                    bids=bids,
                    asks=asks,
                    timestamp=converted["timestamp"],
                    sequence=converted["sequence"]
                )
                await self.orderbooks[symbol].send_to_queue()
                
                # 오더북 카운트 메트릭 업데이트
                self.record_metric("orderbook", symbol=symbol)
                
                # 처리 시간 기록
                processing_time = (time.time() - start_time) * 1000  # ms로 변환
                self.record_metric("processing_time", time_ms=processing_time)
                
                # 데이터 크기 메트릭 업데이트
                data_size = len(str(data))  # 간단한 크기 측정
                self.record_metric("bytes", size=data_size)
            else:
                # 검증 실패 메트릭 기록
                self.record_metric("error", error_type="validation_failed")
                
            return ValidationResult(is_valid, error_messages)

        except Exception as e:
            # 예외 발생 메트릭 기록
            self.record_metric("error", error_type="exception")
            logger.error(
                f"[{EXCHANGE_NAMES_KR[self.exchangename]}] {symbol} 초기화 중 예외 발생 | "
                f"error={str(e)}",
                exc_info=True
            )
            return ValidationResult(False, [str(e)])

    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """실시간 오더북 업데이트 - 업비트는 모든 메시지가 스냅샷이므로 initialize_orderbook과 동일하게 처리"""
        return await self.initialize_orderbook(symbol, data)

    def _convert_upbit_format(self, data: dict) -> dict:
        """업비트 웹소켓 데이터를 기본 오더북 형식으로 변환"""
        try:
            symbol = data.get("code", "").replace("KRW-", "")
            bids = []
            asks = []
            
            # 상위 depth개만 처리
            for unit in data.get("orderbook_units", [])[:self.depth]:
                bid_price = float(unit.get("bid_price", 0))
                bid_size = float(unit.get("bid_size", 0))
                ask_price = float(unit.get("ask_price", 0))
                ask_size = float(unit.get("ask_size", 0))
                
                if bid_price > 0 and bid_size > 0:
                    bids.append([bid_price, bid_size])
                if ask_price > 0 and ask_size > 0:
                    asks.append([ask_price, ask_size])

            return {
                "exchangename": self.exchangename,
                "symbol": symbol,
                "bids": bids,
                "asks": asks,
                "timestamp": data.get("timestamp", 0),
                "sequence": data.get("timestamp", 0)
            }
            
        except Exception as e:
            logger.error(
                f"[{EXCHANGE_NAMES_KR[self.exchangename]}] 데이터 변환 중 오류 | "
                f"error={str(e)}",
                exc_info=True
            )
            raise

    def get_orderbook(self, symbol: str) -> Optional[OrderBookV2]:
        """심볼의 오더북 반환"""
        return self.orderbooks.get(symbol)

    def clear_symbol(self, symbol: str) -> None:
        """심볼 데이터 제거"""
        self.orderbooks.pop(symbol, None)
        logger.info(
            f"[{EXCHANGE_NAMES_KR[self.exchangename]}] {symbol} 심볼 데이터 제거 완료"
        )

    def clear_all(self) -> None:
        """전체 데이터 제거"""
        symbols = list(self.orderbooks.keys())
        self.orderbooks.clear()
        logger.info(
            f"[{EXCHANGE_NAMES_KR[self.exchangename]}] 전체 데이터 제거 완료 | "
            f"symbols={symbols}"
        )