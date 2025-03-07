# file: orderbook/upbit_orderbook_manager.py

import asyncio
from typing import Dict, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.orderbook.base_orderbook import OrderBook, ValidationResult
from crosskimp.ob_collector.orderbook.orderbook.base_orderbook_manager import BaseOrderBookManager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class UpbitOrderBookManager(BaseOrderBookManager):
    """
    업비트 오더북 관리자
    - 타임스탬프 기반 시퀀스 관리
    - 스냅샷 + 델타 구조
    """
    def __init__(self, depth: int = 100):
        super().__init__(depth)
        self.exchangename = "upbit"

    def is_initialized(self, symbol: str) -> bool:
        """오더북 스냅샷 초기화 여부"""
        st = self.sequence_states.get(symbol)
        return bool(st and st.get("initialized"))

    def get_sequence(self, symbol: str) -> Optional[int]:
        """현재 시퀀스 반환"""
        st = self.sequence_states.get(symbol)
        return st.get("last_sequence") if st else None

    def update_sequence(self, symbol: str, sequence: int) -> None:
        """시퀀스(timestamp) 업데이트"""
        if symbol in self.sequence_states:
            self.sequence_states[symbol]["last_sequence"] = sequence

    async def initialize_orderbook(self, symbol: str, snapshot: dict) -> ValidationResult:
        """스냅샷으로 오더북 초기화"""
        try:
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = OrderBook(
                    exchangename=self.exchangename,
                    symbol=symbol,
                    depth=self.depth
                )

            # self.logger.debug(
            #     f"[{self.exchangename}] {symbol} 스냅샷 초기화 시작 | "
            #     f"timestamp={snapshot.get('timestamp')}"
            # )

            result = await self.orderbooks[symbol].update(snapshot)
            if not result.is_valid:
                logger.error(
                    f"[{self.exchangename}] {symbol} 스냅샷 적용 실패 | "
                    f"error={result.error_messages}"
                )
                return result

            # 타임스탬프를 시퀀스로 사용
            seq = snapshot.get("sequence", snapshot.get("timestamp", 0))
            self.sequence_states[symbol] = {
                "initialized": True,
                "last_sequence": int(seq)
            }

            # self.logger.debug(
            #     f"[{self.exchangename}] {symbol} 스냅샷 초기화 완료 | "
            #     f"sequence={seq}"
            # )

            return result

        except Exception as e:
            logger.error(
                f"[{self.exchangename}] {symbol} 초기화 중 예외 발생 | "
                f"error={str(e)}",
                exc_info=True
            )
            return ValidationResult(False, [str(e)])

    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """델타 업데이트 처리"""
        try:
            # 버퍼링
            self.buffer_event(symbol, data)
            
            # 초기화 상태 확인
            if not self.is_initialized(symbol):
                logger.debug(f"[{self.exchangename}] {symbol} 초기화 전 버퍼링")
                return ValidationResult(True, [])

            # 버퍼 처리
            orderbook = self.get_orderbook(symbol)
            if not orderbook:
                return ValidationResult(False, ["오더북 객체 없음"])

            current_seq = self.get_sequence(symbol)
            delta_seq = data.get("sequence", data.get("timestamp", 0))

            # 시퀀스 검증
            if delta_seq <= current_seq:
                logger.debug(f"[{self.exchangename}] {symbol} 이전 시퀀스 스킵 | delta_seq={delta_seq}, current_seq={current_seq}")
                return ValidationResult(True, [])

            # 시퀀스 갭 체크
            if delta_seq > current_seq + 1:
                logger.warning(
                    f"[{self.exchangename}] {symbol} 시퀀스 갭 감지 | "
                    f"expected={current_seq + 1}, received={delta_seq}"
                )

            # 델타 적용
            result = await orderbook.update(data)
            if result.is_valid:
                self.update_sequence(symbol, delta_seq)
                # 큐 전송
                if self._output_queue:
                    ob_dict = orderbook.to_dict()
                    await self._output_queue.put((self.exchangename, ob_dict))
                    logger.debug(f"[{self.exchangename}] {symbol} 오더북 큐 전송 완료")

            return result

        except Exception as e:
            logger.error(f"[{self.exchangename}] {symbol} 업데이트 중 오류: {e}")
            return ValidationResult(False, [str(e)])

    def get_orderbook(self, symbol: str) -> Optional[OrderBook]:
        """심볼의 오더북 반환"""
        return self.orderbooks.get(symbol)

    async def buffer_delta(self, symbol: str, delta: dict) -> None:
        """델타 메시지를 버퍼에 저장"""
        if symbol not in self.delta_buffers:
            self.delta_buffers[symbol] = asyncio.Queue()
        await self.delta_buffers[symbol].put(delta)
        logger.debug(
            f"[{self.exchangename}] {symbol} 델타 버퍼링 | "
            f"queue_size={self.delta_buffers[symbol].qsize()}"
        )

    async def process_deltas(self, symbol: str) -> None:
        """버퍼에 저장된 델타 메시지를 순차적으로 처리"""
        lock = self.locks[symbol]
        while self.processing.get(symbol, False):
            delta = await self.delta_buffers[symbol].get()
            async with lock:
                orderbook = self.get_orderbook(symbol)
                if not orderbook:
                    logger.warning(
                        f"[{self.exchangename}] {symbol} 오더북 객체 없음"
                    )
                    self.delta_buffers[symbol].task_done()
                    continue

                current_seq = self.get_sequence(symbol)
                delta_seq = delta.get("sequence")
                if delta_seq is None:
                    logger.warning(
                        f"[{self.exchangename}] {symbol} 시퀀스 누락 | "
                        f"delta={delta}"
                    )
                    self.delta_buffers[symbol].task_done()
                    continue

                if delta_seq <= current_seq:
                    logger.debug(
                        f"[{self.exchangename}] {symbol} 이전 시퀀스 스킵 | "
                        f"delta_seq={delta_seq}, current_seq={current_seq}"
                    )
                    self.delta_buffers[symbol].task_done()
                    continue

                # 시퀀스가 올바른지 확인 (delta_seq > current_seq)
                if delta_seq > current_seq + 1:
                    logger.warning(
                        f"[{self.exchangename}] {symbol} 시퀀스 갭 감지 | "
                        f"expected={current_seq + 1}, "
                        f"received={delta_seq}, "
                        f"gap={delta_seq - current_seq - 1}"
                    )
                    # 시퀀스 누락 시 스냅샷 재요청
                    snap = await self.request_snapshot(symbol)
                    if snap:
                        await self.initialize_orderbook(symbol, snap)
                    self.delta_buffers[symbol].task_done()
                    continue

                # 시퀀스가 정상적인 경우 델타 적용
                result = await orderbook.update(delta)
                if result.is_valid:
                    self.update_sequence(symbol, delta_seq)

                    # 파싱 로깅
                    ob_dict = orderbook.to_dict()
                    logger.info(f"[{self.exchangename}] {symbol} 오더북 업데이트: {ob_dict}")

                    # 큐 전송
                    if self._output_queue:
                        await self._output_queue.put((self.exchangename, ob_dict))
                        logger.debug(
                            f"[{self.exchangename}] {symbol} 오더북 큐 전송 완료"
                        )
                else:
                    # 역전 등 실패 시 스냅샷 재동기화
                    logger.error(
                        f"[{self.exchangename}] {symbol} 업데이트 실패 | "
                        f"error={result.error_messages}"
                    )
                    self.logger.info(
                        f"[{self.exchangename}] {symbol} 스냅샷 재요청 시작"
                    )
                    snap = await self.request_snapshot(symbol)
                    if snap:
                        await self.initialize_orderbook(symbol, snap)

                self.delta_buffers[symbol].task_done()

    async def request_snapshot(self, symbol: str) -> Optional[dict]:
        """
        Upbit REST API로 스냅샷 수집
        - 예: https://api.upbit.com/v1/orderbook?markets=KRW-BTC
        """
        # 이 메서드는 UpbitWebsocket 클래스에서 구현되며, 실제 호출 시 연동이 필요합니다.
        pass

    def clear_symbol(self, symbol: str) -> None:
        """심볼 데이터 제거"""
        self.orderbooks.pop(symbol, None)
        self.sequence_states.pop(symbol, None)
        self.logger.info(
            f"[{self.exchangename}] {symbol} 심볼 데이터 제거 완료"
        )

    def clear_all(self) -> None:
        """전체 데이터 제거"""
        symbols = list(self.orderbooks.keys())
        self.orderbooks.clear()
        self.sequence_states.clear()
        self.logger.info(
            f"[{self.exchangename}] 전체 데이터 제거 완료 | "
            f"symbols={symbols}"
        )