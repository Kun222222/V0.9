# file: orderbook/upbit_orderbook_manager.py

import asyncio
import time
from typing import Dict, Optional, List

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.orderbook.base_ob_v2 import BaseOrderBookManagerV2, OrderBookV2, ValidationResult
from crosskimp.ob_collector.utils.config.constants import Exchange, EXCHANGE_NAMES_KR
from crosskimp.ob_collector.core.metrics_manager import WebsocketMetricsManager

# ============================
# 업비트 오더북 관련 상수
# ============================
EXCHANGE_CODE = Exchange.UPBIT.value  # 거래소 코드
EXCHANGE_KR = EXCHANGE_NAMES_KR[EXCHANGE_CODE]  # 거래소 한글 이름

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
        self.exchangename = EXCHANGE_CODE

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
        """웹소켓 데이터로 스냅샷으로 오더북 초기화"""
        try:
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
            
            if bids and asks:
                max_bid = max(bid[0] for bid in bids)
                min_ask = min(ask[0] for ask in asks)
                if max_bid >= min_ask:
                    is_valid = False
                    error_messages.append(f"가격 역전 발생: 최고매수가({max_bid}) >= 최저매도가({min_ask})")
                    # 가격 역전 메트릭 기록
                    self.record_metric("error", error_type="price_inversion")
                    logger.warning(f"{EXCHANGE_KR} {symbol} {error_messages[0]}")
            
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
                
                # 데이터 크기 메트릭 업데이트
                data_size = len(str(data))  # 간단한 크기 측정
                self.record_metric("bytes", size=data_size)
                
                # DEBUG 레벨로 변경하여 스팸 로깅 감소
                logger.debug(f"{EXCHANGE_KR} {symbol} 오더북 업데이트 | "
                           f"매수:{len(bids)}건, 매도:{len(asks)}건, "
                           f"시퀀스:{converted['sequence']}")
            else:
                # 검증 실패 메트릭 기록
                self.record_metric("error", error_type="validation_failed")
                
            return ValidationResult(is_valid, error_messages)

        except Exception as e:
            # 예외 발생 메트릭 기록
            self.record_metric("error", error_type="exception")
            logger.error(
                f"{EXCHANGE_KR} {symbol} 초기화 중 예외 발생 | "
                f"error={str(e)} | data={str(data)[:200]}...",
                exc_info=True
            )
            return ValidationResult(False, [str(e)])

    async def update(self, symbol: str, data: dict) -> ValidationResult:
        """실시간 오더북 업데이트 - 업비트는 모든 메시지가 스냅샷이므로 initialize_orderbook과 동일하게 처리"""
        return await self.initialize_orderbook(symbol, data)

    def _convert_upbit_format(self, data: dict) -> dict:
        """업비트 데이터 형식으로 변환"""
        # 이미 변환된 형식인지 확인
        if isinstance(data.get('bids', []), list) and isinstance(data.get('asks', []), list):
            bids = [[item['price'], item['size']] for item in data['bids']]
            asks = [[item['price'], item['size']] for item in data['asks']]
        else:
            # 원본 데이터에서 변환
            orderbook_units = data.get('orderbook_units', [])
            bids = [[float(unit['bid_price']), float(unit['bid_size'])] for unit in orderbook_units]
            asks = [[float(unit['ask_price']), float(unit['ask_size'])] for unit in orderbook_units]
        
        return {
            'bids': bids,
            'asks': asks,
            'timestamp': data.get('timestamp'),
            'sequence': data.get('sequence')
        }

    def get_orderbook(self, symbol: str) -> Optional[OrderBookV2]:
        """심볼의 오더북 반환"""
        return self.orderbooks.get(symbol)

    def clear_symbol(self, symbol: str) -> None:
        """심볼 데이터 제거"""
        self.orderbooks.pop(symbol, None)
        logger.info(f"{EXCHANGE_KR} {symbol} 심볼 데이터 제거 완료")

    def clear_all(self) -> None:
        """전체 데이터 제거"""
        symbols = list(self.orderbooks.keys())
        self.orderbooks.clear()
        logger.info(f"{EXCHANGE_KR} 전체 데이터 제거 완료 | symbols={symbols}")