# file: orderbook/upbit_orderbook_manager.py

import asyncio
import time
from typing import Dict, Optional, List

from crosskimp.logger.logger import get_unified_logger

from crosskimp.ob_collector.orderbook.orderbook.base_ob import BaseOrderBookManagerV2, OrderBookV2, ValidationResult
from crosskimp.config.ob_constants import Exchange, EXCHANGE_NAMES_KR, WEBSOCKET_CONFIG

from crosskimp.ob_collector.core.metrics_manager import WebsocketMetricsManager
from crosskimp.ob_collector.cpp.cpp_interface import send_orderbook_to_cpp

# ============================
# 업비트 오더북 관련 상수
# ============================
EXCHANGE_CODE = Exchange.UPBIT.value  # 거래소 코드
EXCHANGE_KR = EXCHANGE_NAMES_KR[EXCHANGE_CODE]  # 거래소 한글 이름
UPBIT_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 업비트 설정

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class UpbitOrderBook(OrderBookV2):
    """
    업비트 전용 오더북 클래스
    - 타임스탬프 기반 시퀀스 관리
    - 전체 스냅샷 방식 처리
    """
    def __init__(self, exchangename: str, symbol: str, depth: int = UPBIT_CONFIG["default_depth"]):
        super().__init__(exchangename, symbol, depth)
        # 업비트 특화 속성
        self.last_timestamp = 0
        self.bids_dict = {}  # price -> quantity
        self.asks_dict = {}  # price -> quantity

    def should_process_update(self, timestamp: Optional[int]) -> bool:
        """
        업데이트 처리 여부 결정
        - 타임스탬프가 이전보다 작거나 같으면 무시
        """
        if self.last_timestamp is not None and timestamp:
            if timestamp <= self.last_timestamp:
                logger.debug(
                    f"{EXCHANGE_KR} {self.symbol} 이전 타임스탬프 무시 | "
                    f"current={timestamp}, last={self.last_timestamp}"
                )
                return False
        return True

    def update_orderbook(self, bids: List[List[float]], asks: List[List[float]], 
                        timestamp: Optional[int] = None, sequence: Optional[int] = None) -> None:
        """
        오더북 데이터 업데이트 (업비트 전용)
        """
        try:
            # 타임스탬프 검증
            if timestamp and not self.should_process_update(timestamp):
                return
                
            # 딕셔너리 초기화 (업비트는 항상 전체 스냅샷)
            self.bids_dict.clear()
            self.asks_dict.clear()
            
            # 딕셔너리에 데이터 추가
            for price, qty in bids:
                if qty > 0:
                    self.bids_dict[price] = qty
                    
            for price, qty in asks:
                if qty > 0:
                    self.asks_dict[price] = qty
            
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
                
            # C++로 데이터 직접 전송
            asyncio.create_task(self.send_to_cpp())
                
        except Exception as e:
            logger.error(
                f"{EXCHANGE_KR} {self.symbol} 오더북 업데이트 실패: {str(e)}", 
                exc_info=True
            )

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
        self.orderbooks: Dict[str, UpbitOrderBook] = {}  # UpbitOrderBook 사용

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
                self.orderbooks[symbol] = UpbitOrderBook(
                    exchangename=self.exchangename,
                    symbol=symbol,
                    depth=self.depth
                )
                
                # 새로 생성된 오더북에 큐 설정
                if self._output_queue:
                    self.orderbooks[symbol].set_output_queue(self._output_queue)
            
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
                # UpbitOrderBook 클래스의 update_orderbook 메서드 호출
                self.orderbooks[symbol].update_orderbook(
                    bids=bids,
                    asks=asks,
                    timestamp=converted["timestamp"],
                    sequence=converted["sequence"]
                )
                
                # 큐로 데이터 전송 (필요한 경우)
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

    def get_orderbook(self, symbol: str) -> Optional[UpbitOrderBook]:
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