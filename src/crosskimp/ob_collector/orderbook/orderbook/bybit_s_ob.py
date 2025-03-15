import time
import json
import logging
import asyncio
from typing import Dict, Optional, List

from crosskimp.logger.logger import get_unified_logger, get_queue_logger
from crosskimp.config.ob_constants import Exchange, EXCHANGE_NAMES_KR, WEBSOCKET_CONFIG

from crosskimp.ob_collector.orderbook.orderbook.base_ob import BaseOrderBookManagerV2, OrderBookV2, ValidationResult

# ============================
# 바이빗 오더북 관련 상수
# ============================
EXCHANGE_CODE = Exchange.BYBIT.value  # 거래소 코드
EXCHANGE_KR = EXCHANGE_NAMES_KR[EXCHANGE_CODE]  # 거래소 한글 이름
BYBIT_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이빗 설정

# 로거 인스턴스 가져오기
logger = get_unified_logger()
queue_logger = get_queue_logger()

class BybitOrderBook(OrderBookV2):
    """
    Bybit 전용 오더북 클래스
    - 시퀀스 기반 검증
    - 3초 스냅샷 처리
    - 델타 업데이트 처리
    """
    def __init__(self, exchangename: str, symbol: str, depth: int = BYBIT_CONFIG["default_depth"]):
        super().__init__(exchangename, symbol, depth)
        self.last_snapshot_time = 0
        self.snapshot_interval = BYBIT_CONFIG["snapshot_interval"]  # 스냅샷 간격 (초)
        self.bids_dict = {}  # price -> quantity
        self.asks_dict = {}  # price -> quantity

    def should_process_update(self, sequence: Optional[int], timestamp: Optional[int]) -> bool:
        """
        업데이트 처리 여부 결정
        - sequence가 1이면 무조건 처리 (서비스 재시작)
        - 3초 이상 경과하면 스냅샷으로 처리
        - sequence가 이전보다 작거나 같으면 무시
        """
        current_time = time.time()
        
        # 서비스 재시작 케이스
        if sequence == 1:
            logger.info(f"{EXCHANGE_KR} {self.symbol} 서비스 재시작 감지 (u=1)")
            return True
            
        # 3초 이상 경과
        if timestamp and (timestamp - self.last_snapshot_time) >= 3000:  # 3초 = 3000ms
            logger.debug(f"{EXCHANGE_KR} {self.symbol} 3초 경과 - 스냅샷으로 처리")
            return True
            
        # 시퀀스 검증
        if self.last_sequence is not None:
            if sequence and sequence <= self.last_sequence:
                logger.debug(
                    f"{EXCHANGE_KR} {self.symbol} 이전 시퀀스 무시 | "
                    f"current={sequence}, last={self.last_sequence}"
                )
                return False
                
        return True

    async def send_to_queue(self) -> None:
        """큐로 데이터 전송"""
        if self.output_queue:
            try:
                data = self.to_dict()
                await self.output_queue.put((self.exchangename, data))
            except Exception as e:
                logger.error(
                    f"{EXCHANGE_KR} {self.symbol} 큐 전송 실패: {str(e)}", 
                    exc_info=True
                )

    def update_orderbook(self, bids: List[List[float]], asks: List[List[float]], 
                        timestamp: Optional[int] = None, sequence: Optional[int] = None,
                        is_snapshot: bool = False) -> None:
        try:
            # 스냅샷인 경우 딕셔너리 초기화
            if is_snapshot:
                self.bids_dict.clear()
                self.asks_dict.clear()
                self.last_snapshot_time = timestamp
                
            # 임시 딕셔너리에 복사 (가격 역전 검사를 위해)
            temp_bids_dict = self.bids_dict.copy()
            temp_asks_dict = self.asks_dict.copy()
                
            # bids 업데이트 및 가격 역전 검사
            for price, qty in bids:
                if qty > 0:
                    # 새로운 매수 호가가 기존 매도 호가보다 높거나 같은 경우 검사
                    invalid_asks = [ask_price for ask_price in temp_asks_dict.keys() if ask_price <= price]
                    if invalid_asks and not is_snapshot:  # 스냅샷이 아닌 경우에만 검사
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
                    invalid_bids = [bid_price for bid_price in temp_bids_dict.keys() if bid_price >= price]
                    if invalid_bids and not is_snapshot:  # 스냅샷이 아닌 경우에만 검사
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
            if sequence:
                self.last_sequence = sequence
                
        except Exception as e:
            logger.error(
                f"{EXCHANGE_KR} {self.symbol} 오더북 업데이트 실패: {str(e)}", 
                exc_info=True
            )

class BybitSpotOrderBookManager(BaseOrderBookManagerV2):
    def __init__(self, depth: int = BYBIT_CONFIG["default_depth"]):
        super().__init__(depth)
        self.exchangename = EXCHANGE_CODE
        self.orderbooks: Dict[str, BybitOrderBook] = {}  # BybitOrderBook 사용
        # output_queue는 부모 클래스의 _output_queue를 사용
        self.ws = None  # 웹소켓 연결 객체
        
    def set_websocket(self, ws):
        """웹소켓 연결 설정"""
        self.ws = ws
        logger.info(f"{EXCHANGE_KR} 웹소켓 연결 설정 완료")
        
    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """
        출력 큐 설정 (부모 클래스 메서드 오버라이드)
        """
        self._output_queue = queue
        # 각 오더북 객체에도 큐 설정
        for ob in self.orderbooks.values():
            ob.set_output_queue(queue)
        logger.info(f"{EXCHANGE_KR} 오더북 매니저 출력 큐 설정 완료 (큐 ID: {id(queue)})")
        
    async def update(self, symbol: str, data: dict) -> ValidationResult:
        try:
            # 오더북이 없으면 생성
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = BybitOrderBook(
                    exchangename=self.exchangename,
                    symbol=symbol,
                    depth=self.depth
                )
                # 새로 생성된 오더북에 큐 설정
                if self._output_queue:
                    self.orderbooks[symbol].set_output_queue(self._output_queue)
                logger.info(f"{EXCHANGE_KR} {symbol} 오더북 초기화")
            
            ob = self.orderbooks[symbol]
            msg_type = data.get("type", "")
            ob_data = data.get("data", {})
            
            # 시퀀스 및 타임스탬프
            sequence = ob_data.get("u")
            timestamp = data.get("ts", int(time.time() * 1000))
            
            # 업데이트 처리 여부 확인
            if not ob.should_process_update(sequence, timestamp):
                return ValidationResult(True)  # 무시해도 에러는 아님
            
            # bids 업데이트
            bids = []
            for price_str, qty_str in ob_data.get("b", []):
                price = float(price_str)
                qty = float(qty_str)
                if qty > 0:  # qty가 0이면 무시 (삭제)
                    bids.append([price, qty])
            
            # asks 업데이트
            asks = []
            for price_str, qty_str in ob_data.get("a", []):
                price = float(price_str)
                qty = float(qty_str)
                if qty > 0:  # qty가 0이면 무시 (삭제)
                    asks.append([price, qty])
            
            # 정렬
            bids.sort(key=lambda x: x[0], reverse=True)  # 가격 내림차순
            asks.sort(key=lambda x: x[0])  # 가격 오름차순
            
            # depth 제한
            bids = bids[:self.depth]
            asks = asks[:self.depth]
            
            # 오더북 업데이트
            is_snapshot = (msg_type == "snapshot")
            ob.update_orderbook(
                bids=bids,
                asks=asks,
                timestamp=timestamp,
                sequence=sequence,
                is_snapshot=is_snapshot  # 스냅샷 여부 전달
            )
            
            # 스냅샷 메시지인 경우 큐로 직접 전달 - 스냅샷은 큐로 전송하지 않도록 주석 처리
            """
            if is_snapshot and self._output_queue:
                snapshot_data = {
                    "exchangename": self.exchangename,
                    "symbol": symbol,
                    "bids": bids,
                    "asks": asks,
                    "timestamp": timestamp,
                    "sequence": sequence
                }
                await self._output_queue.put((self.exchangename, snapshot_data))
                logger.info(f"{EXCHANGE_KR} {symbol} 스냅샷 메시지 큐로 전달")
            """
            
            # 바이빗 특화 검증: bid/ask가 모두 있는 경우에만 가격 순서 체크
            if ob.bids and ob.asks:
                best_bid = ob.bids[0][0]
                best_ask = ob.asks[0][0]
                
                # 가격 역전 검사 (이미 update_orderbook에서 가격 역전을 방지하지만, 혹시 모를 상황을 대비해 로깅)
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
            
            # 큐로 전송 (직접 전송 방식으로 변경)
            if self._output_queue and not is_snapshot:  # 스냅샷은 이미 위에서 전송했으므로 중복 방지
                orderbook_data = {
                    "exchangename": self.exchangename,
                    "symbol": symbol,
                    "bids": ob.bids[:10],
                    "asks": ob.asks[:10],
                    "timestamp": timestamp,
                    "sequence": sequence
                }
                await self._output_queue.put((self.exchangename, orderbook_data))
                # 디버그 로깅 추가
                logger.debug(f"{EXCHANGE_KR} {symbol} 오더북 데이터 큐 전송 완료 (큐 ID: {id(self._output_queue)})")
            
            # DEBUG 레벨로 변경하여 스팸 로깅 감소
            logger.debug(
                f"{EXCHANGE_KR} {symbol} 오더북 업데이트 | "
                f"매수:{len(ob.bids)}건, 매도:{len(ob.asks)}건, "
                f"시퀀스:{sequence}"
            )
            
            return ValidationResult(True)
            
        except Exception as e:
            logger.error(f"{EXCHANGE_KR} {symbol} 오더북 업데이트 실패: {str(e)}", exc_info=True)
            return ValidationResult(False, [f"업데이트 실패: {str(e)}"])

    def clear_symbol(self, symbol: str):
        self.orderbooks.pop(symbol, None)
        logger.info(f"{EXCHANGE_KR} {symbol} 데이터 제거됨")

    def clear_all(self):
        symbols = list(self.orderbooks.keys())
        self.orderbooks.clear()
        logger.info(f"{EXCHANGE_KR} 전체 데이터 제거됨 | symbols={symbols}") 