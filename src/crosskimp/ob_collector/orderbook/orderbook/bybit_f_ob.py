# file: core/websocket/exchanges/bybit_future_orderbook_manager.py

import asyncio
import time
import json
from typing import Dict, List, Optional

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import Exchange, EXCHANGE_NAMES_KR, WEBSOCKET_CONFIG

from crosskimp.ob_collector.orderbook.orderbook.base_ob import OrderBookV2, ValidationResult, BaseOrderBookManagerV2
from crosskimp.ob_collector.orderbook.parser.bybit_f_pa import BybitFutureParser

# ============================
# 바이빗 선물 오더북 관련 상수
# ============================
EXCHANGE_CODE = Exchange.BYBIT_FUTURE.value  # 거래소 코드
EXCHANGE_KR = EXCHANGE_NAMES_KR[EXCHANGE_CODE]  # 거래소 한글 이름
BYBIT_FUTURE_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이빗 선물 설정

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class BybitFutureOrderBook(OrderBookV2):
    """
    Bybit 선물 전용 오더북 클래스
    - 시퀀스 기반 검증
    - 3초 스냅샷 처리
    - 델타 업데이트 처리
    """
    def __init__(self, exchangename: str, symbol: str, depth: int = BYBIT_FUTURE_CONFIG["default_depth"]):
        super().__init__(exchangename, symbol, depth)
        self.last_snapshot_time = 0
        self.snapshot_interval = BYBIT_FUTURE_CONFIG["snapshot_interval"]  # 스냅샷 간격 (초)
        self.bids_dict = {}  # price -> quantity
        self.asks_dict = {}  # price -> quantity
        self.last_sequence = None

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

    def to_dict(self) -> dict:
        """오더북 데이터를 딕셔너리로 변환 (부모 클래스 메서드 오버라이드)"""
        return {
            "exchangename": self.exchangename,
            "symbol": self.symbol,
            "bids": self.bids[:10],  # 상위 10개만
            "asks": self.asks[:10],  # 상위 10개만
            "timestamp": self.last_update_time or int(time.time() * 1000),
            "sequence": self.last_sequence  # last_update_id 대신 last_sequence 사용
        }

    def update_orderbook(self, bids: List[List[float]], asks: List[List[float]], 
                        timestamp: Optional[int] = None, sequence: Optional[int] = None,
                        is_snapshot: bool = False) -> None:
        try:
            # 스냅샷인 경우 딕셔너리 초기화
            if is_snapshot:
                self.bids_dict.clear()
                self.asks_dict.clear()
                self.last_snapshot_time = timestamp
                logger.info(f"{EXCHANGE_KR} {self.symbol} 스냅샷 적용 | 매수:{len(bids)}건, 매도:{len(asks)}건")
                
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
            
            # 딕셔너리 크기 로깅
            if len(self.bids_dict) < 10 or len(self.asks_dict) < 10:
                logger.warning(
                    f"{EXCHANGE_KR} {self.symbol} 딕셔너리 뎁스 부족 | "
                    f"매수:{len(self.bids_dict)}건, 매도:{len(self.asks_dict)}건"
                )
            else:
                logger.debug(
                    f"{EXCHANGE_KR} {self.symbol} 딕셔너리 뎁스 | "
                    f"매수:{len(self.bids_dict)}건, 매도:{len(self.asks_dict)}건"
                )
                    
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

class BybitFutureOrderBookManager(BaseOrderBookManagerV2):
    """
    Bybit 선물 오더북 매니저
    - BaseOrderBookManagerV2 상속
    - 시퀀스 기반 검증
    - 스냅샷/델타 처리
    """
    def __init__(self, depth: int = BYBIT_FUTURE_CONFIG["default_depth"]):
        super().__init__(depth)
        self.exchangename = EXCHANGE_CODE
        self.orderbooks: Dict[str, BybitFutureOrderBook] = {}  # BybitFutureOrderBook 사용
        # output_queue는 부모 클래스의 _output_queue를 사용
        self.ws = None  # 웹소켓 연결 객체
        self.parser = BybitFutureParser()  # 파서 초기화
        
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
            # 파서를 사용하여 데이터 파싱
            parsed_data = self.parser.parse_message(json.dumps(data))
            if not parsed_data:
                logger.debug(f"{EXCHANGE_KR} {symbol} 파싱 실패 또는 무시된 메시지")
                return ValidationResult(True)  # 무시해도 에러는 아님
                
            # 파싱된 데이터에서 필요한 정보 추출
            symbol = parsed_data.get("symbol", symbol)
            bids = parsed_data.get("bids", [])
            asks = parsed_data.get("asks", [])
            timestamp = parsed_data.get("timestamp")
            sequence = parsed_data.get("sequence")
            msg_type = parsed_data.get("type", "delta")
            
            # 오더북이 없으면 생성
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = BybitFutureOrderBook(
                    exchangename=self.exchangename,
                    symbol=symbol,
                    depth=self.depth
                )
                # 새로 생성된 오더북에 큐 설정
                if self._output_queue:
                    self.orderbooks[symbol].set_output_queue(self._output_queue)
                logger.info(f"{EXCHANGE_KR} {symbol} 오더북 초기화")
            
            ob = self.orderbooks[symbol]
            
            # 업데이트 처리 여부 확인
            if not ob.should_process_update(sequence, timestamp):
                return ValidationResult(True)  # 무시해도 에러는 아님
            
            # 정렬
            bids.sort(key=lambda x: x[0], reverse=True)  # 가격 내림차순
            asks.sort(key=lambda x: x[0])  # 가격 오름차순
            
            # 원본 데이터 뎁스 로깅
            logger.debug(
                f"{EXCHANGE_KR} {symbol} 원본 데이터 | "
                f"매수:{len(bids)}건, 매도:{len(asks)}건"
            )
            
            # 오더북 업데이트 (depth 제한은 update_orderbook 내부에서 처리)
            is_snapshot = (msg_type == "snapshot")
            ob.update_orderbook(
                bids=bids,
                asks=asks,
                timestamp=timestamp,
                sequence=sequence,
                is_snapshot=is_snapshot  # 스냅샷 여부 전달
            )
            
            # 업데이트 후 뎁스 확인 로깅
            logger.debug(
                f"{EXCHANGE_KR} {symbol} 업데이트 후 | "
                f"매수:{len(ob.bids)}건, 매도:{len(ob.asks)}건, "
                f"딕셔너리 매수:{len(ob.bids_dict)}건, 매도:{len(ob.asks_dict)}건"
            )
            
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
            
            # 큐로 전송
            await ob.send_to_queue()
            
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