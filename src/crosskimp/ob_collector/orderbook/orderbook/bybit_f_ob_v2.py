# file: core/websocket/exchanges/bybit_future_orderbook_manager.py

import asyncio
import time
import json
from typing import Dict, List, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, get_queue_logger, get_raw_logger
from crosskimp.ob_collector.orderbook.orderbook.base_ob_v2 import BaseOrderBookManagerV2, OrderBookV2, ValidationResult

# 로거 인스턴스 가져오기
logger = get_unified_logger()
queue_logger = get_queue_logger()
raw_logger = get_raw_logger("bybit_future")

class BybitFutureOrderBookV2(OrderBookV2):
    def __init__(self, exchangename: str, symbol: str, depth: int = 50):
        super().__init__(exchangename, symbol, depth)
        self.last_snapshot_time = 0
        self.snapshot_interval = 3  # 3초마다 스냅샷 체크
        self.bids_dict = {}  # price -> quantity
        self.asks_dict = {}  # price -> quantity

    def should_process_update(self, sequence: Optional[int], timestamp: Optional[int]) -> bool:
        current_time = time.time()
        
        # 서비스 재시작 케이스
        if sequence == 1:
            logger.info(f"[Bybit] {self.symbol} 서비스 재시작 감지 (u=1)")
            return True
            
        # 3초 이상 경과
        if timestamp and (timestamp - self.last_snapshot_time) >= 3000:
            logger.debug(f"[Bybit] {self.symbol} 3초 경과 - 스냅샷으로 처리")
            return True
            
        # 시퀀스 검증
        if self.last_sequence is not None:
            if sequence and sequence <= self.last_sequence:
                logger.debug(
                    f"[Bybit] {self.symbol} 이전 시퀀스 무시 | "
                    f"current={sequence}, last={self.last_sequence}"
                )
                return False
                
        return True

    def update_orderbook(self, bids: List[List[float]], asks: List[List[float]], 
                        timestamp: Optional[int] = None, sequence: Optional[int] = None,
                        is_snapshot: bool = False) -> None:
        try:
            # 스냅샷인 경우 딕셔너리 초기화
            if is_snapshot:
                self.bids_dict.clear()
                self.asks_dict.clear()
                self.last_snapshot_time = timestamp
                
            # bids 업데이트
            for price, qty in bids:
                if qty > 0:
                    self.bids_dict[price] = qty
                else:
                    self.bids_dict.pop(price, None)
                    
            # asks 업데이트
            for price, qty in asks:
                if qty > 0:
                    self.asks_dict[price] = qty
                else:
                    self.asks_dict.pop(price, None)
                    
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
                f"[Bybit] {self.symbol} 오더북 업데이트 실패: {str(e)}", 
                exc_info=True
            )

class BybitFutureOrderBookManagerV2(BaseOrderBookManagerV2):
    def __init__(self, depth: int = 50):
        super().__init__(depth)
        self.exchangename = "bybitfuture2"
        self.orderbooks: Dict[str, BybitFutureOrderBookV2] = {}
        
    async def update(self, symbol: str, data: dict) -> ValidationResult:
        try:
            # 로거 초기화 (전역 변수 대신 로컬 변수로 사용)
            from crosskimp.ob_collector.utils.logging.logger import get_queue_logger
            queue_logger = get_queue_logger()
            
            # 오더북이 없으면 생성
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = BybitFutureOrderBookV2(
                    exchangename=self.exchangename,
                    symbol=symbol,
                    depth=self.depth
                )
                logger.info(f"[Bybit] {symbol} 오더북 초기화")
            
            ob = self.orderbooks[symbol]
            msg_type = data.get("type", "")
            ob_data = data.get("data", {})
            
            # 시퀀스 및 타임스탬프
            sequence = ob_data.get("seq")
            timestamp = data.get("ts", int(time.time() * 1000))
            
            # Raw 데이터 로깅 (raw_bybit_future 로거 사용)
            raw_logger.info(
                f"{symbol}|{msg_type}|{sequence}|{timestamp}|{json.dumps(ob_data)}"
            )
            
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
            ob.update_orderbook(
                bids=bids,
                asks=asks,
                timestamp=timestamp,
                sequence=sequence,
                is_snapshot=(msg_type == "snapshot")  # 스냅샷 여부 전달
            )
            
            # 큐 로깅
            if ob.bids and ob.asks:
                best_bid = ob.bids[0][0]
                best_ask = ob.asks[0][0]
                spread = ((best_ask - best_bid) / best_bid) * 100
                
                queue_data = {
                    "exchangename": self.exchangename,
                    "symbol": symbol,
                    "bids": ob.bids[:10],
                    "asks": ob.asks[:10],
                    "timestamp": timestamp,
                    "sequence": sequence
                }
                
                try:
                    queue_logger.info(
                        f"bybitfuture_v2|{symbol}|"
                        f"매수호가={best_bid}|"
                        f"매도호가={best_ask}|"
                        f"스프레드={spread:.4f}%|"
                        f"매수건수={len(ob.bids)}|"
                        f"매도건수={len(ob.asks)}|"
                        f"시퀀스={sequence}|"
                        f"원본데이터={json.dumps(queue_data)}"
                    )
                except Exception as e:
                    logger.error(f"[BybitFuture] {symbol} 큐 로깅 실패: {e}")
                    # 로거 재초기화 시도
                    from crosskimp.ob_collector.utils.logging.logger import get_queue_logger
                    queue_logger = get_queue_logger()
            
            # 메트릭 기록
            self.record_metric(
                event_type="orderbook",
                symbol=symbol
            )
            
            # 큐로 전송
            await ob.send_to_queue()
            
            return ValidationResult(True)
            
        except Exception as e:
            logger.error(f"[Bybit] {symbol} 오더북 업데이트 실패: {str(e)}", exc_info=True)
            return ValidationResult(False, [str(e)])

    def clear_symbol(self, symbol: str):
        self.orderbooks.pop(symbol, None)
        logger.info(f"[Bybit] {symbol} 데이터 제거됨")

    def clear_all(self):
        symbols = list(self.orderbooks.keys())
        self.orderbooks.clear()
        logger.info(f"[Bybit] 전체 데이터 제거됨 | symbols={symbols}")