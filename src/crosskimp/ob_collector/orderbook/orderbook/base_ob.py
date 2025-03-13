# file: orderbook/base_orderbook.py

import time
import asyncio
from dataclasses import dataclass
from typing import List, Dict, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.utils.config.constants import EXCHANGE_NAMES_KR

# 로거 인스턴스 가져오기
logger = get_unified_logger()

@dataclass
class ValidationResult:
    is_valid: bool
    error_messages: List[str]

@dataclass
class UpdateResult:
    is_valid: bool
    error_messages: List[str]

class OrderBookError(Exception):
    pass

class OrderBookValidationError(OrderBookError):
    pass

class OrderBook:
    """
    depth 기본값을 100으로 두되,
    특정 거래소에서 500으로 쓰고 싶으면 생성 시 depth=500을 넘김.
    """
    def __init__(self, exchangename: str, symbol: str, depth: int = 100, logger=None):
        self.exchangename = exchangename
        self.symbol = symbol
        self.depth = depth  # 기본 100
        self.logger = logger if logger else get_unified_logger()
        
        # 한글 거래소명 가져오기
        self.exchange_kr = EXCHANGE_NAMES_KR.get(self.exchangename, f"[{self.exchangename}]")

        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}

        self.last_update_id = None
        self.last_update_time = None
        self.message_type = "snapshot"

        self.ignore_cross_detection = True
        self.cross_count = 0
        self.last_cross_time = 0.0
        self.cross_cooldown_sec = 0.5
        self.cross_threshold = 3

        self.output_queue: Optional[asyncio.Queue] = None

    def set_output_queue(self, queue: asyncio.Queue):
        self.output_queue = queue

    def enable_cross_detection(self):
        self.ignore_cross_detection = False
        self.logger.info(f"{self.exchange_kr} {self.symbol} 역전감지 활성화")

    def _get_best_prices(self) -> tuple[Optional[float], Optional[float]]:
        """현재 최고 매수호가와 최저 매도호가 반환"""
        best_bid = max(self.bids.keys()) if self.bids else None
        best_ask = min(self.asks.keys()) if self.asks else None
        return best_bid, best_ask

    def _validate_new_price(self, price: float, is_bid: bool, new_bids: Dict[float, float], new_asks: Dict[float, float]) -> bool:
        """
        새로운 호가가 들어올 때 기존 호가들의 유효성을 검사하고 필요한 경우 제거
        """
        if self.ignore_cross_detection:
            return True

        try:
            if is_bid:
                # 1. 새로운 매수호가가 기존 매도호가보다 높거나 같은 경우
                invalid_asks = [ask_price for ask_price in new_asks.keys() if ask_price <= price]
                if invalid_asks:
                    # 가격 역전이 발생하는 매도호가들 제거
                    for ask_price in invalid_asks:
                        new_asks.pop(ask_price)
                        self.logger.debug(
                            f"{self.exchange_kr} {self.symbol} 매수호가({price}) 추가로 인해 낮은 매도호가({ask_price}) 제거"
                        )
            else:
                # 1. 새로운 매도호가가 기존 매수호가보다 낮거나 같은 경우
                invalid_bids = [bid_price for bid_price in new_bids.keys() if bid_price >= price]
                if invalid_bids:
                    # 가격 역전이 발생하는 매수호가들 제거
                    for bid_price in invalid_bids:
                        new_bids.pop(bid_price)
                        self.logger.debug(
                            f"{self.exchange_kr} {self.symbol} 매도호가({price}) 추가로 인해 높은 매수호가({bid_price}) 제거"
                        )

            return True

        except Exception as e:
            self.logger.error(f"{self.exchange_kr} {self.symbol} 가격 검증 중 오류: {e}", exc_info=True)
            return False

    def _maintain_depth(self) -> None:
        """매수/매도 정렬 & 상위 depth 유지"""
        try:
            # bids 내림차순, asks 오름차순 정렬
            sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)
            sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])

            # 상위 depth만 유지
            self.bids = dict(sorted_bids[:self.depth])
            self.asks = dict(sorted_asks[:self.depth])

        except Exception as e:
            self.logger.error(f"{self.exchange_kr} {self.symbol} 오더북 깊이 유지 중 오류: {e}", exc_info=True)

    async def update(self, data: dict) -> UpdateResult:
        """오더북 업데이트 후, 필요 시 큐에 전달"""
        try:
            msg_type = data.get("type", "delta")
            self.message_type = msg_type

            if msg_type == "snapshot":
                self.logger.info(f"{self.exchange_kr} {self.symbol} 스냅샷 초기화")
                self.bids.clear()
                self.asks.clear()
                self.ignore_cross_detection = True

            # 이전 상태 백업
            prev_bids = self.bids.copy()
            prev_asks = self.asks.copy()

            # 임시 딕셔너리에 업데이트 적용
            new_bids = prev_bids.copy()
            new_asks = prev_asks.copy()

            # bids 업데이트 검증 및 적용
            for (px, qty) in data.get("bids", []):
                if qty > 0:
                    if not self.ignore_cross_detection:
                        if not self._validate_new_price(px, is_bid=True, new_bids=new_bids, new_asks=new_asks):
                            continue
                    new_bids[px] = qty
                else:
                    new_bids.pop(px, None)

            # asks 업데이트 검증 및 적용
            for (px, qty) in data.get("asks", []):
                if qty > 0:
                    if not self.ignore_cross_detection:
                        if not self._validate_new_price(px, is_bid=False, new_bids=new_bids, new_asks=new_asks):
                            continue
                    new_asks[px] = qty
                else:
                    new_asks.pop(px, None)

            # 임시 상태로 교체
            self.bids = new_bids
            self.asks = new_asks

            # depth 유지 및 최종 검증
            self._maintain_depth()
            validation = self._validate_orderbook()
            if not validation.is_valid:
                # rollback
                self.bids = prev_bids
                self.asks = prev_asks
                self.logger.error(f"{self.exchange_kr} {self.symbol} 업데이트 검증 실패, 롤백")
                return UpdateResult(False, validation.error_messages)

            self.last_update_id = data.get("sequence", self.last_update_id)
            self.last_update_time = data.get("timestamp", int(time.time() * 1000))

            # 스냅샷 → 첫 delta 시 역전감지 on
            if self.ignore_cross_detection and msg_type == "delta":
                self.logger.info(f"{self.exchange_kr} {self.symbol} 역전감지 활성화")
                self.ignore_cross_detection = False

            # 데이터 품질 로깅 추가
            if not self.ignore_cross_detection:
                best_bid = max(self.bids.keys()) if self.bids else None
                best_ask = min(self.asks.keys()) if self.asks else None
                
                if best_bid and best_ask:
                    spread = (best_ask - best_bid) / best_bid * 100
                    
                    if spread < -0.1:  # 음수 스프레드(역전) 감지
                        self.logger.warning(
                            f"{self.exchange_kr} {self.symbol} 가격 역전 감지 | "
                            f"최고매수={best_bid:,.8f}, "
                            f"최저매도={best_ask:,.8f}, "
                            f"스프레드={spread:.2f}%"
                        )
                    elif spread > 5:  # 비정상적으로 큰 스프레드 감지
                        self.logger.warning(
                            f"{self.exchange_kr} {self.symbol} 큰 스프레드 감지 | "
                            f"최고매수={best_bid:,.8f}, "
                            f"최저매도={best_ask:,.8f}, "
                            f"스프레드={spread:.2f}%"
                        )

            parsed = self.to_dict()
            # 스냅샷 데이터는 큐로 전송하지 않고, 델타 업데이트만 큐로 전송
            if self.output_queue and msg_type != "snapshot":
                await self.output_queue.put((self.exchangename, parsed))
            return UpdateResult(True, [])

        except Exception as e:
            self.logger.error(f"{self.exchange_kr} {self.symbol} 오더북 업데이트 중 오류: {e}")
            return UpdateResult(False, [str(e)])

    def to_dict(self) -> dict:
        """상위 10레벨만 반환"""
        try:
            sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)
            sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])
            top_n = 10
            return {
                "exchangename": self.exchangename,
                "symbol": self.symbol,
                "bids": [[p, q] for p, q in sorted_bids[:top_n]],
                "asks": [[p, q] for p, q in sorted_asks[:top_n]],
                "timestamp": self.last_update_time or int(time.time()*1000),
                "sequence": self.last_update_id
            }
        except Exception as e:
            self.logger.error(f"{self.exchange_kr} {self.symbol} to_dict 실패: {e}", exc_info=True)
            return {
                "exchangename": self.exchangename,
                "symbol": self.symbol,
                "bids": [],
                "asks": [],
                "timestamp": int(time.time()*1000),
                "sequence": self.last_update_id
            }

    def _validate_orderbook(self) -> ValidationResult:
        errors = []
        if len(self.bids) == 0 or len(self.asks) == 0:
            errors.append("오더북이 비어있음")
        if len(self.bids) > self.depth or len(self.asks) > self.depth:
            errors.append("깊이 제한 초과")
        return ValidationResult(len(errors) == 0, errors)

    def get_bids(self, limit: int = 10) -> List[List[float]]:
        """
        상위 N개의 매수 호가를 반환합니다.
        
        Args:
            limit: 반환할 호가 수
            
        Returns:
            List[List[float]]: [[가격, 수량], ...] 형태의 매수 호가 목록
        """
        try:
            sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)
            return [[p, q] for p, q in sorted_bids[:limit]]
        except Exception as e:
            self.logger.error(f"{self.exchange_kr} {self.symbol} get_bids 실패: {e}")
            return []
            
    def get_asks(self, limit: int = 10) -> List[List[float]]:
        """
        상위 N개의 매도 호가를 반환합니다.
        
        Args:
            limit: 반환할 호가 수
            
        Returns:
            List[List[float]]: [[가격, 수량], ...] 형태의 매도 호가 목록
        """
        try:
            sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])
            return [[p, q] for p, q in sorted_asks[:limit]]
        except Exception as e:
            self.logger.error(f"{self.exchange_kr} {self.symbol} get_asks 실패: {e}")
            return []