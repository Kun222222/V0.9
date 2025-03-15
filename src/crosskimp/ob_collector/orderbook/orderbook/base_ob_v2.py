from dataclasses import dataclass
from typing import List, Dict, Optional
import time
import asyncio
from collections import defaultdict

from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.core.metrics_manager import WebsocketMetricsManager
from crosskimp.config.constants import EXCHANGE_NAMES_KR
from crosskimp.ob_collector.cpp.cpp_interface import send_orderbook_to_cpp

logger = get_unified_logger()

@dataclass
class ValidationResult:
    is_valid: bool
    error_messages: List[str] = None

    def __post_init__(self):
        if self.error_messages is None:
            self.error_messages = []

class OrderBookV2:
    """
    기본 오더북 V2
    - 최소한의 공통 기능만 제공
    - 검증 로직은 각 거래소 구현체에 위임
    """
    def __init__(self, exchangename: str, symbol: str, depth: int = 100):
        self.exchangename = exchangename
        self.symbol = symbol
        self.depth = depth
        
        # 한글 거래소명 가져오기
        self.exchange_kr = EXCHANGE_NAMES_KR.get(self.exchangename, f"[{self.exchangename}]")
        
        # 기본 데이터 구조
        self.bids: List[List[float]] = []  # [[price, quantity], ...]
        self.asks: List[List[float]] = []  # [[price, quantity], ...]
        
        # 메타데이터
        self.last_update_id = None
        self.last_update_time = None
        
        # 출력 큐
        self.output_queue: Optional[asyncio.Queue] = None

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """출력 큐 설정"""
        self.output_queue = queue

    def update_orderbook(self, bids: List[List[float]], asks: List[List[float]], 
                        timestamp: Optional[int] = None, sequence: Optional[int] = None) -> None:
        """오더북 데이터 업데이트"""
        self.bids = bids[:self.depth] if bids else []
        self.asks = asks[:self.depth] if asks else []
        
        if timestamp:
            self.last_update_time = timestamp
        if sequence:
            self.last_update_id = sequence
            
        # 오더북 업데이트 후 바로 C++로 데이터 전송
        asyncio.create_task(self.send_to_cpp())

    def to_dict(self) -> dict:
        """오더북 데이터를 딕셔너리로 변환"""
        return {
            "exchangename": self.exchangename,
            "symbol": self.symbol,
            "bids": self.bids[:10],  # 상위 10개만
            "asks": self.asks[:10],  # 상위 10개만
            "timestamp": self.last_update_time or int(time.time() * 1000),
            "sequence": self.last_update_id
        }

    async def send_to_cpp(self) -> None:
        """C++로 데이터 직접 전송"""
        # 오더북 데이터를 딕셔너리로 변환
        orderbook_data = self.to_dict()
        
        # C++로 데이터 전송
        await send_orderbook_to_cpp(self.exchangename, orderbook_data)

    async def send_to_queue(self) -> None:
        """큐로 데이터 전송 및 C++로 데이터 전송"""
        # 오더북 데이터를 딕셔너리로 변환
        orderbook_data = self.to_dict()
        
        # 큐로 데이터 전송
        if self.output_queue:
            await self.output_queue.put((self.exchangename, orderbook_data))
        
        # C++로 데이터 전송 (비동기 태스크로 실행하여 블로킹하지 않도록 함)
        asyncio.create_task(send_orderbook_to_cpp(self.exchangename, orderbook_data))

class BaseOrderBookManagerV2:
    """
    기본 오더북 관리자 V2
    - 오더북 객체의 생명주기 관리
    - 검증 로직은 하위 클래스에서 구현
    - 메트릭 수집 및 모니터링
    """
    def __init__(self, depth: int = 100):
        self.depth = depth
        self.orderbooks: Dict[str, OrderBookV2] = {}
        self._output_queue: Optional[asyncio.Queue] = None
        self.exchangename: str = ""  # 하위 클래스에서 설정해야 함
        
        # 메트릭 매니저 초기화
        self.metrics = WebsocketMetricsManager()
        self._metrics_task: Optional[asyncio.Task] = None

    async def _log_metrics_periodically(self):
        """1분 간격으로 메트릭 정보 로깅"""
        while True:
            try:
                metrics = self.metrics.get_metrics()
                exchange_metrics = metrics.get(self.exchangename, {})
                exchange_name_kr = EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)
                
                # 메시지 카운트 확인
                message_count = 0
                if "message_counts" in self.metrics.metrics and self.exchangename in self.metrics.metrics["message_counts"]:
                    message_count = self.metrics.metrics["message_counts"][self.exchangename]
                
                # 오더북 카운트 확인
                orderbook_count = 0
                if "orderbook_counts" in self.metrics.metrics and self.exchangename in self.metrics.metrics["orderbook_counts"]:
                    orderbook_count = self.metrics.metrics["orderbook_counts"][self.exchangename]
                
                # 에러 카운트 확인
                error_count = 0
                if "error_counts" in self.metrics.metrics and self.exchangename in self.metrics.metrics["error_counts"]:
                    error_count = self.metrics.metrics["error_counts"][self.exchangename]
                
                # 평균 처리 시간 계산
                avg_processing_time = 0
                max_processing_time = 0
                min_processing_time = 0
                if "processing_times" in self.metrics.metrics and self.exchangename in self.metrics.metrics["processing_times"]:
                    processing_times = self.metrics.metrics["processing_times"][self.exchangename]
                    if processing_times:
                        avg_processing_time = sum(processing_times) / len(processing_times)
                        max_processing_time = max(processing_times)
                        min_processing_time = min(processing_times)
                        # 리스트가 너무 길어지지 않도록 최근 100개만 유지
                        if len(processing_times) > 100:
                            self.metrics.metrics["processing_times"][self.exchangename] = processing_times[-100:]
                
                # 바이트 수 확인
                bytes_received = 0
                if "bytes_received" in self.metrics.metrics and self.exchangename in self.metrics.metrics["bytes_received"]:
                    bytes_received = self.metrics.metrics["bytes_received"][self.exchangename]
                
                # 메시지 처리율 계산
                message_rate = 0
                if "message_rates" in self.metrics.metrics and self.exchangename in self.metrics.metrics["message_rates"]:
                    message_rate = self.metrics.metrics["message_rates"][self.exchangename]
                
                # 연결 상태 확인
                connection_status = "연결 안됨"
                if self.exchangename in metrics:
                    connection_status = "연결됨" if metrics[self.exchangename].get("connected", False) else "연결 안됨"
                
                # 기본 메트릭 로깅
                logger.info(
                    f"{exchange_name_kr} 메트릭 현황 | "
                    f"메시지={message_count:,}건, "
                    f"오더북={orderbook_count:,}건, "
                    f"처리시간={avg_processing_time:.2f}ms (최소={min_processing_time:.2f}ms, 최대={max_processing_time:.2f}ms), "
                    f"오류={error_count:,}건, "
                    f"초당처리={message_rate:.1f}건/초, "
                    f"수신={bytes_received/1024/1024:.2f}MB, "
                    f"상태={connection_status}"
                )
                
                # 심볼별 상태 로깅
                for symbol, ob in self.orderbooks.items():
                    if ob.bids and ob.asks:  # 매수/매도 호가가 있는 경우만
                        best_bid = max(bid[0] for bid in ob.bids)
                        best_ask = min(ask[0] for ask in ob.asks)
                        spread = ((best_ask - best_bid) / best_bid) * 100
                        
                        logger.info(
                            f"{exchange_name_kr} {symbol} 호가 현황 | "
                            f"매수호가={len(ob.bids)}건, "
                            f"매도호가={len(ob.asks)}건, "
                            f"최고매수가={best_bid:,.0f}, "
                            f"최저매도가={best_ask:,.0f}, "
                            f"스프레드={spread:.2f}%, "
                            f"마지막업데이트={ob.last_update_time or 0}"
                        )
                
                await asyncio.sleep(60)  # 1분 대기
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"{exchange_name_kr} 메트릭 로깅 중 오류 발생: {str(e)}", exc_info=True)
                await asyncio.sleep(5)

    async def start_metrics_logging(self):
        """메트릭 로깅 시작"""
        if self._metrics_task is None:
            self._metrics_task = asyncio.create_task(self._log_metrics_periodically())
            exchange_name_kr = EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)
            logger.info(f"{exchange_name_kr} 메트릭 로깅 시작")

    async def stop_metrics_logging(self):
        """메트릭 로깅 중지"""
        if self._metrics_task:
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                pass
            self._metrics_task = None
            exchange_name_kr = EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)
            logger.info(f"{exchange_name_kr} 메트릭 로깅 중지")

    def record_metric(self, event_type: str, **kwargs):
        """메트릭 기록"""
        try:
            if event_type == "message":
                # 기본 메시지 카운트 업데이트
                self.metrics.update_metric(self.exchangename, "message")
                
                # 메시지 카운트 직접 업데이트 (추가)
                if "message_counts" not in self.metrics.metrics:
                    self.metrics.metrics["message_counts"] = {}
                if self.exchangename not in self.metrics.metrics["message_counts"]:
                    self.metrics.metrics["message_counts"][self.exchangename] = 0
                self.metrics.metrics["message_counts"][self.exchangename] += 1
            elif event_type == "error":
                # 에러 카운트 업데이트
                error_type = kwargs.get("error_type", "unknown")
                self.metrics.update_metric(self.exchangename, "error", error_type=error_type)
            elif event_type == "orderbook":
                # 오더북 카운트 업데이트
                symbol = kwargs.get("symbol", "unknown")
                if "orderbook_counts" not in self.metrics.metrics:
                    self.metrics.metrics["orderbook_counts"] = {}
                if self.exchangename not in self.metrics.metrics["orderbook_counts"]:
                    self.metrics.metrics["orderbook_counts"][self.exchangename] = 0
                self.metrics.metrics["orderbook_counts"][self.exchangename] += 1
            elif event_type == "processing_time":
                # 처리 시간 기록
                time_ms = kwargs.get("time_ms", 0)
                if "processing_times" not in self.metrics.metrics:
                    self.metrics.metrics["processing_times"] = defaultdict(list)
                self.metrics.metrics["processing_times"][self.exchangename].append(time_ms)
            elif event_type == "bytes":
                # 바이트 수 업데이트
                size = kwargs.get("size", 0)
                if "bytes_received" not in self.metrics.metrics:
                    self.metrics.metrics["bytes_received"] = {}
                if self.exchangename not in self.metrics.metrics["bytes_received"]:
                    self.metrics.metrics["bytes_received"][self.exchangename] = 0
                self.metrics.metrics["bytes_received"][self.exchangename] += size
            else:
                # 기타 이벤트 타입은 그대로 전달
                self.metrics.update_metric(self.exchangename, event_type, **kwargs)
        except Exception as e:
            exchange_name_kr = EXCHANGE_NAMES_KR.get(self.exchangename, self.exchangename)
            logger.error(
                f"{exchange_name_kr} "
                f"메트릭 기록 중 오류 발생: {str(e)}", 
                exc_info=True
            )

    def get_metrics(self) -> Dict:
        """현재 메트릭 상태 조회"""
        metrics = self.metrics.get_metrics()
        
        # 추가 메트릭 정보 수집
        orderbook_stats = self.get_orderbook_stats()
        
        # 메트릭에 오더북 통계 추가
        if self.exchangename in metrics:
            metrics[self.exchangename]["orderbook_stats"] = orderbook_stats
        
        return metrics
        
    def get_orderbook_stats(self) -> Dict:
        """오더북 관련 통계 조회"""
        stats = {}
        for symbol, ob in self.orderbooks.items():
            stats[symbol] = {
                "bid_count": len(ob.bids),
                "ask_count": len(ob.asks),
                "last_update": ob.last_update_time,
                "spread": self._calculate_spread(ob) if ob.bids and ob.asks else None
            }
        return stats
        
    def _calculate_spread(self, ob: OrderBookV2) -> float:
        """스프레드 계산"""
        if not ob.bids or not ob.asks:
            return None
        best_bid = max(bid[0] for bid in ob.bids)
        best_ask = min(ask[0] for ask in ob.asks)
        return ((best_ask - best_bid) / best_bid) * 100  # 백분율로 반환

    async def initialize(self):
        """초기화 및 메트릭 로깅 시작"""
        if not self.exchangename:
            raise ValueError("거래소 이름이 설정되지 않았습니다.")
        self.metrics.initialize_exchange(self.exchangename)
        await self.start_metrics_logging()

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """출력 큐 설정"""
        self._output_queue = queue
        for ob in self.orderbooks.values():
            ob.set_output_queue(queue)

    def is_initialized(self, symbol: str) -> bool:
        """심볼의 오더북 초기화 여부 확인"""
        return symbol in self.orderbooks

    def get_orderbook(self, symbol: str) -> Optional[OrderBookV2]:
        """심볼의 오더북 객체 반환"""
        return self.orderbooks.get(symbol)

    def clear_symbol(self, symbol: str) -> None:
        """심볼 데이터 제거"""
        self.orderbooks.pop(symbol, None)

    def clear_all(self) -> None:
        """전체 데이터 제거"""
        self.orderbooks.clear() 