"""
오더북 관리자 모듈

이 모듈은 거래소별 웹소켓 연결, 구독, 파싱, 오더북 처리의 전체 흐름을 관리합니다.
각 거래소별 구현은 해당 거래소 모듈에 위임합니다.

흐름: connection → subscription → parser → orderbook
"""

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Set
from datetime import datetime

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import EXCHANGE_NAMES_KR, LOG_SYSTEM

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class OrderManager(ABC):
    """
    기본 오더북 관리자 추상 클래스
    
    각 거래소별 구현을 위한 기본 구조를 정의합니다.
    """
    
    def __init__(self, settings: dict, exchange_code: str):
        """
        초기화
        
        Args:
            settings: 설정 딕셔너리
            exchange_code: 거래소 코드
        """
        self.settings = settings
        self.exchange_code = exchange_code
        self.exchange_name_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        
        # 컴포넌트들
        self.connection = None       # 웹소켓 연결 객체
        self.subscription = None     # 구독 객체
        self.parser = None           # 파서 객체
        self.orderbook_manager = None  # 오더북 관리자
        
        # 구독 심볼 관리
        self.symbols = set()
        
        # 출력 큐
        self.output_queue = None
        
        # 상태 관리
        self.is_running = False
        self.tasks = {}
        
        logger.info(f"{self.exchange_name_kr} 오더북 관리자 초기화")
    
    @abstractmethod
    async def initialize(self) -> bool:
        """
        모든 컴포넌트 초기화
        
        Returns:
            bool: 초기화 성공 여부
        """
        pass
    
    @abstractmethod
    async def start(self, symbols: List[str]) -> bool:
        """
        오더북 수집 시작
        
        Args:
            symbols: 수집할 심볼 목록
            
        Returns:
            bool: 시작 성공 여부
        """
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """오더북 수집 중지"""
        pass
    
    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """
        출력 큐 설정
        
        Args:
            queue: 출력 큐
        """
        self.output_queue = queue
        logger.info(f"{self.exchange_name_kr} 출력 큐 설정 완료 (큐 ID: {id(queue)})")
    
    def update_connection_status(self, exchange=None, status=None):
        """
        연결 상태 업데이트
        
        Args:
            exchange: 거래소 코드 (기본값: None, 자체 exchange_code 사용)
            status: 연결 상태 (True: 연결됨, False: 연결 끊김, 문자열: 상태 메시지)
        """
        # exchange 인자가 없거나 self.exchange_code와 일치하는 경우에만 처리
        if exchange is None or exchange == self.exchange_code:
            # status가 문자열인 경우 (WebsocketManager 호환)
            if isinstance(status, str):
                if status in ["connect", "message"]:
                    actual_status = True
                elif status in ["disconnect", "error"]:
                    actual_status = False
                else:
                    actual_status = None
            else:
                actual_status = status
            
            # 실제 상태가 None이 아닌 경우에만 콜백 호출
            if actual_status is not None and self.connection_status_callback:
                self.connection_status_callback(self.exchange_code, actual_status)

class UpbitOrderManager(OrderManager):
    """
    업비트 오더북 관리자
    
    업비트 거래소의 연결, 구독, 파싱, 오더북 처리 흐름을 관리합니다.
    세부 구현은 각 모듈에 위임합니다.
    """
    
    def __init__(self, settings: dict):
        """
        초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, "upbit")
        
        # WebsocketManager와 호환되는 속성들
        self.connection_status_callback = None
        self.start_time = time.time()
        self.message_stats = {
            "total_received": 0,
            "snapshot_received": 0,
            "delta_received": 0,
            "errors": 0,
            "last_received": None
        }
    
    async def initialize(self) -> bool:
        """
        모든 컴포넌트 초기화
        
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            # 업비트 관련 모듈 임포트 (지연 임포트로 순환 참조 방지)
            from crosskimp.ob_collector.orderbook.connection.upbit_s_cn import UpbitWebSocketConnector
            from crosskimp.ob_collector.orderbook.subscription.upbit_s_sub import UpbitSubscription 
            from crosskimp.ob_collector.orderbook.parser.upbit_s_pa import UpbitParser
            from crosskimp.ob_collector.orderbook.orderbook.upbit_s_ob import UpbitOrderBookManager
            
            # 1. 웹소켓 연결 객체 생성
            self.connection = UpbitWebSocketConnector(self.settings)
            
            # 2. 파서 객체 생성
            self.parser = UpbitParser()
            
            # 3. 오더북 관리자 생성
            depth = self.settings.get("websocket", {}).get("orderbook_depth", 15)
            self.orderbook_manager = UpbitOrderBookManager(depth=depth)
            
            # 4. 구독 객체 생성 (연결, 파서 연결)
            self.subscription = UpbitSubscription(self.connection, self.parser)
            
            # 5. 출력 큐 설정 (있는 경우)
            if self.output_queue:
                self.orderbook_manager.set_output_queue(self.output_queue)
            
            # 6. WebsocketManager와 호환되는 설정
            if hasattr(self.connection, 'connection_status_callback'):
                self.connection.connection_status_callback = self.update_connection_status
            
            logger.info(f"{self.exchange_name_kr} 모든 컴포넌트 초기화 완료")
            return True
            
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 초기화 실패: {str(e)}")
            return False
    
    async def start(self, symbols: List[str]) -> bool:
        """
        오더북 수집 시작
        
        Args:
            symbols: 수집할 심볼 목록
            
        Returns:
            bool: 시작 성공 여부
        """
        try:
            if not symbols:
                logger.warning(f"{self.exchange_name_kr} 구독할 심볼이 없습니다.")
                return False
            
            # 초기화 확인
            if not self.connection or not self.subscription or not self.parser or not self.orderbook_manager:
                await self.initialize()
            
            # 실행 중인지 확인
            if self.is_running:
                logger.warning(f"{self.exchange_name_kr} 이미 실행 중입니다.")
                return True
            
            # 웹소켓 연결
            await self.connection.connect()
            
            # 모든 심볼을 한 번에 구독
            try:
                logger.info(f"{self.exchange_name_kr} 전체 {len(symbols)}개 심볼 구독 시작")
                
                # 구독 시작 (콜백 함수는 업비트 구독 모듈에서 처리)
                result = await self.subscription.subscribe(
                    symbols,
                    on_snapshot=self._handle_snapshot,
                    on_error=self._on_error
                )
                
                # 구독 성공한 심볼 추적
                if result:
                    self.symbols.update(symbols)
                    self.is_running = True
                    logger.info(f"{self.exchange_name_kr} 오더북 수집 시작: {len(symbols)}개 심볼 구독 성공")
                    return True
                else:
                    logger.error(f"{self.exchange_name_kr} 구독 실패")
                    return False
                
            except Exception as e:
                logger.error(f"{self.exchange_name_kr} 구독 실패: {str(e)}")
                return False
            
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 시작 실패: {str(e)}")
            return False
    
    async def stop(self) -> None:
        """오더북 수집 중지"""
        try:
            if not self.is_running:
                return
                
            # 구독 중인 모든 심볼 구독 취소
            if self.subscription:
                await self.subscription.close()
            
            # 연결 종료
            if self.connection:
                await self.connection.stop()
            
            # 오더북 관리자 정리
            if self.orderbook_manager:
                self.orderbook_manager.clear_all()
            
            # 태스크 취소
            for task in self.tasks.values():
                task.cancel()
            
            self.is_running = False
            self.symbols.clear()
            
            logger.info(f"{self.exchange_name_kr} 오더북 수집 중지 완료")
            
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 중지 중 오류 발생: {str(e)}")
    
    async def _handle_snapshot(self, symbol: str, data: Dict) -> None:
        """
        스냅샷 처리
        
        Args:
            symbol: 심볼
            data: 스냅샷 데이터
        """
        try:
            logger.info(f"[스냅샷 수신] {self.exchange_name_kr} {symbol}")
            
            # 오더북 초기화 (세부 구현은 orderbook_manager에 위임)
            result = await self.orderbook_manager.initialize_orderbook(symbol, data)
            
            # 출력 큐에 추가
            if result.is_valid and self.output_queue:
                orderbook = self.orderbook_manager.get_orderbook(symbol).to_dict()
                self.output_queue.put_nowait({
                    "exchange": self.exchange_code,
                    "symbol": symbol,
                    "timestamp": time.time(),
                    "data": orderbook
                })
            
            # 메시지 통계 업데이트
            self.update_message_stats("snapshot")
            
            # 연결 상태 콜백 호출
            self.update_connection_status(None, True)
            
        except Exception as e:
            logger.error(f"[스냅샷 오류] {self.exchange_name_kr} {symbol} 스냅샷 처리 실패: {str(e)}")
            self.update_message_stats("error")
    
    def _on_error(self, symbol: str, error: str) -> None:
        """
        에러 콜백
        
        Args:
            symbol: 심볼
            error: 에러 메시지
        """
        logger.error(f"{self.exchange_name_kr} {symbol} 에러 발생: {error}")
        self.update_message_stats("error")
    
    def get_status(self) -> Dict:
        """
        상태 정보 반환
        
        Returns:
            상태 정보 딕셔너리
        """
        return {
            "exchange": self.exchange_code,
            "connected": self.is_running,
            "message_count": self.message_stats["total_received"],
            "snapshot_count": self.message_stats["snapshot_received"],
            "delta_count": self.message_stats["delta_received"],
            "error_count": self.message_stats["errors"],
            "uptime": time.time() - self.start_time,
            "symbols": list(self.symbols)
        }

    def set_connection_status_callback(self, callback):
        """
        연결 상태 콜백 설정
        
        Args:
            callback: 연결 상태 변경 시 호출할 콜백 함수
        """
        self.connection_status_callback = callback
        
    def update_message_stats(self, message_type: str):
        """
        메시지 통계 업데이트
        
        Args:
            message_type: 메시지 타입 ('snapshot', 'delta', 'error')
        """
        self.message_stats["total_received"] += 1
        self.message_stats["last_received"] = datetime.now()
        
        if message_type == "snapshot":
            self.message_stats["snapshot_received"] += 1
        elif message_type == "delta":
            self.message_stats["delta_received"] += 1
        elif message_type == "error":
            self.message_stats["errors"] += 1


# OrderManager 팩토리 함수
def create_order_manager(exchange: str, settings: dict) -> Optional[OrderManager]:
    """
    거래소별 OrderManager 인스턴스 생성
    
    Args:
        exchange: 거래소 코드
        settings: 설정 딕셔너리
        
    Returns:
        Optional[OrderManager]: OrderManager 인스턴스 또는 None
    """
    if exchange.lower() == "upbit":
        return UpbitOrderManager(settings)
    # 이후 다른 거래소 추가 예정
    
    logger.error(f"지원되지 않는 거래소: {exchange}")
    return None


# WebsocketManager와 통합하기 위한 함수
async def integrate_with_websocket_manager(ws_manager, settings, filtered_data):
    """
    OrderManager를 WebsocketManager와 통합합니다.
    
    Args:
        ws_manager: WebsocketManager 인스턴스
        settings: 설정 딕셔너리
        filtered_data: 필터링된 심볼 데이터 (exchange -> symbols)
        
    Returns:
        성공 여부
    """
    try:
        # 업비트 심볼 추출
        upbit_symbols = filtered_data.get("upbit", [])
        if not upbit_symbols:
            logger.info("업비트 심볼이 없어 OrderManager를 초기화하지 않습니다.")
            return False
            
        # 업비트 매니저 생성
        upbit_manager = UpbitOrderManager(settings)
        
        # 출력 큐 공유
        upbit_manager.set_output_queue(ws_manager.output_queue)
        
        # 연결 상태 콜백 공유
        upbit_manager.set_connection_status_callback(
            lambda exchange, status: ws_manager.update_connection_status(exchange, status)
        )
        
        # 초기화 및 시작
        await upbit_manager.initialize()
        await upbit_manager.start(upbit_symbols)
        
        # WebsocketManager에 OrderManager 저장
        if not hasattr(ws_manager, "order_managers"):
            ws_manager.order_managers = {}
        ws_manager.order_managers["upbit"] = upbit_manager
        
        logger.info("업비트 OrderManager가 WebsocketManager와 통합되었습니다.")
        return True
        
    except Exception as e:
        logger.error(f"OrderManager 통합 중 오류 발생: {str(e)}")
        return False
