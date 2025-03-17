"""
오더북 관리자 모듈

이 모듈은 거래소별 웹소켓 연결, 구독, 파싱, 오더북 처리의 전체 흐름을 관리합니다.
베이스 클래스들을 활용하여 중앙에서 컨트롤 타워 역할을 합니다.

흐름: connection → subscription → parser
"""

import asyncio
import time
import importlib
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Set, Callable, Tuple, Union
from datetime import datetime

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import EXCHANGE_NAMES_KR, LOG_SYSTEM, Exchange
from crosskimp.ob_collector.orderbook.metric.metrics_manager import WebsocketMetricsManager
from crosskimp.ob_collector.orderbook.validator.validators import UpbitOrderBookValidator, BaseOrderBookValidator

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class OrderManager:
    """
    통합 오더북 관리자 클래스
    
    베이스 클래스들을 활용하여 중앙에서 컨트롤 타워 역할을 합니다.
    거래소별 특화 로직은 설정과 콜백으로 처리합니다.
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
        
        # 메트릭 매니저 싱글톤 인스턴스 사용
        self.metrics_manager = WebsocketMetricsManager.get_instance()
        
        # 컴포넌트들
        self.connection = None       # 웹소켓 연결 객체
        self.subscription = None     # 구독 객체
        self.parser = None           # 파서 객체
        self.validator = None        # 검증 객체
        
        # 구독 심볼 관리
        self.symbols = set()
        
        # 출력 큐
        self.output_queue = None
        
        # 상태 관리
        self.is_running = False
        self.tasks = {}
        
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
        
        # 거래소별 설정
        self.exchange_config = self._get_exchange_config()
        
        logger.info(f"{self.exchange_name_kr} 오더북 관리자 초기화")
    
    def _get_exchange_config(self) -> dict:
        """
        거래소별 기본 설정 가져오기
        
        Returns:
            dict: 거래소별 기본 설정
        """
        # 기본 설정
        config = {
            "module_paths": {
                "connection": f"crosskimp.ob_collector.orderbook.connection.{self.exchange_code}_s_cn",
                "subscription": f"crosskimp.ob_collector.orderbook.subscription.{self.exchange_code}_s_sub",
                "parser": f"crosskimp.ob_collector.orderbook.parser.{self.exchange_code}_s_pa",
                "validator": "crosskimp.ob_collector.orderbook.validator.validators"
            },
            "class_names": {
                "connection": f"{self.exchange_code.capitalize()}WebSocketConnector",
                "subscription": f"{self.exchange_code.capitalize()}Subscription",
                "parser": f"{self.exchange_code.capitalize()}Parser",
                "validator": "UpbitOrderBookValidator" if self.exchange_code == "upbit" else "BaseOrderBookValidator"
            }
        }
        
        # 거래소별 델타 지원 여부만 설정
        if self.exchange_code == "upbit":
            config["supports_delta"] = False  # 업비트는 델타를 지원하지 않음
        else:
            config["supports_delta"] = True   # 다른 거래소는 델타 지원
            
        return config
    
    async def initialize(self) -> bool:
        """
        모든 컴포넌트 초기화
        
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            # 1. 동적으로 모듈 임포트
            connection_module = self._import_module(self.exchange_config["module_paths"]["connection"])
            subscription_module = self._import_module(self.exchange_config["module_paths"]["subscription"])
            parser_module = self._import_module(self.exchange_config["module_paths"]["parser"])
            validator_module = self._import_module(self.exchange_config["module_paths"]["validator"])
            
            if not all([connection_module, subscription_module, parser_module, validator_module]):
                logger.error(f"{self.exchange_name_kr} 모듈 임포트 실패")
                return False
            
            # 2. 클래스 가져오기
            connection_class = getattr(connection_module, self.exchange_config["class_names"]["connection"])
            subscription_class = getattr(subscription_module, self.exchange_config["class_names"]["subscription"])
            parser_class = getattr(parser_module, self.exchange_config["class_names"]["parser"])
            validator_class = getattr(validator_module, self.exchange_config["class_names"]["validator"])
            
            # 3. 인스턴스 생성
            # 3.1 웹소켓 연결 객체 생성
            self.connection = connection_class(self.settings)
            
            # 3.2 파서 객체 생성
            self.parser = parser_class()
            
            # 3.3 validator 생성
            depth = self.settings.get("websocket", {}).get("orderbook_depth", 15)
            self.validator = validator_class(self.exchange_code, depth)
            
            # 3.4 구독 객체 생성 (연결, 파서 연결)
            self.subscription = subscription_class(self.connection, self.parser)
            
            # 3.5 출력 큐 설정 (있는 경우)
            if self.output_queue:
                self.validator.set_output_queue(self.output_queue)
            
            # 3.6 WebsocketManager와 호환되는 설정
            if hasattr(self.connection, 'connection_status_callback'):
                self.connection.connection_status_callback = self.update_connection_status
            
            logger.info(f"{self.exchange_name_kr} 모든 컴포넌트 초기화 완료")
            return True
            
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 초기화 실패: {str(e)}", exc_info=True)
            return False
    
    def _import_module(self, module_path: str) -> Optional[Any]:
        """
        모듈 동적 임포트
        
        Args:
            module_path: 모듈 경로
            
        Returns:
            Optional[Any]: 임포트된 모듈 또는 None
        """
        try:
            return importlib.import_module(module_path)
        except ImportError as e:
            logger.error(f"{self.exchange_name_kr} 모듈 임포트 실패: {module_path} - {str(e)}")
            return None
    
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
            if not self.connection or not self.subscription or not self.parser or not self.validator:
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
                
                # 구독 시작
                callbacks = {
                    "on_snapshot": self._handle_snapshot,
                    "on_error": self._on_error
                }
                
                # 델타 지원 여부에 따라 콜백 추가
                if self.exchange_config["supports_delta"]:
                    callbacks["on_delta"] = self._handle_delta
                
                result = await self.subscription.subscribe(
                    symbols,
                    **callbacks
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
            if self.validator:
                self.validator.clear_all()
            
            # 태스크 취소
            for task in self.tasks.values():
                task.cancel()
            
            self.is_running = False
            self.symbols.clear()
            
            logger.info(f"{self.exchange_name_kr} 오더북 수집 중지 완료")
            
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 중지 중 오류 발생: {str(e)}")
    
    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """
        출력 큐 설정
        
        Args:
            queue: 출력 큐
        """
        self.output_queue = queue
        if self.validator:
            self.validator.set_output_queue(queue)
        logger.info(f"{self.exchange_name_kr} 출력 큐 설정 완료 (큐 ID: {id(queue)})")
    
    def update_connection_status(self, exchange=None, status=None):
        """
        연결 상태 업데이트
        
        Args:
            exchange: 거래소 코드 (기본값: None, 자체 exchange_code 사용)
            status: 연결 상태 (문자열: "connected" 또는 "disconnected")
        """
        # exchange 인자가 없거나 self.exchange_code와 일치하는 경우에만 처리
        if exchange is None or exchange == self.exchange_code:
            # status가 문자열인 경우 (WebsocketManager 호환)
            if isinstance(status, str):
                if status in ["connect", "message", "connected"]:
                    actual_status = "connected"
                    # 메트릭 매니저 상태 업데이트
                    if self.validator and hasattr(self.validator, 'metrics'):
                        self.validator.metrics.update_connection_state(self.exchange_code, "connected")
                elif status in ["disconnect", "error", "disconnected"]:
                    actual_status = "disconnected"
                    # 메트릭 매니저 상태 업데이트
                    if self.validator and hasattr(self.validator, 'metrics'):
                        self.validator.metrics.update_connection_state(self.exchange_code, "disconnected")
                else:
                    actual_status = status
            # 불리언 값이 들어온 경우 문자열로 변환
            elif isinstance(status, bool):
                actual_status = "connected" if status else "disconnected"
                # 메트릭 매니저 상태 업데이트
                if self.validator and hasattr(self.validator, 'metrics'):
                    if status:
                        self.validator.metrics.update_connection_state(self.exchange_code, "connected")
                    else:
                        self.validator.metrics.update_connection_state(self.exchange_code, "disconnected")
            else:
                actual_status = "disconnected"
            
            # 실제 상태가 None이 아닌 경우에만 콜백 호출
            if actual_status is not None and self.connection_status_callback:
                self.connection_status_callback(self.exchange_code, actual_status)
    
    async def _handle_snapshot(self, symbol: str, data: Dict) -> None:
        """
        스냅샷 처리
        
        Args:
            symbol: 심볼
            data: 스냅샷 데이터
        """
        try:
            # 처리 시작 시간 기록
            start_time = time.time()
            
            # 오더북 초기화 (세부 구현은 validator에 위임)
            result = await self.validator.initialize_orderbook(symbol, data)
            
            # 출력 큐에 추가
            if result.is_valid and self.output_queue:
                orderbook = self.validator.get_orderbook(symbol).to_dict()
                self.output_queue.put_nowait({
                    "exchange": self.exchange_code,
                    "symbol": symbol,
                    "timestamp": time.time(),
                    "data": orderbook
                })
            
            # 메시지 통계 업데이트
            self.update_message_stats("snapshot")
            
            # 메트릭 업데이트 - 중앙 메트릭 매니저 사용
            if hasattr(self.validator, 'metrics'):
                # 메시지 수신 기록
                self.validator.metrics.record_message(self.exchange_code)
                # 오더북 업데이트 기록
                self.validator.metrics.record_orderbook(self.exchange_code)
                # 데이터 크기 메트릭 업데이트
                data_size = len(str(data))  # 간단한 크기 측정
                self.validator.metrics.record_bytes(self.exchange_code, data_size)
                
                # 처리 시간 기록
                end_time = time.time()
                processing_time_ms = (end_time - start_time) * 1000
                self.validator.metrics.record_processing_time(self.exchange_code, processing_time_ms)
            
            # 연결 상태 콜백 호출
            self.update_connection_status(None, "connected")
            
        except Exception as e:
            logger.error(f"[스냅샷 오류] {self.exchange_name_kr} {symbol} 스냅샷 처리 실패: {str(e)}")
            self.update_message_stats("error")
            if hasattr(self.validator, 'metrics'):
                self.validator.metrics.record_error(self.exchange_code)
    
    async def _handle_delta(self, symbol: str, data: Dict) -> None:
        """
        델타 처리
        
        Args:
            symbol: 심볼
            data: 델타 데이터
        """
        try:
            # 처리 시작 시간 기록
            start_time = time.time()
            
            # 오더북 업데이트 (세부 구현은 validator에 위임)
            result = await self.validator.update(symbol, data)
            
            # 출력 큐에 추가
            if result.is_valid and self.output_queue:
                orderbook = self.validator.get_orderbook(symbol).to_dict()
                self.output_queue.put_nowait({
                    "exchange": self.exchange_code,
                    "symbol": symbol,
                    "timestamp": time.time(),
                    "data": orderbook
                })
            
            # 메시지 통계 업데이트
            self.update_message_stats("delta")
            
            # 메트릭 업데이트 - 중앙 메트릭 매니저 사용
            if hasattr(self.validator, 'metrics'):
                # 메시지 수신 기록
                self.validator.metrics.record_message(self.exchange_code)
                # 오더북 업데이트 기록
                self.validator.metrics.record_orderbook(self.exchange_code)
                # 데이터 크기 메트릭 업데이트
                data_size = len(str(data))  # 간단한 크기 측정
                self.validator.metrics.record_bytes(self.exchange_code, data_size)
                
                # 처리 시간 기록
                end_time = time.time()
                processing_time_ms = (end_time - start_time) * 1000
                self.validator.metrics.record_processing_time(self.exchange_code, processing_time_ms)
            
            # 연결 상태 콜백 호출
            self.update_connection_status(None, "connected")
            
        except Exception as e:
            logger.error(f"[델타 오류] {self.exchange_name_kr} {symbol} 델타 처리 실패: {str(e)}")
            self.update_message_stats("error")
            if hasattr(self.validator, 'metrics'):
                self.validator.metrics.record_error(self.exchange_code)
    
    def _on_error(self, symbol: str, error: str) -> None:
        """
        에러 콜백
        
        Args:
            symbol: 심볼
            error: 에러 메시지
        """
        logger.error(f"{self.exchange_name_kr} {symbol} 에러 발생: {error}")
        self.update_message_stats("error")
        
        # 메트릭 업데이트 - 중앙 메트릭 매니저 사용
        if hasattr(self.validator, 'metrics'):
            self.validator.metrics.record_error(self.exchange_code)
    
    def get_status(self) -> Dict:
        """
        상태 정보 반환
        
        Returns:
            상태 정보 딕셔너리
        """
        # 기본 상태 정보
        status = {
            "exchange": self.exchange_code,
            "connected": self.is_running,
            "uptime": time.time() - self.start_time,
            "symbols": list(self.symbols)
        }
        
        # 메시지 통계 정보 추가
        status.update({
            "message_count": self.message_stats["total_received"],
            "snapshot_count": self.message_stats["snapshot_received"],
            "delta_count": self.message_stats["delta_received"],
            "error_count": self.message_stats["errors"]
        })
        
        # 중앙 메트릭 매니저에서 추가 정보 가져오기
        if hasattr(self.validator, 'metrics'):
            metrics = self.validator.metrics.get_metrics()
            if self.exchange_code in metrics:
                exchange_metrics = metrics[self.exchange_code]
                status.update({
                    "connected": exchange_metrics.get("connected", self.is_running),
                    "processing_rate": exchange_metrics.get("processing_rate", 0),
                    "avg_processing_time": exchange_metrics.get("avg_processing_time", 0),
                    "bytes_received": exchange_metrics.get("bytes_received", 0)
                })
        
        return status

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
        # 내부 통계 업데이트
        self.message_stats["total_received"] += 1
        self.message_stats["last_received"] = datetime.now()
        
        if message_type == "snapshot":
            self.message_stats["snapshot_received"] += 1
        elif message_type == "delta":
            self.message_stats["delta_received"] += 1
        elif message_type == "error":
            self.message_stats["errors"] += 1
        
        # 중앙 메트릭 매니저 업데이트
        if hasattr(self.validator, 'metrics'):
            # 연결 상태 업데이트 - 메시지 수신 시 연결됨으로 간주
            if message_type != "error":
                self.validator.metrics.update_connection_state(self.exchange_code, "connected")
            else:
                # 에러 메시지는 연결 상태에 영향을 주지 않음
                pass


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
    try:
        # 지원하는 거래소 목록
        supported_exchanges = ["upbit", "bybit"]
        
        exchange_lower = exchange.lower()
        if exchange_lower not in supported_exchanges:
            logger.warning(f"지원되지 않는 거래소: {exchange}")
            return None
        
        # OrderManager 인스턴스 생성
        return OrderManager(settings, exchange_lower)
        
    except Exception as e:
        logger.error(f"OrderManager 생성 실패: {exchange} - {str(e)}", exc_info=True)
        return None


# WebsocketManager와 통합하기 위한 함수
async def integrate_with_websocket_manager(ws_manager, settings, filtered_data):
    """
    OrderManager를 WebsocketManager와 통합합니다.
    모든 거래소를 OrderManager를 통해 처리합니다.
    
    Args:
        ws_manager: WebsocketManager 인스턴스
        settings: 설정 딕셔너리
        filtered_data: 필터링된 심볼 데이터 (exchange -> symbols)
        
    Returns:
        성공 여부
    """
    try:
        # WebsocketManager에 OrderManager 저장 공간 생성
        if not hasattr(ws_manager, "order_managers"):
            ws_manager.order_managers = {}
        
        # 각 거래소별 처리
        for exchange, symbols in filtered_data.items():
            if not symbols:
                logger.info(f"{exchange} 심볼이 없어 OrderManager를 초기화하지 않습니다.")
                continue
                
            # OrderManager 생성
            manager = create_order_manager(exchange, settings)
            if not manager:
                logger.error(f"{exchange} OrderManager 생성 실패")
                continue
                
            # 출력 큐 공유
            manager.set_output_queue(ws_manager.output_queue)
            
            # 연결 상태 콜백 공유
            manager.set_connection_status_callback(
                lambda ex, status: ws_manager.update_connection_status(ex, status)
            )
            
            # 초기화 및 시작
            await manager.initialize()
            await manager.start(symbols)
            
            # WebsocketManager에 OrderManager 저장
            ws_manager.order_managers[exchange] = manager
            
            logger.info(f"{exchange} OrderManager가 WebsocketManager와 통합되었습니다.")
        
        return True
        
    except Exception as e:
        logger.error(f"OrderManager 통합 중 오류 발생: {str(e)}", exc_info=True)
        return False
