"""
오더북 관리자 모듈

이 모듈은 거래소별 웹소켓 연결, 구독, 파싱, 오더북 처리의 전체 흐름을 관리합니다.
베이스 클래스들을 활용하여 중앙에서 컨트롤 타워 역할을 합니다.

흐름: connection → subscription → parser
"""

import asyncio
import time
import importlib
import json
import struct
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Set, Callable, Tuple, Union
from datetime import datetime
from contextlib import asynccontextmanager

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import EXCHANGE_NAMES_KR, LOG_SYSTEM, Exchange
from crosskimp.config.paths import LOG_SUBDIRS
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
from crosskimp.ob_collector.orderbook.metric.metrics_manager import WebsocketMetricsManager

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
        self.metrics_manager.initialize_exchange(self.exchange_code)
        
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
        
        # 외부 콜백
        self.connection_status_callback = None
        self.start_time = time.time()
        
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
                "validator": "BaseOrderBookValidator"  # 업비트도 기본 검증기 사용
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
            if self.exchange_config["class_names"]["validator"] == "UpbitOrderBookValidator":
                self.validator = validator_class(self.exchange_code, depth)
            else:
                self.validator = validator_class(self.exchange_code)
            
            # 3.4 구독 객체 생성 (연결, 파서 연결)
            self.subscription = subscription_class(self.connection, self.parser)
            
            # 3.5 출력 큐 설정 (있는 경우)
            if self.output_queue:
                self.validator.set_output_queue(self.output_queue)
            
            # 3.6 연결 상태 콜백 등록 (메트릭 매니저 활용)
            if self.connection_status_callback:
                # 외부 콜백이 있는 경우 메트릭 매니저에 등록
                self.metrics_manager.register_callback(self.exchange_code, self.update_connection_status)
            
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
            if not await self.connection.connect():
                logger.error(f"{self.exchange_name_kr} 웹소켓 연결 실패")
                return False
                
            # 연결 상태 확인 후 구독 시도
            if not self.connection.is_connected:
                logger.error(f"{self.exchange_name_kr} 웹소켓이 연결되지 않아 구독을 시도하지 않습니다.")
                return False
                
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
                await self.connection.disconnect()
            
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
    
    @property
    def is_connected(self) -> bool:
        """연결 상태 확인 (메트릭 매니저 사용)"""
        return self.metrics_manager.is_connected(self.exchange_code)
    
    def update_connection_status(self, exchange_code=None, status=None):
        """
        외부에서 연결 상태 변경 시 호출되는 콜백
        
        Args:
            exchange_code: 거래소 코드
            status: 연결 상태 ('connected', 'disconnected' 등)
        """
        if not exchange_code:
            exchange_code = self.exchange_code
            
        # 메트릭 매니저를 통해 상태 업데이트
        self.metrics_manager.update_connection_state(exchange_code, status)
        
        # 외부 콜백이 있는 경우 호출
        if self.connection_status_callback:
            self.connection_status_callback(exchange_code, status)
    
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
            
            # 데이터 검증
            if self.exchange_code == "upbit":
                # 업비트는 subscription에서 검증 완료된 데이터를 받으므로 별도 검증 없이 바로 처리
                is_valid = True
                orderbook = data
            else:
                # 다른 거래소는 validator를 통한 오더북 초기화 수행
                result = await self.validator.initialize_orderbook(symbol, data)
                is_valid = result.is_valid
                orderbook = self.validator.get_orderbook(symbol) if is_valid else None
            
            # 출력 큐에 전송
            self._send_to_output_queue(symbol, orderbook, is_valid)
            
            # 메트릭 및 통계 업데이트
            self._update_metrics(start_time, "snapshot", data)
            
        except Exception as e:
            logger.error(f"[스냅샷 오류] {self.exchange_name_kr} {symbol} 스냅샷 처리 실패: {str(e)}")
            self._handle_error()
    
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
            
            # 검증 결과 처리
            is_valid = result.is_valid
            orderbook = self.validator.get_orderbook(symbol) if is_valid else None
            
            # 출력 큐에 전송
            self._send_to_output_queue(symbol, orderbook, is_valid)
            
            # 메트릭 및 통계 업데이트
            self._update_metrics(start_time, "delta", data)
            
        except Exception as e:
            logger.error(f"[델타 오류] {self.exchange_name_kr} {symbol} 델타 처리 실패: {str(e)}")
            self._handle_error()
    
    def _on_error(self, symbol: str, error: str) -> None:
        """
        에러 콜백
        
        Args:
            symbol: 심볼
            error: 에러 메시지
        """
        logger.error(f"{self.exchange_name_kr} {symbol} 에러 발생: {error}")
        self._handle_error()
    
    def _handle_error(self):
        """에러 처리 통합 메서드"""
        # 내부 통계 업데이트
        self.message_stats["errors"] += 1
        self.message_stats["last_received"] = datetime.now()
        
        # 메트릭 시스템 업데이트
        self.metrics_manager.record_error(self.exchange_code)
    
    def _update_metrics(self, start_time: float, message_type: str, data: Dict) -> None:
        """
        메트릭 및 통계 업데이트
        
        Args:
            start_time: 처리 시작 시간
            message_type: 메시지 타입 ("snapshot" 또는 "delta")
            data: 메시지 데이터
        """
        # 처리 시간 계산 (밀리초)
        processing_time_ms = (time.time() - start_time) * 1000
        
        # 추정 데이터 크기 계산 (바이트)
        data_size = self._estimate_data_size(data)
        
        # 메트릭 매니저를 통해 통합 업데이트
        self.metrics_manager.update_message_stats(self.exchange_code, message_type)
        self.metrics_manager.record_processing_time(self.exchange_code, processing_time_ms)
        self.metrics_manager.record_bytes(self.exchange_code, data_size)
        
        # 연결 상태 갱신 (메시지 수신은 연결이 활성화된 증거)
        self.metrics_manager.update_connection_state(self.exchange_code, "connected")
    
    def _estimate_data_size(self, data: Dict) -> int:
        """
        데이터 크기 추정 (효율적인 방식)
        
        Args:
            data: 데이터 객체
            
        Returns:
            int: 추정된 바이트 크기
        """
        # 빈 데이터 처리
        if not data:
            return 0
            
        size = 0
        
        # bids와 asks가 있는 경우 (오더북 데이터)
        if "bids" in data and isinstance(data["bids"], list):
            # 각 호가 항목의 대략적인 크기 (숫자 + 콤마 + 대괄호)
            size += len(data["bids"]) * 20  # 호가당 평균 20바이트로 추정
            
        if "asks" in data and isinstance(data["asks"], list):
            size += len(data["asks"]) * 20
        
        # 기타 메타데이터의 대략적인 크기
        size += 100  # 타임스탬프, 시퀀스 번호 등 (고정 100바이트로 추정)
        
        return size
    
    def get_status(self) -> Dict:
        """
        현재 상태 정보 반환
        
        Returns:
            Dict: 상태 정보
        """
        # 메트릭 매니저에서 상태 정보 가져오기
        connection_state = self.is_connected
        message_stats = self.metrics_manager.get_message_stats(self.exchange_code)
        
        # 오더북 정보 수집
        orderbooks = {}
        for symbol in self.symbols:
            if self.validator and hasattr(self.validator, 'get_orderbook'):
                ob = self.validator.get_orderbook(symbol)
                if ob:
                    # 주요 정보만 포함
                    orderbooks[symbol] = {
                        "best_bid": ob.best_bid() if hasattr(ob, 'best_bid') else None,
                        "best_ask": ob.best_ask() if hasattr(ob, 'best_ask') else None,
                        "bid_count": len(ob.bids) if hasattr(ob, 'bids') else 0,
                        "ask_count": len(ob.asks) if hasattr(ob, 'asks') else 0,
                        "last_update": ob.last_update_time if hasattr(ob, 'last_update_time') else None,
                    }
        
        # 상태 정보 구성
        return {
            "exchange": self.exchange_code,
            "exchange_kr": self.exchange_name_kr,
            "is_connected": connection_state,
            "is_running": self.is_running,
            "uptime": time.time() - self.start_time,
            "symbols": list(self.symbols),
            "message_stats": message_stats,
            "orderbooks": orderbooks
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
        # 연결 상태 업데이트 - 메시지 수신 시 연결됨으로 간주
        if message_type != "error":
            self.metrics_manager.update_connection_state(self.exchange_code, "connected")
        else:
            # 에러 메시지는 연결 상태에 영향을 주지 않음
            pass

    def _send_to_output_queue(self, symbol: str, orderbook: Any, is_valid: bool) -> None:
        """
        검증된 오더북 데이터를 출력 큐에 전송
        
        Args:
            symbol: 심볼
            orderbook: 오더북 데이터
            is_valid: 데이터 유효성 여부
        """
        if is_valid and self.output_queue and orderbook:
            # 오더북 데이터가 객체일 경우 dict로 변환
            data = orderbook.to_dict() if hasattr(orderbook, 'to_dict') else orderbook
            
            self.output_queue.put_nowait({
                "exchange": self.exchange_code,
                "symbol": symbol,
                "timestamp": time.time(),
                "data": data
            })


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
