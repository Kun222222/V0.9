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
from crosskimp.config.paths import LOG_SUBDIRS
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
from crosskimp.ob_collector.orderbook.metric.metrics_manager import WebsocketMetricsManager

# ============================
# 거래소 이름 및 상수
# ============================
# 거래소 코드 -> 한글 이름 매핑 (모두 대문자로 통일)
EXCHANGE_NAMES_KR = {
    "UPBIT": "[업비트]",
    "BYBIT": "[바이빗]",
    "BINANCE": "[바이낸스]",
    "BITHUMB": "[빗썸]",
    "BINANCE_FUTURE": "[바이낸스 선물]",
    "BYBIT_FUTURE": "[바이빗 선물]",
}

# 로깅 관련 상수
LOG_SYSTEM = {
    "warning": "⚠️",
    "error": "❌",
    "info": "ℹ️",
    "success": "✅",
    "wait": "⏳",
    "system": "🔧"
}

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
        OrderManager 초기화

        Args:
            settings: 설정 정보
            exchange_code: 거래소 코드 (대문자로 전달 필요)
        """
        # 설정 저장
        self.settings = settings
        
        # 거래소 정보 (대문자로 전달 받음)
        self.exchange_code = exchange_code  # 대문자로 전달받아야 함
        self.exchange_name_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        
        # 출력 큐
        self.output_queue = None
        
        # 구독 관리자
        self.subscription = None
        
        # 연결 상태 콜백
        self.connection_status_callback = None
        
        # 거래소별 컴포넌트 설정
        self.config = self._get_exchange_config()
        
        # 로거 설정
        self.logger = logger
        
        # 메트릭 매니저 싱글톤 인스턴스 사용
        self.metrics_manager = WebsocketMetricsManager.get_instance()
        self.metrics_manager.initialize_exchange(self.exchange_code)
        
        # 컴포넌트들
        self.connection = None       # 웹소켓 연결 객체
        self.validator = None        # 검증 객체
        
        # 구독 심볼 관리
        self.symbols = set()
        
        # 상태 관리
        self.is_running = False
        self.tasks = {}
        
        # 외부 콜백
        self.start_time = time.time()
        
        logger.info(f"{self.exchange_name_kr} 오더북 관리자 초기화")
    
    def _get_exchange_config(self) -> dict:
        """
        거래소별 기본 설정 가져오기
        
        Returns:
            dict: 거래소별 기본 설정
        """
        # 간소화된 거래소별 컴포넌트 매핑
        EXCHANGE_COMPONENTS = {
            "UPBIT": {
                "connection": "crosskimp.ob_collector.orderbook.connection.upbit_s_cn.UpbitWebSocketConnector",
                "subscription": "crosskimp.ob_collector.orderbook.subscription.upbit_s_sub.UpbitSubscription",
                "supports_delta": False
            },
            "BYBIT": {
                "connection": "crosskimp.ob_collector.orderbook.connection.bybit_s_cn.BybitWebSocketConnector",
                "subscription": "crosskimp.ob_collector.orderbook.subscription.bybit_s_sub.BybitSubscription",
                "supports_delta": True
            },
            "BYBIT_FUTURE": {
                "connection": "crosskimp.ob_collector.orderbook.connection.bybit_f_cn.BybitFutureWebSocketConnector",
                "subscription": "crosskimp.ob_collector.orderbook.subscription.bybit_f_sub.BybitFutureSubscription",
                "supports_delta": True
            },
            "BINANCE": {
                "connection": "crosskimp.ob_collector.orderbook.connection.binance_s_cn.BinanceWebSocketConnector",
                "subscription": "crosskimp.ob_collector.orderbook.subscription.binance_s_sub.BinanceSubscription",
                "supports_delta": True
            },
            "BINANCE_FUTURE": {
                "connection": "crosskimp.ob_collector.orderbook.connection.binance_f_cn.BinanceFutureWebSocketConnector",
                "subscription": "crosskimp.ob_collector.orderbook.subscription.binance_f_sub.BinanceFutureSubscription",
                "supports_delta": True
            },
            "BITHUMB": {
                "connection": "crosskimp.ob_collector.orderbook.connection.bithumb_s_cn.BithumbWebSocketConnector",
                "subscription": "crosskimp.ob_collector.orderbook.subscription.bithumb_s_sub.BithumbSubscription",
                "supports_delta": False
            }
        }
        
        # 해당 거래소 설정 가져오기
        if self.exchange_code not in EXCHANGE_COMPONENTS:
            logger.error(f"지원하지 않는 거래소 코드: {self.exchange_code}")
            return {}
            
        return {
            "components": EXCHANGE_COMPONENTS[self.exchange_code],
            "validator": "crosskimp.ob_collector.orderbook.validator.validators.BaseOrderBookValidator",
            "supports_delta": EXCHANGE_COMPONENTS[self.exchange_code].get("supports_delta", True)
        }
    
    async def initialize(self) -> bool:
        """
        모든 컴포넌트 초기화
        
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            # 거래소 설정이 없는 경우
            if not self.config:
                logger.error(f"{self.exchange_name_kr} 설정을 찾을 수 없습니다")
                return False
            
            # 필요한 컴포넌트 경로 가져오기
            connection_path = self.config["components"].get("connection")
            subscription_path = self.config["components"].get("subscription")
            validator_path = self.config.get("validator")
            
            # 필수 컴포넌트 경로 검증
            if not connection_path or not subscription_path:
                logger.error(f"{self.exchange_name_kr} 필수 컴포넌트 경로가 없습니다")
                return False
            
            # 동적으로 컴포넌트 로드
            import importlib
            
            try:
                # 컴포넌트 경로 분리
                conn_parts = connection_path.split(".")
                sub_parts = subscription_path.split(".")
                
                # 모듈과 클래스 분리
                conn_module_path, conn_class_name = ".".join(conn_parts[:-1]), conn_parts[-1]
                sub_module_path, sub_class_name = ".".join(sub_parts[:-1]), sub_parts[-1]
                
                # connection과 subscription 모듈 로드 (필수)
                try:
                    conn_module = importlib.import_module(conn_module_path)
                    conn_class = getattr(conn_module, conn_class_name)
                except (ImportError, AttributeError) as e:
                    logger.error(f"{self.exchange_name_kr} 연결 컴포넌트 로드 실패: {str(e)}")
                    return False
                    
                try:
                    sub_module = importlib.import_module(sub_module_path)
                    sub_class = getattr(sub_module, sub_class_name)
                except (ImportError, AttributeError) as e:
                    logger.error(f"{self.exchange_name_kr} 구독 컴포넌트 로드 실패: {str(e)}")
                    return False
                
                # validator 모듈 로드 (선택적)
                validator_class = None
                if validator_path:
                    try:
                        validator_parts = validator_path.split(".")
                        validator_module_path, validator_class_name = ".".join(validator_parts[:-1]), validator_parts[-1]
                        validator_module = importlib.import_module(validator_module_path)
                        validator_class = getattr(validator_module, validator_class_name)
                    except (ImportError, AttributeError) as e:
                        logger.warning(f"{self.exchange_name_kr} 검증기 컴포넌트 로드 실패: {str(e)}")
                        # 검증기는 선택적이므로 계속 진행
                
                # 컴포넌트 인스턴스 생성
                # 먼저 연결 객체 생성
                try:
                    self.connection = conn_class(self.settings)
                except Exception as e:
                    logger.error(f"{self.exchange_name_kr} 연결 객체 생성 실패: {str(e)}")
                    return False
                
                # 검증기 객체 생성 (선택적)
                if validator_class:
                    try:
                        self.validator = validator_class(self.exchange_code)
                    except Exception as e:
                        logger.warning(f"{self.exchange_name_kr} 검증기 객체 생성 실패: {str(e)}")
                        # 계속 진행

                # 구독 객체 생성 (필수)
                try:
                    # 파서 없이 구독 객체 생성
                    self.subscription = sub_class(self.connection)
                except Exception as e:
                    logger.error(f"{self.exchange_name_kr} 구독 객체 생성 실패: {str(e)}")
                    self.connection = None  # 생성된 객체 정리
                    return False
                
                # 리소스 연결
                if self.output_queue:
                    self.set_output_queue(self.output_queue)
                
                # 컨넥션에 콜백 설정
                if self.connection and self.connection_status_callback:
                    self.connection.set_connection_status_callback(
                        lambda status: self.update_connection_status(self.exchange_code, status)
                    )
                
                logger.info(f"{self.exchange_name_kr} 컴포넌트 초기화 완료")
                return True
                
            except Exception as e:
                logger.error(f"{self.exchange_name_kr} 컴포넌트 로드 중 예외 발생: {str(e)}", exc_info=True)
                return False
            
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 초기화 실패: {str(e)}", exc_info=True)
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
                logger.warning(f"{self.exchange_name_kr} 심볼이 없어 오더북 수집을 시작하지 않습니다")
                return False
                
            # 필수 컴포넌트 검증
            if not self.connection:
                logger.error(f"{self.exchange_name_kr} 연결 객체가 초기화되지 않았습니다. 먼저 initialize()를 호출해야 합니다.")
                return False
                
            if not self.subscription:
                logger.error(f"{self.exchange_name_kr} 구독 객체가 초기화되지 않았습니다. 먼저 initialize()를 호출해야 합니다.")
                return False
            
            # 이미 실행 중인 경우 처리
            if self.is_running:
                # 새로운 심볼만 추가
                new_symbols = [s for s in symbols if s not in self.symbols]
                if new_symbols:
                    logger.info(f"{self.exchange_name_kr} 추가 심볼 구독: {len(new_symbols)}개")
                    self.symbols.update(new_symbols)
                    await self.subscription.subscribe(new_symbols)
                return True
            
            # 심볼 저장
            self.symbols = set(symbols)
            
            # 시작 상태 설정
            self.is_running = True
            
            try:
                # 연결 및 구독 시작
                await self.connection.connect()
                
                # 구독 시작 (심볼에 따른)
                subscription_task = asyncio.create_task(
                    self.subscription.subscribe(list(self.symbols))
                )
                self.tasks["subscription"] = subscription_task
                
                # 심볼 개수 로깅
                logger.info(f"{self.exchange_name_kr} 오더북 수집 시작 - 심볼 {len(self.symbols)}개")
                
                # 메트릭 업데이트 태스크 시작
                self._start_metric_tasks()
                
                return True
            except Exception as e:
                self.is_running = False
                logger.error(f"{self.exchange_name_kr} 연결 및 구독 중 오류: {str(e)}", exc_info=True)
                return False
            
        except Exception as e:
            self.is_running = False
            logger.error(f"{self.exchange_name_kr} 오더북 수집 시작 실패: {str(e)}", exc_info=True)
            return False
    
    async def stop(self) -> None:
        """오더북 수집 중지"""
        try:
            if not self.is_running:
                return
                
            # 구독 중인 모든 심볼 구독 취소
            if self.subscription:
                await self.subscription.unsubscribe(None)
            
            # 연결 종료 (이미 unsubscribe에서 처리했으므로 별도 처리 불필요)
            
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
        # 큐 설정 및 하위 컴포넌트에 전달
        self.output_queue = queue
        
        # 컴포넌트에 출력 큐 설정
        if self.validator:
            self.validator.set_output_queue(queue)
        if self.subscription:
            self.subscription.set_output_queue(queue)
            
        logger.debug(f"{self.exchange_name_kr} 출력 큐 설정 완료")
    
    @property
    def is_connected(self) -> bool:
        """
        현재 연결 상태 확인
        
        Returns:
            bool: 현재 연결 상태
        """
        # 연결 객체가 있으면 그 상태를 직접 사용
        if hasattr(self, 'connection') and self.connection:
            return self.connection.is_connected
        # 구독 객체가 있으면 그 상태를 사용
        elif hasattr(self, 'subscription') and self.subscription:
            return self.subscription.is_connected
        # 아무 것도 없으면 메트릭 매니저에서 상태 확인
        else:
            return self.metrics_manager.is_connected(self.exchange_code)
    
    def update_connection_status(self, exchange_code=None, status=None):
        """
        연결 상태 업데이트 (외부 이벤트에 의한 업데이트)
        
        Args:
            exchange_code: 거래소 코드 (기본값: self.exchange_code)
            status: 상태 ('connected' 또는 'disconnected')
        """
        # 거래소 코드가 없으면 기본값 사용
        exchange = exchange_code or self.exchange_code
        
        # 상태가 있는 경우에만 메트릭 매니저에 위임
        if status is not None:
            self.metrics_manager.update_connection_state(exchange, status)
        
        # 콜백이 설정되어 있으면 호출
        if self.connection_status_callback:
            self.connection_status_callback(exchange, status)
    
    async def _handle_snapshot(self, symbol: str, data: Dict) -> None:
        """
        스냅샷 처리 - 이제 BaseSubscription에서 처리
        
        Args:
            symbol: 심볼
            data: 스냅샷 데이터
        """
        pass
    
    async def _handle_delta(self, symbol: str, data: Dict) -> None:
        """
        델타 처리 - 이제 BaseSubscription에서 처리
        
        Args:
            symbol: 심볼
            data: 델타 데이터
        """
        pass
    
    def _on_error(self, symbol: str, error: str) -> None:
        """
        에러 콜백 - 이제 BaseSubscription에서 처리
        
        Args:
            symbol: 심볼
            error: 오류 메시지
        """
        pass
    
    def set_connection_status_callback(self, callback: Callable) -> None:
        """
        연결 상태 콜백 설정
        
        Args:
            callback: 콜백 함수 (exchange, status) -> None
        """
        self.connection_status_callback = callback
        
        # 하위 구성 요소에도 콜백 적용
        if self.connection:
            # 컨넥터 콜백 설정 (래핑된 콜백)
            self.connection.set_connection_status_callback(
                lambda status: self.update_connection_status(self.exchange_code, status)
            )
        
        logger.debug(f"{self.exchange_name_kr} 연결 상태 콜백 설정 완료")

    def _start_metric_tasks(self) -> None:
        """
        메트릭 업데이트 태스크 시작
        """
        # 연결 상태 검사 태스크
        self.tasks["connection_check"] = asyncio.create_task(
            self._check_connection_task()
        )
        
        # 다른 메트릭 태스크가 필요하면 여기에 추가
        logger.debug(f"{self.exchange_name_kr} 메트릭 태스크 시작")

    async def _check_connection_task(self) -> None:
        """
        연결 상태 검사 태스크 (주기적으로 연결 상태 체크)
        """
        try:
            # 체크 간격(초)
            check_interval = self.settings.get("connection_check_interval", 5)
            
            while self.is_running:
                # 현재 상태 체크
                is_connected = self.is_connected
                
                # 메트릭 매니저에 상태 업데이트
                self.metrics_manager.update_connection_state(
                    self.exchange_code,
                    "connected" if is_connected else "disconnected"
                )
                
                # 연결이 끊어졌을 때 처리
                if not is_connected and self.is_running:
                    # 마지막 연결 시간 체크
                    elapsed = time.time() - self.connection.last_activity_time
                    limit = self.settings.get("reconnect_threshold", 60)
                    
                    # 오래 연결이 끊어진 경우 재연결 시도
                    if elapsed > limit:
                        logger.warning(f"{self.exchange_name_kr} 연결이 {int(elapsed)}초 동안 없음, 재연결 시도 중")
                        
                        # 재연결 및 구독 시도
                        try:
                            await self.connection.reconnect()
                            if self.symbols:
                                await self.subscription.subscribe(list(self.symbols))
                        except Exception as e:
                            logger.error(f"{self.exchange_name_kr} 재연결 실패: {str(e)}")
                
                # 지정된 간격만큼 대기
                await asyncio.sleep(check_interval)
                
        except asyncio.CancelledError:
            logger.debug(f"{self.exchange_name_kr} 연결 상태 검사 태스크 취소됨")
        except Exception as e:
            logger.error(f"{self.exchange_name_kr} 연결 상태 검사 중 오류: {str(e)}")

    async def get_status(self) -> Dict[str, Any]:
        """
        현재 상태 정보 가져오기
        
        Returns:
            Dict: 상태 정보
        """
        # 가동 시간 계산
        uptime = time.time() - self.start_time
        
        # 메트릭 정보 가져오기
        metrics = self.metrics_manager.get_exchange_metrics(self.exchange_code)
        
        # 연결 정보
        connection_info = {}
        if self.connection:
            connection_info = {
                "is_connected": self.connection.is_connected,
                "last_activity": datetime.fromtimestamp(self.connection.last_activity_time).isoformat(),
                "reconnect_count": self.connection.reconnect_count
            }
        
        # 종합 상태 정보
        status = {
            "exchange": self.exchange_code,
            "exchange_kr": self.exchange_name_kr,
            "is_running": self.is_running,
            "uptime_seconds": uptime,
            "uptime_formatted": f"{int(uptime // 3600)}시간 {int((uptime % 3600) // 60)}분 {int(uptime % 60)}초",
            "symbols_count": len(self.symbols),
            "connection": connection_info,
            "metrics": metrics
        }
        
        return status


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
        # 지원하는 거래소 목록 (언더스코어가 있는 형식으로 통일)
        supported_exchanges = ["UPBIT", "BYBIT", "BINANCE", "BITHUMB", "BINANCE_FUTURE", "BYBIT_FUTURE"]
        
        # 항상 대문자로 처리
        exchange_code = exchange.upper()
        
        # 지원하는 거래소인지 확인
        if exchange_code not in supported_exchanges:
            exchange_korean = EXCHANGE_NAMES_KR.get(exchange_code, f"[{exchange_code}]")
            logger.warning(f"{exchange_korean} 지원되지 않는 거래소")
            return None
        
        # OrderManager 인스턴스 생성
        return OrderManager(settings, exchange_code)
        
    except Exception as e:
        exchange_code = exchange.upper()
        exchange_korean = EXCHANGE_NAMES_KR.get(exchange_code, f"[{exchange_code}]")
        logger.error(f"{exchange_korean} OrderManager 생성 실패: {str(e)}", exc_info=True)
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
            # 대문자로 통일
            exchange_code = exchange.upper()
            exchange_korean = EXCHANGE_NAMES_KR.get(exchange_code, f"[{exchange_code}]")
            
            if not symbols:
                logger.info(f"{exchange_korean} 심볼이 없어 OrderManager를 초기화하지 않습니다.")
                continue
                
            # OrderManager 생성
            manager = create_order_manager(exchange_code, settings)
            if not manager:
                logger.error(f"{exchange_korean} OrderManager 생성 실패")
                continue
                
            # 출력 큐 공유
            manager.set_output_queue(ws_manager.output_queue)
            
            # 연결 상태 콜백 공유
            manager.set_connection_status_callback(
                lambda ex, status: ws_manager.update_connection_status(ex, status)
            )
            
            # 초기화 수행
            init_success = await manager.initialize()
            if not init_success:
                logger.error(f"{exchange_korean} 초기화 실패, 해당 거래소는 건너뜁니다.")
                continue
            
            # 시작 수행
            start_success = await manager.start(symbols)
            if not start_success:
                logger.error(f"{exchange_korean} 시작 실패, 해당 거래소는 건너뜁니다.")
                continue
            
            # WebsocketManager에 OrderManager 저장
            ws_manager.order_managers[exchange_code] = manager
            
            logger.info(f"{exchange_korean} OrderManager가 WebsocketManager와 통합되었습니다.")
        
        return True
        
    except Exception as e:
        logger.error(f"OrderManager 통합 중 오류 발생: {str(e)}", exc_info=True)
        return False
