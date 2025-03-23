"""
거래소 이벤트 처리 및 통합 관리 모듈

이 모듈은 거래소 이벤트 처리 및 통합 관리 클래스를 제공합니다.
각 거래소별 이벤트 핸들러 인스턴스를 생성하여 거래소 이벤트를 처리합니다.
"""

import time
import logging
import asyncio
import inspect
from typing import Dict, List, Any, Optional, Set, Tuple

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import EXCHANGE_NAMES_KR
from crosskimp.common.events.domains.orderbook import OrderbookEventTypes
from crosskimp.ob_collector.orderbook.util.event_adapter import get_event_adapter
from crosskimp.system_manager.metric_manager import get_metric_manager, MetricKeys
from crosskimp.system_manager.notification_manager import get_notification_manager, NotificationType

# 로거 설정
logger = get_unified_logger()

class LoggingMixin:
    """
    로깅 믹스인 클래스
    
    로깅 기능을 제공하는 믹스인 클래스입니다.
    이벤트 핸들러 및 기타 클래스에서 로깅 기능을 사용할 수 있도록 합니다.
    """
    
    def setup_logger(self, exchange_code: str):
        """
        로거 설정
        
        Args:
            exchange_code: 거래소 코드
        """
        self.exchange_code = exchange_code.lower()
        self.exchange_name_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        self._logger = logger  # 전역 로거 참조
    
    def log_error(self, message: str, exc_info=False) -> None:
        """오류 로깅 (거래소 이름 포함)"""
        # 재귀 방지를 위한 여부 확인
        is_wrapper = False
        for frame_info in inspect.stack()[1:3]:  # 호출 스택 확인
            if frame_info.function == 'log_error' and 'base_subscription.py' in frame_info.filename:
                is_wrapper = True
                break
        
        # 래핑된 호출이면 메시지만 기록
        if is_wrapper:
            self._logger.error(message, exc_info=exc_info)
        # 일반 호출이면 거래소 이름 포함해서 기록
        else:
            if not hasattr(self, 'exchange_name_kr'):
                self._logger.error(message, exc_info=exc_info)
            else:
                self._logger.error(f"{self.exchange_name_kr} {message}", exc_info=exc_info)
    
    def log_warning(self, message: str, exc_info=False) -> None:
        """경고 로깅 (거래소 이름 포함)"""
        # 재귀 방지를 위한 여부 확인
        is_wrapper = False
        for frame_info in inspect.stack()[1:3]:  # 호출 스택 확인
            if frame_info.function == 'log_warning' and 'base_subscription.py' in frame_info.filename:
                is_wrapper = True
                break
        
        # 래핑된 호출이면 메시지만 기록
        if is_wrapper:
            self._logger.warning(message, exc_info=exc_info)
        # 일반 호출이면 거래소 이름 포함해서 기록
        else:
            if not hasattr(self, 'exchange_name_kr'):
                self._logger.warning(message, exc_info=exc_info)
            else:
                self._logger.warning(f"{self.exchange_name_kr} {message}", exc_info=exc_info)
    
    def log_info(self, message: str, exc_info=False) -> None:
        """정보 로깅 (거래소 이름 포함)"""
        # 재귀 방지를 위한 여부 확인
        is_wrapper = False
        for frame_info in inspect.stack()[1:3]:  # 호출 스택 확인
            if frame_info.function == 'log_info' and 'base_subscription.py' in frame_info.filename:
                is_wrapper = True
                break
        
        # 래핑된 호출이면 메시지만 기록
        if is_wrapper:
            self._logger.info(message, exc_info=exc_info)
        # 일반 호출이면 거래소 이름 포함해서 기록
        else:
            if not hasattr(self, 'exchange_name_kr'):
                self._logger.info(message, exc_info=exc_info)
            else:
                self._logger.info(f"{self.exchange_name_kr} {message}", exc_info=exc_info)
    
    def log_debug(self, message: str, exc_info=False) -> None:
        """디버그 로깅 (거래소 이름 포함)"""
        # 재귀 방지를 위한 여부 확인
        is_wrapper = False
        for frame_info in inspect.stack()[1:3]:  # 호출 스택 확인
            if frame_info.function == 'log_debug' and 'base_subscription.py' in frame_info.filename:
                is_wrapper = True
                break
        
        # 래핑된 호출이면 메시지만 기록
        if is_wrapper:
            self._logger.debug(message, exc_info=exc_info)
        # 일반 호출이면 거래소 이름 포함해서 기록
        else:
            if not hasattr(self, 'exchange_name_kr'):
                self._logger.debug(message, exc_info=exc_info)
            else:
                self._logger.debug(f"{self.exchange_name_kr} {message}", exc_info=exc_info)

    def log_critical(self, message: str, exc_info=False) -> None:
        """심각한 오류 로깅 (거래소 이름 포함)"""
        if not hasattr(self, 'exchange_name_kr'):
            self._logger.critical(message, exc_info=exc_info)
        else:
            self._logger.critical(f"{self.exchange_name_kr} {message}", exc_info=exc_info)

class EventHandler(LoggingMixin):
    """
    거래소 이벤트 처리 및 통합 관리 클래스
    
    거래소 이벤트 처리와 이벤트 통합을 담당합니다.
    로깅, 이벤트 발행, 알림 전송 등의 역할을 수행합니다.
    """
    
    _instances = {}  # 거래소 코드 -> 핸들러 인스턴스
    
    @classmethod
    def get_instance(cls, exchange_code: str, settings: Dict[str, Any]) -> 'EventHandler':
        """
        거래소별 이벤트 핸들러 인스턴스 반환 (싱글톤 패턴)
        
        Args:
            exchange_code: 거래소 코드
            settings: 설정 정보
            
        Returns:
            EventHandler: 이벤트 핸들러 인스턴스
        """
        exchange_code = exchange_code.lower()  # 정규화
        
        if exchange_code not in cls._instances:
            cls._instances[exchange_code] = EventHandler(exchange_code, settings)
            
        return cls._instances[exchange_code]
    
    def __init__(self, exchange_code: str, settings: Dict[str, Any]):
        """
        초기화
        
        Args:
            exchange_code: 거래소 코드 (예: 'upbit', 'bithumb', 'bybit' 등)
            settings: 설정 정보
        """
        # 로거 설정 (믹스인 메서드 사용)
        self.setup_logger(exchange_code.lower())
        
        self.settings = settings
        self.exchange_code = exchange_code.lower()
        
        # 거래소 이름 (한글) 설정
        self.exchange_name_kr = EXCHANGE_NAMES_KR.get(self.exchange_code, f"[{self.exchange_code}]")
        
        # 이벤트 버스 가져오기
        self.event_bus = get_event_adapter()
        
        # 연결 상태 저장소 (운영 로직용)
        self.connection_status = "disconnected"
        
        # 구독 상태 저장소 (운영 로직용)
        self.subscriptions = {
            "symbols": [],
            "status": "unsubscribed"
        }
        
        # 알림 제한 관련 변수
        self._last_notification_time = {}  # 이벤트 타입별 마지막 알림 시간
        self._notification_cooldown = 60  # 알림 쿨다운 (초)
    
        # 종료 상태 플래그
        self._is_shutting_down = False
        
        # 메트릭 관리자 참조
        self.metric_manager = get_metric_manager()
        
        # 초기화 완료 로그
        self.log_info(f"{self.exchange_name_kr} 이벤트 핸들러 초기화 완료")

    async def handle_error(self, error_type: str, message: str, severity: str = "error", **kwargs) -> None:
        """
        오류 이벤트 처리
        
        Args:
            error_type: 오류 유형 (예: 'connection_error', 'timeout', 'auth_error' 등)
            message: 오류 메시지
            severity: 심각도 ('error', 'warning', 'critical' 등)
            **kwargs: 추가 데이터
        """
        # 로깅
        if severity == "critical":
            self.log_critical(f"{error_type}: {message}")
        elif severity == "warning":
            self.log_warning(f"{error_type}: {message}")
        else:
            self.log_error(f"{error_type}: {message}")
        
        # 메트릭 업데이트 - 오류 카운트 증가
        self.metric_manager.update_metric(
            self.exchange_code,
            MetricKeys.ERROR_COUNT,
            1,
            op="increment",
            error_type=error_type,
            error_message=message,
            severity=severity,
            timestamp=time.time()
        )
        
        # 메트릭 업데이트 - 마지막 오류 시간
        self.metric_manager.update_metric(
            self.exchange_code,
            MetricKeys.LAST_ERROR_TIME,
            time.time(),
            error_type=error_type
        )
        
        # 텔레그램 알림 전송 (심각한 오류만)
        if severity in ["error", "critical"]:
            # 거래소 이름 포함 오류 메시지 생성
            exchange_name = self.exchange_name_kr.replace('[', '').replace(']', '')
            error_message = f"🚨 {exchange_name} 오류: {error_type} - {message}"
            
            # 알림 관리자를 통한 알림 전송
            notification_manager = get_notification_manager()
            await notification_manager.send_notification(
                message=error_message,
                notification_type=NotificationType.ERROR,
                source=f"event_handler_{self.exchange_code}",
                metadata={"error_type": error_type, "severity": severity}
            )

    def update_metrics(self, metric_name: str, value: float = 1.0, op: str = "increment", **kwargs) -> None:
        """
        메트릭 업데이트 - 구독 클래스에서 호출하는 메서드
        
        Args:
            metric_name: 메트릭 이름
            value: 메트릭 값
            op: 연산자 (increment, set, max, min)
            **kwargs: 추가 파라미터
        """
        try:
            # 메트릭 관리자를 통해 지정된 메트릭 업데이트
            self.metric_manager.update_metric(
                self.exchange_code,
                metric_name,
                value,
                op=op
            )
            
            # 태그가 있는 경우 태그별 메트릭도 업데이트
            for tag_name, tag_value in kwargs.items():
                self.metric_manager.update_metric(
                    self.exchange_code,
                    f"{metric_name}_{tag_name}_{tag_value}",
                    value,
                    op=op
                )
                
        except Exception as e:
            # 메트릭 업데이트 중 오류가 발생해도 주요 로직에 영향을 주지 않도록 함
            logger.error(f"메트릭 업데이트 중 오류: {e}")
            pass  # 에러 무시
    
    def get_status(self) -> Dict[str, Any]:
        """
        상태 정보 조회
        
        Returns:
            Dict: 상태 정보
        """
        # 기본 정보로 보강
        exchange_status = {
            "exchange_code": self.exchange_code,
            "exchange_name": self.exchange_name_kr,
            "connection_status": self.connection_status,
            "subscriptions": self.subscriptions,
            "is_shutting_down": self._is_shutting_down
        }
        
        return exchange_status
    
    def get_exchange_status(self) -> Dict[str, Any]:
        """
        거래소 상태 정보 조회 (축약 버전)
        
        Returns:
            Dict: 거래소 상태 정보
        """
        return {
            "exchange": self.exchange_name_kr,
            "code": self.exchange_code,
            "connection": self.connection_status,
            "subscribed_symbols": len(self.subscriptions["symbols"])
        }

    async def handle_connection_status(self, status: str, timestamp: float = None, **kwargs) -> None:
        """
        연결 상태 변경 이벤트 처리
        
        Args:
            status: 연결 상태 ("connected", "disconnected", "reconnecting")
            timestamp: 이벤트 발생 시간 (None이면 현재 시간)
            **kwargs: 추가 데이터
        """
        # 중복 상태 변경 이벤트 필터링
        if status == self.connection_status:
            return
            
        # 타임스탬프 설정
        if timestamp is None:
            timestamp = time.time()
        
        # 이전 연결 상태 저장
        old_status = self.connection_status
        
        # 현재 연결 상태 업데이트 (운영 로직용 상태 저장)
        self.connection_status = status
        
        # 메트릭 업데이트 (모니터링 및 보고용 상태 저장)
        self.metric_manager.update_metric(
            self.exchange_code,
            MetricKeys.CONNECTION_STATUS,
            status,
            old_status=old_status,
            timestamp=timestamp,
            message=kwargs.get('message', '')
        )
        
        # 상태 변경 시간 메트릭 업데이트
        self.metric_manager.update_metric(
            self.exchange_code,
            MetricKeys.LAST_STATE_CHANGE,
            timestamp
        )
        
        # 로그 기록
        status_msg = kwargs.get('message', '')
        status_emoji = {
            'connected': '🟢',
            'disconnected': '🔴',
            'reconnecting': '🔵',
        }.get(status, '')
        
        # 연결 상태 변경 로그
        if status_msg:
            self.log_info(f"{status_emoji} 연결 상태 변경: {old_status} → {status} ({status_msg})")
        else:
            self.log_info(f"{status_emoji} 연결 상태 변경: {old_status} → {status}")
        
        # 심각한 상태인 경우 알림 메시지 전송
        need_notification = False
        
        # 연결 끊김: 알림 필요
        if old_status == "connected" and status == "disconnected":
            need_notification = True
            alert_message = f"🔴 {self.exchange_name_kr} 연결 끊김"
        
        # 최초 연결 성공: 알림 필요
        elif old_status == "disconnected" and status == "connected" and kwargs.get('initial_connection', False):
            need_notification = True
            alert_message = f"🟢 {self.exchange_name_kr} 연결됨"
        # 재연결 성공: 알림 필요
        elif old_status in ["disconnected", "reconnecting"] and status == "connected":
            need_notification = True
            alert_message = f"{self.exchange_name_kr} 연결 복구됨"
            
        # 텔레그램 알림 전송
        if need_notification:
            asyncio.create_task(
                self.send_telegram_message("connection_status", alert_message)
            )
            
        # 이벤트 발행 (시스템 컴포넌트들이 구독할 수 있음)
        event_data = {
            "exchange_code": self.exchange_code,
            "status": status,
            "old_status": old_status,
            "timestamp": timestamp,
            "message": status_msg
        }
        await self.event_bus.publish(OrderbookEventTypes.CONNECTION_STATUS, event_data)

    async def handle_subscription_status(self, status: str, symbols: list, **kwargs) -> None:
        """
        구독 상태 이벤트 처리
        
        Args:
            status: 구독 상태 ('subscribed', 'unsubscribed', 'error' 등)
            symbols: 구독 중인 심볼 목록
            **kwargs: 추가 데이터
        """
        # 로깅
        self.log_info(f"구독 상태 변경: {status}, 심볼: {len(symbols)}개")
        
        # 타임스탬프 설정
        timestamp = kwargs.get("timestamp", time.time())
        
        # 상태 저장 (운영 로직용)
        self.subscriptions = {
            "status": status,
            "symbols": symbols,
            "timestamp": timestamp
        }
        
        # 메트릭 업데이트 (모니터링 및 보고용)
        status_value = 1 if status == 'subscribed' else 0
        self.metric_manager.update_metric(
            self.exchange_code,
            "subscription_status",
            status_value,
            status=status,
            symbols_count=len(symbols),
            timestamp=timestamp
        )
        
        # 심볼 수 메트릭 업데이트
        self.metric_manager.update_metric(
            self.exchange_code,
            "subscribed_symbols_count",
            len(symbols),
            timestamp=timestamp
        )
        
        # 이벤트 버스 발행
        event_data = {
            "exchange_code": self.exchange_code,
            "status": status,
            "symbols": symbols,
            "count": len(symbols),
            "timestamp": timestamp
        }
        await self.event_bus.publish(OrderbookEventTypes.SUBSCRIPTION_STATUS, event_data)
    
    async def handle_message_received(self, message_type: str, size: int = 0, **kwargs) -> None:
        """
        메시지 수신 이벤트 처리
        
        Args:
            message_type: 메시지 유형 ('snapshot', 'delta', 'heartbeat' 등)
            size: 메시지 크기 (바이트)
            **kwargs: 추가 데이터
        """
        # 타임스탬프 설정
        timestamp = time.time()
        
        # 메트릭 업데이트
        # 총 메시지 수 증가
        self.metric_manager.update_metric(
            self.exchange_code,
            MetricKeys.MESSAGE_COUNT,
            1,
            op="increment",
            timestamp=timestamp
        )
        
        # 메시지 타입별 카운트 업데이트
        if message_type == 'snapshot':
            self.metric_manager.update_metric(
                self.exchange_code,
                MetricKeys.SNAPSHOT_COUNT,
                1,
                op="increment",
                timestamp=timestamp
            )
        elif message_type == 'delta':
            self.metric_manager.update_metric(
                self.exchange_code,
                MetricKeys.DELTA_COUNT,
                1,
                op="increment",
                timestamp=timestamp
            )
        
        # 마지막 메시지 시간 업데이트
        self.metric_manager.update_metric(
            self.exchange_code,
            MetricKeys.LAST_MESSAGE_TIME,
            timestamp
        )
        
        # 이벤트 버스를 통한 메시지 이벤트 발행
        event_data = {
            "exchange_code": self.exchange_code,
            "message_type": message_type,
            "size": size,
            "timestamp": timestamp
        }
        
        # 추가 데이터 병합
        for key, value in kwargs.items():
            event_data[key] = value
            
        # 필요한 경우 여기서 이벤트 발행 구현

    async def handle_data_event(self, event_type: str, symbol: str, data: Any, **kwargs) -> None:
        """
        데이터 이벤트 처리
        
        Args:
            event_type: 이벤트 타입 ('snapshot', 'delta' 등)
            symbol: 심볼명
            data: 이벤트 데이터
            **kwargs: 추가 데이터
        """
        # 타임스탬프 설정
        timestamp = kwargs.get("timestamp", time.time())
        
        # 처리 시간 메트릭 업데이트 (제공된 경우)
        if "processing_time" in kwargs:
            processing_time = kwargs["processing_time"]
            self.metric_manager.update_metric(
                self.exchange_code,
                MetricKeys.PROCESSING_TIME,
                processing_time,
                symbol=symbol,
                event_type=event_type,
                timestamp=timestamp
            )
        
        # 이벤트 버스 발행
        event_data = {
            "exchange_code": self.exchange_code,
            "symbol": symbol,
            "data": data,
            "timestamp": timestamp
        }
        
        # 추가 데이터 병합
        for key, value in kwargs.items():
            if key != "timestamp":  # timestamp는 이미 처리함
                event_data[key] = value
        
        await self.event_bus.publish(event_type, event_data)

    def set_shutting_down(self) -> None:
        """
        프로그램 종료 상태 설정
        
        이 메서드는 프로그램이 종료 중임을 표시하여 
        불필요한 로깅이나 알림을 방지합니다.
        """
        self._is_shutting_down = True
        self.log_debug("종료 상태로 설정됨")

    async def publish_system_event(self, event_type: str, **data) -> None:
        """
        시스템 이벤트 발행
        
        Args:
            event_type: 이벤트 타입
            **data: 이벤트 데이터
        """
        try:
            # 기본 이벤트 데이터 생성
            event_data = {
                "exchange_code": self.exchange_code,
                "timestamp": time.time()
            }
            
            # 추가 데이터 병합
            if data:
                event_data.update(data)
            
            # 이벤트 버스를 통해 발행
            if hasattr(self, 'event_bus') and self.event_bus:
                # 이벤트 타입 매핑
                bus_event_type = event_type
                
                # 문자열 기반 이벤트 타입을 적절한 OrderbookEventTypes 상수로 매핑
                if event_type == "SYSTEM_STARTUP" or event_type.lower() == "system_startup":
                    bus_event_type = OrderbookEventTypes.CONNECTION_STATUS
                elif event_type == "SYSTEM_SHUTDOWN" or event_type.lower() == "system_shutdown":
                    bus_event_type = OrderbookEventTypes.CONNECTION_CLOSED
                elif event_type == "CONNECTION_STATUS" or event_type.lower() == "connection_status":
                    bus_event_type = OrderbookEventTypes.CONNECTION_STATUS
                elif event_type == "SUBSCRIPTION_STATUS" or event_type.lower() == "subscription_status":
                    bus_event_type = OrderbookEventTypes.SUBSCRIPTION_STATUS
                elif event_type == "ERROR_EVENT" or event_type.lower() == "error_event":
                    bus_event_type = OrderbookEventTypes.ERROR_EVENT
                    
                # 이벤트 발행
                await self.event_bus.publish(bus_event_type, event_data)
            else:
                self.log_warning(f"이벤트 버스가 없어 이벤트 발행 불가: {event_type}")
                
        except Exception as e:
            self.log_error(f"시스템 이벤트 발행 중 오류: {str(e)}", exc_info=True)

    async def send_telegram_message(self, event_type: str, message: str):
        """
        텔레그램 알림 전송 (notification_manager 사용)
        
        Args:
            event_type: 이벤트 유형
            message: 전송할 메시지
        """
        # 이벤트 유형에 따른 알림 타입 설정
        notification_type = NotificationType.INFO
        if "error" in event_type.lower():
            notification_type = NotificationType.ERROR
        elif "warning" in event_type.lower():
            notification_type = NotificationType.WARNING
        
        # 메시지에 거래소 정보 추가
        if not message.startswith(f"[{self.exchange_code}]"):
            message = f"[{self.exchange_code}] {message}"
        
        # notification_manager를 통해 메시지 전송
        try:
            notification_manager = get_notification_manager()
            await notification_manager.send_notification(
                message=message,
                notification_type=notification_type,
                source="ob_collector",
                key=f"{self.exchange_code}:{event_type}:{hash(message)}",
                metadata={"exchange_code": self.exchange_code}
            )
        except Exception as e:
            # 실패 시 로깅만 수행 (중요 알림이 누락되지 않도록)
            self.log_error(f"텔레그램 메시지 전송 실패: {str(e)}")