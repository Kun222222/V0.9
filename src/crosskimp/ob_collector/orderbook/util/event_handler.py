"""
이벤트 핸들러 모듈

이 모듈은 거래소 이벤트 처리를 위한 핸들러 클래스를 제공합니다.
기존에 각 거래소 커넥터 및 이벤트 관련 파일에 흩어져 있던 이벤트 처리 기능을 중앙화합니다.
"""

import asyncio
import time
from datetime import datetime
from typing import Dict, Optional, Any, List
import inspect
from crosskimp.logger.logger import get_unified_logger
from crosskimp.telegrambot.telegram_notification import send_telegram_message
from crosskimp.config.constants_v3 import EXCHANGE_NAMES_KR
from crosskimp.ob_collector.orderbook.util.event_bus import EventBus, EVENT_TYPES

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class LoggingMixin:
    """
    로깅 기능을 제공하는 믹스인 클래스
    
    이 클래스는 거래소 이름을 포함한 일관된 로그 포맷팅을 제공합니다.
    여러 클래스에서 중복으로 구현된 로깅 메서드들을 통합합니다.
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
    
    def log_error(self, message: str) -> None:
        """오류 로깅 (거래소 이름 포함)"""
        # 재귀 방지를 위한 여부 확인
        is_wrapper = False
        for frame_info in inspect.stack()[1:3]:  # 호출 스택 확인
            if frame_info.function == 'log_error' and 'base_subscription.py' in frame_info.filename:
                is_wrapper = True
                break
        
        # 래핑된 호출이면 메시지만 기록
        if is_wrapper:
            self._logger.error(message)
        # 일반 호출이면 거래소 이름 포함해서 기록
        else:
            if not hasattr(self, 'exchange_name_kr'):
                self._logger.error(message)
            else:
                self._logger.error(f"{self.exchange_name_kr} {message}")
    
    def log_warning(self, message: str) -> None:
        """경고 로깅 (거래소 이름 포함)"""
        # 재귀 방지를 위한 여부 확인
        is_wrapper = False
        for frame_info in inspect.stack()[1:3]:  # 호출 스택 확인
            if frame_info.function == 'log_warning' and 'base_subscription.py' in frame_info.filename:
                is_wrapper = True
                break
        
        # 래핑된 호출이면 메시지만 기록
        if is_wrapper:
            self._logger.warning(message)
        # 일반 호출이면 거래소 이름 포함해서 기록
        else:
            if not hasattr(self, 'exchange_name_kr'):
                self._logger.warning(message)
            else:
                self._logger.warning(f"{self.exchange_name_kr} {message}")
    
    def log_info(self, message: str) -> None:
        """정보 로깅 (거래소 이름 포함)"""
        # 재귀 방지를 위한 여부 확인
        is_wrapper = False
        for frame_info in inspect.stack()[1:3]:  # 호출 스택 확인
            if frame_info.function == 'log_info' and 'base_subscription.py' in frame_info.filename:
                is_wrapper = True
                break
        
        # 래핑된 호출이면 메시지만 기록
        if is_wrapper:
            self._logger.info(message)
        # 일반 호출이면 거래소 이름 포함해서 기록
        else:
            if not hasattr(self, 'exchange_name_kr'):
                self._logger.info(message)
            else:
                self._logger.info(f"{self.exchange_name_kr} {message}")
    
    def log_debug(self, message: str) -> None:
        """디버그 로깅 (거래소 이름 포함)"""
        # 재귀 방지를 위한 여부 확인
        is_wrapper = False
        for frame_info in inspect.stack()[1:3]:  # 호출 스택 확인
            if frame_info.function == 'log_debug' and 'base_subscription.py' in frame_info.filename:
                is_wrapper = True
                break
        
        # 래핑된 호출이면 메시지만 기록
        if is_wrapper:
            self._logger.debug(message)
        # 일반 호출이면 거래소 이름 포함해서 기록
        else:
            if not hasattr(self, 'exchange_name_kr'):
                self._logger.debug(message)
            else:
                # 디버그 메시지를 INFO 레벨로 출력하여 항상 보이도록 함
                self._logger.info(f"{self.exchange_name_kr} {message}")

class EventHandler(LoggingMixin):
    """
    거래소 이벤트 처리 및 통합 관리 클래스
    
    거래소 이벤트 처리와 메트릭 관리를 통합하여 책임을 명확히 하고
    중복 로직을 제거합니다. SystemEventManager의 메트릭 관리 기능을
    EventHandler로 통합하여 의존성을 단순화합니다.
    
    [SystemEventManager 통합 과정]
    1. 메트릭 초기화 및 관리 기능 -> initialize_metrics() 메서드로 통합
    2. 상태 조회 기능 -> get_status(), get_metrics(), get_errors() 등으로 통합
    3. 메트릭 요약 로그 -> _log_metrics_summary() 메서드로 통합
    4. 이벤트 처리 -> handle_*() 메서드로 통합
    
    기존의 SystemEventManager는 여러 거래소의 메트릭을 모두 관리했지만,
    EventHandler는 각 거래소별로 인스턴스가 생성되어 단일 거래소만 관리합니다.
    이를 통해 코드의 책임이 명확해지고 중복이 제거됩니다.
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
        
        # 이벤트 버스 가져오기
        self.event_bus = EventBus.get_instance()
        
        # 메트릭 저장소
        self.metrics = {}  # exchange_code -> metrics
        
        # 연결 상태 저장소
        self.connection_status = "disconnected"
        
        # 구독 상태 저장소
        self.subscriptions = {
            "symbols": [],
            "status": "unsubscribed"
        }
        
        # 메시지 카운터
        self.message_counters = {
            "message_count": 0,
            "error_count": 0,
            "processing_time": 0,
            "last_message_time": 0,
            "last_error_time": 0,
            "last_error_message": "",
            "first_message_time": None,
            "message_rate": 0
        }
        
        # 최근 오류 저장소 (최대 5개)
        self.errors = []
        
        # 알림 제한 관련 변수
        self._last_notification_time = {}  # 이벤트 타입별 마지막 알림 시간
        self._notification_cooldown = 60  # 알림 쿨다운 (초)
    
        # 메트릭 요약 로그 관련 변수
        self.last_summary_log_time = time.time()
        self.summary_log_interval = 1.0  # 1초마다 요약 로그 출력
        self.previous_message_count = 0
        
        # 메트릭 요약 로그 태스크 시작
        self._start_summary_log_task()
        
        # 초기화 완료 로그
        self.log_info(f"{self.exchange_name_kr} 이벤트 핸들러 초기화 완료")
    
    def _start_summary_log_task(self):
        """주기적 메트릭 요약 로그 태스크 시작"""
        try:
            asyncio.create_task(self._summary_log_loop())
        except RuntimeError:
            self.log_debug("이벤트 루프가 실행 중이 아니므로 요약 로그 태스크를 시작할 수 없습니다.")
    
    async def _summary_log_loop(self):
        """메트릭 요약 로그 출력 루프"""
        try:
            while True:
                await asyncio.sleep(self.summary_log_interval)
                await self._log_metrics_summary()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log_error(f"메트릭 요약 로그 루프 오류: {e}")
    
    async def _log_metrics_summary(self):
        """메트릭 요약 로그 출력"""
        current_time = time.time()
        elapsed = current_time - self.last_summary_log_time
        
        if elapsed < self.summary_log_interval:
            return  # 아직 시간이 충분히 지나지 않음
        
        # 메시지 카운트 정보
        current_count = self.message_counters.get("message_count", 0)
        previous_count = self.previous_message_count
        
        # 1. 총 메시지 수
        total_count = current_count
        
        # 2. 이번 수신 갯수 (이번 1초 동안)
        current_diff = current_count - previous_count
        
        # 3. 평균 수신 갯수 계산
        start_time = self.message_counters.get("first_message_time")
        if start_time is None and current_count > 0:
            # 첫 메시지 수신 시간 기록
            start_time = current_time
            self.message_counters["first_message_time"] = start_time
        
        total_elapsed = current_time - (start_time or current_time)
        avg_per_second = 0
        if total_elapsed > 0 and current_count > 0:
            avg_per_second = current_count / total_elapsed
        
        # 4. 이번 수신 속도 (1초 동안)
        current_rate = self.message_counters.get("message_rate", 0)
        if current_rate == 0 and elapsed > 0:
            current_rate = current_diff / elapsed
        
        # 메시지 레이트 업데이트 (지수 이동 평균)
        if elapsed > 0:
            new_rate = current_diff / elapsed
            if current_rate > 0:
                # 이전 레이트의 80%와 새 레이트의 20%로 가중 평균
                current_rate = (current_rate * 0.8) + (new_rate * 0.2)
            else:
                current_rate = new_rate
            
            self.message_counters["message_rate"] = current_rate
        
        # 로그 출력 (INFO 레벨로 출력하여 항상 보이도록 함)
        if current_count > 0:
            logger.info(f"[메트릭] {self.exchange_name_kr:15} | 총: {total_count:8d}건 | 수신: {current_diff:6d}건/1초 | 평균: {avg_per_second:.2f}건/초 | 속도: {current_rate:.2f}건/초")
        
        # 이전 카운트 업데이트
        self.previous_message_count = current_count
        
        # 마지막 로그 시간 업데이트
        self.last_summary_log_time = current_time
    
    def increment_message_count(self, n: int = 1) -> None:
        """메시지 카운트 증가"""
        current_time = time.time()
        
        # 메시지 카운트 증가
        self.message_counters["message_count"] += n
        
        # 첫 메시지 시간 설정 (첫 메시지인 경우에만)
        if self.message_counters["message_count"] == n:
            self.message_counters["first_message_time"] = current_time
        
        # 마지막 메시지 시간 업데이트
        self.message_counters["last_message_time"] = current_time
    
    def update_metrics(self, metric_name: str, value: float = 1.0, **kwargs) -> None:
        """
        메트릭 업데이트
        
        Args:
            metric_name: 메트릭 이름
            value: 메트릭 값
            **kwargs: 추가 데이터
        """
        try:
            # 특수 메트릭 처리
            if metric_name == "processing_time":
                current = self.message_counters.get("processing_time", 0)
                self.message_counters["processing_time"] = (current * 0.8) + (value * 0.2)
            elif metric_name == "error_count":
                self.message_counters["error_count"] += value
                self.message_counters["last_error_time"] = time.time()
            elif metric_name == "data_size":
                # 데이터 사이즈는 누적하지 않고 가장 최근 값만 저장
                self.message_counters["data_size"] = value
            elif metric_name in self.message_counters:
                # 기존 메트릭이면 값 증가
                self.message_counters[metric_name] += value
            else:
                # 새 메트릭이면 추가
                self.message_counters[metric_name] = value
            
            # 메트릭 이벤트 발행
            asyncio.create_task(
                self.handle_metric_update(metric_name, value, kwargs)
            )
        except Exception as e:
            self.log_error(f"메트릭 업데이트 중 오류: {e}")

    async def handle_metric_update(self, metric_name: str, value: float = 1.0, data: Dict = None) -> None:
        """
        메트릭 업데이트 이벤트 처리
        
        이 메서드는 SystemEventManager의 _handle_metric_update 메서드와 유사한 역할을 합니다.
        다만 차이점은 SystemEventManager는 여러 거래소의 메트릭을 처리하는 반면,
        EventHandler는 단일 거래소의 메트릭만 처리합니다.
        
        Args:
            metric_name: 메트릭 이름
            value: 메트릭 값
            data: 추가 데이터
        """
        try:
            # 기본 이벤트 데이터 생성
            event_data = {
                "exchange_code": self.exchange_code,
                "metric_name": metric_name,
                "value": value,
                "timestamp": time.time()
            }
            
            # 추가 데이터가 있으면 병합
            if data:
                event_data.update(data)
                
            # 이벤트 발행
            await self.event_bus.publish(
                EVENT_TYPES["METRIC_UPDATE"],
                event_data
            )
        except Exception as e:
            self.log_error(f"메트릭 업데이트 이벤트 발행 중 오류: {e}")
    
    async def send_telegram_message(self, event_type: str, message: str) -> None:
        """
        텔레그램 알림 전송
        
        Args:
            event_type: 이벤트 타입
            message: 알림 메시지
        """
        try:
            # 쿨다운 확인
            current_time = time.time()
            last_time = self._last_notification_time.get(event_type, 0)
            if current_time - last_time < self._notification_cooldown:
                return
            
            # 마지막 알림 시간 업데이트
            self._last_notification_time[event_type] = current_time
            
            # 현재 시간 추가
            current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            message_with_time = f"[{current_time_str}]\n{message}"
            
            # 메시지 전송
            await send_telegram_message(message_with_time)
        except Exception as e:
            self.log_error(f"텔레그램 알림 전송 실패: {str(e)}")
    
    async def handle_connection_status(self, status: str, message: Optional[str] = None, **kwargs) -> None:
        """
        연결 상태 변경 이벤트 처리
        
        Args:
            status: 연결 상태 ('connected', 'disconnected', 'reconnecting' 등)
            message: 상태 메시지 (없으면 기본값 사용)
            **kwargs: 추가 데이터
        """
        # 기본 메시지 설정
        if not message:
            if status == "connected":
                message = "웹소켓 연결됨"
            elif status == "disconnected":
                message = "웹소켓 연결 끊김"
            elif status == "reconnecting":
                message = "웹소켓 재연결 중"
            else:
                message = f"웹소켓 상태 변경: {status}"
        
        # 로깅
        log_method = self.log_info
        if status == "disconnected" or status == "reconnecting":
            log_method = self.log_warning
        log_method(message)
        
        # 상태 저장
        self.connection_status = status
        
        # 텔레그램 알림 전송
        event_type = "connect" if status == "connected" else "disconnect" if status == "disconnected" else "reconnect"
        
        # 이모지 선택
        emoji = "🟢" if event_type == "connect" else "🔴" if event_type == "disconnect" else "🟠"
        
        # 거래소 이름에서 대괄호 제거
        exchange_name = self.exchange_name_kr.replace('[', '').replace(']', '')
        
        # 메시지 직접 포맷팅
        formatted_message = f"{emoji} {exchange_name} 웹소켓: {message}"
        
        # 메시지 직접 전송
        await self.send_telegram_message(event_type, formatted_message)
        
        # 이벤트 버스 발행 (다른 컴포넌트에 알림)
        event_data = {
            "exchange_code": self.exchange_code,
            "status": status,
            "message": message,
            "timestamp": kwargs.get("timestamp", time.time())
        }
        await self.event_bus.publish(EVENT_TYPES["CONNECTION_STATUS"], event_data)
    
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
            self.log_error(f"{error_type}: {message}")
        elif severity == "warning":
            self.log_warning(f"{error_type}: {message}")
        else:
            self.log_error(f"{error_type}: {message}")
        
        # 오류 카운트 증가 및 마지막 오류 저장
        self.update_metrics("error_count")
        self.message_counters["last_error_message"] = message
        
        # 오류 기록 저장 (최대 5개)
        timestamp = kwargs.get("timestamp", time.time())
        self.errors.append({
            "timestamp": timestamp,
            "type": error_type,
            "message": message,
            "severity": severity
        })
        
        # 최대 5개만 유지
        if len(self.errors) > 5:
            self.errors = self.errors[-5:]
        
        # 텔레그램 알림 전송 (심각한 오류만)
        if severity in ["error", "critical"]:
            # 거래소 이름 포함 오류 메시지 생성
            exchange_name = self.exchange_name_kr.replace('[', '').replace(']', '')
            error_message = f"🚨 {exchange_name} 오류: {error_type} - {message}"
            await self.send_telegram_message("error", error_message)
    
        # 이벤트 버스 발행
        event_data = {
            "exchange_code": self.exchange_code,
            "error_type": error_type,
            "message": message,
            "severity": severity,
            "timestamp": timestamp
        }
        
        # 추가 데이터 병합
        for key, value in kwargs.items():
            if key != "timestamp":  # timestamp는 이미 처리함
                event_data[key] = value
        
        await self.event_bus.publish(EVENT_TYPES["ERROR_EVENT"], event_data)
    
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
        
        # 상태 저장
        self.subscriptions = {
            "status": status,
            "symbols": symbols,
            "timestamp": kwargs.get("timestamp", time.time())
        }
        
        # 이벤트 버스 발행
        event_data = {
            "exchange_code": self.exchange_code,
            "status": status,
            "symbols": symbols,
            "count": len(symbols),
            "timestamp": kwargs.get("timestamp", time.time())
        }
        await self.event_bus.publish(EVENT_TYPES["SUBSCRIPTION_STATUS"], event_data)
    
    async def handle_message_received(self, message_type: str, size: int = 0, **kwargs) -> None:
        """
        메시지 수신 이벤트 처리
        
        Args:
            message_type: 메시지 유형 ('snapshot', 'delta', 'heartbeat' 등)
            size: 메시지 크기 (바이트)
            **kwargs: 추가 데이터
        """
        # 메시지 카운트 증가
        self.increment_message_count()
        
        # 크기 메트릭 추가
        if size > 0:
            self.update_metrics("data_size", size)
        
        # 메시지 타입별 카운트
        if message_type in ["snapshot", "delta"]:
            self.update_metrics(f"{message_type}_count")
    
    async def handle_data_event(self, event_type: str, symbol: str, data: Any, **kwargs) -> None:
        """
        데이터 이벤트 처리 (스냅샷, 델타 등)
        
        Args:
            event_type: 이벤트 타입 (EVENT_TYPES 상수 사용)
            symbol: 심볼명
            data: 이벤트 데이터
            **kwargs: 추가 데이터
        """
        # 필요한 경우 메트릭 기록
        if event_type == EVENT_TYPES["DATA_SNAPSHOT"]:
            self.update_metrics("snapshot_count")
        elif event_type == EVENT_TYPES["DATA_DELTA"]:
            self.update_metrics("delta_count")
        
        # 데이터 크기 추정
        try:
            data_size = len(str(data)) if data else 0
            if data_size > 0:
                self.update_metrics("data_size", data_size)
        except:
            pass
        
        # 이벤트 버스 발행
        event_data = {
            "exchange_code": self.exchange_code,
            "symbol": symbol,
            "data": data,
            "timestamp": kwargs.get("timestamp", time.time())
        }
        
        # 추가 데이터 병합
        for key, value in kwargs.items():
            if key != "timestamp":  # timestamp는 이미 처리함
                event_data[key] = value
        
        await self.event_bus.publish(event_type, event_data)
    
    def get_status(self) -> Dict[str, Any]:
        """
        현재 상태 정보 가져오기
            
        Returns:
            Dict: 상태 정보
        """
        result = {
            "connection_status": self.connection_status,
            "subscription": self.subscriptions,
            "metrics": self.message_counters,
            "errors": self.errors
        }
        
        # 상태 이모지 추가
        result["emoji"] = self._get_status_emoji(self.connection_status)
        
        return result
    
    def _get_status_emoji(self, status: str) -> str:
        """상태에 따른 이모지 반환"""
        emojis = {
            "connected": "🟢",
            "disconnected": "🔴",
            "reconnecting": "🟠",
            "error": "⚠️",
            "unknown": "⚪"
        }
        return emojis.get(status, "⚪")
    
    def get_connection_status(self) -> str:
        """
        연결 상태 가져오기
        
        Returns:
            str: 연결 상태
        """
        return self.connection_status
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        메트릭 정보 가져오기
        
        Returns:
            Dict: 메트릭 정보
        """
        return self.message_counters
    
    def get_errors(self, limit: int = None) -> List:
        """
        오류 목록 가져오기
        
        Args:
            limit: 최대 개수 (None이면 모두 반환)
            
        Returns:
            List: 오류 목록
        """
        if limit:
            return self.errors[-limit:]
        return self.errors

    async def publish_system_event(self, event_type: str, **data) -> None:
        """
        시스템 이벤트 발행
        
        SystemEventManager의 publish_system_event 메서드와 유사하지만
        단일 거래소(self.exchange_code)만 처리하도록 단순화되었습니다.
        
        Args:
            event_type: 이벤트 타입
            **data: 추가 데이터
        """
        try:
            # 타임스탬프 처리 (data에 timestamp가 있으면 해당 값 사용, 없으면 현재 시간)
            timestamp = time.time()
            if "timestamp" in data:
                timestamp = data.pop("timestamp")
            
            # 이벤트 데이터 준비
            event_data = {
                "exchange_code": self.exchange_code,
                "timestamp": timestamp
            }
            
            # 추가 데이터 병합
            if data:
                event_data["data"] = data
            
            # 이벤트 발행
            await self.event_bus.publish(event_type, event_data)
            
        except Exception as e:
            self.log_error(f"시스템 이벤트 발행 실패: {str(e)}")

        # 기존 메트릭이 있으면 초기화하지 않음
        if self.message_counters.get("message_count", 0) > 0:
            return
            
        # 메트릭 초기화
        self.message_counters = {
            "message_count": 0,
            "error_count": 0,
            "processing_time": 0,
            "last_message_time": 0,
            "last_error_time": 0,
            "last_error_message": "",
            "first_message_time": None,
            "message_rate": 0
        }

    def initialize_metrics(self) -> None:
        """
        메트릭 초기화 - 필요한 메트릭 초기값 설정
        
        이 메서드는 SystemEventManager의 initialize_exchange 메서드에서 
        메트릭 초기화 관련 기능을 가져온 것입니다.
        """
        # 기존 메트릭이 있으면 초기화하지 않음
        if self.message_counters.get("message_count", 0) > 0:
            return
            
        # 메트릭 초기화
        self.message_counters = {
            "message_count": 0,
            "error_count": 0,
            "processing_time": 0,
            "last_message_time": 0,
            "last_error_time": 0,
            "last_error_message": "",
            "first_message_time": None,
            "message_rate": 0
        }

# 팩토리 함수로 EventHandler 인스턴스를 가져오는 함수 (호환성 유지)
def EventHandlerFactory():
    # 실제로는 EventHandler 클래스의 get_instance 메서드를 사용함을 알림
    raise DeprecationWarning(
        "EventHandlerFactory는 더 이상 사용되지 않습니다. "
        "EventHandler.get_instance(exchange_code, settings)를 대신 사용하세요."
    ) 