"""
시스템 이벤트 관리자 모듈

이 모듈은 이벤트 버스를 통해 시스템 이벤트를 관리하고 메트릭을 유지합니다.
"""

import asyncio
import time
from typing import Dict, List, Any, Optional, Callable
from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.util.event_bus import EventBus
from crosskimp.config.constants_v3 import EXCHANGE_NAMES_KR

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# 이벤트 타입 정의
EVENT_TYPES = {
    "CONNECTION_STATUS": "connection_status",  # 연결 상태 변경
    "METRIC_UPDATE": "metric_update",          # 메트릭 업데이트
    "ERROR_EVENT": "error_event",              # 오류 이벤트
    "SUBSCRIPTION_STATUS": "subscription_status"  # 구독 상태 변경
}

class MessageCounter:
    """
    메시지 카운터 클래스 (배치 처리)
    
    메시지 수신 카운트를 효율적으로 집계하기 위한 클래스입니다.
    설정된 시간 간격마다 배치 처리를 수행합니다.
    """
    
    def __init__(self, on_update: Callable = None, update_interval: float = 1.0):
        """
        초기화
        
        Args:
            on_update: 업데이트 콜백 함수
            update_interval: 업데이트 간격 (초)
        """
        self.count = 0
        self.total_count = 0
        self.last_update = time.time()
        self.update_interval = update_interval
        self.on_update = on_update
        
        # 비동기 태스크 관련 변수
        self.update_task = None
        self.async_mode = False
        
        # 비동기 모드 확인 및 설정
        try:
            asyncio.get_running_loop()
            self.async_mode = True
            self._start_update_task()
        except RuntimeError:
            # 이벤트 루프가 없으면 동기 모드로 실행
            logger.debug("이벤트 루프가 실행 중이 아니므로 동기 모드로 메시지 카운터가 초기화됩니다.")
    
    def increment(self, n: int = 1) -> None:
        """
        카운트 증가
        
        Args:
            n: 증가시킬 값 (기본값: 1)
        """
        self.count += n
        self.total_count += n
        
        # 동기 모드에서는 임계값에 도달하거나 일정 시간이 지나면 업데이트
        if not self.async_mode:
            current_time = time.time()
            time_passed = current_time - self.last_update
            
            # 업데이트 간격이 지났거나 카운트가 10 이상이면 업데이트
            if (time_passed >= self.update_interval or self.count >= 10) and self.on_update:
                try:
                    self.on_update(self.count)
                    self.count = 0
                    self.last_update = current_time
                except Exception as e:
                    logger.error(f"메시지 카운터 콜백 오류: {e}")
    
    def get_count(self) -> int:
        """현재 카운트 반환"""
        return self.count
    
    def get_total_count(self) -> int:
        """총 카운트 반환"""
        return self.total_count
    
    def _start_update_task(self) -> None:
        """주기적 업데이트 태스크 시작 (비동기 모드에서만 사용)"""
        async def update_loop():
            try:
                while True:
                    # 업데이트 간격만큼 대기
                    await asyncio.sleep(self.update_interval)
                    
                    # 현재 카운트 가져오기
                    current_count = self.count
                    
                    # 콜백이 있으면 호출
                    if self.on_update and current_count > 0:
                        try:
                            self.on_update(current_count)
                        except Exception as e:
                            logger.error(f"메시지 카운터 콜백 오류: {e}")
                    
                    # 카운트 초기화
                    self.count = 0
                    self.last_update = time.time()
            except asyncio.CancelledError:
                # 정상 종료
                pass
            except Exception as e:
                logger.error(f"메시지 카운터 업데이트 루프 오류: {e}")
        
        # 비동기 모드일 때만 태스크 시작
        try:
            self.update_task = asyncio.create_task(update_loop())
        except RuntimeError:
            logger.debug("이벤트 루프가 실행 중이 아니므로 업데이트 태스크를 시작할 수 없습니다.")
            self.update_task = None
            self.async_mode = False

class SystemEventManager:
    """
    시스템 이벤트 관리 클래스
    
    이벤트 버스를 통해 모든 시스템 이벤트를 수신하고 관리합니다.
    필수 메트릭만 유지하고 배치 처리를 지원합니다.
    """
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """싱글톤 인스턴스 반환"""
        if cls._instance is None:
            cls._instance = SystemEventManager()
        return cls._instance
    
    def __init__(self):
        """초기화"""
        if SystemEventManager._instance is not None:
            raise Exception("SystemEventManager는 싱글톤입니다. get_instance()를 사용하세요.")
        
        # 메트릭 저장소
        self.metrics = {}  # exchange_code -> metrics
        
        # 연결 상태 저장소
        self.connection_status = {}  # exchange_code -> status
        
        # 구독 상태 저장소
        self.subscriptions = {}  # exchange_code -> subscription_status
        
        # 이벤트 버스 초기화
        from crosskimp.ob_collector.orderbook.util.event_bus import EventBus
        self.event_bus = EventBus.get_instance()
        
        # 메시지 카운터 저장소
        self.message_counters = {}
        
        # 이벤트 핸들러 등록
        self.event_bus.subscribe("system_event", self.handle_event)
        
        # 최근 오류 저장소 (거래소별 최대 10개)
        self.errors = {}  # exchange_code -> [(timestamp, error_msg)]
        
        # 현재 거래소 설정 (컨텍스트 관리용)
        self.current_exchange = None
        
        # 메트릭 요약 로그 주기적 출력을 위한 변수
        self.last_summary_log_time = time.time()
        self.summary_log_interval = 2.0  # 5초마다 요약 로그 출력
        self.previous_message_counts = {}  # 이전 카운트 저장용
        
        # 메트릭 요약 로그 태스크 시작
        self._start_summary_log_task()

        logger.info("[SystemEventManager] 초기화 완료")
    
    def _start_summary_log_task(self):
        """주기적 메트릭 요약 로그 태스크 시작"""
        try:
            loop = asyncio.get_running_loop()
            asyncio.create_task(self._summary_log_loop())
        except RuntimeError:
            # 이벤트 루프가 없으면 태스크 시작 안함
            logger.debug("이벤트 루프가 실행 중이 아니므로 요약 로그 태스크를 시작할 수 없습니다.")
    
    async def _summary_log_loop(self):
        """메트릭 요약 로그 출력 루프"""
        try:
            while True:
                await asyncio.sleep(self.summary_log_interval)
                self._log_metrics_summary()
        except asyncio.CancelledError:
            # 정상 종료
            pass
        except Exception as e:
            logger.error(f"메트릭 요약 로그 루프 오류: {e}")
    
    def _log_metrics_summary(self):
        """각 거래소별 메트릭 요약 로그 출력"""
        current_time = time.time()
        elapsed = current_time - self.last_summary_log_time
        
        if elapsed < self.summary_log_interval:
            return  # 아직 시간이 충분히 지나지 않음
        
        # 각 거래소별 메트릭 정보 출력
        for exchange_code in sorted(self.metrics.keys()):
            # 메시지 카운트 정보
            current_count = self.metrics[exchange_code].get("message_count", 0)
            previous_count = self.previous_message_counts.get(exchange_code, 0)
            
            # 차이 및 속도 계산
            diff = current_count - previous_count
            rate = diff / elapsed if elapsed > 0 else 0
            
            # 한글 거래소명 변환
            exchange_name_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code.upper())
            
            # 로그 출력 (INFO 레벨로 출력하여 항상 보이도록 함)
            logger.info(f"[메트릭] {exchange_name_kr:15} | 현재: {current_count:8d}건 | 이전: {previous_count:8d}건 | 차이: {diff:6d}건 | 속도: {rate:.2f}건/초")
            
            # 이전 카운트 업데이트
            self.previous_message_counts[exchange_code] = current_count
        
        # 마지막 로그 시간 업데이트
        self.last_summary_log_time = current_time
    
    def initialize_exchange(self, exchange_code: str) -> None:
        """거래소 초기화"""
        if exchange_code not in self.connection_status:
            self.connection_status[exchange_code] = "disconnected"
            
        if exchange_code not in self.metrics:
            self.metrics[exchange_code] = {
                "message_count": 0,
                "error_count": 0,
                "processing_time": 0,
                "last_message_time": 0,
                "last_error_time": 0,
                "last_error_message": ""
            }
            
        if exchange_code not in self.errors:
            self.errors[exchange_code] = []
            
        if exchange_code not in self.subscriptions:
            self.subscriptions[exchange_code] = {
                "symbols": [],
                "status": "unsubscribed"
            }
            
        # 1초마다 배치 업데이트 수행하는 메시지 카운터 생성
        self.message_counters[exchange_code] = MessageCounter(
            on_update=lambda count: self._update_metric(
                exchange_code=exchange_code,
                metric_name="message_count",
                count=count
            ),
            update_interval=1.0
        )
    
    async def handle_event(self, event) -> None:
        """이벤트 처리"""
        try:
            event_type = event.get("event_type")
            exchange_code = event.get("exchange_code")
            timestamp = event.get("timestamp", time.time())
            data = event.get("data", {})
            
            # 거래소 초기화 확인
            if exchange_code and exchange_code not in self.connection_status:
                self.initialize_exchange(exchange_code)
            
            # 이벤트 타입별 처리
            if event_type == EVENT_TYPES["CONNECTION_STATUS"]:
                await self._handle_connection_status(exchange_code, data, timestamp)
                
            elif event_type == EVENT_TYPES["METRIC_UPDATE"]:
                await self._handle_metric_update(exchange_code, data, timestamp)
                
            elif event_type == EVENT_TYPES["ERROR_EVENT"]:
                await self._handle_error_event(exchange_code, data, timestamp)
                
            elif event_type == EVENT_TYPES["SUBSCRIPTION_STATUS"]:
                await self._handle_subscription_status(exchange_code, data, timestamp)
                
        except Exception as e:
            logger.error(f"이벤트 처리 중 오류: {str(e)}")
    
    async def _handle_connection_status(self, exchange_code, data, timestamp) -> None:
        """연결 상태 이벤트 처리"""
        status = data.get("status", "disconnected")
        self.connection_status[exchange_code] = status
        
        # 이전 상태가 없고 현재 연결됨 상태인 경우에만 기록
        if status == "connected":
            self.metrics[exchange_code]["connection_time"] = timestamp
            
        logger.debug(f"{exchange_code} 연결 상태 변경: {status}")
    
    async def _handle_metric_update(self, exchange_code, data, timestamp) -> None:
        """메트릭 업데이트 이벤트 처리"""
        metric_name = data.get("metric_name")
        value = data.get("value")
        
        if not metric_name or value is None:
            return
            
        # 메트릭 업데이트
        self.metrics[exchange_code][metric_name] = value
        
        # 특수 메트릭 처리
        if metric_name == "message_count":
            self.metrics[exchange_code]["last_message_time"] = timestamp
        
        # 처리 시간 평균 계산 (이동 평균)
        if metric_name == "processing_time":
            current = self.metrics[exchange_code].get("processing_time", 0)
            self.metrics[exchange_code]["processing_time"] = (current * 0.8) + (value * 0.2)
    
    async def _handle_error_event(self, exchange_code, data, timestamp) -> None:
        """오류 이벤트 처리"""
        error_type = data.get("error_type", "unknown")
        message = data.get("message", "")
        severity = data.get("severity", "error")
        
        # 오류 카운터 증가
        self.metrics[exchange_code]["error_count"] = self.metrics[exchange_code].get("error_count", 0) + 1
        self.metrics[exchange_code]["last_error_time"] = timestamp
        self.metrics[exchange_code]["last_error_message"] = message
        
        # 최근 오류 목록 유지 (최대 10개)
        self.errors[exchange_code].append({
            "timestamp": timestamp,
            "type": error_type,
            "message": message,
            "severity": severity
        })
        
        # 최대 10개만 유지
        if len(self.errors[exchange_code]) > 10:
            self.errors[exchange_code] = self.errors[exchange_code][-10:]
        
        # 심각한 오류만 로깅
        if severity in ["error", "critical"]:
            logger.error(f"{exchange_code} {error_type}: {message}")
    
    async def _handle_subscription_status(self, exchange_code, data, timestamp) -> None:
        """구독 상태 이벤트 처리"""
        status = data.get("status", "unknown")
        symbols = data.get("symbols", [])
        
        self.subscriptions[exchange_code] = {
            "status": status,
            "symbols": symbols,
            "timestamp": timestamp
        }
        
        logger.debug(f"{exchange_code} 구독 상태: {status}, 심볼: {len(symbols)}개")
    
    def get_status(self, exchange_code: str = None) -> dict:
        """
        시스템 상태 정보 조회
        
        Args:
            exchange_code: 거래소 코드 (None이면 모든 거래소)
            
        Returns:
            dict: 상태 정보
        """
        if exchange_code:
            # 특정 거래소 상태 반환
            if exchange_code not in self.metrics:
                return {}
                
            # 배치 카운터에서 실시간 데이터 가져와서 병합
            result = self.metrics[exchange_code].copy()
            
            # 배치 필드는 제거
            for key in list(result.keys()):
                if key.startswith("_"):
                    del result[key]
                    
            # 상태 이모지 추가
            connection_status = result.get("status", "disconnected")
            result["emoji"] = self._get_status_emoji(connection_status)
            
            # 최근 오류 추가
            if exchange_code in self.errors:
                result["recent_errors"] = self.errors[exchange_code][-5:]  # 최근 5개 오류만
                
            return result
            
        else:
            # 모든 거래소 상태 반환
            result = {}
            for ex_code in self.metrics:
                result[ex_code] = self.get_status(ex_code)
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
    
    def increment_message_count(self, exchange_code: str) -> None:
        """
        메시지 카운트 증가 (동기식, 빠른 처리)
        
        Args:
            exchange_code: 거래소 코드
        """
        if exchange_code in self.message_counters:
            self.message_counters[exchange_code].increment()
    
    def get_connection_status(self, exchange_code: str) -> str:
        """
        연결 상태 가져오기
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            str: 연결 상태
        """
        return self.connection_status.get(exchange_code, "unknown")
    
    def get_metrics(self, exchange_code: str) -> dict:
        """
        메트릭 정보 가져오기
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            dict: 메트릭 정보
        """
        return self.metrics.get(exchange_code, {})
    
    def get_errors(self, exchange_code: str, limit: int = None) -> List:
        """
        오류 목록 가져오기
        
        Args:
            exchange_code: 거래소 코드
            limit: 최대 항목 수
            
        Returns:
            List: 오류 목록
        """
        errors = self.errors.get(exchange_code, [])
        if limit:
            return errors[-limit:]
        return errors
    
    def _update_metric(self, exchange_code: str, metric_name: str, count: int = 1) -> None:
        """
        메트릭 업데이트 (내부 메서드, 배치 처리에서 호출됨)
        
        Args:
            exchange_code: 거래소 코드
            metric_name: 메트릭 이름
            count: 증가시킬 값
        """
        # 거래소 초기화 확인
        if exchange_code not in self.metrics:
            self.initialize_exchange(exchange_code)
            
        # 메트릭 업데이트
        current_value = self.metrics[exchange_code].get(metric_name, 0)
        updated_value = current_value + count
        
        # 업데이트
        self.metrics[exchange_code][metric_name] = updated_value
        
        # 타임스탬프 업데이트
        current_time = time.time()
        self.metrics[exchange_code]["last_update_time"] = current_time
        
        # 특수 메트릭인 경우 추가 처리
        if metric_name == "message_count":
            self.metrics[exchange_code]["last_message_time"] = current_time
            
        # 로깅 (디버깅용) - 필요한 경우에만 로그 출력
        # 일반적인 메트릭 업데이트는 로깅하지 않음
        if metric_name == "error_count":
            logger.debug(f"[SystemEventManager] {exchange_code} {metric_name} 업데이트: +{count}")
            
    def handle_metric_update(self, exchange_code: str, metric_name: str, count: int = 1) -> None:
        """
        메트릭 업데이트 (MessageCounter 콜백에서 호출됨)
        
        Args:
            exchange_code: 거래소 코드
            metric_name: 메트릭 이름
            count: 증가시킬 값
        """
        self._update_metric(exchange_code, metric_name, count)

    def record_metric(self, exchange_code: str, metric_name: str, **data) -> None:
        """
        메트릭 기록 메서드
        
        모든 메트릭을 기록하는 중앙 메서드입니다.
        
        Args:
            exchange_code: 거래소 코드
            metric_name: 메트릭 이름
            **data: 추가 데이터
        """
        try:
            # 거래소 초기화 확인
            if exchange_code not in self.metrics:
                self.initialize_exchange(exchange_code)
                
            # 값 추출 (기본값은 1)
            value = 1
                
            # 특수 메트릭 처리
            if metric_name == "processing_time" or "processing_time" in data:
                value = data.get("processing_time", 0)
                metric_name = "processing_time"
            elif metric_name == "data_size" or "byte_size" in data:
                value = data.get("byte_size", 0)
                metric_name = "data_size"
                
            # 메트릭 업데이트
            self._update_metric(exchange_code, metric_name, value)
            
            # 메시지 관련 메트릭인 경우 메시지 카운터 증가
            if metric_name in ["message_count", "delta_count", "snapshot_count"]:
                if exchange_code in self.message_counters:
                    self.message_counters[exchange_code].increment()
                    
        except Exception as e:
            logger.error(f"메트릭 기록 중 오류: {str(e)}")
    
    async def publish_system_event(self, event_type: str, **data) -> None:
        """
        시스템 이벤트 발행 (비동기)
        
        이벤트 버스를 통해 시스템 이벤트를 발행합니다.
        
        Args:
            event_type: 이벤트 타입 (CONNECTION_STATUS, METRIC_UPDATE, ERROR_EVENT, SUBSCRIPTION_STATUS)
            **data: 이벤트 데이터
            
        Raises:
            ValueError: 거래소 코드가 없는 경우
        """
        try:
            # 현재 거래소 코드 가져오기
            exchange_code = data.pop("exchange_code", self.current_exchange)
            
            # 거래소 코드가 없으면 오류 로그 남기고 예외 발생
            if not exchange_code:
                import traceback
                call_stack = traceback.format_stack()[-3]  # 호출 위치 정보
                logger.error(f"시스템 이벤트 발행 실패: 거래소 코드가 필요합니다 (이벤트: {event_type})")
                logger.error(f"호출 위치: {call_stack.strip()}")
                raise ValueError(f"시스템 이벤트 발행 실패: 거래소 코드가 필요합니다 (이벤트: {event_type})")
                
            # 타임스탬프 설정
            timestamp = data.pop("timestamp", time.time())
            
            # 거래소 초기화 확인
            if exchange_code not in self.metrics:
                self.initialize_exchange(exchange_code)
                
            # 이벤트 객체 생성
            event = {
                "event_type": event_type,
                "exchange_code": exchange_code,
                "timestamp": timestamp,
                "data": data
            }
            
            # 이벤트 버스를 통해 이벤트 발행 (비동기)
            await self.event_bus.publish("system_event", event)
                
        except Exception as e:
            logger.error(f"시스템 이벤트 발행 중 오류: {str(e)}")
            # ValueError면 예외를 다시 발생시킴
            if isinstance(e, ValueError):
                raise
            
    def publish_system_event_sync(self, event_type: str, **data) -> None:
        """
        시스템 이벤트 발행 (동기식)
        
        이벤트 버스를 통해 시스템 이벤트를 발행합니다.
        비동기 컨텍스트 외부에서 사용할 수 있는 동기식 버전입니다.
        
        Args:
            event_type: 이벤트 타입 (CONNECTION_STATUS, METRIC_UPDATE, ERROR_EVENT, SUBSCRIPTION_STATUS)
            **data: 이벤트 데이터
            
        Raises:
            ValueError: 거래소 코드가 없는 경우
        """
        try:
            # 현재 거래소 코드 가져오기
            exchange_code = data.pop("exchange_code", self.current_exchange)
            
            # 거래소 코드가 없으면 오류 로그 남기고 예외 발생
            if not exchange_code:
                import traceback
                call_stack = traceback.format_stack()[-3]  # 호출 위치 정보
                logger.error(f"시스템 이벤트 발행 실패: 거래소 코드가 필요합니다 (이벤트: {event_type})")
                logger.error(f"호출 위치: {call_stack.strip()}")
                raise ValueError(f"시스템 이벤트 발행 실패: 거래소 코드가 필요합니다 (이벤트: {event_type})")
                
            # 타임스탬프 설정
            timestamp = data.pop("timestamp", time.time())
            
            # 거래소 초기화 확인
            if exchange_code not in self.metrics:
                self.initialize_exchange(exchange_code)
                
            # 이벤트 객체 생성
            event = {
                "event_type": event_type,
                "exchange_code": exchange_code,
                "timestamp": timestamp,
                "data": data
            }
            
            # 이벤트 버스를 통해 이벤트 발행 (동기식)
            self.event_bus.publish_sync("system_event", event)
                
        except Exception as e:
            logger.error(f"시스템 이벤트 발행(동기식) 중 오류: {str(e)}")
            # ValueError면 예외를 다시 발생시킴
            if isinstance(e, ValueError):
                raise

    def set_current_exchange(self, exchange_code: str) -> None:
        """
        현재 컨텍스트의 거래소 코드 설정
        
        이후 거래소 코드를 명시하지 않은 이벤트 발행 시 이 값이 사용됩니다.
        
        Args:
            exchange_code: 거래소 코드
        """
        self.current_exchange = exchange_code
        
        # 거래소 초기화 확인
        if exchange_code and exchange_code not in self.metrics:
            self.initialize_exchange(exchange_code)
    
    def clear_current_exchange(self) -> None:
        """현재 컨텍스트의 거래소 코드 초기화"""
        self.current_exchange = None
    
    class ExchangeContext:
        """거래소 컨텍스트 관리자"""
        def __init__(self, manager, exchange_code: str):
            self.manager = manager
            self.exchange_code = exchange_code
            self.previous_exchange = None
            
        def __enter__(self):
            self.previous_exchange = self.manager.current_exchange
            self.manager.set_current_exchange(self.exchange_code)
            return self
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            self.manager.current_exchange = self.previous_exchange
    
    def with_exchange(self, exchange_code: str):
        """
        거래소 컨텍스트 관리자 반환
        
        with 문과 함께 사용하여 특정 블록에서만 거래소 컨텍스트를 설정할 수 있습니다.
        
        예시:
        ```python
        with system_event_manager.with_exchange("upbit"):
            system_event_manager.publish_system_event_sync(EVENT_TYPES["ERROR_EVENT"], message="오류 발생")
        ```
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            ExchangeContext: 컨텍스트 관리자
        """
        return self.ExchangeContext(self, exchange_code) 