"""
메트릭 이벤트 핸들러 모듈

이 모듈은 메트릭 관련 이벤트를 처리하는 핸들러 클래스를 제공합니다.
WebsocketMetricsManager에서 이벤트 처리 로직을 분리하여 코드 복잡성을 줄입니다.
"""

import time
import inspect
import functools
import traceback
from typing import Dict, Callable, Any, List, Optional
from collections import defaultdict, deque

from crosskimp.logger.logger import get_unified_logger

# 데코레이터: exchange_code 정규화 및 초기화 보장
def prepare_exchange(func):
    @functools.wraps(func)
    def wrapper(self, exchange_code, *args, **kwargs):
        # 소문자로 변환
        normalized_code = exchange_code.lower() if exchange_code else exchange_code
        # 초기화 보장
        self._ensure_exchange_initialized(normalized_code)
        # 원래 함수 호출
        return func(self, normalized_code, *args, **kwargs)
    return wrapper

# 데코레이터: 필수 필드 검증
def validate_required_fields(required_fields):
    """
    이벤트 데이터의 필수 필드를 검증하는 데코레이터
    
    Args:
        required_fields: 필수 필드 목록
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, event_data, *args, **kwargs):
            # 필수 필드 검증
            missing_fields = [field for field in required_fields if field not in event_data]
            if missing_fields:
                self.logger.warning(f"[Metrics] 유효하지 않은 이벤트 (필수 필드 누락: {', '.join(missing_fields)}): {event_data}")
                return None
            # 원래 함수 호출
            return func(self, event_data, *args, **kwargs)
        return wrapper
    return decorator

# 데코레이터: 표준 예외 처리
def with_standard_error_handling(func):
    """
    표준 예외 처리를 적용하는 데코레이터
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            method_name = func.__name__
            self.logger.error(f"[Metrics] {method_name} 실행 중 오류 발생: {e}")
            traceback.print_exc()
            return None
    return wrapper

class MetricsEventHandler:
    """
    메트릭 이벤트 핸들러 클래스
    
    웹소켓 메트릭 매니저에서 이벤트 처리 로직을 분리한 클래스입니다.
    각종 이벤트를 수신하고 메트릭 데이터를 적절히 업데이트합니다.
    """
    
    # 이벤트 타입 매핑을 클래스 수준에서 정의
    # 간단한 카운터 업데이트 매핑 (카운터 이름, 증가값, 타임스탬프 필드)
    COUNTER_MAPPING = {
        "message": ("message_count", 1, "last_message_time"),
        "error": ("error_count", 1, "last_error_time"),
        "orderbook": ("orderbook_count", 1, "last_orderbook_time")
    }
    
    # 연결 상태 업데이트 매핑 (값은 연결 상태)
    CONNECTION_MAPPING = {
        "connect": True,     # 연결됨
        "disconnect": False, # 연결 해제됨
        "reconnect": True    # 재연결됨
    }
    
    # 메시지 통계 업데이트 매핑 (값은 통계 카운터에 사용할 타입 키)
    MESSAGE_STATS_MAPPING = {
        "message_stats": None,  # 이벤트에서 message_type 속성 사용
        "snapshot": "snapshot", # 스냅샷으로 고정
        "delta": "delta"        # 델타로 고정
    }
    
    # 구독 관련 이벤트 매핑
    SUBSCRIPTION_MAPPING = {
        "subscription_metric": True,
        "subscription": True
    }
    
    def __init__(self, metrics_store, events_store, event_bus, logger=None, status_emojis=None):
        """
        메트릭 이벤트 핸들러 초기화
        
        Args:
            metrics_store: 메트릭 데이터 저장소 (딕셔너리 참조)
            events_store: 이벤트 기록 저장소 (딕셔너리 참조)
            event_bus: 이벤트 버스 인스턴스
            logger: 로거 인스턴스 (없으면 기본 로거 사용)
            status_emojis: 상태 이모지 딕셔너리 (없으면 기본값 사용)
        """
        self.metrics = metrics_store
        self.events = events_store
        self.event_bus = event_bus
        self.logger = logger or get_unified_logger()
        
        # 상태 이모지 설정
        self.status_emojis = status_emojis or {
            "CONNECTED": "🟢",
            "DISCONNECTED": "🔴",
            "CONNECTING": "🟡",
            "ERROR": "⛔",
            "UNKNOWN": "⚪"
        }
        
    def _ensure_exchange_initialized(self, exchange_code):
        """
        거래소가 초기화되었는지 확인하고, 초기화되지 않았으면 메인 매니저에 요청
        
        Args:
            exchange_code: 거래소 코드
        """
        # 이 메서드는 매니저에서 호출 전에 이미 초기화 여부를 체크했으므로
        # 여기서는 중복 체크를 하지 않음
        pass
    
    @with_standard_error_handling
    @validate_required_fields(["exchange_code", "event_type"])
    def handle_metric_event(self, event_data):
        """
        메트릭 이벤트 처리 중앙 메서드
        
        모든 메트릭 업데이트는 이 메서드를 통해 처리됩니다.
        
        Args:
            event_data: 이벤트 데이터 (딕셔너리)
        """
        exchange_code = event_data["exchange_code"].lower()
        event_type = event_data["event_type"]
        
        # 간단한 카운터 업데이트 (message, error, orderbook 등)
        if event_type in self.COUNTER_MAPPING:
            counter_name, value, timestamp_field = self.COUNTER_MAPPING[event_type]
            self.update_counter(exchange_code, counter_name, value, timestamp_field)
            
        # 특수 파라미터가 필요한 카운터 업데이트 (byte_size 등)
        elif event_type == "bytes":
            self.update_counter(exchange_code, "bytes_received", event_data.get("byte_size", 0))
            
        # 연결 상태 업데이트
        elif event_type in self.CONNECTION_MAPPING:
            status = self.CONNECTION_MAPPING[event_type]
            # connect 이벤트는 status 필드를 확인하고, 없으면 기본값 사용
            if event_type == "connect" and "status" in event_data:
                status = event_data.get("status", True)
            self.update_connection_metric(exchange_code, status)
            
        # 메시지 통계 업데이트
        elif event_type in self.MESSAGE_STATS_MAPPING:
            message_type = self.MESSAGE_STATS_MAPPING[event_type]
            # message_stats 이벤트 타입은 추가 데이터에서 message_type을 가져옴
            if message_type is None:
                message_type = event_data.get("message_type", "unknown")
            self.update_message_stats(exchange_code, message_type)
            
        # 구독 관련 이벤트 업데이트
        elif event_type in self.SUBSCRIPTION_MAPPING:
            self.handle_subscription_metric(
                exchange_code, 
                event_data.get("status", "unknown"), 
                event_data.get("symbols_count", 0)
            )
            
        # 처리 시간 업데이트
        elif event_type == "processing_time":
            self.update_processing_time(exchange_code, event_data.get("processing_time", 0))
            
        # 알 수 없는 이벤트 타입
        else:
            self.logger.warning(f"[Metrics] 알 수 없는 이벤트 타입: {event_type}")
        
        # 이벤트 기록
        self.record_event(exchange_code, event_data)
    
    @prepare_exchange
    def update_message_stats(self, exchange_code, message_type):
        """
        메시지 통계 업데이트 내부 메서드
        
        Args:
            exchange_code: 거래소 코드
            message_type: 메시지 타입
        """
        # 통합된 메트릭 구조에서 메시지 통계 접근
        message_stats = self.metrics[exchange_code]["message_stats"]
        
        # 총 메시지 수 증가
        message_stats["total_received"] += 1
        
        # 타입별 카운트 증가 (snapshot, delta, error 등)
        type_key = f"{message_type}_received"
        if type_key not in message_stats:
            message_stats[type_key] = 0
        message_stats[type_key] += 1
        
        # 마지막 수신 시간 업데이트
        message_stats["last_received"] = time.time()
    
    @prepare_exchange
    def update_connection_metric(self, exchange_code, status):
        """
        연결 상태 업데이트 내부 메서드
        
        Args:
            exchange_code: 거래소 코드
            status: 연결 상태 (True/False)
        """
        # 기존 상태와 다른 경우에만 업데이트
        if self.metrics[exchange_code]["connected"] != status:
            self.metrics[exchange_code]["connected"] = status
            self.metrics[exchange_code]["connection_time"] = time.time()
            
            # 로그 출력
            status_str = "연결됨" if status else "연결 해제됨"
            self.logger.info(f"[Metrics] {exchange_code} 연결 상태 변경: {status_str}")
            
            # 상태 변경 이벤트 발행 (내부에서 변경된 경우)
            connection_event = {
                "exchange_code": exchange_code,
                "status": "connected" if status else "disconnected",
                "timestamp": time.time(),
                "source": "metrics_internal"
            }
            
            # 이벤트 발행 (중요한 이벤트이므로 실패해도 폴백을 호출하지 않음)
            self._publish_event("connection_status_changed", connection_event)
    
    @prepare_exchange
    def update_counter(self, exchange_code, counter_name, value=1, timestamp_field=None):
        """
        범용 카운터 업데이트 내부 메서드
        
        여러 카운터 관련 메서드들의 중복을 제거하고 하나로 통합합니다.
        메시지, 에러, 바이트, 오더북 카운트 등을 모두 이 하나의 메서드로 처리합니다.
        
        Args:
            exchange_code: 거래소 코드
            counter_name: 업데이트할 카운터 이름
            value: 증가시킬 값 (기본값: 1)
            timestamp_field: 타임스탬프를 업데이트할 필드 (없으면 업데이트 안함)
        """
        # 0이나 음수 값은 증가시키지 않음 (바이트 수 등을 위한 안전장치)
        if value <= 0:
            return
            
        # 카운터 증가
        self.metrics[exchange_code][counter_name] += value
        
        # 타임스탬프 업데이트 (필드가 지정된 경우)
        if timestamp_field:
            self.metrics[exchange_code][timestamp_field] = time.time()
    
    def create_event_data(self, exchange_code, event_type, source=None, normalize_code=True, **additional_data):
        """
        이벤트 데이터 생성을 위한 공통 헬퍼 메서드
        
        Args:
            exchange_code: 거래소 코드
            event_type: 이벤트 타입
            source: 이벤트 소스 (기본값: 호출자 정보)
            normalize_code: 거래소 코드를 소문자로 정규화할지 여부
            **additional_data: 추가 데이터
            
        Returns:
            dict: 생성된 이벤트 데이터
        """
        # 소스가 지정되지 않은 경우 기본값 설정
        if source is None:
            # 호출자 정보 추적을 통한 자동 소스 설정
            frame = inspect.currentframe().f_back
            caller_name = frame.f_code.co_name if frame else "unknown"
            source = f"method:{caller_name}"
        
        # 거래소 코드 정규화 (소문자 변환)
        if normalize_code and exchange_code:
            exchange_code = exchange_code.lower()
            
        # 기본 이벤트 데이터 생성
        event_data = {
            "exchange_code": exchange_code,
            "event_type": event_type,
            "timestamp": time.time(),
            "source": source
        }
        
        # 추가 데이터 병합
        if additional_data:
            event_data.update(additional_data)
        
        return event_data
    
    def record_event(self, exchange_code, event_data):
        """
        이벤트 기록 내부 메서드
        
        시간 기반 이벤트 필터링을 적용하여 초당 최대 5개의 이벤트만 저장합니다 (0.2초마다)
        
        Args:
            exchange_code: 거래소 코드
            event_data: 이벤트 데이터
        """
        # 타임스탬프 지정되지 않은 경우 현재 시간 사용
        current_time = time.time()
        if "timestamp" not in event_data:
            event_data["timestamp"] = current_time
        
        # 마지막 이벤트 저장 시간 가져오기 (거래소별로 저장)
        last_event_time_key = "_last_event_recorded_time"
        last_event_time = self.metrics[exchange_code].get(last_event_time_key, 0)
        
        # 중요 이벤트 여부 확인
        event_type = event_data.get("event_type", "")
        important_events = ["connect", "disconnect", "error", "subscription"]
        is_important = event_type in important_events
        
        # 중요 이벤트는 항상 저장, 일반 이벤트는 0.2초에 한 번만 저장
        if is_important or (current_time - last_event_time) >= 0.2:
            # 이벤트 저장
            self.events[exchange_code].append(event_data)
            
            # 마지막 이벤트 저장 시간 업데이트 (중요 이벤트는 타이밍에 영향 없음)
            if not is_important:
                self.metrics[exchange_code][last_event_time_key] = current_time
            
            # 버퍼 상태 확인
            buffer_size = len(self.events[exchange_code])
            buffer_capacity = self.events[exchange_code].maxlen or 300
            
            # 버퍼가 90% 이상 찼을 때 처리
            if buffer_size >= int(buffer_capacity * 0.9):
                # 경고 로그 (1초에 한 번만 출력)
                last_warning_key = "_last_buffer_warning_time"
                last_warning_time = self.metrics[exchange_code].get(last_warning_key, 0)
                
                if (current_time - last_warning_time) >= 1.0:
                    self.logger.warning(f"[Metrics] {exchange_code} 이벤트 버퍼가 거의 가득 찼습니다: {buffer_size}/{buffer_capacity}")
                    self.metrics[exchange_code][last_warning_key] = current_time
                
                # 버퍼 크기를 50%로 즉시 줄임
                target_size = int(buffer_capacity * 0.5)
                
                # 이벤트 분류
                important_events = []
                normal_events = []
                
                for evt in self.events[exchange_code]:
                    evt_type = evt.get("event_type", "")
                    if evt_type in ["connect", "disconnect", "error", "subscription"]:
                        important_events.append(evt)
                    else:
                        normal_events.append(evt)
                
                # 새 이벤트 목록 생성
                new_events = deque(maxlen=buffer_capacity)
                
                # 중요 이벤트 추가 (최대 10개)
                important_events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
                for evt in important_events[:10]:
                    new_events.append(evt)
                
                # 나머지는 최신 일반 이벤트로 채움
                remaining_slots = target_size - len(new_events)
                if remaining_slots > 0:
                    normal_events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
                    for evt in normal_events[:remaining_slots]:
                        new_events.append(evt)
                
                # 이벤트 목록 교체
                self.events[exchange_code] = new_events
                
                # 로그 출력
                self.logger.info(f"[Metrics] {exchange_code} 이벤트 버퍼 축소: {buffer_size} → {len(new_events)}")
    
    @prepare_exchange
    def update_processing_time(self, exchange_code, processing_time_ms):
        """
        메시지 처리 시간 업데이트 내부 메서드
        
        Args:
            exchange_code: 거래소 코드
            processing_time_ms: 처리 시간 (ms)
        """
        metrics = self.metrics[exchange_code]
        
        # 처리 시간 목록에 추가 (최대 100개 유지)
        metrics["processing_times"].append(processing_time_ms)
        if len(metrics["processing_times"]) > 100:
            metrics["processing_times"].pop(0)
            
        # 평균 계산 (최적화: 매번 전체 합계를 계산하는 대신 이동 평균 사용)
        # 이전 평균에 새 값의 영향을 가중치로 적용 (0.95 * old + 0.05 * new)
        if metrics["avg_processing_time"] == 0:
            # 첫 번째 값은 그대로 사용
            metrics["avg_processing_time"] = processing_time_ms
        else:
            # 이동 평균 계산 (지수 가중 이동 평균)
            metrics["avg_processing_time"] = 0.95 * metrics["avg_processing_time"] + 0.05 * processing_time_ms
        
        # 처리 시간이 너무 길면 경고
        if processing_time_ms > 100:  # 100ms 이상이면 경고
            self.logger.warning(f"[Metrics] {exchange_code} 메시지 처리 시간이 긺: {processing_time_ms}ms")
    
    @prepare_exchange
    def handle_subscription_metric(self, exchange_code, status, symbols_count):
        """
        구독 상태 메트릭 업데이트 내부 메서드
        
        Args:
            exchange_code: 거래소 코드
            status: 구독 상태 ("subscribing", "subscribed", "unsubscribed" 등)
            symbols_count: 심볼 수
        """
        metrics = self.metrics[exchange_code]
        
        # 구독 상태 업데이트
        metrics["subscription_status"] = status
        metrics["subscription_time"] = time.time()
        metrics["subscribed_symbols"] = symbols_count
        
        # 로그 출력
        self.logger.info(f"[Metrics] {exchange_code} 구독 상태 변경: {status}, 심볼 수: {symbols_count}")
    
    @with_standard_error_handling
    @validate_required_fields(["exchange_code"])
    def handle_external_event(self, event_data, target_event_type=None, source=None, **kwargs):
        """
        외부 이벤트를 메트릭 이벤트로 변환하는 공통 메서드
        
        Args:
            event_data: 외부 이벤트 데이터
            target_event_type: 변환할 이벤트 타입 (없으면 원본 이벤트 타입 사용)
            source: 이벤트 소스 (없으면 자동 설정)
            **kwargs: 추가 데이터 (event_data보다 우선 적용)
            
        Returns:
            bool: 처리 성공 여부
        """
        # 이벤트 타입 결정
        event_type = target_event_type or event_data.get("event_type")
        if not event_type:
            self.logger.warning(f"[Metrics] 유효하지 않은 이벤트 (event_type 없음): {event_data}")
            return False
            
        # 메트릭 이벤트 생성
        exchange_code = event_data["exchange_code"]
        source = source or f"external_{event_data.get('type', 'event')}"
        
        # 필요한 속성만 추출하여 추가 데이터로 전달
        additional_data = {}
        
        # 많이 사용되는 필드들 복사
        for key in ["status", "message_type", "symbols_count", "processing_time", "byte_size"]:
            if key in event_data:
                additional_data[key] = event_data[key]
        
        # 직접 전달된 kwargs 값 추가 (event_data보다 우선 적용)
        additional_data.update(kwargs)
        
        # 함께 업데이트할 추가 이벤트 타입 목록
        also_update = additional_data.pop('_also_update', [])
        
        # 메트릭 이벤트 생성
        metric_event = self.create_event_data(
            exchange_code,
            event_type,
            source,
            **additional_data
        )
        
        # 중앙 처리 메서드 호출
        self.handle_metric_event(metric_event)
        
        # 추가 이벤트 타입도 함께 처리
        for additional_event_type in also_update:
            additional_metric_event = self.create_event_data(
                exchange_code,
                additional_event_type,
                source,
                **additional_data
            )
            self.handle_metric_event(additional_metric_event)
        
        return True
    
    @with_standard_error_handling
    @validate_required_fields(["exchange_code", "status"])
    def handle_connection_event(self, event_data):
        """
        연결 상태 변경 이벤트 처리
        
        외부에서 발생한 연결 상태 이벤트를 메트릭 이벤트로 변환합니다.
        
        Args:
            event_data: 이벤트 데이터
        """
        # "status" 값 확인 (True/False 또는 문자열)
        status_value = event_data["status"]
        is_connected = status_value in ["connected", "connect", True]
        
        # 이벤트 처리
        self.handle_external_event(
            event_data,
            "connect",
            "connection_event",
            status=is_connected  # 키워드 인자로 status 값 전달
        )
    
    @with_standard_error_handling
    @validate_required_fields(["exchange_code", "symbol"])
    def handle_orderbook_event(self, event_data):
        """
        오더북 이벤트 처리
        
        스냅샷 또는 델타 오더북 이벤트를 메트릭 이벤트로 변환합니다.
        
        Args:
            event_data: 이벤트 데이터
        """
        exchange_code = event_data["exchange_code"]
        
        # 이벤트 타입 추출 (스냅샷 또는 델타)
        event_subtype = "snapshot" if "snapshot" in event_data.get("type", "") else "delta"
        
        # 한 번에 여러 메트릭 업데이트 (최적화)
        self.handle_external_event(
            {"exchange_code": exchange_code},
            "message_stats",
            "orderbook_event",
            message_type=event_subtype,  # 메시지 통계용
            _also_update=["message", "orderbook"]  # 추가로 업데이트할 카운터들
        )
    
    @with_standard_error_handling
    @validate_required_fields(["exchange_code"])
    def handle_orderbook_error_event(self, event_data):
        """
        오더북 에러 이벤트 처리
        
        오더북 에러 이벤트를 메트릭 이벤트로 변환합니다.
        
        Args:
            event_data: 이벤트 데이터
        """
        # 공통 이벤트 처리 메서드 사용
        self.handle_external_event(
            event_data,
            "error",
            "orderbook_error"
        )
    
    @with_standard_error_handling
    @validate_required_fields(["exchange_code", "status"])
    def handle_subscription_event(self, event_data):
        """
        구독 상태 변경 이벤트 처리
        
        외부에서 발생한 구독 상태 이벤트를 메트릭 이벤트로 변환합니다.
        
        Args:
            event_data: 이벤트 데이터
        """
        # 공통 이벤트 처리 메서드 사용
        self.handle_external_event(
            event_data,
            "subscription_metric",
            "subscription_event"
        )
    
    def _publish_event(self, event_type: str, data: dict, on_failure: Callable = None) -> None:
        """
        이벤트 버스를 통해 이벤트를 발행하는 내부 공통 메서드
        
        Args:
            event_type: 이벤트 유형 (metric_event, connection_status_changed 등)
            data: 이벤트 데이터
            on_failure: 이벤트 발행 실패 시 호출할 폴백 함수 (선택 사항)
        """
        try:
            # 비동기 컨텍스트에서 실행 중인지 확인하고 적절한 메서드 호출
            try:
                import asyncio
                asyncio.get_running_loop()
                # 이벤트 루프가 실행 중이면 비동기로 발행
                asyncio.create_task(self.event_bus.publish(event_type, data))
            except (RuntimeError, ImportError):
                # 이벤트 루프가 없으면 동기식으로 발행
                self.event_bus.publish_sync(event_type, data)
        except Exception as e:
            self.logger.error(f"[Metrics] 이벤트 발행 중 오류 ({event_type}): {e}")
            if on_failure:
                try:
                    on_failure()
                except Exception as e2:
                    self.logger.error(f"[Metrics] 오류 핸들러 실행 중 추가 오류: {e2}") 