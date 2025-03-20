# file: orderbook/manager/websocket_metrics_manager.py

import time
import inspect
from typing import Dict, Callable, Any, List, Optional
from collections import defaultdict, deque

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import (
    METRICS_MAX_EVENTS,
    METRICS_HEALTH_THRESHOLD,
    EXCHANGE_NAMES_KR
)

# 핸들러 클래스 임포트
from crosskimp.ob_collector.orderbook.metric.metrics_handler import (
    MetricsEventHandler, prepare_exchange, validate_required_fields, with_standard_error_handling
)

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# STATUS_EMOJIS 직접 정의 (ob_constants에서 가져오지 않고 직접 정의)
STATUS_EMOJIS = {
    "CONNECTED": "🟢",
    "DISCONNECTED": "🔴",
    "CONNECTING": "🟡",
    "ERROR": "⛔",
    "UNKNOWN": "⚪"
}

class WebsocketMetricsManager:
    """
    웹소켓 메트릭 및 상태 관리 클래스 (싱글톤 패턴)
    
    연결 상태, 메시지 통계, 메트릭을 중앙에서 관리합니다.
    이벤트 버스 기반으로 모든 메트릭 데이터를 처리합니다.
    """
    _instance = None
    
    # 메트릭 초기값 정의 (중첩 dict 대신 함수로 구조화)
    @classmethod
    def _create_default_metrics(cls):
        """기본 메트릭 구조 생성"""
        return {
            # 연결 관련 메트릭
            "connected": False,
            "connection_time": 0,
            
            # 메시지 관련 메트릭
            "message_count": 0, 
            "error_count": 0,
            "last_message_time": 0,
            "last_error_time": 0,
            "bytes_received": 0,
            
            # 오더북 관련 메트릭
            "orderbook_count": 0,
            "last_orderbook_time": 0,
            
            # 처리 시간 관련 메트릭
            "processing_times": [],
            "avg_processing_time": 0,
            
            # 구독 관련 메트릭
            "subscription_status": "none",
            "subscription_time": 0,
            "subscribed_symbols": 0,
            
            # 메시지 통계를 메트릭 내부로 통합
            "message_stats": cls._create_default_message_stats()
        }
    
    @classmethod
    def _create_default_message_stats(cls):
        """기본 메시지 통계 구조 생성"""
        return {
            "total_received": 0,      # 총 메시지 수
            "snapshot_received": 0,   # 스냅샷 메시지 수
            "delta_received": 0,      # 델타 메시지 수
            "errors": 0,              # 오류 수
            "last_received": None     # 마지막 메시지 시간
        }
    
    @classmethod
    def get_instance(cls):
        """싱글톤 인스턴스 반환"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        """초기화 - 싱글톤 패턴으로 직접 호출 방지"""
        # 이미 인스턴스가 있는 경우 예외 발생
        if WebsocketMetricsManager._instance is not None:
            raise Exception("이 클래스는 싱글톤입니다. get_instance()를 사용하세요.")
            
        # 통합 메트릭 저장 구조
        self.metrics = {}  # 거래소 코드를 키로 하는 메트릭 저장소
        
        # 이벤트 기록 (디버깅 및 모니터링용)
        self.events = defaultdict(lambda: deque(maxlen=METRICS_MAX_EVENTS))
        
        # 성능 관련 설정
        self.health_threshold = METRICS_HEALTH_THRESHOLD
        
        # 처리율 계산을 위한 임시 저장소
        self.last_message_counts = {}
        self.last_rate_calculation_time = {}
        
        # 로거 설정
        self.logger = logger
        
        # 이벤트 버스 초기화 및 구독 설정
        from crosskimp.ob_collector.orderbook.util.event_bus import EventBus
        self.event_bus = EventBus.get_instance()
        
        # 메트릭 이벤트 핸들러 생성
        self.metrics_handler = MetricsEventHandler(
            self.metrics, 
            self.events, 
            self.event_bus, 
            self.logger, 
            STATUS_EMOJIS
        )
        
        # 이벤트 구독 설정
        self._setup_event_subscriptions()
        
        # 정리 타이머 시작
        self._start_cleanup_timer()
        
        # 처리율 계산 타이머 시작
        self._start_rate_calculation_timer()
        
        self.logger.info("메트릭 매니저 초기화 완료")
            
    def _setup_event_subscriptions(self):
        """이벤트 버스 구독 설정"""            
        # 메트릭 관련 이벤트 구독
        self.event_bus.subscribe("metric_event", self._handle_metric_event)
        
        # 연결 관련 이벤트 구독
        self.event_bus.subscribe("connection_status_changed", self._handle_connection_event)
        
        # 구독 관련 이벤트 구독
        self.event_bus.subscribe("subscription_status_changed", self._handle_subscription_event)
        
        # 오더북 관련 이벤트 구독
        self.event_bus.subscribe("orderbook_snapshot", self._handle_orderbook_event)
        self.event_bus.subscribe("orderbook_delta", self._handle_orderbook_event)
        self.event_bus.subscribe("orderbook_error", self._handle_orderbook_error_event)
        
        self.logger.debug("메트릭 매니저의 이벤트 구독 설정 완료")
    
    def _ensure_exchange_initialized(self, exchange_code):
        """
        거래소가 초기화되었는지 확인하고, 초기화되지 않았으면 초기화
        
        Args:
            exchange_code: 거래소 코드
        """
        if exchange_code not in self.metrics:
            self.metrics[exchange_code] = self._create_default_metrics()
            self.logger.debug(f"[Metrics] {exchange_code} 거래소 메트릭 초기화")
    
    # 이벤트 핸들러 메서드들 - 이제 모두 MetricsEventHandler로 위임
    @with_standard_error_handling
    def _handle_metric_event(self, event_data):
        """
        메트릭 이벤트 처리 중앙 메서드 - 핸들러로 위임
        
        Args:
            event_data: 이벤트 데이터 (딕셔너리)
        """
        # 거래소 초기화 보장
        exchange_code = event_data.get("exchange_code", "").lower() if event_data.get("exchange_code") else ""
        if exchange_code:
            self._ensure_exchange_initialized(exchange_code)
        
        # 핸들러로 이벤트 처리 위임
        self.metrics_handler.handle_metric_event(event_data)
        
    @with_standard_error_handling
    def _handle_connection_event(self, event_data):
        """
        연결 상태 변경 이벤트 처리 - 핸들러로 위임
        
        Args:
            event_data: 이벤트 데이터
        """
        # 거래소 초기화 보장
        exchange_code = event_data.get("exchange_code", "").lower() if event_data.get("exchange_code") else ""
        if exchange_code:
            self._ensure_exchange_initialized(exchange_code)
        
        # 핸들러로 이벤트 처리 위임
        self.metrics_handler.handle_connection_event(event_data)
    
    @with_standard_error_handling
    def _handle_orderbook_event(self, event_data):
        """
        오더북 이벤트 처리 - 핸들러로 위임
        
        Args:
            event_data: 이벤트 데이터
        """
        # 거래소 초기화 보장
        exchange_code = event_data.get("exchange_code", "").lower() if event_data.get("exchange_code") else ""
        if exchange_code:
            self._ensure_exchange_initialized(exchange_code)
        
        # 핸들러로 이벤트 처리 위임
        self.metrics_handler.handle_orderbook_event(event_data)
        
    @with_standard_error_handling
    def _handle_orderbook_error_event(self, event_data):
        """
        오더북 에러 이벤트 처리 - 핸들러로 위임
        
        Args:
            event_data: 이벤트 데이터
        """
        # 거래소 초기화 보장
        exchange_code = event_data.get("exchange_code", "").lower() if event_data.get("exchange_code") else ""
        if exchange_code:
            self._ensure_exchange_initialized(exchange_code)
        
        # 핸들러로 이벤트 처리 위임
        self.metrics_handler.handle_orderbook_error_event(event_data)
            
    @with_standard_error_handling
    def _handle_subscription_event(self, event_data):
        """
        구독 상태 변경 이벤트 처리 - 핸들러로 위임
        
        Args:
            event_data: 이벤트 데이터
        """
        # 거래소 초기화 보장
        exchange_code = event_data.get("exchange_code", "").lower() if event_data.get("exchange_code") else ""
        if exchange_code:
            self._ensure_exchange_initialized(exchange_code)
        
        # 핸들러로 이벤트 처리 위임
        self.metrics_handler.handle_subscription_event(event_data)
    
    def _publish_event(self, event_type: str, data: dict, on_failure: Callable = None) -> None:
        """
        이벤트 버스를 통해 이벤트를 발행하는 내부 공통 메서드
        
        Args:
            event_type: 이벤트 유형 (metric_event, connection_status_changed 등)
            data: 이벤트 데이터
            on_failure: 이벤트 발행 실패 시 호출할 폴백 함수 (선택 사항)
        """
        # 핸들러를 통해 이벤트 발행
        self.metrics_handler._publish_event(event_type, data, on_failure)
    
    # ------ 공개 API 메서드 (사용자 인터페이스) ------

    @prepare_exchange
    def get_exchange_metrics(self, exchange_code: str) -> dict:
        """특정 거래소의 메트릭 정보 반환"""
        # 필요한 데이터만 추출하여 새로운 딕셔너리 생성
        metrics = self.metrics[exchange_code]
        result = {}
        
        # 메시지 통계는 이미 metrics 내부에 포함되어 있으므로 
        # 최상위 수준의 메트릭으로 일부 통계만 노출
        result.update({
            # 기본 값들 복사
            "connected": metrics["connected"],
            "connection_time": metrics["connection_time"],
            "message_count": metrics["message_count"],
            "error_count": metrics["error_count"],
            "last_message_time": metrics["last_message_time"],
            "last_error_time": metrics["last_error_time"],
            "bytes_received": metrics["bytes_received"],
            "orderbook_count": metrics["orderbook_count"],
            "last_orderbook_time": metrics["last_orderbook_time"],
            "avg_processing_time": metrics["avg_processing_time"],
            "subscription_status": metrics["subscription_status"],
            "subscription_time": metrics["subscription_time"],
            "subscribed_symbols": metrics["subscribed_symbols"],
            # 메시지 통계에서 추출
            "total_messages": metrics["message_stats"]["total_received"],
            "snapshot_messages": metrics["message_stats"].get("snapshot_received", 0),
            "delta_messages": metrics["message_stats"].get("delta_received", 0)
        })
        
        # 연결 상태 정보 추가
        state_name = "CONNECTED" if metrics["connected"] else "DISCONNECTED"
        result["connection_state_name"] = state_name
        result["connection_emoji"] = STATUS_EMOJIS.get(state_name, "⚪")
        
        return result

    def initialize_exchange(self, exchange_code):
        """
        거래소 메트릭 초기화 (이전 코드와의 호환성 유지용)
        
        Args:
            exchange_code: 거래소 코드
        """
        exchange_code = exchange_code.lower() if exchange_code else exchange_code
        self._ensure_exchange_initialized(exchange_code)
            
    @prepare_exchange
    def record_metric(self, exchange_code: str, event_type: str, **additional_data) -> None:
        """
        다양한 메트릭 이벤트를 기록하는 통합 메서드 (외부 API)
        
        이전 버전과의 호환성을 위해 유지
        
        Args:
            exchange_code: 거래소 코드
            event_type: 이벤트 타입 (message, error, bytes, orderbook 등)
            **additional_data: 추가 데이터 (byte_size, processing_time, message_type 등)
        """
        # 이벤트 데이터 생성
        event_data = self.create_metric_event(exchange_code, event_type, **additional_data)
        
        # 이벤트 발행
        self._publish_event("metric_event", event_data)

    def get_metrics(self) -> Dict:
        """모든 메트릭 데이터 반환"""
        result = {}
        current_time = time.time()
        
        for exchange_code in self.metrics:
            # 기존의 get_exchange_metrics 메서드를 활용하여 중복 코드 제거
            exchange_metrics = self.get_exchange_metrics(exchange_code)
            
            # 필요한 항목만 선택하여 결과에 추가
            result[exchange_code] = {
                "connected": exchange_metrics["connected"],
                "connection_state": exchange_metrics["connection_state_name"],
                "message_count": exchange_metrics["message_count"],
                "error_count": exchange_metrics["error_count"],
                "last_message_time": exchange_metrics["last_message_time"],
                "bytes_received": exchange_metrics["bytes_received"],
                "orderbook_count": exchange_metrics["orderbook_count"],
                "total_messages": exchange_metrics["total_messages"],
                "emoji": exchange_metrics["connection_emoji"]
            }
            
            # 연결 상태에 따른 추가 정보
            if exchange_metrics["connected"]:
                # 연결 중인 경우 업타임 계산
                uptime = current_time - exchange_metrics["connection_time"]
                result[exchange_code]["uptime_seconds"] = int(uptime)
                result[exchange_code]["uptime_formatted"] = f"{int(uptime // 3600)}시간 {int((uptime % 3600) // 60)}분 {int(uptime % 60)}초"
            
        return result

    @prepare_exchange
    def get_events(self, exchange_code: str, limit: int = None) -> List[Dict]:
        """특정 거래소의 이벤트 기록 반환"""
        # 거래소 이벤트 가져오기
        exchange_events = list(self.events[exchange_code])
        
        # 제한이 있으면 최신 이벤트만 반환
        if limit and limit > 0:
            exchange_events = exchange_events[-limit:]
            
        return exchange_events
    
    def create_metric_event(self, exchange_code: str, event_type: str, **data) -> Dict:
        """
        메트릭 이벤트 생성
        
        Args:
            exchange_code: 거래소 코드
            event_type: 이벤트 타입
            **data: 추가 이벤트 데이터
            
        Returns:
            Dict: 생성된 이벤트 데이터
        """
        # 핸들러의 create_event_data 메서드 활용
        return self.metrics_handler.create_event_data(
            exchange_code, 
            event_type, 
            source="api_call", 
            **data
        )
    
    def publish_metric_event(self, exchange_code: str, event_type: str, **data) -> bool:
        """
        메트릭 이벤트 생성 및 발행
        
        Args:
            exchange_code: 거래소 코드
            event_type: 이벤트 타입
            **data: 추가 이벤트 데이터
            
        Returns:
            bool: 성공 여부
        """
        # 이벤트 생성
        event_data = self.create_metric_event(exchange_code, event_type, **data)
        
        # 이벤트 발행
        try:
            self._publish_event("metric_event", event_data)
            return True
        except Exception as e:
            self.logger.error(f"[Metrics] 이벤트 발행 실패: {e}")
            return False
    
    def get_dashboard_metrics(self) -> Dict:
        """
        대시보드용 요약 메트릭 데이터 반환
        
        웹 인터페이스나 상태 모니터링용으로 최적화된 메트릭 데이터
        
        Returns:
            Dict: 요약된 메트릭 데이터
        """
        result = {}
        current_time = time.time()
        
        for exchange_code in self.metrics:
            metrics = self.metrics[exchange_code]
            
            # 활성 상태 계산 (마지막 메시지 수신 후 60초 이내이면 활성)
            last_message_time = metrics.get("last_message_time", 0)
            is_active = (current_time - last_message_time) < 60
            
            # 마지막 메시지 수신 시간으로부터 경과 시간 계산
            time_since_last_message = current_time - last_message_time if last_message_time > 0 else None
            
            # 필수 정보만 추출
            result[exchange_code] = {
                "connected": metrics["connected"],
                "active": is_active,
                "status": "CONNECTED" if metrics["connected"] else "DISCONNECTED",
                "emoji": STATUS_EMOJIS.get("CONNECTED" if metrics["connected"] else "DISCONNECTED", "⚪"),
                "message_count": metrics["message_count"],
                "orderbook_count": metrics["orderbook_count"],
                "error_count": metrics["error_count"],
                "subscription_status": metrics["subscription_status"],
                "subscribed_symbols": metrics["subscribed_symbols"],
                "time_since_last_message": int(time_since_last_message) if time_since_last_message else None,
                "bytes_received": metrics["bytes_received"],
                "avg_processing_time": round(metrics["avg_processing_time"], 2)
            }
            
        return result
    
    def get_event_summary(self, exchange_code=None) -> Dict:
        """
        이벤트 타입별 통계 요약 반환
        
        Args:
            exchange_code: 거래소 코드 (None이면 모든 거래소)
            
        Returns:
            Dict: 이벤트 통계 요약
        """
        # 유효한 거래소 목록 결정
        exchanges = [exchange_code] if exchange_code else list(self.metrics.keys())
        
        result = {}
        for ex_code in exchanges:
            # 거래소가 초기화되지 않았으면 건너뛰기
            if ex_code not in self.metrics:
                continue
                
            # 이벤트 타입별 카운트 집계
            event_counts = defaultdict(int)
            for event in self.events[ex_code]:
                event_type = event.get("event_type", "unknown")
                event_counts[event_type] += 1
                
            # 결과에 추가
            result[ex_code] = dict(event_counts)
            
        return result
    
    def clean_up(self, exchange_code=None, purge_events=True, clean_inactive=True):
        """
        메트릭 데이터 정리 및 최적화
        
        Args:
            exchange_code: 정리할 거래소 코드 (None이면 모든 거래소)
            purge_events: 오래된 이벤트 제거 여부
            clean_inactive: 비활성 거래소 정리 여부
            
        Returns:
            Dict: 정리 결과 요약
        """
        # 유효한 거래소 목록 결정
        exchanges = [exchange_code] if exchange_code else list(self.metrics.keys())
        
        result = {
            "purged_events": 0,
            "cleaned_exchanges": 0,
        }
        
        for ex_code in exchanges:
            # 거래소가 초기화되지 않았으면 건너뛰기
            if ex_code not in self.metrics:
                continue
            
            # 이벤트 정리
            if purge_events and ex_code in self.events:
                # 현재 이벤트 수
                current_events = list(self.events[ex_code])
                event_count = len(current_events)
                buffer_capacity = self.events[ex_code].maxlen or 300
                
                # 버퍼의 50%로 정리
                target_size = int(buffer_capacity * 0.5)
                
                # 중요 이벤트와 일반 이벤트 분류
                important_events = []
                normal_events = []
                
                for event in current_events:
                    event_type = event.get("event_type", "")
                    if event_type in ["connect", "disconnect", "error", "subscription"]:
                        important_events.append(event)
                    else:
                        normal_events.append(event)
                
                # 새 이벤트 목록 생성
                new_events = deque(maxlen=buffer_capacity)
                
                # 중요 이벤트 추가 (최대 10개)
                important_events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
                for event in important_events[:10]:
                    new_events.append(event)
                
                # 나머지는 최신 일반 이벤트로 채움
                remaining_slots = target_size - len(new_events)
                if remaining_slots > 0:
                    normal_events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
                    for event in normal_events[:remaining_slots]:
                        new_events.append(event)
                
                # 제거된 이벤트 수
                purged_count = event_count - len(new_events)
                result["purged_events"] += purged_count
                
                # 이벤트 목록 교체
                self.events[ex_code] = new_events
                
                # 로그 출력
                self.logger.info(f"[메트릭] 버퍼정리: [{ex_code}] | 이전: {event_count}건 | 현재: {len(new_events)}건 | 제거: {purged_count}건")
        
        # 메모리 정리 명시적 요청
        if purge_events and result["purged_events"] > 0:
            try:
                import gc
                gc.collect()
            except:
                pass
        
        # 정리 결과 로깅
        if result["purged_events"] > 0 or result["cleaned_exchanges"] > 0:
            self.logger.info(f"[메트릭] 자동정리 | 이벤트: {result['purged_events']}건 제거 | 거래소: {result['cleaned_exchanges']}개 정리")
        
        return result
    
    def get_memory_usage(self, exchange_code=None) -> Dict:
        """
        메트릭 매니저의 메모리 사용량 추정
        
        Args:
            exchange_code: 거래소 코드 (None이면 모든 거래소)
            
        Returns:
            Dict: 메모리 사용량 정보
        """
        # 유효한 거래소 목록 결정
        exchanges = [exchange_code] if exchange_code else list(self.metrics.keys())
        
        result = {
            "total_exchanges": len(exchanges),
            "total_events": 0,
            "metrics_size_estimate": 0,
            "events_size_estimate": 0,
            "exchanges": {}
        }
        
        for ex_code in exchanges:
            # 거래소가 초기화되지 않았으면 건너뛰기
            if ex_code not in self.metrics:
                continue
                
            # 이벤트 수
            events_count = len(self.events[ex_code])
            result["total_events"] += events_count
            
            # 추정 이벤트 메모리 크기 (바이트)
            avg_event_size = 500  # 기본값 (이벤트 데이터 크기 추정)
            events_size = events_count * avg_event_size
            result["events_size_estimate"] += events_size
            
            # 추정 메트릭 메모리 크기 (바이트)
            metrics_size = 2000  # 기본값 (메트릭 데이터 크기 추정)
            result["metrics_size_estimate"] += metrics_size
            
            # 거래소별 정보
            result["exchanges"][ex_code] = {
                "events_count": events_count,
                "events_size": events_size,
                "metrics_size": metrics_size
            }
            
        # 총 메모리 사용량 추정
        result["total_size_estimate"] = result["metrics_size_estimate"] + result["events_size_estimate"]
        
        # 가독성 좋게 단위 변환 (MB)
        result["total_size_mb"] = round(result["total_size_estimate"] / (1024 * 1024), 2)
        
        return result 

    def _start_cleanup_timer(self):
        """
        주기적인 메트릭 정리 타이머 시작
        
        10초마다 메트릭 정리 작업을 수행합니다.
        """
        try:
            import threading
            
            # 정리 작업 수행 함수
            def cleanup_task():
                try:
                    # 모든 거래소의 메트릭 정리
                    cleanup_result = self.clean_up(purge_events=True, clean_inactive=True)
                    
                    # 정리 결과 로깅
                    if cleanup_result["purged_events"] > 0 or cleanup_result["cleaned_exchanges"] > 0:
                        self.logger.info(f"[메트릭] 자동정리 | 이벤트: {cleanup_result['purged_events']}건 제거 | 거래소: {cleanup_result['cleaned_exchanges']}개 정리")
                    
                    # 다음 타이머 설정 (재귀)
                    self._cleanup_timer = threading.Timer(10, cleanup_task)
                    self._cleanup_timer.daemon = True  # 데몬 스레드로 설정
                    self._cleanup_timer.start()
                except Exception as e:
                    self.logger.error(f"[메트릭] 주기적 정리 작업 오류: {e}")
                    # 오류가 발생해도 타이머 재시작
                    self._cleanup_timer = threading.Timer(10, cleanup_task)
                    self._cleanup_timer.daemon = True
                    self._cleanup_timer.start()
            
            # 첫 타이머 시작
            self._cleanup_timer = threading.Timer(10, cleanup_task)
            self._cleanup_timer.daemon = True  # 메인 스레드가 종료되면 같이 종료
            self._cleanup_timer.start()
            
            self.logger.info("[메트릭] 주기적 정리 타이머 시작 (10초 간격)")
        except Exception as e:
            self.logger.error(f"[메트릭] 정리 타이머 시작 오류: {e}")

    def _start_rate_calculation_timer(self):
        """
        처리율 계산 타이머 시작
        
        1초마다 처리율을 계산하고 로깅합니다.
        """
        try:
            import threading
            
            # 처리율 계산 함수
            def rate_calculation_task():
                try:
                    # 모든 활성 거래소의 처리율 계산
                    for exchange_code in self.metrics.keys():
                        self._calculate_and_log_processing_rate(exchange_code)
                    
                    # 다음 타이머 설정 (재귀)
                    self._rate_timer = threading.Timer(1.0, rate_calculation_task)
                    self._rate_timer.daemon = True  # 데몬 스레드로 설정
                    self._rate_timer.start()
                except Exception as e:
                    self.logger.error(f"[메트릭] 처리율 계산 오류: {e}")
                    # 오류가 발생해도 타이머 재시작
                    self._rate_timer = threading.Timer(1.0, rate_calculation_task)
                    self._rate_timer.daemon = True
                    self._rate_timer.start()
            
            # 첫 타이머 시작
            self._rate_timer = threading.Timer(1.0, rate_calculation_task)
            self._rate_timer.daemon = True  # 메인 스레드가 종료되면 같이 종료
            self._rate_timer.start()
            
            self.logger.info("[메트릭] 처리율 계산 타이머 시작 (1초 간격)")
        except Exception as e:
            self.logger.error(f"[메트릭] 처리율 계산 타이머 시작 오류: {e}")
    
    def _calculate_and_log_processing_rate(self, exchange_code):
        """
        특정 거래소의 처리율을 계산하고 로깅
        
        Args:
            exchange_code: 거래소 코드
        """
        try:
            if exchange_code not in self.metrics:
                return
                
            # 현재 메시지 카운트와 시간
            current_time = time.time()
            current_count = self.metrics[exchange_code].get("message_count", 0)
            
            # 이전 값이 없으면 초기화
            if exchange_code not in self.last_message_counts:
                self.last_message_counts[exchange_code] = current_count
                self.last_rate_calculation_time[exchange_code] = current_time
                return
                
            # 메시지 차이 및 시간 차이 계산
            last_count = self.last_message_counts[exchange_code]
            last_time = self.last_rate_calculation_time[exchange_code]
            
            message_diff = current_count - last_count
            time_diff = current_time - last_time
            
            # 0으로 나누기 방지
            if time_diff <= 0:
                return
                
            # 처리율 계산 (초당 메시지 수)
            processing_rate = message_diff / time_diff
            
            # 값이 변경된 경우에만 로깅
            if message_diff > 0:
                # constants_v3.py에 정의된 한글 거래소 이름 사용
                exchange_name_kr = EXCHANGE_NAMES_KR.get(exchange_code, f"[{exchange_code}]")
                
                # [] 없이 깔끔하게 표시하기 위해 괄호 제거 및 너비 조정
                exchange_name = exchange_name_kr.replace("[", "").replace("]", "")
                exchange_name = f"[{exchange_name}]".ljust(18)  # 거래소 이름 필드 너비 넓게 설정
                
                self.logger.debug(
                    f"[메트릭] {exchange_name} | "
                    f"현재: {str(current_count).rjust(7)}건 | "
                    f"이전: {str(last_count).rjust(7)}건 | "
                    f"차이: {str(message_diff).rjust(5)}건 | "
                    f"속도: {processing_rate:.2f}건/초"
                )
            
            # 현재 값을 저장
            self.last_message_counts[exchange_code] = current_count
            self.last_rate_calculation_time[exchange_code] = current_time
            
        except Exception as e:
            self.logger.error(f"[메트릭] {exchange_code} 처리율 계산 중 오류: {e}")

# 모듈 레벨에서 싱글톤 인스턴스 미리 생성
metrics_manager = WebsocketMetricsManager.get_instance()