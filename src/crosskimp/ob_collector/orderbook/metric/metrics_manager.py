# file: orderbook/manager/websocket_metrics_manager.py

import time
from typing import Dict, Optional, List
from datetime import datetime
from collections import defaultdict, deque

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants import (
    EXCHANGE_NAMES_KR, STATUS_EMOJIS, WEBSOCKET_STATES, LOG_SYSTEM,
    METRICS_RATE_CALCULATION_INTERVAL, METRICS_DELAY_THRESHOLD_MS,
    METRICS_ALERT_COOLDOWN, METRICS_MAX_HISTORY, METRICS_MAX_EVENTS,
    METRICS_HEALTH_THRESHOLD
)

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# 연결 상태 상수 정의 (단순화)
CONNECTION_STATE = {
    "CONNECTED": "connected",
    "DISCONNECTED": "disconnected"
}

class WebsocketMetricsManager:
    """
    웹소켓 메트릭 및 상태 관리 클래스 (싱글톤 패턴)
    
    연결 상태, 메시지 통계, 메트릭을 중앙에서 관리합니다.
    """
    _instance = None
    
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
            
        # 메트릭 저장
        self.metrics = {
            "connection_state": {},          # 거래소별 연결 상태
            "processing_times": defaultdict(list), # 거래소별 처리 시간 기록
            "message_counts": {},            # 전체 메시지 수
            "orderbook_counts": {},          # 오더북 메시지 수
            "error_counts": {},              # 에러 수
            "last_update_times": {},         # 마지막 업데이트 시간
            "start_times": {},               # 시작 시간
            "bytes_received": {},            # 수신 바이트
            "processing_rates": {},          # 초당 처리율
            "last_rate_calc_time": {},       # 마지막 처리율 계산 시간
            "message_count_at_last_calc": {} # 마지막 계산 시 메시지 수
        }
        
        # 상태 변경 콜백 (ConnectionStateManager 기능 통합)
        self.callbacks = defaultdict(list)  # 거래소별 상태 변경 콜백 함수 리스트
        
        # 메시지 통계
        self.message_stats = defaultdict(lambda: {
            "total_received": 0,      # 총 메시지 수
            "snapshot_received": 0,   # 스냅샷 메시지 수
            "delta_received": 0,      # 델타 메시지 수
            "errors": 0,              # 오류 수
            "last_received": None     # 마지막 메시지 시간
        })
        
        # 이벤트 기록 (디버깅 및 모니터링용)
        self.events = defaultdict(lambda: deque(maxlen=METRICS_MAX_EVENTS))
        
        # 샘플링 관련
        self.last_rate_calculation = {}  # 거래소별 마지막 처리율 계산 시간
        self.rate_interval = METRICS_RATE_CALCULATION_INTERVAL  # 처리율 계산 간격 (초)
        
        # 경고 관련
        self.alerts = {}
        self.delay_threshold = METRICS_DELAY_THRESHOLD_MS  # 지연 경고 임계값 (밀리초)
        self.alert_cooldown = METRICS_ALERT_COOLDOWN  # 경고 쿨다운 (초)
        
        # 성능 관련
        self.max_history = METRICS_MAX_HISTORY  # 최대 히스토리 항목 수
        self.pong_timeout = 10   # 퐁 타임아웃 (초)
        self.health_threshold = METRICS_HEALTH_THRESHOLD  # 건강 임계값
        
        self.logger = get_unified_logger()

    def initialize_exchange(self, exchange_code: str) -> None:
        """
        거래소별 메트릭 초기화
        
        Args:
            exchange_code: 거래소 코드
        """
        # 소문자로 통일
        exchange_code = exchange_code.lower()
        current_time = time.time()
        
        if exchange_code not in self.metrics:
            self.metrics[exchange_code] = {
                "connection_state": "disconnected",  # 연결 상태
                "message_count": 0,                  # 메시지 수 
                "byte_count": 0,                     # 바이트 수
                "error_count": 0,                    # 에러 수
                "orderbook_count": 0,                # 오더북 업데이트 수
                "last_message_time": current_time,   # 마지막 메시지 시간
                "processing_times": [],              # 처리 시간 (밀리초)
                "avg_processing_time": 0,            # 평균 처리 시간 (밀리초)
                "last_count": 0,                     # 이전 메시지 수 (처리율 계산용)
                "last_count_time": current_time,     # 이전 메시지 시간 (처리율 계산용)
                "messages_per_second": 0,            # 초당 메시지 수 (처리율)
                "last_sequence": 0,                  # 마지막 시퀀스 번호
            }
            
            # 메시지 통계 초기화
            self.message_stats[exchange_code] = {
                "total_received": 0,
                "snapshot_received": 0,
                "delta_received": 0,
                "errors": 0,
                "last_received": None
            }
            
            # 기존 메트릭 시스템 호환 필드 초기화
            self.metrics["message_counts"][exchange_code] = 0
            self.metrics["orderbook_counts"][exchange_code] = 0
            self.metrics["error_counts"][exchange_code] = 0
            self.metrics["last_update_times"][exchange_code] = current_time
            self.metrics["start_times"][exchange_code] = current_time
            self.metrics["bytes_received"][exchange_code] = 0
            self.metrics["processing_rates"][exchange_code] = 0
            self.metrics["last_rate_calc_time"][exchange_code] = current_time
            self.metrics["message_count_at_last_calc"][exchange_code] = 0
            
            self.logger.debug(f"[Metrics] {exchange_code} 메트릭 초기화 완료")

    def update_metric(self, exchange_code: str, event_type: str, **kwargs):
        """메트릭 업데이트"""
        try:
            current_time = time.time()
            
            if exchange_code not in self.metrics["connection_state"]:
                self.initialize_exchange(exchange_code)
            
            if event_type == "connect" or event_type == "connected":
                self._handle_connect_event(exchange_code, current_time)
                
            elif event_type == "disconnect" or event_type == "disconnected":
                self._handle_disconnect_event(exchange_code, current_time)
                
            elif event_type == "message":
                self._handle_message_event(exchange_code, current_time)
                
            elif event_type == "error":
                self._handle_error_event(exchange_code, current_time)
                
            elif event_type == "orderbook":
                self._handle_orderbook_event(exchange_code, current_time)
                
            elif event_type == "processing_time":
                self._handle_processing_time_event(exchange_code, kwargs.get("time_ms", 0))
                
            elif event_type == "bytes":
                self._handle_bytes_event(exchange_code, kwargs.get("size", 0))
                
            # 처리율 계산 (일정 간격마다)
            self._calculate_processing_rate(exchange_code, current_time)
                
        except Exception as e:
            logger.error(f"[Metrics] 메트릭 업데이트 실패 | exchange={exchange_code}, event={event_type}, error={str(e)}", exc_info=True)

    def _handle_connect_event(self, exchange_code: str, current_time: float):
        """연결 이벤트 처리 (단순화)"""
        self.metrics["connection_state"][exchange_code] = CONNECTION_STATE["CONNECTED"]
        self.metrics["last_update_times"][exchange_code] = current_time

    def _handle_disconnect_event(self, exchange_code: str, current_time: float):
        """연결 해제 이벤트 처리 (단순화)"""
        self.metrics["connection_state"][exchange_code] = CONNECTION_STATE["DISCONNECTED"]
        self.metrics["last_update_times"][exchange_code] = current_time

    def _handle_message_event(self, exchange_code: str, current_time: float):
        """메시지 이벤트 처리"""
        # 메시지 카운트 증가
        if exchange_code not in self.metrics["message_counts"]:
            self.metrics["message_counts"][exchange_code] = 0
        self.metrics["message_counts"][exchange_code] += 1
        
        # 마지막 업데이트 시간 갱신
        self.metrics["last_update_times"][exchange_code] = current_time
        
        # 최근 메시지 타임스탬프 업데이트
        self.metrics["recent_messages"][exchange_code].append(current_time)
        
        # 메시지 수신 시 연결 상태 확인 (단순화)
        if self.metrics["connection_state"][exchange_code] != CONNECTION_STATE["CONNECTED"]:
            self._handle_connect_event(exchange_code, current_time)

    def _calculate_processing_rate(self, exchange_code: str, current_time: float):
        """처리율 계산 (일정 간격마다)"""
        # 기존 필드가 없으면 초기화
        if "last_rate_calc_time" not in self.metrics:
            self.metrics["last_rate_calc_time"] = {}
        if "message_count_at_last_calc" not in self.metrics:
            self.metrics["message_count_at_last_calc"] = {}
        
        last_calc_time = self.metrics["last_rate_calc_time"].get(exchange_code, 0)
        
        # 계산 간격이 지났는지 확인
        if current_time - last_calc_time >= self.rate_interval:
            # 이전 계산 이후 메시지 수 계산
            current_count = self.metrics["message_counts"].get(exchange_code, 0)
            last_count = self.metrics["message_count_at_last_calc"].get(exchange_code, 0)
            message_diff = current_count - last_count
            
            # 시간 간격
            time_diff = current_time - last_calc_time
            
            # 초당 처리율 계산
            if time_diff > 0:
                rate = message_diff / time_diff
                
                # 신규 메트릭과 기존 메트릭 모두 업데이트
                if "processing_rates" not in self.metrics:
                    self.metrics["processing_rates"] = {}
                self.metrics["processing_rates"][exchange_code] = rate
                
                if exchange_code in self.metrics:
                    self.metrics[exchange_code]["messages_per_second"] = rate
                
                # 디버깅 로그 추가
                self.logger.debug(
                    f"[Metrics] 처리율 계산 | exchange={exchange_code}, "
                    f"current_count={current_count}, last_count={last_count}, "
                    f"message_diff={message_diff}, time_diff={time_diff:.2f}초, "
                    f"rate={rate:.2f}건/초"
                )
            
            # 다음 계산을 위한 값 저장
            self.metrics["last_rate_calc_time"][exchange_code] = current_time
            self.metrics["message_count_at_last_calc"][exchange_code] = current_count

    def _handle_orderbook_event(self, exchange_code: str, current_time: float):
        """오더북 이벤트 처리"""
        if exchange_code not in self.metrics["orderbook_counts"]:
            self.metrics["orderbook_counts"][exchange_code] = 0
        self.metrics["orderbook_counts"][exchange_code] += 1
        self.metrics["last_update_times"][exchange_code] = current_time

    def _handle_processing_time_event(self, exchange_code: str, time_ms: float):
        """처리 시간 이벤트 처리"""
        self.metrics["processing_times"][exchange_code].append(time_ms)
        # 리스트가 너무 길어지지 않도록 최근 100개만 유지
        if len(self.metrics["processing_times"][exchange_code]) > 100:
            self.metrics["processing_times"][exchange_code] = self.metrics["processing_times"][exchange_code][-100:]

    def _handle_bytes_event(self, exchange_code: str, size: int):
        """바이트 수신 이벤트 처리"""
        if exchange_code not in self.metrics["bytes_received"]:
            self.metrics["bytes_received"][exchange_code] = 0
        self.metrics["bytes_received"][exchange_code] += size

    def _handle_error_event(self, exchange_code: str, current_time: float):
        """에러 이벤트 처리"""
        self.metrics["error_counts"][exchange_code] += 1
        self.metrics["last_update_times"][exchange_code] = current_time

    def _get_state_name(self, state_value: str) -> str:
        """상태 값에 대한 이름 반환 (단순화)"""
        if state_value == CONNECTION_STATE["CONNECTED"]:
            return "CONNECTED"
        return "DISCONNECTED"

    def get_connection_state(self, exchange_code: str) -> Dict:
        """거래소 연결 상태 정보 반환"""
        if exchange_code not in self.metrics["connection_state"]:
            self.initialize_exchange(exchange_code)
            
        state_value = self.metrics["connection_state"][exchange_code]
        state_name = self._get_state_name(state_value)
        
        return {
            "state": state_name,
            "emoji": STATUS_EMOJIS.get(state_name, "⚪")
        }

    def get_metrics(self) -> Dict:
        """모든 메트릭 데이터 반환"""
        result = {}
        current_time = time.time()
        
        for exchange_code in self.metrics["message_counts"].keys():
            # 최신 처리율 계산 보장
            self._calculate_processing_rate(exchange_code, current_time)
            
            # 기본 메트릭
            result[exchange_code] = {
                "connected": self.metrics["connection_state"][exchange_code] == CONNECTION_STATE["CONNECTED"],
                "connection_state": self._get_state_name(self.metrics["connection_state"][exchange_code]),
                "message_count": self.metrics["message_counts"].get(exchange_code, 0),
                "orderbook_count": self.metrics["orderbook_counts"].get(exchange_code, 0),
                "error_count": self.metrics["error_counts"].get(exchange_code, 0),
                "last_update_time": self.metrics["last_update_times"].get(exchange_code, 0),
                "uptime": current_time - self.metrics["start_times"].get(exchange_code, current_time),
                "processing_rate": self.metrics["processing_rates"].get(exchange_code, 0),
                "bytes_received": self.metrics["bytes_received"].get(exchange_code, 0),
                "avg_processing_time": self._calculate_avg_processing_time(exchange_code)
            }
            
        return result

    def _calculate_avg_processing_time(self, exchange_code: str) -> float:
        """평균 처리 시간 계산"""
        times = self.metrics["processing_times"].get(exchange_code, [])
        if not times:
            return 0.0
        return sum(times[-100:]) / min(100, len(times))

    def update_connection_state(self, exchange_code: str, state: str) -> None:
        """
        연결 상태 업데이트 및 콜백 호출
        
        Args:
            exchange_code: 거래소 코드
            state: 연결 상태 ("connected" 또는 "disconnected")
        """
        # 소문자로 통일
        exchange_code = exchange_code.lower()
        
        # 상태 정규화 (두 가지 상태만 허용)
        normalized_state = CONNECTION_STATE["CONNECTED"] if state in ["connected", "connect"] else CONNECTION_STATE["DISCONNECTED"]
        
        # 초기화되지 않은 경우 초기화
        if exchange_code not in self.metrics:
            self.initialize_exchange(exchange_code)
        
        # 이전 상태와 비교하여 변경 여부 확인
        prev_state = self.metrics["connection_state"].get(exchange_code, False)
        prev_normalized = CONNECTION_STATE["CONNECTED"] if prev_state == CONNECTION_STATE["CONNECTED"] else CONNECTION_STATE["DISCONNECTED"]
        
        # 상태가 변경된 경우에만 처리
        if prev_normalized != normalized_state:
            # 상태 업데이트
            self.metrics["connection_state"][exchange_code] = normalized_state
            
            # 거래소별 메트릭에도 상태 저장
            if exchange_code in self.metrics:
                self.metrics[exchange_code]["connection_state"] = normalized_state
            
            # 로깅
            status_text = "연결됨" if normalized_state == CONNECTION_STATE["CONNECTED"] else "연결 끊김"
            log_method = self.logger.info if normalized_state == CONNECTION_STATE["CONNECTED"] else self.logger.warning
            log_method(f"[{exchange_code}] {status_text}")
            
            # 이벤트 기록
            event_type = "connect" if normalized_state == CONNECTION_STATE["CONNECTED"] else "disconnect"
            self.events[exchange_code].append({
                "type": event_type,
                "timestamp": time.time()
            })
            
            # 콜백 호출
            for callback in self.callbacks.get(exchange_code, []):
                try:
                    callback(exchange_code, normalized_state)
                except Exception as e:
                    self.logger.error(f"[{exchange_code}] 상태 변경 콜백 호출 오류: {str(e)}")

    def record_message(self, exchange_code: str) -> None:
        """
        메시지 수신 기록
        
        Args:
            exchange_code: 거래소 코드
        """
        # 소문자로 통일
        exchange_code = exchange_code.lower()
        
        if exchange_code not in self.metrics:
            self.initialize_exchange(exchange_code)
            
        self.metrics[exchange_code]["message_count"] += 1
        self.metrics[exchange_code]["last_message_time"] = time.time()
        
        # 기존 메트릭 시스템과 호환성 유지
        if exchange_code not in self.metrics["message_counts"]:
            self.metrics["message_counts"][exchange_code] = 0
        self.metrics["message_counts"][exchange_code] += 1
        
        # 처리율 계산 (일정 간격마다)
        current_time = time.time()
        self._calculate_processing_rate(exchange_code, current_time)
        
    def record_bytes(self, exchange_code: str, byte_count: int) -> None:
        """
        바이트 수신량 기록
        
        Args:
            exchange_code: 거래소 코드
            byte_count: 바이트 수
        """
        # 소문자로 통일
        exchange_code = exchange_code.lower()
        
        if exchange_code not in self.metrics:
            self.initialize_exchange(exchange_code)
            
        self.metrics[exchange_code]["byte_count"] += byte_count

    def record_error(self, exchange_code: str) -> None:
        """
        에러 발생 기록
        
        Args:
            exchange_code: 거래소 코드
        """
        # 소문자로 통일
        exchange_code = exchange_code.lower()
        
        if exchange_code not in self.metrics:
            self.initialize_exchange(exchange_code)
            
        self.metrics[exchange_code]["error_count"] += 1
        self.logger.debug(f"[Metrics] 에러 발생 기록 | exchange={exchange_code}, total_errors={self.metrics[exchange_code]['error_count']}")

    def record_orderbook(self, exchange_code: str) -> None:
        """
        오더북 업데이트 기록
        
        Args:
            exchange_code: 거래소 코드
        """
        # 소문자로 통일
        exchange_code = exchange_code.lower()
        
        if exchange_code not in self.metrics:
            self.initialize_exchange(exchange_code)
            
        self.metrics[exchange_code]["orderbook_count"] += 1

    def record_processing_time(self, exchange_code: str, processing_time_ms: float) -> None:
        """
        메시지 처리 시간 기록
        
        Args:
            exchange_code: 거래소 코드
            processing_time_ms: 처리 시간 (밀리초)
        """
        # 소문자로 통일
        exchange_code = exchange_code.lower()
        
        if exchange_code not in self.metrics:
            self.initialize_exchange(exchange_code)
            
        # 처리 시간 리스트에 추가 (최대 100개)
        times = self.metrics[exchange_code]["processing_times"]
        times.append(processing_time_ms)
        if len(times) > 100:
            times.pop(0)
            
        # 평균 처리 시간 계산
        avg_time = sum(times) / len(times) if times else 0
        self.metrics[exchange_code]["avg_processing_time"] = avg_time 

    # ConnectionStateManager 기능 통합
    def is_connected(self, exchange_code: str) -> bool:
        """
        연결 여부 확인
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            bool: 연결 여부
        """
        exchange_code = exchange_code.lower()
        return self.metrics.get("connection_state", {}).get(exchange_code, False)
        
    def register_callback(self, exchange_code: str, callback) -> None:
        """
        상태 변경 콜백 등록
        
        Args:
            exchange_code: 거래소 코드
            callback: 콜백 함수 (exchange_code, status)를 인자로 받는 함수
        """
        exchange_code = exchange_code.lower()
        
        if callback not in self.callbacks[exchange_code]:
            self.callbacks[exchange_code].append(callback)
            self.logger.debug(f"[Metrics] {exchange_code} 콜백 등록 완료")
            
    # 통합 메시지 통계 관리 
    def update_message_stats(self, exchange_code: str, message_type: str) -> None:
        """
        메시지 통계 업데이트
        
        Args:
            exchange_code: 거래소 코드
            message_type: 메시지 타입 ("snapshot", "delta", "error" 등)
        """
        exchange_code = exchange_code.lower()
        current_time = time.time()
        
        if exchange_code not in self.message_stats:
            self.initialize_exchange(exchange_code)
            
        stats = self.message_stats[exchange_code]
        stats["total_received"] += 1
        stats["last_received"] = current_time
        
        if message_type == "snapshot":
            stats["snapshot_received"] += 1
        elif message_type == "delta":
            stats["delta_received"] += 1
        elif message_type == "error":
            stats["errors"] += 1
            
        # 메시지 카운트도 함께 업데이트 (기존 메트릭과 통합)
        if exchange_code in self.metrics:
            self.metrics[exchange_code]["message_count"] += 1
            self.metrics[exchange_code]["last_message_time"] = current_time
        
        # 기존 메트릭 시스템과 호환성 유지 (message_counts 필드)
        if "message_counts" not in self.metrics:
            self.metrics["message_counts"] = {}
        if exchange_code not in self.metrics["message_counts"]:
            self.metrics["message_counts"][exchange_code] = 0
        self.metrics["message_counts"][exchange_code] += 1
        
        # 처리율 계산 (일정 간격마다)
        self._calculate_processing_rate(exchange_code, current_time)

    def get_message_stats(self, exchange_code: str = None) -> Dict:
        """
        메시지 통계 조회
        
        Args:
            exchange_code: 거래소 코드 (None이면 전체 반환)
            
        Returns:
            Dict: 메시지 통계
        """
        if exchange_code:
            exchange_code = exchange_code.lower()
            return self.message_stats.get(exchange_code, {})
        else:
            return {code: stats for code, stats in self.message_stats.items()} 