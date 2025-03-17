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
    """웹소켓 메트릭 관리 클래스 (싱글톤 패턴)"""
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
            
        WebsocketMetricsManager._instance = self
        
        self.metrics = {
            "message_counts": {},      # 전체 메시지 수
            "orderbook_counts": {},    # 오더북 메시지 수
            "error_counts": {},        # 에러 수
            "last_update_times": {},   # 마지막 업데이트 시간
            "start_times": {},         # 시작 시간
            "recent_messages": {},     # 최근 메시지 타임스탬프 (1초)
            "processing_rates": {},    # 초당 처리율
            "processing_times": defaultdict(list),  # 처리 시간 기록
            "bytes_received": {},      # 수신 바이트
            
            # 연결 상태 추적 (단순화: 연결됨/연결안됨)
            "connection_state": {},
            
            # 처리율 계산을 위한 추가 필드
            "last_rate_calc_time": {},  # 마지막 처리율 계산 시간
            "message_count_at_last_calc": {},  # 마지막 계산 시 메시지 수
        }
        
        # 설정값
        self.max_events = METRICS_MAX_EVENTS  # 최대 이벤트 저장 수
        self.delay_threshold_ms = METRICS_DELAY_THRESHOLD_MS  # 지연 임계값 (ms)
        self.rate_calc_interval = 5.0  # 처리율 계산 간격 (초)
        
        # 추가 설정값 (다른 클래스에서 사용)
        self.ping_interval = 30  # 핑 간격 (초)
        self.pong_timeout = 10   # 퐁 타임아웃 (초)
        self.health_threshold = METRICS_HEALTH_THRESHOLD  # 건강 임계값
        
        self.logger = get_unified_logger()

    def initialize_exchange(self, exchange: str) -> None:
        """새로운 거래소 메트릭 초기화"""
        current_time = time.time()
        self.metrics["message_counts"][exchange] = 0
        self.metrics["orderbook_counts"][exchange] = 0
        self.metrics["error_counts"][exchange] = 0
        self.metrics["last_update_times"][exchange] = current_time
        self.metrics["start_times"][exchange] = current_time
        self.metrics["recent_messages"][exchange] = deque(maxlen=1000)  # 최대 1000개 메시지 저장
        self.metrics["processing_rates"][exchange] = 0.0
        self.metrics["bytes_received"][exchange] = 0
        
        # 처리율 계산을 위한 필드 초기화
        self.metrics["last_rate_calc_time"][exchange] = current_time
        self.metrics["message_count_at_last_calc"][exchange] = 0
        
        # 연결 상태 초기화 (단순화: 연결안됨)
        self.metrics["connection_state"][exchange] = CONNECTION_STATE["DISCONNECTED"]

    def update_metric(self, exchange: str, event_type: str, **kwargs):
        """메트릭 업데이트"""
        try:
            current_time = time.time()
            
            if exchange not in self.metrics["connection_state"]:
                self.initialize_exchange(exchange)
            
            if event_type == "connect" or event_type == "connected":
                self._handle_connect_event(exchange, current_time)
                
            elif event_type == "disconnect" or event_type == "disconnected":
                self._handle_disconnect_event(exchange, current_time)
                
            elif event_type == "message":
                self._handle_message_event(exchange, current_time)
                
            elif event_type == "error":
                self._handle_error_event(exchange, current_time)
                
            elif event_type == "orderbook":
                self._handle_orderbook_event(exchange, current_time)
                
            elif event_type == "processing_time":
                self._handle_processing_time_event(exchange, kwargs.get("time_ms", 0))
                
            elif event_type == "bytes":
                self._handle_bytes_event(exchange, kwargs.get("size", 0))
                
            # 처리율 계산 (일정 간격마다)
            self._calculate_processing_rate(exchange, current_time)
                
        except Exception as e:
            logger.error(f"[Metrics] 메트릭 업데이트 실패 | exchange={exchange}, event={event_type}, error={str(e)}", exc_info=True)

    def _handle_connect_event(self, exchange: str, current_time: float):
        """연결 이벤트 처리 (단순화)"""
        self.metrics["connection_state"][exchange] = CONNECTION_STATE["CONNECTED"]
        self.metrics["last_update_times"][exchange] = current_time

    def _handle_disconnect_event(self, exchange: str, current_time: float):
        """연결 해제 이벤트 처리 (단순화)"""
        self.metrics["connection_state"][exchange] = CONNECTION_STATE["DISCONNECTED"]
        self.metrics["last_update_times"][exchange] = current_time

    def _handle_message_event(self, exchange: str, current_time: float):
        """메시지 이벤트 처리"""
        # 메시지 카운트 증가
        if exchange not in self.metrics["message_counts"]:
            self.metrics["message_counts"][exchange] = 0
        self.metrics["message_counts"][exchange] += 1
        
        # 마지막 업데이트 시간 갱신
        self.metrics["last_update_times"][exchange] = current_time
        
        # 최근 메시지 타임스탬프 업데이트
        self.metrics["recent_messages"][exchange].append(current_time)
        
        # 메시지 수신 시 연결 상태 확인 (단순화)
        if self.metrics["connection_state"][exchange] != CONNECTION_STATE["CONNECTED"]:
            self._handle_connect_event(exchange, current_time)

    def _calculate_processing_rate(self, exchange: str, current_time: float):
        """처리율 계산 (일정 간격마다)"""
        last_calc_time = self.metrics["last_rate_calc_time"].get(exchange, 0)
        
        # 계산 간격이 지났는지 확인
        if current_time - last_calc_time >= self.rate_calc_interval:
            # 이전 계산 이후 메시지 수 계산
            current_count = self.metrics["message_counts"].get(exchange, 0)
            last_count = self.metrics["message_count_at_last_calc"].get(exchange, 0)
            message_diff = current_count - last_count
            
            # 시간 간격
            time_diff = current_time - last_calc_time
            
            # 초당 처리율 계산
            if time_diff > 0:
                rate = message_diff / time_diff
                self.metrics["processing_rates"][exchange] = rate
                
                # 디버깅 로그 추가
                self.logger.debug(
                    f"[Metrics] 처리율 계산 | exchange={exchange}, "
                    f"current_count={current_count}, last_count={last_count}, "
                    f"message_diff={message_diff}, time_diff={time_diff:.2f}초, "
                    f"rate={rate:.2f}건/초"
                )
            
            # 다음 계산을 위한 값 저장
            self.metrics["last_rate_calc_time"][exchange] = current_time
            self.metrics["message_count_at_last_calc"][exchange] = current_count

    def _handle_orderbook_event(self, exchange: str, current_time: float):
        """오더북 이벤트 처리"""
        if exchange not in self.metrics["orderbook_counts"]:
            self.metrics["orderbook_counts"][exchange] = 0
        self.metrics["orderbook_counts"][exchange] += 1
        self.metrics["last_update_times"][exchange] = current_time

    def _handle_processing_time_event(self, exchange: str, time_ms: float):
        """처리 시간 이벤트 처리"""
        self.metrics["processing_times"][exchange].append(time_ms)
        # 리스트가 너무 길어지지 않도록 최근 100개만 유지
        if len(self.metrics["processing_times"][exchange]) > 100:
            self.metrics["processing_times"][exchange] = self.metrics["processing_times"][exchange][-100:]

    def _handle_bytes_event(self, exchange: str, size: int):
        """바이트 수신 이벤트 처리"""
        if exchange not in self.metrics["bytes_received"]:
            self.metrics["bytes_received"][exchange] = 0
        self.metrics["bytes_received"][exchange] += size

    def _handle_error_event(self, exchange: str, current_time: float):
        """에러 이벤트 처리"""
        self.metrics["error_counts"][exchange] += 1
        self.metrics["last_update_times"][exchange] = current_time

    def _get_state_name(self, state_value: str) -> str:
        """상태 값에 대한 이름 반환 (단순화)"""
        if state_value == CONNECTION_STATE["CONNECTED"]:
            return "CONNECTED"
        return "DISCONNECTED"

    def get_connection_state(self, exchange: str) -> Dict:
        """거래소 연결 상태 정보 반환"""
        if exchange not in self.metrics["connection_state"]:
            self.initialize_exchange(exchange)
            
        state_value = self.metrics["connection_state"][exchange]
        state_name = self._get_state_name(state_value)
        
        return {
            "state": state_name,
            "emoji": STATUS_EMOJIS.get(state_name, "⚪")
        }

    def get_metrics(self) -> Dict:
        """모든 메트릭 데이터 반환"""
        result = {}
        current_time = time.time()
        
        for exchange in self.metrics["message_counts"].keys():
            # 최신 처리율 계산 보장
            self._calculate_processing_rate(exchange, current_time)
            
            # 기본 메트릭
            result[exchange] = {
                "connected": self.metrics["connection_state"][exchange] == CONNECTION_STATE["CONNECTED"],
                "connection_state": self._get_state_name(self.metrics["connection_state"][exchange]),
                "message_count": self.metrics["message_counts"].get(exchange, 0),
                "orderbook_count": self.metrics["orderbook_counts"].get(exchange, 0),
                "error_count": self.metrics["error_counts"].get(exchange, 0),
                "last_update_time": self.metrics["last_update_times"].get(exchange, 0),
                "uptime": current_time - self.metrics["start_times"].get(exchange, current_time),
                "processing_rate": self.metrics["processing_rates"].get(exchange, 0),
                "bytes_received": self.metrics["bytes_received"].get(exchange, 0),
                "avg_processing_time": self._calculate_avg_processing_time(exchange)
            }
            
        return result

    def _calculate_avg_processing_time(self, exchange: str) -> float:
        """평균 처리 시간 계산"""
        times = self.metrics["processing_times"].get(exchange, [])
        if not times:
            return 0.0
        return sum(times[-100:]) / min(100, len(times))

    def update_connection_state(self, exchange: str, state: str):
        """
        연결 상태 직접 업데이트 (단순화)
        
        Args:
            exchange: 거래소 코드
            state: 연결 상태 (connect, connected, disconnect, disconnected 등)
        """
        try:
            current_time = time.time()
            
            if exchange not in self.metrics["connection_state"]:
                self.initialize_exchange(exchange)
            
            # 상태 문자열에 따라 처리 (대소문자 구분 없이)
            state_lower = state.lower()
            
            # 단순화: 연결 관련 상태는 모두 "connected"로, 연결 해제 관련 상태는 모두 "disconnected"로 처리
            if "connect" in state_lower and "dis" not in state_lower:
                self._handle_connect_event(exchange, current_time)
            elif "disconnect" in state_lower or "disconnected" in state_lower:
                self._handle_disconnect_event(exchange, current_time)
            else:
                # 알 수 없는 상태는 로그만 남기고 무시
                logger.warning(f"[Metrics] 알 수 없는 연결 상태: {state}")
                
        except Exception as e:
            logger.error(f"[Metrics] 연결 상태 업데이트 실패 | exchange={exchange}, state={state}, error={str(e)}", exc_info=True)

    def record_message(self, exchange: str):
        """메시지 수신 기록"""
        self.update_metric(exchange, "message")
        
    def record_orderbook(self, exchange: str):
        """오더북 업데이트 기록"""
        self.update_metric(exchange, "orderbook")
        
    def record_error(self, exchange: str):
        """에러 발생 기록"""
        self.update_metric(exchange, "error")
        
    def record_processing_time(self, exchange: str, time_ms: float):
        """처리 시간 기록"""
        self.update_metric(exchange, "processing_time", time_ms=time_ms)
        
    def record_bytes(self, exchange: str, size: int):
        """바이트 수신 기록"""
        self.update_metric(exchange, "bytes", size=size) 