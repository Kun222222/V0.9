# file: orderbook/manager/websocket_metrics_manager.py

import time
from typing import Dict, Optional, List
from datetime import datetime
from collections import defaultdict

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.utils.config.constants import (
    EXCHANGE_NAMES_KR, STATUS_EMOJIS, WEBSOCKET_STATES, LOG_SYSTEM,
    METRICS_RATE_CALCULATION_INTERVAL, METRICS_DELAY_THRESHOLD_MS,
    METRICS_ALERT_COOLDOWN, METRICS_MAX_HISTORY, METRICS_MAX_EVENTS,
    METRICS_HEALTH_THRESHOLD
)

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class WebsocketMetricsManager:
    """웹소켓 메트릭 관리 클래스"""
    def __init__(self):
        self.metrics = {
            "message_counts": {},      # 전체 메시지 수
            "error_counts": {},        # 에러 수
            "last_message_times": {},  # 마지막 메시지 시간
            "connection_status": {},   # 연결 상태
            "start_times": {},         # 시작 시간
            "latencies": {},           # 레이턴시 기록
            "last_ping_times": {},     # 핑 전송 시간
            "last_metric_update": {},  # 마지막 메트릭 업데이트 시간
            "recent_messages": {},     # 최근 메시지 타임스탬프 (1초)
            "message_rates": {},       # 실시간 처리율
            "last_rate_update": {},    # 마지막 처리율 업데이트 시간
            "reconnect_counts": {},    # 재연결 횟수
            "orderbook_counts": {},    # 오더북 메시지 수
            "orderbook_rates": {},     # 오더북 처리율
            "processing_times": defaultdict(list),  # 처리 시간 기록
            "bytes_received": {},      # 수신 바이트
            "bytes_sent": {},          # 송신 바이트
            "connection_events": defaultdict(list),  # 연결 상태 변경 이벤트
            
            # 추가된 연결 통계
            "connection_attempts": {},  # 연결 시도 횟수
            "connection_successes": {}, # 연결 성공 횟수
            "connection_failures": {},  # 연결 실패 횟수
            "connection_durations": defaultdict(list),  # 연결 유지 시간 기록
            "last_connection_time": {}, # 마지막 연결 시작 시간
            "reconnect_intervals": defaultdict(list),  # 재연결 간격 기록
            "connection_patterns": defaultdict(list),   # 연결/해제 패턴 기록
            
            # 연결 상태 추적을 위한 새로운 메트릭
            "connection_state": {},     # CONNECTING, CONNECTED, DISCONNECTING, DISCONNECTED
            "last_ping_sent": {},      # 마지막 핑 전송 시간
            "last_pong_received": {},   # 마지막 퐁 수신 시간
            "ping_latencies": defaultdict(list),  # 핑-퐁 레이턴시 기록
            "connection_health": {},    # 연결 상태 점수 (0-100)
            "state_transitions": defaultdict(list)  # 상태 전이 기록
        }
        
        # 설정값
        self.rate_calculation_interval = METRICS_RATE_CALCULATION_INTERVAL  # 처리율 계산 주기 (초)
        self.delay_threshold_ms = METRICS_DELAY_THRESHOLD_MS                # 지연 감지 임계값 (ms)
        self.alert_cooldown = METRICS_ALERT_COOLDOWN                        # 알림 쿨다운 (초)
        self.max_history = METRICS_MAX_HISTORY                              # 메트릭 보관 기간 (초)
        self.max_events = METRICS_MAX_EVENTS                                # 최대 이벤트 저장 수
        self.ping_interval = 30.0      # 핑 전송 간격 (초)
        self.pong_timeout = 5.0        # 퐁 타임아웃 (초)
        self.health_threshold = METRICS_HEALTH_THRESHOLD  # 상태 점수 임계값
        
        # 알림 상태
        self.last_delay_alert = {}
        
        self.logger = get_unified_logger()

    def initialize_exchange(self, exchange: str) -> None:
        """새로운 거래소 메트릭 초기화"""
        current_time = time.time()
        self.metrics["message_counts"][exchange] = 0
        self.metrics["error_counts"][exchange] = 0
        self.metrics["last_message_times"][exchange] = current_time
        self.metrics["connection_status"][exchange] = False
        self.metrics["start_times"][exchange] = current_time
        self.metrics["latencies"][exchange] = []
        self.metrics["last_ping_times"][exchange] = current_time * 1000
        self.metrics["last_metric_update"][exchange] = current_time
        self.metrics["recent_messages"][exchange] = []
        self.metrics["message_rates"][exchange] = 0.0
        self.metrics["last_rate_update"][exchange] = current_time
        self.metrics["reconnect_counts"][exchange] = 0
        self.metrics["orderbook_counts"][exchange] = 0
        self.metrics["orderbook_rates"][exchange] = 0.0
        self.metrics["bytes_received"][exchange] = 0
        self.metrics["bytes_sent"][exchange] = 0
        
        # 추가된 연결 통계 초기화
        self.metrics["connection_attempts"][exchange] = 0
        self.metrics["connection_successes"][exchange] = 0
        self.metrics["connection_failures"][exchange] = 0
        self.metrics["last_connection_time"][exchange] = 0
        
        # 새로운 상태 추적 메트릭 초기화
        self.metrics["connection_state"][exchange] = WEBSOCKET_STATES['DISCONNECTED']
        self.metrics["last_ping_sent"][exchange] = 0
        self.metrics["last_pong_received"][exchange] = 0
        self.metrics["connection_health"][exchange] = 0

    def update_metric(self, exchange: str, event_type: str, **kwargs):
        """메트릭 업데이트"""
        try:
            current_time = time.time()
            
            if exchange not in self.metrics["connection_state"]:
                self.initialize_exchange(exchange)
            
            if event_type == "connect":
                self._handle_connect_event(exchange, current_time)
                
            elif event_type == "disconnect":
                self._handle_disconnect_event(exchange, current_time)
                
            elif event_type == "message":
                self._handle_message_event(exchange, current_time)
                
            elif event_type == "ping":
                self._handle_ping_event(exchange, current_time)
                
            elif event_type == "pong":
                self._handle_pong_event(exchange, current_time)
                
            elif event_type == "error":
                self._handle_error_event(exchange, current_time)
                
            # 연결 상태 점수 업데이트
            self._update_connection_health(exchange, current_time)
                
        except Exception as e:
            logger.error(f"[Metrics] 메트릭 업데이트 실패 | exchange={exchange}, event={event_type}, error={str(e)}", exc_info=True)

    def _handle_connect_event(self, exchange: str, current_time: float):
        """연결 이벤트 처리"""
        prev_state = self.metrics["connection_state"][exchange]
        if prev_state != WEBSOCKET_STATES['CONNECTED']:
            self.metrics["connection_state"][exchange] = WEBSOCKET_STATES['CONNECTED']
            self.metrics["connection_attempts"][exchange] += 1
            self.metrics["connection_successes"][exchange] += 1
            self.metrics["last_connection_time"][exchange] = current_time
            self.metrics["connection_health"][exchange] = 100
            
            # 상태 전이 기록
            self._record_state_transition(exchange, "CONNECTED", current_time)
            
            # 연결 패턴 기록
            self.metrics["connection_patterns"][exchange].append({
                "type": "connect",
                "time": current_time,
                "attempt": self.metrics["connection_attempts"][exchange]
            })

    def _handle_disconnect_event(self, exchange: str, current_time: float):
        """연결 해제 이벤트 처리"""
        prev_state = self.metrics["connection_state"][exchange]
        if prev_state == WEBSOCKET_STATES['CONNECTED']:
            self.metrics["connection_state"][exchange] = WEBSOCKET_STATES['DISCONNECTED']
            
            # 연결 지속 시간 계산 및 기록
            last_connect_time = self.metrics["last_connection_time"][exchange]
            if last_connect_time > 0:
                duration = current_time - last_connect_time
                self.metrics["connection_durations"][exchange].append(duration)
            
            # 재연결 간격 계산
            if len(self.metrics["connection_patterns"][exchange]) > 0:
                last_disconnect = next(
                    (p for p in reversed(self.metrics["connection_patterns"][exchange])
                     if p["type"] == "disconnect"), None)
                if last_disconnect:
                    interval = current_time - last_disconnect["time"]
                    self.metrics["reconnect_intervals"][exchange].append(interval)
            
            # 상태 전이 기록
            self._record_state_transition(exchange, "DISCONNECTED", current_time)
            
            # 연결 패턴 기록
            self.metrics["connection_patterns"][exchange].append({
                "type": "disconnect",
                "time": current_time,
                "duration": duration if 'duration' in locals() else 0
            })

    def _handle_message_event(self, exchange: str, current_time: float):
        """메시지 이벤트 처리"""
        # 메시지 카운트 증가
        if exchange not in self.metrics["message_counts"]:
            self.metrics["message_counts"][exchange] = 0
        self.metrics["message_counts"][exchange] += 1
        
        # 마지막 메시지 시간 업데이트
        self.metrics["last_message_times"][exchange] = current_time
        
        # 최근 메시지 타임스탬프 업데이트
        recent_msgs = self.metrics["recent_messages"][exchange]
        recent_msgs.append(current_time)
        
        # 1초 이내의 메시지만 유지
        while recent_msgs and recent_msgs[0] < current_time - 1.0:
            recent_msgs.pop(0)
        
        # 초당 처리율 계산
        self.metrics["message_rates"][exchange] = len(recent_msgs)
        
        # 메시지 수신 시 연결 상태 확인
        if self.metrics["connection_state"][exchange] != WEBSOCKET_STATES['CONNECTED']:
            self._handle_connect_event(exchange, current_time)
            
        # 연결 상태 점수 업데이트
        self._update_connection_health(exchange, current_time)

    def _handle_ping_event(self, exchange: str, current_time: float):
        """핑 이벤트 처리"""
        self.metrics["last_ping_sent"][exchange] = current_time

    def _handle_pong_event(self, exchange: str, current_time: float):
        """퐁 이벤트 처리"""
        last_ping = self.metrics["last_ping_sent"][exchange]
        if last_ping > 0:
            latency = (current_time - last_ping) * 1000  # ms로 변환
            self.metrics["ping_latencies"][exchange].append(latency)
            self.metrics["last_pong_received"][exchange] = current_time

    def _handle_error_event(self, exchange: str, current_time: float):
        """에러 이벤트 처리"""
        self.metrics["error_counts"][exchange] += 1
        self.metrics["connection_failures"][exchange] += 1

    def _update_connection_health(self, exchange: str, current_time: float):
        """연결 상태 점수 업데이트"""
        if self.metrics["connection_state"][exchange] != WEBSOCKET_STATES['CONNECTED']:
            self.metrics["connection_health"][exchange] = 0
            return
            
        health_score = 100
        
        # 핑-퐁 상태 체크
        last_pong = self.metrics["last_pong_received"][exchange]
        if current_time - last_pong > self.pong_timeout and last_pong > 0:
            health_score -= 30
        
        # 메시지 수신 상태 체크
        last_message = self.metrics["last_message_times"][exchange]
        if current_time - last_message > self.ping_interval * 2:
            health_score -= 30
        
        # 에러 발생 빈도 체크
        error_count = self.metrics["error_counts"][exchange]
        if error_count > 0:
            recent_errors = error_count / max(1, (current_time - self.metrics["start_times"][exchange]) / 3600)
            if recent_errors > 10:  # 시간당 10개 이상의 에러
                health_score -= 20
        
        # 핑 레이턴시 체크
        ping_latencies = self.metrics["ping_latencies"][exchange]
        if ping_latencies:
            avg_latency = sum(ping_latencies[-10:]) / min(10, len(ping_latencies))
            if avg_latency > self.delay_threshold_ms:
                health_score -= 20
        
        self.metrics["connection_health"][exchange] = max(0, health_score)

    def _record_state_transition(self, exchange: str, new_state: str, timestamp: float):
        """상태 전이 기록"""
        transitions = self.metrics["state_transitions"][exchange]
        transitions.append({
            "from": self._get_state_name(self.metrics["connection_state"][exchange]),
            "to": new_state,
            "time": timestamp
        })
        
        # 최대 이벤트 수 제한
        if len(transitions) > self.max_events:
            transitions.pop(0)

    def _get_state_name(self, state_value: int) -> str:
        """상태 값에 대한 이름 반환"""
        return next((k for k, v in WEBSOCKET_STATES.items() if v == state_value), "UNKNOWN")

    def get_connection_state(self, exchange: str) -> Dict:
        """거래소 연결 상태 정보 반환"""
        if exchange not in self.metrics["connection_state"]:
            self.initialize_exchange(exchange)
            
        state_value = self.metrics["connection_state"][exchange]
        state_name = self._get_state_name(state_value)
        health = self.metrics["connection_health"].get(exchange, 0)
        
        return {
            "state": state_name,
            "health": health,
            "healthy": health >= self.health_threshold,
            "emoji": STATUS_EMOJIS.get(state_name, "⚪")
        }

    def analyze_reconnect_patterns(self, exchange: str) -> str:
        """재연결 패턴 분석"""
        if exchange not in self.metrics["connection_patterns"]:
            return "데이터 없음"
            
        patterns = self.metrics["connection_patterns"][exchange]
        if not patterns:
            return "데이터 없음"
            
        # 연결 지속 시간 분석
        durations = self.metrics["connection_durations"][exchange]
        avg_duration = sum(durations) / max(1, len(durations)) if durations else 0
        
        # 재연결 간격 분석
        intervals = self.metrics["reconnect_intervals"][exchange]
        avg_interval = sum(intervals) / max(1, len(intervals)) if intervals else 0
        
        # 연결 성공률 계산
        attempts = self.metrics["connection_attempts"][exchange]
        successes = self.metrics["connection_successes"][exchange]
        success_rate = (successes / max(1, attempts)) * 100
        
        # 결과 포맷팅
        result = (
            f"연결 시도: {attempts}회, 성공률: {success_rate:.1f}%\n"
            f"평균 연결 지속 시간: {avg_duration:.1f}초\n"
            f"평균 재연결 간격: {avg_interval:.1f}초"
        )
        
        return result

    def get_connection_stats(self, exchange: str) -> Dict:
        """거래소 연결 통계 반환"""
        if exchange not in self.metrics["connection_state"]:
            self.initialize_exchange(exchange)
            
        return {
            "attempts": self.metrics["connection_attempts"].get(exchange, 0),
            "successes": self.metrics["connection_successes"].get(exchange, 0),
            "failures": self.metrics["connection_failures"].get(exchange, 0),
            "reconnects": self.metrics["reconnect_counts"].get(exchange, 0),
            "avg_duration": sum(self.metrics["connection_durations"][exchange]) / max(1, len(self.metrics["connection_durations"][exchange])) if exchange in self.metrics["connection_durations"] else 0,
            "state": self._get_state_name(self.metrics["connection_state"][exchange]),
            "health": self.metrics["connection_health"].get(exchange, 0)
        }

    def get_metrics(self) -> Dict:
        """모든 메트릭 데이터 반환"""
        result = {}
        current_time = time.time()
        
        for exchange in self.metrics["message_counts"].keys():
            # 기본 메트릭
            result[exchange] = {
                "connected": self.metrics["connection_state"][exchange] == WEBSOCKET_STATES['CONNECTED'],
                "connection_state": self._get_state_name(self.metrics["connection_state"][exchange]),
                "connection_health": self.metrics["connection_health"].get(exchange, 0),
                "message_count": self.metrics["message_counts"].get(exchange, 0),
                "error_count": self.metrics["error_counts"].get(exchange, 0),
                "reconnect_count": self.metrics["reconnect_counts"].get(exchange, 0),
                "last_message_time": self.metrics["last_message_times"].get(exchange, 0),
                "uptime": current_time - self.metrics["start_times"].get(exchange, current_time),
                "messages_per_second": self.metrics["message_rates"].get(exchange, 0),
                "orderbook_count": self.metrics["orderbook_counts"].get(exchange, 0),
                "orderbook_rate": self.metrics["orderbook_rates"].get(exchange, 0),
                "bytes_received": self.metrics["bytes_received"].get(exchange, 0),
                "bytes_sent": self.metrics["bytes_sent"].get(exchange, 0),
                "latency_ms": self._calculate_latency(exchange),
                "avg_processing_time": self._calculate_avg_processing_time(exchange),
                "connection_stats": self.get_connection_stats(exchange)
            }
            
        return result

    def _calculate_latency(self, exchange: str) -> float:
        """평균 핑 레이턴시 계산"""
        latencies = self.metrics["ping_latencies"].get(exchange, [])
        if not latencies:
            return 0.0
        return sum(latencies[-10:]) / min(10, len(latencies))

    def _calculate_avg_processing_time(self, exchange: str) -> float:
        """평균 처리 시간 계산"""
        times = self.metrics["processing_times"].get(exchange, [])
        if not times:
            return 0.0
        return sum(times[-100:]) / min(100, len(times))

    def cleanup_old_metrics(self) -> None:
        """오래된 메트릭 데이터 정리"""
        current_time = time.time()
        
        # 이벤트 기록 정리
        for exchange in self.metrics["state_transitions"]:
            transitions = self.metrics["state_transitions"][exchange]
            self.metrics["state_transitions"][exchange] = [
                t for t in transitions
                if current_time - t["time"] < self.max_history
            ]
            
        # 연결 패턴 정리
        for exchange in self.metrics["connection_patterns"]:
            patterns = self.metrics["connection_patterns"][exchange]
            self.metrics["connection_patterns"][exchange] = [
                p for p in patterns
                if current_time - p["time"] < self.max_history
            ]
            
        # 핑 레이턴시 정리
        for exchange in self.metrics["ping_latencies"]:
            # 최대 100개만 유지
            if len(self.metrics["ping_latencies"][exchange]) > 100:
                self.metrics["ping_latencies"][exchange] = self.metrics["ping_latencies"][exchange][-100:] 