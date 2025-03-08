# file: orderbook/manager/websocket_metrics_manager.py

import time
from typing import Dict, Optional, List
from datetime import datetime
from collections import defaultdict

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, EXCHANGE_LOGGER_MAP
from crosskimp.ob_collector.config.constants import EXCHANGE_NAMES_KR, STATUS_EMOJIS

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
        
        # 상태 전이 정의
        self.STATES = {
            'CONNECTING': 0,
            'CONNECTED': 1,
            'DISCONNECTING': 2,
            'DISCONNECTED': 3
        }
        
        # 설정값
        self.rate_calculation_interval = 1.0  # 처리율 계산 주기 (초)
        self.delay_threshold_ms = 1000        # 지연 감지 임계값 (ms)
        self.alert_cooldown = 300             # 알림 쿨다운 (초)
        self.max_history = 3600               # 메트릭 보관 기간 (초)
        self.max_events = 100                 # 최대 이벤트 저장 수
        self.ping_interval = 30.0      # 핑 전송 간격 (초)
        self.pong_timeout = 5.0        # 퐁 타임아웃 (초)
        self.health_threshold = 70     # 상태 점수 임계값
        
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
        self.metrics["connection_state"][exchange] = self.STATES['DISCONNECTED']
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
        if prev_state != self.STATES['CONNECTED']:
            self.metrics["connection_state"][exchange] = self.STATES['CONNECTED']
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
        if prev_state == self.STATES['CONNECTED']:
            self.metrics["connection_state"][exchange] = self.STATES['DISCONNECTED']
            
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
        self.metrics["message_counts"][exchange] += 1
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
        if self.metrics["connection_state"][exchange] != self.STATES['CONNECTED']:
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
        if self.metrics["connection_state"][exchange] != self.STATES['CONNECTED']:
            self.metrics["connection_health"][exchange] = 0
            return
            
        health_score = 100
        
        # 핑-퐁 상태 체크
        last_pong = self.metrics["last_pong_received"][exchange]
        if current_time - last_pong > self.pong_timeout and last_pong > 0:
            health_score -= 30
        
        # 메시지 수신 상태 체크
        last_msg = self.metrics["last_message_times"][exchange]
        if current_time - last_msg > 5.0:  # 5초 이상 메시지 없음
            health_score -= 20
        
        # 에러 발생 상태 체크
        recent_errors = sum(1 for t in self.metrics["connection_patterns"][exchange][-5:]
                          if t["type"] == "error")
        health_score -= recent_errors * 10
        
        # 최종 점수 업데이트
        self.metrics["connection_health"][exchange] = max(0, min(100, health_score))
        
        # 상태 점수가 임계값 미만이면 연결 해제 처리
        if health_score < self.health_threshold and self.metrics["connection_state"][exchange] == self.STATES['CONNECTED']:
            self._handle_disconnect_event(exchange, current_time)

    def _record_state_transition(self, exchange: str, new_state: str, timestamp: float):
        """상태 전이 기록"""
        self.metrics["state_transitions"][exchange].append({
            "from": self.STATES[self._get_state_name(self.metrics["connection_state"][exchange])],
            "to": self.STATES[new_state],
            "time": timestamp
        })
        
        # 최대 100개까지만 유지
        if len(self.metrics["state_transitions"][exchange]) > 100:
            self.metrics["state_transitions"][exchange].pop(0)

    def _get_state_name(self, state_value: int) -> str:
        """상태 값에 대한 이름 반환"""
        return next(name for name, value in self.STATES.items() if value == state_value)

    def get_connection_state(self, exchange: str) -> Dict:
        """현재 연결 상태 정보 조회"""
        current_time = time.time()
        state = self.metrics["connection_state"].get(exchange, self.STATES['DISCONNECTED'])
        health = self.metrics["connection_health"].get(exchange, 0)
        
        return {
            "state": self._get_state_name(state),
            "health": health,
            "last_transition": self.metrics["state_transitions"][exchange][-1] if self.metrics["state_transitions"][exchange] else None,
            "uptime": current_time - self.metrics["last_connection_time"].get(exchange, current_time) if state == self.STATES['CONNECTED'] else 0,
            "ping_latency": sum(self.metrics["ping_latencies"][exchange][-5:]) / 5 if self.metrics["ping_latencies"][exchange] else 0
        }

    def analyze_reconnect_patterns(self, exchange: str) -> str:
        """재연결 패턴 분석"""
        patterns = self.metrics["connection_patterns"][exchange][-10:]  # 최근 10개 이벤트만
        intervals = self.metrics["reconnect_intervals"][exchange][-5:]  # 최근 5개 간격만
        
        if not patterns:
            return "패턴 없음"
            
        result = []
        
        # 평균 연결 유지 시간
        durations = [p.get("duration", 0) for p in patterns if p["type"] == "disconnect"]
        if durations:
            avg_duration = sum(durations) / len(durations)
            result.append(f"평균 유지={avg_duration:.1f}초")
            
        # 평균 재연결 간격
        if intervals:
            avg_interval = sum(intervals) / len(intervals)
            result.append(f"재연결 간격={avg_interval:.1f}초")
            
        # 연결 성공률
        success_rate = (self.metrics["connection_successes"][exchange] / 
                       self.metrics["connection_attempts"][exchange] * 100)
        result.append(f"성공률={success_rate:.1f}%")
        
        return " | ".join(result)

    def get_connection_stats(self, exchange: str) -> Dict:
        """연결 통계 조회"""
        return {
            "attempts": self.metrics["connection_attempts"].get(exchange, 0),
            "successes": self.metrics["connection_successes"].get(exchange, 0),
            "failures": self.metrics["connection_failures"].get(exchange, 0),
            "avg_duration": (sum(self.metrics["connection_durations"][exchange]) / 
                           len(self.metrics["connection_durations"][exchange])
                           if self.metrics["connection_durations"][exchange] else 0),
            "reconnect_count": self.metrics["reconnect_counts"].get(exchange, 0),
            "avg_reconnect_interval": (sum(self.metrics["reconnect_intervals"][exchange]) / 
                                     len(self.metrics["reconnect_intervals"][exchange])
                                     if self.metrics["reconnect_intervals"][exchange] else 0),
            "recent_patterns": self.metrics["connection_patterns"][exchange][-5:]  # 최근 5개 이벤트
        }

    def get_metrics(self) -> Dict:
        """현재 메트릭 상태 조회"""
        result = {}
        current_time = time.time()
        
        for exchange in self.metrics["connection_state"].keys():
            stats = self.get_connection_stats(exchange)
            state = self.metrics["connection_state"][exchange]
            
            # 연결 상태 확인 (CONNECTED 상태이고 health score가 임계값 이상인 경우만 연결된 것으로 간주)
            is_connected = (state == self.STATES['CONNECTED'] and 
                          self.metrics["connection_health"][exchange] >= self.health_threshold)
            
            result[exchange] = {
                "connected": is_connected,
                "message_count": self.metrics["message_counts"].get(exchange, 0),
                "messages_per_second": self.metrics["message_rates"].get(exchange, 0),
                "error_count": self.metrics["error_counts"].get(exchange, 0),
                "reconnect_count": self.metrics["reconnect_counts"].get(exchange, 0),
                "last_message_time": self.metrics["last_message_times"].get(exchange, 0),
                "start_time": self.metrics["start_times"].get(exchange, current_time),
                "latency_ms": self._calculate_latency(exchange),
                "orderbook_count": self.metrics["orderbook_counts"].get(exchange, 0),
                "orderbook_per_second": self.metrics["orderbook_rates"].get(exchange, 0),
                "avg_processing_time": self._calculate_avg_processing_time(exchange),
                "bytes_received": self.metrics["bytes_received"].get(exchange, 0),
                "bytes_sent": self.metrics["bytes_sent"].get(exchange, 0),
                "state": self._get_state_name(state),
                "health": self.metrics["connection_health"].get(exchange, 0),
                
                # 추가된 연결 통계
                "connection_attempts": stats["attempts"],
                "connection_successes": stats["successes"],
                "connection_failures": stats["failures"],
                "avg_connection_duration": stats["avg_duration"],
                "avg_reconnect_interval": stats["avg_reconnect_interval"],
                "connection_success_rate": (stats["successes"] / stats["attempts"] * 100 
                                          if stats["attempts"] > 0 else 0)
            }
        
        return result

    def _calculate_latency(self, exchange: str) -> float:
        """평균 레이턴시 계산"""
        latencies = self.metrics["latencies"].get(exchange, [])
        if not latencies:
            return 0.0
        return sum(latencies[-10:]) / len(latencies[-10:])

    def _calculate_avg_processing_time(self, exchange: str) -> float:
        """평균 처리 시간 계산"""
        times = self.metrics["processing_times"].get(exchange, [])
        if not times:
            return 0.0
        return sum(times) / len(times)

    def cleanup_old_metrics(self) -> None:
        """오래된 메트릭 정리"""
        current_time = time.time()
        cutoff_time = current_time - self.max_history
        
        for exchange in self.metrics["latencies"]:
            self.metrics["latencies"][exchange] = [
                lat for lat in self.metrics["latencies"][exchange]
                if lat > cutoff_time
            ]
            
            # 처리 시간 기록 정리
            self.metrics["processing_times"][exchange] = self.metrics["processing_times"][exchange][-100:]
            
            # 최근 메시지 정리
            self.metrics["recent_messages"][exchange] = [
                t for t in self.metrics["recent_messages"][exchange]
                if t > current_time - 1.0
            ] 