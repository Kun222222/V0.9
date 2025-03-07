# file: core/websocket/websocket_metrics_manager.py

import time
from typing import Dict, Optional
from datetime import datetime
from utils.logging.logger import get_unified_logger, EXCHANGE_LOGGER_MAP

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
            "last_rate_update": {}     # 마지막 처리율 업데이트 시간
        }
        
        # 설정값
        self.rate_calculation_interval = 1.0  # 처리율 계산 주기 (초)
        self.delay_threshold_ms = 1000        # 지연 감지 임계값 (ms)
        self.alert_cooldown = 300             # 알림 쿨다운 (초)
        self.max_history = 3600               # 메트릭 보관 기간 (초)
        
        # 알림 상태
        self.last_delay_alert = {}
        
        self.logger = get_unified_logger()

    def initialize_exchange(self, exchange: str) -> None:
        """새로운 거래소 메트릭 초기화"""
        current_time = time.time()
        if exchange not in self.metrics["message_counts"]:
            self.metrics["message_counts"][exchange] = 0
            self.metrics["error_counts"][exchange] = 0
            self.metrics["last_message_times"][exchange] = 0
            self.metrics["connection_status"][exchange] = False
            self.metrics["start_times"][exchange] = current_time
            self.metrics["latencies"][exchange] = []
            self.metrics["last_ping_times"][exchange] = current_time * 1000
            self.metrics["last_metric_update"][exchange] = 0
            self.metrics["recent_messages"][exchange] = []
            self.metrics["message_rates"][exchange] = 0.0
            self.metrics["last_rate_update"][exchange] = current_time

    def update_metric(self, exchange: str, event_type: str) -> None:
        """메트릭 업데이트"""
        try:
            current_time = time.time()
            exchange_logger = EXCHANGE_LOGGER_MAP.get(exchange.lower(), self.logger)
            
            if event_type == "message":
                self._handle_message_event(exchange, current_time, exchange_logger)
            elif event_type == "error":
                self._handle_error_event(exchange, exchange_logger)
            elif event_type == "connect":
                self._handle_connect_event(exchange, current_time, exchange_logger)
            elif event_type == "disconnect":
                self._handle_disconnect_event(exchange, exchange_logger)
                
        except Exception as e:
            self.logger.error(
                f"[Metrics] 메트릭 업데이트 실패 | "
                f"exchange={exchange}, event={event_type}, error={str(e)}",
                exc_info=True
            )

    def _handle_message_event(self, exchange: str, current_time: float, logger) -> None:
        """메시지 이벤트 처리"""
        # 초기화 확인
        if exchange not in self.metrics["message_counts"]:
            self.initialize_exchange(exchange)
        
        # 메시지 카운트 업데이트
        self.metrics["message_counts"][exchange] += 1
        self.metrics["last_message_times"][exchange] = current_time * 1000
        
        # 실시간 처리율 계산
        recent_msgs = self.metrics["recent_messages"][exchange]
        recent_msgs.append(current_time)
        
        # 1초 이내의 메시지만 유지
        while recent_msgs and recent_msgs[0] < current_time - 1.0:
            recent_msgs.pop(0)
        
        # 초당 처리율 업데이트 - 매 메시지마다 계산
        self.metrics["message_rates"][exchange] = len(recent_msgs)
        self.metrics["last_rate_update"][exchange] = current_time
        
        # 디버그 로깅 추가
        if self.metrics["message_counts"][exchange] % 100 == 0:  # 100개 메시지마다 로깅
            logger.debug(
                f"[Metrics] {exchange} 처리율 업데이트 | "
                f"rate={self.metrics['message_rates'][exchange]:.1f} msg/s, "
                f"recent_msgs={len(recent_msgs)}, "
                f"total={self.metrics['message_counts'][exchange]:,}"
            )
        
        # 지연 체크
        self._check_latency(exchange, current_time, logger)

    def _handle_error_event(self, exchange: str, logger) -> None:
        """에러 이벤트 처리"""
        if exchange not in self.metrics["error_counts"]:
            self.metrics["error_counts"][exchange] = 0
        self.metrics["error_counts"][exchange] += 1
        logger.warning(
            f"[Metrics] {exchange} 에러 발생 | "
            f"총 에러={self.metrics['error_counts'][exchange]}"
        )

    def _handle_connect_event(self, exchange: str, current_time: float, logger) -> None:
        """연결 이벤트 처리"""
        prev_status = self.metrics["connection_status"].get(exchange, False)
        self.metrics["connection_status"][exchange] = True
        if not prev_status:
            logger.info(
                f"[Metrics] [{exchange}] 연결 성공 | "
                f"time={datetime.fromtimestamp(current_time).strftime('%H:%M:%S.%f')}"
            )

    def _handle_disconnect_event(self, exchange: str, logger) -> None:
        """연결 종료 이벤트 처리"""
        prev_status = self.metrics["connection_status"].get(exchange, True)
        self.metrics["connection_status"][exchange] = False
        if prev_status:
            logger.warning(
                f"[Metrics] {exchange} 연결 끊김 | "
                f"마지막 메시지={datetime.fromtimestamp(self.metrics['last_message_times'].get(exchange, 0)/1000).strftime('%H:%M:%S.%f')}"
            )

    def _check_latency(self, exchange: str, current_time: float, logger) -> None:
        """레이턴시 체크"""
        if exchange in self.metrics["last_ping_times"]:
            last_ping = self.metrics["last_ping_times"][exchange]
            latency = (current_time * 1000) - last_ping
            
            if latency > self.delay_threshold_ms:
                last_alert = self.last_delay_alert.get(exchange, 0)
                if current_time - last_alert > self.alert_cooldown:
                    logger.warning(
                        f"[{exchange}] 높은 네트워크 지연 감지 | "
                        f"지연={latency:.1f}ms"
                    )
                    self.last_delay_alert[exchange] = current_time

    def get_metrics(self) -> Dict:
        """현재 메트릭 상태 조회"""
        current_time = time.time()
        aggregated = {}
        
        for exchange in self.metrics["connection_status"].keys():
            connected = self.metrics["connection_status"].get(exchange, False)
            message_count = self.metrics["message_counts"].get(exchange, 0)
            error_count = self.metrics["error_counts"].get(exchange, 0)
            last_message_time = self.metrics["last_message_times"].get(exchange, current_time * 1000)
            
            # 실시간 처리율
            messages_per_second = self.metrics["message_rates"].get(exchange, 0.0)
            
            # 레이턴시
            latencies = self.metrics.get("latencies", {}).get(exchange, [])
            avg_latency = sum(latencies[-10:]) / len(latencies[-10:]) if latencies else 0
            
            # 디버그 로깅 추가
            logger = EXCHANGE_LOGGER_MAP.get(exchange.lower(), self.logger)
            logger.debug(
                f"[Metrics] {exchange} 상태 | "
                f"rate={messages_per_second:.1f} msg/s, "
                f"recent_msgs={len(self.metrics['recent_messages'].get(exchange, []))}, "
                f"total={message_count:,}"
            )
            
            aggregated[exchange] = {
                "connected": connected,
                "message_count": message_count,
                "messages_per_second": messages_per_second,
                "error_count": error_count,
                "last_message_time": last_message_time / 1000,
                "start_time": self.metrics["start_times"].get(exchange, current_time),
                "latency_ms": round(avg_latency, 2)
            }
        
        return aggregated

    def cleanup_old_metrics(self) -> None:
        """오래된 메트릭 정리"""
        current_time = time.time()
        cutoff_time = current_time - self.max_history
        
        for exchange in self.metrics["latencies"]:
            self.metrics["latencies"][exchange] = [
                lat for lat in self.metrics["latencies"][exchange]
                if lat > cutoff_time
            ] 