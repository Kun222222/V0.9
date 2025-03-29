"""
메트릭 관리자

오더북 수집기의 메트릭 수집 및 처리 기능을 관리합니다.
"""

import time
from typing import Dict, Any, Optional

# 직접 로깅 모듈 가져오는 대신 공통 로거 사용
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent

from crosskimp.common.events.handler.metric.collectors import HybridMessageCounter, ErrorCounter

class ObcMetricManager:
    """오더북 수집기를 위한 메트릭 관리자"""
    
    def __init__(self):
        """초기화"""
        # 시작 시간
        self.start_time = None
        
        # 메트릭 추적
        self.message_counters = {}
        self.error_counters = {}
        self.reconnect_counts = {}
        self.symbol_timestamps = {}
        
        # 상태 추적 (기술적 목적)
        self.exchange_status = {}
        self.subscription_status = {}
        self.last_status_update = {}
        self.connection_start_time = {}
        
        # 시스템 상태
        self.is_starting = False
        self.is_fully_started = False
        
        # 컴포넌트 상태
        self.component_status = {}
        
        # 로거 설정 - 공통 로거 사용
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        
    def initialize(self):
        """메트릭 초기화"""
        self.start_time = time.time()
        
    # 메트릭 데이터 관리 메서드들
    # ======================================================
    
    def init_metrics_for_exchange(self, exchange):
        """특정 거래소에 대한 메트릭 트래커 초기화"""
        if exchange not in self.message_counters:
            self.message_counters[exchange] = HybridMessageCounter()
            
        if exchange not in self.error_counters:
            self.error_counters[exchange] = ErrorCounter()
            
        if exchange not in self.reconnect_counts:
            self.reconnect_counts[exchange] = 0
            
    def update_message_counter(self, exchange, count=1):
        """메시지 카운터 업데이트"""
        if exchange not in self.message_counters:
            self.init_metrics_for_exchange(exchange)
            
        self.message_counters[exchange].update(count)
        
    def update_error_counter(self, exchange, error_type, count=1):
        """오류 카운터 업데이트"""
        if exchange not in self.error_counters:
            self.init_metrics_for_exchange(exchange)
            
        self.error_counters[exchange].update(error_type, count)
        
    def increment_reconnect_counter(self, exchange):
        """재연결 카운터 증가"""
        if exchange not in self.reconnect_counts:
            self.reconnect_counts[exchange] = 0
            
        self.reconnect_counts[exchange] += 1
        
        # 로그 추가
        count = self.reconnect_counts[exchange]
        self.logger.info(f"🔄 [{exchange}] 재연결 카운터 증가: #{count}")
        
        # 메트릭 업데이트
        if count > 3:
            self.logger.warning(f"⚠️ [{exchange}] 재연결 빈도가 높습니다 (총 {count}회)")
            
        return count
        
    def update_symbol_timestamp(self, exchange, symbol, update_type="data"):
        """심볼별 타임스탬프 업데이트"""
        key = f"{exchange}:{symbol}"
        current_time = time.time()
        
        if update_type == "subscribe":
            if key not in self.symbol_timestamps:
                self.symbol_timestamps[key] = current_time
        else:  # data update
            self.symbol_timestamps[key] = current_time
    
    def set_system_state(self, is_starting=None, is_fully_started=None):
        """시스템 상태 설정 - ObCollector의 상태를 받아서 저장"""
        if is_starting is not None:
            self.is_starting = is_starting
            
        if is_fully_started is not None:
            self.is_fully_started = is_fully_started
            
    def update_component_status(self, component_name, status):
        """컴포넌트 상태 업데이트
        
        Args:
            component_name: 컴포넌트 이름 (예: aggregator, websocket)
            status: 상태 문자열 (예: running, stopped, error)
        """
        self.component_status[component_name] = {
            "status": status,
            "updated_at": time.time()
        }
        
    # 통합 메트릭 제공 메서드
    # ======================================================
    
    def get_metrics(self):
        """
        모든 메트릭 데이터를 수집하여 반환
        
        Returns:
            Dict: 메트릭 데이터 딕셔너리
        """
        return {
            "connection": self._get_connection_metrics(),
            "message": self._get_message_metrics(),
            "subscription": self._get_subscription_metrics(),
            "error": self._get_error_metrics(),
            "system": self._get_system_metrics()
        }
        
    def _get_connection_metrics(self):
        """연결 관련 메트릭 수집"""
        current_time = time.time()
        result = {}
        
        for exchange in self.exchange_status:
            connected = self.exchange_status.get(exchange, False)
            
            # 업타임 계산
            uptime = 0
            if connected and exchange in self.connection_start_time:
                uptime = current_time - self.connection_start_time[exchange]
            
            result[exchange] = {
                "connected": connected,
                "uptime": uptime,
                "reconnect_count": self.reconnect_counts.get(exchange, 0),
                "last_connected": self.connection_start_time.get(exchange, 0)
            }
            
        return result
        
    def _get_message_metrics(self):
        """메시지 관련 메트릭 수집"""
        result = {}
        
        for exchange, counter in self.message_counters.items():
            metrics = counter.get_metrics()
            result[exchange] = {
                "total_count": metrics["total_estimated"],
                "rate": metrics["rate"],
                # 최근 메시지 시간은 symbol_timestamps에서 최신값을 찾아서 사용
                "last_message_time": self._get_latest_symbol_update(exchange)
            }
            
        return result
        
    def _get_latest_symbol_update(self, exchange):
        """특정 거래소에 대한 가장 최근 심볼 업데이트 시간 조회"""
        latest_time = 0
        prefix = f"{exchange}:"
        
        for key, timestamp in self.symbol_timestamps.items():
            if key.startswith(prefix) and timestamp > latest_time:
                latest_time = timestamp
                
        return latest_time
        
    def _get_subscription_metrics(self):
        """구독 관련 메트릭 수집"""
        result = {}
        
        for exchange, status in self.subscription_status.items():
            symbols_data = {}
            prefix = f"{exchange}:"
            
            # 구독 중인 심볼들의 상태 정보 수집
            filtered_symbols = status.get("symbols", [])
            for symbol in filtered_symbols:
                key = f"{exchange}:{symbol}"
                symbols_data[symbol] = {
                    "active": status["active"],
                    "last_update": self.symbol_timestamps.get(key, 0),
                    "subscribe_time": self.symbol_timestamps.get(key, 0)
                }
                
            result[exchange] = {
                "active": status["active"],
                "total_symbols": status["symbol_count"],
                "symbols": symbols_data
            }
            
        return result
        
    def _get_error_metrics(self):
        """오류 관련 메트릭 수집"""
        result = {}
        
        for exchange, counter in self.error_counters.items():
            metrics = counter.get_metrics()
            
            # 오류율 계산 (메시지 수 대비)
            error_rate = 0
            if exchange in self.message_counters:
                msg_metrics = self.message_counters[exchange].get_metrics()
                total_messages = msg_metrics["total_estimated"]
                if total_messages > 0:
                    error_rate = metrics["total"] / total_messages
                    
            result[exchange] = {
                "total": metrics["total"],
                "types": metrics["types"],
                "last_error_time": metrics["last_error_time"],
                "rate": error_rate
            }
            
        return result
        
    def _get_system_metrics(self):
        """시스템 상태 메트릭 수집"""
        current_time = time.time()
        uptime = 0
        
        if self.start_time:
            uptime = current_time - self.start_time
            
        # 연결된 거래소 수 계산
        connected_exchanges = sum(1 for status in self.exchange_status.values() if status)
        total_exchanges = len(self.exchange_status)
        
        # 간소화된 상태 매핑
        status_map = {
            "stopped": "process/stopped",
            "starting": "process/starting",
            "running": "process/started"
        }
        
        # 상태 판단 - 두 값을 조합하여 결정
        status = "process/stopped"  # 기본값
        
        # is_fully_started/is_starting 기반으로 상태 결정
        if self.is_fully_started:
            status = "process/started"  # 모든 거래소 연결 완료됨
        elif self.is_starting:
            status = "process/starting"  # 프로그램 시작됨
        
        return {
            "status": status,
            "start_time": self.start_time or 0,
            "uptime": uptime,
            "connected_exchanges": connected_exchanges,
            "total_exchanges": total_exchanges,
            "components": self.component_status
        }
        
    # 상태 업데이트 메서드들
    # ======================================================
    
    def update_exchange_status(self, exchange, connected: bool):
        """거래소 연결 상태 업데이트"""
        self.exchange_status[exchange] = connected
        
        # 연결 시작 시간 기록
        if connected and exchange not in self.connection_start_time:
            self.connection_start_time[exchange] = time.time()
        elif not connected and exchange in self.connection_start_time:
            del self.connection_start_time[exchange]
            
    def update_subscription_status(self, exchange, active: bool, symbol_count: int, symbols=None):
        """구독 상태 업데이트"""
        if exchange not in self.subscription_status:
            self.subscription_status[exchange] = {"active": False, "symbol_count": 0, "symbols": []}
            
        self.subscription_status[exchange]["active"] = active
        self.subscription_status[exchange]["symbol_count"] = symbol_count
        
        # 심볼 목록도 저장 (제공된 경우)
        if symbols:
            self.subscription_status[exchange]["symbols"] = symbols
            
        # 상태 업데이트 시간 기록
        self.last_status_update[exchange] = time.time()
        
    def reset_message_counter(self, exchange: str):
        """
        특정 거래소의 메시지 카운터를 초기화
        재연결 이후 호출되어 메시지 카운팅을 리셋합니다.

        Args:
            exchange: 거래소 코드
        """
        # 거래소가 없으면 초기화
        if exchange not in self.message_counters:
            self.init_metrics_for_exchange(exchange)
        
        # 메시지 카운터 리셋
        # HybridMessageCounter 객체를 초기화하지 않고 내부 값만 리셋
        self.message_counters[exchange].reset_rate_counter()
        
        # 로그 남기기 - 인스턴스 로거 사용
        self.logger.info(f"⚠️ [{exchange}] 메시지 카운터가 초기화되었습니다 (재연결 후)")
        
    def _ensure_exchange_metrics(self, exchange: str):
        """
        거래소 메트릭 구조가 있는지 확인하고 없으면 생성
        """
        # 거래소가 없으면 초기화
        if exchange not in self.message_counters:
            self.init_metrics_for_exchange(exchange) 