"""
구독 메트릭 관리 모듈

이 모듈은 거래소 구독 관련 메트릭을 수집, 가공하여 중앙 이벤트 버스로 발행합니다.
0.5초 간격으로 가공된 메트릭 데이터를 발행하여 UI 및 모니터링 시스템에서 사용할 수 있게 합니다.
"""

import time
import asyncio
from typing import Dict, Any, List, Optional
import datetime
from datetime import timezone
from dataclasses import dataclass, field

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.system_event_bus import get_event_bus
from crosskimp.common.events.system_event_types import EventType, EventPriority
from crosskimp.common.config.common_constants import SystemComponent

# 거래소별 표시 설정
EXCHANGE_DISPLAY_INFO = {
    "binance": {
        "display_name": "Binance",
        "color": "#F0B90B",  # 바이낸스 노란색
        "icon": "binance_logo.png"
    },
    "binance_futures": {
        "display_name": "Binance Futures",
        "color": "#F0B90B",  # 바이낸스 노란색
        "icon": "binance_futures_logo.png"
    },
    "upbit": {
        "display_name": "Upbit",
        "color": "#093687",  # 업비트 파란색
        "icon": "upbit_logo.png"
    },
    "bybit": {
        "display_name": "Bybit",
        "color": "#FFCC00",  # 바이비트 노란색
        "icon": "bybit_logo.png"
    },
    "bybit_futures": {
        "display_name": "Bybit Futures",
        "color": "#FFCC00",  # 바이비트 노란색
        "icon": "bybit_futures_logo.png"
    },
    "bithumb": {
        "display_name": "Bithumb",
        "color": "#D7232B",  # 빗썸 빨간색
        "icon": "bithumb_logo.png"
    }
}

# 상태 임계값 설정
STATUS_THRESHOLDS = {
    "message_rate": {
        "warning": 10,
        "critical": 2
    },
    "error_count": {
        "warning": 5,
        "critical": 10
    }
}

# 로거 설정
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

@dataclass
class ExchangeMetrics:
    """거래소별 메트릭 데이터 클래스"""
    exchange_code: str
    
    # 기본 메트릭
    message_count: int = 0
    message_rate: float = 0.0
    error_count: int = 0
    subscription_count: int = 0
    last_message_time: float = 0.0
    
    # 고급 메트릭
    processing_time: float = 0.0
    snapshot_count: int = 0
    delta_count: int = 0
    
    # 상태 추적
    status: str = "initialized"
    status_since: float = field(default_factory=time.time)
    start_time: float = field(default_factory=time.time)
    
    # 변화 추적
    prev_message_count: int = 0
    prev_message_rate: float = 0.0
    

class SubscriptionMetricsManager:
    """구독 메트릭 관리자"""
    
    def __init__(self):
        """초기화"""
        self.logger = logger
        self.event_bus = get_event_bus()
        
        # 거래소별 메트릭 데이터
        self.exchange_metrics: Dict[str, ExchangeMetrics] = {}
        
        # 태스크 관리
        self.publish_task = None
        self.running = False
        
        # 발행 주기 (초)
        self.publish_interval = 0.5
        
        self.logger.info("구독 메트릭 관리자 초기화 완료")

    def start(self):
        """메트릭 발행 시작"""
        if self.running:
            return
            
        self.running = True
        self.publish_task = asyncio.create_task(self._publish_metrics_loop())
        self.logger.info("메트릭 발행 태스크 시작")
    
    async def stop(self):
        """메트릭 발행 중지"""
        if not self.running:
            return
            
        self.running = False
        
        if self.publish_task and not self.publish_task.done():
            self.publish_task.cancel()
            try:
                await self.publish_task
            except asyncio.CancelledError:
                pass
                
        self.logger.info("메트릭 발행 태스크 중지")
    
    def update_metric(self, exchange_code: str, metric_name: str, value: Any, **kwargs):
        """
        메트릭 업데이트
        
        Args:
            exchange_code: 거래소 코드
            metric_name: 메트릭 이름
            value: 메트릭 값
            **kwargs: 추가 컨텍스트 정보
        """
        # 거래소 메트릭 객체 가져오기 (없으면 생성)
        if exchange_code not in self.exchange_metrics:
            self.exchange_metrics[exchange_code] = ExchangeMetrics(exchange_code=exchange_code)
        
        # 메트릭 객체 업데이트
        metrics = self.exchange_metrics[exchange_code]
        
        # 기본 메트릭 업데이트
        if metric_name == "message_count":
            # 증분 업데이트인 경우
            if kwargs.get("op") == "increment":
                metrics.message_count += int(value)
            else:
                metrics.message_count = int(value)
        elif metric_name == "message_rate":
            metrics.prev_message_rate = metrics.message_rate
            metrics.message_rate = float(value)
        elif metric_name == "error_count":
            # 증분 업데이트인 경우
            if kwargs.get("op") == "increment":
                metrics.error_count += int(value)
            else:
                metrics.error_count = int(value)
        elif metric_name == "subscription_count":
            # 증분 업데이트인 경우
            if kwargs.get("op") == "increment":
                metrics.subscription_count += int(value)
            elif kwargs.get("op") == "decrement":
                metrics.subscription_count = max(0, metrics.subscription_count - int(value))
            else:
                metrics.subscription_count = int(value)
        elif metric_name == "last_message_time":
            metrics.last_message_time = float(value)
        elif metric_name == "processing_time":
            metrics.processing_time = float(value)
        elif metric_name == "snapshot_count":
            # 증분 업데이트인 경우
            if kwargs.get("op") == "increment":
                metrics.snapshot_count += int(value)
            else:
                metrics.snapshot_count = int(value)
        elif metric_name == "delta_count":
            # 증분 업데이트인 경우
            if kwargs.get("op") == "increment":
                metrics.delta_count += int(value)
            else:
                metrics.delta_count = int(value)
                
        # 상태 업데이트
        self._update_exchange_status(exchange_code)
    
    def _update_exchange_status(self, exchange_code: str):
        """
        거래소 상태 업데이트
        
        메트릭 값을 기반으로 거래소 상태를 결정합니다.
        """
        if exchange_code not in self.exchange_metrics:
            return
            
        metrics = self.exchange_metrics[exchange_code]
        
        # 이전 상태 저장
        previous_status = metrics.status
        
        # 상태 결정 로직
        new_status = "healthy"
        
        # 상태 결정 근거 (디버깅용)
        status_reasons = []
        
        # 메시지 속도 확인
        if metrics.message_rate <= STATUS_THRESHOLDS["message_rate"]["critical"]:
            new_status = "critical"
            status_reasons.append(f"메시지 속도 너무 낮음: {metrics.message_rate:.1f}/s")
        elif metrics.message_rate <= STATUS_THRESHOLDS["message_rate"]["warning"]:
            new_status = "warning"
            status_reasons.append(f"메시지 속도 낮음: {metrics.message_rate:.1f}/s")
            
        # 오류 수 확인
        if metrics.error_count >= STATUS_THRESHOLDS["error_count"]["critical"]:
            new_status = "critical"
            status_reasons.append(f"오류 많음: {metrics.error_count}개")
        elif metrics.error_count >= STATUS_THRESHOLDS["error_count"]["warning"] and new_status != "critical":
            new_status = "warning"
            status_reasons.append(f"오류 있음: {metrics.error_count}개")
            
        # 마지막 메시지 시간 확인 (10초 이상 메시지 없으면 경고)
        if metrics.last_message_time > 0:
            elapsed = time.time() - metrics.last_message_time
            if elapsed > 30:
                new_status = "critical"
                status_reasons.append(f"메시지 없음: {elapsed:.1f}초")
            elif elapsed > 10 and new_status != "critical":
                new_status = "warning"
                status_reasons.append(f"메시지 지연: {elapsed:.1f}초")
        else:
            # 시작 후 메시지가 없는 경우
            elapsed = time.time() - metrics.start_time
            new_status = "critical"
            status_reasons.append(f"메시지 없음 (초기화 후): {elapsed:.1f}초")
        
        # 상태가 변경된 경우 시간 업데이트
        if new_status != previous_status:
            metrics.status = new_status
            metrics.status_since = time.time()
            
            # 상태 변경 로깅 (이유 포함)
            reason_text = ', '.join(status_reasons) if status_reasons else "알 수 없는 이유"
            self.logger.info(f"거래소 {exchange_code} 상태 변경: {previous_status} → {new_status} (이유: {reason_text})")
        elif previous_status == "critical" and new_status == "critical" and status_reasons:
            # 계속 critical 상태인 경우도 주기적으로 이유 로깅
            if int(time.time()) % 10 == 0:  # 10초마다 로깅
                reason_text = ', '.join(status_reasons)
                self.logger.debug(f"거래소 {exchange_code} 상태 유지: {new_status} (이유: {reason_text})")
    
    def _format_timestamp(self, timestamp: float) -> str:
        """타임스탬프를 가독성 있는 형식으로 변환"""
        dt = datetime.datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    
    def _format_duration(self, seconds: float) -> str:
        """초 단위 시간을 가독성 있는 형식으로 변환"""
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)
        
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    def _get_exchange_display_info(self, exchange_code: str) -> Dict[str, str]:
        """거래소 표시 정보 가져오기"""
        # 기본 코드로 시도
        if exchange_code in EXCHANGE_DISPLAY_INFO:
            return EXCHANGE_DISPLAY_INFO[exchange_code]
        
        # 'futures' 문자열이 있는지 확인
        if "futures" in exchange_code:
            base_code = exchange_code.replace("_futures", "")
            if f"{base_code}_futures" in EXCHANGE_DISPLAY_INFO:
                return EXCHANGE_DISPLAY_INFO[f"{base_code}_futures"]
        
        # 기본값 반환
        return {
            "display_name": exchange_code.capitalize(),
            "color": "#888888",
            "icon": "exchange_logo.png"
        }
    
    def get_formatted_metrics(self, exchange_code: str) -> Dict[str, Any]:
        """
        가공된 메트릭 데이터 가져오기
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            Dict: 가공된 메트릭 데이터
        """
        if exchange_code not in self.exchange_metrics:
            return None
            
        metrics = self.exchange_metrics[exchange_code]
        display_info = self._get_exchange_display_info(exchange_code)
        
        # 메트릭 계산
        uptime = time.time() - metrics.start_time
        uptime_formatted = self._format_duration(uptime)
        
        # 메시지 변화율 계산 (%)
        message_rate_change = 0
        if metrics.prev_message_rate > 0:
            message_rate_change = ((metrics.message_rate - metrics.prev_message_rate) / metrics.prev_message_rate) * 100
        
        # 최종 데이터 구성
        current_time = time.time()
        
        formatted_data = {
            "exchange_code": exchange_code,
            "display_name": display_info["display_name"],
            "timestamp": current_time,
            "formatted_time": self._format_timestamp(current_time),
            
            # 주요 지표 요약
            "metrics_summary": {
                "message_count": metrics.message_count,
                "message_rate": round(metrics.message_rate, 1),
                "message_rate_change": round(message_rate_change, 1),
                "error_count": metrics.error_count,
                "subscription_count": metrics.subscription_count,
                "uptime": uptime_formatted,
                "processing_time": round(metrics.processing_time, 2) if metrics.processing_time > 0 else 0,
                "snapshot_count": metrics.snapshot_count,
                "delta_count": metrics.delta_count
            },
            
            # 상태 정보
            "status": metrics.status,
            "status_since": self._format_timestamp(metrics.status_since),
            "status_duration": self._format_duration(current_time - metrics.status_since),
            
            # 시각화 정보
            "color": display_info["color"],
            "icon": display_info["icon"],
            
            # 단위 및 형식 정보
            "units": {
                "message_rate": "msgs/sec",
                "processing_time": "ms"
            },
            
            # 표시 형식 정보
            "formatted_values": {
                "message_rate": f"{round(metrics.message_rate, 1)} msgs/sec",
                "message_rate_change": f"{'+' if message_rate_change >= 0 else ''}{round(message_rate_change, 1)}%",
                "processing_time": f"{round(metrics.processing_time, 2)} ms" if metrics.processing_time > 0 else "N/A"
            }
        }
        
        # 마지막 메시지 시간 정보 추가 (있는 경우에만)
        if metrics.last_message_time > 0:
            last_message_ago = current_time - metrics.last_message_time
            formatted_data["last_message"] = {
                "timestamp": metrics.last_message_time,
                "formatted_time": self._format_timestamp(metrics.last_message_time),
                "seconds_ago": round(last_message_ago, 1),
                "formatted_ago": self._format_duration(last_message_ago)
            }
        
        return formatted_data
    
    async def _publish_metrics_loop(self):
        """메트릭 발행 루프"""
        try:
            while self.running:
                await self._publish_all_metrics()
                await asyncio.sleep(self.publish_interval)
        except asyncio.CancelledError:
            self.logger.info("메트릭 발행 루프 취소됨")
        except Exception as e:
            self.logger.error(f"메트릭 발행 중 오류: {str(e)}")
    
    async def _publish_all_metrics(self):
        """모든 거래소 메트릭 발행"""
        try:
            for exchange_code in self.exchange_metrics:
                formatted_data = self.get_formatted_metrics(exchange_code)
                if formatted_data:
                    await self.event_bus.publish(
                        EventType.OB_METRIC_UPDATE,
                        formatted_data,
                        priority=EventPriority.NORMAL
                    )
        except Exception as e:
            self.logger.error(f"메트릭 발행 중 오류: {str(e)}")

# 싱글톤 인스턴스
_metrics_manager_instance = None

def get_metrics_manager() -> SubscriptionMetricsManager:
    """
    구독 메트릭 관리자 싱글톤 인스턴스 가져오기
    
    Returns:
        SubscriptionMetricsManager: 메트릭 관리자 인스턴스
    """
    global _metrics_manager_instance
    if _metrics_manager_instance is None:
        _metrics_manager_instance = SubscriptionMetricsManager()
    return _metrics_manager_instance 