"""
시스템 메트릭 타입 정의

이 모듈은 시스템에서 사용하는 모든 메트릭 타입과 유틸리티 함수를 정의합니다.
메트릭은 시스템 성능, 상태 등을 모니터링하기 위한 측정값으로 사용됩니다.
"""

from enum import Enum, auto
from typing import Dict, Any, Optional, Union, List
from crosskimp.common.config.common_constants import SystemComponent
from datetime import datetime

#
# 메트릭 타입 정의
#

class MetricType(Enum):
    """
    시스템 메트릭 유형
    
    메트릭 수집 데이터의 종류를 정의합니다.
    """
    # 시스템 자원 메트릭
    CPU_USAGE = "cpu_usage"              # CPU 사용량 (%)
    MEMORY_USAGE = "memory_usage"        # 메모리 사용량 (%)
    DISK_USAGE = "disk_usage"            # 디스크 사용량 (%)
    NETWORK_TRAFFIC = "network_traffic"  # 네트워크 트래픽 (bytes/s)
    
    # 성능 메트릭
    RESPONSE_TIME = "response_time"      # 응답 시간 (ms)
    THROUGHPUT = "throughput"            # 처리량 (requests/s)
    LATENCY = "latency"                  # 지연 시간 (ms)
    
    # 애플리케이션 메트릭
    ORDERBOOK_UPDATE_RATE = "orderbook_update_rate"  # 오더북 업데이트 빈도 (updates/s)
    OPPORTUNITY_COUNT = "opportunity_count"          # 포착된 기회 수
    TRADE_COUNT = "trade_count"                      # 거래 수
    ERROR_COUNT = "error_count"                      # 오류 발생 수
    
    # 비즈니스 메트릭
    PROFIT_LOSS = "profit_loss"          # 수익/손실 (원)
    KIMCHI_PREMIUM = "kimchi_premium"    # 김프 수치 (%)


#
# 메트릭 데이터 클래스
#

class MetricData:
    """
    메트릭 데이터
    
    수집된 메트릭 데이터를 저장하는 클래스입니다.
    """
    
    def __init__(
        self,
        metric_type: MetricType,
        value: Union[float, int, str],
        component: SystemComponent,
        timestamp: Optional[datetime] = None,
        tags: Optional[Dict[str, str]] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """
        메트릭 데이터 초기화
        
        Args:
            metric_type: 메트릭 타입
            value: 메트릭 값
            component: 메트릭을 발생시킨 컴포넌트
            timestamp: 수집 시간 (기본값: 현재 시간)
            tags: 메트릭 태그 (필터링 및 그룹화용)
            details: 추가 정보
        """
        self.metric_type = metric_type
        self.value = value
        self.component = component
        self.timestamp = timestamp or datetime.now()
        self.tags = tags or {}
        self.details = details or {}
    
    def to_dict(self) -> Dict[str, Any]:
        """메트릭 데이터를 딕셔너리로 변환"""
        return {
            "metric_type": self.metric_type.value,
            "value": self.value,
            "component": self.component.value,
            "timestamp": self.timestamp.isoformat(),
            "tags": self.tags,
            "details": self.details
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MetricData':
        """딕셔너리에서 메트릭 데이터 생성"""
        return cls(
            metric_type=MetricType(data["metric_type"]),
            value=data["value"],
            component=SystemComponent(data["component"]),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            tags=data.get("tags", {}),
            details=data.get("details", {})
        )


#
# 메트릭 유틸리티 함수
#

def publish_metric(
    component: SystemComponent,
    metric_type: MetricType,
    value: Union[float, int, str],
    tags: Optional[Dict[str, str]] = None,
    details: Optional[Dict[str, Any]] = None
) -> None:
    """
    메트릭 발행 (이벤트 버스를 통해)
    
    Args:
        component: 메트릭을 발생시킨 컴포넌트
        metric_type: 메트릭 타입
        value: 메트릭 값
        tags: 메트릭 태그 (필터링 및 그룹화용)
        details: 추가 정보
    """
    # 실제 구현에서는 이벤트 버스를 통해 메트릭을 발행합니다.
    # 이 함수는 향후 구현될 예정입니다.
    from crosskimp.common.events.system_event_bus import get_event_bus
    from crosskimp.common.events.system_event_types import EventType, EventPriority
    
    event_bus = get_event_bus()
    metric_data = MetricData(
        metric_type=metric_type,
        value=value,
        component=component,
        tags=tags,
        details=details
    )
    
    # 메트릭 데이터를 이벤트로 발행
    event_bus.publish(
        event_type=EventType.STATUS_UPDATE,
        component=component,
        priority=EventPriority.LOW,  # 메트릭은 낮은 우선순위로 처리
        data=metric_data.to_dict(),
        topic=f"metric.{metric_type.value}"
    )


def calculate_rate(
    current_value: float,
    previous_value: float,
    time_elapsed_seconds: float
) -> float:
    """
    변화율 계산 (초당)
    
    Args:
        current_value: 현재 값
        previous_value: 이전 값
        time_elapsed_seconds: 경과 시간 (초)
        
    Returns:
        초당 변화율
    """
    if time_elapsed_seconds <= 0:
        return 0.0
    
    return (current_value - previous_value) / time_elapsed_seconds


def calculate_moving_average(values: List[float], window_size: int = 5) -> float:
    """
    이동 평균 계산
    
    Args:
        values: 값 목록
        window_size: 윈도우 크기
        
    Returns:
        이동 평균값
    """
    if not values:
        return 0.0
    
    window = values[-window_size:] if len(values) >= window_size else values
    return sum(window) / len(window) 