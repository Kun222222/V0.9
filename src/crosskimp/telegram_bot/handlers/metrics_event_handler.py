"""
성능 메트릭 이벤트 핸들러 모듈

시스템 성능 메트릭 관련 이벤트를 처리합니다.
"""

from typing import Dict, Any, List
from crosskimp.common.events.system_types import EventPaths

class MetricsEventHandler:
    """성능 메트릭 이벤트 처리 클래스"""
    
    def __init__(self, notifier):
        """
        메트릭 이벤트 핸들러 초기화
        
        Args:
            notifier: 알림 전송을 위한 TelegramNotifier 인스턴스
        """
        self.notifier = notifier
        self.logger = notifier.logger
        
        # 구독할 이벤트 타입 목록
        self.event_types = [
            EventPaths.PERFORMANCE_CPU,
            EventPaths.PERFORMANCE_MEMORY,
            EventPaths.PERFORMANCE_LATENCY,
            EventPaths.PERFORMANCE_QUEUE,  # THROUGHPUT 대신 QUEUE 사용
        ]
    
    async def handle(self, data: Dict[str, Any]):
        """
        성능 메트릭 이벤트 처리
        
        Args:
            data: 성능 메트릭 이벤트 데이터
        """
        try:
            metrics_type = data.get("metrics_type", "system")
            metrics_value = data.get("value", 0)
            component = data.get("component", "system")
            
            # 주요 지표에 대해서만 알림 (임계값 초과 시)
            if metrics_type == "cpu" and metrics_value > 80:
                message = f"⚠️ CPU 사용률 경고 ({component}): {metrics_value}%"
                await self.notifier.send_notification(message, level=self.notifier.NotificationLevel.WARNING)
                
            elif metrics_type == "memory" and metrics_value > 85:
                message = f"⚠️ 메모리 사용률 경고 ({component}): {metrics_value}%"
                await self.notifier.send_notification(message, level=self.notifier.NotificationLevel.WARNING)
                
            elif metrics_type == "latency" and metrics_value > 5000:
                message = f"⚠️ 지연 시간 경고 ({component}): {metrics_value}ms"
                await self.notifier.send_notification(message, level=self.notifier.NotificationLevel.WARNING)
                
        except Exception as e:
            self.logger.error(f"성능 메트릭 이벤트 처리 중 오류: {str(e)}") 