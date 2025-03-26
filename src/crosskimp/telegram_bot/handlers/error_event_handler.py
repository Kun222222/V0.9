"""
오류 이벤트 핸들러 모듈

시스템에서 발생하는 오류 이벤트를 처리합니다.
"""

from typing import Dict, Any
from crosskimp.common.events.system_types import EventPaths

class ErrorEventHandler:
    """오류 이벤트 처리 클래스"""
    
    def __init__(self, notifier):
        """
        오류 이벤트 핸들러 초기화
        
        Args:
            notifier: 알림 전송을 위한 TelegramNotifier 인스턴스
        """
        self.notifier = notifier
        self.logger = notifier.logger
        self.event_type = EventPaths.SYSTEM_ERROR
        
        # 오류 상태 정보
        self._last_error_by_type = {}
        self._error_counts = {}
    
    async def handle(self, data: Dict[str, Any]):
        """
        오류 이벤트 처리
        
        Args:
            data: 오류 이벤트 데이터
        """
        try:
            error_type = data.get("error_type", "unknown")
            error_message = data.get("error_message", "알 수 없는 오류")
            component = data.get("component", "system")
            
            # 오류 그룹화 (동일 유형 오류 카운팅)
            if self.notifier.notification_settings["error_grouping"]:
                error_key = f"{component}_{error_type}"
                
                # 첫 발생이거나 마지막 오류와 다른 경우 즉시 알림
                if error_key not in self._last_error_by_type or self._last_error_by_type[error_key] != error_message:
                    self._last_error_by_type[error_key] = error_message
                    self._error_counts[error_key] = 1
                    
                    message = f"⚠️ 오류 발생 ({component}):\n{error_message}"
                    await self.notifier.send_notification(message, level=self.notifier.NotificationLevel.ERROR)
                else:
                    # 동일 오류 반복 - 카운트만 증가
                    self._error_counts[error_key] += 1
                    
                    # 카운트가 10, 100, 1000 등 10의 배수일 때만 알림
                    count = self._error_counts[error_key]
                    if count in [10, 100, 1000] or count % 1000 == 0:
                        message = f"⚠️ 반복 오류 ({component}):\n{error_message}\n\n동일 오류 {count}회 발생"
                        await self.notifier.send_notification(message, level=self.notifier.NotificationLevel.ERROR)
            else:
                # 그룹화 없이 모든 오류 알림
                message = f"⚠️ 오류 발생 ({component}):\n{error_message}"
                await self.notifier.send_notification(message, level=self.notifier.NotificationLevel.ERROR)
                
        except Exception as e:
            self.logger.error(f"오류 이벤트 처리 중 오류: {str(e)}") 