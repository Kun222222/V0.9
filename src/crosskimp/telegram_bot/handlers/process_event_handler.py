"""
프로세스 이벤트 핸들러 모듈

시스템 프로세스의 상태 변화 이벤트를 처리합니다.
"""

from typing import Dict, Any
from crosskimp.common.events.system_types import EventPaths

class ProcessEventHandler:
    """프로세스 상태 이벤트 처리 클래스"""
    
    def __init__(self, notifier):
        """
        프로세스 이벤트 핸들러 초기화
        
        Args:
            notifier: 알림 전송을 위한 TelegramNotifier 인스턴스
        """
        self.notifier = notifier
        self.logger = notifier.logger
        self.event_type = EventPaths.PROCESS_STATUS
    
    async def handle(self, data: Dict[str, Any]):
        """
        프로세스 이벤트 처리
        
        Args:
            data: 프로세스 이벤트 데이터
        """
        try:
            process_name = data.get("process_name")
            status = data.get("status")
            event_type = data.get("event_type")
            
            if not process_name or not status:
                return
                
            # 상태에 따른 알림 메시지 생성
            if status == "running":
                message = f"✅ '{process_name}' 프로세스가 시작되었습니다."
                level = self.notifier.NotificationLevel.INFO
            elif status == "stopped":
                message = f"🛑 '{process_name}' 프로세스가 중지되었습니다."
                level = self.notifier.NotificationLevel.INFO
            elif status == "error":
                error_msg = data.get("error_message", "알 수 없는 오류")
                message = f"❌ '{process_name}' 프로세스 오류: {error_msg}"
                level = self.notifier.NotificationLevel.ERROR
            else:
                return
                
            # 알림 전송
            await self.notifier.send_notification(message, level=level)
                
        except Exception as e:
            self.logger.error(f"프로세스 이벤트 처리 중 오류: {str(e)}") 