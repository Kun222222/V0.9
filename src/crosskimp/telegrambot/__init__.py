"""
텔레그램 봇 패키지 - 시스템 관리를 위한 텔레그램 인터페이스를 제공합니다.
"""

__version__ = "0.1.0"

from .bot_manager import TelegramBotManager
from .notification import NotificationService

# 봇 관리자 기본 인스턴스
_bot_manager_instance = None

def get_bot_manager():
    """텔레그램 봇 관리자 인스턴스를 반환합니다."""
    global _bot_manager_instance
    if _bot_manager_instance is None:
        from .bot_manager import TelegramBotManager
        _bot_manager_instance = TelegramBotManager()
    return _bot_manager_instance

def get_notification_service():
    """알림 서비스 인스턴스를 반환합니다."""
    bot_manager = get_bot_manager()
    return bot_manager.notification_service 