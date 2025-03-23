"""
텔레그램 관련 이벤트 타입 정의

이 모듈은 텔레그램 봇과 관련된 이벤트 타입을 정의합니다.
"""

from typing import Dict


class TelegramEventTypes:
    """
    텔레그램 관련 이벤트 타입

    텔레그램 봇의 상태, 알림, 명령어 처리와 관련된
    모든 이벤트 타입을 상수로 정의합니다.
    """
    # 텔레그램 봇 관련 이벤트
    BOT_STARTUP = "telegram:bot_startup"
    BOT_SHUTDOWN = "telegram:bot_shutdown"
    
    # 알림 관련 이벤트
    NOTIFICATION_SENT = "telegram:notification_sent"
    COMMAND_RECEIVED = "telegram:command_received"
    COMMAND_EXECUTED = "telegram:command_executed"
    
    # 시스템 모니터링 관련 이벤트
    RESOURCE_ALERT = "telegram:resource_alert"
    DAILY_REPORT = "telegram:daily_report"
    
    # 오류 관련 이벤트
    BOT_ERROR = "telegram:bot_error"


# 이전 이벤트 타입에서 새 이벤트 타입으로의 매핑
EVENT_TYPE_MAPPING: Dict[str, str] = {
    # 일반적인 시스템 이벤트를 텔레그램 도메인에 맵핑
    "SYSTEM_STARTUP": TelegramEventTypes.BOT_STARTUP,
    "SYSTEM_SHUTDOWN": TelegramEventTypes.BOT_SHUTDOWN,
    "ERROR_EVENT": TelegramEventTypes.BOT_ERROR
} 