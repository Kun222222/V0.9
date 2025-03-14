# file: telegrambot/notification/telegram_bot.py

"""
텔레그램 봇 알림 모듈

이 모듈은 텔레그램 봇을 통한 알림 기능을 제공합니다.
bot_constants.py에 정의된 함수들을 사용하여 텔레그램 메시지를 전송합니다.

최종수정: 2024.03
"""

from crosskimp.ob_collector.utils.config.config_loader import get_settings
from crosskimp.telegrambot.bot_constants import (
    # 메시지 타입 및 아이콘
    MessageType, MessageIcon,
    
    # 텔레그램 API 유틸리티
    setup_logger, send_telegram_message,
    
    # 편의 함수
    send_error, send_trade, send_profit, send_market_status, send_system_status
)

# 로거 초기화
logger = setup_logger()

# 테스트 코드
if __name__ == "__main__":
    async def test_telegram():
        settings = {
            "notifications": {
                "telegram": {
                    "enabled": True
                }
            }
        }
        
        # 테스트 메시지 전송
        await send_error(settings, "텔레그램 봇", "테스트 에러 메시지")
        await send_system_status(settings, 25.5, 40.2, "1일 2시간 30분")
        
    import asyncio
    asyncio.run(test_telegram())