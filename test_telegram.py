#!/usr/bin/env python3
"""
텔레그램 알림 기능 테스트 스크립트
"""

import asyncio
from crosskimp.telegrambot.telegram_notification import send_error
from crosskimp.ob_collector.utils.config.config_loader import initialize_config, get_settings

async def test_telegram():
    """텔레그램 알림 기능 테스트"""
    print("설정 초기화 중...")
    await initialize_config()
    
    print("텔레그램 메시지 전송 중...")
    result = await send_error(
        get_settings(), 
        '테스트', 
        '환경 변수에서 텔레그램 설정 로드 테스트'
    )
    
    if result:
        print("텔레그램 메시지 전송 성공!")
    else:
        print("텔레그램 메시지 전송 실패!")

if __name__ == "__main__":
    asyncio.run(test_telegram()) 