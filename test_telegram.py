#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
텔레그램 알림 전송 테스트 스크립트
"""

import asyncio
import os
from dotenv import load_dotenv
from crosskimp.telegrambot.telegram_notification import (
    send_error_notification,
    send_system_status_notification,
    send_telegram_message
)
from crosskimp.telegrambot.bot_constants import (
    MessageType, 
    NOTIFICATION_BOT_TOKEN, 
    NOTIFICATION_CHAT_IDS
)

async def test_telegram():
    # 환경 변수 로드
    load_dotenv()
    
    # 환경 변수 확인
    print("환경 변수 확인:")
    print(f"TELEGRAM_BOT_TOKEN: {os.getenv('TELEGRAM_BOT_TOKEN')}")
    print(f"TELEGRAM_CHAT_ID: {os.getenv('TELEGRAM_CHAT_ID')}")
    
    # 봇 상수 확인
    print("\n봇 상수 확인:")
    print(f"NOTIFICATION_BOT_TOKEN: {NOTIFICATION_BOT_TOKEN}")
    print(f"NOTIFICATION_CHAT_IDS: {NOTIFICATION_CHAT_IDS}")
    
    # 테스트 메시지 전송
    print("\n테스트 메시지 전송 중...")
    result = await send_telegram_message(
        None, 
        MessageType.ERROR, 
        {
            "component": "테스트 스크립트",
            "message": "이것은 텔레그램 알림 테스트 메시지입니다."
        }
    )
    print(f"전송 결과: {result}")
    
    # 에러 메시지 전송 테스트
    print("\n에러 메시지 전송 중...")
    result = await send_error_notification("테스트 스크립트", "이것은 에러 테스트 메시지입니다.")
    print(f"전송 결과: {result}")
    
    # 시스템 상태 메시지 전송 테스트
    print("\n시스템 상태 메시지 전송 중...")
    result = await send_system_status_notification(25.5, 40.2, "1일 2시간 30분")
    print(f"전송 결과: {result}")

if __name__ == "__main__":
    asyncio.run(test_telegram()) 