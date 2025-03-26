"""
간단한 텔레그램 알림 테스트 프로그램

복잡한 초기화 없이 메시지를 바로 보냅니다.
"""

import asyncio
from telegram import Bot

# 환경 설정에서 직접 값을 가져옴 (실제로는 dotenv 등을 사용하는 것이 좋음)
TELEGRAM_BOT_TOKEN = "6421983135:AAHOtWMe5gwX1OyEBjoZEiVqNa8VWg-APgc"
TELEGRAM_CHAT_ID = 1213855908

async def send_telegram_message(message: str):
    """텔레그램으로 메시지 전송"""
    try:
        # 초기화 과정 없이 바로 Bot 인스턴스 생성
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        
        # 메시지 전송
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
        print(f"✅ 메시지가 성공적으로 전송되었습니다: '{message}'")
        return True
    except Exception as e:
        print(f"❌ 메시지 전송 실패: {str(e)}")
        return False

async def main():
    """메인 함수"""
    print("📱 간단한 텔레그램 알림 테스트 시작")
    
    # 테스트 메시지 전송
    await send_telegram_message("⚠️ 테스트 알림: 초기화 없이 직접 메시지 전송 테스트입니다.")
    
    print("📱 테스트 완료")

if __name__ == "__main__":
    # 비동기 메인 함수 실행
    asyncio.run(main()) 