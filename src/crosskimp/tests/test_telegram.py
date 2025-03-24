#!/usr/bin/env python
"""
텔레그램 메시지 전송 테스트

이 스크립트는 텔레그램 봇이 올바르게 설정되었는지 확인하고 
메시지를 보낼 수 있는지 테스트합니다.
"""

import asyncio
import sys
import os
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('telegram_test')

# 시스템 경로에 프로젝트 루트 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from crosskimp.services.telegram_commander import get_telegram_commander
from crosskimp.common.events import get_event_bus

async def test_telegram_send():
    """텔레그램 메시지 전송 테스트"""
    try:
        logger.info("텔레그램 커맨더 초기화 중...")
        
        # 이벤트 버스 초기화
        event_bus = get_event_bus()
        await event_bus.start()
        
        # 텔레그램 커맨더 초기화
        telegram = get_telegram_commander()
        await telegram.initialize()
        
        if not telegram.is_initialized():
            logger.error("텔레그램 커맨더 초기화 실패")
            return False
            
        logger.info("텔레그램 설정 정보:")
        logger.info(f"- 토큰: {telegram.telegram_token[:4]}...{telegram.telegram_token[-4:] if telegram.telegram_token else '없음'}")
        logger.info(f"- 허용된 채팅 ID: {telegram.allowed_chat_ids}")
        
        # 테스트 메시지 전송
        logger.info("텔레그램 테스트 메시지 전송 중...")
        success = await telegram.send_message("🧪 텔레그램 테스트 메시지입니다. 시간: " + 
                                            asyncio.get_running_loop().time().__str__())
        
        if success:
            logger.info("✅ 메시지 전송 성공")
        else:
            logger.error("❌ 메시지 전송 실패")
            
        return success
    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {e}", exc_info=True)
        return False
    finally:
        # 종료 처리
        if 'telegram' in locals() and telegram.is_initialized():
            logger.info("텔레그램 커맨더 종료 중...")
            await telegram.shutdown()
        
        if 'event_bus' in locals():
            logger.info("이벤트 버스 정지 중...")
            await event_bus.stop()

async def main():
    """메인 함수"""
    result = await test_telegram_send()
    print(f"\n테스트 결과: {'성공' if result else '실패'}")

if __name__ == "__main__":
    asyncio.run(main()) 