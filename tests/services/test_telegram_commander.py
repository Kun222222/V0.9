#!/usr/bin/env python3
"""
텔레그램 커맨더 테스트 스크립트

텔레그램 커맨더 모듈만 단독으로 테스트합니다.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime
import traceback

# 모듈 경로 추가
src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.append(src_dir)
print(f"추가된 경로: {src_dir}")

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger("test_telegram_commander")

async def test_telegram_commander():
    """텔레그램 커맨더 테스트"""
    logger.info("=== 텔레그램 커맨더 테스트 시작 ===")
    
    try:
        # 텔레그램 커맨더 직접 초기화
        from crosskimp.services.telegram_commander import TelegramCommander
        
        telegram = TelegramCommander()
        
        # 1. 초기화 시도
        logger.info("1. 텔레그램 커맨더 초기화 중...")
        await telegram.initialize()
        logger.info(f"   - 초기화 결과: {telegram.initialized}")
        
        # 2. 봇 객체 확인
        if telegram.bot:
            logger.info("2. 텔레그램 봇 객체 확인...")
            try:
                bot_info = await telegram.bot.get_me()
                logger.info(f"   - 봇 정보: {bot_info.username} (ID: {bot_info.id})")
            except Exception as e:
                logger.error(f"   - 봇 정보 조회 실패: {str(e)}")
                logger.error(traceback.format_exc())
        else:
            logger.error("   - 텔레그램 봇 객체가 없습니다.")
        
        # 3. 테스트 메시지 전송 시도
        if telegram.initialized and telegram.bot:
            logger.info("3. 테스트 메시지 전송 시도...")
            try:
                test_message = f"🧪 텔레그램 커맨더 테스트 메시지: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                success = await telegram.send_message(test_message)
                logger.info(f"   - 메시지 전송 결과: {'성공' if success else '실패'}")
            except Exception as e:
                logger.error(f"   - 메시지 전송 중 오류: {str(e)}")
                logger.error(traceback.format_exc())
        
        # 4. 잠시 대기 (메시지 처리 시간)
        logger.info("4. 잠시 대기 (3초)...")
        await asyncio.sleep(3)
        
        # 5. 텔레그램 커맨더 종료
        logger.info("5. 텔레그램 커맨더 종료 중...")
        await telegram.shutdown()
        logger.info(f"   - 종료 후 초기화 상태: {telegram.initialized}")
        
        logger.info("=== 텔레그램 커맨더 테스트 완료 ===")
        
    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(test_telegram_commander()) 