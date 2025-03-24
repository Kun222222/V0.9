#!/usr/bin/env python3
"""
텔레그램 프로세스 시작 디버깅 스크립트

텔레그램 프로세스의 시작 과정을 단계별로 검증합니다.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime

# 모듈 경로 추가 (src 디렉토리를 추가)
src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.append(src_dir)
print(f"추가된 경로: {src_dir}")
print(f"sys.path: {sys.path}")

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger("telegram_debug")

async def debug_telegram_start():
    """텔레그램 프로세스 시작 디버깅"""
    logger.info("=== 텔레그램 프로세스 시작 디버깅 시작 ===")
    
    try:
        # 1. 설정 확인
        from crosskimp.common.config.app_config import get_config
        config = get_config()
        
        telegram_token = config.get_env("telegram.bot_token")
        chat_ids = config.get_env("telegram.chat_id")
        
        logger.info(f"텔레그램 봇 토큰: {'설정됨' if telegram_token else '설정되지 않음'}")
        logger.info(f"텔레그램 채팅 ID: {chat_ids}")
        
        if not telegram_token:
            logger.error("텔레그램 봇 토큰이 설정되지 않았습니다.")
            return
        
        # 2. 텔레그램 커맨더 초기화 과정 확인
        logger.info("텔레그램 커맨더 초기화 시작...")
        from crosskimp.services.telegram_commander import TelegramCommander
        
        telegram = TelegramCommander()
        await telegram.initialize()
        logger.info(f"텔레그램 커맨더 초기화 결과: {telegram.initialized}")
        
        if not telegram.initialized:
            logger.error("텔레그램 커맨더 초기화 실패")
            return
        
        # 3. 텔레그램 봇 정보 확인
        logger.info("텔레그램 봇 정보 확인...")
        if telegram.bot:
            bot_info = await telegram.bot.get_me()
            logger.info(f"봇 정보: {bot_info.username} (ID: {bot_info.id})")
        else:
            logger.error("텔레그램 봇 객체가 초기화되지 않았습니다.")
        
        # 4. 테스트 메시지 전송
        logger.info("테스트 메시지 전송 시도...")
        success = await telegram.send_message("🧪 텔레그램 봇 테스트 메시지 - " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        logger.info(f"테스트 메시지 전송 결과: {'성공' if success else '실패'}")
        
        logger.info("=== 텔레그램 프로세스 시작 디버깅 완료 ===")
        
    except Exception as e:
        logger.error(f"텔레그램 프로세스 디버깅 중 오류 발생: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(debug_telegram_start()) 