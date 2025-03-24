#!/usr/bin/env python
"""
텔레그램 메시지 발송 직접 테스트 스크립트

이 스크립트는 텔레그램 봇 설정을 로드하고 메시지를 직접 보내는 기능만을 테스트합니다.
"""

import os
import sys
import asyncio
import logging
import traceback
from datetime import datetime

# 프로젝트 루트 디렉토리를 시스템 경로에 추가
src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(src_dir)
print(f"추가된 경로: {src_dir}")

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger('telegram_test')

async def send_direct_message():
    """텔레그램 메시지 직접 전송 테스트"""
    logger.info("==== 텔레그램 메시지 직접 전송 테스트 ====")
    
    try:
        # 1. 설정 로드
        logger.info("1. 텔레그램 설정 로드 중...")
        from crosskimp.common.config.app_config import get_config
        
        config = get_config()
        
        bot_token = config.get_env("telegram.bot_token")
        chat_ids_str = config.get_env("telegram.chat_id")
        
        if not bot_token:
            logger.error("텔레그램 봇 토큰이 설정되지 않았습니다.")
            return
            
        if not chat_ids_str:
            logger.error("텔레그램 채팅 ID가 설정되지 않았습니다.")
            return
            
        logger.info(f"설정 로드 성공 - 봇 토큰: ...{bot_token[-5:]}, 채팅 ID: {chat_ids_str}")
        
        # 2. 채팅 ID 처리
        logger.info("2. 채팅 ID 처리 중...")
        chat_ids = []
        
        try:
            if isinstance(chat_ids_str, str):
                if ',' in chat_ids_str:
                    chat_ids = [chat_id.strip() for chat_id in chat_ids_str.split(',')]
                else:
                    chat_ids = [chat_ids_str.strip()]
                    
                logger.info(f"처리된 채팅 ID: {chat_ids} (타입: {type(chat_ids)})")
                
                # 문자열에서 정수로 변환
                int_chat_ids = []
                for chat_id in chat_ids:
                    try:
                        int_chat_id = int(chat_id)
                        int_chat_ids.append(int_chat_id)
                        logger.info(f"채팅 ID 변환 성공: {chat_id} -> {int_chat_id}")
                    except ValueError as e:
                        logger.error(f"채팅 ID '{chat_id}' 변환 실패: {str(e)}")
                
                chat_ids = int_chat_ids
                logger.info(f"최종 채팅 ID 목록: {chat_ids}")
            else:
                logger.error(f"지원되지 않는 채팅 ID 형식: {type(chat_ids_str)}")
                return
        except Exception as e:
            logger.error(f"채팅 ID 처리 중 오류: {str(e)}")
            logger.error(traceback.format_exc())
            return
        
        # 3. 봇 객체 생성
        logger.info("3. 텔레그램 봇 객체 생성 중...")
        from telegram import Bot
        
        bot = Bot(bot_token)
        logger.info("봇 객체 생성 완료")
        
        # 4. 봇 정보 확인
        logger.info("4. 봇 정보 확인 중...")
        bot_info = await bot.get_me()
        logger.info(f"봇 정보: {bot_info.username} (ID: {bot_info.id})")
        
        # 5. 메시지 전송
        logger.info("5. 메시지 전송 중...")
        
        for chat_id in chat_ids:
            try:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                test_message = f"🔧 직접 전송 테스트 메시지: {timestamp}"
                
                logger.info(f"채팅 ID {chat_id}로 메시지 전송 시도 중...")
                
                # 전송 실행
                start_time = datetime.now()
                message = await bot.send_message(
                    chat_id=chat_id,
                    text=test_message
                )
                end_time = datetime.now()
                
                duration = (end_time - start_time).total_seconds()
                logger.info(f"메시지 전송 성공! (메시지 ID: {message.message_id}, 소요 시간: {duration:.3f}초)")
                
                # 메시지 객체 정보 로깅
                logger.debug(f"메시지 객체 정보: chat_id={message.chat.id}, message_id={message.message_id}, date={message.date}")
                
            except Exception as e:
                logger.error(f"채팅 ID {chat_id}로 메시지 전송 실패: {str(e)}")
                logger.error(traceback.format_exc())
        
        logger.info("==== 테스트 완료 ====")
        
    except Exception as e:
        logger.error(f"테스트 실행 중 예외 발생: {str(e)}")
        logger.error(traceback.format_exc())

async def main():
    """메인 함수"""
    # 직접 메시지 전송 테스트
    await send_direct_message()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        logger.error(f"테스트 실행 중 예외 발생: {str(e)}")
        logger.error(traceback.format_exc()) 