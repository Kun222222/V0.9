#!/usr/bin/env python
"""
텔레그램 메시지 전송 디버깅 테스트 스크립트

이 스크립트는 텔레그램 메시지 전송 과정을 단계별로 테스트하고 디버깅합니다.
"""

import os
import sys
import asyncio
import logging
import traceback
from datetime import datetime
import signal

# 프로젝트 루트 디렉토리를 시스템 경로에 추가
src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(src_dir)
print(f"추가된 경로: {src_dir}")

# 필요한 모듈 미리 import
from crosskimp.services.telegram_commander import get_telegram_commander
from crosskimp.common.events import get_event_bus
from crosskimp.services.orchestrator import Orchestrator

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # 콘솔 출력
    ]
)
logger = logging.getLogger('telegram_test')

# 시그널 핸들러
def signal_handler(sig, frame):
    logger.info("Ctrl+C를 누르셨습니다. 프로그램을 종료합니다.")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

async def test_telegram_message():
    """텔레그램 메시지 전송 테스트"""
    try:
        logger.info("=== 텔레그램 메시지 전송 테스트 시작 ===")
        
        # 1. 설정 로드
        logger.info("1. 설정 로드 중...")
        from crosskimp.common.config.app_config import get_config
        config = get_config()
        
        # 텔레그램 설정 확인
        telegram_token = config.get_env("telegram.bot_token")
        chat_ids_str = config.get_env("telegram.chat_id")
        
        logger.info(f"텔레그램 토큰: {'설정됨' if telegram_token else '설정되지 않음'}")
        if telegram_token:
            masked_token = f"...{telegram_token[-5:]}"
            logger.info(f"토큰 마스킹: {masked_token}")
        
        logger.info(f"텔레그램 채팅 ID: {chat_ids_str}")
        logger.info(f"채팅 ID 타입: {type(chat_ids_str)}")
        
        if not telegram_token:
            logger.error("텔레그램 토큰이 설정되지 않았습니다.")
            return
            
        # 2. 직접 텔레그램 봇 테스트
        logger.info("2. 텔레그램 봇 직접 테스트...")
        from telegram import Bot
        
        # 봇 객체 생성
        try:
            bot = Bot(telegram_token)
            logger.info("봇 객체 생성 성공")
        except Exception as e:
            logger.error(f"봇 객체 생성 실패: {str(e)}")
            logger.error(traceback.format_exc())
            return
            
        # 봇 정보 가져오기
        try:
            logger.info("봇 정보 확인 중...")
            bot_info = await bot.get_me()
            logger.info(f"봇 정보: {bot_info.username} (ID: {bot_info.id})")
        except Exception as e:
            logger.error(f"봇 정보 가져오기 실패: {str(e)}")
            logger.error(traceback.format_exc())
            return
            
        # 직접 메시지 전송
        logger.info("직접 메시지 전송 테스트 중...")
        
        # 채팅 ID 처리
        chat_ids = []
        if isinstance(chat_ids_str, str):
            if ',' in chat_ids_str:
                chat_ids = [id.strip() for id in chat_ids_str.split(',')]
            else:
                chat_ids = [chat_ids_str.strip()]
        elif isinstance(chat_ids_str, (list, tuple)):
            chat_ids = chat_ids_str
        else:
            logger.error(f"지원되지 않는 채팅 ID 형식: {type(chat_ids_str)}")
            return
            
        logger.info(f"처리된 채팅 ID 목록: {chat_ids}")
        
        # 각 채팅 ID로 메시지 전송
        for chat_id in chat_ids:
            try:
                logger.info(f"채팅 ID {chat_id}로 메시지 전송 시도...")
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                message = await bot.send_message(
                    chat_id=chat_id,
                    text=f"🔍 디버깅 테스트 메시지 (직접 전송): {timestamp}"
                )
                logger.info(f"메시지 전송 성공! 메시지 ID: {message.message_id}")
            except Exception as e:
                logger.error(f"채팅 ID {chat_id}로 메시지 전송 실패: {str(e)}")
                logger.error(traceback.format_exc())
        
        # 3. 텔레그램 커맨더를 통한 테스트
        logger.info("3. 텔레그램 커맨더를 통한 테스트...")
        from crosskimp.services.telegram_commander import get_telegram_commander
        from crosskimp.common.events import get_event_bus
        
        # 이벤트 버스 초기화
        event_bus = get_event_bus()
        await event_bus.start()
        
        # 텔레그램 커맨더 초기화
        telegram = get_telegram_commander()
        
        try:
            logger.info("텔레그램 커맨더 초기화 중...")
            await telegram.initialize()
            logger.info(f"텔레그램 커맨더 초기화 결과: {telegram.is_initialized()}")
            
            # 봇 상태 확인을 위한 대기
            logger.info("봇 초기화 대기 중 (3초)...")
            await asyncio.sleep(3)
            
            # 봇 상태 확인
            if telegram.bot:
                try:
                    bot_info = await telegram.bot.get_me()
                    logger.info(f"텔레그램 커맨더 봇 정보: {bot_info.username} (ID: {bot_info.id})")
                except Exception as e:
                    logger.error(f"텔레그램 커맨더 봇 정보 확인 실패: {str(e)}")
            else:
                logger.error("텔레그램 커맨더의 봇 객체가 없습니다.")
            
            # 앱 상태 확인
            if telegram.app:
                logger.info(f"텔레그램 앱 상태: running={getattr(telegram.app, 'running', None)}")
                if hasattr(telegram.app, 'updater'):
                    logger.info(f"텔레그램 업데이터 상태: running={getattr(telegram.app.updater, 'running', None)}")
            else:
                logger.error("텔레그램 커맨더의 앱 객체가 없습니다.")
            
            # 텔레그램 커맨더로 메시지 전송
            if telegram.is_initialized():
                logger.info("텔레그램 커맨더 메시지 전송 시도 중...")
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                success = await telegram.send_message(f"🔍 디버깅 테스트 메시지 (커맨더): {timestamp}")
                logger.info(f"텔레그램 커맨더 메시지 전송 결과: {'성공' if success else '실패'}")
            else:
                logger.error("텔레그램 커맨더가 초기화되지 않았습니다.")
            
            # 잠시 대기 (메시지 전송 완료 대기)
            logger.info("메시지 전송 완료 대기 중 (5초)...")
            await asyncio.sleep(5)
            
            # 텔레그램 커맨더 종료
            logger.info("텔레그램 커맨더 종료 중...")
            await telegram.shutdown()
            logger.info(f"텔레그램 커맨더 종료 후 상태: {telegram.is_initialized()}")
            
        except Exception as e:
            logger.error(f"텔레그램 커맨더 테스트 중 오류: {str(e)}")
            logger.error(traceback.format_exc())
        
        # 4. 오케스트레이터를 통한 텔레그램 프로세스 테스트
        logger.info("4. 오케스트레이터 통합 테스트...")
        try:
            from crosskimp.services.orchestrator import Orchestrator
            
            # 오케스트레이터 초기화
            orchestrator = Orchestrator()
            await orchestrator.initialize()
            logger.info("오케스트레이터 초기화 완료")
            
            # 텔레그램 프로세스 시작
            logger.info("오케스트레이터를 통해 텔레그램 프로세스 시작 중...")
            success = await orchestrator.start_process("telegram")
            logger.info(f"텔레그램 프로세스 시작 결과: {'성공' if success else '실패'}")
            
            # 대기
            logger.info("텔레그램 프로세스 시작 대기 중 (3초)...")
            await asyncio.sleep(3)
            
            # 상태 확인
            is_running = orchestrator.is_process_running("telegram")
            logger.info(f"텔레그램 프로세스 실행 상태: {'실행 중' if is_running else '실행 중이 아님'}")
            
            # 메시지 전송
            logger.info("오케스트레이터 경로를 통한 메시지 전송 시도 중...")
            telegram_from_orchestrator = get_telegram_commander()
            if telegram_from_orchestrator and telegram_from_orchestrator.is_initialized():
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                success = await telegram_from_orchestrator.send_message(f"🔍 디버깅 테스트 메시지 (오케스트레이터): {timestamp}")
                logger.info(f"오케스트레이터 경로 메시지 전송 결과: {'성공' if success else '실패'}")
            else:
                logger.error("오케스트레이터에서 초기화된 텔레그램 커맨더를 가져올 수 없습니다.")
            
            # 대기
            logger.info("메시지 전송 완료 대기 중 (5초)...")
            await asyncio.sleep(5)
            
            # 텔레그램 프로세스 중지
            logger.info("오케스트레이터를 통해 텔레그램 프로세스 중지 중...")
            success = await orchestrator.stop_process("telegram")
            logger.info(f"텔레그램 프로세스 중지 결과: {'성공' if success else '실패'}")
            
            # 오케스트레이터 종료
            logger.info("오케스트레이터 종료 중...")
            await orchestrator.shutdown_system()
            logger.info("오케스트레이터 종료 완료")
            
        except Exception as e:
            logger.error(f"오케스트레이터 통합 테스트 중 오류: {str(e)}")
            logger.error(traceback.format_exc())
        
        logger.info("=== 텔레그램 메시지 전송 테스트 완료 ===")
        
    except Exception as e:
        logger.error(f"테스트 중 예외 발생: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    # 이벤트 루프 실행
    try:
        asyncio.run(test_telegram_message())
    except KeyboardInterrupt:
        logger.info("사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        logger.error(f"테스트 실행 중 예외 발생: {str(e)}")
        logger.error(traceback.format_exc()) 