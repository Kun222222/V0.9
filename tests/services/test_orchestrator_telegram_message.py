#!/usr/bin/env python
"""
오케스트레이터를 통한 텔레그램 메시지 발송 테스트 스크립트

이 스크립트는 오케스트레이터를 통해 텔레그램 프로세스를 시작하고,
메시지를 발송하는 전체 과정을 테스트합니다.
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

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger('orchestrator_test')

# 시그널 핸들러 등록
def signal_handler(sig, frame):
    logger.info("사용자에 의해 테스트가 중단되었습니다.")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

async def test_orchestrator_telegram():
    """오케스트레이터를 통한 텔레그램 메시지 전송 테스트"""
    logger.info("==== 오케스트레이터를 통한 텔레그램 메시지 전송 테스트 ====")
    
    try:
        # 1. 이벤트 버스 초기화
        logger.info("1. 이벤트 버스 초기화 중...")
        from crosskimp.common.events import get_event_bus
        
        event_bus = get_event_bus()
        await event_bus.start()
        logger.info("이벤트 버스 초기화 완료")
        
        # 2. 오케스트레이터 초기화
        logger.info("2. 오케스트레이터 초기화 중...")
        from crosskimp.services.orchestrator import Orchestrator
        
        orchestrator = Orchestrator()
        # 오케스트레이터 초기화 호출
        await orchestrator.initialize()
        logger.info("오케스트레이터 초기화 완료")
        
        # 3. 초기 상태 확인
        logger.info("3. 초기 상태 확인 중...")
        try:
            # 오케스트레이터를 통해 텔레그램 프로세스 상태 확인
            telegram_status = orchestrator.is_process_running("telegram")
            logger.info(f"텔레그램 프로세스 초기 상태: {'실행 중' if telegram_status else '정지됨'}")
        except Exception as e:
            logger.error(f"텔레그램 프로세스 상태 확인 실패: {str(e)}")
            logger.error(traceback.format_exc())
            telegram_status = False
        
        # 4. 텔레그램 프로세스 시작
        logger.info("4. 텔레그램 프로세스 시작 중...")
        try:
            # 이미 실행 중이면 재시작
            if telegram_status:
                logger.info("텔레그램 프로세스가 이미 실행 중입니다. 재시작합니다.")
                result = await orchestrator.restart_process("telegram")
                logger.info(f"텔레그램 프로세스 재시작 결과: {result}")
            else:
                # 정지 상태면 시작
                result = await orchestrator.start_process("telegram")
                logger.info(f"텔레그램 프로세스 시작 결과: {result}")
            
            # 초기화 대기
            logger.info("텔레그램 프로세스 초기화 대기 중... (5초)")
            await asyncio.sleep(5)
            
        except Exception as e:
            logger.error(f"텔레그램 프로세스 시작 실패: {str(e)}")
            logger.error(traceback.format_exc())
            return
        
        # 5. 텔레그램 상태 확인
        logger.info("5. 텔레그램 프로세스 상태 확인 중...")
        try:
            # 오케스트레이터를 통해 텔레그램 프로세스 상태 확인
            telegram_status = orchestrator.is_process_running("telegram")
            logger.info(f"텔레그램 프로세스 현재 상태: {'실행 중' if telegram_status else '정지됨'}")
            
            if not telegram_status:
                logger.error("텔레그램 프로세스가 시작되지 않았습니다.")
                return
        except Exception as e:
            logger.error(f"텔레그램 프로세스 상태 확인 실패: {str(e)}")
            logger.error(traceback.format_exc())
            return
        
        # 6. 텔레그램 커맨더 직접 참조
        logger.info("6. 텔레그램 커맨더 참조 및 상태 확인 중...")
        from crosskimp.services.telegram_commander import get_telegram_commander
        
        telegram = get_telegram_commander()
        logger.info(f"텔레그램 커맨더 초기화 상태: {telegram.is_initialized()}")
        
        if telegram.bot:
            logger.info("텔레그램 봇 객체가 존재합니다.")
            
            try:
                bot_info = await telegram.bot.get_me()
                logger.info(f"봇 정보: {bot_info.username} (ID: {bot_info.id})")
            except Exception as e:
                logger.error(f"봇 정보 확인 실패: {str(e)}")
                logger.error(traceback.format_exc())
        else:
            logger.error("텔레그램 봇 객체가 없습니다.")
        
        # 7. 텔레그램 커맨더로 직접 메시지 전송
        logger.info("7. 텔레그램 커맨더로 직접 메시지 전송 중...")
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            message = f"🔍 오케스트레이터 테스트 (커맨더 직접 전송): {timestamp}"
            
            result = await telegram.send_message(message)
            logger.info(f"메시지 직접 전송 결과: {'성공' if result else '실패'}")
        except Exception as e:
            logger.error(f"메시지 직접 전송 실패: {str(e)}")
            logger.error(traceback.format_exc())
        
        # 8. 오케스트레이터를 통한 메시지 전송
        logger.info("8. 오케스트레이터를 통한 메시지 전송 중...")
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            message = f"🔍 오케스트레이터 테스트 (오케스트레이터 전송): {timestamp}"
            
            # 텔레그램 커맨더로 직접 메시지 전송 (오케스트레이터 사용 대신)
            telegram = get_telegram_commander()
            result = await telegram.send_message(message)
            
            logger.info(f"텔레그램 메시지 전송 결과: {'성공' if result else '실패'}")
        except Exception as e:
            logger.error(f"오케스트레이터 메시지 전송 실패: {str(e)}")
            logger.error(traceback.format_exc())
        
        # 9. 정리 작업
        logger.info("9. 정리 작업 중...")
        
        # 이벤트 버스 정리
        await event_bus.stop()
        logger.info("이벤트 버스 정리 완료")
        
        logger.info("==== 테스트 완료 ====")
        
    except Exception as e:
        logger.error(f"테스트 실행 중 예외 발생: {str(e)}")
        logger.error(traceback.format_exc())

async def main():
    """메인 함수"""
    await test_orchestrator_telegram()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        logger.error(f"테스트 실행 중 예외 발생: {str(e)}")
        logger.error(traceback.format_exc()) 