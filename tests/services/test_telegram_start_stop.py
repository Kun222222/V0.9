#!/usr/bin/env python3
"""
텔레그램 프로세스 시작/종료 테스트 스크립트
"""

import os
import sys
import asyncio
import logging
from datetime import datetime
import traceback
import signal

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

logger = logging.getLogger("test_telegram")

# 시그널 핸들러 설정
def signal_handler(sig, frame):
    logger.info("테스트 중단 요청이 수신되었습니다.")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

async def test_telegram_start_stop():
    """텔레그램 프로세스 시작 및 종료 테스트"""
    logger.info("=== 테스트 시작: 텔레그램 프로세스 시작/종료 ===")
    
    orchestrator = None
    
    try:
        # 1. 오케스트레이터와 프로세스 매니저 초기화
        logger.info("1. 오케스트레이터 초기화 중...")
        from crosskimp.services.orchestrator import Orchestrator
        
        orchestrator = Orchestrator()
        await orchestrator.initialize()
        logger.info("   - 오케스트레이터 초기화 완료")
        
        # 2. 텔레그램 프로세스 시작
        logger.info("2. 텔레그램 프로세스 시작 테스트...")
        try:
            start_result = await orchestrator.start_process("telegram")
            logger.info(f"   - 시작 결과: {'성공' if start_result else '실패'}")
            logger.info(f"   - 실행 중 상태: {orchestrator.is_process_running('telegram')}")
            
            if start_result:
                # 텔레그램 커맨더 참조 가져오기
                from crosskimp.services.telegram_commander import get_telegram_commander
                telegram = get_telegram_commander()
                
                # 초기화 상태 확인
                logger.info(f"   - 텔레그램 초기화 상태: {telegram.initialized}")
                
                # 봇 객체 확인
                logger.info(f"   - 텔레그램 봇 객체: {'있음' if telegram.bot else '없음'}")
                
                if telegram.bot:
                    try:
                        # 봇 정보 확인
                        bot_info = await telegram.bot.get_me()
                        logger.info(f"   - 봇 정보: {bot_info.username} (ID: {bot_info.id})")
                        
                        # 테스트 메시지 전송
                        logger.info("   - 테스트 메시지 전송 시도...")
                        send_result = await telegram.send_message(f"🧪 테스트 메시지: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                        logger.info(f"   - 메시지 전송 결과: {'성공' if send_result else '실패'}")
                    except Exception as e:
                        logger.error(f"   - 봇 테스트 중 오류: {str(e)}")
                        logger.error(traceback.format_exc())
        except Exception as e:
            logger.error(f"텔레그램 프로세스 시작 중 오류: {str(e)}")
            logger.error(traceback.format_exc())
        
        # 3. 잠시 대기 (메시지 전송 및 처리 시간)
        logger.info("3. 잠시 대기 (5초)...")
        await asyncio.sleep(5)
        
        # 4. 텔레그램 프로세스 종료
        logger.info("4. 텔레그램 프로세스 종료 테스트...")
        try:
            stop_result = await orchestrator.stop_process("telegram")
            logger.info(f"   - 종료 결과: {'성공' if stop_result else '실패'}")
            logger.info(f"   - 실행 중 상태: {orchestrator.is_process_running('telegram')}")
        except Exception as e:
            logger.error(f"텔레그램 프로세스 종료 중 오류: {str(e)}")
            logger.error(traceback.format_exc())
        
        logger.info("=== 테스트 완료: 텔레그램 프로세스 시작/종료 ===")
        
    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {str(e)}")
        logger.error(traceback.format_exc())
    
    # 안전한 종료 보장
    finally:
        if orchestrator and hasattr(orchestrator, 'is_process_running') and orchestrator.is_process_running('telegram'):
            logger.info("정리 작업: 텔레그램 프로세스 종료 시도...")
            try:
                await orchestrator.stop_process("telegram")
            except Exception as e:
                logger.error(f"종료 중 오류: {str(e)}")

if __name__ == "__main__":
    try:
        asyncio.run(test_telegram_start_stop())
    except KeyboardInterrupt:
        logger.info("테스트가 중단되었습니다.")
    except Exception as e:
        logger.error(f"메인 실행 중 오류: {str(e)}")
        logger.error(traceback.format_exc()) 