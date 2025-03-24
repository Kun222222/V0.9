#!/usr/bin/env python
"""
텔레그램 메시지 전송 통합 테스트

이 스크립트는 실제 애플리케이션과 동일한 방식으로 시스템을 초기화하고
텔레그램 메시지 전송 과정을 단계별로 테스트하여 문제점을 찾아냅니다.
"""

import os
import sys
import asyncio
import logging
import traceback
from datetime import datetime
import signal
import json

# 프로젝트 루트 디렉토리를 시스템 경로에 추가
src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(src_dir)
print(f"추가된 경로: {src_dir}")

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # 콘솔 출력
    ]
)
logger = logging.getLogger('telegram_integration_test')

# 시그널 핸들러
def signal_handler(sig, frame):
    logger.info("Ctrl+C를 누르셨습니다. 프로그램을 종료합니다.")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

class IntegrationTest:
    """통합 테스트 클래스"""
    
    def __init__(self):
        self.event_bus = None
        self.orchestrator = None
        self.telegram_commander = None
        self.initialized = False
    
    async def initialize(self):
        """시스템 초기화 - 실제 main.py와 동일한 방식으로 초기화"""
        try:
            logger.info("==== 시스템 초기화 시작 ====")
            
            # 1. 이벤트 버스 초기화
            from crosskimp.common.events import get_event_bus
            from crosskimp.services.types import EventType
            
            self.event_bus = get_event_bus()
            await self.event_bus.initialize()
            logger.info("이벤트 버스 초기화 완료")
            
            # 2. 오케스트레이터 초기화
            from crosskimp.services.orchestrator import Orchestrator
            self.orchestrator = Orchestrator()
            await self.orchestrator.initialize()
            logger.info("오케스트레이터 초기화 완료")
            
            # 초기화 완료
            self.initialized = True
            logger.info("==== 시스템 초기화 완료 ====")
            return True
        except Exception as e:
            logger.error(f"시스템 초기화 중 오류 발생: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    async def get_telegram_commander(self):
        """텔레그램 커맨더 인스턴스 가져오기"""
        from crosskimp.services.telegram_commander import get_telegram_commander
        self.telegram_commander = get_telegram_commander()
        return self.telegram_commander
    
    async def check_telegram_state(self):
        """텔레그램 상태 확인"""
        logger.info("==== 텔레그램 상태 확인 ====")
        
        telegram = await self.get_telegram_commander()
        
        # 텔레그램 커맨더 초기화 상태 확인
        logger.info(f"텔레그램 커맨더 초기화 상태: {telegram.is_initialized()}")
        
        # 봇 객체 상태 확인
        if telegram.bot:
            logger.info(f"텔레그램 봇 객체 존재함")
            try:
                bot_info = await telegram.bot.get_me()
                logger.info(f"봇 정보: {bot_info.username} (ID: {bot_info.id})")
            except Exception as e:
                logger.error(f"봇 정보 조회 실패: {str(e)}")
                logger.error(traceback.format_exc())
        else:
            logger.error("텔레그램 봇 객체가 없음")
        
        # 앱 객체 상태 확인
        if telegram.app:
            logger.info(f"텔레그램 앱 객체 존재함, 실행 중: {getattr(telegram.app, 'running', None)}")
            if hasattr(telegram.app, 'updater'):
                logger.info(f"텔레그램 업데이터 객체 존재함, 실행 중: {getattr(telegram.app.updater, 'running', None)}")
        else:
            logger.error("텔레그램 앱 객체가 없음")
        
        # 채팅 ID 확인
        logger.info(f"허용된 채팅 ID: {telegram.allowed_chat_ids}")
        logger.info(f"채팅 ID 타입: {[type(chat_id) for chat_id in telegram.allowed_chat_ids]}")
        
        return telegram.is_initialized()
    
    async def test_direct_message(self):
        """텔레그램 커맨더를 통한 직접 메시지 전송 테스트"""
        logger.info("==== 텔레그램 직접 메시지 전송 테스트 ====")
        
        telegram = await self.get_telegram_commander()
        if not telegram.is_initialized():
            logger.error("텔레그램 커맨더가 초기화되지 않았습니다.")
            return False
        
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            message = f"🔍 통합 테스트 직접 메시지: {timestamp}"
            logger.info(f"메시지 전송 시도: '{message}'")
            
            # 메시지 전송 시도
            result = await telegram.send_message(message)
            logger.info(f"메시지 전송 결과: {'성공' if result else '실패'}")
            return result
        except Exception as e:
            logger.error(f"직접 메시지 전송 중 오류 발생: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    async def test_orchestrator_message(self):
        """오케스트레이터를 통한 텔레그램 메시지 전송 테스트"""
        logger.info("==== 오케스트레이터를 통한 메시지 전송 테스트 ====")
        
        # 텔레그램 프로세스 시작
        logger.info("텔레그램 프로세스 시작 시도...")
        start_result = await self.orchestrator.start_process("telegram")
        logger.info(f"텔레그램 프로세스 시작 결과: {'성공' if start_result else '실패'}")
        
        if not start_result:
            logger.error("텔레그램 프로세스를 시작할 수 없습니다.")
            return False
        
        # 프로세스 상태 확인
        is_running = self.orchestrator.is_process_running("telegram")
        logger.info(f"텔레그램 프로세스 실행 상태: {'실행 중' if is_running else '실행 중이 아님'}")
        
        # 잠시 대기
        logger.info("텔레그램 프로세스 초기화 대기 중 (3초)...")
        await asyncio.sleep(3)
        
        # 텔레그램 커맨더 상태 확인
        telegram = await self.get_telegram_commander()
        logger.info(f"텔레그램 커맨더 초기화 상태: {telegram.is_initialized()}")
        
        if not telegram.is_initialized():
            logger.error("텔레그램 커맨더가 초기화되지 않았습니다.")
            return False
        
        # 메시지 전송 시도
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            message = f"🔍 통합 테스트 오케스트레이터 메시지: {timestamp}"
            logger.info(f"메시지 전송 시도: '{message}'")
            
            result = await telegram.send_message(message)
            logger.info(f"메시지 전송 결과: {'성공' if result else '실패'}")
            
            # 잠시 대기 (메시지 전송 확인을 위해)
            logger.info("메시지 전송 완료 대기 중 (3초)...")
            await asyncio.sleep(3)
            
            # 텔레그램 프로세스 중지
            logger.info("텔레그램 프로세스 중지 시도...")
            stop_result = await self.orchestrator.stop_process("telegram")
            logger.info(f"텔레그램 프로세스 중지 결과: {'성공' if stop_result else '실패'}")
            
            return result
        except Exception as e:
            logger.error(f"오케스트레이터를 통한 메시지 전송 중 오류 발생: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    async def check_send_message_code(self):
        """텔레그램 커맨더의 send_message 메서드 세부 흐름 추적"""
        logger.info("==== 텔레그램 커맨더 send_message 메서드 확인 ====")
        
        telegram = await self.get_telegram_commander()
        if not telegram.is_initialized():
            logger.error("텔레그램 커맨더가 초기화되지 않았습니다.")
            return False
        
        # 필수 속성 확인
        logger.info(f"봇 객체: {telegram.bot is not None}")
        logger.info(f"텔레그램 토큰: {'설정됨' if telegram.telegram_token else '설정 안됨'}")
        logger.info(f"허용된 채팅 ID: {telegram.allowed_chat_ids}")
        
        # 메시지 전송 시도 전 로그 핸들러 추가
        debug_handler = logging.StreamHandler()
        debug_handler.setLevel(logging.DEBUG)
        debug_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        debug_handler.setFormatter(debug_formatter)
        
        telegram._logger.addHandler(debug_handler)
        telegram._logger.setLevel(logging.DEBUG)
        
        # 메시지 전송 시도
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            message = f"🔍 디버깅 추적 메시지: {timestamp}"
            logger.info(f"메시지 전송 시도: '{message}'")
            
            # 채팅 ID 원시 값 확인
            for chat_id in telegram.allowed_chat_ids:
                logger.info(f"채팅 ID 처리 전: '{chat_id}' 타입: {type(chat_id)}")
                try:
                    # 직접 변환 테스트
                    chat_id_int = int(chat_id) if isinstance(chat_id, str) else chat_id
                    logger.info(f"채팅 ID 변환 성공: {chat_id} -> {chat_id_int} (타입: {type(chat_id_int)})")
                except Exception as e:
                    logger.error(f"채팅 ID 변환 실패: {str(e)}")
            
            # 메시지 전송
            result = await telegram.send_message(message)
            logger.info(f"메시지 전송 결과: {'성공' if result else '실패'}")
            return result
        except Exception as e:
            logger.error(f"디버깅 추적 중 오류 발생: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    async def test_config_and_environment(self):
        """설정 및 환경 변수 확인"""
        logger.info("==== 설정 및 환경 변수 확인 ====")
        
        try:
            # 설정 확인
            from crosskimp.common.config.app_config import get_config
            config = get_config()
            
            telegram_token = config.get_env("telegram.bot_token")
            chat_ids = config.get_env("telegram.chat_id")
            
            logger.info(f"텔레그램 토큰: {'설정됨' if telegram_token else '설정 안됨'}")
            if telegram_token:
                masked_token = f"...{telegram_token[-5:]}" if len(telegram_token) > 5 else "***"
                logger.info(f"토큰 마스킹: {masked_token}")
            
            logger.info(f"텔레그램 채팅 ID: {chat_ids}")
            logger.info(f"채팅 ID 타입: {type(chat_ids)}")
            
            # 환경 변수 확인
            logger.info(f"TELEGRAM_BOT_TOKEN 환경변수: {'설정됨' if os.environ.get('TELEGRAM_BOT_TOKEN') else '설정 안됨'}")
            logger.info(f"TELEGRAM_CHAT_ID 환경변수: {'설정됨' if os.environ.get('TELEGRAM_CHAT_ID') else '설정 안됨'}")
            
            return True
        except Exception as e:
            logger.error(f"설정 및 환경 변수 확인 중 오류 발생: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    async def shutdown(self):
        """시스템 종료"""
        logger.info("==== 시스템 종료 시작 ====")
        
        try:
            # 오케스트레이터 종료
            if self.orchestrator and self.orchestrator.is_initialized():
                await self.orchestrator.shutdown_system()
                logger.info("오케스트레이터 종료 완료")
            
            # 이벤트 버스 종료
            if self.event_bus:
                await self.event_bus.stop()
                logger.info("이벤트 버스 종료 완료")
            
            logger.info("==== 시스템 종료 완료 ====")
        except Exception as e:
            logger.error(f"시스템 종료 중 오류 발생: {str(e)}")
            logger.error(traceback.format_exc())

async def run_integration_test():
    """통합 테스트 실행"""
    test = IntegrationTest()
    
    try:
        # 1. 시스템 초기화
        if not await test.initialize():
            logger.error("시스템 초기화 실패. 테스트를 중단합니다.")
            return
        
        # 2. 설정 및 환경 변수 확인
        await test.test_config_and_environment()
        
        # 3. 텔레그램 상태 확인
        await test.check_telegram_state()
        
        # 4. send_message 코드 흐름 추적
        await test.check_send_message_code()
        
        # 5. 직접 메시지 전송 테스트
        await test.test_direct_message()
        
        # 6. 오케스트레이터를 통한 메시지 전송 테스트
        await test.test_orchestrator_message()
        
    except Exception as e:
        logger.error(f"통합 테스트 중 예외 발생: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # 시스템 종료
        await test.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(run_integration_test())
    except KeyboardInterrupt:
        logger.info("사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        logger.error(f"테스트 실행 중 예외 발생: {str(e)}")
        logger.error(traceback.format_exc()) 