"""
크로스 킴프 시스템 엔트리 포인트

이 모듈은 시스템의 메인 엔트리 포인트로, 전체 시스템을 시작합니다.
"""

import os
import sys
import asyncio
import signal
import logging
import time
import traceback
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv

# 시스템 루트 설정 - 모든 모듈 임포트 이전에 수행
# .env 파일 로드
dotenv_path = os.path.join(os.getcwd(), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
    print(f".env 파일 로드 완료: {dotenv_path}")

# PROJECT_ROOT 환경변수 확인 및 설정
project_root = os.environ.get('PROJECT_ROOT')
if not project_root:
    # 환경변수가 없으면 자동 계산하여 설정
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    os.environ['PROJECT_ROOT'] = project_root
    print(f"PROJECT_ROOT 환경변수가 없습니다. 자동 계산된 값으로 설정: {project_root}")
else:
    print(f"PROJECT_ROOT 환경변수 사용: {project_root}")

# 시스템 경로에 추가
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    print(f"시스템 루트 경로를 sys.path에 추가: {project_root}")

# 파이썬 경로 초기화
try:
    # 이전 파이썬 모듈 경로 문제 해결 코드
    print(f"현재 작업 디렉토리: {os.getcwd()}")
    print(f"Python 모듈 검색 경로:")
    for p in sys.path:
        print(f"  - {p}")
except Exception as e:
    print(f"경로 설정 중 오류 발생: {str(e)}")

# 로거 설정
try:
    from crosskimp.common.logger.logger import initialize_logging, get_unified_logger, shutdown_logging
    print("로깅 모듈 임포트 성공")
except ImportError as e:
    print(f"로깅 모듈 임포트 실패: {str(e)}")
    print("PYTHONPATH를 올바르게 설정했는지 확인하세요.")
    print("예: export PYTHONPATH=/Users/kun/Desktop/CrossKimpArbitrage/v0.6")
    print("    cd /Users/kun/Desktop/CrossKimpArbitrage/v0.6")
    print("    python3 -m src.crosskimp.main")
    sys.exit(1)

# 이벤트 버스 및 이벤트 타입
from crosskimp.common.events.system_eventbus import get_event_bus
from crosskimp.common.events.system_types import EventChannels, EventValues

# 서비스 레이어
from crosskimp.services.orchestrator import Orchestrator
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.config.app_config import get_config

# 글로벌 변수
_logger = None
_shutdown_event = None
_is_shutting_down = False

# 오케스트레이터 인스턴스
orchestrator = None

async def initialize() -> bool:
    """
    시스템 초기화
    
    Returns:
        bool: 초기화 성공 여부
    """
    global orchestrator, _logger
    
    try:
        _logger.info("🚀 크로스 킴프 시스템 초기화 시작")
        
        # 이벤트 버스 초기화 및 시작
        event_bus = get_event_bus()
        await event_bus.initialize()
        
        # 중요: 텔레그램 알림 시스템을 오케스트레이터보다 먼저 초기화 (이벤트 핸들러 등록 순서 때문)
        
        # 텔레그램 커맨더 초기화 및 시작
        from crosskimp.telegram_bot.commander import get_telegram_commander
        telegram = get_telegram_commander()
        
        # 텔레그램 커맨더 시작
        await telegram.start()
        _logger.info("텔레그램 커맨더 시작 완료")
        
        # 텔레그램 알림 시스템 초기화
        from crosskimp.telegram_bot.notify import get_telegram_notifier, initialize_notifier
        notifier = get_telegram_notifier()
        # 알림 시스템 초기화 (봇과 채팅 ID 설정)
        await initialize_notifier(notifier)
        # 이벤트 구독자 초기화 및 설정
        from crosskimp.telegram_bot.event_subscriber import get_event_subscriber
        event_subscriber = get_event_subscriber(notifier)
        await notifier.setup_event_subscriber(event_subscriber)
        _logger.info("텔레그램 알림 시스템 초기화 완료")
        
        # 오케스트레이터 초기화 (텔레그램 시스템 초기화 이후에)
        orchestrator = Orchestrator(event_bus)
        await orchestrator.initialize()
        
        # 오케스트레이터 인스턴스 설정 (의존성 주입)
        telegram.set_orchestrator(orchestrator)
        
        _logger.info("✅ 시스템 초기화 완료")
        return True
    
    except Exception as e:
        _logger.error(f"❌ 시스템 초기화 중 오류 발생: {str(e)}")
        _logger.error(traceback.format_exc())
        return False

async def start_system() -> bool:
    """
    시스템 시작
    
    Returns:
        bool: 시작 성공 여부
    """
    global orchestrator, _logger
    
    if not orchestrator:
        _logger.error("오케스트레이터가 초기화되지 않았습니다.")
        return False
    
    try:
        _logger.info("🚀 크로스 킴프 시스템 시작")
        
        # 오케스트레이터를 통한 시스템 시작
        await orchestrator.start()
        
        _logger.info("✅ 시스템이 시작되었습니다.")
        return True
        
    except Exception as e:
        _logger.error(f"❌ 시스템 시작 중 오류 발생: {str(e)}")
        _logger.error(traceback.format_exc())
        return False

async def shutdown_system() -> None:
    """시스템 종료"""
    global orchestrator, _logger, _is_shutting_down, _shutdown_event
    
    # 이미 종료 중이면 중복 실행 방지
    if _is_shutting_down:
        return
        
    _is_shutting_down = True
    
    _logger.info("🛑 시스템 종료 중...")
    
    try:
        # 오케스트레이터를 통한 시스템 종료
        if orchestrator and orchestrator.is_initialized():
            await orchestrator.shutdown_system()
        
        # 이벤트 버스 종료는 오케스트레이터에서 책임지지 않음
        event_bus = get_event_bus()
        await event_bus.shutdown()
        
        _logger.info("✅ 시스템이 정상적으로 종료되었습니다.")
        
    except Exception as e:
        _logger.error(f"❌ 시스템 종료 중 오류 발생: {str(e)}")
        _logger.error(traceback.format_exc())
    finally:
        # 종료 이벤트 설정
        if _shutdown_event:
            _shutdown_event.set()
        
        # 로깅 시스템 종료
        shutdown_logging()

def signal_handler() -> None:
    """시그널 핸들러 (Ctrl+C)"""
    global _logger
    
    _logger.info("종료 신호를 받았습니다. 시스템을 종료합니다...")
    
    # asyncio 이벤트 루프 가져오기
    loop = asyncio.get_event_loop()
    
    # 시스템 종료 태스크 생성 및 실행
    if not loop.is_closed():
        loop.create_task(shutdown_system())
        
        # 1초 후 종료 (비동기 작업 정리 시간)
        loop.call_later(1, loop.stop)

async def run() -> None:
    """메인 실행 함수"""
    global _shutdown_event, _logger
    
    # 종료 이벤트 생성
    _shutdown_event = asyncio.Event()
    
    try:
        # 종료 시그널 핸들러 등록
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)
        
        # 시스템 초기화 및 시작
        if await initialize():
            await start_system()
            
            # 종료 이벤트가 설정될 때까지 대기
            await _shutdown_event.wait()
        else:
            _logger.error("시스템 초기화 실패로 종료합니다.")
                
    except KeyboardInterrupt:
        _logger.info("키보드 인터럽트: 시스템을 종료합니다...")
    except Exception as e:
        _logger.error(f"실행 중 예외 발생: {str(e)}")
        _logger.error(traceback.format_exc())
    finally:
        # 시스템 종료
        await shutdown_system()

def main() -> None:
    """메인 엔트리 포인트"""
    global _logger
    
    try:
        # 로깅 시스템 초기화
        initialize_logging()
        
        # 로거 생성
        _logger = get_unified_logger(component=SystemComponent.SYSTEM.value)
        
        # asyncio 이벤트 루프 실행
        asyncio.run(run())
        
    except Exception as e:
        print(f"치명적 오류: {str(e)}")
        print(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
