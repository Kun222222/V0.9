"""
시스템 진입점 모듈

이 모듈은 시스템의 메인 진입점으로, 필요한 모든 구성 요소를 초기화하고 
이벤트 버스, 오케스트레이터, 텔레그램 커맨더 등을 설정합니다.
시스템 종료 시 모든 구성 요소가 안전하게 종료되도록 처리합니다.
"""

import asyncio
import signal
import sys
import logging
import time
from datetime import datetime
import traceback
import os
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

# 파이썬 경로 초기화 (이전 코드 대체)
try:
    # 이전 파이썬 모듈 경로 문제 해결 코드 대체
    print(f"현재 작업 디렉토리: {os.getcwd()}")
    print(f"Python 모듈 검색 경로:")
    for p in sys.path:
        print(f"  - {p}")
except Exception as e:
    print(f"경로 설정 중 오류 발생: {str(e)}")

# 로거 설정
try:
    from src.crosskimp.common.logger.logger import initialize_logging, get_unified_logger, shutdown_logging
    print("로깅 모듈 임포트 성공")
except ImportError as e:
    print(f"로깅 모듈 임포트 실패: {str(e)}")
    print("PYTHONPATH를 올바르게 설정했는지 확인하세요.")
    print("예: export PYTHONPATH=/Users/kun/Desktop/CrossKimpArbitrage/v0.6")
    print("    cd /Users/kun/Desktop/CrossKimpArbitrage/v0.6")
    print("    python3 -m src.crosskimp.main")
    sys.exit(1)

# 인프라 레이어
from crosskimp.common.events import get_event_bus
from crosskimp.common.events.sys_event_bus import EventType

# 서비스 레이어
from crosskimp.services.orchestrator import get_orchestrator
from crosskimp.services.telegram_commander import get_telegram_commander

# 애플리케이션 레이어 (필요에 따라 추가)
from crosskimp.ob_collector import get_orderbook_manager

# 글로벌 변수
_logger = None
_shutdown_event = None
_is_shutting_down = False

# constants_v3 대신 새로운 모듈에서 가져오기
from crosskimp.common.config.common_constants import Exchange, EXCHANGE_NAMES_KR, SystemComponent
from crosskimp.common.config.app_config import AppConfig, get_config

async def init_system():
    """
    시스템 초기화
    
    오케스트레이터를 통해 모든 시스템 컴포넌트를 초기화합니다.
    """
    global _shutdown_event
    
    # 종료 이벤트 생성
    _shutdown_event = asyncio.Event()
    
    try:
        # 오케스트레이터를 통한 시스템 전체 초기화
        orchestrator = get_orchestrator()
        init_success = await orchestrator.initialize_system_components()
        
        if init_success:
            _logger.info("🚀 크로스킴프 시스템이 시작되었습니다.")
            _logger.info("========== 시스템 초기화 완료 ==========")
            return True
        else:
            _logger.error("시스템 컴포넌트 초기화에 실패했습니다.")
            return False
        
    except Exception as e:
        _logger.error(f"시스템 초기화 중 오류 발생: {str(e)}")
        _logger.error(traceback.format_exc())
        
        # 시작 실패 로깅
        _logger.error(f"🚨 크로스킴프 시스템 시작 실패! 오류: {str(e)}")
        return False

async def shutdown_system():
    """
    시스템 종료
    
    모든 구성 요소를 안전하게 종료합니다.
    """
    global _is_shutting_down
    
    # 이미 종료 중이면 중복 실행 방지
    if _is_shutting_down:
        return
        
    _is_shutting_down = True
    
    if _logger:
        _logger.info("========== 시스템 종료 시작 ==========")
    else:
        print("========== 시스템 종료 시작 ==========")
    
    try:
        # 시스템 종료 로깅 (알림 제거)
        if _logger:
            _logger.info("🔄 크로스킴프 시스템이 종료 중입니다...")
        else:
            print("🔄 크로스킴프 시스템이 종료 중입니다...")
            
        # 오케스트레이터 종료 (모든 프로세스 종료)
        orchestrator = get_orchestrator()
        if orchestrator.is_initialized():
            await orchestrator.shutdown()
            if _logger:
                _logger.info("오케스트레이터가 종료되었습니다.")
            else:
                print("오케스트레이터가 종료되었습니다.")
        
        # 텔레그램 커맨더 종료
        telegram_commander = get_telegram_commander()
        if telegram_commander.is_initialized():
            await telegram_commander.shutdown()
            if _logger:
                _logger.info("텔레그램 커맨더가 종료되었습니다.")
            else:
                print("텔레그램 커맨더가 종료되었습니다.")
        
        # 이벤트 버스 종료 (마지막에 종료)
        event_bus = get_event_bus()
        await event_bus.stop()
        if _logger:
            _logger.info("이벤트 버스가 종료되었습니다.")
        else:
            print("이벤트 버스가 종료되었습니다.")
        
    except Exception as e:
        if _logger:
            _logger.error(f"시스템 종료 중 오류 발생: {str(e)}")
        else:
            print(f"시스템 종료 중 오류 발생: {str(e)}")
    
    # 종료 완료 로깅
    if _logger:
        _logger.info("========== 시스템 종료 완료 ==========")
    else:
        print("========== 시스템 종료 완료 ==========")
        
    # 종료 이벤트 설정
    if _shutdown_event:
        _shutdown_event.set()

def setup_signal_handlers():
    """
    시스템 신호 핸들러 설정
    
    SIGINT, SIGTERM 등의 신호를 처리하여 안전한 종료를 보장합니다.
    """
    loop = asyncio.get_event_loop()
    
    # SIGINT (Ctrl+C) 처리
    loop.add_signal_handler(
        signal.SIGINT,
        lambda: asyncio.create_task(handle_termination_signal("SIGINT"))
    )
    
    # SIGTERM 처리
    loop.add_signal_handler(
        signal.SIGTERM,
        lambda: asyncio.create_task(handle_termination_signal("SIGTERM"))
    )
    
    _logger.info("시스템 신호 핸들러가 설정되었습니다.")

async def handle_termination_signal(signal_name):
    """
    종료 신호 처리
    
    Args:
        signal_name: 신호 이름
    """
    _logger.info(f"{signal_name} 신호를 수신했습니다. 시스템을 종료합니다...")
    await shutdown_system()

async def run_forever():
    """
    시스템을 계속 실행하면서 종료 신호를 대기합니다.
    """
    # 종료 이벤트가 설정될 때까지 대기
    await _shutdown_event.wait()
    _logger.info("메인 루프가 종료됩니다.")

async def main_async():
    """
    비동기 메인 함수
    
    시스템을 초기화하고 신호 핸들러를 설정한 후 무한 루프로 실행합니다.
    """
    global _logger
    
    # 로깅 시스템 초기화
    initialize_logging()
    
    # 로거 생성
    from crosskimp.common.logger.logger import get_logger
    _logger = get_logger(__name__, component=SystemComponent.MAIN_SYSTEM.value)
    
    try:
        # 설정 로드
        config = get_config()
        _logger.info(f"설정 로드 완료. 버전: {config.get_system('global.version', '알 수 없음')}")
        
        # 신호 핸들러 설정
        setup_signal_handlers()
        
        # 시스템 초기화
        await init_system()
        
        # 무한 실행
        await run_forever()
            
    except KeyboardInterrupt:
        _logger.info("Ctrl+C로 프로그램이 중단되었습니다.")
    except Exception as e:
        _logger.error(f"시스템 실행 중 오류: {str(e)}")
        _logger.error(traceback.format_exc())
    finally:
        # 시스템 종료
        await shutdown_system()
        
        # 로깅 시스템 종료
        shutdown_logging()

def main():
    """
    시스템 메인 진입점
    """
    exit_code = 0
    
    try:
        # 이벤트 루프 생성 및 메인 함수 실행
        loop = asyncio.get_event_loop()
        exit_code = loop.run_until_complete(main_async())
        
    except Exception as e:
        print(f"치명적인 오류 발생: {str(e)}")
        print(traceback.format_exc())
        exit_code = 1
        
    finally:
        # 이벤트 루프 종료
        try:
            pending_tasks = asyncio.all_tasks(loop)
            if pending_tasks:
                # 남은 태스크 취소
                for task in pending_tasks:
                    task.cancel()
                
                # 모든 태스크가 종료될 때까지 대기
                loop.run_until_complete(
                    asyncio.gather(*pending_tasks, return_exceptions=True)
                )
                
            # 이벤트 루프 종료
            loop.close()
            
        except Exception as e:
            print(f"이벤트 루프 종료 중 오류: {str(e)}")
    
    # 종료 코드 반환
    sys.exit(exit_code)

# 스크립트로 직접 실행 시 메인 함수 호출
if __name__ == "__main__":
    main()
