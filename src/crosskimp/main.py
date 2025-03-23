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

# 로거 설정
from crosskimp.common.logger.logger import initialize_logging, get_unified_logger

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

async def init_system():
    """
    시스템 초기화
    
    - 이벤트 버스 초기화
    - 오케스트레이터 초기화
    - 텔레그램 커맨더 초기화
    - 모든 구성 요소 연결
    """
    global _shutdown_event
    
    # 종료 이벤트 생성
    _shutdown_event = asyncio.Event()
    
    try:
        # 이벤트 버스 초기화 (인프라 레이어)
        event_bus = get_event_bus()
        await event_bus.initialize()
        _logger.info("이벤트 버스가 초기화되었습니다.")
        
        # 텔레그램 커맨더 초기화
        telegram_commander = get_telegram_commander()
        await telegram_commander.initialize()
        _logger.info("텔레그램 커맨더가 초기화되었습니다.")
        
        # 오케스트레이터 초기화
        orchestrator = get_orchestrator()
        await orchestrator.initialize()
        _logger.info("오케스트레이터가 초기화되었습니다.")
        
        # 오더북 관리자 오케스트레이터에 등록 (객체만 전달)
        orderbook_manager = get_orderbook_manager()
        await orchestrator.register_process(
            "ob_collector",           # 프로세스 이름
            orderbook_manager,        # 객체만 전달 (start, run, initialize 등의 메서드를 자동 탐색)
            description="오더북 수집기" # 설명
        )
        _logger.info("오더북 수집기가 등록되었습니다.")
        
        # 초기 시작이 필요한 프로세스 시작
        await orchestrator.start_process("ob_collector", "시스템 시작 시 자동 시작")
        
        # 시스템 시작 로깅 (알림 제거)
        _logger.info("🚀 크로스킴프 시스템이 시작되었습니다.")
        
        _logger.info("========== 시스템 초기화 완료 ==========")
        return True
        
    except Exception as e:
        _logger.error(f"시스템 초기화 중 오류 발생: {str(e)}")
        _logger.error(traceback.format_exc())
        
        # 시작 실패 로깅 (알림 제거)
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
    """
    global _logger
    
    try:
        # 로깅 설정 (여기서 먼저 초기화)
        initialize_logging()
        _logger = get_unified_logger()
        _logger.info("========== 시스템 시작 ==========")
        
        # 신호 핸들러 설정
        setup_signal_handlers()
        
        # 시스템 초기화
        init_success = await init_system()
        if not init_success:
            _logger.error("시스템 초기화 실패로 종료합니다.")
            return 1
        
        # 메인 루프 실행
        await run_forever()
        
        return 0
        
    except Exception as e:
        if _logger:
            _logger.critical(f"치명적인 오류 발생: {str(e)}")
            _logger.critical(traceback.format_exc())
        else:
            print(f"치명적인 오류 발생: {str(e)}")
            print(traceback.format_exc())
        return 1
    finally:
        # 안전장치: 종료가 호출되지 않았다면 호출
        if not _is_shutting_down:
            await shutdown_system()

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
