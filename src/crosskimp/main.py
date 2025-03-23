"""
크로스 김프 시스템 컨트롤 타워 - 모든 서비스의 시작/중지 및 모니터링을 담당합니다.
"""

import os
import sys
import asyncio
import argparse
import logging
import signal
from typing import Dict, List, Optional, Any

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import LOG_SYSTEM
from crosskimp.common.events import (
    initialize_event_system, 
    shutdown_event_system, 
    EventTypes, 
    Component,
    publish_system_event
)

# 로거 설정
logger = get_unified_logger()

# 시스템 상태
system_running = False

# 메인 컨트롤러 클래스
class CrossKimpController:
    """크로스 김프 시스템 통합 컨트롤러"""
    
    def __init__(self):
        """컨트롤러를 초기화합니다."""
        self.bot_manager = None
        self.system_monitor = None
        
        # 실행 중인 작업 추적
        self.tasks = []
        
        logger.info(f"{LOG_SYSTEM} 크로스 김프 컨트롤러 초기화됨")
    
    async def initialize(self) -> None:
        """모든 서브시스템을 초기화합니다."""
        # 이벤트 시스템 초기화를 가장 먼저 실행
        await initialize_event_system()
        logger.info(f"{LOG_SYSTEM} 이벤트 시스템 초기화 완료")
        
        # 프로세스 관리자 초기화
        from crosskimp.system_manager.process_manager import initialize as init_process_manager
        await init_process_manager()
        logger.info(f"{LOG_SYSTEM} 프로세스 관리자 초기화 완료")
        
        # 텔레그램 봇 초기화
        from crosskimp.telegrambot import get_bot_manager
        self.bot_manager = get_bot_manager()
        logger.info(f"{LOG_SYSTEM} 텔레그램 봇 초기화 완료")
        
        # 시스템 모니터링 초기화
        from crosskimp.telegrambot.monitoring import SystemMonitor
        self.system_monitor = SystemMonitor(self.bot_manager.notification_service)
        logger.info(f"{LOG_SYSTEM} 시스템 모니터링 초기화 완료")
        
        logger.info(f"{LOG_SYSTEM} 모든 서브시스템 초기화 완료")
    
    async def start(self) -> None:
        """시스템을 시작합니다."""
        global system_running
        
        if system_running:
            logger.warning(f"{LOG_SYSTEM} 시스템이 이미 실행 중입니다.")
            return
        
        logger.info(f"{LOG_SYSTEM} 크로스 김프 시스템 시작 중...")
        
        try:
            # 텔레그램 봇 시작
            await self.bot_manager.start()
            
            # 시스템 모니터링 시작
            monitor_task = asyncio.create_task(self.system_monitor.start(interval=300))
            self.tasks.append(monitor_task)
            
            # 자동 재시작 설정
            from crosskimp.system_manager.process_manager import set_auto_restart
            # ob_collector에 대해 자동 재시작 활성화, radar는 비활성화
            set_auto_restart("ob_collector", True)
            set_auto_restart("radar", False)
            logger.info(f"{LOG_SYSTEM} 프로세스 자동 재시작 설정 완료")
            
            # 일일 작업 스케줄링
            await self._schedule_daily_tasks()
            
            # 프로세스 상태 주기적 업데이트 작업
            from crosskimp.system_manager.process_manager import publish_status_event
            status_update_task = asyncio.create_task(self._periodic_status_update(publish_status_event))
            self.tasks.append(status_update_task)
            
            system_running = True
            logger.info(f"{LOG_SYSTEM} 크로스 김프 시스템이 실행 중입니다.")
            
            # 시스템 시작 이벤트 발행
            await publish_system_event(EventTypes.SYSTEM_STARTUP, {
                "message": "크로스 김프 시스템이 시작되었습니다."
            })
            
            # 모든 프로세스 자동 시작 (옵션과 상관없이 항상 시작)
            await self.start_all_processes()
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 시스템 시작 중 오류 발생: {str(e)}")
            await self.stop()
    
    async def stop(self) -> None:
        """시스템을 중지합니다."""
        global system_running
        
        if not system_running:
            logger.warning(f"{LOG_SYSTEM} 시스템이 실행 중이지 않습니다.")
            return
        
        logger.info(f"{LOG_SYSTEM} 크로스 김프 시스템 종료 중...")
        
        try:
            # 시스템 종료 이벤트 발행
            await publish_system_event(EventTypes.SYSTEM_SHUTDOWN, {
                "message": "크로스 김프 시스템이 종료됩니다."
            })
            
            # 모든 작업 취소
            for task in self.tasks:
                if not task.done():
                    task.cancel()
            
            # 시스템 모니터링 중지
            if self.system_monitor:
                await self.system_monitor.stop()
            
            # 텔레그램 봇 중지
            if self.bot_manager:
                await self.bot_manager.stop()
            
            # 실행 중인 모든 프로세스 중지
            from crosskimp.system_manager.process_manager import get_process_status, stop_process
            status = await get_process_status()
            for name, info in status.items():
                if info["running"]:
                    logger.info(f"{LOG_SYSTEM} 프로세스 {name} 종료 중...")
                    await stop_process(name)
            
            # 이벤트 시스템 종료
            await shutdown_event_system()
            
            system_running = False
            logger.info(f"{LOG_SYSTEM} 크로스 김프 시스템이 정상적으로 종료되었습니다.")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 시스템 종료 중 오류 발생: {str(e)}")
            system_running = False
    
    async def _schedule_daily_tasks(self) -> None:
        """일일 작업을 스케줄링합니다."""
        try:
            from crosskimp.system_manager.scheduler import schedule_daily_restart, schedule_task, calculate_next_time
            
            # ob_collector 자정 재시작
            await schedule_daily_restart("ob_collector", 0, 0)
            logger.info(f"{LOG_SYSTEM} ob_collector 00:00 자동 재시작 예약됨")
            
            # radar 00:05 재시작 (자원 부하 분산)
            await schedule_daily_restart("radar", 0, 5)
            logger.info(f"{LOG_SYSTEM} radar 00:05 자동 재시작 예약됨")
            
            # 오전 9시 일일 보고서
            report_time = calculate_next_time(9, 0, 0)
            await schedule_task(
                "daily_report",
                report_time,
                self.system_monitor.send_daily_report
            )
            logger.info(f"{LOG_SYSTEM} 일일 보고서 09:00 예약됨")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 일일 작업 스케줄링 중 오류: {str(e)}")
    
    async def _periodic_status_update(self, publish_status_fn, interval: int = 60) -> None:
        """
        주기적으로 프로세스 상태를 업데이트합니다.
        
        Args:
            publish_status_fn: 상태 발행 함수
            interval: 업데이트 간격 (초)
        """
        while True:
            try:
                await publish_status_fn()
            except Exception as e:
                logger.error(f"{LOG_SYSTEM} 상태 업데이트 중 오류: {str(e)}")
            
            await asyncio.sleep(interval)
    
    async def start_all_processes(self) -> None:
        """모든 프로세스를 시작합니다."""
        logger.info(f"{LOG_SYSTEM} 모든 프로세스 시작 중...")
        
        # 필요할 때 import
        from crosskimp.system_manager.process_manager import start_process
        
        # ob_collector 시작
        logger.info(f"{LOG_SYSTEM} ob_collector 시작 중...")
        await start_process("ob_collector")
        
        # 약간의 지연 후 radar 시작
        await asyncio.sleep(5)
        logger.info(f"{LOG_SYSTEM} radar 시작 중...")
        await start_process("radar")
        
        logger.info(f"{LOG_SYSTEM} 모든 프로세스가 시작되었습니다.")

# 시그널 핸들러
async def signal_handler(controller: CrossKimpController) -> None:
    """
    시그널 핸들러 함수
    
    Args:
        controller: 컨트롤러 인스턴스
    """
    await controller.stop()
    logger.info(f"{LOG_SYSTEM} 프로그램 종료")

# CLI 인자 파서
def parse_arguments():
    """
    명령행 인자를 파싱합니다.
    
    Returns:
        argparse.Namespace: 파싱된 인자
    """
    parser = argparse.ArgumentParser(description="크로스 김프 시스템 컨트롤러")
    
    parser.add_argument(
        "--autostart",
        action="store_true",
        help="시작 시 모든 프로세스 자동 시작"
    )
    
    return parser.parse_args()

# 메인 함수
async def main() -> None:
    """메인 함수"""
    # 인자 파싱
    args = parse_arguments()
    
    # 컨트롤러 초기화
    controller = CrossKimpController()
    await controller.initialize()
    
    # 시그널 핸들러 등록
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(
            sig,
            lambda: asyncio.create_task(signal_handler(controller))
        )
    
    try:
        # 컨트롤러 시작 (이제 내부에서 모든 프로세스를 자동으로 시작합니다)
        await controller.start()
        
        # 프로그램이 종료되지 않도록 대기
        while system_running:
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 예기치 않은 오류: {str(e)}")
    finally:
        # 안전하게 종료
        await controller.stop()

# 프로그램 진입점
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n프로그램이 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"심각한 오류: {str(e)}")
        sys.exit(1) 