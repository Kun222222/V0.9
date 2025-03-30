"""
텔레그램 봇 프로세스 관리 모듈

텔레그램 봇의 중복 실행을 방지하기 위한 프로세스 관리 기능을 제공합니다.
"""

import os
import asyncio
import psutil
from typing import List, Optional

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent

# 로거 설정
logger = get_unified_logger(component=SystemComponent.TELEGRAM.value)

def terminate_existing_telegram_bots() -> List[int]:
    """
    동일한 텔레그램 봇을 실행 중인 다른 프로세스를 종료합니다.
    
    원격 API 충돌 방지를 위해 현재 프로세스 이외의 
    다른 Python 프로세스 중 'telegram'을 포함하는 
    프로세스를 찾아 종료합니다.
    
    Returns:
        List[int]: 종료된 프로세스 ID 목록
    """
    current_pid = os.getpid()
    logger.info(f"현재 프로세스 ID: {current_pid}")
    
    terminated_pids = []
    
    try:
        # 모든 프로세스를 검색
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            # 현재 프로세스가 아니고, Python 프로세스이며, telegram 관련 문자열을 포함하는 경우
            if (proc.info['pid'] != current_pid and 
                proc.info['name'] and 'python' in proc.info['name'].lower() and 
                proc.info['cmdline'] and any('telegram' in cmd.lower() for cmd in proc.info['cmdline'] if isinstance(cmd, str))):
                
                try:
                    pid = proc.info['pid']
                    
                    # 해당 프로세스 종료
                    logger.warning(f"중복된 텔레그램 봇 프로세스 발견: {pid} - 종료합니다.")
                    proc.terminate()
                    
                    # 정상 종료 대기 (3초)
                    gone, alive = psutil.wait_procs([proc], timeout=3)
                    
                    # 강제 종료 (필요시)
                    if alive:
                        logger.warning(f"프로세스 {pid}가 정상 종료되지 않아 강제 종료합니다.")
                        proc.kill()
                    
                    terminated_pids.append(pid)    
                    logger.info(f"텔레그램 봇 프로세스 {pid} 종료 완료")
                    
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
                    logger.error(f"프로세스 종료 중 오류: {str(e)}")
                    
    except Exception as e:
        logger.error(f"기존 텔레그램 봇 프로세스 종료 중 오류 발생: {str(e)}")
    
    return terminated_pids

async def ensure_single_telegram_bot_instance() -> bool:
    """
    텔레그램 봇의 단일 인스턴스 실행을 보장합니다.
    
    기존에 실행 중인 텔레그램 봇 프로세스를 종료하고,
    완전히 종료될 때까지 잠시 대기합니다.
    
    Returns:
        bool: 작업 성공 여부
    """
    try:
        logger.info("기존 텔레그램 봇 인스턴스를 확인하고 종료합니다...")
        
        # 기존 텔레그램 봇 프로세스 종료
        terminated_pids = terminate_existing_telegram_bots()
        
        if terminated_pids:
            # 프로세스가 완전히 종료될 시간을 줌
            logger.info(f"{len(terminated_pids)}개의 프로세스가 종료되었습니다. 종료 완료를 기다립니다...")
            await asyncio.sleep(1.5)
            
        return True
        
    except Exception as e:
        logger.error(f"텔레그램 봇 인스턴스 체크 중 오류: {str(e)}")
        return False 