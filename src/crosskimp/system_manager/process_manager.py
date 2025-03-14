"""
프로세스 관리 모듈 - 시스템 프로세스의 시작, 종료, 재시작을 관리합니다.
"""

import os
import sys
import signal
import asyncio
import logging
import subprocess
from typing import Dict, Optional, List, Tuple

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.utils.config.constants import LOG_SYSTEM

# 로거 설정
logger = get_unified_logger()

# 프로세스 정보를 저장할 딕셔너리
PROCESS_INFO = {
    "ob_collector": {
        "module": "crosskimp.ob_collector.main",
        "description": "OrderBook Collector",
        "env_vars": {"CROSSKIMP_ENV": "production"}
    },
    "radar": {
        "module": "crosskimp.radar.main",
        "description": "Radar Service",
        "env_vars": {"CROSSKIMP_ENV": "production"}
    }
    # 추가 프로세스는 여기에 정의
}

# 실행 중인 프로세스를 추적하는 딕셔너리
running_processes: Dict[str, subprocess.Popen] = {}

async def start_process(process_name: str, env_vars: Optional[Dict[str, str]] = None) -> bool:
    """
    지정된 프로세스를 시작합니다.
    
    Args:
        process_name: 시작할 프로세스 이름 (PROCESS_INFO에 정의된 키)
        env_vars: 추가 환경 변수 (기본 환경 변수를 덮어씁니다)
        
    Returns:
        bool: 프로세스 시작 성공 여부
    """
    if process_name not in PROCESS_INFO:
        logger.error(f"{LOG_SYSTEM} 알 수 없는 프로세스: {process_name}")
        return False
        
    if process_name in running_processes and running_processes[process_name].poll() is None:
        logger.warning(f"{LOG_SYSTEM} {process_name}이(가) 이미 실행 중입니다.")
        return True
        
    process_info = PROCESS_INFO[process_name]
    module_path = process_info["module"]
    
    # 환경 변수 설정
    process_env = os.environ.copy()
    if process_info.get("env_vars"):
        process_env.update(process_info["env_vars"])
    if env_vars:
        process_env.update(env_vars)
    
    try:
        # 프로세스 시작
        cmd = [sys.executable, "-m", module_path]
        process = subprocess.Popen(
            cmd,
            env=process_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        running_processes[process_name] = process
        logger.info(f"{LOG_SYSTEM} {process_info['description']} 시작됨 (PID: {process.pid})")
        
        # 비동기로 출력 로깅
        asyncio.create_task(_log_process_output(process_name, process))
        
        return True
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} {process_info['description']} 시작 실패: {str(e)}")
        return False

async def stop_process(process_name: str, timeout: int = 5) -> bool:
    """
    지정된 프로세스를 중지합니다.
    
    Args:
        process_name: 중지할 프로세스 이름
        timeout: 프로세스 종료 대기 시간(초)
        
    Returns:
        bool: 프로세스 중지 성공 여부
    """
    if process_name not in running_processes:
        logger.warning(f"{LOG_SYSTEM} {process_name}이(가) 실행 중이지 않습니다.")
        return True
        
    process = running_processes[process_name]
    if process.poll() is not None:
        # 이미 종료됨
        del running_processes[process_name]
        return True
    
    process_info = PROCESS_INFO.get(process_name, {"description": process_name})
    
    try:
        # 먼저 SIGTERM으로 정상 종료 시도
        process.terminate()
        
        # 지정된 시간 동안 종료 대기
        for _ in range(timeout):
            if process.poll() is not None:
                logger.info(f"{LOG_SYSTEM} {process_info['description']} 정상 종료됨")
                del running_processes[process_name]
                return True
            await asyncio.sleep(1)
        
        # 여전히 실행 중이면 SIGKILL로 강제 종료
        process.kill()
        await asyncio.sleep(1)
        
        if process.poll() is not None:
            logger.warning(f"{LOG_SYSTEM} {process_info['description']} 강제 종료됨")
            del running_processes[process_name]
            return True
        else:
            logger.error(f"{LOG_SYSTEM} {process_info['description']} 종료 실패")
            return False
            
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} {process_info['description']} 종료 중 오류: {str(e)}")
        return False

async def restart_process(process_name: str) -> bool:
    """
    지정된 프로세스를 재시작합니다.
    
    Args:
        process_name: 재시작할 프로세스 이름
        
    Returns:
        bool: 프로세스 재시작 성공 여부
    """
    # 먼저 프로세스 중지
    stop_success = await stop_process(process_name)
    if not stop_success:
        logger.error(f"{LOG_SYSTEM} {process_name} 중지 실패, 재시작 중단")
        return False
    
    # 잠시 대기 후 재시작
    await asyncio.sleep(2)
    
    # 프로세스 시작
    return await start_process(process_name)

async def get_process_status() -> Dict[str, Dict]:
    """
    모든 프로세스의 상태 정보를 반환합니다.
    
    Returns:
        Dict: 프로세스 이름을 키로 하고 상태 정보를 값으로 하는 딕셔너리
    """
    status = {}
    
    for name, info in PROCESS_INFO.items():
        process_status = {
            "description": info["description"],
            "running": False,
            "pid": None,
            "uptime": None
        }
        
        if name in running_processes:
            process = running_processes[name]
            if process.poll() is None:  # 실행 중
                process_status["running"] = True
                process_status["pid"] = process.pid
                # 업타임 정보는 추후 구현
            
        status[name] = process_status
    
    return status

async def _log_process_output(process_name: str, process: subprocess.Popen) -> None:
    """
    프로세스의 출력을 로깅합니다.
    
    Args:
        process_name: 프로세스 이름
        process: 프로세스 객체
    """
    process_info = PROCESS_INFO.get(process_name, {"description": process_name})
    
    # stdout 로깅
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        if line:
            logger.info(f"[{process_info['description']}] {line.strip()}")
    
    # 프로세스가 종료되면 상태 확인
    return_code = process.poll()
    if return_code != 0:
        stderr_output = process.stderr.read()
        logger.error(f"{LOG_SYSTEM} {process_info['description']} 비정상 종료 (코드: {return_code})")
        if stderr_output:
            logger.error(f"{LOG_SYSTEM} 오류 출력: {stderr_output}")
    
    # 종료된 프로세스 정리
    if process_name in running_processes and running_processes[process_name] == process:
        del running_processes[process_name] 