"""
프로세스 관리 모듈 - 시스템 프로세스의 시작, 종료, 재시작을 관리합니다.

개선된 버전: 이벤트 버스와 통합되어 프로세스 상태 변경 이벤트를 발행하고,
오류 관리자와 연동하여 프로세스 오류를 보고합니다.
"""

import os
import sys
import signal
import asyncio
import logging
import subprocess
import time
from typing import Dict, Optional, List, Tuple, Any

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.constants_v3 import LOG_SYSTEM
from crosskimp.common.events import (
    get_component_event_bus,
    Component,
    EventTypes,
    EventPriority
)

# 로거 설정
logger = get_unified_logger()

# 이벤트 버스 인스턴스
event_bus = None

# 초기화 완료 플래그
_initialized = False

# 프로세스 정보를 저장할 딕셔너리
PROCESS_INFO = {
    "ob_collector": {
        "module": "crosskimp.ob_collector.run_orderbook",
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

# 프로세스 시작 시간을 저장하는 딕셔너리
process_start_times: Dict[str, float] = {}

# 자동 재시작 설정
auto_restart_enabled: Dict[str, bool] = {}

async def initialize():
    """모듈 초기화 - 이벤트 버스 참조를 설정합니다."""
    global event_bus, _initialized
    
    if not _initialized:
        # 시스템 컴포넌트의 이벤트 버스 가져오기
        event_bus = get_component_event_bus(Component.SYSTEM)
        _initialized = True
        logger.info(f"{LOG_SYSTEM} 프로세스 관리자 초기화 완료")

async def start_process(process_name: str, env_vars: Optional[Dict[str, str]] = None) -> bool:
    """
    지정된 프로세스를 시작합니다.
    
    Args:
        process_name: 시작할 프로세스 이름 (PROCESS_INFO에 정의된 키)
        env_vars: 추가 환경 변수 (기본 환경 변수를 덮어씁니다)
        
    Returns:
        bool: 프로세스 시작 성공 여부
    """
    # 모듈 초기화 확인
    if not _initialized:
        await initialize()
        
    if process_name not in PROCESS_INFO:
        error_msg = f"알 수 없는 프로세스: {process_name}"
        logger.error(f"{LOG_SYSTEM} {error_msg}")
        
        # 오류 이벤트 발행
        if event_bus:
            await event_bus.publish(EventTypes.ERROR_EVENT, {
                "message": error_msg,
                "source": "process_manager",
                "category": "PROCESS",
                "severity": "ERROR",
                "timestamp": time.time()
            })
            
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
        process_start_times[process_name] = time.time()
        
        logger.info(f"{LOG_SYSTEM} {process_info['description']} 시작됨 (PID: {process.pid})")
        
        # 비동기로 출력 로깅
        asyncio.create_task(_log_process_output(process_name, process))
        
        # 프로세스 상태 이벤트 발행
        if event_bus:
            await event_bus.publish(EventTypes.PROCESS_STATUS, {
                "process_name": process_name,
                "status": "running",
                "pid": process.pid,
                "description": process_info["description"],
                "timestamp": time.time()
            })
        
        return True
    except Exception as e:
        error_msg = f"{process_info['description']} 시작 실패: {str(e)}"
        logger.error(f"{LOG_SYSTEM} {error_msg}")
        
        # 오류 이벤트 발행
        if event_bus:
            await event_bus.publish(EventTypes.ERROR_EVENT, {
                "message": error_msg,
                "source": "process_manager",
                "category": "PROCESS",
                "severity": "ERROR",
                "timestamp": time.time()
            })
            
        # 프로세스 상태 이벤트 발행
        if event_bus:
            await event_bus.publish(EventTypes.PROCESS_STATUS, {
                "process_name": process_name,
                "status": "error",
                "description": process_info["description"],
                "error": str(e),
                "timestamp": time.time()
            })
            
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
    # 모듈 초기화 확인
    if not _initialized:
        await initialize()
        
    if process_name not in running_processes:
        logger.warning(f"{LOG_SYSTEM} {process_name}이(가) 실행 중이지 않습니다.")
        return True
        
    process = running_processes[process_name]
    if process.poll() is not None:
        # 이미 종료됨
        del running_processes[process_name]
        if process_name in process_start_times:
            del process_start_times[process_name]
        
        # 프로세스 상태 이벤트 발행
        if event_bus:
            await event_bus.publish(EventTypes.PROCESS_STATUS, {
                "process_name": process_name,
                "status": "stopped",
                "description": PROCESS_INFO.get(process_name, {"description": process_name})["description"],
                "timestamp": time.time()
            })
            
        return True
    
    process_info = PROCESS_INFO.get(process_name, {"description": process_name})
    
    try:
        # 먼저 SIGTERM으로 정상 종료 시도
        process.terminate()
        
        # 프로세스 상태 이벤트 발행 (종료 중)
        if event_bus:
            await event_bus.publish(EventTypes.PROCESS_STATUS, {
                "process_name": process_name,
                "status": "stopping",
                "pid": process.pid,
                "description": process_info["description"],
                "timestamp": time.time()
            })
        
        # 지정된 시간 동안 종료 대기
        for _ in range(timeout):
            if process.poll() is not None:
                logger.info(f"{LOG_SYSTEM} {process_info['description']} 정상 종료됨")
                
                if process_name in running_processes:
                    del running_processes[process_name]
                if process_name in process_start_times:
                    del process_start_times[process_name]
                
                # 프로세스 상태 이벤트 발행 (종료됨)
                if event_bus:
                    await event_bus.publish(EventTypes.PROCESS_STATUS, {
                        "process_name": process_name,
                        "status": "stopped",
                        "description": process_info["description"],
                        "timestamp": time.time()
                    })
                    
                return True
            await asyncio.sleep(1)
        
        # 여전히 실행 중이면 SIGKILL로 강제 종료
        process.kill()
        await asyncio.sleep(1)
        
        if process.poll() is not None:
            logger.warning(f"{LOG_SYSTEM} {process_info['description']} 강제 종료됨")
            
            if process_name in running_processes:
                del running_processes[process_name]
            if process_name in process_start_times:
                del process_start_times[process_name]
            
            # 프로세스 상태 이벤트 발행 (강제 종료됨)
            if event_bus:
                await event_bus.publish(EventTypes.PROCESS_STATUS, {
                    "process_name": process_name,
                    "status": "killed",
                    "description": process_info["description"],
                    "timestamp": time.time()
                })
                
            return True
        else:
            error_msg = f"{process_info['description']} 종료 실패"
            logger.error(f"{LOG_SYSTEM} {error_msg}")
            
            # 오류 이벤트 발행
            if event_bus:
                await event_bus.publish(EventTypes.ERROR_EVENT, {
                    "message": error_msg,
                    "source": "process_manager",
                    "category": "PROCESS",
                    "severity": "ERROR",
                    "timestamp": time.time()
                })
                
            return False
            
    except Exception as e:
        error_msg = f"{process_info['description']} 종료 중 오류: {str(e)}"
        logger.error(f"{LOG_SYSTEM} {error_msg}")
        
        # 오류 이벤트 발행
        if event_bus:
            await event_bus.publish(EventTypes.ERROR_EVENT, {
                "message": error_msg,
                "source": "process_manager",
                "category": "PROCESS",
                "severity": "ERROR",
                "timestamp": time.time()
            })
            
        return False

async def restart_process(process_name: str) -> bool:
    """
    지정된 프로세스를 재시작합니다.
    
    Args:
        process_name: 재시작할 프로세스 이름
        
    Returns:
        bool: 프로세스 재시작 성공 여부
    """
    # 모듈 초기화 확인
    if not _initialized:
        await initialize()
        
    if process_name not in PROCESS_INFO:
        logger.error(f"{LOG_SYSTEM} 알 수 없는 프로세스: {process_name}")
        return False
    
    process_info = PROCESS_INFO[process_name]
    
    # 프로세스 상태 이벤트 발행 (재시작 중)
    if event_bus:
        await event_bus.publish(EventTypes.PROCESS_STATUS, {
            "process_name": process_name,
            "status": "restarting",
            "description": process_info["description"],
            "timestamp": time.time()
        })
    
    # 먼저 프로세스 중지
    stop_success = await stop_process(process_name)
    if not stop_success:
        error_msg = f"{process_name} 중지 실패, 재시작 중단"
        logger.error(f"{LOG_SYSTEM} {error_msg}")
        
        # 오류 이벤트 발행
        if event_bus:
            await event_bus.publish(EventTypes.ERROR_EVENT, {
                "message": error_msg,
                "source": "process_manager",
                "category": "PROCESS",
                "severity": "ERROR",
                "timestamp": time.time()
            })
            
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
    # 모듈 초기화 확인
    if not _initialized:
        await initialize()
        
    status = {}
    current_time = time.time()
    
    for name, info in PROCESS_INFO.items():
        process_status = {
            "description": info["description"],
            "running": False,
            "pid": None,
            "uptime": None,
            "auto_restart": auto_restart_enabled.get(name, False)
        }
        
        if name in running_processes:
            process = running_processes[name]
            if process.poll() is None:  # 실행 중
                process_status["running"] = True
                process_status["pid"] = process.pid
                
                # 업타임 계산
                start_time = process_start_times.get(name, current_time)
                uptime_seconds = current_time - start_time
                process_status["uptime"] = uptime_seconds
                process_status["start_time"] = start_time
            
        status[name] = process_status
    
    return status

async def publish_status_event() -> None:
    """현재 모든 프로세스의 상태 이벤트를 발행합니다."""
    # 모듈 초기화 확인
    if not _initialized:
        await initialize()
        
    if not event_bus:
        return
        
    status = await get_process_status()
    current_time = time.time()
    
    for name, process_status in status.items():
        status_value = "running" if process_status["running"] else "stopped"
        
        await event_bus.publish(EventTypes.PROCESS_STATUS, {
            "process_name": name,
            "status": status_value,
            "pid": process_status["pid"],
            "description": process_status["description"],
            "uptime": process_status["uptime"],
            "timestamp": current_time
        })

def set_auto_restart(process_name: str, enabled: bool) -> bool:
    """
    프로세스의 자동 재시작 설정을 변경합니다.
    
    Args:
        process_name: 프로세스 이름
        enabled: 자동 재시작 활성화 여부
        
    Returns:
        bool: 설정 변경 성공 여부
    """
    if process_name not in PROCESS_INFO:
        logger.error(f"{LOG_SYSTEM} 알 수 없는 프로세스: {process_name}")
        return False
        
    auto_restart_enabled[process_name] = enabled
    logger.info(f"{LOG_SYSTEM} {process_name} 자동 재시작 {enabled}로 설정됨")
    return True

async def _log_process_output(process_name: str, process: subprocess.Popen) -> None:
    """
    프로세스의 출력을 로깅하고 종료 시 이벤트를 발행합니다.
    
    Args:
        process_name: 프로세스 이름
        process: 프로세스 객체
    """
    # 모듈 초기화 확인
    if not _initialized:
        await initialize()
        
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
    process_description = process_info['description']
    current_time = time.time()
    
    if return_code != 0:
        stderr_output = process.stderr.read()
        error_msg = f"{process_description} 비정상 종료 (코드: {return_code})"
        logger.error(f"{LOG_SYSTEM} {error_msg}")
        
        if stderr_output:
            logger.error(f"{LOG_SYSTEM} 오류 출력: {stderr_output}")
            
        # 오류 이벤트 발행
        if event_bus:
            await event_bus.publish(EventTypes.ERROR_EVENT, {
                "message": error_msg,
                "source": "process_manager",
                "category": "PROCESS",
                "severity": "ERROR",
                "timestamp": current_time,
                "metadata": {"return_code": return_code, "stderr": stderr_output[:500] if stderr_output else None}
            })
    else:
        logger.info(f"{LOG_SYSTEM} {process_description} 정상 종료 (코드: {return_code})")
    
    # 종료된 프로세스 정리
    if process_name in running_processes and running_processes[process_name] == process:
        del running_processes[process_name]
        if process_name in process_start_times:
            del process_start_times[process_name]
    
    # 프로세스 상태 이벤트 발행
    if event_bus:
        status = "error" if return_code != 0 else "stopped"
        await event_bus.publish(EventTypes.PROCESS_STATUS, {
            "process_name": process_name,
            "status": status,
            "description": process_description,
            "return_code": return_code,
            "timestamp": current_time
        })
    
    # 자동 재시작이 활성화되어 있으면 재시작
    if process_name in auto_restart_enabled and auto_restart_enabled[process_name]:
        logger.info(f"{LOG_SYSTEM} {process_description} 자동 재시작 시도 중...")
        await asyncio.sleep(5)  # 5초 대기 후 재시작
        await start_process(process_name) 