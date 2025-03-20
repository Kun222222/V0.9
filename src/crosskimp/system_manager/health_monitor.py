"""
상태 모니터링 모듈 - 시스템 및 프로세스 상태를 모니터링합니다.
"""

import os
import psutil
import asyncio
import platform
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import LOG_SYSTEM
from crosskimp.system_manager.process_manager import running_processes, PROCESS_INFO, restart_process

# 로거 설정
logger = get_unified_logger()

# 모니터링 상태를 저장할 딕셔너리
monitoring_status = {
    "is_monitoring": False,
    "monitor_task": None,
    "last_check": None,
    "check_interval": 60,  # 초 단위
    "alert_threshold": {
        "cpu": 80.0,  # CPU 사용률 임계값 (%)
        "memory": 80.0,  # 메모리 사용률 임계값 (%)
        "disk": 90.0  # 디스크 사용률 임계값 (%)
    }
}

async def get_system_info() -> Dict[str, Any]:
    """
    시스템 정보를 수집합니다.
    
    Returns:
        Dict: 시스템 정보
    """
    try:
        # CPU 정보
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count(logical=True)
        
        # 메모리 정보
        memory = psutil.virtual_memory()
        memory_total = memory.total / (1024 * 1024 * 1024)  # GB
        memory_used = memory.used / (1024 * 1024 * 1024)  # GB
        memory_percent = memory.percent
        
        # 디스크 정보
        disk = psutil.disk_usage('/')
        disk_total = disk.total / (1024 * 1024 * 1024)  # GB
        disk_used = disk.used / (1024 * 1024 * 1024)  # GB
        disk_percent = disk.percent
        
        # 시스템 정보
        boot_time = datetime.fromtimestamp(psutil.boot_time())
        uptime = datetime.now() - boot_time
        uptime_str = str(uptime).split('.')[0]  # 마이크로초 제거
        
        return {
            "hostname": platform.node(),
            "platform": platform.system(),
            "platform_version": platform.version(),
            "python_version": platform.python_version(),
            "cpu": {
                "percent": cpu_percent,
                "count": cpu_count
            },
            "memory": {
                "total_gb": round(memory_total, 2),
                "used_gb": round(memory_used, 2),
                "percent": memory_percent
            },
            "disk": {
                "total_gb": round(disk_total, 2),
                "used_gb": round(disk_used, 2),
                "percent": disk_percent
            },
            "uptime": uptime_str,
            "boot_time": boot_time.strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 시스템 정보 수집 중 오류: {str(e)}")
        return {"error": str(e)}

async def get_process_info(process_name: Optional[str] = None) -> Dict[str, Any]:
    """
    프로세스 정보를 수집합니다.
    
    Args:
        process_name: 특정 프로세스 이름 (None이면 모든 관리 대상 프로세스)
        
    Returns:
        Dict: 프로세스 정보
    """
    result = {}
    
    try:
        process_list = [process_name] if process_name else running_processes.keys()
        
        for name in process_list:
            if name not in running_processes:
                result[name] = {"status": "not_running"}
                continue
                
            process = running_processes[name]
            if process.poll() is not None:
                result[name] = {"status": "terminated", "return_code": process.poll()}
                continue
                
            # psutil을 사용하여 자세한 프로세스 정보 수집
            try:
                p = psutil.Process(process.pid)
                
                # 프로세스 정보
                cpu_percent = p.cpu_percent(interval=0.1)
                memory_info = p.memory_info()
                memory_mb = memory_info.rss / (1024 * 1024)  # MB
                
                # 프로세스 시작 시간
                create_time = datetime.fromtimestamp(p.create_time())
                uptime = datetime.now() - create_time
                uptime_str = str(uptime).split('.')[0]  # 마이크로초 제거
                
                result[name] = {
                    "status": "running",
                    "pid": process.pid,
                    "cpu_percent": cpu_percent,
                    "memory_mb": round(memory_mb, 2),
                    "start_time": create_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "uptime": uptime_str,
                    "description": PROCESS_INFO.get(name, {}).get("description", name)
                }
            except psutil.NoSuchProcess:
                result[name] = {"status": "not_found", "pid": process.pid}
            except Exception as e:
                result[name] = {"status": "error", "error": str(e)}
    
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 프로세스 정보 수집 중 오류: {str(e)}")
        return {"error": str(e)}
        
    return result

async def start_monitoring(interval: int = 60) -> bool:
    """
    시스템 및 프로세스 모니터링을 시작합니다.
    
    Args:
        interval: 모니터링 간격 (초)
        
    Returns:
        bool: 모니터링 시작 성공 여부
    """
    if monitoring_status["is_monitoring"]:
        logger.warning(f"{LOG_SYSTEM} 모니터링이 이미 실행 중입니다.")
        return False
        
    monitoring_status["check_interval"] = interval
    monitoring_status["is_monitoring"] = True
    
    # 모니터링 태스크 시작
    monitoring_task = asyncio.create_task(_monitoring_loop())
    monitoring_status["monitor_task"] = monitoring_task
    
    logger.info(f"{LOG_SYSTEM} 시스템 모니터링 시작됨 (간격: {interval}초)")
    return True

async def stop_monitoring() -> bool:
    """
    시스템 모니터링을 중지합니다.
    
    Returns:
        bool: 모니터링 중지 성공 여부
    """
    if not monitoring_status["is_monitoring"]:
        logger.warning(f"{LOG_SYSTEM} 모니터링이 실행 중이지 않습니다.")
        return False
        
    # 모니터링 태스크 취소
    if monitoring_status["monitor_task"] and not monitoring_status["monitor_task"].done():
        monitoring_status["monitor_task"].cancel()
        
    monitoring_status["is_monitoring"] = False
    monitoring_status["monitor_task"] = None
    
    logger.info(f"{LOG_SYSTEM} 시스템 모니터링 중지됨")
    return True

async def _monitoring_loop() -> None:
    """
    모니터링 루프 - 주기적으로 시스템 및 프로세스 상태를 확인합니다.
    """
    try:
        while monitoring_status["is_monitoring"]:
            # 시스템 및 프로세스 상태 확인
            system_info = await get_system_info()
            process_info = await get_process_info()
            
            # 상태 업데이트
            monitoring_status["last_check"] = datetime.now()
            
            # 임계값 확인 및 경고
            await _check_thresholds(system_info, process_info)
            
            # 프로세스 상태 확인 및 필요시 재시작
            await _check_process_health(process_info)
            
            # 다음 체크까지 대기
            await asyncio.sleep(monitoring_status["check_interval"])
            
    except asyncio.CancelledError:
        logger.info(f"{LOG_SYSTEM} 모니터링 루프가 취소되었습니다.")
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 모니터링 루프 오류: {str(e)}")
        monitoring_status["is_monitoring"] = False

async def _check_thresholds(system_info: Dict[str, Any], process_info: Dict[str, Any]) -> None:
    """
    시스템 및 프로세스 상태가 임계값을 초과하는지 확인합니다.
    
    Args:
        system_info: 시스템 정보
        process_info: 프로세스 정보
    """
    thresholds = monitoring_status["alert_threshold"]
    alerts = []
    
    # CPU 사용률 확인
    if system_info.get("cpu", {}).get("percent", 0) > thresholds["cpu"]:
        alerts.append(f"CPU 사용률 경고: {system_info['cpu']['percent']}% (임계값: {thresholds['cpu']}%)")
    
    # 메모리 사용률 확인
    if system_info.get("memory", {}).get("percent", 0) > thresholds["memory"]:
        alerts.append(f"메모리 사용률 경고: {system_info['memory']['percent']}% (임계값: {thresholds['memory']}%)")
    
    # 디스크 사용률 확인
    if system_info.get("disk", {}).get("percent", 0) > thresholds["disk"]:
        alerts.append(f"디스크 사용률 경고: {system_info['disk']['percent']}% (임계값: {thresholds['disk']}%)")
    
    # 경고 로깅
    for alert in alerts:
        logger.warning(f"{LOG_SYSTEM} {alert}")
    
    # 추후 알림 시스템 연동 (텔레그램 등)
    if alerts:
        # TODO: 텔레그램 알림 전송 구현
        pass

async def _check_process_health(process_info: Dict[str, Any]) -> None:
    """
    프로세스 상태를 확인하고 필요시 재시작합니다.
    
    Args:
        process_info: 프로세스 정보
    """
    for name, info in process_info.items():
        # 프로세스가 실행 중이 아니면 재시작 시도
        if info.get("status") != "running":
            logger.warning(f"{LOG_SYSTEM} 프로세스 '{name}'이(가) 실행 중이 아닙니다. 상태: {info.get('status')}")
            
            # 자동 재시작 로직 (추후 설정에 따라 활성화)
            # await restart_process(name)
            
    # 추후 프로세스별 상태 모니터링 추가 (메모리 누수, CPU 과다 사용 등) 