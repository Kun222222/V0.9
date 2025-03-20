"""
스케줄러 모듈 - 시스템 작업 예약 및 시간 관련 유틸리티를 제공합니다.
"""

import os
import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Callable, Awaitable, Any, List

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import LOG_SYSTEM
from crosskimp.system_manager.process_manager import restart_process

# 로거 설정
logger = get_unified_logger()

# 예약된 작업을 저장할 딕셔너리
scheduled_tasks: Dict[str, asyncio.Task] = {}

def calculate_next_midnight() -> datetime:
    """
    다음 자정 시간을 계산하여 반환합니다.
    
    Returns:
        datetime: 다음 자정 시간
    """
    now = datetime.now()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return tomorrow

def calculate_next_time(hour: int = 0, minute: int = 0, second: int = 0) -> datetime:
    """
    다음 특정 시간을 계산하여 반환합니다.
    
    Args:
        hour: 시간 (0-23)
        minute: 분 (0-59)
        second: 초 (0-59)
        
    Returns:
        datetime: 다음 특정 시간
    """
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=second, microsecond=0)
    
    # 이미 지난 시간이면 다음 날로 설정
    if target <= now:
        target += timedelta(days=1)
        
    return target

def format_remaining_time(target_time: datetime) -> str:
    """
    목표 시간까지 남은 시간을 HH:MM:SS 형식으로 포맷팅합니다.
    
    Args:
        target_time: 목표 시간
        
    Returns:
        str: 포맷팅된 남은 시간 문자열
    """
    now = datetime.now()
    remaining_seconds = int((target_time - now).total_seconds())
    
    if remaining_seconds < 0:
        return "00:00:00"
        
    hours = remaining_seconds // 3600
    minutes = (remaining_seconds % 3600) // 60
    seconds = remaining_seconds % 60
    
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

async def schedule_task(
    task_id: str,
    target_time: datetime,
    callback: Callable[..., Awaitable[Any]],
    *args: Any,
    **kwargs: Any
) -> bool:
    """
    지정된 시간에 작업을 예약합니다.
    
    Args:
        task_id: 작업 식별자
        target_time: 작업 실행 시간
        callback: 실행할 비동기 콜백 함수
        *args, **kwargs: 콜백 함수에 전달할 인자
        
    Returns:
        bool: 작업 예약 성공 여부
    """
    if task_id in scheduled_tasks and not scheduled_tasks[task_id].done():
        logger.warning(f"{LOG_SYSTEM} 작업 ID '{task_id}'가 이미 예약되어 있습니다.")
        return False
        
    async def _scheduled_runner():
        now = datetime.now()
        wait_seconds = (target_time - now).total_seconds()
        
        if wait_seconds > 0:
            logger.info(f"{LOG_SYSTEM} 작업 '{task_id}'이(가) {format_remaining_time(target_time)} 후에 실행됩니다.")
            await asyncio.sleep(wait_seconds)
            
        try:
            logger.info(f"{LOG_SYSTEM} 예약된 작업 '{task_id}' 실행 중...")
            await callback(*args, **kwargs)
            logger.info(f"{LOG_SYSTEM} 작업 '{task_id}' 완료됨")
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 작업 '{task_id}' 실행 중 오류 발생: {str(e)}")
        finally:
            if task_id in scheduled_tasks:
                del scheduled_tasks[task_id]
    
    # 작업 예약
    task = asyncio.create_task(_scheduled_runner())
    scheduled_tasks[task_id] = task
    
    return True

async def cancel_task(task_id: str) -> bool:
    """
    예약된 작업을 취소합니다.
    
    Args:
        task_id: 취소할 작업 식별자
        
    Returns:
        bool: 작업 취소 성공 여부
    """
    if task_id not in scheduled_tasks:
        logger.warning(f"{LOG_SYSTEM} 작업 ID '{task_id}'를 찾을 수 없습니다.")
        return False
        
    task = scheduled_tasks[task_id]
    if task.done():
        logger.warning(f"{LOG_SYSTEM} 작업 '{task_id}'이(가) 이미 완료되었습니다.")
        del scheduled_tasks[task_id]
        return True
        
    try:
        task.cancel()
        logger.info(f"{LOG_SYSTEM} 작업 '{task_id}' 취소됨")
        del scheduled_tasks[task_id]
        return True
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 작업 '{task_id}' 취소 중 오류 발생: {str(e)}")
        return False

async def schedule_daily_restart(process_name: str, hour: int = 0, minute: int = 0) -> str:
    """
    매일 지정된 시간에 프로세스를 재시작하도록 예약합니다.
    
    Args:
        process_name: 재시작할 프로세스 이름
        hour: 시간 (0-23)
        minute: 분 (0-59)
        
    Returns:
        str: 예약된 작업 ID
    """
    target_time = calculate_next_time(hour, minute)
    task_id = f"daily_restart_{process_name}_{hour:02d}_{minute:02d}"
    
    success = await schedule_task(
        task_id,
        target_time,
        _daily_restart_handler,
        process_name,
        hour,
        minute
    )
    
    if success:
        logger.info(f"{LOG_SYSTEM} {process_name} 매일 {hour:02d}:{minute:02d}에 재시작 예약됨")
    
    return task_id

async def _daily_restart_handler(process_name: str, hour: int, minute: int) -> None:
    """
    일일 재시작 핸들러 - 프로세스를 재시작하고 다음 날 같은 시간에 다시 예약합니다.
    
    Args:
        process_name: 재시작할 프로세스 이름
        hour: 시간 (0-23)
        minute: 분 (0-59)
    """
    try:
        # 프로세스 재시작
        logger.info(f"{LOG_SYSTEM} 예약된 재시작: {process_name}")
        await restart_process(process_name)
        
        # 다음 날 같은 시간에 다시 예약
        await schedule_daily_restart(process_name, hour, minute)
        
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 일일 재시작 중 오류 발생: {str(e)}")

async def get_scheduled_tasks() -> List[Dict[str, Any]]:
    """
    현재 예약된 모든 작업의 정보를 반환합니다.
    
    Returns:
        List[Dict]: 예약된 작업 정보 목록
    """
    tasks_info = []
    
    for task_id, task in scheduled_tasks.items():
        # 작업 ID에서 정보 추출 (예: daily_restart_ob_collector_00_00)
        parts = task_id.split('_')
        task_type = parts[0] if len(parts) > 0 else "unknown"
        
        task_info = {
            "id": task_id,
            "type": task_type,
            "status": "running" if not task.done() else "completed",
            "cancelled": task.cancelled()
        }
        
        tasks_info.append(task_info)
    
    return tasks_info 