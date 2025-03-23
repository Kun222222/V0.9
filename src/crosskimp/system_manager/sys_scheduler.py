"""
시스템 스케줄러 모듈

이 모듈은 작업 스케줄링 기능을 제공합니다.
"""

import asyncio
import logging
from datetime import datetime, timedelta, time
from typing import Dict, Callable, Any, Optional, List, Tuple, Union

from crosskimp.common.logger.logger import get_unified_logger

# 로거 설정
logger = get_unified_logger()

# 글로벌 스케줄러 작업 보관 (작업 ID -> 작업 태스크)
_scheduled_tasks: Dict[str, asyncio.Task] = {}

def calculate_next_midnight() -> datetime:
    """
    다음 자정(00:00:00) 시간을 계산합니다.
    
    Returns:
        datetime: 다음 자정 시간
    """
    now = datetime.now()
    tomorrow = now.date() + timedelta(days=1)
    return datetime.combine(tomorrow, time())

def calculate_next_time(target_time: time) -> datetime:
    """
    지정된 시간의 다음 발생 시간을 계산합니다.
    오늘 해당 시간이 지났으면 내일 해당 시간을 반환합니다.
    
    Args:
        target_time: 목표 시간 (시, 분, 초)
        
    Returns:
        datetime: 다음 목표 시간
    """
    now = datetime.now()
    target_datetime = datetime.combine(now.date(), target_time)
    
    # 이미 오늘의 목표 시간이 지났으면 내일로 설정
    if target_datetime <= now:
        target_datetime += timedelta(days=1)
        
    return target_datetime

def format_remaining_time(target_time: datetime) -> str:
    """
    목표 시간까지 남은 시간을 포맷팅합니다.
    
    Args:
        target_time: 목표 시간
        
    Returns:
        str: "X시간 Y분 Z초" 형식의 문자열
    """
    now = datetime.now()
    remaining = target_time - now
    
    # 음수 시간은 0으로 처리
    if remaining.total_seconds() < 0:
        return "0시간 0분 0초"
    
    hours, remainder = divmod(int(remaining.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    return f"{hours}시간 {minutes}분 {seconds}초"

async def _run_scheduled_task(task_id: str, func: Callable, args: Any = None, 
                             kwargs: Any = None, interval: Optional[float] = None):
    """
    스케줄된 작업을 실행하는 내부 함수
    
    Args:
        task_id: 작업 ID
        func: 실행할 함수
        args: 함수 인자
        kwargs: 함수 키워드 인자
        interval: 반복 간격 (초), None이면 한 번만 실행
    """
    args = args or ()
    kwargs = kwargs or {}
    
    try:
        if interval is None:
            # 단일 실행 작업
            await func(*args, **kwargs)
            logger.info(f"스케줄된 작업 실행 완료: {task_id}")
        else:
            # 반복 작업
            while True:
                await func(*args, **kwargs)
                logger.debug(f"반복 작업 실행: {task_id}, 다음 실행까지 {interval}초")
                await asyncio.sleep(interval)
                
    except asyncio.CancelledError:
        logger.info(f"스케줄된 작업이 취소됨: {task_id}")
        raise
    except Exception as e:
        logger.error(f"스케줄된 작업 실행 중 오류: {task_id}, {str(e)}")
    finally:
        # 작업 완료 시 딕셔너리에서 제거
        if task_id in _scheduled_tasks:
            del _scheduled_tasks[task_id]

def schedule_task(task_id: str, func: Callable, args: Any = None, kwargs: Any = None, 
                 delay: Optional[float] = None, interval: Optional[float] = None) -> str:
    """
    작업을 스케줄링합니다.
    
    Args:
        task_id: 작업 ID (없으면 자동 생성)
        func: 실행할 함수
        args: 함수 인자
        kwargs: 함수 키워드 인자
        delay: 실행 지연 시간 (초)
        interval: 반복 간격 (초), None이면 한 번만 실행
        
    Returns:
        str: 작업 ID
    """
    # 이미 존재하는 작업 ID면 기존 작업 취소
    if task_id in _scheduled_tasks:
        logger.warning(f"이미 존재하는 작업 ID: {task_id}, 기존 작업을 취소합니다.")
        _scheduled_tasks[task_id].cancel()
    
    async def delayed_task():
        if delay:
            logger.debug(f"작업 지연 중: {task_id}, {delay}초")
            await asyncio.sleep(delay)
        await _run_scheduled_task(task_id, func, args, kwargs, interval)
    
    # 작업 생성 및 저장
    task = asyncio.create_task(delayed_task())
    _scheduled_tasks[task_id] = task
    
    # 작업 메타데이터 기록
    logger.info(f"작업 스케줄링: {task_id}" + 
               (f", 지연: {delay}초" if delay else "") + 
               (f", 간격: {interval}초" if interval else ""))
    
    return task_id

def cancel_task(task_id: str) -> bool:
    """
    스케줄된 작업을 취소합니다.
    
    Args:
        task_id: 취소할 작업 ID
        
    Returns:
        bool: 취소 성공 여부
    """
    if task_id in _scheduled_tasks:
        task = _scheduled_tasks[task_id]
        if not task.done():
            task.cancel()
        del _scheduled_tasks[task_id]
        logger.info(f"작업이 취소됨: {task_id}")
        return True
    else:
        logger.warning(f"취소할 작업을 찾을 수 없음: {task_id}")
        return False

def schedule_daily_restart(process_name: str, target_time: time) -> str:
    """
    프로세스의 일일 재시작을 스케줄링합니다.
    
    Args:
        process_name: 재시작할 프로세스 이름
        target_time: 재시작 시간 (시, 분, 초)
        
    Returns:
        str: 스케줄링된 작업 ID
    """
    from crosskimp.system_manager.process_manager import restart_process
    
    # 다음 실행 시간 계산
    next_run = calculate_next_time(target_time)
    delay = (next_run - datetime.now()).total_seconds()
    
    # 24시간마다 실행되는 작업 스케줄링
    task_id = f"daily_restart_{process_name}"
    
    async def daily_restart():
        # 프로세스 재시작
        logger.info(f"{process_name} 일일 재시작 실행")
        await restart_process(process_name, reason="일일 정기 재시작")
        
        # 다음 날 같은 시간에 재시작하도록 재스케줄링
        next_day = datetime.now().date() + timedelta(days=1)
        next_restart = datetime.combine(next_day, target_time)
        new_delay = (next_restart - datetime.now()).total_seconds()
        schedule_task(task_id, daily_restart, delay=new_delay)
    
    return schedule_task(task_id, daily_restart, delay=delay)

def get_scheduled_tasks() -> List[Dict[str, Any]]:
    """
    현재 스케줄된 모든 작업 목록을 반환합니다.
    
    Returns:
        List[Dict]: 작업 정보 목록
    """
    result = []
    
    for task_id, task in _scheduled_tasks.items():
        task_info = {
            "id": task_id,
            "is_done": task.done(),
            "is_cancelled": task.cancelled() if hasattr(task, 'cancelled') else False,
        }
        
        if "daily_restart" in task_id:
            # 일일 재시작 작업의 경우 추가 정보
            process_name = task_id.replace("daily_restart_", "")
            task_info["type"] = "daily_restart"
            task_info["process"] = process_name
        else:
            task_info["type"] = "general"
            
        result.append(task_info)
        
    return result 