"""
시스템 스케줄러 모듈

이 모듈은 작업 스케줄링 기능을 제공합니다.
"""

import asyncio
import logging
from datetime import datetime, timedelta, time
from typing import Dict, Callable, Any, Optional, List, Tuple, Union
import traceback

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.app_config import get_config

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
        logger.error(traceback.format_exc())
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

def schedule_daily_time_task(task_id: str, callback: Callable, hour: int = 0, minute: int = 0, second: int = 0) -> str:
    """
    매일 특정 시간에 실행되는 작업을 스케줄링합니다.
    
    Args:
        task_id: 작업 ID
        callback: 실행할 콜백 함수
        hour: 시간 (0-23)
        minute: 분 (0-59)
        second: 초 (0-59)
        
    Returns:
        str: 스케줄링된 작업 ID
    """
    # 목표 시간 객체 생성
    target_time = time(hour=hour, minute=minute, second=second)
    
    # 다음 실행 시간 계산
    next_run = calculate_next_time(target_time)
    delay = (next_run - datetime.now()).total_seconds()
    
    logger.info(f"일일 작업 스케줄링: {task_id}, 다음 실행 시간: {next_run.strftime('%Y-%m-%d %H:%M:%S')} (남은 시간: {format_remaining_time(next_run)})")
    
    async def daily_task():
        try:
            # 콜백 함수 실행
            logger.info(f"일일 예약 작업 실행: {task_id}")
            await callback()
        except Exception as e:
            logger.error(f"일일 작업 실행 중 오류 발생: {task_id}, {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            # 다음 날 같은 시간에 재실행하도록 재스케줄링
            next_day = datetime.now().date() + timedelta(days=1)
            next_target = datetime.combine(next_day, target_time)
            new_delay = (next_target - datetime.now()).total_seconds()
            
            if new_delay <= 0:
                # 시간 계산에 문제가 있는 경우, 1분 후 재시도
                new_delay = 60
                logger.warning(f"일일 작업 재스케줄링 시간 계산 오류, 1분 후 재시도: {task_id}")
                
            logger.info(f"일일 작업 재스케줄링: {task_id}, 다음 실행 시간: {next_target.strftime('%Y-%m-%d %H:%M:%S')} (남은 시간: {format_remaining_time(next_target)})")
            schedule_task(task_id, daily_task, delay=new_delay)
    
    # 최초 스케줄링
    return schedule_task(task_id, daily_task, delay=delay)

def schedule_interval_task(task_id: str, callback: Callable, interval_seconds: int) -> str:
    """
    일정 간격으로 실행되는 작업을 스케줄링합니다.
    
    Args:
        task_id: 작업 ID
        callback: 실행할 콜백 함수
        interval_seconds: 실행 간격 (초)
        
    Returns:
        str: 스케줄링된 작업 ID
    """
    logger.info(f"주기적 작업 스케줄링: {task_id}, 간격: {interval_seconds}초")
    return schedule_task(task_id, callback, interval=interval_seconds)

def schedule_orderbook_restart(callback: Callable) -> str:
    """
    설정 파일의 시간에 따라 오더북 수집기 재시작을 스케줄링합니다.
    
    Args:
        callback: 재시작을 실행할 콜백 함수
        
    Returns:
        str: 스케줄링된 작업 ID
    """
    config = get_config()
    
    # 설정에서 재시작 시간 가져오기
    hour = config.get_system("scheduling.orderbook_restart_hour", 0)
    minute = config.get_system("scheduling.orderbook_restart_minute", 0)
    second = config.get_system("scheduling.orderbook_restart_second", 0)
    
    # 유효성 검사
    hour = max(0, min(23, int(hour)))
    minute = max(0, min(59, int(minute)))
    second = max(0, min(59, int(second)))
    
    task_id = "orderbook_daily_restart"
    logger.info(f"오더북 수집기 재시작 스케줄링: 매일 {hour:02d}:{minute:02d}:{second:02d}")
    
    return schedule_daily_time_task(task_id, callback, hour, minute, second)

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
        
        if task_id == "orderbook_daily_restart":
            task_info["type"] = "daily_restart"
            task_info["process"] = "ob_collector"
        else:
            task_info["type"] = "general"
            
        result.append(task_info)
        
    return result

class Scheduler:
    """스케줄러 클래스"""
    
    def __init__(self):
        """스케줄러 초기화"""
        self.initialized = False
        self.running = False
        self.tasks = {}
        self._logger = logger
        
    async def initialize(self):
        """스케줄러 초기화"""
        if self.initialized:
            return
            
        self.initialized = True
        self._logger.info("스케줄러가 초기화되었습니다.")
        
    async def start(self):
        """스케줄러 시작"""
        if not self.initialized:
            await self.initialize()
            
        if self.running:
            self._logger.warning("스케줄러가 이미 실행 중입니다.")
            return
            
        self.running = True
        
        # 기본 작업 등록
        self._register_default_tasks()
        
        self._logger.info("스케줄러가 시작되었습니다.")
        
    async def stop(self):
        """스케줄러 중지"""
        if not self.running:
            return
            
        # 모든 작업 취소
        for task_id in list(_scheduled_tasks.keys()):
            cancel_task(task_id)
            
        self.running = False
        self._logger.info("스케줄러가 중지되었습니다.")
        
    def _register_default_tasks(self):
        """기본 작업 등록"""
        # 오더북 재시작 작업
        schedule_task("restart_orderbook", self._noop_task, interval=3600)  # 예시: 1시간마다
        schedule_task("restart_telegram", self._noop_task, interval=3600 * 12)  # 예시: 12시간마다
        schedule_task("restart_monitoring", self._noop_task, interval=3600 * 24)  # 예시: 24시간마다
        schedule_task("restart_data_collector", self._noop_task, interval=3600 * 6)  # 예시: 6시간마다
        schedule_task("restart_trade_executor", self._noop_task, interval=3600 * 8)  # 예시: 8시간마다
        
        for task_id in _scheduled_tasks:
            self._logger.info(f"작업이 추가되었습니다: {task_id}")
    
    async def _noop_task(self):
        """아무 작업도 수행하지 않는 작업 (임시)"""
        pass 