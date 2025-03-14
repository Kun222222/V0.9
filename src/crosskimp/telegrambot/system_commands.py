"""
텔레그램 시스템 명령어 모듈 - 시스템 관리를 위한 텔레그램 봇 명령어를 제공합니다.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Tuple

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.utils.config.constants import LOG_SYSTEM
from crosskimp.system_manager.process_manager import start_process, stop_process, restart_process, get_process_status, PROCESS_INFO
from crosskimp.system_manager.scheduler import schedule_daily_restart, cancel_task, get_scheduled_tasks, calculate_next_midnight, format_remaining_time
from crosskimp.system_manager.health_monitor import get_system_info, get_process_info, start_monitoring, stop_monitoring

# 로거 설정
logger = get_unified_logger()

# 명령어 접근 권한이 있는 사용자 ID 목록
AUTHORIZED_USERS = [
    # 여기에 허용된 텔레그램 사용자 ID 추가
]

async def handle_start_command(update, context):
    """
    /start 명령어 처리 - 시스템 관리 봇 소개 메시지를 표시합니다.
    """
    user_id = update.effective_user.id
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("⛔ 접근 권한이 없습니다.")
        logger.warning(f"{LOG_SYSTEM} 권한 없는 사용자의 접근 시도: {user_id}")
        return
    
    help_text = """
🤖 *CrossKimp 시스템 관리 봇*

다음 명령어를 사용하여 시스템을 관리할 수 있습니다:

*프로세스 관리*
/status - 모든 프로세스 상태 확인
/start\_process [이름] - 프로세스 시작
/stop\_process [이름] - 프로세스 중지
/restart\_process [이름] - 프로세스 재시작

*예약 작업*
/schedule\_restart [이름] [시간] [분] - 매일 지정된 시간에 재시작 예약
/list\_schedules - 예약된 작업 목록 확인
/cancel\_schedule [작업ID] - 예약된 작업 취소

*시스템 모니터링*
/system\_info - 시스템 정보 확인
/start\_monitoring - 시스템 모니터링 시작
/stop\_monitoring - 시스템 모니터링 중지

*도움말*
/help - 이 도움말 메시지 표시
    """
    
    await update.message.reply_text(help_text, parse_mode="Markdown")

async def handle_status_command(update, context):
    """
    /status 명령어 처리 - 모든 프로세스의 상태를 확인합니다.
    """
    user_id = update.effective_user.id
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("⛔ 접근 권한이 없습니다.")
        return
    
    await update.message.reply_text("🔍 프로세스 상태 확인 중...")
    
    try:
        # 프로세스 상태 확인
        process_status = await get_process_status()
        
        # 응답 메시지 생성
        status_text = "📊 *프로세스 상태*\n\n"
        
        for name, info in process_status.items():
            description = info.get("description", name)
            running = info.get("running", False)
            pid = info.get("pid")
            
            status_emoji = "✅" if running else "❌"
            pid_text = f"(PID: {pid})" if pid else ""
            
            status_text += f"{status_emoji} *{description}* {pid_text}\n"
            
        await update.message.reply_text(status_text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 상태 명령어 처리 중 오류: {str(e)}")
        await update.message.reply_text(f"❌ 오류 발생: {str(e)}")

async def handle_start_process_command(update, context):
    """
    /start_process [이름] 명령어 처리 - 지정된 프로세스를 시작합니다.
    """
    user_id = update.effective_user.id
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("⛔ 접근 권한이 없습니다.")
        return
    
    # 인자 확인
    if not context.args or len(context.args) < 1:
        await update.message.reply_text("❌ 프로세스 이름을 지정해주세요.\n사용법: /start_process [이름]")
        return
    
    process_name = context.args[0]
    
    # 프로세스 이름 유효성 검사
    if process_name not in PROCESS_INFO:
        valid_processes = ", ".join(PROCESS_INFO.keys())
        await update.message.reply_text(f"❌ 알 수 없는 프로세스: {process_name}\n유효한 프로세스: {valid_processes}")
        return
    
    await update.message.reply_text(f"🚀 {process_name} 시작 중...")
    
    try:
        # 프로세스 시작
        success = await start_process(process_name)
        
        if success:
            await update.message.reply_text(f"✅ {PROCESS_INFO[process_name]['description']} 시작됨")
        else:
            await update.message.reply_text(f"❌ {PROCESS_INFO[process_name]['description']} 시작 실패")
            
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 프로세스 시작 명령어 처리 중 오류: {str(e)}")
        await update.message.reply_text(f"❌ 오류 발생: {str(e)}")

async def handle_stop_process_command(update, context):
    """
    /stop_process [이름] 명령어 처리 - 지정된 프로세스를 중지합니다.
    """
    user_id = update.effective_user.id
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("⛔ 접근 권한이 없습니다.")
        return
    
    # 인자 확인
    if not context.args or len(context.args) < 1:
        await update.message.reply_text("❌ 프로세스 이름을 지정해주세요.\n사용법: /stop_process [이름]")
        return
    
    process_name = context.args[0]
    
    # 프로세스 이름 유효성 검사
    if process_name not in PROCESS_INFO:
        valid_processes = ", ".join(PROCESS_INFO.keys())
        await update.message.reply_text(f"❌ 알 수 없는 프로세스: {process_name}\n유효한 프로세스: {valid_processes}")
        return
    
    await update.message.reply_text(f"🛑 {process_name} 중지 중...")
    
    try:
        # 프로세스 중지
        success = await stop_process(process_name)
        
        if success:
            await update.message.reply_text(f"✅ {PROCESS_INFO[process_name]['description']} 중지됨")
        else:
            await update.message.reply_text(f"❌ {PROCESS_INFO[process_name]['description']} 중지 실패")
            
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 프로세스 중지 명령어 처리 중 오류: {str(e)}")
        await update.message.reply_text(f"❌ 오류 발생: {str(e)}")

async def handle_restart_process_command(update, context):
    """
    /restart_process [이름] 명령어 처리 - 지정된 프로세스를 재시작합니다.
    """
    user_id = update.effective_user.id
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("⛔ 접근 권한이 없습니다.")
        return
    
    # 인자 확인
    if not context.args or len(context.args) < 1:
        await update.message.reply_text("❌ 프로세스 이름을 지정해주세요.\n사용법: /restart_process [이름]")
        return
    
    process_name = context.args[0]
    
    # 프로세스 이름 유효성 검사
    if process_name not in PROCESS_INFO:
        valid_processes = ", ".join(PROCESS_INFO.keys())
        await update.message.reply_text(f"❌ 알 수 없는 프로세스: {process_name}\n유효한 프로세스: {valid_processes}")
        return
    
    await update.message.reply_text(f"🔄 {process_name} 재시작 중...")
    
    try:
        # 프로세스 재시작
        success = await restart_process(process_name)
        
        if success:
            await update.message.reply_text(f"✅ {PROCESS_INFO[process_name]['description']} 재시작됨")
        else:
            await update.message.reply_text(f"❌ {PROCESS_INFO[process_name]['description']} 재시작 실패")
            
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 프로세스 재시작 명령어 처리 중 오류: {str(e)}")
        await update.message.reply_text(f"❌ 오류 발생: {str(e)}")

async def handle_schedule_restart_command(update, context):
    """
    /schedule_restart [이름] [시간] [분] 명령어 처리 - 매일 지정된 시간에 프로세스를 재시작하도록 예약합니다.
    """
    user_id = update.effective_user.id
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("⛔ 접근 권한이 없습니다.")
        return
    
    # 인자 확인
    if not context.args or len(context.args) < 3:
        await update.message.reply_text("❌ 프로세스 이름과 시간을 지정해주세요.\n사용법: /schedule_restart [이름] [시간] [분]")
        return
    
    process_name = context.args[0]
    
    # 프로세스 이름 유효성 검사
    if process_name not in PROCESS_INFO:
        valid_processes = ", ".join(PROCESS_INFO.keys())
        await update.message.reply_text(f"❌ 알 수 없는 프로세스: {process_name}\n유효한 프로세스: {valid_processes}")
        return
    
    # 시간 파싱
    try:
        hour = int(context.args[1])
        minute = int(context.args[2])
        
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            await update.message.reply_text("❌ 유효하지 않은 시간 형식입니다. 시간은 0-23, 분은 0-59 범위여야 합니다.")
            return
            
    except ValueError:
        await update.message.reply_text("❌ 시간과 분은 숫자여야 합니다.")
        return
    
    await update.message.reply_text(f"🕒 {process_name} 매일 {hour:02d}:{minute:02d}에 재시작 예약 중...")
    
    try:
        # 재시작 예약
        task_id = await schedule_daily_restart(process_name, hour, minute)
        
        await update.message.reply_text(
            f"✅ {PROCESS_INFO[process_name]['description']} 매일 {hour:02d}:{minute:02d}에 재시작 예약됨\n"
            f"작업 ID: `{task_id}`",
            parse_mode="Markdown"
        )
            
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 재시작 예약 명령어 처리 중 오류: {str(e)}")
        await update.message.reply_text(f"❌ 오류 발생: {str(e)}")

async def handle_list_schedules_command(update, context):
    """
    /list_schedules 명령어 처리 - 예약된 작업 목록을 확인합니다.
    """
    user_id = update.effective_user.id
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("⛔ 접근 권한이 없습니다.")
        return
    
    await update.message.reply_text("🔍 예약된 작업 확인 중...")
    
    try:
        # 예약된 작업 목록 확인
        tasks = await get_scheduled_tasks()
        
        if not tasks:
            await update.message.reply_text("📅 예약된 작업이 없습니다.")
            return
            
        # 응답 메시지 생성
        tasks_text = "📅 *예약된 작업 목록*\n\n"
        
        for task in tasks:
            task_id = task.get("id", "unknown")
            task_type = task.get("type", "unknown")
            task_status = task.get("status", "unknown")
            
            status_emoji = "✅" if task_status == "running" else "❌"
            
            tasks_text += f"{status_emoji} *{task_type}*\n"
            tasks_text += f"  ID: `{task_id}`\n"
            tasks_text += f"  상태: {task_status}\n\n"
            
        await update.message.reply_text(tasks_text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 예약 목록 명령어 처리 중 오류: {str(e)}")
        await update.message.reply_text(f"❌ 오류 발생: {str(e)}")

async def handle_cancel_schedule_command(update, context):
    """
    /cancel_schedule [작업ID] 명령어 처리 - 예약된 작업을 취소합니다.
    """
    user_id = update.effective_user.id
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("⛔ 접근 권한이 없습니다.")
        return
    
    # 인자 확인
    if not context.args or len(context.args) < 1:
        await update.message.reply_text("❌ 작업 ID를 지정해주세요.\n사용법: /cancel_schedule [작업ID]")
        return
    
    task_id = context.args[0]
    
    await update.message.reply_text(f"🛑 작업 '{task_id}' 취소 중...")
    
    try:
        # 작업 취소
        success = await cancel_task(task_id)
        
        if success:
            await update.message.reply_text(f"✅ 작업 '{task_id}' 취소됨")
        else:
            await update.message.reply_text(f"❌ 작업 '{task_id}' 취소 실패")
            
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 작업 취소 명령어 처리 중 오류: {str(e)}")
        await update.message.reply_text(f"❌ 오류 발생: {str(e)}")

async def handle_system_info_command(update, context):
    """
    /system_info 명령어 처리 - 시스템 정보를 확인합니다.
    """
    user_id = update.effective_user.id
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("⛔ 접근 권한이 없습니다.")
        return
    
    await update.message.reply_text("🔍 시스템 정보 확인 중...")
    
    try:
        # 시스템 정보 확인
        system_info = await get_system_info()
        
        # 응답 메시지 생성
        info_text = "💻 *시스템 정보*\n\n"
        
        # 기본 정보
        info_text += f"*호스트명:* {system_info.get('hostname', 'N/A')}\n"
        info_text += f"*플랫폼:* {system_info.get('platform', 'N/A')} {system_info.get('platform_version', '')}\n"
        info_text += f"*Python:* {system_info.get('python_version', 'N/A')}\n"
        info_text += f"*가동 시간:* {system_info.get('uptime', 'N/A')}\n\n"
        
        # CPU 정보
        cpu_info = system_info.get('cpu', {})
        info_text += f"*CPU 사용률:* {cpu_info.get('percent', 'N/A')}%\n"
        info_text += f"*CPU 코어:* {cpu_info.get('count', 'N/A')}\n\n"
        
        # 메모리 정보
        memory_info = system_info.get('memory', {})
        info_text += f"*메모리 사용률:* {memory_info.get('percent', 'N/A')}%\n"
        info_text += f"*메모리 사용량:* {memory_info.get('used_gb', 'N/A')} GB / {memory_info.get('total_gb', 'N/A')} GB\n\n"
        
        # 디스크 정보
        disk_info = system_info.get('disk', {})
        info_text += f"*디스크 사용률:* {disk_info.get('percent', 'N/A')}%\n"
        info_text += f"*디스크 사용량:* {disk_info.get('used_gb', 'N/A')} GB / {disk_info.get('total_gb', 'N/A')} GB\n"
        
        await update.message.reply_text(info_text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 시스템 정보 명령어 처리 중 오류: {str(e)}")
        await update.message.reply_text(f"❌ 오류 발생: {str(e)}")

async def handle_start_monitoring_command(update, context):
    """
    /start_monitoring 명령어 처리 - 시스템 모니터링을 시작합니다.
    """
    user_id = update.effective_user.id
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("⛔ 접근 권한이 없습니다.")
        return
    
    # 인자 확인 (선택적)
    interval = 60  # 기본값
    if context.args and len(context.args) > 0:
        try:
            interval = int(context.args[0])
            if interval < 10:
                await update.message.reply_text("⚠️ 모니터링 간격은 최소 10초 이상이어야 합니다. 기본값 60초로 설정합니다.")
                interval = 60
        except ValueError:
            await update.message.reply_text("⚠️ 유효하지 않은 간격입니다. 기본값 60초로 설정합니다.")
    
    await update.message.reply_text(f"🔍 시스템 모니터링 시작 중... (간격: {interval}초)")
    
    try:
        # 모니터링 시작
        success = await start_monitoring(interval)
        
        if success:
            await update.message.reply_text(f"✅ 시스템 모니터링 시작됨 (간격: {interval}초)")
        else:
            await update.message.reply_text("❌ 시스템 모니터링 시작 실패")
            
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 모니터링 시작 명령어 처리 중 오류: {str(e)}")
        await update.message.reply_text(f"❌ 오류 발생: {str(e)}")

async def handle_stop_monitoring_command(update, context):
    """
    /stop_monitoring 명령어 처리 - 시스템 모니터링을 중지합니다.
    """
    user_id = update.effective_user.id
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("⛔ 접근 권한이 없습니다.")
        return
    
    await update.message.reply_text("🛑 시스템 모니터링 중지 중...")
    
    try:
        # 모니터링 중지
        success = await stop_monitoring()
        
        if success:
            await update.message.reply_text("✅ 시스템 모니터링 중지됨")
        else:
            await update.message.reply_text("❌ 시스템 모니터링 중지 실패")
            
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 모니터링 중지 명령어 처리 중 오류: {str(e)}")
        await update.message.reply_text(f"❌ 오류 발생: {str(e)}")

# 명령어 핸들러 등록 함수
def register_system_commands(application):
    """
    텔레그램 봇 애플리케이션에 시스템 관리 명령어 핸들러를 등록합니다.
    
    Args:
        application: 텔레그램 봇 애플리케이션 인스턴스
    """
    application.add_handler(CommandHandler("start", handle_start_command))
    application.add_handler(CommandHandler("help", handle_start_command))
    application.add_handler(CommandHandler("status", handle_status_command))
    application.add_handler(CommandHandler("start_process", handle_start_process_command))
    application.add_handler(CommandHandler("stop_process", handle_stop_process_command))
    application.add_handler(CommandHandler("restart_process", handle_restart_process_command))
    application.add_handler(CommandHandler("schedule_restart", handle_schedule_restart_command))
    application.add_handler(CommandHandler("list_schedules", handle_list_schedules_command))
    application.add_handler(CommandHandler("cancel_schedule", handle_cancel_schedule_command))
    application.add_handler(CommandHandler("system_info", handle_system_info_command))
    application.add_handler(CommandHandler("start_monitoring", handle_start_monitoring_command))
    application.add_handler(CommandHandler("stop_monitoring", handle_stop_monitoring_command)) 