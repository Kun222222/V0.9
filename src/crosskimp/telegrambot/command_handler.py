"""
텔레그램 명령어 핸들러 - 텔레그램 봇 명령어 처리를 담당합니다.
"""

import asyncio
import logging
from typing import Dict, List, Any
from datetime import datetime

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import LOG_TELEGRAM
from crosskimp.system_manager.scheduler import get_scheduled_tasks, schedule_daily_restart, cancel_task

# 로거 설정
logger = get_unified_logger()

# 도움말 메시지
HELP_MESSAGE = """
<b>🤖 크로스 김프 시스템 컨트롤 봇 도움말</b>

<b>💻 시스템 명령어</b>
/status - 전체 프로세스 상태 확인
/start_process [이름] - 프로세스 시작
/stop_process [이름] - 프로세스 중지
/restart_process [이름] - 프로세스 재시작

<b>🔄 스케줄러 명령어</b>
/schedule_list - 예약된 작업 목록
/schedule_restart [프로세스] [시간] [분] - 일일 재시작 예약
/cancel_schedule [작업ID] - 예약 작업 취소

<b>⚙️ 설정 명령어</b>
/auto_restart [프로세스] [on/off] - 자동 재시작 설정

<b>ℹ️ 기타 명령어</b>
/help - 이 도움말 표시
"""

async def admin_required(update: Update) -> bool:
    """
    관리자 권한이 필요한 작업인지 확인합니다.
    
    Args:
        update: 텔레그램 업데이트 객체
        
    Returns:
        bool: 관리자 여부
    """
    # 여기서는 임시로 모든 사용자에게 허용
    # 실제 구현에서는 bot_manager.is_admin 등을 통해 확인해야 함
    return True

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """시작 명령어 처리"""
    await update.message.reply_html(
        f"안녕하세요! 크로스 김프 시스템 컨트롤 봇입니다.\n\n{HELP_MESSAGE}"
    )
    logger.info(f"{LOG_TELEGRAM} /start 명령어 처리됨 (사용자: {update.effective_user.id})")

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """도움말 명령어 처리"""
    await update.message.reply_html(HELP_MESSAGE)
    logger.info(f"{LOG_TELEGRAM} /help 명령어 처리됨 (사용자: {update.effective_user.id})")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """상태 확인 명령어 처리"""
    if not await admin_required(update):
        await update.message.reply_text("권한이 없습니다.")
        return
        
    # 지연 임포트
    from crosskimp.system_manager.process_manager import get_process_status
    
    status = await get_process_status()
    
    # 상태 메시지 생성
    now = datetime.now()
    message = "<b>🖥️ 시스템 프로세스 상태</b>\n\n"
    
    for name, process_info in status.items():
        status_emoji = "🟢" if process_info["running"] else "🔴"
        
        # 실행 중이면 업타임 표시
        uptime_str = ""
        if process_info["running"] and process_info["uptime"] is not None:
            uptime_seconds = int(process_info["uptime"])
            hours, remainder = divmod(uptime_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            uptime_str = f"({hours}시간 {minutes}분 {seconds}초)"
        
        # 자동 재시작 상태
        auto_restart = "✅" if process_info["auto_restart"] else "❌"
        
        message += (
            f"{status_emoji} <b>{process_info['description']}</b> ({name})\n"
            f"   상태: {'실행 중' if process_info['running'] else '중지됨'} {uptime_str}\n"
            f"   PID: {process_info['pid'] or 'N/A'}\n"
            f"   자동 재시작: {auto_restart}\n\n"
        )
    
    message += f"<i>갱신 시간: {now.strftime('%Y-%m-%d %H:%M:%S')}</i>"
    
    await update.message.reply_html(message)
    logger.info(f"{LOG_TELEGRAM} /status 명령어 처리됨 (사용자: {update.effective_user.id})")

async def cmd_start_process(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """프로세스 시작 명령어 처리"""
    if not await admin_required(update):
        await update.message.reply_text("권한이 없습니다.")
        return
        
    # 지연 임포트
    from crosskimp.system_manager.process_manager import start_process
    
    # 인자 확인
    if not context.args or len(context.args) < 1:
        await update.message.reply_text("사용법: /start_process [프로세스명]")
        return
        
    process_name = context.args[0]
    
    # 프로세스 시작 메시지
    await update.message.reply_html(
        f"<b>{process_name}</b> 프로세스 시작 중..."
    )
    
    # 프로세스 시작
    result = await start_process(process_name)
    
    if result:
        await update.message.reply_html(
            f"✅ <b>{process_name}</b> 프로세스가 시작되었습니다."
        )
    else:
        await update.message.reply_html(
            f"❌ <b>{process_name}</b> 프로세스 시작 실패"
        )
    
    logger.info(f"{LOG_TELEGRAM} /start_process {process_name} 명령어 처리됨 (사용자: {update.effective_user.id})")

async def cmd_stop_process(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """프로세스 중지 명령어 처리"""
    if not await admin_required(update):
        await update.message.reply_text("권한이 없습니다.")
        return
        
    # 지연 임포트
    from crosskimp.system_manager.process_manager import stop_process
    
    # 인자 확인
    if not context.args or len(context.args) < 1:
        await update.message.reply_text("사용법: /stop_process [프로세스명]")
        return
        
    process_name = context.args[0]
    
    # 프로세스 중지 메시지
    await update.message.reply_html(
        f"<b>{process_name}</b> 프로세스 중지 중..."
    )
    
    # 프로세스 중지
    result = await stop_process(process_name)
    
    if result:
        await update.message.reply_html(
            f"✅ <b>{process_name}</b> 프로세스가 중지되었습니다."
        )
    else:
        await update.message.reply_html(
            f"❌ <b>{process_name}</b> 프로세스 중지 실패"
        )
    
    logger.info(f"{LOG_TELEGRAM} /stop_process {process_name} 명령어 처리됨 (사용자: {update.effective_user.id})")

async def cmd_restart_process(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """프로세스 재시작 명령어 처리"""
    if not await admin_required(update):
        await update.message.reply_text("권한이 없습니다.")
        return
        
    # 지연 임포트
    from crosskimp.system_manager.process_manager import restart_process
    
    # 인자 확인
    if not context.args or len(context.args) < 1:
        await update.message.reply_text("사용법: /restart_process [프로세스명]")
        return
        
    process_name = context.args[0]
    
    # 프로세스 재시작 메시지
    await update.message.reply_html(
        f"<b>{process_name}</b> 프로세스 재시작 중..."
    )
    
    # 프로세스 재시작
    result = await restart_process(process_name)
    
    if result:
        await update.message.reply_html(
            f"✅ <b>{process_name}</b> 프로세스가 재시작되었습니다."
        )
    else:
        await update.message.reply_html(
            f"❌ <b>{process_name}</b> 프로세스 재시작 실패"
        )
    
    logger.info(f"{LOG_TELEGRAM} /restart_process {process_name} 명령어 처리됨 (사용자: {update.effective_user.id})")

async def cmd_auto_restart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """자동 재시작 설정 명령어 처리"""
    if not await admin_required(update):
        await update.message.reply_text("권한이 없습니다.")
        return
        
    # 지연 임포트
    from crosskimp.system_manager.process_manager import set_auto_restart
    
    # 인자 확인
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("사용법: /auto_restart [프로세스명] [on/off]")
        return
        
    process_name = context.args[0]
    enabled_str = context.args[1].lower()
    
    if enabled_str not in ["on", "off"]:
        await update.message.reply_text("사용법: /auto_restart [프로세스명] [on/off]")
        return
        
    enabled = (enabled_str == "on")
    
    # 자동 재시작 설정
    result = set_auto_restart(process_name, enabled)
    
    if result:
        status = "활성화" if enabled else "비활성화"
        await update.message.reply_html(
            f"✅ <b>{process_name}</b> 프로세스 자동 재시작이 {status}되었습니다."
        )
    else:
        await update.message.reply_html(
            f"❌ <b>{process_name}</b> 프로세스 자동 재시작 설정 실패"
        )
    
    logger.info(f"{LOG_TELEGRAM} /auto_restart {process_name} {enabled_str} 명령어 처리됨 (사용자: {update.effective_user.id})")

async def cmd_schedule_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """예약된 작업 목록 명령어 처리"""
    if not await admin_required(update):
        await update.message.reply_text("권한이 없습니다.")
        return
        
    tasks = await get_scheduled_tasks()
    
    if not tasks:
        await update.message.reply_text("예약된 작업이 없습니다.")
        return
        
    message = "<b>📅 예약된 작업 목록</b>\n\n"
    
    for task in tasks:
        status_emoji = "🟢" if task["status"] == "running" else "🔴"
        message += (
            f"{status_emoji} <b>작업 ID:</b> {task['id']}\n"
            f"   유형: {task['type']}\n"
            f"   상태: {task['status']}\n\n"
        )
    
    await update.message.reply_html(message)
    logger.info(f"{LOG_TELEGRAM} /schedule_list 명령어 처리됨 (사용자: {update.effective_user.id})")

async def cmd_schedule_restart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """일일 재시작 예약 명령어 처리"""
    if not await admin_required(update):
        await update.message.reply_text("권한이 없습니다.")
        return
        
    # 인자 확인
    if not context.args or len(context.args) < 3:
        await update.message.reply_text("사용법: /schedule_restart [프로세스명] [시간] [분]")
        return
        
    try:
        process_name = context.args[0]
        hour = int(context.args[1])
        minute = int(context.args[2])
        
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            raise ValueError("시간은 0-23, 분은 0-59 범위여야 합니다.")
            
    except ValueError as e:
        await update.message.reply_text(f"잘못된 인자: {str(e)}\n사용법: /schedule_restart [프로세스명] [시간] [분]")
        return
        
    # 재시작 예약
    await update.message.reply_html(
        f"<b>{process_name}</b> 프로세스의 일일 재시작을 {hour:02d}:{minute:02d}에 예약 중..."
    )
    
    task_id = await schedule_daily_restart(process_name, hour, minute)
    
    await update.message.reply_html(
        f"✅ <b>{process_name}</b> 프로세스의 일일 재시작이 {hour:02d}:{minute:02d}에 예약되었습니다.\n"
        f"작업 ID: <code>{task_id}</code>"
    )
    
    logger.info(f"{LOG_TELEGRAM} /schedule_restart {process_name} {hour} {minute} 명령어 처리됨 (사용자: {update.effective_user.id})")

async def cmd_cancel_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """예약 작업 취소 명령어 처리"""
    if not await admin_required(update):
        await update.message.reply_text("권한이 없습니다.")
        return
        
    # 인자 확인
    if not context.args or len(context.args) < 1:
        await update.message.reply_text("사용법: /cancel_schedule [작업ID]")
        return
        
    task_id = context.args[0]
    
    # 작업 취소
    result = await cancel_task(task_id)
    
    if result:
        await update.message.reply_html(
            f"✅ 작업 <code>{task_id}</code>가 취소되었습니다."
        )
    else:
        await update.message.reply_html(
            f"❌ 작업 <code>{task_id}</code> 취소 실패"
        )
    
    logger.info(f"{LOG_TELEGRAM} /cancel_schedule {task_id} 명령어 처리됨 (사용자: {update.effective_user.id})")

def register_commands(application: Application) -> None:
    """
    텔레그램 봇 명령어 핸들러를 등록합니다.
    
    Args:
        application: 텔레그램 봇 애플리케이션 인스턴스
    """
    # 기본 명령어
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    
    # 프로세스 관리 명령어
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("start_process", cmd_start_process))
    application.add_handler(CommandHandler("stop_process", cmd_stop_process))
    application.add_handler(CommandHandler("restart_process", cmd_restart_process))
    application.add_handler(CommandHandler("auto_restart", cmd_auto_restart))
    
    # 스케줄러 명령어
    application.add_handler(CommandHandler("schedule_list", cmd_schedule_list))
    application.add_handler(CommandHandler("schedule_restart", cmd_schedule_restart))
    application.add_handler(CommandHandler("cancel_schedule", cmd_cancel_schedule)) 