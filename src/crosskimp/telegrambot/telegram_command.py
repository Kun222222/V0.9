import logging
import subprocess
import sys
from pathlib import Path
from typing import Optional
import os

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
import telegram.error
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, CallbackContext
from crosskimp.config.bot_constants import COMMAND_BOT_TOKEN, ADMIN_USER_IDS, BotCommands, WELCOME_MESSAGE, HELP_MESSAGE, PROGRAM_STARTED, PROGRAM_STOPPED, PROGRAM_ALREADY_RUNNING, PROGRAM_NOT_RUNNING, UNAUTHORIZED_USER, setup_logger

# 시스템 관리 모듈 가져오기
from crosskimp.telegrambot.system_commands import register_system_commands, AUTHORIZED_USERS
from crosskimp.system_manager.process_manager import start_process, stop_process, restart_process, get_process_status, PROCESS_INFO

# 로거 설정
logger = setup_logger()

# 관리자 사용자 ID를 시스템 명령어 모듈의 인증된 사용자 목록에 추가
AUTHORIZED_USERS.extend(ADMIN_USER_IDS)

class OrderbookCommandBot:
    def __init__(self):
        self.ob_process: Optional[subprocess.Popen] = None
        self.application = Application.builder().token(COMMAND_BOT_TOKEN).build()
        self._setup_handlers()
        logger.info("텔레그램 봇 초기화 완료")

    def _setup_handlers(self):
        """Set up command handlers"""
        self.application.add_handler(CommandHandler(BotCommands.START, self.start))
        self.application.add_handler(CommandHandler(BotCommands.HELP, self.help))
        self.application.add_handler(CommandHandler(BotCommands.START_OB, self.start_ob))
        self.application.add_handler(CommandHandler(BotCommands.STOP_OB, self.stop_ob))
        self.application.add_handler(CommandHandler(BotCommands.STATUS, self.status))
        # 버튼 콜백 핸들러 추가
        self.application.add_handler(CallbackQueryHandler(self.button_click))
        
        # 시스템 관리 명령어 핸들러 등록
        register_system_commands(self.application)
        
        logger.info("텔레그램 봇 핸들러 설정 완료")

    def get_keyboard(self) -> InlineKeyboardMarkup:
        """Create inline keyboard with command buttons"""
        keyboard = [
            [
                InlineKeyboardButton("🔄 상태 확인", callback_data=BotCommands.STATUS),
            ],
            [
                InlineKeyboardButton("▶️ 시작", callback_data=BotCommands.START_OB),
                InlineKeyboardButton("⏹️ 중지", callback_data=BotCommands.STOP_OB),
            ],
            [
                InlineKeyboardButton("❓ 도움말", callback_data=BotCommands.HELP),
            ],
        ]
        return InlineKeyboardMarkup(keyboard)

    async def check_admin(self, update: Update) -> bool:
        """Check if the user is an admin"""
        user_id = update.effective_user.id
        if user_id not in ADMIN_USER_IDS:
            if update.callback_query:
                await update.callback_query.answer(UNAUTHORIZED_USER)
            else:
                await update.message.reply_text(UNAUTHORIZED_USER)
            return False
        return True

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send welcome message when the command /start is issued."""
        await update.message.reply_text(
            WELCOME_MESSAGE,
            reply_markup=self.get_keyboard()
        )

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send help message when the command /help is issued."""
        if isinstance(update.message, Update):
            await update.message.reply_text(
                HELP_MESSAGE,
                reply_markup=self.get_keyboard()
            )
        else:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=HELP_MESSAGE,
                reply_markup=self.get_keyboard()
            )

    async def button_click(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle button clicks"""
        query = update.callback_query
        await query.answer()  # 버튼 클릭 응답

        if not await self.check_admin(update):
            return

        command = query.data
        if command == BotCommands.START_OB:
            await self.start_ob(update, context)
        elif command == BotCommands.STOP_OB:
            await self.stop_ob(update, context)
        elif command == BotCommands.STATUS:
            await self.status(update, context)
        elif command == BotCommands.HELP:
            await self.help(update, context)

    async def start_ob(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start the orderbook collector program"""
        if not await self.check_admin(update):
            return

        # 시스템 관리 모듈을 사용하여 프로세스 상태 확인
        process_status = await get_process_status()
        if "ob_collector" in process_status and process_status["ob_collector"]["running"]:
            await self._send_message(update, PROGRAM_ALREADY_RUNNING)
            return

        try:
            # 시스템 관리 모듈을 사용하여 프로세스 시작
            success = await start_process("ob_collector")
            
            if success:
                await self._send_message(update, f"{PROGRAM_STARTED}\n🆔 프로세스 시작됨")
            else:
                await self._send_message(update, "❌ 오더북 수집 프로그램 시작 실패")
            
        except Exception as e:
            error_message = f"오더북 수집 프로그램 시작 중 오류 발생: {str(e)}"
            logger.error(error_message)
            await self._send_message(update, error_message)

    async def stop_ob(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Stop the orderbook collector program"""
        if not await self.check_admin(update):
            return

        # 시스템 관리 모듈을 사용하여 프로세스 상태 확인
        process_status = await get_process_status()
        if "ob_collector" not in process_status or not process_status["ob_collector"]["running"]:
            await self._send_message(update, PROGRAM_NOT_RUNNING)
            return

        try:
            # 시스템 관리 모듈을 사용하여 프로세스 중지
            success = await stop_process("ob_collector")
            
            if success:
                await self._send_message(update, PROGRAM_STOPPED)
            else:
                await self._send_message(update, "❌ 오더북 수집 프로그램 중지 실패")
            
        except Exception as e:
            error_message = f"오더북 수집 프로그램 종료 중 오류 발생: {str(e)}"
            logger.error(error_message)
            await self._send_message(update, error_message)

    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Check the status of the orderbook collector program"""
        if not await self.check_admin(update):
            return

        try:
            # 시스템 관리 모듈을 사용하여 프로세스 상태 확인
            process_status = await get_process_status()
            
            if "ob_collector" in process_status and process_status["ob_collector"]["running"]:
                status_message = f"✅ 오더북 수집 프로그램이 실행 중입니다. (PID: {process_status['ob_collector']['pid']})"
            else:
                status_message = "❌ 오더북 수집 프로그램이 실행 중이지 않습니다."
                
            await self._send_message(update, status_message)
            
        except Exception as e:
            error_message = f"상태 확인 중 오류 발생: {str(e)}"
            logger.error(error_message)
            await self._send_message(update, error_message)

    async def _send_message(self, update: Update, text: str):
        """Helper method to send messages with keyboard"""
        if update.callback_query:
            try:
                await update.callback_query.edit_message_text(
                    text=text,
                    reply_markup=self.get_keyboard()
                )
            except telegram.error.BadRequest as e:
                if "Message is not modified" in str(e):
                    # 메시지가 동일한 경우 무시
                    await update.callback_query.answer("✓ 상태가 변경되지 않았습니다")
                else:
                    # 다른 종류의 BadRequest 에러는 다시 발생
                    raise
        else:
            await update.message.reply_text(
                text=text,
                reply_markup=self.get_keyboard()
            )

    def run(self):
        """Start the bot"""
        self.application.run_polling() 