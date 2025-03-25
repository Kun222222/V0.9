"""
텔레그램 커맨더 모듈

이 모듈은 텔레그램 봇을 통한 시스템 명령어 처리 인터페이스를 제공합니다.
"""

import os
import asyncio
import traceback
import uuid
from typing import Dict, Any, Optional

from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.config.app_config import get_config
from crosskimp.common.events.system_eventbus import get_event_bus
from crosskimp.common.events.system_types import SystemEventType, TelegramEventType

# 로거 설정
logger = get_unified_logger(component=SystemComponent.SYSTEM.value)

# 싱글톤 인스턴스
_telegram_commander_instance = None

def get_telegram_commander():
    """글로벌 텔레그램 커맨더 인스턴스를 반환합니다."""
    global _telegram_commander_instance
    if _telegram_commander_instance is None:
        _telegram_commander_instance = TelegramCommander()
    return _telegram_commander_instance

class TelegramCommander:
    """
    텔레그램 봇과 이벤트 버스를 연결하는 어댑터 클래스
    """
    
    def __init__(self):
        """텔레그램 커맨더 초기화"""
        # 텔레그램 봇 토큰
        self.telegram_token = None
        
        # 허용된 채팅 ID
        self.allowed_chat_ids = []
        
        # 텔레그램 봇 애플리케이션
        self.app = None
        
        # 텔레그램 봇
        self.bot = None
        
        # 초기화 완료 여부
        self.initialized = False
        
        # 로거
        self.logger = logger
        
        # 폴링 태스크
        self._polling_task = None
        
        # 이벤트 버스
        self.event_bus = None
    
    async def start(self) -> bool:
        """
        텔레그램 봇 초기화 및 시작
        
        Returns:
            bool: 시작 성공 여부
        """
        try:
            self.logger.info("텔레그램 커맨더 초기화 중...")
            
            # 설정에서 텔레그램 봇 토큰 가져오기
            config = get_config()
            self.telegram_token = config.get_env("telegram.bot_token", "")
            
            if not self.telegram_token:
                self.logger.warning("텔레그램 봇 토큰이 설정되지 않았습니다. 텔레그램 봇 기능을 사용할 수 없습니다.")
                return False
                
            # 설정에서 허용된 채팅 ID 가져오기
            chat_ids_str = config.get_env("telegram.chat_id", "")
            
            if chat_ids_str:
                if ',' in chat_ids_str:
                    self.allowed_chat_ids = [int(id.strip()) for id in chat_ids_str.split(',')]
                else:
                    self.allowed_chat_ids = [int(chat_ids_str.strip())]
                self.logger.info(f"허용된 텔레그램 채팅 ID: {self.allowed_chat_ids}")
            else:
                self.logger.warning("허용된 텔레그램 채팅 ID가 설정되지 않았습니다.")
            
            # 이벤트 버스 초기화
            self.event_bus = get_event_bus()
            
            # 텔레그램 봇 시작
            await self._start_telegram_bot()
            
            self.initialized = True
            self.logger.info("텔레그램 커맨더가 초기화되었습니다.")
            
            # 시스템 시작 알림 메시지 전송
            try:
                await self.send_message("🚀 시스템이 성공적으로 시작되었습니다.\n\n명령어 목록을 보려면 /help를 입력하세요.")
                self.logger.info("시스템 시작 알림을 전송했습니다.")
            except Exception as e:
                self.logger.error(f"시스템 시작 알림 전송 실패: {str(e)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"텔레그램 커맨더 초기화 중 오류 발생: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
    
    async def _start_telegram_bot(self):
        """텔레그램 봇 시작"""
        try:
            # 텔레그램 봇 애플리케이션 생성
            self.app = Application.builder().token(self.telegram_token).build()
            
            # 봇 객체 참조 가져오기
            self.bot = self.app.bot
            
            # 핸들러 설정
            self._setup_telegram_handlers()
            
            # 봇 시작 (논블로킹)
            await self.app.initialize()
            await self.app.start()
            
            # 폴링을 비동기로 시작 (백그라운드 태스크)
            self._polling_task = asyncio.create_task(self._run_polling())
            
            # 짧은 대기 시간 추가 (봇 초기화 완료 대기)
            await asyncio.sleep(0.5)
            
            self.logger.info("텔레그램 봇이 성공적으로 시작되었습니다.")
            
        except Exception as e:
            self.logger.error(f"텔레그램 봇 시작 중 오류 발생: {str(e)}")
            self.logger.error(traceback.format_exc())
            # 리소스 정리
            await self._cleanup_bot_resources()
            raise
    
    async def _run_polling(self):
        """텔레그램 봇 폴링 실행 (백그라운드)"""
        try:
            self.logger.info("텔레그램 봇 폴링 시작...")
            await self.app.updater.start_polling()
            self.logger.info("텔레그램 봇 폴링이 성공적으로 시작되었습니다.")
            
        except asyncio.CancelledError:
            self.logger.info("텔레그램 봇 폴링이 취소되었습니다.")
            
        except Exception as e:
            self.logger.error(f"텔레그램 봇 폴링 중 오류 발생: {str(e)}")
            self.logger.error(traceback.format_exc())
    
    async def _cleanup_bot_resources(self):
        """텔레그램 봇 리소스 정리"""
        try:
            # 폴링 태스크 취소
            if hasattr(self, '_polling_task') and self._polling_task and not self._polling_task.done():
                self._polling_task.cancel()
                try:
                    await asyncio.wait_for(self._polling_task, timeout=2)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            # 앱 셧다운
            if self.app:
                try:
                    # updater 종료
                    if self.app.updater and self.app.updater.running:
                        await self.app.updater.stop()
                    
                    # app 종료
                    if self.app.running:
                        await self.app.stop()
                    
                    # app 셧다운
                    await self.app.shutdown()
                except Exception:
                    pass
            
            # 참조 정리
            self.app = None
            self.bot = None
            
        except Exception as e:
            self.logger.error(f"텔레그램 봇 리소스 정리 중 오류 발생: {str(e)}")
    
    async def stop(self) -> bool:
        """
        텔레그램 봇 종료
        
        Returns:
            bool: 종료 성공 여부
        """
        try:
            self.logger.info("텔레그램 커맨더를 종료합니다...")
            
            # 텔레그램 봇 리소스 정리
            await self._cleanup_bot_resources()
            
            self.initialized = False
            self.logger.info("텔레그램 커맨더가 종료되었습니다.")
            return True
            
        except Exception as e:
            self.logger.error(f"텔레그램 커맨더 종료 중 오류 발생: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _setup_telegram_handlers(self):
        """텔레그램 봇 핸들러 설정"""
        # 기본 명령 핸들러
        self.app.add_handler(CommandHandler("start", self._handle_start_command))
        self.app.add_handler(CommandHandler("help", self._handle_help_command))
        self.app.add_handler(CommandHandler("status", self._handle_status_command))
        
        # 프로세스 관리 명령어
        self.app.add_handler(CommandHandler("processes", self._handle_processes_command))
        self.app.add_handler(CommandHandler("start_process", self._handle_start_process_command))
        self.app.add_handler(CommandHandler("stop_process", self._handle_stop_process_command))
        self.app.add_handler(CommandHandler("restart_process", self._handle_restart_process_command))
        
        # 인라인 키보드 콜백 핸들러
        self.app.add_handler(CallbackQueryHandler(self._handle_button_callback))
        
        # 오류 핸들러
        self.app.add_error_handler(self._handle_telegram_error)
        
        self.logger.debug("텔레그램 봇 핸들러가 설정되었습니다.")
    
    # 프로세스 관리 메서드
    async def _send_process_command(self, command: str, process_name: str) -> dict:
        """
        프로세스 관련 명령을 이벤트 버스를 통해 오케스트레이터로 전송
        
        Args:
            command: 명령어 (start_process, stop_process, restart_process)
            process_name: 프로세스 이름
            
        Returns:
            dict: 명령 처리 결과
        """
        try:
            if not self.event_bus:
                self.event_bus = get_event_bus()
                
            # 명령 데이터 생성
            command_data = {
                "command": command,
                "args": {"process_name": process_name},
                "source": "telegram",
                "request_id": str(uuid.uuid4())  # 고유 요청 ID
            }
            
            # 명령 이벤트 발행
            await self.event_bus.publish(TelegramEventType.COMMAND, command_data)
            
            # 성공 응답
            return {"success": True}
            
        except Exception as e:
            self.logger.error(f"프로세스 명령 전송 중 오류: {str(e)}")
            return {"success": False, "error": str(e)}
    
    # 텔레그램 명령어 핸들러 함수
    async def _handle_start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시작 명령 처리"""
        message = (
            "👋 크로스킴프 봇에 오신 것을 환영합니다!\n\n"
            "명령어 목록을 보려면 /help를 입력하세요."
        )
        await update.message.reply_text(message)
    
    async def _handle_help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """도움말 명령 처리"""
        help_text = (
            "📌 **사용 가능한 명령어**\n\n"
            "/status - 시스템 상태 확인\n"
            "/processes - 프로세스 관리 메뉴 (버튼으로 제어)\n"
            "/help - 도움말 표시\n\n"
            "**프로세스 명령어**\n"
            "/start_process [프로세스명] - 프로세스 시작\n"
            "/stop_process [프로세스명] - 프로세스 중지\n"
            "/restart_process [프로세스명] - 프로세스 재시작\n\n"
            "사용 가능한 프로세스: orderbook, monitoring, data_collector, trade_executor"
        )
        await update.message.reply_text(help_text)
    
    async def _handle_status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """상태 확인 명령 처리"""
        await update.message.reply_text("✅ 시스템이 정상 작동 중입니다.")
    
    async def _handle_processes_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """프로세스 관리 명령어 - 인라인 키보드로 프로세스 목록 표시"""
        # 프로세스 목록 키보드 생성
        keyboard = [
            [
                InlineKeyboardButton("오더북 시작", callback_data="start_orderbook"),
                InlineKeyboardButton("오더북 중지", callback_data="stop_orderbook")
            ],
            [
                InlineKeyboardButton("모니터링 시작", callback_data="start_monitoring"),
                InlineKeyboardButton("모니터링 중지", callback_data="stop_monitoring")
            ],
            [
                InlineKeyboardButton("데이터 수집기 시작", callback_data="start_data_collector"),
                InlineKeyboardButton("데이터 수집기 중지", callback_data="stop_data_collector")
            ],
            [
                InlineKeyboardButton("거래 실행기 시작", callback_data="start_trade_executor"),
                InlineKeyboardButton("거래 실행기 중지", callback_data="stop_trade_executor")
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("프로세스 관리:", reply_markup=reply_markup)
    
    async def _handle_button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """인라인 키보드 버튼 콜백 처리"""
        query = update.callback_query
        await query.answer()  # 콜백 응답
        
        # 콜백 데이터 파싱 (예: "start_orderbook")
        action, process_name = query.data.split('_', 1)
        
        # 로그 추가 - 명령 수신
        self.logger.info(f"텔레그램에서 '{action}_{process_name}' 명령을 수신했습니다. (사용자: {query.from_user.username or query.from_user.id})")
        
        # 진행 중 메시지 표시
        command_text = "시작" if action == "start" else "중지"
        await query.edit_message_text(f"⏳ {process_name} 프로세스 {command_text} 요청 중...")
        
        # 이벤트 버스로 명령 전송
        command = f"{action}_process"
        result = await self._send_process_command(command, process_name)
        
        # 결과 메시지 표시
        if result.get("success", False):
            await query.edit_message_text(f"✅ '{process_name}' 프로세스 {command_text} 요청이 전송되었습니다.")
        else:
            error_msg = result.get("error", "알 수 없는 오류")
            await query.edit_message_text(f"❌ 오류: '{process_name}' 프로세스 {command_text} 실패\n{error_msg}")
    
    async def _handle_start_process_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """프로세스 시작 명령 처리"""
        # 로그 추가 - 명령 수신
        user = update.message.from_user
        self.logger.info(f"텔레그램에서 '/start_process' 명령을 수신했습니다. (사용자: {user.username or user.id})")
        
        message = await update.message.reply_text("⏳ 프로세스 시작 요청 중...")
        
        args = context.args
        if not args or len(args) < 1:
            await message.edit_text("❌ 오류: 프로세스 이름을 지정해야 합니다.\n사용법: /start_process [프로세스명]")
            return
        
        process_name = args[0].lower()
        # 로그 추가 - 구체적인 프로세스 정보
        self.logger.info(f"텔레그램에서 '{process_name}' 프로세스 시작 명령을 처리합니다.")
        
        result = await self._send_process_command("start_process", process_name)
        
        if result.get("success", False):
            await message.edit_text(f"✅ '{process_name}' 프로세스 시작 요청이 전송되었습니다.")
        else:
            error_msg = result.get("error", "알 수 없는 오류")
            await message.edit_text(f"❌ 오류: '{process_name}' 프로세스 시작 실패\n{error_msg}")
    
    async def _handle_stop_process_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """프로세스 중지 명령 처리"""
        # 로그 추가 - 명령 수신
        user = update.message.from_user
        self.logger.info(f"텔레그램에서 '/stop_process' 명령을 수신했습니다. (사용자: {user.username or user.id})")
        
        message = await update.message.reply_text("⏳ 프로세스 중지 요청 중...")
        
        args = context.args
        if not args or len(args) < 1:
            await message.edit_text("❌ 오류: 프로세스 이름을 지정해야 합니다.\n사용법: /stop_process [프로세스명]")
            return
        
        process_name = args[0].lower()
        # 로그 추가 - 구체적인 프로세스 정보
        self.logger.info(f"텔레그램에서 '{process_name}' 프로세스 중지 명령을 처리합니다.")
        
        result = await self._send_process_command("stop_process", process_name)
        
        if result.get("success", False):
            await message.edit_text(f"✅ '{process_name}' 프로세스 중지 요청이 전송되었습니다.")
        else:
            error_msg = result.get("error", "알 수 없는 오류")
            await message.edit_text(f"❌ 오류: '{process_name}' 프로세스 중지 실패\n{error_msg}")
    
    async def _handle_restart_process_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """프로세스 재시작 명령 처리"""
        # 로그 추가 - 명령 수신
        user = update.message.from_user
        self.logger.info(f"텔레그램에서 '/restart_process' 명령을 수신했습니다. (사용자: {user.username or user.id})")
        
        message = await update.message.reply_text("⏳ 프로세스 재시작 요청 중...")
        
        args = context.args
        if not args or len(args) < 1:
            await message.edit_text("❌ 오류: 프로세스 이름을 지정해야 합니다.\n사용법: /restart_process [프로세스명]")
            return
        
        process_name = args[0].lower()
        # 로그 추가 - 구체적인 프로세스 정보
        self.logger.info(f"텔레그램에서 '{process_name}' 프로세스 재시작 명령을 처리합니다.")
        
        result = await self._send_process_command("restart_process", process_name)
        
        if result.get("success", False):
            await message.edit_text(f"✅ '{process_name}' 프로세스 재시작 요청이 전송되었습니다.")
        else:
            error_msg = result.get("error", "알 수 없는 오류")
            await message.edit_text(f"❌ 오류: '{process_name}' 프로세스 재시작 실패\n{error_msg}")
    
    async def _handle_telegram_error(self, update: object, context: ContextTypes.DEFAULT_TYPE):
        """텔레그램 오류 처리"""
        self.logger.error(f"텔레그램 봇 오류: {context.error}")
        
    async def send_message(self, text: str, parse_mode: str = None) -> bool:
        """
        텔레그램 메시지 전송
        
        Args:
            text: 전송할 메시지 텍스트
            parse_mode: 파싱 모드 (Markdown, HTML 등)
            
        Returns:
            bool: 전송 성공 여부
        """
        if not self.bot or not self.allowed_chat_ids:
            return False
            
        try:
            for chat_id in self.allowed_chat_ids:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode=parse_mode
                )
            return True
        except Exception as e:
            self.logger.error(f"메시지 전송 중 오류 발생: {str(e)}")
            return False
