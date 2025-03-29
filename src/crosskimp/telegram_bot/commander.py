"""
텔레그램 명령 처리기 모듈

사용자로부터 명령을 수신하고 시스템에 전달하는 인터페이스를 제공합니다.
"""

import asyncio
import traceback
import uuid
import subprocess
import os
import sys
import signal
from typing import Dict, Any, Optional, List

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import CommandHandler, CallbackQueryHandler, ContextTypes, Application

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.config.app_config import get_config

# 로거 설정
logger = get_unified_logger(component=SystemComponent.TELEGRAM.value)

# 싱글톤 인스턴스
_telegram_commander_instance = None
_telegram_bot_instance = None
_telegram_app_instance = None

def get_telegram_commander():
    """글로벌 텔레그램 커맨더 인스턴스를 반환합니다."""
    global _telegram_commander_instance
    if _telegram_commander_instance is None:
        _telegram_commander_instance = TelegramCommander()
    return _telegram_commander_instance

def get_allowed_chat_ids() -> List[int]:
    """허용된 채팅 ID 목록 반환"""
    config = get_config()
    chat_ids_str = config.get_env("telegram.chat_id", "")
    
    if not chat_ids_str:
        return []
        
    if ',' in chat_ids_str:
        return [int(id.strip()) for id in chat_ids_str.split(',')]
    else:
        return [int(chat_ids_str.strip())]

async def initialize_telegram_bot() -> Bot:
    """텔레그램 봇 초기화 및 공유 인스턴스 반환"""
    global _telegram_bot_instance, _telegram_app_instance
    
    if _telegram_bot_instance is not None:
        return _telegram_bot_instance
        
    config = get_config()
    telegram_token = config.get_env("telegram.bot_token", "")
    
    if not telegram_token:
        logger.warning("텔레그램 봇 토큰이 설정되지 않았습니다.")
        return None
    
    try:
        # 텔레그램 봇 애플리케이션 생성
        app = Application.builder().token(telegram_token).build()
        _telegram_app_instance = app
        
        # 봇 객체 참조 가져오기
        _telegram_bot_instance = app.bot
        
        # 봇 초기화
        await app.initialize()
        
        logger.info("텔레그램 봇이 초기화되었습니다.")
        return _telegram_bot_instance
        
    except Exception as e:
        logger.error(f"텔레그램 봇 초기화 중 오류: {str(e)}")
        return None

class TelegramCommander:
    """
    텔레그램 봇과 이벤트 버스를 연결하는 명령 처리 클래스
    """
    
    def __init__(self):
        """텔레그램 커맨더 초기화"""
        # 텔레그램 봇 (공유 인스턴스 사용)
        self.bot = None
        self.app = None
        
        # 허용된 채팅 ID
        self.allowed_chat_ids = []
        
        # 초기화 완료 여부
        self.initialized = False
        
        # 로거
        self.logger = logger
        
        # 폴링 태스크
        self._polling_task = None
        
        # 오케스트레이터 인스턴스
        self.orchestrator = None
    
    def set_orchestrator(self, orchestrator):
        """
        오케스트레이터 인스턴스 설정
        
        Args:
            orchestrator: 오케스트레이터 인스턴스
        """
        self.orchestrator = orchestrator
        self.logger.info("텔레그램 커맨더에 오케스트레이터가 설정되었습니다.")
    
    async def start(self) -> bool:
        """
        텔레그램 명령 처리기 초기화 및 시작
        
        Returns:
            bool: 시작 성공 여부
        """
        try:
            self.logger.info("텔레그램 커맨더 초기화 중...")
            
            # 공유 봇 인스턴스 가져오기
            self.bot = await initialize_telegram_bot()
            if not self.bot:
                self.logger.error("텔레그램 봇 초기화 실패")
                return False
                
            self.app = _telegram_app_instance
            
            # 허용된 채팅 ID 가져오기
            self.allowed_chat_ids = get_allowed_chat_ids()
            
            # 핸들러 설정
            self._setup_telegram_handlers()
            
            # 봇 시작
            await self.app.start()
            
            # 폴링을 비동기로 시작 (백그라운드 태스크)
            self._polling_task = asyncio.create_task(self._run_polling())
            
            self.initialized = True
            self.logger.info("텔레그램 커맨더가 초기화되었습니다.")
            
            return True
            
        except Exception as e:
            self.logger.error(f"텔레그램 커맨더 초기화 중 오류 발생: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
    
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
    
    async def stop(self) -> bool:
        """
        텔레그램 명령 처리기 종료
        
        Returns:
            bool: 종료 성공 여부
        """
        try:
            self.logger.info("텔레그램 커맨더를 종료합니다...")
            
            # 폴링 태스크 취소
            if self._polling_task and not self._polling_task.done():
                self._polling_task.cancel()
                try:
                    await asyncio.wait_for(self._polling_task, timeout=2)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
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
        self.app.add_handler(CommandHandler("menu", self._handle_main_menu_command))
        
        # 프로세스 관리 명령어
        self.app.add_handler(CommandHandler("processes", self._handle_processes_command))
        self.app.add_handler(CommandHandler("system", self._handle_system_info_command))
        self.app.add_handler(CommandHandler("account", self._handle_account_info_command))
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
        프로세스 관련 명령을 오케스트레이터로 직접 전송
        
        Args:
            command: 명령어 (start_process, stop_process, restart_process)
            process_name: 프로세스 이름
            
        Returns:
            dict: 명령 처리 결과
        """
        try:
            if not self.orchestrator:
                self.logger.error("오케스트레이터가 설정되지 않았습니다. set_orchestrator()를 먼저 호출해야 합니다.")
                return {"success": False, "error": "오케스트레이터가 설정되지 않았습니다."}
                
            # 명령 처리 결과
            result = {"success": False}
            
            # 명령에 따라 오케스트레이터 메서드 직접 호출
            if command == "start_process":
                success = await self.orchestrator.start_process(process_name)
                result = {"success": success, "process_name": process_name}
                
            elif command == "stop_process":
                success = await self.orchestrator.stop_process(process_name)
                result = {"success": success, "process_name": process_name}
                
            elif command == "restart_process":
                # 재시작 로직
                await self.orchestrator.stop_process(process_name)
                await asyncio.sleep(1)  # 종료 대기
                success = await self.orchestrator.start_process(process_name)
                result = {"success": success, "process_name": process_name}
                
            elif command == "get_process_status":
                # 상태 조회 로직
                if not self.orchestrator.is_process_registered(process_name):
                    result = {"success": False, "error": f"프로세스 '{process_name}'이(가) 등록되지 않았습니다."}
                else:
                    is_running = self.orchestrator.is_process_running(process_name)
                    result = {
                        "success": True,
                        "process_name": process_name,
                        "status": "running" if is_running else "stopped"
                    }
            
            self.logger.info(f"프로세스 명령 '{command}', 프로세스: '{process_name}' 결과: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"프로세스 명령 전송 중 오류: {str(e)}")
            return {"success": False, "error": str(e)}
    
    # 텔레그램 명령어 핸들러 함수
    async def _handle_start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시작 명령 처리"""
        welcome_message = (
            "👋 크로스킴프 봇에 오신 것을 환영합니다!\n\n"
            "아래 메뉴에서 원하는 기능을 선택하세요."
        )
        
        # 메인 메뉴 인라인 키보드 생성
        keyboard = [
            [InlineKeyboardButton("🛠️ 프로그램 제어", callback_data="menu_processes")],
            [InlineKeyboardButton("📊 시스템 정보", callback_data="menu_system")],
            [InlineKeyboardButton("💰 주문/수익 정보", callback_data="menu_trades")],
            [InlineKeyboardButton("💼 계좌 정보", callback_data="menu_account")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(welcome_message, reply_markup=reply_markup)
    
    async def _handle_help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """도움말 명령 처리"""
        help_text = (
            "📌 **사용 가능한 명령어**\n\n"
            "/menu - 메인 메뉴 표시\n"
            "/status - 시스템 상태 확인\n"
            "/processes - 프로세스 관리 메뉴\n"
            "/system - 시스템 정보 메뉴\n"
            "/account - 계좌 정보 메뉴\n"
            "/help - 도움말 표시\n\n"
            "**프로세스 명령어**\n"
            "/start_process [프로세스명] - 프로세스 시작\n"
            "/stop_process [프로세스명] - 프로세스 중지\n"
            "/restart_process [프로세스명] - 프로세스 재시작\n\n"
            "사용 가능한 프로세스: orderbook, radar, trader, web_server"
        )
        
        # 인라인 키보드 추가
        keyboard = [[InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(help_text, reply_markup=reply_markup)
    
    async def _handle_main_menu_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """메인 메뉴 명령 처리"""
        keyboard = [
            [InlineKeyboardButton("🛠️ 프로그램 제어", callback_data="menu_processes")],
            [InlineKeyboardButton("📊 시스템 정보", callback_data="menu_system")],
            [InlineKeyboardButton("💰 주문/수익 정보", callback_data="menu_trades")],
            [InlineKeyboardButton("💼 계좌 정보", callback_data="menu_account")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("📋 메인 메뉴:", reply_markup=reply_markup)
    
    async def _handle_status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """상태 확인 명령 처리"""
        status_text = "✅ 시스템이 정상 작동 중입니다."
        
        # 인라인 키보드 추가
        keyboard = [
            [InlineKeyboardButton("📊 상세 정보", callback_data="system_status")],
            [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(status_text, reply_markup=reply_markup)
    
    async def _handle_processes_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """프로그램 제어 메뉴 표시"""
        keyboard = [
            [
                InlineKeyboardButton("📚 오더북 시작", callback_data="start_orderbook"),
                InlineKeyboardButton("📚 오더북 중지", callback_data="stop_orderbook")
            ],
            [
                InlineKeyboardButton("📡 레이더 시작[개발중]", callback_data="start_radar"),
                InlineKeyboardButton("📡 레이더 중지[개발중]", callback_data="stop_radar")
            ],
            [
                InlineKeyboardButton("🤖 트레이더 시작[개발중]", callback_data="start_trader"),
                InlineKeyboardButton("🤖 트레이더 중지[개발중]", callback_data="stop_trader")
            ],
            [
                InlineKeyboardButton("🌐 웹서버 시작[개발중]", callback_data="start_web_server"),
                InlineKeyboardButton("🌐 웹서버 중지[개발중]", callback_data="stop_web_server")
            ],
            [
                InlineKeyboardButton("🚀 전체 시작", callback_data="start_all"),
                InlineKeyboardButton("🛑 전체 중지", callback_data="stop_all")
            ],
            [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("🛠️ 프로그램 제어:", reply_markup=reply_markup)
    
    async def _handle_system_info_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시스템 정보 메뉴 표시"""
        keyboard = [
            [InlineKeyboardButton("🔍 시스템 작동정보[개발중]", callback_data="system_status")],
            [InlineKeyboardButton("🔌 웹소켓 연결/구독 상태[개발중]", callback_data="websocket_status")],
            [InlineKeyboardButton("📈 시스템 성능[개발중]", callback_data="system_performance")],
            [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("📊 시스템 정보:", reply_markup=reply_markup)
    
    async def _handle_account_info_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """계좌 정보 메뉴 표시"""
        keyboard = [
            [InlineKeyboardButton("💰 보유자산 현황[개발중]", callback_data="account_balance")],
            [InlineKeyboardButton("💱 거래소별 잔고[개발중]", callback_data="exchange_balance")],
            [InlineKeyboardButton("📊 자산 분포[개발중]", callback_data="asset_distribution")],
            [InlineKeyboardButton("📝 API 키 상태[개발중]", callback_data="api_key_status")],
            [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("💼 계좌 정보:", reply_markup=reply_markup)
    
    async def _handle_button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """인라인 키보드 버튼 콜백 처리"""
        query = update.callback_query
        await query.answer()  # 콜백 응답
        
        callback_data = query.data
        
        # 메인 메뉴 네비게이션
        if callback_data == "back_to_main":
            return await self._show_main_menu(query)
        elif callback_data.startswith("menu_"):
            return await self._handle_menu_navigation(query, callback_data)
            
        # 전체 시작/중지 처리
        if callback_data in ["start_all", "stop_all"]:
            await self._handle_all_processes(query, callback_data)
            return
            
        # 시스템 정보 요청 처리
        if callback_data.startswith("system_") or callback_data.startswith("websocket_"):
            await self._handle_system_info_request(query, callback_data)
            return
            
        # 계좌 정보 요청 처리
        if callback_data.startswith("account_") or callback_data.startswith("exchange_") or callback_data.startswith("asset_") or callback_data.startswith("api_"):
            await self._handle_account_info_request(query, callback_data)
            return
            
        # 일반 프로세스 시작/중지 처리
        if "_" in callback_data:
            action, process_name = callback_data.split('_', 1)
            
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
    
    async def _show_main_menu(self, query):
        """메인 메뉴 표시"""
        keyboard = [
            [InlineKeyboardButton("🛠️ 프로그램 제어", callback_data="menu_processes")],
            [InlineKeyboardButton("📊 시스템 정보[개발중]", callback_data="menu_system")],
            [InlineKeyboardButton("💰 주문/수익 정보[개발중]", callback_data="menu_trades")],
            [InlineKeyboardButton("💼 계좌 정보[개발중]", callback_data="menu_account")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("📋 메인 메뉴:", reply_markup=reply_markup)
    
    async def _handle_menu_navigation(self, query, callback_data):
        """메뉴 네비게이션 처리"""
        menu_type = callback_data.split('_')[1]
        
        if menu_type == "processes":
            # 프로그램 제어 메뉴
            keyboard = [
                [
                    InlineKeyboardButton("📚 오더북 시작", callback_data="start_orderbook"),
                    InlineKeyboardButton("📚 오더북 중지", callback_data="stop_orderbook")
                ],
                [
                    InlineKeyboardButton("📡 레이더 시작[개발중]", callback_data="start_radar"),
                    InlineKeyboardButton("📡 레이더 중지[개발중]", callback_data="stop_radar")
                ],
                [
                    InlineKeyboardButton("🤖 트레이더 시작[개발중]", callback_data="start_trader"),
                    InlineKeyboardButton("🤖 트레이더 중지[개발중]", callback_data="stop_trader")
                ],
                [
                    InlineKeyboardButton("🌐 웹서버 시작[개발중]", callback_data="start_web_server"),
                    InlineKeyboardButton("🌐 웹서버 중지[개발중]", callback_data="stop_web_server")
                ],
                [
                    InlineKeyboardButton("🚀 전체 시작", callback_data="start_all"),
                    InlineKeyboardButton("🛑 전체 중지", callback_data="stop_all")
                ],
                [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("🛠️ 프로그램 제어:", reply_markup=reply_markup)
            
        elif menu_type == "system":
            # 시스템 정보 메뉴
            keyboard = [
                [InlineKeyboardButton("🔍 시스템 작동정보[개발중]", callback_data="system_status")],
                [InlineKeyboardButton("🔌 웹소켓 연결/구독 상태[개발중]", callback_data="websocket_status")],
                [InlineKeyboardButton("📈 시스템 성능[개발중]", callback_data="system_performance")],
                [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("📊 시스템 정보:", reply_markup=reply_markup)
            
        elif menu_type == "trades":
            # 주문/수익 정보 메뉴
            keyboard = [
                [InlineKeyboardButton("📅 일간 수익현황[개발중]", callback_data="trade_daily")],
                [InlineKeyboardButton("📆 월간 수익현황[개발중]", callback_data="trade_monthly")],
                [InlineKeyboardButton("📊 거래통계[개발중]", callback_data="trade_stats")],
                [InlineKeyboardButton("📜 최근 주문내역 (20개)[개발중]", callback_data="trade_recent")],
                [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("💰 주문/수익 정보:", reply_markup=reply_markup)
            
        elif menu_type == "account":
            # 계좌 정보 메뉴
            keyboard = [
                [InlineKeyboardButton("💰 보유자산 현황[개발중]", callback_data="account_balance")],
                [InlineKeyboardButton("💱 거래소별 잔고[개발중]", callback_data="exchange_balance")],
                [InlineKeyboardButton("📊 자산 분포[개발중]", callback_data="asset_distribution")],
                [InlineKeyboardButton("📝 API 키 상태[개발중]", callback_data="api_key_status")],
                [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("💼 계좌 정보:", reply_markup=reply_markup)
    
    async def _handle_all_processes(self, query, callback_data):
        """모든 프로세스 시작/중지 처리"""
        action = callback_data.split('_')[0]  # start 또는 stop
        
        command_text = "시작" if action == "start" else "중지"
        await query.edit_message_text(f"⏳ 모든 프로세스 {command_text} 요청 중...")
        
        # 프로세스 목록
        processes = ["orderbook", "radar", "trader", "web_server"]
        success_count = 0
        
        # 각 프로세스에 명령 전송
        for process_name in processes:
            command = f"{action}_process"
            result = await self._send_process_command(command, process_name)
            if result.get("success", False):
                success_count += 1
                
        # 결과 메시지 표시
        if success_count == len(processes):
            await query.edit_message_text(f"✅ 모든 프로세스 {command_text} 요청이 전송되었습니다.")
        else:
            await query.edit_message_text(f"⚠️ {len(processes)}개 중 {success_count}개 프로세스 {command_text} 요청 성공")
    
    async def _handle_system_info_request(self, query, callback_data):
        """시스템 정보 요청 처리"""
        await query.edit_message_text(f"⏳ 정보 요청 처리 중...")
        
        try:
            # 오케스트레이터가 없으면 오류 메시지 표시
            if not self.orchestrator:
                await query.edit_message_text(f"❌ 오류: 오케스트레이터가 설정되지 않았습니다.")
                return
            
            # 키보드 생성
            keyboard = [[InlineKeyboardButton("🔙 돌아가기", callback_data="menu_system")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # 결과 메시지 전송
            await query.edit_message_text(
                "✅ 시스템 정보:\n\n" + 
                "시스템 정보 기능은 아직 개발 중입니다. 업데이트를 기다려주세요.",
                reply_markup=reply_markup
            )
            
        except Exception as e:
            self.logger.error(f"시스템 정보 요청 처리 중 오류: {str(e)}")
            await query.edit_message_text(f"❌ 오류: 시스템 정보 요청 처리 실패\n{str(e)}")
    
    async def _handle_account_info_request(self, query, callback_data):
        """계좌 정보 요청 처리"""
        await query.edit_message_text(f"⏳ 계좌 정보 요청 처리 중...")
        
        try:
            # 오케스트레이터가 없으면 오류 메시지 표시
            if not self.orchestrator:
                await query.edit_message_text(f"❌ 오류: 오케스트레이터가 설정되지 않았습니다.")
                return
            
            # 키보드 생성
            keyboard = [[InlineKeyboardButton("🔙 돌아가기", callback_data="menu_account")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # 결과 메시지 전송
            await query.edit_message_text(
                "✅ 계좌 정보:\n\n" + 
                "계좌 정보 기능은 아직 개발 중입니다. 업데이트를 기다려주세요.",
                reply_markup=reply_markup
            )
            
        except Exception as e:
            self.logger.error(f"계좌 정보 요청 처리 중 오류: {str(e)}")
            await query.edit_message_text(f"❌ 오류: 계좌 정보 요청 처리 실패\n{str(e)}")
    
    async def _handle_start_process_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """프로세스 시작 명령 처리"""
        # 로그 추가 - 명령 수신
        user = update.message.from_user
        self.logger.info(f"텔레그램에서 '/start_process' 명령을 수신했습니다. (사용자: {user.username or user.id})")
        
        args = context.args
        if not args or len(args) < 1:
            error_text = "❌ 오류: 프로세스 이름을 지정해야 합니다.\n사용법: /start_process [프로세스명]"
            
            # 인라인 키보드 추가
            keyboard = [
                [InlineKeyboardButton("🛠️ 프로그램 제어 메뉴", callback_data="menu_processes")],
                [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(error_text, reply_markup=reply_markup)
            return
        
        process_name = args[0].lower()
        # 로그 추가 - 구체적인 프로세스 정보
        self.logger.info(f"텔레그램에서 '{process_name}' 프로세스 시작 명령을 처리합니다.")
        
        message = await update.message.reply_text("⏳ 프로세스 시작 요청 중...")
        
        result = await self._send_process_command("start_process", process_name)
        
        # 인라인 키보드 생성
        keyboard = [
            [InlineKeyboardButton("🛠️ 프로그램 제어 메뉴", callback_data="menu_processes")],
            [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if result.get("success", False):
            await message.edit_text(f"✅ '{process_name}' 프로세스 시작 요청이 전송되었습니다.", reply_markup=reply_markup)
        else:
            error_msg = result.get("error", "알 수 없는 오류")
            await message.edit_text(f"❌ 오류: '{process_name}' 프로세스 시작 실패\n{error_msg}", reply_markup=reply_markup)
    
    async def _handle_stop_process_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """프로세스 중지 명령 처리"""
        # 로그 추가 - 명령 수신
        user = update.message.from_user
        self.logger.info(f"텔레그램에서 '/stop_process' 명령을 수신했습니다. (사용자: {user.username or user.id})")
        
        args = context.args
        if not args or len(args) < 1:
            error_text = "❌ 오류: 프로세스 이름을 지정해야 합니다.\n사용법: /stop_process [프로세스명]"
            
            # 인라인 키보드 추가
            keyboard = [
                [InlineKeyboardButton("🛠️ 프로그램 제어 메뉴", callback_data="menu_processes")],
                [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(error_text, reply_markup=reply_markup)
            return
        
        process_name = args[0].lower()
        # 로그 추가 - 구체적인 프로세스 정보
        self.logger.info(f"텔레그램에서 '{process_name}' 프로세스 중지 명령을 처리합니다.")
        
        message = await update.message.reply_text("⏳ 프로세스 중지 요청 중...")
        
        result = await self._send_process_command("stop_process", process_name)
        
        # 인라인 키보드 생성
        keyboard = [
            [InlineKeyboardButton("🛠️ 프로그램 제어 메뉴", callback_data="menu_processes")],
            [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if result.get("success", False):
            await message.edit_text(f"✅ '{process_name}' 프로세스 중지 요청이 전송되었습니다.", reply_markup=reply_markup)
        else:
            error_msg = result.get("error", "알 수 없는 오류")
            await message.edit_text(f"❌ 오류: '{process_name}' 프로세스 중지 실패\n{error_msg}", reply_markup=reply_markup)
    
    async def _handle_restart_process_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """프로세스 재시작 명령 처리"""
        # 로그 추가 - 명령 수신
        user = update.message.from_user
        self.logger.info(f"텔레그램에서 '/restart_process' 명령을 수신했습니다. (사용자: {user.username or user.id})")
        
        args = context.args
        if not args or len(args) < 1:
            error_text = "❌ 오류: 프로세스 이름을 지정해야 합니다.\n사용법: /restart_process [프로세스명]"
            
            # 인라인 키보드 추가
            keyboard = [
                [InlineKeyboardButton("🛠️ 프로그램 제어 메뉴", callback_data="menu_processes")],
                [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(error_text, reply_markup=reply_markup)
            return
        
        process_name = args[0].lower()
        # 로그 추가 - 구체적인 프로세스 정보
        self.logger.info(f"텔레그램에서 '{process_name}' 프로세스 재시작 명령을 처리합니다.")
        
        message = await update.message.reply_text("⏳ 프로세스 재시작 요청 중...")
        
        result = await self._send_process_command("restart_process", process_name)
        
        # 인라인 키보드 생성
        keyboard = [
            [InlineKeyboardButton("🛠️ 프로그램 제어 메뉴", callback_data="menu_processes")],
            [InlineKeyboardButton("🔙 메인메뉴", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if result.get("success", False):
            await message.edit_text(f"✅ '{process_name}' 프로세스 재시작 요청이 전송되었습니다.", reply_markup=reply_markup)
        else:
            error_msg = result.get("error", "알 수 없는 오류")
            await message.edit_text(f"❌ 오류: '{process_name}' 프로세스 재시작 실패\n{error_msg}", reply_markup=reply_markup)
    
    async def _handle_telegram_error(self, update: object, context: ContextTypes.DEFAULT_TYPE):
        """텔레그램 오류 처리"""
        self.logger.error(f"텔레그램 봇 오류: {context.error}")

    # 이벤트 버스와 관련된 메서드들은 더 이상 필요하지 않으므로 삭제됨
    # 필요한 기능은 추후 오케스트레이터를 직접 호출하는 방식으로 재구현 가능
