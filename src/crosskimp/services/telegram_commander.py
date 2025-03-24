"""
텔레그램 봇 어댑터 모듈

이 모듈은 텔레그램 봇 명령을 이벤트 버스에 연결하고, 
이벤트 버스의 메시지를 텔레그램 알림으로 변환합니다.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import re

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.sys_event_bus import EventType
from crosskimp.common.events import get_event_bus
from crosskimp.services.command_handler import get_command_handler
from crosskimp.common.config.app_config import get_config

# python-telegram-bot 라이브러리 임포트
try:
    from telegram import Update, Bot
    from telegram.ext import Application, CommandHandler, ContextTypes, CallbackContext
    from telegram.error import TelegramError
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False

# 로거 설정
logger = get_unified_logger()

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
        """텔레그램 어댑터 초기화"""
        # 이벤트 버스 참조
        self.event_bus = None
        
        # 명령 핸들러 참조
        self.command_handler = None
        
        # 텔레그램 봇 서비스 참조
        self.telegram_app = None
        self.bot = None
        self.chat_id = None
        self.bot_token = None
        
        # 초기화 완료 여부
        self.initialized = False
        
        # 상태 메시지 캐시
        self.last_status_message = None
        
        # 로거
        self._logger = logger
    
    async def initialize(self):
        """텔레그램 어댑터 초기화"""
        if self.initialized:
            return
            
        # 환경변수 로드
        config = get_config()
        self.bot_token = config.get_env("telegram.bot_token")
        self.chat_id = config.get_env("telegram.chat_id")
        
        if not self.bot_token or not self.chat_id:
            self._logger.error("텔레그램 봇 토큰 또는 채팅 ID가 설정되지 않았습니다.")
            return
            
        try:
            self.chat_id = int(self.chat_id)
        except ValueError:
            self._logger.error(f"유효하지 않은 채팅 ID 형식: {self.chat_id}")
            return
            
        # 이벤트 버스 참조
        self.event_bus = get_event_bus()
        
        # 명령 핸들러 참조
        self.command_handler = get_command_handler()
        if not self.command_handler.is_initialized():
            await self.command_handler.initialize()
        
        # 이벤트 핸들러 등록
        if self.event_bus:
            self.event_bus.register_handler(EventType.STATUS_UPDATE, self._handle_status_update)
            self.event_bus.register_handler(EventType.ERROR, self._handle_error)
            self.event_bus.register_handler(EventType.PROCESS_START, self._handle_process_event)
            self.event_bus.register_handler(EventType.PROCESS_STOP, self._handle_process_event)
        
        # 텔레그램 봇 설정
        if TELEGRAM_AVAILABLE:
            try:
                # 봇 초기화
                self.bot = Bot(token=self.bot_token)
                self.telegram_app = Application.builder().token(self.bot_token).build()
                
                # 텔레그램 명령어 핸들러 설정
                self._setup_telegram_handlers()
                
                # 비동기로 봇 시작
                asyncio.create_task(self._start_telegram_bot())
                
                self._logger.info("텔레그램 봇이 초기화되었습니다.")
            except Exception as e:
                self._logger.error(f"텔레그램 봇 초기화 실패: {str(e)}")
                return
        else:
            self._logger.error("python-telegram-bot 라이브러리가 설치되지 않았습니다.")
            self._logger.error("pip install python-telegram-bot 명령으로 설치하세요.")
            return
        
        self.initialized = True
        self._logger.info("텔레그램 커맨더가 초기화되었습니다.")
        
        # 시작 메시지 전송
        await self.send_message("🚀 텔레그램 봇이 시작되었습니다.\n명령어 목록을 보려면 /help를 입력하세요.")
    
    async def _start_telegram_bot(self):
        """텔레그램 봇 시작"""
        try:
            await self.telegram_app.initialize()
            await self.telegram_app.start()
            await self.telegram_app.updater.start_polling()
            self._logger.info("텔레그램 봇 폴링이 시작되었습니다.")
        except Exception as e:
            self._logger.error(f"텔레그램 봇 시작 실패: {str(e)}")
    
    async def shutdown(self):
        """텔레그램 커맨더 종료"""
        if not self.initialized:
            return
            
        # 종료 메시지 전송
        await self.send_message("🔄 텔레그램 봇이 종료됩니다...")
            
        # 텔레그램 봇 종료
        if self.telegram_app:
            try:
                await self.telegram_app.updater.stop()
                await self.telegram_app.stop()
                await self.telegram_app.shutdown()
                self._logger.info("텔레그램 봇이 종료되었습니다.")
            except Exception as e:
                self._logger.error(f"텔레그램 봇 종료 실패: {str(e)}")
            
        # 이벤트 핸들러 등록 해제
        if self.event_bus:
            self.event_bus.unregister_handler(EventType.STATUS_UPDATE, self._handle_status_update)
            self.event_bus.unregister_handler(EventType.ERROR, self._handle_error)
            self.event_bus.unregister_handler(EventType.PROCESS_START, self._handle_process_event)
            self.event_bus.unregister_handler(EventType.PROCESS_STOP, self._handle_process_event)
        
        self.initialized = False
        self._logger.info("텔레그램 커맨더가 종료되었습니다.")
    
    def _setup_telegram_handlers(self):
        """텔레그램 명령어 핸들러 설정"""
        # 기본 명령어
        self.telegram_app.add_handler(CommandHandler("start", self._handle_start_command))
        self.telegram_app.add_handler(CommandHandler("help", self._handle_help_command))
        self.telegram_app.add_handler(CommandHandler("status", self._handle_status_command))
        
        # 오더북 수집기 명령어
        self.telegram_app.add_handler(CommandHandler("start_ob", self._handle_start_ob_command))
        self.telegram_app.add_handler(CommandHandler("stop_ob", self._handle_stop_ob_command))
        self.telegram_app.add_handler(CommandHandler("restart_ob", self._handle_restart_ob_command))
        
        # 레이더 명령어
        self.telegram_app.add_handler(CommandHandler("start_radar", self._handle_start_radar_command))
        self.telegram_app.add_handler(CommandHandler("stop_radar", self._handle_stop_radar_command))
        self.telegram_app.add_handler(CommandHandler("restart_radar", self._handle_restart_radar_command))
        
        # 오류 핸들러 추가
        self.telegram_app.add_error_handler(self._handle_telegram_error)
        
        self._logger.info("텔레그램 명령어 핸들러가 등록되었습니다.")
    
    # 텔레그램 명령어 핸들러 함수
    async def _handle_start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시작 명령 처리"""
        message = (
            "👋 크로스킴프 봇에 오신 것을 환영합니다!\n\n"
            "이 봇을 통해 시스템 상태를 확인하고 프로세스를 제어할 수 있습니다.\n"
            "명령어 목록을 보려면 /help를 입력하세요."
        )
        await update.message.reply_text(message)
    
    async def _handle_help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """도움말 명령 처리"""
        help_text = (
            "📌 **사용 가능한 명령어**\n\n"
            "**일반 명령어**\n"
            "/status - 시스템 상태 확인\n"
            "/help - 도움말 표시\n\n"
            "**오더북 수집기 명령어**\n"
            "/start_ob - 오더북 수집기 시작\n"
            "/stop_ob - 오더북 수집기 중지\n"
            "/restart_ob - 오더북 수집기 재시작\n\n"
            "**레이더 명령어**\n"
            "/start_radar - 레이더 시작\n"
            "/stop_radar - 레이더 중지\n"
            "/restart_radar - 레이더 재시작"
        )
        await update.message.reply_text(help_text)
    
    async def _handle_status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """상태 확인 명령 처리"""
        # 명령 처리 중임을 알림
        status_message = await update.message.reply_text("⏳ 시스템 상태를 확인 중입니다...")
        
        try:
            # _cmd_status 함수 호출
            status = await self._cmd_status([], update.effective_chat.id)
            
            # 결과 전송
            await status_message.edit_text(status)
        except Exception as e:
            self._logger.error(f"상태 명령 처리 중 오류: {str(e)}")
            await status_message.edit_text(f"⚠️ 상태 확인 중 오류가 발생했습니다: {str(e)}")
    
    async def _handle_start_ob_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """오더북 수집기 시작 명령 처리"""
        message = await update.message.reply_text("⏳ 오더북 수집기 시작 중...")
        result = await self._cmd_start_ob([], update.effective_chat.id)
        await message.edit_text(result)
    
    async def _handle_stop_ob_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """오더북 수집기 중지 명령 처리"""
        message = await update.message.reply_text("⏳ 오더북 수집기 중지 중...")
        result = await self._cmd_stop_ob([], update.effective_chat.id)
        await message.edit_text(result)
    
    async def _handle_restart_ob_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """오더북 수집기 재시작 명령 처리"""
        message = await update.message.reply_text("⏳ 오더북 수집기 재시작 중...")
        result = await self._cmd_restart_ob([], update.effective_chat.id)
        await message.edit_text(result)
    
    async def _handle_start_radar_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """레이더 시작 명령 처리"""
        message = await update.message.reply_text("⏳ 레이더 시작 중...")
        result = await self._cmd_start_radar([], update.effective_chat.id)
        await message.edit_text(result)
    
    async def _handle_stop_radar_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """레이더 중지 명령 처리"""
        message = await update.message.reply_text("⏳ 레이더 중지 중...")
        result = await self._cmd_stop_radar([], update.effective_chat.id)
        await message.edit_text(result)
    
    async def _handle_restart_radar_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """레이더 재시작 명령 처리"""
        message = await update.message.reply_text("⏳ 레이더 재시작 중...")
        result = await self._cmd_restart_radar([], update.effective_chat.id)
        await message.edit_text(result)
    
    async def _handle_telegram_error(self, update: object, context: ContextTypes.DEFAULT_TYPE):
        """텔레그램 오류 처리"""
        self._logger.error(f"텔레그램 봇 오류: {context.error}")
        try:
            # 스택 트레이스 정보 로깅
            self._logger.error(f"에러 정보: {context.error.__class__.__name__}: {context.error}")
            if update and hasattr(update, 'effective_chat'):
                self._logger.error(f"채팅 ID: {update.effective_chat.id}")
            if update and hasattr(update, 'effective_message') and update.effective_message:
                self._logger.error(f"메시지: {update.effective_message.text}")
        except Exception as e:
            self._logger.error(f"오류 처리 중 추가 오류 발생: {str(e)}")
    
    async def send_message(self, text: str, parse_mode: str = None) -> bool:
        """
        텔레그램 메시지 전송
        
        Args:
            text: 전송할 메시지 텍스트
            parse_mode: 파싱 모드 (Markdown, HTML 등)
            
        Returns:
            bool: 전송 성공 여부
        """
        if not self.bot or not self.chat_id:
            self._logger.error("텔레그램 봇 또는 채팅 ID가 설정되지 않았습니다.")
            return False
            
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=text,
                parse_mode=parse_mode
            )
            return True
        except Exception as e:
            self._logger.error(f"텔레그램 메시지 전송 실패: {str(e)}")
            return False
    
    # 기존 명령어 핸들러 함수들 (이전과 동일하게 유지)
    async def _cmd_start_ob(self, args: List[str], chat_id: int) -> str:
        """오더북 수집기 시작 명령"""
        result = await self.command_handler.send_command(
            "start_process", 
            {"process_name": "ob_collector"},
            source="telegram",
            wait_response=True
        )
        return "오더북 수집기 시작 명령을 전송했습니다." + (f"\n⚠️ 오류: {result.get('error')}" if not result.get('success', True) else "")
    
    async def _cmd_stop_ob(self, args: List[str], chat_id: int) -> str:
        """오더북 수집기 중지 명령"""
        result = await self.command_handler.send_command(
            "stop_process", 
            {"process_name": "ob_collector"},
            source="telegram",
            wait_response=True
        )
        return "오더북 수집기 중지 명령을 전송했습니다." + (f"\n⚠️ 오류: {result.get('error')}" if not result.get('success', True) else "")
    
    async def _cmd_restart_ob(self, args: List[str], chat_id: int) -> str:
        """오더북 수집기 재시작 명령"""
        result = await self.command_handler.send_command(
            "restart_process", 
            {"process_name": "ob_collector"},
            source="telegram",
            wait_response=True
        )
        return "오더북 수집기 재시작 명령을 전송했습니다." + (f"\n⚠️ 오류: {result.get('error')}" if not result.get('success', True) else "")
    
    async def _cmd_start_radar(self, args: List[str], chat_id: int) -> str:
        """레이더 시작 명령"""
        result = await self.command_handler.send_command(
            "start_process", 
            {"process_name": "radar"},
            source="telegram",
            wait_response=True
        )
        return "레이더 시작 명령을 전송했습니다." + (f"\n⚠️ 오류: {result.get('error')}" if not result.get('success', True) else "")
    
    async def _cmd_stop_radar(self, args: List[str], chat_id: int) -> str:
        """레이더 중지 명령"""
        result = await self.command_handler.send_command(
            "stop_process", 
            {"process_name": "radar"},
            source="telegram",
            wait_response=True
        )
        return "레이더 중지 명령을 전송했습니다." + (f"\n⚠️ 오류: {result.get('error')}" if not result.get('success', True) else "")
    
    async def _cmd_restart_radar(self, args: List[str], chat_id: int) -> str:
        """레이더 재시작 명령"""
        result = await self.command_handler.send_command(
            "restart_process", 
            {"process_name": "radar"},
            source="telegram",
            wait_response=True
        )
        return "레이더 재시작 명령을 전송했습니다." + (f"\n⚠️ 오류: {result.get('error')}" if not result.get('success', True) else "")
    
    async def _cmd_status(self, args: List[str], chat_id: int) -> str:
        """시스템 상태 확인 명령"""
        try:
            # 명령 핸들러를 통해 상태 요청 (응답 대기)
            status = await self.command_handler.send_command(
                "get_status",
                {},
                source="telegram",
                wait_response=True,
                timeout=5.0
            )
            
            if not status or 'error' in status:
                return f"⚠️ 상태 정보 조회 중 오류 발생: {status.get('error', '알 수 없는 오류')}"
                
            # 응답받은 상태로 메시지 구성
            status_msg = "📊 시스템 상태 보고:\n\n"
            
            # 시스템 가동 상태
            status_msg += f"시스템: {'✅ 실행 중' if status['system_running'] else '❌ 중지됨'}\n"
            
            # 프로세스 상태
            status_msg += "\n📋 프로세스 상태:\n"
            for name, proc_status in status["processes"].items():
                running = proc_status.get("running", False)
                uptime = proc_status.get("uptime", 0)
                restart_count = proc_status.get("restart_count", 0)
                
                # 가동 시간 포맷팅
                hours, remainder = divmod(int(uptime or 0), 3600)
                minutes, seconds = divmod(remainder, 60)
                uptime_str = f"{hours}시간 {minutes}분 {seconds}초"
                
                status_msg += f"- {name}: {'✅ 실행 중' if running else '❌ 중지됨'}"
                if running:
                    status_msg += f" (가동시간: {uptime_str})"
                if restart_count > 0:
                    status_msg += f" (재시작: {restart_count}회)"
                status_msg += "\n"
            
            # 통계
            status_msg += f"\n⏱ 가동 시간: {int(status['stats']['uptime'] // 3600)}시간 {int((status['stats']['uptime'] % 3600) // 60)}분\n"
            
            # 이벤트 버스 통계
            bus_stats = status.get("event_bus_stats", {})
            status_msg += f"\n📨 이벤트 통계:\n"
            status_msg += f"- 발행: {bus_stats.get('published_events', 0)}\n"
            status_msg += f"- 처리: {bus_stats.get('processed_events', 0)}\n"
            status_msg += f"- 오류: {bus_stats.get('errors', 0)}\n"
            
            # 캐시에 저장
            self.last_status_message = status_msg
            
            return status_msg
            
        except Exception as e:
            # 기타 오류 발생 시
            self._logger.error(f"상태 조회 중 오류: {str(e)}")
            return f"⚠️ 상태 정보 조회 중 오류 발생: {str(e)}"
    
    # 이벤트 핸들러
    async def _handle_status_update(self, data: Dict):
        """
        상태 업데이트 이벤트 처리
        
        Args:
            data: 상태 이벤트 데이터
        """
        # 주기적인 상태 업데이트는 텔레그램으로 전송하지 않음
        # 필요한 경우 여기서 중요한 상태 변경만 알림으로 전송
        pass
    
    async def _handle_error(self, data: Dict):
        """
        오류 이벤트 처리
        
        Args:
            data: 오류 이벤트 데이터
        """
        message = data.get("message", "알 수 없는 오류")
        source = data.get("source", "unknown")
        severity = data.get("severity", "error")
        
        # 로깅
        self._logger.error(f"오류 발생: [{severity.upper()}] {source} - {message}")
        
        # 심각한 오류는 텔레그램으로 전송
        if severity == "critical":
            error_message = f"🚨 심각한 오류 발생!\n\n소스: {source}\n메시지: {message}"
            await self.send_message(error_message)
    
    async def _handle_process_event(self, data: Dict):
        """
        프로세스 이벤트 처리
        
        Args:
            data: 프로세스 이벤트 데이터
        """
        event_type = data.get("event_type")
        process_name = data.get("process_name")
        description = data.get("description", process_name)
        was_error = data.get("was_error", False)
        
        if not process_name:
            return
            
        # 프로세스 이벤트 로깅
        if event_type == EventType.PROCESS_START.name:
            self._logger.info(f"프로세스 시작됨: {description}")
            # 프로세스 시작 알림
            await self.send_message(f"✅ 프로세스 시작됨: {description}")
        elif event_type == EventType.PROCESS_STOP.name:
            status = "오류로 인해 중지됨" if was_error else "중지됨"
            self._logger.info(f"프로세스 {status}: {description}")
            
            # 오류로 인한 중지는 알림
            if was_error:
                await self.send_message(f"⚠️ 프로세스 오류로 인해 중지됨: {description}")
            else:
                await self.send_message(f"🛑 프로세스 중지됨: {description}")
    
    async def send_system_notification(self, message: str, level: str = "info"):
        """
        시스템 알림 메시지 전송
        
        Args:
            message: 알림 메시지
            level: 알림 레벨 (info, warning, error)
        """
        # 로깅
        if level == "info":
            self._logger.info(f"시스템 알림: {message}")
        elif level == "warning":
            self._logger.warning(f"시스템 경고: {message}")
        elif level == "error":
            self._logger.error(f"시스템 오류: {message}")
        
        # 텔레그램 메시지 전송
        if level == "info":
            await self.send_message(f"ℹ️ {message}")
        elif level == "warning":
            await self.send_message(f"⚠️ {message}")
        elif level == "error":
            await self.send_message(f"🚨 {message}")
    
    def is_initialized(self) -> bool:
        """
        초기화 완료 여부 확인
        
        Returns:
            bool: 초기화 완료 여부
        """
        return self.initialized
