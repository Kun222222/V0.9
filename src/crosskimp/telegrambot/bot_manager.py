"""
텔레그램 봇 관리자 - 텔레그램 봇 연결 및 명령어 처리를 관리합니다.
"""

import os
import asyncio
import logging
from typing import Dict, List, Optional, Callable, Any, Union
import telegram
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, CallbackContext, ContextTypes

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import LOG_TELEGRAM
# 새로운 이벤트 시스템 임포트 추가
from crosskimp.common.events import (
    get_component_event_bus,
    Component,
    StatusEventTypes,
    TelegramEventTypes
)

# 순환 참조 제거: process_manager 직접 import 제거
# from crosskimp.system_manager.process_manager import get_process_status, start_process, stop_process, restart_process
from .command_handler import register_commands
from .notification import NotificationService

# 로거 설정
logger = get_unified_logger()

class TelegramBotManager:
    """텔레그램 봇 관리 클래스"""
    
    def __init__(self, token: Optional[str] = None):
        """
        텔레그램 봇 관리자를 초기화합니다.
        
        Args:
            token: 텔레그램 봇 토큰 (없으면 환경 변수에서 가져옴)
        """
        self.token = token or os.environ.get("TELEGRAM_BOT_TOKEN")
        if not self.token:
            raise ValueError("텔레그램 봇 토큰이 필요합니다. 환경 변수 TELEGRAM_BOT_TOKEN을 설정하세요.")
            
        # 봇 및 애플리케이션 초기화
        self.bot = Bot(self.token)
        self.application = Application.builder().token(self.token).build()
        
        # 관리자 채팅 ID 목록
        self.admin_chat_ids = self._load_admin_chat_ids()
        
        # 이벤트 버스 (나중에 초기화)
        self.event_bus = None
        
        # 알림 서비스 초기화
        self.notification_service = NotificationService(self)
        
        # 기본 명령어 등록
        self._register_default_commands()
        
        # 실행 중 플래그
        self.is_running = False
        
        logger.info(f"{LOG_TELEGRAM} 텔레그램 봇 관리자 초기화됨")
        
    def _load_admin_chat_ids(self) -> List[int]:
        """
        관리자 채팅 ID 목록을 불러옵니다.
        환경 변수나 설정 파일에서 가져올 수 있습니다.
        
        Returns:
            List[int]: 관리자 채팅 ID 목록
        """
        # 먼저 TELEGRAM_CHAT_ID 환경 변수 확인
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        if chat_id:
            try:
                return [int(chat_id.strip())]
            except ValueError:
                logger.error(f"{LOG_TELEGRAM} 잘못된 TELEGRAM_CHAT_ID 형식")
                
        # 없으면 TELEGRAM_ADMIN_CHAT_IDS 확인 (여러 ID 지원)
        admin_ids_str = os.environ.get("TELEGRAM_ADMIN_CHAT_IDS", "")
        if admin_ids_str:
            try:
                return [int(id_str.strip()) for id_str in admin_ids_str.split(",")]
            except ValueError:
                logger.error(f"{LOG_TELEGRAM} 잘못된 관리자 채팅 ID 형식")
        
        logger.warning(f"{LOG_TELEGRAM} 관리자 채팅 ID가 설정되지 않음, 모든 사용자가 접근 가능")
        return []
        
    def _register_default_commands(self) -> None:
        """기본 명령어 핸들러를 등록합니다."""
        register_commands(self.application)
    
    def is_admin(self, chat_id: int) -> bool:
        """
        주어진 채팅 ID가 관리자인지 확인합니다.
        관리자 ID가 설정되지 않았다면 모든 사용자를 허용합니다.
        
        Args:
            chat_id: 확인할 채팅 ID
            
        Returns:
            bool: 관리자 여부
        """
        return not self.admin_chat_ids or chat_id in self.admin_chat_ids
    
    async def start(self) -> None:
        """텔레그램 봇을 시작합니다."""
        if self.is_running:
            logger.warning(f"{LOG_TELEGRAM} 텔레그램 봇이 이미 실행 중입니다.")
            return
            
        logger.info(f"{LOG_TELEGRAM} 텔레그램 봇 시작 중...")
        
        try:
            # 이벤트 버스 초기화
            self.event_bus = get_component_event_bus(Component.TELEGRAM)
            
            # 봇 시작
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            
            self.is_running = True
            logger.info(f"{LOG_TELEGRAM} 텔레그램 봇 실행 중")
            
            # 이벤트 발행
            if self.event_bus:
                await self.event_bus.publish(StatusEventTypes.SYSTEM_STATUS_CHANGE, {
                    "component": Component.TELEGRAM,
                    "status": "running",
                    "message": "텔레그램 봇이 시작되었습니다.",
                    "timestamp": asyncio.get_event_loop().time()
                })
            
            # 시작 알림 전송
            await self.notification_service.send_system_startup_notification()
            
        except Exception as e:
            logger.error(f"{LOG_TELEGRAM} 텔레그램 봇 시작 실패: {str(e)}")
            
            # 오류 이벤트 발행
            if self.event_bus:
                await self.event_bus.publish(StatusEventTypes.ERROR_EVENT, {
                    "component": Component.TELEGRAM,
                    "error": str(e),
                    "message": "텔레그램 봇 시작 실패",
                    "timestamp": asyncio.get_event_loop().time()
                })
            
            raise
    
    async def stop(self) -> None:
        """텔레그램 봇을 중지합니다."""
        if not self.is_running:
            logger.warning(f"{LOG_TELEGRAM} 텔레그램 봇이 실행 중이지 않습니다.")
            return
            
        logger.info(f"{LOG_TELEGRAM} 텔레그램 봇 중지 중...")
        
        try:
            # 종료 알림 전송
            await self.notification_service.send_system_shutdown_notification()
            
            # 종료 이벤트 발행
            if self.event_bus:
                await self.event_bus.publish(StatusEventTypes.SYSTEM_STATUS_CHANGE, {
                    "component": Component.TELEGRAM,
                    "status": "stopping",
                    "message": "텔레그램 봇이 종료 중입니다.",
                    "timestamp": asyncio.get_event_loop().time()
                })
            
            # 봇 종료
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
            
            self.is_running = False
            logger.info(f"{LOG_TELEGRAM} 텔레그램 봇 중지됨")
            
        except Exception as e:
            logger.error(f"{LOG_TELEGRAM} 텔레그램 봇 중지 실패: {str(e)}")
            
    async def restart(self) -> None:
        """텔레그램 봇을 재시작합니다."""
        logger.info(f"{LOG_TELEGRAM} 텔레그램 봇 재시작 중...")
        
        # 재시작 이벤트 발행
        if self.event_bus:
            await self.event_bus.publish(StatusEventTypes.SYSTEM_STATUS_CHANGE, {
                "component": Component.TELEGRAM,
                "status": "restarting",
                "message": "텔레그램 봇이 재시작 중입니다.",
                "timestamp": asyncio.get_event_loop().time()
            })
        
        await self.stop()
        await asyncio.sleep(1)  # 약간의 지연
        await self.start()
        
        logger.info(f"{LOG_TELEGRAM} 텔레그램 봇 재시작 완료")
        
    async def send_message(
        self, 
        chat_id: Union[int, List[int]], 
        text: str, 
        parse_mode: str = "HTML",
        **kwargs
    ) -> None:
        """
        지정된 채팅에 메시지를 전송합니다.
        
        Args:
            chat_id: 메시지를 보낼 채팅 ID 또는 ID 목록
            text: 전송할 메시지 내용
            parse_mode: 메시지 파싱 모드 (HTML, Markdown 등)
            **kwargs: telegram.Bot.send_message에 전달할 추가 인자
        """
        if isinstance(chat_id, list):
            # 여러 채팅에 전송
            for cid in chat_id:
                await self._send_single_message(cid, text, parse_mode, **kwargs)
        else:
            # 단일 채팅에 전송
            await self._send_single_message(chat_id, text, parse_mode, **kwargs)
    
    async def _send_single_message(
        self, 
        chat_id: int, 
        text: str, 
        parse_mode: str = "HTML",
        **kwargs
    ) -> None:
        """
        단일 채팅에 메시지를 전송합니다.
        
        Args:
            chat_id: 메시지를 보낼 채팅 ID
            text: 전송할 메시지 내용
            parse_mode: 메시지 파싱 모드 (HTML, Markdown 등)
            **kwargs: telegram.Bot.send_message에 전달할 추가 인자
        """
        try:
            await self.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
                **kwargs
            )
        except Exception as e:
            logger.error(f"{LOG_TELEGRAM} 메시지 전송 실패 (chat_id: {chat_id}): {str(e)}") 