"""
텔레그램 알림 모듈

텔레그램을 통해 시스템 알림 메시지를 전송합니다.
"""

from typing import Dict, Any, Optional, List, Union
import traceback
import asyncio
import enum
from enum import Enum

from telegram import Bot, InlineKeyboardMarkup

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.telegram_bot.notify_formatter import NotificationLevel

# 로거 설정
logger = get_unified_logger(component=SystemComponent.TELEGRAM.value)

# 싱글톤 인스턴스
_telegram_notifier_instance = None

def get_telegram_notifier():
    """텔레그램 알림 인스턴스를 반환합니다."""
    global _telegram_notifier_instance
    if _telegram_notifier_instance is None:
        # 인스턴스 생성
        _telegram_notifier_instance = TelegramNotifier()
        
        # 자동 초기화 간소화
        logger.info("🤖 텔레그램 노티파이어 생성됨")
        
    return _telegram_notifier_instance

async def initialize_notifier(notifier):
    """텔레그램 노티파이어 초기화 - 단순화됨"""
    try:
        logger.info("🤖 텔레그램 노티파이어 초기화")
        
        # 지연 임포트로 순환참조 방지
        from crosskimp.telegram_bot.commander import _telegram_bot_instance, get_allowed_chat_ids
        
        # 봇 설정
        if _telegram_bot_instance is not None:
            logger.info("🤖 텔레그램 봇 인스턴스 감지됨")
            notifier.set_bot(_telegram_bot_instance)
            notifier.set_allowed_chat_ids(get_allowed_chat_ids())
            logger.info("✅ 텔레그램 노티파이어 초기화 완료")
        else:
            logger.warning("⚠️ 텔레그램 봇 인스턴스가 초기화되지 않음")
    
    except Exception as e:
        logger.error(f"❌ 텔레그램 노티파이어 초기화 중 오류: {str(e)}", exc_info=True)

class TelegramNotifier:
    """
    텔레그램을 통해 시스템 알림을 전송하는 클래스
    """
    
    # 알림 레벨 속성 참조
    NotificationLevel = NotificationLevel
    
    def __init__(self):
        """텔레그램 알림 모듈 초기화"""
        # 텔레그램 봇 (외부에서 설정)
        self.bot = None
        
        # 알림 수신자 채팅 ID
        self.allowed_chat_ids = []
        
        # 로거
        self.logger = logger
        
        # 이벤트 구독 관리자
        self.event_subscriber = None
        
        # 알림 설정
        self.notification_settings = {
            "error_grouping": True,  # 동일 오류 그룹화 여부
            "timeout": 10.0,         # 메시지 전송 타임아웃 (초)
            "retry_count": 2         # 전송 실패시 재시도 횟수
        }
        
        self.logger.info("🤖 텔레그램 노티파이어 객체 생성됨")
    
    def set_bot(self, bot: Bot = None) -> None:
        """텔레그램 봇 인스턴스 설정"""
        self.logger.info("📱 텔레그램 봇 설정 시작")
        
        # 봇 상태 로깅
        old_bot = self.bot
        self.logger.info(f"ℹ️ 기존 봇 상태: {old_bot is not None}")
        
        # 지연 임포트 - 순환참조 방지
        from crosskimp.telegram_bot.commander import _telegram_bot_instance
        
        # 새 봇 설정
        self.bot = bot if bot is not None else _telegram_bot_instance
        self.logger.info(f"ℹ️ 새 봇 상태: {self.bot is not None}")
        self.logger.info("📱 텔레그램 봇 설정 완료")
    
    def set_allowed_chat_ids(self, chat_ids: List[int]) -> None:
        """
        허용된 채팅 ID 목록 설정
        
        Args:
            chat_ids: 허용된 채팅 ID 목록
        """
        self.allowed_chat_ids = chat_ids
        self.logger.info(f"👥 허용된 채팅 ID 설정됨: {self.allowed_chat_ids}")
    
    async def setup_event_subscriber(self, event_subscriber) -> bool:
        """
        이벤트 구독자 설정 및 구독 초기화
        
        Args:
            event_subscriber: 이벤트 구독자 인스턴스
            
        Returns:
            bool: 설정 성공 여부
        """
        try:
            self.event_subscriber = event_subscriber
            setup_result = self.event_subscriber.setup_subscriptions()
            self.logger.info(f"📱 이벤트 구독 설정 결과: {setup_result}")
            return setup_result
        except Exception as e:
            self.logger.error(f"❌ 이벤트 구독 설정 중 오류: {str(e)}", exc_info=True)
            return False
    
    async def stop(self) -> bool:
        """
        이벤트 구독 해제
        
        Returns:
            bool: 종료 성공 여부
        """
        try:
            self.logger.info("텔레그램 알림 모듈 이벤트 구독을 해제합니다...")
            
            # 이벤트 구독 해제
            if self.event_subscriber:
                await self.event_subscriber.unsubscribe_all()
            
            self.logger.info("텔레그램 알림 모듈 이벤트 구독이 해제되었습니다.")
            return True
            
        except Exception as e:
            self.logger.error(f"텔레그램 알림 모듈 이벤트 구독 해제 중 오류 발생: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
    
    async def send_notification(self, text: str, level: NotificationLevel = None, reply_markup: InlineKeyboardMarkup = None, timeout: float = None) -> bool:
        """
        모든 허용된 채팅 ID에 알림 메시지 전송
        
        Args:
            text: 전송할 메시지 텍스트
            level: 알림 레벨 (기본값: None, INFO로 처리)
            reply_markup: 인라인 키보드 (옵션)
            timeout: 전송 타임아웃 (기본값: None, 설정값 사용)
            
        Returns:
            bool: 전송 성공 여부
        """
        if not self.bot or not self.allowed_chat_ids:
            self.logger.warning("텔레그램 봇 또는 채팅 ID가 설정되지 않아 알림을 전송할 수 없습니다.")
            return False
        
        # 레벨이 None인 경우 기본값 설정
        if level is None:
            level = self.NotificationLevel.INFO
        
        # 타임아웃 설정
        if timeout is None:
            timeout = self.notification_settings.get("timeout", 10.0)
            
        try:
            self.logger.info(f"📱 텔레그램 알림 전송 시작: '{text[:30]}...' (수신자: {len(self.allowed_chat_ids)}명)")
            success = True
            
            for chat_id in self.allowed_chat_ids:
                try:
                    self.logger.debug(f"채팅 ID {chat_id}로 메시지 전송 중...")
                    
                    # 타임아웃 처리
                    try:
                        await asyncio.wait_for(
                            self.bot.send_message(
                                chat_id=chat_id,
                                text=text,
                                reply_markup=reply_markup,
                                parse_mode='HTML'  # HTML 파싱 모드 추가
                            ), 
                            timeout=timeout
                        )
                        self.logger.debug(f"채팅 ID {chat_id}로 메시지 전송 성공")
                    except asyncio.TimeoutError:
                        self.logger.warning(f"채팅 ID {chat_id}로 메시지 전송 타임아웃 (제한시간: {timeout}초)")
                        success = False
                        
                except Exception as e:
                    self.logger.error(f"채팅 ID {chat_id}로 메시지 전송 중 오류: {str(e)}", exc_info=True)
                    success = False
                    
            if success:
                self.logger.info(f"✅ 텔레그램 알림이 모든 수신자에게 성공적으로 전송되었습니다.")
            else:
                self.logger.warning(f"⚠️ 일부 수신자에게 텔레그램 알림 전송에 실패했습니다.")
                
            return success
        except Exception as e:
            self.logger.error(f"❌ 알림 전송 중 오류 발생: {str(e)}", exc_info=True)
            return False
    
    async def send_direct_message(self, chat_id: int, text: str, reply_markup: InlineKeyboardMarkup = None) -> bool:
        """
        특정 채팅 ID에 메시지 전송
        
        Args:
            chat_id: 전송할 채팅 ID
            text: 전송할 메시지 텍스트
            reply_markup: 인라인 키보드 (옵션)
            
        Returns:
            bool: 전송 성공 여부
        """
        if not self.bot:
            return False
            
        try:
            await self.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode='HTML'  # HTML 파싱 모드 추가
            )
            return True
        except Exception as e:
            self.logger.error(f"메시지 전송 중 오류 발생: {str(e)}")
            return False

    async def handle_bot_initialized(self, data):
        """봇 초기화 이벤트 처리"""
        if 'bot_instance' in data:
            self.set_bot(data['bot_instance'])
        if 'allowed_chat_ids' in data:
            self.set_allowed_chat_ids(data['allowed_chat_ids'])
