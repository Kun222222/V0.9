"""
텔레그램 알림 이벤트 구독 모듈

이벤트 버스에서 필요한 이벤트를 구독하고 텔레그램으로 알림을 보냅니다.
"""

import asyncio
import traceback
from typing import List, Dict, Any, Tuple, Optional, Set
import time
import re

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.events.system_eventbus import get_event_bus
from crosskimp.common.events.system_types import EventChannels
from crosskimp.telegram_bot.notify_formatter import format_event, NotificationLevel

# 로거 설정
logger = get_unified_logger(component=SystemComponent.TELEGRAM.value)

class UnifiedEventSubscriber:
    """
    통합 이벤트 구독자
    
    텔레그램 알림을 위한 모든 시스템 이벤트를 구독하고 처리합니다.
    """
    
    def __init__(self, notifier):
        """
        통합 이벤트 구독 관리자 초기화
        
        Args:
            notifier: 알림 전송을 위한 TelegramNotifier 인스턴스
        """
        self.notifier = notifier
        self.logger = notifier.logger
        self.event_bus = get_event_bus()
        
        # 등록된 구독 목록
        self.subscriptions = []
        
        self.logger.info("통합 이벤트 구독 관리자 초기화됨")
    
    def setup_subscriptions(self):
        """
        필요한 이벤트 구독 설정 - 핵심 이벤트 구독으로 복원

        Returns:
            bool: 구독 설정 성공 여부
        """
        try:
            self.logger.info("텔레그램 알림 이벤트 구독 설정 시작")
            
            # 핵심 이벤트 구독 복원
            core_events = [
                EventChannels.System.ERROR,         # 시스템 오류
                EventChannels.System.STATUS,        # 시스템 상태
                EventChannels.Process.STATUS,       # 프로세스 상태
                EventChannels.Process.COMMAND_START, # 프로세스 시작 명령
                EventChannels.Process.COMMAND_STOP,  # 프로세스 중지 명령
                EventChannels.Process.COMMAND_RESTART, # 프로세스 재시작 명령
                EventChannels.Component.ObCollector.RUNNING, # 오더북 수집기 실행
                EventChannels.Component.ObCollector.CONNECTION_LOST # 오더북 연결 끊김
            ]
            
            # 핵심 이벤트에 일반 핸들러 등록
            for event_path in core_events:
                self._register_handler(event_path, self.handle_general_event)
                self.logger.info(f"[디버깅] 이벤트 {event_path} 구독 등록 완료")
            
            self.logger.info(f"텔레그램 알림 이벤트 구독 설정 완료 (총 {len(core_events)}개)")
            return True
            
        except Exception as e:
            self._log_handler_error("이벤트 구독 설정", e)
            return False
    
    def _register_handler(self, event_path, handler):
        """이벤트 핸들러 등록 헬퍼 메서드"""
        self.logger.info(f"이벤트 구독 등록: {event_path}")
        
        # 핸들러 등록
        self.event_bus.register_handler(event_path, handler)
        
        # 구독 목록에 추가
        self.subscriptions.append((event_path, handler))
    
    #
    # 헬퍼 메서드
    #
    
    async def _send_notification(self, message, level=None, reply_markup=None, timeout=5.0):
        """알림 전송 헬퍼 메서드"""
        try:
            result = await self.notifier.send_notification(
                message,
                level=level,
                reply_markup=reply_markup,
                timeout=timeout
            )
            
            if not result:
                self.logger.warning(f"알림 전송이 실패했습니다: '{message[:30]}...'")
                
            return result
        except Exception as e:
            self.logger.error(f"알림 전송 중 오류: {str(e)}")
            return False
    
    def _log_handler_error(self, context, error):
        """핸들러 오류 로깅 헬퍼 메서드"""
        self.logger.error(f"❌ {context} 처리 중 오류: {str(error)}", exc_info=True)
        self.logger.error(f"🔍 스택 트레이스: {traceback.format_exc()}")
    
    async def unsubscribe_all(self):
        """
        모든 이벤트 구독 해제
        
        Returns:
            bool: 구독 해제 성공 여부
        """
        try:
            for event_type, handler in self.subscriptions:
                self.event_bus.unregister_handler(event_type, handler)
            
            self.subscriptions = []
            self.logger.info("✅ 모든 이벤트 구독이 해제되었습니다.")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 이벤트 구독 해제 중 오류 발생: {str(e)}")
            return False

    async def handle_general_event(self, data):
        """일반 이벤트 처리를 위한 범용 핸들러 - 포맷팅 로직 분리 적용"""
        try:
            event_path = data.get("_event_path", "unknown")
            process_name = data.get("process_name", "unknown")
            status = data.get("status", "unknown")
            
            self.logger.info(f"[디버깅] 이벤트 수신: {event_path}, 프로세스: {process_name}, 상태: {status}")
            self.logger.debug(f"[디버깅] 이벤트 데이터: {data}")
            
            # 포맷팅 로직을 별도 모듈로 위임
            message, level_str = format_event(data)
            
            # 알림 발송이 필요하지 않은 경우
            if message is None:
                self.logger.info(f"[디버깅] 이벤트 {event_path}에 대한 알림 메시지가 None으로 반환됨 (알림 발송 건너뜀)")
                return
            
            self.logger.info(f"[디버깅] 이벤트 {event_path}에 대한 알림 메시지 생성 완료, 레벨: {level_str}")
            
            # 레벨 문자열을 NotificationLevel 객체로 변환
            level_map = {
                "info": self.notifier.NotificationLevel.INFO,
                "success": self.notifier.NotificationLevel.SUCCESS,
                "warning": self.notifier.NotificationLevel.WARNING,
                "error": self.notifier.NotificationLevel.ERROR
            }
            level = level_map.get(level_str, self.notifier.NotificationLevel.INFO)
            
            # 알림 전송
            self.logger.info(f"[디버깅] 알림 전송 시작: 이벤트 {event_path}, 메시지 길이: {len(message)}")
            await self._send_notification(message, level=level)
            self.logger.info(f"[디버깅] 알림 전송 완료: 이벤트 {event_path}")
                
        except Exception as e:
            self._log_handler_error(f"이벤트 처리 오류", e)

# 싱글톤 인스턴스 유지
_event_subscriber_instance = None

def get_event_subscriber(notifier=None):
    """
    통합 이벤트 구독자 인스턴스를 반환합니다.
    
    Args:
        notifier: 노티파이어 인스턴스 (None이면 이미 생성된 인스턴스를 반환)
        
    Returns:
        UnifiedEventSubscriber: 통합 이벤트 구독자 인스턴스
    """
    global _event_subscriber_instance
    
    if _event_subscriber_instance is None and notifier is not None:
        _event_subscriber_instance = UnifiedEventSubscriber(notifier)
        
    return _event_subscriber_instance 