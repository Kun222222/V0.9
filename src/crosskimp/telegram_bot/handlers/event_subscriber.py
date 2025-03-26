"""
이벤트 구독 관리 모듈

텔레그램 알림을 위한 다양한 이벤트 핸들러들을 등록하고 관리합니다.
"""

from typing import Dict, List, Tuple, Callable, Any
import asyncio

from crosskimp.common.events.system_eventbus import get_event_bus
from crosskimp.common.events.system_types import EventPaths
from crosskimp.telegram_bot.handlers.process_event_handler import ProcessEventHandler
from crosskimp.telegram_bot.handlers.error_event_handler import ErrorEventHandler
from crosskimp.telegram_bot.handlers.metrics_event_handler import MetricsEventHandler
from crosskimp.telegram_bot.handlers.info_request_handler import InfoRequestHandler
from crosskimp.telegram_bot.handlers.trade_event_handler import TradeEventHandler

class EventSubscriber:
    """이벤트 구독 관리 클래스"""
    
    def __init__(self, notifier):
        """
        이벤트 구독 관리자 초기화
        
        Args:
            notifier: 알림 전송을 위한 TelegramNotifier 인스턴스
        """
        self.notifier = notifier
        self.logger = notifier.logger
        self.event_bus = get_event_bus()
        
        # 핸들러 인스턴스 생성
        self.process_handler = ProcessEventHandler(notifier)
        self.error_handler = ErrorEventHandler(notifier)
        self.metrics_handler = MetricsEventHandler(notifier)
        self.info_request_handler = InfoRequestHandler(notifier)
        self.trade_handler = TradeEventHandler(notifier)
        
        # 등록된 구독 목록
        self.subscriptions = []
    
    def setup_subscriptions(self):
        """
        모든 이벤트 구독 설정

        Returns:
            bool: 구독 설정 성공 여부
        """
        try:
            self.logger.info("⚙️ 텔레그램 알림 이벤트 구독 설정 시작")
            self.logger.info(f"ℹ️ 이벤트 버스 인스턴스 ID: {id(self.event_bus)}")
            
            # 이벤트 경로 상수 사용
            from crosskimp.common.events.system_types import EventPaths
            
            # 오더북 수집기 구동 완료 이벤트 구독 추가 (상세 로깅)
            event_path = EventPaths.OB_COLLECTOR_RUNNING
            self.logger.info(f"📢 오더북 수집기 구동 완료 이벤트 구독 등록 시작: {event_path}")
            
            # 핸들러 참조 저장 (디버깅 용이성)
            handler = self.handle_OB_COLLECTOR_RUNNING
            handler_id = id(handler)
            self.logger.info(f"🧩 핸들러 정보: {handler.__qualname__} (ID: {handler_id})")
            
            # 등록 전 이벤트 버스 상태 확인
            pre_handlers = self.event_bus._handlers.get(event_path, [])
            self.logger.info(f"⏱️ 등록 전 핸들러 수: {len(pre_handlers)}")
            
            # 핸들러 등록
            self.event_bus.register_handler(event_path, handler)
            
            # 등록 후 이벤트 버스 상태 확인
            post_handlers = self.event_bus._handlers.get(event_path, [])
            self.logger.info(f"⏱️ 등록 후 핸들러 수: {len(post_handlers)}")
            
            # 변화 있는지 확인
            if len(post_handlers) > len(pre_handlers):
                self.logger.info(f"✅ 핸들러 등록 성공: {event_path}")
            else:
                self.logger.warning(f"⚠️ 핸들러 등록에 변화가 없습니다: {event_path}")
            
            # 구독 목록에 추가
            self.subscriptions.append((event_path, handler))
            
            # 구독 설정 완료 로그
            self.logger.info(f"⚙️ 텔레그램 알림 이벤트 구독 설정 완료 (총 {len(self.subscriptions)}개 이벤트 등록)")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 이벤트 구독 설정 중 오류 발생: {str(e)}", exc_info=True)
            # 스택 트레이스 출력
            import traceback
            self.logger.error(f"🔍 스택 트레이스: {traceback.format_exc()}")
            return False
    
    async def handle_OB_COLLECTOR_RUNNING(self, data):
        """오더북 수집기 구동 완료 이벤트 처리"""
        try:
            self.logger.info(f"📩 오더북 수집기 구동 완료 이벤트 수신: {data}")
            
            # 알림 전송 전 상태 확인
            self.logger.info(f"📱 텔레그램 봇 설정 상태: {self.notifier.bot is not None}")
            self.logger.info(f"👥 허용된 채팅 ID: {self.notifier.allowed_chat_ids}")
            
            # 텔레그램 알림 전송
            message = data.get("message", "오더북 수집기가 성공적으로 구동되었습니다.")
            self.logger.info(f"📱 텔레그램 알림 전송 시작: '{message}'")
            
            # SUCCESS 레벨로 전송 (✅ 접두어 사용)
            result = await self.notifier.send_notification(
                message,
                level=self.notifier.NotificationLevel.SUCCESS,
                timeout=5.0  # 5초 타임아웃 (빠른 피드백을 위해)
            )
            
            # 결과 로깅
            if result:
                self.logger.info("✅ 오더북 수집기 구동 완료 알림이 성공적으로 전송되었습니다.")
            else:
                self.logger.warning("⚠️ 오더북 수집기 구동 완료 알림 전송이 실패했습니다.")
                
        except Exception as e:
            self.logger.error(f"❌ 오더북 구동 완료 알림 처리 중 오류: {str(e)}", exc_info=True)
            # 스택 트레이스 출력
            import traceback
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
            self.logger.debug("이벤트 구독이 해제되었습니다.")
            return True
            
        except Exception as e:
            self.logger.error(f"이벤트 구독 해제 중 오류 발생: {str(e)}")
            return False 