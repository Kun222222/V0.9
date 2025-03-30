"""
텔레그램 알림 이벤트 구독 모듈

이벤트 버스에서 필요한 이벤트를 구독하고 텔레그램으로 알림을 보냅니다.
"""

import asyncio
import traceback
import time
from typing import List, Dict, Any, Tuple, Optional, Set
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
        
        # 레벨 문자열을 NotificationLevel 객체로 변환하는 맵
        self.level_map = {
            "info": self.notifier.NotificationLevel.INFO,
            "success": self.notifier.NotificationLevel.SUCCESS,
            "warning": self.notifier.NotificationLevel.WARNING,
            "error": self.notifier.NotificationLevel.ERROR
        }
        
        # 알림 묶음 처리를 위한 변수들
        self.exchange_events = []  # 거래소 이벤트 수집
        self.processing_task = None  # 이벤트 처리 태스크
        self.all_connected_notified = False  # 모든 거래소 연결 알림 발송 여부
        
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
                EventChannels.Process.COMMAND_RESTART # 프로세스 재시작 명령
            ]
            
            # 핵심 이벤트에 일반 핸들러 등록
            for event_path in core_events:
                self._register_handler(event_path, self.handle_general_event)
                self.logger.info(f"[디버깅] 이벤트 {event_path} 구독 등록 완료")
            
            # 오더북 수집기 이벤트 구독 - 특화된 핸들러 사용
            # RUNNING 이벤트 제거 (더 이상 발생하지 않음)
            self._register_handler(EventChannels.Component.ObCollector.CONNECTION_LOST, self.handle_connection_lost_event)
            self._register_handler(EventChannels.Component.ObCollector.EXCHANGE_STATUS, self.handle_exchange_status_event)
            
            self.logger.info(f"텔레그램 알림 이벤트 구독 설정 완료")
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
            level = self.level_map.get(level_str, self.notifier.NotificationLevel.INFO)
            
            # 알림 전송
            self.logger.info(f"[디버깅] 알림 전송 시작: 이벤트 {event_path}, 메시지 길이: {len(message)}")
            await self._send_notification(message, level=level)
            self.logger.info(f"[디버깅] 알림 전송 완료: 이벤트 {event_path}")
                
        except Exception as e:
            self._log_handler_error(f"이벤트 처리 오류", e)

    # 이벤트 핸들러 추가
    async def handle_exchange_status_event(self, data):
        """거래소 연결 상태 변경 이벤트 처리"""
        try:
            # 이벤트 수집 후 일괄 처리
            self.exchange_events.append(data)
            
            # 이미 일괄 처리 태스크가 실행 중이면 종료
            if self.processing_task is not None and not self.processing_task.done():
                return
                
            # 아니면 새로운 태스크 시작
            self.processing_task = asyncio.create_task(self._process_exchange_events_batch())
            
        except Exception as e:
            self._log_handler_error("거래소 연결 상태 변경 이벤트 처리", e)
    
    async def _process_exchange_events_batch(self):
        """수집된 거래소 이벤트 일괄 처리"""
        try:
            # 대기 시간 늘림 (0.5초 → 2초)
            await asyncio.sleep(2.0)
            
            # 수집된 이벤트가 없으면 종료
            if not self.exchange_events:
                return
                
            # 수집된 이벤트 처리 준비
            events = self.exchange_events.copy()
            self.exchange_events = []  # 초기화
            
            # 최신 상태 기반으로 요약
            connected_exchanges = []
            disconnected_exchanges = []
            latest_timestamp = 0
            all_connected = False
            connected_count = 0
            total_count = 0
            
            # 거래소 상세 정보 수집
            exchanges_info = []
            
            # 이벤트 데이터 분석
            for event in events:
                exchange = event.get("exchange", "알 수 없음")
                status = event.get("status", False)
                timestamp = event.get("timestamp", 0)
                
                # 타임스탬프 업데이트
                if timestamp > latest_timestamp:
                    latest_timestamp = timestamp
                    all_connected = event.get("all_connected", False)
                    connected_count = event.get("connected_count", 0)
                    total_count = event.get("total_count", 0)
                
                # 연결 상태에 따라 분류
                if status:
                    if exchange not in connected_exchanges:
                        connected_exchanges.append(exchange)
                else:
                    if exchange not in disconnected_exchanges:
                        disconnected_exchanges.append(exchange)
                
                # 각 이벤트의 거래소 상세 정보 추출 및 통합
                if "exchanges_info" in event and event["exchanges_info"]:
                    event_exchanges_info = event.get("exchanges_info", [])
                    
                    # 이미 있는 거래소 정보는 업데이트하고 없으면 추가
                    for exchange_info in event_exchanges_info:
                        exchange_code = exchange_info.get("name", "")
                        
                        # 이미 있는 정보인지 확인
                        existing_idx = next((i for i, ex in enumerate(exchanges_info) 
                                            if ex.get("name") == exchange_code), None)
                        
                        if existing_idx is not None:
                            # 기존 정보 업데이트
                            exchanges_info[existing_idx] = exchange_info
                        else:
                            # 새 정보 추가
                            exchanges_info.append(exchange_info)
            
            # 로그
            self.logger.info(f"일괄 처리: 연결={len(connected_exchanges)}, 끊김={len(disconnected_exchanges)}, 전체 상태: {connected_count}/{total_count}")
            self.logger.debug(f"수집된 거래소 상세 정보: {len(exchanges_info)}개")
            
            # 알림 생성 결정
            notification_level = self.notifier.NotificationLevel.SUCCESS
            status_changed = False
            
            # 메시지 제목 및 강조 항목 결정
            if disconnected_exchanges:
                # 연결 끊김이 있는 경우
                notification_level = self.notifier.NotificationLevel.WARNING
                self.all_connected_notified = False
                status_changed = True
                highlight_exchanges = disconnected_exchanges
                highlight_type = "disconnected"
            elif connected_count == total_count and not self.all_connected_notified:
                # 모든 거래소 첫 연결
                self.all_connected_notified = True
                status_changed = True
                highlight_exchanges = connected_exchanges
                highlight_type = "all_connected"
            elif connected_exchanges and self.all_connected_notified:
                # 재연결
                status_changed = True
                highlight_exchanges = connected_exchanges
                highlight_type = "reconnected"
            
            # 상태 변경이 있는 경우에만 알림 발송
            if status_changed:
                self.logger.info(f"거래소 상태 변경 감지: {highlight_type}, 알림 발송 준비")
                
                # 수집된 거래소 정보가 없는 경우 디버그 로그
                if not exchanges_info:
                    self.logger.warning(f"상태 변경이 감지되었으나 거래소 상세 정보가 없습니다.")
                
                # 통합 이벤트 데이터 구성
                event_data = {
                    "_event_path": "exchange/status",
                    "timestamp": latest_timestamp,
                    "connected_count": connected_count, 
                    "total_count": total_count,
                    "details": {
                        "exchanges": exchanges_info
                    },
                    "highlight_exchanges": highlight_exchanges,
                    "highlight_type": highlight_type
                }
                
                # 포맷팅 및 알림 전송
                message, level = format_event(event_data)
                
                # 알림 전송
                if message:
                    await self._send_notification(message, level=notification_level)
                
        except Exception as e:
            self._log_handler_error("거래소 이벤트 일괄 처리", e)
    
    async def handle_connection_lost_event(self, data):
        """거래소 연결 끊김 이벤트 처리"""
        try:
            # 연결 끊김 이벤트 수신 시
            self.all_connected_notified = False
            
            # format_event 함수로 포맷팅
            message, level_str = format_event(data)
            
            # 알림 발송이 필요하지 않은 경우
            if message is None:
                return
                
            # 레벨 문자열을 NotificationLevel 객체로 변환
            level = self.level_map.get(level_str, self.notifier.NotificationLevel.WARNING)
            
            # 알림 전송
            await self._send_notification(message, level=level)
                
        except Exception as e:
            self._log_handler_error("거래소 연결 끊김 이벤트 처리", e)

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