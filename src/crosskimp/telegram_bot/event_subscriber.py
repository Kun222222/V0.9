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
from crosskimp.common.events.system_types import EventPaths

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
        
        # 오류 상태 정보
        self._last_error_by_type = {}
        self._error_counts = {}
        
        self.logger.info("🔄 통합 이벤트 구독 관리자 초기화됨")
    
    def setup_subscriptions(self):
        """
        모든 이벤트 구독 설정

        Returns:
            bool: 구독 설정 성공 여부
        """
        try:
            self.logger.info("⚙️ 텔레그램 알림 이벤트 구독 설정 시작")
            self.logger.info(f"ℹ️ 이벤트 버스 인스턴스 ID: {id(self.event_bus)}")
            
            # 모든 이벤트 경로 가져오기
            all_event_paths = []
            
            # EventPaths 클래스의 모든 상수 가져오기
            for attr_name in dir(EventPaths):
                # 언더스코어로 시작하지 않는 대문자 속성만 선택 (상수)
                if not attr_name.startswith('_') and attr_name.isupper():
                    event_path = getattr(EventPaths, attr_name)
                    # 문자열 타입의 이벤트 경로만 추가
                    if isinstance(event_path, str) and '/' in event_path:
                        all_event_paths.append(event_path)
            
            # 이벤트 경로 중복 제거 및 정렬
            all_event_paths = sorted(set(all_event_paths))
            self.logger.info(f"총 {len(all_event_paths)}개 이벤트 경로 발견")
            
            # UI 관련 이벤트 목록 - 이제 더이상 필요하지 않음
            
            # 모든 이벤트에 일반 핸들러 등록
            for event_path in all_event_paths:
                self._register_handler(event_path, self.handle_general_event)
            
            # 구독 설정 완료 로그
            self.logger.info(f"⚙️ 텔레그램 알림 이벤트 구독 설정 완료 (총 {len(self.subscriptions)}개 이벤트 등록)")
            return True
            
        except Exception as e:
            self._log_handler_error("이벤트 구독 설정", e)
            return False
    
    def _register_handler(self, event_path, handler):
        """이벤트 핸들러 등록 헬퍼 메서드"""
        self.logger.info(f"📢 이벤트 구독 등록: {event_path} → {handler.__name__}")
        
        # 등록 전 이벤트 버스 상태 확인
        pre_handlers = self.event_bus._handlers.get(event_path, [])
        
        # 핸들러 등록
        self.event_bus.register_handler(event_path, handler)
        
        # 등록 후 이벤트 버스 상태 확인
        post_handlers = self.event_bus._handlers.get(event_path, [])
        
        # 변화 있는지 확인
        if len(post_handlers) > len(pre_handlers):
            self.logger.info(f"✅ 핸들러 등록 성공: {event_path}")
        else:
            self.logger.warning(f"⚠️ 핸들러 등록에 변화가 없습니다: {event_path}")
        
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
                self.logger.warning(f"⚠️ 알림 전송이 실패했습니다: '{message[:30]}...'")
                
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
        """일반 이벤트 처리를 위한 범용 핸들러"""
        try:
            # 이벤트 경로 확인
            event_path = data.get("_event_path", "unknown")
            
            # 핵심 정보만 추출
            event_category = event_path.split('/')[0] if '/' in event_path else event_path
            process_name = data.get("process_name", "")
            message = data.get("message", "")
            
            # 데이터를 간략하게 요약
            summary = f"이벤트: {event_path}"
            if process_name:
                summary += f" | 프로세스: {process_name}"
            if message:
                # 메시지가 있으면 메시지를 주요 내용으로 사용
                summary = message
            
            # 추가 데이터가 있으면 요약
            details = data.get("details", {})
            if details and isinstance(details, dict):
                summary_details = []
                for key, value in details.items():
                    # 값이 단순 타입이면 추가
                    if isinstance(value, (str, int, float, bool)):
                        summary_details.append(f"{key}: {value}")
                # 요약 정보 추가
                if summary_details and not message:  # 메시지가 없을 때만 상세 정보 추가
                    summary += f" | 상세: {', '.join(summary_details[:3])}"
                    if len(summary_details) > 3:
                        summary += f" 외 {len(summary_details) - 3}개"
            
            # 특정 이벤트 타입별 처리
            level = self.notifier.NotificationLevel.INFO  # 기본값
            
            # 1. 시스템 오류 이벤트 처리 (오류 그룹화)
            if event_path == EventPaths.SYSTEM_ERROR:
                await self._handle_system_error(data)
                return
                
            # 2. 성능 메트릭 이벤트 처리 (임계값 체크)
            elif event_path.startswith("performance/"):
                if self._should_send_performance_alert(data):
                    await self._send_performance_notification(data)
                return
                
            # 4. 프로세스 상태 이벤트 처리
            elif "process/status" in event_path:
                status = data.get("status", "")
                if status == EventPaths.PROCESS_STATUS_RUNNING:
                    summary = f"✅ '{process_name}' 프로세스가 시작되었습니다."
                    level = self.notifier.NotificationLevel.SUCCESS
                elif status == EventPaths.PROCESS_STATUS_STOPPED:
                    summary = f"🛑 '{process_name}' 프로세스가 중지되었습니다."
                    level = self.notifier.NotificationLevel.INFO
                elif status == EventPaths.PROCESS_STATUS_ERROR:
                    error_msg = data.get("error_message", "알 수 없는 오류")
                    summary = f"❌ '{process_name}' 프로세스 오류: {error_msg}"
                    level = self.notifier.NotificationLevel.ERROR
                else:
                    # 시작 중, 종료 중 상태는 알림 제외
                    return
                    
            # 5. 프로세스 오류 이벤트 처리
            elif "process/error" in event_path:
                error_message = data.get("error_message", "알 수 없는 오류")
                summary = f"❌ 프로세스 오류 ({process_name}):\n{error_message}"
                level = self.notifier.NotificationLevel.ERROR
                
            # 6. 오더북 컬렉터 이벤트 처리
            elif "component/ob_collector/" in event_path:
                if "start" in event_path:
                    summary = data.get("message", "🚀 오더북 수집기 실행이 요청되었습니다.")
                    level = self.notifier.NotificationLevel.INFO
                elif "running" in event_path:
                    summary = data.get("message", "✅ 오더북 수집기가 성공적으로 구동되었습니다.")
                    level = self.notifier.NotificationLevel.SUCCESS
                elif "connection_lost" in event_path:
                    summary = data.get("message", "⚠️ 오더북 수집기 거래소 연결 끊김 감지!")
                    level = self.notifier.NotificationLevel.WARNING
                elif "stop" in event_path:
                    summary = data.get("message", "🛑 오더북 수집기가 종료되었습니다.")
                    level = self.notifier.NotificationLevel.INFO
                else:
                    # 이벤트 범주에 따라 알림 레벨 결정
                    level = self._determine_notification_level(event_path)
            else:
                # 이벤트 범주에 따라 알림 레벨 결정
                level = self._determine_notification_level(event_path)
            
            # 일부 경로는 알림 제외 (너무 빈번한 이벤트)
            excluded_paths = [
                'heartbeat', 'metrics', 'status/starting', 'status/stopping',
                'subscription', 'system/info'
            ]
            
            if not any(exclude in event_path for exclude in excluded_paths):
                await self._send_notification(summary, level=level)
                
        except Exception as e:
            self._log_handler_error(f"일반 이벤트 처리 (경로: {data.get('_event_path', 'unknown')})", e)
    
    async def _handle_system_error(self, data):
        """시스템 오류 이벤트 처리 (오류 그룹화 기능)"""
        try:
            error_type = data.get("error_type", "unknown")
            error_message = data.get("error_message", "알 수 없는 오류")
            component = data.get("component", "system")
            
            # 오류 그룹화 (동일 유형 오류 카운팅)
            if self.notifier.notification_settings["error_grouping"]:
                error_key = f"{component}_{error_type}"
                
                # 첫 발생이거나 마지막 오류와 다른 경우 즉시 알림
                if error_key not in self._last_error_by_type or self._last_error_by_type[error_key] != error_message:
                    self._last_error_by_type[error_key] = error_message
                    self._error_counts[error_key] = 1
                    
                    message = f"⚠️ 오류 발생 ({component}):\n{error_message}"
                    await self._send_notification(message, level=self.notifier.NotificationLevel.ERROR)
                else:
                    # 동일 오류 반복 - 카운트만 증가
                    self._error_counts[error_key] += 1
                    
                    # 카운트가 10, 100, 1000 등 10의 배수일 때만 알림
                    count = self._error_counts[error_key]
                    if count in [10, 100, 1000] or count % 1000 == 0:
                        message = f"⚠️ 반복 오류 ({component}):\n{error_message}\n\n동일 오류 {count}회 발생"
                        await self._send_notification(message, level=self.notifier.NotificationLevel.ERROR)
            else:
                # 그룹화 없이 모든 오류 알림
                message = f"⚠️ 오류 발생 ({component}):\n{error_message}"
                await self._send_notification(message, level=self.notifier.NotificationLevel.ERROR)
        except Exception as e:
            self._log_handler_error("시스템 오류 알림", e)
    
    def _should_send_performance_alert(self, data):
        """성능 메트릭이 알림 임계값을 초과했는지 확인"""
        metrics_type = data.get("metrics_type", "")
        metrics_value = data.get("value", 0)
        
        if metrics_type == "cpu" and metrics_value > 80:
            return True
        elif metrics_type == "memory" and metrics_value > 85:
            return True
        elif metrics_type == "latency" and metrics_value > 5000:
            return True
            
        return False
        
    async def _send_performance_notification(self, data):
        """성능 알림 전송"""
        metrics_type = data.get("metrics_type", "system")
        metrics_value = data.get("value", 0)
        component = data.get("component", "system")
        
        if metrics_type == "cpu":
            message = f"⚠️ CPU 사용률 경고 ({component}): {metrics_value}%"
        elif metrics_type == "memory":
            message = f"⚠️ 메모리 사용률 경고 ({component}): {metrics_value}%"
        elif metrics_type == "latency":
            message = f"⚠️ 지연 시간 경고 ({component}): {metrics_value}ms"
        else:
            message = f"⚠️ 성능 경고 ({component}): {metrics_type} = {metrics_value}"
            
        await self._send_notification(message, level=self.notifier.NotificationLevel.WARNING)
    
    def _determine_notification_level(self, event_path):
        """이벤트 경로에 따라 알림 레벨 결정"""
        if 'error' in event_path:
            return self.notifier.NotificationLevel.ERROR
        elif 'warning' in event_path or 'connection_lost' in event_path:
            return self.notifier.NotificationLevel.WARNING
        elif any(word in event_path for word in ['start', 'success', 'complete', 'running', 'filled']):
            return self.notifier.NotificationLevel.SUCCESS
        else:
            return self.notifier.NotificationLevel.INFO

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