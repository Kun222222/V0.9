"""
알림 서비스 - 텔레그램을 통한 시스템 알림 전송을 담당합니다.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.constants_v3 import LOG_TELEGRAM

# 새 이벤트 시스템 임포트로 변경
from crosskimp.common.events import get_component_event_bus, Component, StatusEventTypes, TelegramEventTypes

# 로거 설정
logger = get_unified_logger()

class NotificationService:
    """텔레그램 알림 서비스 클래스"""
    
    def __init__(self, bot_manager):
        """
        알림 서비스를 초기화합니다.
        
        Args:
            bot_manager: 텔레그램 봇 관리자 인스턴스
        """
        self.bot_manager = bot_manager
        # 텔레그램 컴포넌트 이벤트 버스 가져오기
        self.event_bus = None
        
        # 이벤트 구독 설정 (비동기 함수는 생성자에서 직접 호출할 수 없으므로 별도 태스크로 등록)
        asyncio.create_task(self._setup_event_subscriptions())
        
        logger.info(f"{LOG_TELEGRAM} 알림 서비스 초기화됨")
        
    async def _setup_event_subscriptions(self) -> None:
        """이벤트 버스 구독을 설정합니다."""
        # 텔레그램 컴포넌트 이벤트 버스 가져오기
        self.event_bus = get_component_event_bus(Component.TELEGRAM)
        
        # 프로세스 상태 변경 이벤트 구독
        await self.event_bus.subscribe(
            StatusEventTypes.PROCESS_STATUS, 
            self._handle_process_status_event
        )
        
        # 오류 발생 이벤트 구독
        await self.event_bus.subscribe(
            StatusEventTypes.ERROR_EVENT, 
            self._handle_error_event
        )
        
        # 시스템 메트릭 이벤트 구독 (리소스 사용량 알림 등)
        await self.event_bus.subscribe(
            StatusEventTypes.RESOURCE_USAGE,
            self._handle_system_metrics_event
        )
        
        logger.info(f"{LOG_TELEGRAM} 이벤트 구독 설정 완료")
        
    async def _handle_process_status_event(self, event_data: Dict[str, Any]) -> None:
        """
        프로세스 상태 변경 이벤트를 처리합니다.
        
        Args:
            event_data: 이벤트 데이터
        """
        process_name = event_data.get("process_name", "알 수 없음")
        status = event_data.get("status", "unknown")
        description = event_data.get("description", process_name)
        
        # 중요하지 않은 이벤트는 무시 (주기적인 상태 업데이트 등)
        if status == "running" and "timestamp" in event_data and "start_time" not in event_data:
            return
        
        # 특정 상태에 대해서만 알림 생성
        if status in ["started", "stopped", "restarting", "error", "killed"]:
            status_formatted = self._format_status(status)
            
            message = f"🔔 프로세스 상태 변경\n"
            message += f"<b>{description}</b> 프로세스가 {status_formatted} 상태로 전환됨"
            
            # 추가 정보가 있으면 포함
            if "error" in event_data:
                message += f"\n\n<b>오류:</b> {event_data['error']}"
                
            if "pid" in event_data:
                message += f"\n<b>PID:</b> {event_data['pid']}"
                
            # 관리자 알림 전송
            await self._send_admin_notification(message)
    
    async def _handle_error_event(self, event_data: Dict[str, Any]) -> None:
        """
        오류 이벤트를 처리합니다.
        
        Args:
            event_data: 이벤트 데이터
        """
        message = event_data.get("message", "알 수 없는 오류")
        source = event_data.get("source", "알 수 없음")
        category = event_data.get("category", "일반")
        severity = event_data.get("severity", "ERROR")
        
        # 심각도에 따라 처리 분기
        if severity in ["CRITICAL", "ERROR"]:
            # 심각한 오류는 즉시 알림
            emoji = "🔥" if severity == "CRITICAL" else "⚠️"
            message_text = (
                f"{emoji} <b>시스템 오류 발생</b>\n\n"
                f"<b>출처:</b> {source}\n"
                f"<b>분류:</b> {category}\n"
                f"<b>심각도:</b> {severity}\n\n"
                f"<b>메시지:</b> {message}"
            )
            
            # 추가 정보가 있으면 포함
            if "details" in event_data:
                message_text += f"\n\n<b>세부 정보:</b> {event_data['details']}"
                
            # 타임스탬프 추가
            timestamp = event_data.get("timestamp", datetime.now().timestamp())
            message_text += f"\n\n<i>{datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')}</i>"
            
            # 관리자 알림 전송
            await self._send_admin_notification(message_text)
        
        elif severity == "WARNING":
            # 경고는 알림 없이 로그만 남김
            logger.warning(f"{LOG_TELEGRAM} 경고 이벤트 발생: [{source}/{category}] {message}")
    
    async def _handle_system_metrics_event(self, event_data: Dict[str, Any]) -> None:
        """
        시스템 메트릭 이벤트를 처리합니다.
        
        Args:
            event_data: 이벤트 데이터
        """
        # 리소스 사용량이 임계치를 초과하는 경우에만 알림
        if "cpu_usage" in event_data and event_data["cpu_usage"] > 90:
            message = (
                f"⚠️ <b>CPU 사용량 경고</b>\n\n"
                f"CPU 사용량이 {event_data['cpu_usage']:.1f}%로 임계치(90%)를 초과했습니다."
            )
            await self._send_admin_notification(message)
            
        if "memory_usage" in event_data and event_data["memory_usage"] > 90:
            message = (
                f"⚠️ <b>메모리 사용량 경고</b>\n\n"
                f"메모리 사용량이 {event_data['memory_usage']:.1f}%로 임계치(90%)를 초과했습니다."
            )
            await self._send_admin_notification(message)
            
        if "disk_usage" in event_data and event_data["disk_usage"] > 90:
            message = (
                f"⚠️ <b>디스크 사용량 경고</b>\n\n"
                f"디스크 사용량이 {event_data['disk_usage']:.1f}%로 임계치(90%)를 초과했습니다."
            )
            await self._send_admin_notification(message)
    
    async def _send_admin_notification(self, message: str) -> None:
        """
        관리자에게 알림을 전송합니다.
        
        Args:
            message: 전송할 메시지
        """
        try:
            # 관리자 채팅 ID 목록 가져오기
            chat_ids = self.bot_manager.admin_chat_ids
            
            # 관리자가 없는 경우 전송 취소
            if not chat_ids:
                logger.warning(f"{LOG_TELEGRAM} 관리자 채팅 ID가 없어 알림을 전송할 수 없습니다.")
                return
                
            # 메시지 전송
            await self.bot_manager.send_message(chat_ids, message)
            
        except Exception as e:
            logger.error(f"{LOG_TELEGRAM} 관리자 알림 전송 실패: {str(e)}")
    
    async def send_admin_message(self, message: str) -> None:
        """
        관리자에게 메시지를 전송합니다. (외부에서 직접 호출)
        
        Args:
            message: 전송할 메시지
        """
        await self._send_admin_notification(message)
    
    def _format_status(self, status: str) -> str:
        """상태 문자열을 포맷팅합니다."""
        status_formats = {
            "running": "▶️ 실행 중",
            "started": "✅ 시작됨",
            "stopping": "⏹️ 중지 중",
            "stopped": "⏹️ 중지됨",
            "killed": "🔴 강제 종료됨",
            "restarting": "🔄 재시작 중",
            "error": "⚠️ 오류 발생"
        }
        return status_formats.get(status, status)
    
    async def send_notification(self, message: str, admin_only: bool = False) -> None:
        """
        알림을 전송합니다.
        
        Args:
            message: 전송할 메시지
            admin_only: 관리자에게만 전송할지 여부
        """
        try:
            if admin_only:
                # 관리자에게만 전송
                await self._send_admin_notification(message)
            else:
                # 모든 클라이언트에게 전송 (향후 구현)
                await self._send_admin_notification(message)
                
        except Exception as e:
            logger.error(f"{LOG_TELEGRAM} 알림 전송 실패: {str(e)}")
    
    async def send_system_startup_notification(self) -> None:
        """시스템 시작 알림을 전송합니다."""
        message = (
            f"🟢 <b>시스템 시작됨</b>\n\n"
            f"크로스 김프 시스템이 시작되었습니다.\n"
            f"<i>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
        )
        await self._send_admin_notification(message)
    
    async def send_system_shutdown_notification(self) -> None:
        """시스템 종료 알림을 전송합니다."""
        message = (
            f"🔴 <b>시스템 종료 중</b>\n\n"
            f"크로스 김프 시스템이 종료 중입니다.\n"
            f"<i>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
        )
        await self._send_admin_notification(message)
    
    async def send_daily_report(self) -> None:
        """일일 보고서를 전송합니다."""
        # 여기에서 리포트 데이터를 수집하고 포맷팅할 수 있음
        message = (
            f"📋 <b>일일 시스템 보고서</b>\n\n"
            f"<i>{datetime.now().strftime('%Y-%m-%d')}</i>\n\n"
            f"모든 서비스가 정상 작동 중입니다."
        )
        await self._send_admin_notification(message)
    
    async def send_custom_notification(
        self, 
        title: str, 
        content: str, 
        admin_only: bool = True,
        emoji: str = "ℹ️"
    ) -> None:
        """
        사용자 정의 알림을 전송합니다.
        
        Args:
            title: 알림 제목
            content: 알림 내용
            admin_only: 관리자에게만 전송할지 여부
            emoji: 제목 앞에 표시할 이모지
        """
        message = (
            f"{emoji} <b>{title}</b>\n\n"
            f"{content}\n\n"
            f"<i>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
        )
        await self.send_notification(message, admin_only=admin_only)
    
    async def publish_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """
        이벤트를 발행합니다. (외부에서 호출)
        
        Args:
            event_type: 이벤트 타입
            data: 이벤트 데이터
        """
        if self.event_bus:
            await self.event_bus.publish(event_type, data) 