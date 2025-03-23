"""
모니터링 서비스 - 시스템 자원 및 프로세스 모니터링을 담당합니다.
"""

import os
import asyncio
import psutil
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import LOG_SYSTEM, LOG_TELEGRAM
from crosskimp.system_manager.process_manager import get_process_status

# 기존 이벤트 버스 임포트 제거
# from crosskimp.ob_collector.orderbook.util.event_bus import EventBus, EVENT_TYPES

# 새 이벤트 시스템 임포트 추가
from crosskimp.common.events import (
    get_component_event_bus,
    Component,
    EventTypes,
    StatusEventTypes,
    TelegramEventTypes
)

# 로거 설정
logger = get_unified_logger()

class SystemMonitor:
    """시스템 리소스 및 프로세스 모니터링 클래스"""
    
    def __init__(self, notification_service):
        """
        시스템 모니터링 서비스를 초기화합니다.
        
        Args:
            notification_service: 알림 서비스 인스턴스
        """
        self.notification_service = notification_service
        # 이벤트 버스 수정
        self.event_bus = get_component_event_bus(Component.TELEGRAM)
        
        # 모니터링 설정
        self.cpu_threshold = 80.0  # CPU 사용량 임계값 (%)
        self.memory_threshold = 90.0  # 메모리 사용량 임계값 (%)
        self.disk_threshold = 90.0  # 디스크 사용량 임계값 (%)
        
        # 알림 쿨다운 (같은 경고를 반복해서 보내지 않기 위함)
        self.alert_cooldown = {}
        self.cooldown_period = 3600  # 알림 쿨다운 시간 (초)
        
        # 모니터링 실행 중 플래그
        self.is_running = False
        
        # 이벤트 구독 등록
        asyncio.create_task(self._register_event_handlers())
        
        logger.info(f"{LOG_SYSTEM} 시스템 모니터링 서비스 초기화됨")
    
    async def _register_event_handlers(self):
        """이벤트 핸들러를 등록합니다."""
        # 프로세스 상태 변경 이벤트 구독
        await self.event_bus.subscribe(
            StatusEventTypes.PROCESS_STATUS,
            self._handle_process_status_event
        )
        
        # 오류 이벤트 구독
        await self.event_bus.subscribe(
            StatusEventTypes.ERROR_EVENT,
            self._handle_error_event
        )
        
        logger.info(f"{LOG_SYSTEM} 모니터링 이벤트 핸들러 등록 완료")
    
    async def _handle_process_status_event(self, event_data):
        """
        프로세스 상태 이벤트를 처리합니다.
        
        Args:
            event_data: 이벤트 데이터
        """
        try:
            process_name = event_data.get("process_name", "알 수 없음")
            status = event_data.get("status", "unknown")
            description = event_data.get("description", process_name)
            
            # 중요 상태 변경 시 알림 전송
            if status in ["error", "stopped", "killed"]:
                message = (
                    f"⚠️ <b>프로세스 상태 변경</b>\n"
                    f"• 프로세스: {description} ({process_name})\n"
                    f"• 상태: {status}\n"
                )
                
                if status == "error" and "return_code" in event_data:
                    message += f"• 종료 코드: {event_data['return_code']}\n"
                
                if "timestamp" in event_data:
                    timestamp = datetime.fromtimestamp(event_data["timestamp"])
                    message += f"• 시간: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
                
                # 알림 전송
                await self.notification_service.send_admin_message(message)
                
            logger.debug(f"{LOG_SYSTEM} 프로세스 상태 이벤트 처리됨: {process_name} -> {status}")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 프로세스 상태 이벤트 처리 중 오류: {str(e)}")
    
    async def _handle_error_event(self, event_data):
        """
        오류 이벤트를 처리합니다.
        
        Args:
            event_data: 이벤트 데이터
        """
        try:
            message = event_data.get("message", "알 수 없는 오류")
            source = event_data.get("source", "알 수 없음")
            category = event_data.get("category", "UNKNOWN")
            severity = event_data.get("severity", "ERROR")
            
            # 심각도에 따라 알림 전송
            if severity in ["ERROR", "CRITICAL"]:
                alert_message = (
                    f"🔴 <b>시스템 오류 발생</b>\n"
                    f"• 소스: {source}\n"
                    f"• 카테고리: {category}\n"
                    f"• 심각도: {severity}\n"
                    f"• 메시지: {message}\n"
                )
                
                if "timestamp" in event_data:
                    timestamp = datetime.fromtimestamp(event_data["timestamp"])
                    alert_message += f"• 시간: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
                
                # 알림 전송
                await self.notification_service.send_admin_message(alert_message)
                
            logger.debug(f"{LOG_SYSTEM} 오류 이벤트 처리됨: {source} - {message}")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 오류 이벤트 처리 중 오류: {str(e)}")
    
    async def start(self, interval: int = 300) -> None:
        """
        모니터링을 시작합니다.
        
        Args:
            interval: 모니터링 간격 (초)
        """
        if self.is_running:
            logger.warning(f"{LOG_SYSTEM} 모니터링 서비스가 이미 실행 중입니다.")
            return
        
        self.is_running = True
        logger.info(f"{LOG_SYSTEM} 시스템 모니터링 시작 (간격: {interval}초)")
        
        try:
            while self.is_running:
                await self._check_system_resources()
                await self._check_processes()
                await asyncio.sleep(interval)
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 모니터링 중 오류 발생: {str(e)}")
            self.is_running = False
    
    async def stop(self) -> None:
        """모니터링을 중지합니다."""
        if not self.is_running:
            logger.warning(f"{LOG_SYSTEM} 모니터링 서비스가 실행 중이지 않습니다.")
            return
        
        self.is_running = False
        logger.info(f"{LOG_SYSTEM} 시스템 모니터링 중지됨")
    
    async def _check_system_resources(self) -> None:
        """시스템 자원 사용량을 확인합니다."""
        try:
            # CPU 사용량 확인
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # 메모리 사용량 확인
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # 디스크 사용량 확인
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            
            # 이벤트 발행
            metrics_data = {
                "cpu_percent": cpu_percent,
                "memory_percent": memory_percent,
                "disk_percent": disk_percent,
                "timestamp": datetime.now().timestamp()
            }
            
            await self.event_bus.publish(StatusEventTypes.METRIC_UPDATE, metrics_data)
            
            # 임계값 초과 시 알림
            alerts = []
            
            if cpu_percent > self.cpu_threshold:
                alerts.append(self._create_resource_alert("CPU", cpu_percent, self.cpu_threshold))
                
            if memory_percent > self.memory_threshold:
                alerts.append(self._create_resource_alert("메모리", memory_percent, self.memory_threshold))
                
            if disk_percent > self.disk_threshold:
                alerts.append(self._create_resource_alert("디스크", disk_percent, self.disk_threshold))
            
            # 알림 전송 (쿨다운 고려)
            for alert in alerts:
                alert_type = alert["type"]
                current_time = datetime.now()
                
                # 쿨다운 확인
                if alert_type in self.alert_cooldown:
                    last_alert_time = self.alert_cooldown[alert_type]
                    if (current_time - last_alert_time).total_seconds() < self.cooldown_period:
                        continue  # 쿨다운 중이면 스킵
                
                # 알림 전송
                await self.event_bus.publish(StatusEventTypes.METRIC_UPDATE, alert)
                
                # 쿨다운 업데이트
                self.alert_cooldown[alert_type] = current_time
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 시스템 자원 확인 중 오류: {str(e)}")
    
    def _create_resource_alert(self, resource_type: str, value: float, threshold: float) -> Dict[str, Any]:
        """
        리소스 알림 데이터를 생성합니다.
        
        Args:
            resource_type: 리소스 유형 (CPU, 메모리 등)
            value: 현재 값
            threshold: 임계값
            
        Returns:
            Dict: 알림 데이터
        """
        return {
            "type": f"{resource_type} 사용량 경고",
            "value": f"{value:.1f}%",
            "threshold": f"{threshold:.1f}%",
            "alert": True,
            "timestamp": datetime.now().timestamp()
        }
    
    async def _check_processes(self) -> None:
        """프로세스 상태를 확인합니다."""
        try:
            # 프로세스 상태 가져오기
            status = await get_process_status()
            
            # 오류 상태 확인
            for name, process_info in status.items():
                # 실행 중이지 않은 프로세스 중 자동 재시작이 활성화된 것이 있으면 로깅
                if not process_info["running"] and process_info["auto_restart"]:
                    logger.warning(
                        f"{LOG_SYSTEM} 프로세스 {name}({process_info['description']})가 "
                        f"중지되었지만 자동 재시작이 활성화되어 있습니다."
                    )
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 프로세스 상태 확인 중 오류: {str(e)}")
    
    async def generate_daily_report(self) -> Dict[str, Any]:
        """
        일일 시스템 보고서를 생성합니다.
        
        Returns:
            Dict: 보고서 데이터
        """
        try:
            # 시스템 자원 정보
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            # 프로세스 상태
            process_status = await get_process_status()
            
            # 보고서 데이터 구성
            report = {
                "timestamp": datetime.now(),
                "system_resources": {
                    "cpu_percent": cpu_percent,
                    "memory": {
                        "total": memory.total,
                        "available": memory.available,
                        "percent": memory.percent
                    },
                    "disk": {
                        "total": disk.total,
                        "free": disk.free,
                        "percent": disk.percent
                    }
                },
                "processes": process_status
            }
            
            return report
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 일일 보고서 생성 중 오류: {str(e)}")
            return {
                "timestamp": datetime.now(),
                "error": str(e)
            }
    
    async def send_daily_report(self) -> None:
        """일일 보고서를 생성하고 전송합니다."""
        try:
            report_data = await self.generate_daily_report()
            
            # 보고서 형식 지정
            timestamp = report_data["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            
            # 시스템 자원 정보
            if "system_resources" in report_data:
                resources = report_data["system_resources"]
                cpu = resources["cpu_percent"]
                memory = resources["memory"]["percent"]
                disk = resources["disk"]["percent"]
                
                # 메모리 정보 변환 (바이트 -> MB/GB)
                mem_total_gb = resources["memory"]["total"] / (1024 ** 3)
                mem_avail_gb = resources["memory"]["available"] / (1024 ** 3)
                
                # 디스크 정보 변환
                disk_total_gb = resources["disk"]["total"] / (1024 ** 3)
                disk_free_gb = resources["disk"]["free"] / (1024 ** 3)
                
                system_info = (
                    f"<b>시스템 자원</b>\n"
                    f"• CPU 사용량: {cpu:.1f}%\n"
                    f"• 메모리: {memory:.1f}% ({mem_avail_gb:.1f}GB 가용 / {mem_total_gb:.1f}GB 전체)\n"
                    f"• 디스크: {disk:.1f}% ({disk_free_gb:.1f}GB 가용 / {disk_total_gb:.1f}GB 전체)\n"
                )
            else:
                system_info = "<b>시스템 자원 정보를 가져올 수 없습니다.</b>\n"
            
            # 프로세스 상태 정보
            process_info = "<b>프로세스 상태</b>\n"
            
            if "processes" in report_data:
                processes = report_data["processes"]
                for name, proc in processes.items():
                    status_emoji = "🟢" if proc["running"] else "🔴"
                    process_info += f"• {status_emoji} {proc['description']} ({name})\n"
            else:
                process_info += "프로세스 정보를 가져올 수 없습니다.\n"
            
            # 전체 보고서 메시지 생성
            message = (
                f"📊 <b>크로스 김프 일일 시스템 보고서</b>\n\n"
                f"{system_info}\n"
                f"{process_info}\n"
                f"<i>생성 시간: {timestamp}</i>"
            )
            
            # 알림 전송
            await self.notification_service.send_notification(message, admin_only=True)
            logger.info(f"{LOG_SYSTEM} 일일 시스템 보고서 전송됨")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 일일 보고서 전송 중 오류: {str(e)}")
            
            # 오류 알림 전송
            error_message = (
                f"⚠️ <b>일일 보고서 생성 실패</b>\n\n"
                f"오류: {str(e)}\n\n"
                f"<i>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
            )
            await self.notification_service.send_notification(error_message, admin_only=True) 