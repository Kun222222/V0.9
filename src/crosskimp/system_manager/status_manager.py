"""
시스템 상태 관리 모듈 - 개선 버전

이 모듈은 시스템 전체의 상태를 중앙에서 관리하고 모니터링합니다.
모든 컴포넌트의 상태 정보를 수집하고 통합하여 시스템 전체 상태를 결정합니다.
"""

import asyncio
import time
import threading
import logging
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.constants_v3 import LOG_SYSTEM, EXCHANGE_NAMES_KR

from crosskimp.common.events import get_component_event_bus, Component
from crosskimp.common.events.domains.status import StatusEventTypes

from crosskimp.system_manager.metric_manager import get_metric_manager
from crosskimp.system_manager.error_manager import get_error_manager, ErrorSeverity, ErrorCategory

# 로거 설정
logger = get_unified_logger()

# 상태 유형 정의
class StatusType:
    NORMAL = "NORMAL"       # 정상
    WARNING = "WARNING"     # 경고
    ERROR = "ERROR"         # 오류
    CRITICAL = "CRITICAL"   # 심각
    UNKNOWN = "UNKNOWN"     # 알 수 없음
    INITIALIZING = "INITIALIZING"  # 초기화 중
    SHUTDOWN = "SHUTDOWN"   # 종료 중

class StatusManager:
    """
    시스템 상태 관리자 클래스
    
    모든 서비스 컴포넌트의 상태를 중앙에서 관리하는 싱글톤 클래스입니다.
    시스템 전체 상태를 모니터링합니다.
    
    특징:
    1. 모든 거래소 및 서비스의 연결 상태 통합 관리
    2. 시스템 리소스 모니터링과 통합
    3. 프로세스 상태 관리와 통합
    4. 주기적인 상태 보고서 생성
    5. 이벤트 버스와 통합되어 상태 변경 이벤트 처리
    6. ErrorManager와 통합되어 오류 기반 상태 평가
    
    [v0.7 업데이트]
    - 개선된 이벤트 시스템과 통합
    - ErrorManager와 통합
    - 상태 평가 알고리즘 개선
    - 이벤트 기반 상태 업데이트 강화
    """
    
    _instance = None
    _lock = threading.RLock()
    
    @classmethod
    def get_instance(cls) -> 'StatusManager':
        """싱글톤 인스턴스 반환"""
        with cls._lock:
            if cls._instance is None:
                cls._instance = StatusManager()
            return cls._instance
    
    def __init__(self):
        """초기화 - StatusManager는 싱글톤이므로 get_instance()로만 접근해야 함"""
        if StatusManager._instance is not None:
            raise Exception("StatusManager는 싱글톤입니다. get_instance()를 사용하세요.")
        
        # 이벤트 버스 인스턴스 가져오기 (변경됨)
        self.event_bus = get_component_event_bus(Component.SERVER)
        
        # 중앙 메트릭 관리자 인스턴스 가져오기
        self.metric_manager = get_metric_manager()
        
        # 오류 관리자 인스턴스 가져오기
        self.error_manager = get_error_manager()
        
        # 상태 업데이트 태스크
        self.status_task = None
        
        # 종료 이벤트
        self.stop_event = asyncio.Event()
        
        # 전체 시스템 상태
        self.overall_status = {
            "status_type": StatusType.INITIALIZING,
            "message": "시스템 초기화 중",
            "last_update": time.time(),
            "startup_time": None
        }
        
        # 서비스 상태 (프로세스 및 컴포넌트)
        self.service_status = {}
        
        # 시스템 리소스 상태
        self.system_resources = {
            "cpu": {
                "percent": 0.0,
                "status_type": StatusType.NORMAL,
                "cores": 0
            },
            "memory": {
                "percent": 0.0,
                "total_gb": 0.0,
                "used_gb": 0.0,
                "status_type": StatusType.NORMAL
            },
            "disk": {
                "percent": 0.0,
                "total_gb": 0.0,
                "used_gb": 0.0,
                "status_type": StatusType.NORMAL
            },
            "last_update": 0
        }
        
        # 임계값 설정
        self.thresholds = {
            "cpu": {
                "warning": 70.0,
                "error": 90.0
            },
            "memory": {
                "warning": 70.0,
                "error": 90.0
            },
            "disk": {
                "warning": 80.0,
                "error": 95.0
            }
        }
        
        # 상태 업데이트 주기 (초)
        self.update_interval = 60
        
        # 상태 로깅 주기 (초)
        self.log_interval = 300  # 5분마다 로깅
        self.last_log_time = 0
        
        # 테스트 모드 플래그
        self.test_mode = False
        
        logger.info(f"{LOG_SYSTEM} 상태 관리자 초기화 완료")
        
    async def start(self):
        """상태 관리자 시작 - 상태 업데이트 태스크 시작"""
        if self.status_task is None or self.status_task.done():
            self.stop_event.clear()
            self.status_task = asyncio.create_task(self._monitor_status())
            logger.info(f"{LOG_SYSTEM} 시스템 상태 업데이트 태스크가 시작되었습니다.")
            
            # 시스템 레벨 이벤트 구독 (변경됨)
            await self.event_bus.subscribe(StatusEventTypes.SYSTEM_STARTUP, self._handle_system_event)
            await self.event_bus.subscribe(StatusEventTypes.SYSTEM_SHUTDOWN, self._handle_system_event)
            await self.event_bus.subscribe(StatusEventTypes.PROCESS_STATUS, self._handle_process_status)
            await self.event_bus.subscribe(StatusEventTypes.RESOURCE_USAGE, self._handle_resource_usage)
            await self.event_bus.subscribe(StatusEventTypes.SYSTEM_STATUS_CHANGE, self._handle_system_status_change)
            
            # 거래소 건강 상태 요약 이벤트 구독 (EventHandler로부터)
            await self.event_bus.subscribe(StatusEventTypes.EXCHANGE_HEALTH_UPDATE, self._handle_exchange_health_update)
            
            # 오류 이벤트 구독
            await self.event_bus.subscribe(StatusEventTypes.ERROR_EVENT, self._handle_error_event)
            
            # 기존 이벤트 구독 (호환성 유지)
            await self.event_bus.subscribe(StatusEventTypes.CONNECTION_STATUS, self._handle_connection_status)
            await self.event_bus.subscribe(StatusEventTypes.METRIC_UPDATE, self._handle_metric_update)
    
    async def stop(self):
        """상태 관리자 중지 - 상태 업데이트 태스크 중지"""
        if self.status_task and not self.status_task.done():
            self.stop_event.set()
            try:
                await asyncio.wait_for(self.status_task, timeout=1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self.status_task.cancel()
                logger.debug(f"{LOG_SYSTEM} 시스템 상태 업데이트 태스크가 취소되었습니다.")
            
            # 이벤트 버스 구독 해제 (변경됨)
            await self.event_bus.unsubscribe(StatusEventTypes.CONNECTION_STATUS, self._handle_connection_status)
            await self.event_bus.unsubscribe(StatusEventTypes.METRIC_UPDATE, self._handle_metric_update)
            await self.event_bus.unsubscribe(StatusEventTypes.SYSTEM_STARTUP, self._handle_system_event)
            await self.event_bus.unsubscribe(StatusEventTypes.SYSTEM_SHUTDOWN, self._handle_system_event)
            await self.event_bus.unsubscribe(StatusEventTypes.PROCESS_STATUS, self._handle_process_status)
            await self.event_bus.unsubscribe(StatusEventTypes.RESOURCE_USAGE, self._handle_resource_usage)
            await self.event_bus.unsubscribe(StatusEventTypes.SYSTEM_STATUS_CHANGE, self._handle_system_status_change)
            await self.event_bus.unsubscribe(StatusEventTypes.EXCHANGE_HEALTH_UPDATE, self._handle_exchange_health_update)
            await self.event_bus.unsubscribe(StatusEventTypes.ERROR_EVENT, self._handle_error_event)
    
    async def _monitor_status(self):
        """상태 모니터링 루프 실행"""
        logger.info(f"{LOG_SYSTEM} 상태 모니터링 시작됨")
        
        try:
            # 초기 메트릭 및 상태 설정
            self.last_log_time = time.time()
            
            while not self.stop_event.is_set():
                try:
                    # 시스템 리소스 상태 확인
                    await self._update_system_resources()
                
                    # 서비스 상태 확인
                    await self._update_service_status()
                
                    # 주기적으로 상태 요약 로깅
                    current_time = time.time()
                    if current_time - self.last_log_time > self.log_interval:
                        await self._log_status_summary()
                        self.last_log_time = current_time
                    
                    # 다음 업데이트까지 대기
                    await asyncio.sleep(self.update_interval)
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"{LOG_SYSTEM} 상태 업데이트 중 오류: {str(e)}")
                    await asyncio.sleep(5)  # 오류 발생 시 짧게 대기
                    
        except asyncio.CancelledError:
            # 정상적인 취소 처리
            pass
        except Exception as e:
            # 예상치 못한 오류
            logger.error(f"{LOG_SYSTEM} 상태 모니터링 루프 오류: {str(e)}")
            
        logger.info(f"{LOG_SYSTEM} 상태 모니터링 종료됨")
    
    async def _update_system_resources(self):
        """시스템 리소스 상태 업데이트 (HealthMonitor 이벤트 수신 기반)"""
        # 리소스 이벤트를 받기 때문에 별도 로직 필요 없음
        # 마지막 업데이트 시간 확인하여 오래된 경우 알림
        current_time = time.time()
        last_update = self.system_resources.get("last_update", 0)
        
        if current_time - last_update > 300:  # 5분 이상 업데이트 없음
            logger.warning(f"{LOG_SYSTEM} 시스템 리소스 정보가 오래되었습니다 (마지막 업데이트: {datetime.fromtimestamp(last_update).strftime('%Y-%m-%d %H:%M:%S')})")
            
            # 오류 관리자를 통해 오류 보고
            await self.error_manager.handle_error(
                message="시스템 리소스 정보가 오래되었습니다",
                source="StatusManager",
                category=ErrorCategory.RESOURCE,
                severity=ErrorSeverity.WARNING,
                cooldown_override=900  # 15분마다만 알림
            )
    
    async def _update_service_status(self):
        """서비스 상태 업데이트 (ProcessManager 이벤트 수신 기반)"""
        # 서비스 상태는 PROCESS_STATUS 이벤트를 통해 업데이트됨
        # 서비스 상태 만료 확인
        current_time = time.time()
        expired_services = []
        
        for service_name, status in self.service_status.items():
            last_update = status.get("last_update", 0)
            if current_time - last_update > 300:  # 5분 이상 업데이트 없음
                # 서비스 상태 만료로 표시
                self.service_status[service_name]["status_type"] = StatusType.UNKNOWN
                self.service_status[service_name]["status"] = "unknown"
                expired_services.append(service_name)
        
        if expired_services:
            logger.warning(f"{LOG_SYSTEM} 서비스 상태 정보가 만료됨: {', '.join(expired_services)}")
    
    def _check_exchange_status(self):
        """
        모든 거래소 연결 상태 확인
        
        Returns:
            List[str]: 모든 거래소의 상태 목록
        """
        status_list = []
        
        try:
            # 메트릭 관리자로부터 모든 거래소 상태 조회
            metric_manager = get_metric_manager()
            
            # 모든 거래소 코드 리스트
            exchange_codes = list(EXCHANGE_NAMES_KR.keys())
            
            for exchange_code in exchange_codes:
                # 각 거래소의 connection_status 메트릭 조회
                connection_status = metric_manager.get_metric(exchange_code, "connection_status", "unknown")
                error_count = metric_manager.get_metric(exchange_code, "error_count", 0)
                
                # 연결 상태에 따른 상태 유형 결정
                if connection_status == "connected":
                    # 오류 개수에 따라 상태 결정
                    if error_count and error_count > 10:
                        status_type = StatusType.WARNING
                    else:
                        status_type = StatusType.NORMAL
                elif connection_status == "disconnected":
                    status_type = StatusType.ERROR
                elif connection_status == "reconnecting":
                    status_type = StatusType.WARNING
                else:
                    status_type = StatusType.UNKNOWN
                
                # 상태 목록에 추가
                status_list.append(status_type)
                
                # 서비스 상태 사전에 업데이트
                service_key = f"exchange:{exchange_code}"
                if service_key not in self.service_status:
                    self.service_status[service_key] = {
                        "service_type": "exchange",
                        "name": EXCHANGE_NAMES_KR.get(exchange_code, exchange_code),
                        "status_type": status_type,
                        "status": connection_status,
                        "last_update": time.time(),
                        "message": f"연결 상태: {connection_status}",
                        "details": {
                            "error_count": error_count,
                            "connection_status": connection_status
                        }
                    }
                else:
                    self.service_status[service_key].update({
                        "status_type": status_type,
                        "status": connection_status,
                        "details": {
                            "error_count": error_count,
                            "connection_status": connection_status
                        }
                    })
                    
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 거래소 상태 확인 중 오류: {str(e)}")
            
        return status_list
    
    def _evaluate_overall_status(self):
        """
        전체 시스템 상태 평가 (모든 컴포넌트 상태 기반)
        """
        try:
            # 이전 상태 저장
            old_status_type = self.overall_status["status_type"]
            
            # 모든 거래소 연결 상태 확인
            exchange_status = self._check_exchange_status()
            
            # 모든 서비스 상태 확인
            service_status_list = [s.get("status_type", StatusType.UNKNOWN) for s in self.service_status.values()]
            
            # 시스템 리소스 상태 확인 (CPU, 메모리, 디스크)
            resource_status = []
            for resource in ["cpu", "memory", "disk"]:
                if resource in self.system_resources:
                    resource_status.append(self.system_resources[resource].get("status_type", StatusType.NORMAL))
            
            # 모든 상태를 하나의 리스트로 합침
            all_status = service_status_list + resource_status + exchange_status
            
            # 상태 우선순위에 따라 전체 상태 결정
            status_priority = {
                StatusType.CRITICAL: 1,
                StatusType.ERROR: 2,
                StatusType.WARNING: 3,
                StatusType.UNKNOWN: 4,
                StatusType.NORMAL: 5,
                StatusType.INITIALIZING: 6,
                StatusType.SHUTDOWN: 7
            }
            
            # 시스템이 종료 중이면 SHUTDOWN 상태로 설정
            if self.overall_status.get("status_type") == StatusType.SHUTDOWN:
                new_status_type = StatusType.SHUTDOWN
                status_message = "시스템 종료 중"
            else:
                # 모든 상태 중 가장 심각한 상태 선택
                highest_priority = min([status_priority.get(s, 4) for s in all_status], default=5)
                
                # 우선순위에 따른 상태 맵핑
                status_map = {v: k for k, v in status_priority.items()}
                new_status_type = status_map.get(highest_priority, StatusType.NORMAL)
                
                # 상태별 메시지 설정
                if new_status_type == StatusType.CRITICAL:
                    status_message = "심각한 오류 발생"
                elif new_status_type == StatusType.ERROR:
                    status_message = "오류 상태"
                elif new_status_type == StatusType.WARNING:
                    status_message = "경고 상태"
                elif new_status_type == StatusType.UNKNOWN:
                    status_message = "상태 알 수 없음"
                elif new_status_type == StatusType.INITIALIZING:
                    status_message = "초기화 중"
                else:
                    status_message = "정상 작동 중"
        
            # 상태 변경 감지 및 업데이트
            if old_status_type != new_status_type:
                # 상태 업데이트
                self.overall_status = {
                    "status_type": new_status_type,
                    "message": status_message,
                    "last_update": time.time(),
                    "startup_time": self.overall_status.get("startup_time")
                }
            
                # 상태 변경 이벤트 발행
                if not self.test_mode:
                    asyncio.create_task(self._publish_status_change_event(
                        old_status=old_status_type,
                        new_status=new_status_type,
                        message=status_message
                    ))
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 시스템 상태 평가 중 오류: {str(e)}")
    
    async def _publish_status_change_event(self, old_status: str, new_status: str, message: str):
        """상태 변경 이벤트를 발행합니다."""
        try:
            # 상태가 실제로 변경되었는지 확인
            if old_status == new_status:
                return
                
            # 상태 변경 이벤트 발행 (변경됨)
            await self.event_bus.publish(
                StatusEventTypes.SYSTEM_STATUS_CHANGE,
                {
                    "old_status": old_status,
                    "new_status": new_status,
                    "message": message,
                    "timestamp": time.time()
                }
            )
            
            logger.info(f"{LOG_SYSTEM} 시스템 상태 변경: {old_status} -> {new_status}")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 상태 변경 이벤트 발행 중 오류: {str(e)}")
    
    async def _send_status_alert(self, status: str, message: str):
        """상태 알림 이벤트를 발행합니다."""
        try:
            # 알림 이벤트 발행 (변경됨)
            await self.event_bus.publish(
                StatusEventTypes.SYSTEM_STATUS_CHANGE,
                {
                    "status": status,
                    "message": message,
                    "timestamp": time.time()
                }
            )
            
            # 오류 관리자에 알림
            if status in [StatusType.ERROR, StatusType.CRITICAL]:
                severity = ErrorSeverity.ERROR if status == StatusType.ERROR else ErrorSeverity.CRITICAL
                await self.error_manager.handle_error(
                    message=message,
                    source="StatusManager",
                    category=ErrorCategory.SYSTEM,
                    severity=severity
                )
                
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 상태 알림 발행 중 오류: {str(e)}")
    
    async def _log_status_summary(self):
        """상태 요약 로깅"""
        try:
            # 시스템 전체 상태
            status_type = self.overall_status.get("status_type", "UNKNOWN")
            message = self.overall_status.get("message", "")
            
            summary_lines = [
                f"==== 시스템 상태 요약 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ====",
                f"전체 상태: {status_type} - {message}"
            ]
            
            # 시스템 리소스 상태
            cpu_percent = self.system_resources.get("cpu", {}).get("percent", 0)
            cpu_status = self.system_resources.get("cpu", {}).get("status_type", "UNKNOWN")
            
            memory_percent = self.system_resources.get("memory", {}).get("percent", 0)
            memory_used = self.system_resources.get("memory", {}).get("used_gb", 0)
            memory_total = self.system_resources.get("memory", {}).get("total_gb", 0)
            memory_status = self.system_resources.get("memory", {}).get("status_type", "UNKNOWN")
            
            disk_percent = self.system_resources.get("disk", {}).get("percent", 0)
            disk_used = self.system_resources.get("disk", {}).get("used_gb", 0)
            disk_total = self.system_resources.get("disk", {}).get("total_gb", 0)
            disk_status = self.system_resources.get("disk", {}).get("status_type", "UNKNOWN")
            
            summary_lines.extend([
                "---- 시스템 리소스 ----",
                f"CPU: {cpu_percent:.1f}% ({cpu_status})",
                f"메모리: {memory_percent:.1f}% - {memory_used:.1f}GB/{memory_total:.1f}GB ({memory_status})",
                f"디스크: {disk_percent:.1f}% - {disk_used:.1f}GB/{disk_total:.1f}GB ({disk_status})"
            ])
            
            # 서비스 상태
            summary_lines.append("---- 서비스 상태 ----")
            for service_name, status in self.service_status.items():
                status_type = status.get("status_type", "UNKNOWN")
                process_status = status.get("status", "unknown")
                description = status.get("description", service_name)
                
                summary_lines.append(f"{description}: {process_status} ({status_type})")
            
            # 거래소 상태
            summary_lines.append("---- 거래소 상태 ----")
            exchange_status = self.metric_manager.get_all_exchange_status()
            
            for exchange_code, status in exchange_status.items():
                status_type = status.get("status_type", "UNKNOWN")
                connection_status = status.get("connection_status", "disconnected")
                exchange_name = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
                
                # 오류 메시지가 있으면 표시
                error_message = status.get("last_error_message", "")
                if error_message and status_type != StatusType.NORMAL:
                    summary_lines.append(f"{exchange_name}: {connection_status} ({status_type}) - {error_message}")
                else:
                    summary_lines.append(f"{exchange_name}: {connection_status} ({status_type})")
            
            # 요약 로깅
            logger.info("\n".join(summary_lines))
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 상태 요약 로깅 중 오류: {str(e)}")
    
    async def _handle_connection_status(self, event_data):
        """
        연결 상태 이벤트 처리 - 이제 주요 CONNECTION_STATUS 이벤트 처리를 담당합니다.
        EventHandler는 이제 연결 상태만 업데이트하고 이벤트를 발행합니다.
        
        Args:
            event_data: 이벤트 데이터 (exchange_code, status, timestamp 포함)
        """
        try:
            # 필수 데이터 추출
            exchange_code = event_data.get("exchange_code")
            status = event_data.get("status")
            timestamp = event_data.get("timestamp", time.time())
            old_status = event_data.get("old_status", None)  # 이전 상태
            exchange_name = event_data.get("exchange_name", exchange_code)
            
            if not exchange_code or not status:
                logger.warning(f"{LOG_SYSTEM} 연결 상태 이벤트 데이터 누락: {event_data}")
                return
            
            # 서비스 상태 업데이트
            service_key = f"exchange:{exchange_code}"
            if service_key not in self.service_status:
                self.service_status[service_key] = {
                    "service_type": "exchange",
                    "name": exchange_name,
                    "status_type": StatusType.UNKNOWN,
                    "status": "unknown",
                    "last_update": 0,
                    "message": "초기화 중",
                    "details": {}
                }
            
            # 연결 상태 기반으로 서비스 상태 결정
            status_type = StatusType.NORMAL
            if status == "connected":
                status_type = StatusType.NORMAL
                status_message = f"{exchange_name} 연결됨"
            elif status == "disconnected":
                status_type = StatusType.ERROR
                status_message = f"{exchange_name} 연결 끊김"
            elif status == "reconnecting":
                status_type = StatusType.WARNING
                status_message = f"{exchange_name} 재연결 중"
            else:
                status_type = StatusType.UNKNOWN
                status_message = f"{exchange_name} 상태: {status}"
            
            # 서비스 상태 업데이트
            self.service_status[service_key].update({
                "status_type": status_type,
                "status": status,
                "last_update": timestamp,
                "message": status_message,
                "details": {
                    "connection_status": status,
                    "last_status_change": timestamp
                }
            })
            
            # 연결 끊김이 발생하고 종료 중이 아닌 경우 경고 알림
            if status == "disconnected" and old_status == "connected":
                await self._send_status_alert(
                    status=status_type,
                    message=f"{exchange_name} 연결이 끊어졌습니다"
                )
                
                # 오류 관리자에 오류 보고 (심각도: 경고)
                await self.error_manager.handle_error(
                    message=f"{exchange_name} 연결이 끊어졌습니다",
                    source="StatusManager",
                    category="connection",
                    severity="warning",
                    exchange=exchange_code
                )
            
            # 상태가 복구된 경우 정보 알림
            elif status == "connected" and old_status == "disconnected":
                await self._send_status_alert(
                    status=status_type,
                    message=f"{exchange_name} 연결이 복구되었습니다"
                )
            
            # 전체 시스템 상태 재평가
            self._evaluate_overall_status()
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 연결 상태 이벤트 처리 중 오류: {str(e)}")
    
    async def _handle_metric_update(self, event_data):
        """
        메트릭 업데이트 이벤트 처리 - 이제 단순 위임(Delegate) 역할만 수행
        MetricManager가 메트릭 처리 담당, StatusManager는 상태 변경 이벤트만 발행
        
        Args:
            event_data: 이벤트 데이터 (exchange_code, metric_name, value 포함)
        """
        # 처리가 필요한 메트릭만 처리 (상태 변경 관련)
        # 나머지는 MetricManager가 처리
        try:
            exchange_code = event_data.get("exchange_code", "")
            metric_name = event_data.get("metric_name", "")
            
            # 중요 상태 변경 메트릭만 처리 (예: error_count가 임계값 초과 등)
            if metric_name == "error_count" and exchange_code:
                # 오류 횟수가 특정 임계값을 초과하면 상태 변경 이벤트 발행
                value = event_data.get("value", 0)
                if isinstance(value, (int, float)) and value > 10:  # 임계값
                    # 오류 발생이 지속적인 경우 경고 상태로 변경
                    service_key = f"exchange:{exchange_code}"
                    if service_key in self.service_status:
                        current_status = self.service_status[service_key].get("status_type")
                        if current_status != StatusType.ERROR:
                            # 상태 업데이트
                            self.service_status[service_key].update({
                                "status_type": StatusType.WARNING,
                                "message": f"오류 발생 ({int(value)}회)",
                                "last_update": time.time()
                            })
                            
                            # 전체 시스템 상태 재평가
                            self._evaluate_overall_status()
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 메트릭 업데이트 이벤트 처리 중 오류: {str(e)}")
    
    async def _handle_exchange_health_update(self, event_data):
        """거래소 건강 상태 요약 이벤트 처리"""
        try:
            exchange_code = event_data.get("exchange_code", "").lower()
            status = event_data.get("status")
            message = event_data.get("message", "")
            timestamp = event_data.get("timestamp", time.time())
            
            if not exchange_code or not status:
                return
            
            # 상태 유형 매핑
            status_type = StatusType.UNKNOWN
            if status == "healthy":
                status_type = StatusType.NORMAL
            elif status == "degraded":
                status_type = StatusType.WARNING
            elif status == "critical":
                status_type = StatusType.ERROR
            
            # 중앙 메트릭 관리자에 상태 업데이트
            self.metric_manager.update_metric(
                exchange_code,
                "status_type",
                status_type
            )
            
            # 메시지가 있으면 마지막 오류 메시지로 저장
            if message and status_type in [StatusType.WARNING, StatusType.ERROR]:
                self.metric_manager.update_metric(
                    exchange_code,
                    "last_error_message",
                    message
                )
            
            # 시스템 전체 상태 재평가
            self._evaluate_overall_status()
            
            # 상태 로깅
            if status_type != StatusType.NORMAL:
                logger.warning(f"{LOG_SYSTEM} {EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 상태: {status} - {message}")
            else:
                logger.debug(f"{LOG_SYSTEM} {EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 상태: {status}")
                
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 거래소 상태 이벤트 처리 중 오류: {str(e)}")
    
    async def _handle_process_status(self, event_data):
        """프로세스 상태 변경 이벤트 처리"""
        try:
            process_name = event_data.get("process_name")
            status = event_data.get("status")
            pid = event_data.get("pid")
            description = event_data.get("description", process_name)
            
            if not process_name or not status:
                return
            
            # 상태 유형 매핑
            status_type = StatusType.UNKNOWN
            if status == "running":
                status_type = StatusType.NORMAL
            elif status == "stopped":
                status_type = StatusType.WARNING
            elif status in ["error", "terminated", "failed"]:
                status_type = StatusType.ERROR
            
            # 서비스 상태 업데이트
            self.service_status[process_name] = {
                "status": status,
                "status_type": status_type,
                "last_update": time.time(),
                "pid": pid,
                "description": description
            }
            
            # 시스템 전체 상태 재평가
            self._evaluate_overall_status()
            
            # 상태 로깅
            if status_type != StatusType.NORMAL:
                logger.warning(f"{LOG_SYSTEM} 프로세스 '{description}' 상태: {status}")
            else:
                logger.info(f"{LOG_SYSTEM} 프로세스 '{description}' 상태: {status}")
                
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 프로세스 상태 이벤트 처리 중 오류: {str(e)}")
    
    async def _handle_resource_usage(self, event_data):
        """리소스 사용량 이벤트 처리"""
        try:
            resource_type = event_data.get("resource_type")
            value = event_data.get("value")
            
            if not resource_type or value is None:
                return
            
            # 리소스 타입에 따라 처리
            if resource_type == "cpu":
                self.system_resources["cpu"]["percent"] = value
                
                # 상태 평가
                if value >= self.thresholds["cpu"]["error"]:
                    self.system_resources["cpu"]["status_type"] = StatusType.ERROR
                elif value >= self.thresholds["cpu"]["warning"]:
                    self.system_resources["cpu"]["status_type"] = StatusType.WARNING
                else:
                    self.system_resources["cpu"]["status_type"] = StatusType.NORMAL
                    
            elif resource_type == "memory":
                self.system_resources["memory"]["percent"] = value
                if "total" in event_data:
                    self.system_resources["memory"]["total_gb"] = event_data["total"]
                if "used" in event_data:
                    self.system_resources["memory"]["used_gb"] = event_data["used"]
                
                # 상태 평가
                if value >= self.thresholds["memory"]["error"]:
                    self.system_resources["memory"]["status_type"] = StatusType.ERROR
                elif value >= self.thresholds["memory"]["warning"]:
                    self.system_resources["memory"]["status_type"] = StatusType.WARNING
                else:
                    self.system_resources["memory"]["status_type"] = StatusType.NORMAL
                    
            elif resource_type == "disk":
                self.system_resources["disk"]["percent"] = value
                if "total" in event_data:
                    self.system_resources["disk"]["total_gb"] = event_data["total"]
                if "used" in event_data:
                    self.system_resources["disk"]["used_gb"] = event_data["used"]
                
                # 상태 평가
                if value >= self.thresholds["disk"]["error"]:
                    self.system_resources["disk"]["status_type"] = StatusType.ERROR
                elif value >= self.thresholds["disk"]["warning"]:
                    self.system_resources["disk"]["status_type"] = StatusType.WARNING
                else:
                    self.system_resources["disk"]["status_type"] = StatusType.NORMAL
            
            # 시간 업데이트
            self.system_resources["last_update"] = time.time()
            
            # 시스템 전체 상태 재평가
            self._evaluate_overall_status()
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 리소스 사용량 이벤트 처리 중 오류: {str(e)}")

    async def _handle_system_status_change(self, event_data):
        """시스템 상태 변경 이벤트 처리"""
        try:
            old_status = event_data.get("old_status")
            new_status = event_data.get("new_status")
            message = event_data.get("message", "")
            timestamp = event_data.get("timestamp", time.time())
            
            if not new_status:
                return
            
            # 상태 업데이트 (외부 컴포넌트로부터 받은 상태)
            self.overall_status = {
                "status_type": new_status,
                "message": message,
                "last_update": timestamp
            }
            
            # 상태 변경 알림 (ERROR 또는 CRITICAL로 변경 시에만)
            if new_status in [StatusType.ERROR, StatusType.CRITICAL] and old_status not in [StatusType.ERROR, StatusType.CRITICAL]:
                asyncio.create_task(self._send_status_alert(new_status, message))
            
            # 상태 로깅
            logger.info(f"{LOG_SYSTEM} 시스템 상태 변경: {old_status} -> {new_status}")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 시스템 상태 변경 이벤트 처리 중 오류: {str(e)}")
    
    async def _handle_error_event(self, event_data):
        """오류 이벤트 처리"""
        try:
            error_type = event_data.get("error_type", "")
            severity = event_data.get("severity", ErrorSeverity.WARNING)
            message = event_data.get("message", "")
            source = event_data.get("source", "")
            exchange_code = event_data.get("exchange_code")
            
            # 심각도에 따른 상태 유형 결정
            status_type = StatusType.NORMAL
            if severity == ErrorSeverity.CRITICAL:
                status_type = StatusType.CRITICAL
            elif severity == ErrorSeverity.ERROR:
                status_type = StatusType.ERROR
            elif severity == ErrorSeverity.WARNING:
                status_type = StatusType.WARNING
            
            # 거래소 관련 오류인 경우
            if exchange_code:
                # 메트릭 업데이트
                self.metric_manager.update_metric(
                    exchange_code,
                    "error_count",
                    1.0,
                    message=message,
                    error_type=error_type
                )
                
                # 오류 메시지 저장
                if status_type in [StatusType.WARNING, StatusType.ERROR, StatusType.CRITICAL]:
                    self.metric_manager.update_metric(
                        exchange_code,
                        "last_error_message",
                        message
                    )
                    
                    # 상태 유형 업데이트
                    self.metric_manager.update_metric(
                        exchange_code,
                        "status_type",
                        status_type
                    )
            
            # 시스템 오류인 경우
            elif error_type in [ErrorCategory.SYSTEM, ErrorCategory.PROCESS, ErrorCategory.RESOURCE]:
                # 서비스 상태 업데이트
                if source in self.service_status:
                    self.service_status[source].update({
                        "status_type": status_type,
                        "last_error": message,
                        "last_error_time": time.time()
                    })
            
            # 시스템 전체 상태 재평가
            self._evaluate_overall_status()
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 오류 이벤트 처리 중 예외: {str(e)}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        시스템 전체 상태 정보 반환
        
        Returns:
            Dict: 시스템 상태 정보
        """
        return {
            "overall_status": self.overall_status.copy(),
            "system_resources": self.system_resources.copy(),
            "service_count": len(self.service_status),
            "service_errors": sum(1 for s in self.service_status.values() 
                            if s.get("status_type") in [StatusType.ERROR, StatusType.CRITICAL]),
            "exchange_count": len(self.metric_manager.get_all_exchange_status()),
            "exchange_errors": sum(1 for s in self.metric_manager.get_all_exchange_status().values() 
                              if s.get("status_type") in [StatusType.ERROR, StatusType.CRITICAL])
        }
    
    def get_exchange_status(self, exchange_code: Optional[str] = None) -> Dict[str, Any]:
        """
        거래소 상태 정보 반환
        
        Args:
            exchange_code: 거래소 코드 (None이면 모든 거래소)
            
        Returns:
            Dict: 거래소 상태 정보
        """
        if exchange_code:
            return self.metric_manager.get_exchange_status(exchange_code)
        else:
            return self.metric_manager.get_all_exchange_status()
    
    def get_service_status(self, service_name: Optional[str] = None) -> Dict[str, Any]:
        """
        서비스 상태 정보 반환
        
        Args:
            service_name: 서비스 이름 (None이면 모든 서비스)
            
        Returns:
            Dict: 서비스 상태 정보
        """
        if service_name:
            return self.service_status.get(service_name, {})
        else:
            return self.service_status.copy()
    
    def update_service_status(self, service_name: str, status: str, **kwargs) -> None:
        """
        서비스 상태 수동 업데이트
        
        Args:
            service_name: 서비스 이름
            status: 상태값
            **kwargs: 추가 상태 데이터
        """
        # 상태 유형 결정
        status_type = self._get_status_type(status)
        
        # 기존 항목 업데이트 또는 새로 추가
        if service_name in self.service_status:
            self.service_status[service_name].update({
                "status": status,
                "status_type": status_type,
                "last_update": time.time(),
                **kwargs
            })
        else:
            self.service_status[service_name] = {
                "status": status,
                "status_type": status_type,
                "last_update": time.time(),
                **kwargs
            }
        
        # 전체 시스템 상태 재평가
        self._evaluate_overall_status()
        
        logger.debug(f"{LOG_SYSTEM} 서비스 '{service_name}' 상태 수동 업데이트: {status} ({status_type})")
    
    def set_thresholds(self, resource_type: str, warning: float, error: float) -> None:
        """
        리소스 임계값 설정
        
        Args:
            resource_type: 리소스 유형 ('cpu', 'memory', 'disk')
            warning: 경고 임계값 (%)
            error: 오류 임계값 (%)
        """
        if resource_type in self.thresholds:
            self.thresholds[resource_type]["warning"] = warning
            self.thresholds[resource_type]["error"] = error
            logger.debug(f"{LOG_SYSTEM} {resource_type} 임계값 설정: 경고={warning}%, 오류={error}%")
    
    def set_update_interval(self, seconds: int) -> None:
        """
        상태 업데이트 주기 설정
        
        Args:
            seconds: 업데이트 주기 (초)
        """
        if seconds > 0:
            self.update_interval = seconds
            logger.debug(f"{LOG_SYSTEM} 상태 업데이트 주기 설정: {seconds}초")
    
    def set_log_interval(self, seconds: int) -> None:
        """
        상태 로깅 주기 설정
        
        Args:
            seconds: 로깅 주기 (초)
        """
        if seconds > 0:
            self.log_interval = seconds
            logger.debug(f"{LOG_SYSTEM} 상태 로깅 주기 설정: {seconds}초")

    def _get_status_type(self, status: str) -> str:
        """
        상태 문자열에서 상태 유형 결정
        
        Args:
            status: 상태 문자열
            
        Returns:
            str: 상태 유형 (StatusType 상수 중 하나)
        """
        status_type = StatusType.UNKNOWN
        
        if status == "running":
            status_type = StatusType.NORMAL
        elif status == "stopped":
            status_type = StatusType.WARNING
        elif status in ["error", "terminated", "failed"]:
            status_type = StatusType.ERROR
        elif status == "initializing":
            status_type = StatusType.INITIALIZING
        elif status == "shutdown":
            status_type = StatusType.SHUTDOWN
        # 상태값이 StatusType의 상수값 중 하나와 일치하는 경우
        elif status in StatusType.__dict__.values():
            status_type = status
            
        return status_type

    async def _handle_system_event(self, event_data):
        """시스템 시작/종료 이벤트 처리"""
        event_type = event_data.get("event_type", "unknown")
        
        # 이벤트 타입 확인 및 수정
        if "startup" in event_type or event_type == StatusEventTypes.SYSTEM_STARTUP:
            # 시스템 시작 이벤트 처리
            logger.info(f"{LOG_SYSTEM} 시스템 시작 이벤트 수신")
            
            # 시스템 상태 업데이트
            self.overall_status["status_type"] = StatusType.INITIALIZING
            self.overall_status["message"] = "시스템 초기화 중"
            self.overall_status["startup_time"] = time.time()
            
        elif "shutdown" in event_type or event_type == StatusEventTypes.SYSTEM_SHUTDOWN:
            # 시스템 종료 이벤트 처리
            logger.info(f"{LOG_SYSTEM} 시스템 종료 이벤트 수신")
            
            # 시스템 상태 업데이트
            old_status = self.overall_status["status_type"]
            self.overall_status["status_type"] = StatusType.SHUTDOWN
            self.overall_status["message"] = "시스템 종료 중"
            
            # 상태 변경 이벤트 발행
            await self._publish_status_change_event(
                old_status,
                StatusType.SHUTDOWN,
                "시스템 종료 중"
            )

# 편의를 위한 글로벌 함수
def get_status_manager() -> StatusManager:
    """StatusManager 싱글톤 인스턴스 반환"""
    return StatusManager.get_instance() 