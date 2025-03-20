# file: orderbook/connection/base_connector.py

import asyncio
import time
from typing import Optional, Callable
from asyncio import Event
from dataclasses import dataclass
from abc import ABC, abstractmethod

from crosskimp.logger.logger import get_unified_logger
from crosskimp.telegrambot.telegram_notification import send_telegram_message
from crosskimp.config.constants_v3 import Exchange, EXCHANGE_NAMES_KR, normalize_exchange_code

# 이벤트 타입 정의 추가
EVENT_TYPES = {
    "CONNECTION_STATUS": "connection_status",  # 연결 상태 변경
    "METRIC_UPDATE": "metric_update",          # 메트릭 업데이트
    "ERROR_EVENT": "error_event",              # 오류 이벤트
    "SUBSCRIPTION_STATUS": "subscription_status"  # 구독 상태 변경
}

# 전역 로거 설정
logger = get_unified_logger()

@dataclass
class WebSocketStats:
    """웹소켓 통계 데이터"""
    message_count: int = 0
    error_count: int = 0
    reconnect_count: int = 0
    last_message_time: float = 0.0
    last_error_time: float = 0.0
    last_error_message: str = ""
    connection_start_time: float = 0.0

class WebSocketError(Exception):
    """웹소켓 관련 기본 예외 클래스"""
    pass

@dataclass
class ReconnectStrategy:
    """웹소켓 재연결 전략"""
    initial_delay: float = 1.0  # 초기 재연결 지연 시간
    max_delay: float = 60.0     # 최대 지연 시간
    multiplier: float = 2.0     # 지연 시간 증가 배수
    max_attempts: int = 0       # 최대 시도 횟수 (0=무제한)
    
    current_delay: float = 1.0
    attempts: int = 0
    
    def next_delay(self) -> float:
        """다음 재연결 지연 시간 계산"""
        self.attempts += 1
        
        # 최대 시도 횟수 초과 확인
        if self.max_attempts > 0 and self.attempts > self.max_attempts:
            raise WebSocketError(f"최대 재연결 시도 횟수 초과: {self.attempts}/{self.max_attempts}")
        
        # 첫 번째 시도는 초기 지연 시간 사용
        if self.attempts == 1:
            self.current_delay = self.initial_delay
            return self.current_delay
        
        # 지연 시간 증가 (최대값 제한)
        self.current_delay = min(self.current_delay * self.multiplier, self.max_delay)
        return self.current_delay
    
    def reset(self) -> None:
        """재연결 시도 횟수 및 지연 시간 초기화"""
        self.attempts = 0
        self.current_delay = self.initial_delay

class BaseWebsocketConnector(ABC):
    """웹소켓 연결 관리 기본 클래스"""
    def __init__(self, settings: dict, exchangename: str):
        """초기화"""
        # 기본 정보
        self.exchangename = normalize_exchange_code(exchangename)  # 소문자로 정규화
        self.exchange_code = self.exchangename  # 필드명 일관성을 위한 별칭
        self.settings = settings
        
        # 거래소 한글 이름 가져오기 (소문자 키 사용)
        self.exchange_name_kr = EXCHANGE_NAMES_KR.get(self.exchangename, f"[{self.exchangename}]")

        # 기본 상태 변수
        self.ws = None
        self.stop_event = Event()
        
        # 연결 상태 플래그 추가
        self.connecting = False
        self._is_connected = False  # 내부 연결 상태 플래그 추가
        
        # 웹소켓 통계
        self.stats = WebSocketStats()
        
        # 이벤트 버스 초기화
        from crosskimp.ob_collector.orderbook.util.event_bus import EventBus
        self.event_bus = EventBus.get_instance()
        
        # SystemEventManager 초기화
        from crosskimp.ob_collector.orderbook.util.system_event_manager import SystemEventManager, EVENT_TYPES
        self.system_event_manager = SystemEventManager.get_instance()
        self.system_event_manager.initialize_exchange(self.exchangename)
        
        # 현재 거래소 코드 설정
        self.system_event_manager.set_current_exchange(self.exchangename)
        
        # 자식 클래스에서 설정해야 하는 변수들
        self.reconnect_strategy = None  # 재연결 전략
        self.message_timeout = None     # 메시지 타임아웃 (초)
        self.health_check_interval = 5  # 헬스 체크 간격 기본값 (초)
        self.ping_interval = None       # 핑 전송 간격 (초)
        self.ping_timeout = None        # 핑 응답 타임아웃 (초)
        
        # 알림 제한 관련 변수 추가
        self._last_notification_time = {}  # 이벤트 타입별 마지막 알림 시간
        self._notification_cooldown = 60  # 알림 쿨다운 (초)
        
        # 헬스 체크 태스크 변수 추가
        self.health_check_task = None
        self._start_health_check_task()

    # 속성 관리
    @property
    def is_connected(self) -> bool:
        """연결 상태 확인 - 모든 다른 클래스가 이 속성을 참조함"""
        return self._is_connected
    
    @is_connected.setter
    def is_connected(self, value: bool) -> None:
        """연결 상태 설정"""
        if self._is_connected != value:
            self._is_connected = value
            
            # 상태 변경 이벤트 발생
            state = "connected" if value else "disconnected"
            
            # 로깅
            if value:
                self.log_debug("연결됨")
            else:
                self.log_debug("연결 끊김")
                
            # 시스템 이벤트 발행 - 연결 상태 변경
            self.publish_system_event(
                EVENT_TYPES["CONNECTION_STATUS"],
                status=state,
                timestamp=time.time(),
                duration=time.time() - self.stats.connection_start_time if value else 0
            )
            
            # 텔레그램 알림 전송 (비동기)
            event_type = "connect" if value else "disconnect"
            asyncio.create_task(self.send_telegram_notification(event_type, f"웹소켓 {state}"))

    def publish_system_event(self, event_type: str, **data) -> None:
        """
        시스템 이벤트 발행 (표준화된 방식)
        
        Args:
            event_type: 이벤트 타입 (EVENT_TYPES 상수 사용)
            **data: 이벤트 데이터
        """
        try:
            # exchange_code 필드가 없으면 추가
            if "exchange_code" not in data:
                data["exchange_code"] = self.exchangename
                
            # 시스템 이벤트 발행 (동기식)
            if hasattr(self, "system_event_manager"):
                self.system_event_manager.publish_system_event_sync(event_type, **data)
            else:
                # 이전 방식으로 직접 이벤트 발행 (호환성 유지)
                event = {
                    "event_type": event_type,
                    "exchange_code": self.exchangename,
                    "timestamp": time.time(),
                    "data": data
                }
                
                # 이벤트 루프 확인하여 비동기 또는 동기식 발행
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(self.event_bus.publish("system_event", event))
                else:
                    self.event_bus.publish_sync("system_event", event)
                    
        except Exception as e:
            self.log_error(f"이벤트 발행 실패: {str(e)}")

    def _publish_connection_event(self, status: str) -> None:
        """이벤트 버스를 통해 연결 상태 이벤트 발행"""
        # 이전 방식의 이벤트 발행 유지 (호환성)
        # 이벤트 데이터 준비
        event_data = {
            "exchange_code": self.exchangename, 
            "status": status,
            "timestamp": time.time()
        }
        
        # 현재 실행 중인 이벤트 루프가 있으면 create_task 사용
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 두 가지 이벤트 채널로 발행:
                # 1. 직접 연결 상태 이벤트 (구독 관리용)
                asyncio.create_task(self.event_bus.publish("connection_status_direct", event_data))
                # 2. 시스템 전체 연결 상태 변경 이벤트 (UI 및 외부 시스템용)
                asyncio.create_task(self.event_bus.publish("connection_status_changed", event_data))
            else:
                # 이벤트 루프가 실행 중이 아니면 동기식 publish 사용
                self.event_bus.publish_sync("connection_status_direct", event_data)
                self.event_bus.publish_sync("connection_status_changed", event_data)
        except Exception as e:
            self.log_warning(f"이벤트 버스 발행 실패: {str(e)}")

    def _start_health_check_task(self) -> None:
        """헬스 체크 태스크 시작"""
        # 이미 실행 중인 태스크가 있는지 확인
        if hasattr(self, 'health_check_task') and self.health_check_task and not self.health_check_task.done():
            # 이미 실행 중인 태스크가 있으면 새로 시작하지 않음
            self.log_debug("헬스 체크 태스크가 이미 실행 중입니다")
            return
            
        self.health_check_task = asyncio.create_task(self.health_check())
        self.log_debug("헬스 체크 태스크 시작됨")

    def _should_start_health_check(self) -> bool:
        """
        헬스 체크 태스크를 시작해야 하는지 확인
        
        Returns:
            bool: 태스크가 없거나 완료된 경우 True, 실행 중인 경우 False
        """
        return not hasattr(self, 'health_check_task') or self.health_check_task is None or self.health_check_task.done()

    # 웹소켓 연결 관리
    @abstractmethod
    async def connect(self) -> bool:
        """웹소켓 연결"""
        pass
        
    async def disconnect(self) -> bool:
        """웹소켓 연결 종료"""
        try:
            # 헬스 체크 태스크 취소 (완전히 종료되는 경우)
            local_task = getattr(self, 'health_check_task', None)
            if local_task and not local_task.done():
                try:
                    local_task.cancel()
                    await asyncio.wait_for(asyncio.shield(local_task), timeout=0.5)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                except Exception as e:
                    self.log_debug(f"태스크 취소 중 예외 발생: {e}")
                finally:
                    self.health_check_task = None
            
            # 웹소켓 연결 종료
            if self.ws:
                await self.ws.close()
                self.ws = None
            
            # 연결 상태 업데이트 - 이것이 콜백과 메트릭 업데이트를 모두 트리거합니다
            self.is_connected = False
            
            self.log_info("웹소켓 연결 종료됨")
            return True
                
        except Exception as e:
            self.log_error(f"연결 종료 실패: {str(e)}")
            # 상태는 여전히 업데이트해야 함
            self.is_connected = False
            return False
    
    async def reconnect(self) -> bool:
        """웹소켓 재연결"""
        try:
            self.stats.reconnect_count += 1
            reconnect_msg = f"웹소켓 재연결 시도"
            self.log_info(reconnect_msg)
            await self.send_telegram_notification("reconnect", reconnect_msg)
            
            await self.disconnect()
            
            # 재연결 지연 계산
            delay = self.reconnect_strategy.next_delay()
            await asyncio.sleep(delay)
            
            success = await self.connect()
            
            # 연결 성공시 알림 추가
            if success:
                connect_msg = "웹소켓 재연결 성공"
                await self.send_telegram_notification("connect", connect_msg)
            
            return success
            
        except Exception as e:
            self.log_error(f"재연결 실패: {str(e)}")
            return False
    
    async def get_websocket(self):
        """
        웹소켓 객체 반환 (필요시 자동 연결)
        
        Returns:
            웹소켓 객체 또는 None (연결 실패 시)
        """
        # 이미 연결된 상태면 현재 웹소켓 반환
        if self.is_connected and self.ws:
            return self.ws
        
        # 연결 중이면 중복 연결 방지
        if self.connecting:
            self.log_debug("이미 연결 시도 중")
            # 잠시 대기 후 상태 확인 (동시 연결 시도 방지)
            for _ in range(10):  # 최대 1초 대기
                await asyncio.sleep(0.1)
                if self.is_connected and self.ws:
                    return self.ws
            # 여전히 연결되지 않았다면 현재 상태 반환 (None일 수 있음)
            return self.ws
        
        # 연결되지 않았다면 자동으로 연결 시도
        self.log_info("웹소켓 연결이 없어 자동 연결 시도")
        
        try:
            # connect()는 is_connected 속성을 설정하므로 여기서는 설정하지 않음
            success = await self.connect()
            return self.ws if success else None
        except Exception as e:
            self.log_error(f"자동 연결 중 오류 발생: {str(e)}")
            return None

    # 상태 모니터링 (중앙화)
    async def health_check(self) -> None:
        """웹소켓 상태 체크 (주기적 모니터링)"""
        cancel_log_shown = False
        try:
            self.log_info("상태 모니터링 시작")
            
            while not self.stop_event.is_set():
                try:
                    # 연결 상태 확인 로직
                    current_connection_state = False
                    
                    # 웹소켓 객체가 존재하는지 확인
                    if self.ws:
                        # 여기서 추가적인 연결 상태 확인 (자식 클래스에서 구현 가능)
                        current_connection_state = True
                    
                    # 상태가 변경되었거나 특정 조건을 만족하면 상태 업데이트
                    if current_connection_state != self.is_connected:
                        self.is_connected = current_connection_state
                    
                    # 핑 전송 등 추가 상태 체크 로직은 자식 클래스에서 구현
                    
                    # 대기
                    await asyncio.sleep(self.health_check_interval)
                    
                except asyncio.CancelledError:
                    cancel_log_shown = True
                    raise  # 상위로 전파
                    
                except Exception as e:
                    self.log_error(f"상태 체크 중 오류: {str(e)}")
                    await asyncio.sleep(1)  # 오류 발생 시 짧게 대기
                
        except asyncio.CancelledError:
            if not cancel_log_shown:
                self.log_info("상태 모니터링 태스크 취소됨")
        
        except Exception as e:
            self.log_error(f"상태 모니터링 루프 오류: {str(e)}")
            
            # 모니터링 태스크가 중단되지 않도록 재시작
            # 단, 종료 이벤트가 설정되지 않은 경우에만 재시작
            if not self.stop_event.is_set():
                asyncio.create_task(self._restart_health_check())

    async def _restart_health_check(self) -> None:
        """헬스 체크 태스크 재시작 (오류 발생 시)"""
        try:
            await asyncio.sleep(1)  # 잠시 대기
            self._start_health_check_task()
        except Exception as e:
            self.log_error(f"헬스 체크 태스크 재시작 실패: {str(e)}")

    # 로깅 및 알림
    def log_error(self, message: str) -> None:
        """오류 로깅 (거래소 이름 포함)"""
        logger.error(f"{self.exchange_name_kr} {message}")
        
        # 웹소켓 통계 업데이트 - 오류 카운트 증가
        self.stats.error_count += 1
        self.stats.last_error_time = time.time()
        self.stats.last_error_message = message
        
        # 오류 이벤트 발행 (간소화된 방식)
        try:
            self.system_event_manager.record_metric(self.exchangename, "error_count")
        except Exception:
            pass  # 이벤트 발행 실패는 무시

    def log_warning(self, message: str) -> None:
        """경고 로깅 (거래소 이름 포함)"""
        logger.warning(f"{self.exchange_name_kr} {message}")

    def log_info(self, message: str) -> None:
        """정보 로깅 (거래소 이름 포함)"""
        logger.info(f"{self.exchange_name_kr} {message}")

    def log_debug(self, message: str) -> None:
        """디버그 로깅 (거래소 이름 포함)"""
        logger.debug(f"{self.exchange_name_kr} {message}")

    async def send_telegram_notification(self, event_type: str, message: str) -> None:
        """텔레그램 알림 전송 (쿨다운 적용)"""
        if event_type not in ["error", "connect", "disconnect", "reconnect"]:
            return
            
        current_time = time.time()
        
        # 쿨다운 체크 - 동일 이벤트 타입에 대해 일정 시간 내 중복 알림 방지
        last_time = self._last_notification_time.get(event_type, 0)
        if current_time - last_time < self._notification_cooldown:
            self.log_debug(f"알림 쿨다운 중: {event_type} (남은 시간: {self._notification_cooldown - (current_time - last_time):.1f}초)")
            return
            
        # 현재 시간 기록
        self._last_notification_time[event_type] = current_time
        
        try:
            # 이벤트 타입에 맞는 시스템 이벤트 발행
            if event_type == "error":
                self.publish_system_event_sync(
                    EVENT_TYPES["ERROR_EVENT"],
                    error_type="connection_error",
                    message=message,
                    severity="error"
                )
            elif event_type == "reconnect":
                self.publish_system_event_sync(
                    EVENT_TYPES["CONNECTION_STATUS"],
                    status="reconnecting",
                    message=message
                )
            
            # 이모지 선택
            emoji = "🔴"  # 기본값 (오류)
            if event_type == "connect":
                emoji = "🟢"  # 연결됨
            elif event_type == "reconnect":
                emoji = "🟠"  # 재연결
            
            # 거래소 이름에서 대괄호 제거
            exchange_name = self.exchange_name_kr.replace('[', '').replace(']', '')
            
            # 메시지 포맷팅
            formatted_message = f"{emoji} {exchange_name} 웹소켓: "
            
            # 메시지 내용 추가
            if isinstance(message, dict):
                if "message" in message:
                    formatted_message += message["message"]
                else:
                    formatted_message += str(message)
            else:
                formatted_message += message
            
            # 텔레그램으로 전송할 데이터 생성
            # ERROR, WARNING, RECONNECT 타입은 component 필드가 필요
            message_data = {"message": formatted_message}
            
            # 오류, 경고, 재연결 메시지 타입에는 component 필드 추가
            if event_type in ["error", "warning", "reconnect", "disconnect"]:
                message_data["component"] = exchange_name
                
            # 텔레그램으로 전송
            await send_telegram_message(
                self.settings, 
                self._get_message_type_for_event(event_type), 
                message_data
            )
            
        except Exception as e:
            self.log_error(f"텔레그램 알림 전송 실패: {event_type} - {str(e)}")

    def _get_message_type_for_event(self, event_type: str) -> str:
        """이벤트 타입에 따른 메시지 타입"""
        if event_type == "error":
            return "error"  # MessageType.ERROR 대신 문자열 사용
        elif event_type == "connect":
            return "info"
        elif event_type in ["disconnect", "reconnect"]:
            return "warning"
        else:
            return "info"   # MessageType.INFO 대신 문자열 사용