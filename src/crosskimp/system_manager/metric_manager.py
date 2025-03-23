"""
중앙 메트릭 관리자 모듈

이 모듈은 시스템 전체의 메트릭을 중앙에서 관리하는 싱글톤 클래스를 제공합니다.
EventHandler와 함께 작동하여 모든 거래소의 메트릭을 통합 관리합니다.
"""

import time
import asyncio
import threading
from typing import Dict, List, Any, Optional, Set, Union, Tuple
from collections import defaultdict
import logging

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.constants_v3 import EXCHANGE_NAMES_KR, LOG_SYSTEM

# 로거 설정
logger = get_unified_logger()

# 표준 메트릭 키 정의
class MetricKeys:
    # 메시지 관련 메트릭
    MESSAGE_COUNT = "message_count"          # 메시지 수신 총 개수
    SNAPSHOT_COUNT = "snapshot_count"        # 스냅샷 메시지 수
    DELTA_COUNT = "delta_count"              # 델타 메시지 수
    ERROR_COUNT = "error_count"              # 오류 수
    MESSAGE_RATE = "message_rate"            # 초당 메시지 처리 속도
    
    # 시간 관련 메트릭
    PROCESSING_TIME = "processing_time"      # 메시지 처리 평균 시간(ms)
    LAST_ERROR_TIME = "last_error_time"      # 마지막 오류 시간
    FIRST_MESSAGE_TIME = "first_message_time"# 첫 메시지 시간
    LAST_MESSAGE_TIME = "last_message_time"  # 마지막 메시지 시간
    LAST_STATE_CHANGE = "last_state_change"  # 마지막 상태 변경 시간
    
    # 상태 관련 메트릭
    COLLECTOR_STATUS = "collector_status"    # 수집기 상태
    CONNECTION_STATUS = "connection_status"  # 연결 상태
    STATUS_TYPE = "status_type"              # 상태 유형
    ERROR_REASON = "error_reason"            # 오류 원인

# 메트릭 유형 정의
METRIC_TYPES = {
    # 메시지 관련 메트릭
    "message_count": "counter",       # 메시지 수신 총 개수
    "snapshot_count": "counter",      # 스냅샷 메시지 수
    "delta_count": "counter",         # 델타 메시지 수 
    "error_count": "counter",         # 오류 수
    "message_rate": "gauge",          # 초당 메시지 처리 속도
    
    # 시간 관련 메트릭
    "processing_time": "gauge",       # 메시지 처리 평균 시간(ms)
    "last_error_time": "timestamp",   # 마지막 오류 시간
    "first_message_time": "timestamp",# 첫 메시지 시간
    "last_state_change": "timestamp", # 마지막 상태 변경 시간
    
    # 시스템 상태 관련 메트릭 (문자열)
    "collector_status": "string",     # 수집기 상태 (initializing, starting, running 등)
    "connection_status": "string",    # 연결 상태 (connected, disconnected, connecting 등)
    "status_type": "string",          # 상태 유형 (NORMAL, WARNING, ERROR 등)
    "environment": "string",          # 환경 (development, production)
    "python_version": "string",       # 파이썬 버전
    "error_reason": "string",         # 오류 원인
    
    # 기타 메트릭
    "connection_check_needed": "event", # 연결 확인 필요 이벤트
    "is_shutting_down": "counter",    # 종료 중 플래그 (0/1)
}

class MetricManager:
    """
    중앙 메트릭 관리자 클래스
    
    모든 거래소의 메트릭을 중앙에서 관리하는 싱글톤 클래스입니다.
    기존 EventHandler와 함께 작동하도록 설계되었습니다.
    
    특징:
    1. 거래소별, 메트릭 유형별 데이터 관리
    2. 주기적인 메트릭 요약 생성
    3. 메트릭 기반 알림 기능
    4. 이벤트 버스와 통합
    """
    
    _instance = None
    _lock = threading.RLock()
    
    @classmethod
    def get_instance(cls) -> 'MetricManager':
        """싱글톤 인스턴스 반환"""
        with cls._lock:
            if cls._instance is None:
                cls._instance = MetricManager()
            return cls._instance
    
    def __init__(self):
        """초기화 - 싱글톤 강제"""
        if MetricManager._instance is not None:
            raise RuntimeError("싱글톤 클래스입니다. get_instance()를 사용하세요.")
        
        # 메트릭 저장소
        self.metrics = defaultdict(lambda: defaultdict(dict))  # {exchange_code -> {metric_name -> value}}
        
        # 오류 저장소
        self.errors = defaultdict(list)  # {exchange_code -> [error1, error2, ...]}
        
        # 마지막 요약 시간
        self.last_summary_time = time.time()
        self.summary_interval = 10  # 10초마다 요약 로그 출력
        
        # 이전 메시지 카운트 (계산용)
        self.previous_counts = {}  # {exchange_code -> previous_message_count}
        
        # 요약 로그 태스크
        self.summary_task = None
        self.stop_event = asyncio.Event()
        
        # 메트릭 변경 알림 설정
        self.alert_thresholds = {
            "error_count": 10,         # 10개 이상 오류 발생 시 알림
            "message_rate": 0.1,       # 메시지 속도가 0.1 이하로 떨어지면 알림
            "processing_time": 1000,   # 처리 시간이 1000ms 이상이면 알림
        }
        
        # 알림 쿨다운 (중복 알림 방지)
        self.last_alert_time = {}  # {exchange_code -> {metric_name -> timestamp}}
        self.alert_cooldown = 300  # 5분 동안 동일 메트릭 알림 안함
        
        logger.info(f"{LOG_SYSTEM} 중앙 메트릭 관리자가 초기화되었습니다.")
    
    async def start(self):
        """메트릭 관리자 시작 - 요약 태스크 시작"""
        if self.summary_task is None or self.summary_task.done():
            self.stop_event.clear()
            self.summary_task = asyncio.create_task(self._summary_loop())
            logger.info(f"{LOG_SYSTEM} 메트릭 요약 태스크가 시작되었습니다.")
            
            # 새 이벤트 시스템 사용으로 변경
            from crosskimp.common.events import get_component_event_bus, Component, StatusEventTypes
            event_bus = get_component_event_bus(Component.SERVER)
            
            # METRIC_UPDATE 이벤트 구독
            asyncio.create_task(event_bus.subscribe(StatusEventTypes.METRIC_UPDATE, self._handle_metric_update))
    
    async def stop(self):
        """메트릭 관리자 중지 - 요약 태스크 중지"""
        if self.summary_task and not self.summary_task.done():
            self.stop_event.set()
            try:
                await asyncio.wait_for(self.summary_task, timeout=1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self.summary_task.cancel()
                logger.debug(f"{LOG_SYSTEM} 메트릭 요약 태스크가 취소되었습니다.")
            
            # 이벤트 구독 해제 - 새 이벤트 시스템 사용
            from crosskimp.common.events import get_component_event_bus, Component, StatusEventTypes
            event_bus = get_component_event_bus(Component.SERVER)
            asyncio.create_task(event_bus.unsubscribe(StatusEventTypes.METRIC_UPDATE, self._handle_metric_update))
    
    def get_metric(self, exchange_code: str, metric_name: str, default=None):
        """
        특정 거래소의 특정 메트릭 조회
        
        Args:
            exchange_code: 거래소 코드
            metric_name: 메트릭 이름
            default: 기본값 (메트릭이 없을 경우 반환)
            
        Returns:
            메트릭 값 또는 기본값
        """
        try:
            # 입력 파라미터 정규화
            exchange_code = exchange_code.lower()
            
            # 메트릭 조회
            return self.metrics[exchange_code].get(metric_name, default)
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 메트릭 조회 중 오류: {str(e)}")
            return default

    def update_metric(self, exchange_code: str, metric_name: str, value: Any, **kwargs) -> None:
        """
        메트릭 값 업데이트
        
        모든 메트릭 업데이트는 이 메서드를 통해 수행되어야 합니다.
        
        Args:
            exchange_code: 거래소 코드
            metric_name: 메트릭 이름
            value: 메트릭 값
            **kwargs: 추가 데이터
                - op: 연산 유형 (update, increment, decrement, set 등)
                - 그 외 메트릭별 추가 데이터
        """
        try:
            # 입력 파라미터 타입 확인
            if not isinstance(exchange_code, str):
                logger.warning(f"{LOG_SYSTEM} 거래소 코드가 문자열이 아님: {exchange_code}, 타입: {type(exchange_code)}")
                exchange_code = str(exchange_code)
                
            if not isinstance(metric_name, str):
                logger.warning(f"{LOG_SYSTEM} 메트릭 이름이 문자열이 아님: {metric_name}, 타입: {type(metric_name)}")
                metric_name = str(metric_name)
                
            # 거래소 코드 정규화
            exchange_code = exchange_code.lower()
            
            # 메트릭 유형 확인 (없으면 'counter' 기본값)
            metric_type = METRIC_TYPES.get(metric_name, 'counter')
            
            # 기존 값 가져오기
            current_value = self.metrics[exchange_code].get(metric_name, 0)
            
            # 연산 유형 확인 (kwargs에서 추출)
            operation = kwargs.get("op", "update")  # 기본값은 update
            
            # 메트릭 유형에 따른 업데이트 로직
            if metric_type == 'counter':
                # 카운터 유형: 기존 값에 추가 또는 설정된 연산 수행
                try:
                    # 타입 체크 및 변환
                    if not isinstance(value, (int, float)):
                        try:
                            value = float(value)
                            if value.is_integer():
                                value = int(value)
                        except (ValueError, TypeError):
                            logger.warning(f"{LOG_SYSTEM} 카운터 메트릭 {metric_name}에 숫자가 아닌 값 수신: {value}")
                            value = 1 if operation == "increment" else 0
                    
                    # 현재 값도 숫자인지 확인
                    if not isinstance(current_value, (int, float)):
                        try:
                            current_value = float(current_value)
                            if current_value.is_integer():
                                current_value = int(current_value)
                        except (ValueError, TypeError):
                            logger.warning(f"{LOG_SYSTEM} 저장된 메트릭 {metric_name}이 숫자가 아님: {current_value}")
                            current_value = 0
                    
                    # 연산 유형에 따른 처리
                    if operation == "increment":
                        new_value = current_value + value
                    elif operation == "decrement":
                        new_value = current_value - value
                    elif operation == "set" or operation == "update":
                        new_value = value
                    else:
                        # 기본적으로 카운터는 누적
                        new_value = current_value + value
                        
                except Exception as e:
                    logger.error(f"{LOG_SYSTEM} 카운터 메트릭 {metric_name} 업데이트 중 오류: {str(e)}")
                    new_value = current_value
                
            elif metric_type == 'string':
                # 문자열 유형: 값을 그대로 저장 (항상 대체)
                new_value = str(value)
                
            elif metric_type == 'gauge':
                # 게이지 유형: 이동 평균 또는 단순 대체
                try:
                    if not isinstance(value, (int, float)):
                        try:
                            value = float(value)
                        except (ValueError, TypeError):
                            logger.warning(f"{LOG_SYSTEM} 게이지 메트릭 {metric_name}에 숫자가 아닌 값 수신: {value}")
                            return
                    
                    # 이동 평균 적용 여부 확인 (processing_time만 이동 평균 적용)
                    if metric_name == "processing_time" and current_value:
                        if not isinstance(current_value, (int, float)):
                            try:
                                current_value = float(current_value)
                            except (ValueError, TypeError):
                                current_value = 0
                                
                        new_value = (current_value * 0.8) + (value * 0.2)
                    else:
                        new_value = value  # 단순 대체
                except Exception as e:
                    logger.error(f"{LOG_SYSTEM} 게이지 메트릭 {metric_name} 업데이트 중 오류: {str(e)}")
                    new_value = current_value
                    
            elif metric_type == 'timestamp':
                # 타임스탬프 유형: 단순 대체
                new_value = value
                
                # 첫 메시지 시간은 특별 처리 (새로운 값이 작을 때만 업데이트)
                if metric_name == "first_message_time" and current_value:
                    if value > current_value:
                        new_value = current_value  # 기존 값 유지
            else:
                # 알 수 없는 유형: 단순 대체
                new_value = value
            
            # 메트릭 저장
            self.metrics[exchange_code][metric_name] = new_value
            
            # 메트릭 변경 알림 체크
            self._check_alert_condition(exchange_code, metric_name, new_value)
            
            # 중요 메트릭 변경 이벤트 발행 (이벤트 버스 존재 확인 후)
            if metric_name in ['connection_status', 'error_count', 'collector_status']:
                # 이벤트 버스가 임포트되지 않은 경우 (순환 참조 방지)
                # 필요할 때만 임포트하여 이벤트 발행
                try:
                    from crosskimp.common.events import get_component_event_bus, Component, StatusEventTypes
                    event_bus = get_component_event_bus(Component.SERVER)
                    
                    # 이벤트 데이터 준비
                    event_data = {
                        "exchange_code": exchange_code,
                        "metric_name": metric_name,
                        "value": new_value,
                        "timestamp": time.time()
                    }
                    
                    # kwargs에서 추가 정보 추출
                    for k, v in kwargs.items():
                        if k != "op":  # 연산자는 제외
                            event_data[k] = v
                    
                    # 이벤트 발행 (비동기 처리)
                    asyncio.create_task(event_bus.publish(
                        StatusEventTypes.METRIC_UPDATE,
                        event_data
                    ))
                except ImportError:
                    # 개발 환경 초기화 과정에서 임포트가 안 되는 경우 무시
                    pass
                except Exception as e:
                    logger.error(f"{LOG_SYSTEM} 메트릭 이벤트 발행 중 오류: {str(e)}")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 메트릭 {metric_name} 업데이트 중 예외 발생: {str(e)}")
    
    def _check_alert_condition(self, exchange_code: str, metric_name: str, value: Any) -> None:
        """
        메트릭 값이 알림 임계값을 초과하는지 확인
        
        Args:
            exchange_code: 거래소 코드
            metric_name: 메트릭 이름
            value: 메트릭 값
        """
        # 알림 대상 메트릭이 아니면 무시
        if metric_name not in self.alert_thresholds:
            return
            
        # 쿨다운 체크 (최근에 알림을 보냈으면 무시)
        current_time = time.time()
        key = f"{exchange_code}:{metric_name}"
        last_time = self.last_alert_time.get(key, 0)
        
        if current_time - last_time < self.alert_cooldown:
            return  # 쿨다운 중
        
        # 임계값 체크
        threshold = self.alert_thresholds[metric_name]
        
        # 메트릭별 다른 비교 로직
        alert_needed = False
        alert_message = ""
        
        if metric_name == "error_count" and value >= threshold:
            # 누적 오류가 임계값 이상
            alert_needed = True
            alert_message = f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 오류 {value}건 발생 (임계값: {threshold}건)"
            
        elif metric_name == "message_rate" and value <= threshold and self.metrics[exchange_code].get('message_count', 0) > 0:
            # 메시지 수신 속도가 임계값 이하
            alert_needed = True
            alert_message = f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 메시지 수신 속도 저하: {value:.2f}/초 (임계값: {threshold}/초)"
            
        elif metric_name == "processing_time" and value >= threshold:
            # 처리 시간이 임계값 이상
            alert_needed = True
            alert_message = f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 메시지 처리 시간 증가: {value:.2f}ms (임계값: {threshold}ms)"
        
        # 알림 전송
        if alert_needed:
            # 알림 전송 시간 업데이트
            self.last_alert_time[key] = current_time
            
            # 로그 및 텔레그램 알림
            logger.warning(f"{LOG_SYSTEM} {alert_message}")
            
            # 비동기 컨텍스트가 아니므로 직접 호출 불가
            # 대신 로깅만 수행하고 실제 텔레그램 전송은 별도 태스크로
            asyncio.create_task(self._send_alert("metric_alert", alert_message))
    
    async def _send_alert(self, alert_type: str, message: str) -> None:
        """
        알림 전송
        
        Args:
            alert_type: 알림 유형 (warning, error, info 등)
            message: 알림 메시지
        """
        try:
            # 텔레그램 알림 대신 로깅으로 대체
            if alert_type == "warning":
                logger.warning(f"[메트릭 알림] {message}")
            elif alert_type == "error":
                logger.error(f"[메트릭 알림] {message}")
            else:
                logger.info(f"[메트릭 알림] {message}")
        except Exception as e:
            logger.error(f"메트릭 알림 전송 중 오류: {str(e)}")
    
    async def _summary_loop(self) -> None:
        """메트릭 요약 루프 - 주기적으로 요약 정보 출력"""
        try:
            logger.info(f"{LOG_SYSTEM} 메트릭 요약 태스크 시작됨")
            
            while not self.stop_event.is_set():
                # 현재 시간 확인
                current_time = time.time()
                
                # 마지막 요약 이후 충분한 시간이 지났는지 확인
                if current_time - self.last_summary_time >= self.summary_interval:
                    # 요약 정보 생성 및 출력
                    await self._generate_summary()
                    
                    # 마지막 요약 시간 업데이트
                    self.last_summary_time = current_time
                
                # 다음 체크까지 대기
                await asyncio.sleep(1)  # 1초마다 체크
                
        except asyncio.CancelledError:
            logger.info(f"{LOG_SYSTEM} 메트릭 요약 태스크가 취소되었습니다.")
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 메트릭 요약 루프 오류: {str(e)}")
    
    async def _generate_summary(self) -> None:
        """메트릭 요약 정보 생성 및 출력"""
        try:
            # 전체 합계 계산을 위한 변수 초기화
            total_message_count = 0
            total_interval_count = 0
            total_current_rate = 0
            active_exchanges = 0  # 활성 거래소 수
            
            # 거래소별 요약 정보 출력
            for exchange_code, metrics in self.metrics.items():
                # 기본 정보 없으면 스킵
                if 'message_count' not in metrics:
                    continue
                
                # 거래소 이름 (한글)
                exchange_name = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
                
                # 총 메시지 수
                total_count = metrics.get('message_count', 0)
                if total_count == 0:
                    continue  # 메시지가 없으면 보고 안함
                
                # 이전 카운트 가져오기 (없으면 0)
                previous_count = self.previous_counts.get(exchange_code, 0)
                
                # 구간 메시지 수 (요약 간격 동안)
                interval_count = total_count - previous_count
                
                # 현재 수신 속도 (요약 간격 동안)
                current_time = time.time()
                summary_elapsed = current_time - self.last_summary_time
                current_rate = interval_count / summary_elapsed if summary_elapsed > 0 else 0
                
                # 메시지 레이트 갱신
                self.metrics[exchange_code]['message_rate'] = current_rate
                
                # 요약 로그 출력
                logger.info(
                    f"[메트릭 요약] {exchange_name:15} | "
                    f"총: {int(total_count):8d}건 | "
                    f"수신: {int(interval_count):6d}건/{self.summary_interval}초 | "
                    f"속도: {current_rate:.2f}건/초"
                )
                
                # 이전 카운트 업데이트
                self.previous_counts[exchange_code] = total_count
                
                # 전체 합계 누적
                total_message_count += total_count
                total_interval_count += interval_count
                total_current_rate += current_rate
                active_exchanges += 1
            
            # 활성 거래소가 있는 경우에만 전체 합계 출력
            if active_exchanges > 0:
                logger.info(
                    f"[메트릭 합계] {'전체 거래소':15} | "
                    f"총: {int(total_message_count):8d}건 | "
                    f"수신: {int(total_interval_count):6d}건/{self.summary_interval}초 | "
                    f"속도: {total_current_rate:.2f}건/초"
                )
                
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 메트릭 요약 생성 중 오류: {str(e)}")
    
    def get_metrics(self, exchange_code: Optional[str] = None) -> Dict:
        """
        메트릭 정보 가져오기
        
        Args:
            exchange_code: 거래소 코드 (None이면 모든 거래소)
            
        Returns:
            Dict: 메트릭 정보
        """
        if exchange_code:
            # 특정 거래소 메트릭만 반환
            exchange_code = exchange_code.lower()
            return dict(self.metrics.get(exchange_code, {}))
        else:
            # 모든 거래소 메트릭 반환
            return {code: dict(metrics) for code, metrics in self.metrics.items()}
    
    def get_errors(self, exchange_code: Optional[str] = None, limit: int = None) -> Dict:
        """
        오류 정보 가져오기
        
        Args:
            exchange_code: 거래소 코드 (None이면 모든 거래소)
            limit: 각 거래소별 최대 오류 수 (None이면 제한 없음)
            
        Returns:
            Dict: 오류 정보
        """
        if exchange_code:
            # 특정 거래소 오류만 반환
            exchange_code = exchange_code.lower()
            errors = self.errors.get(exchange_code, [])
            if limit:
                errors = errors[-limit:]
            return errors
        else:
            # 모든 거래소 오류 반환
            result = {}
            for code, errors in self.errors.items():
                if limit:
                    result[code] = errors[-limit:]
                else:
                    result[code] = errors
            return result
    
    def get_exchange_status(self, exchange_code: str) -> Dict[str, Any]:
        """
        거래소 상태 정보 가져오기
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            Dict: 상태 정보
        """
        exchange_code = exchange_code.lower()
        metrics = self.metrics.get(exchange_code, {})
        
        # 메시지 카운트 및 에러 카운트
        msg_count = metrics.get('message_count', 0)
        err_count = metrics.get('error_count', 0)
        
        # 메시지 수신 속도
        msg_rate = metrics.get('message_rate', 0)
        
        # 마지막 오류 시간
        last_err_time = metrics.get('last_error_time', 0)
        
        # 연결 상태로 receiving 판단 (connection_status 메트릭 사용)
        connection_status = metrics.get('connection_status', '')
        receiving = connection_status == 'connected'
        
        # 마지막 오류 메시지
        last_error = metrics.get('last_error_message', '')
        if last_error and len(last_error) > 50:
            last_error = last_error[:47] + "..."
        
        # 거래소 이름 (한글)
        exchange_name = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        
        # 결과 반환
        return {
            "exchange": exchange_name,
            "exchange_code": exchange_code,
            "receiving": receiving,
            "message_count": msg_count,
            "error_count": err_count,
            "messages_per_sec": msg_rate,
            "last_error_time": last_err_time,
            "last_error_message": last_error
        }
    
    def get_all_exchange_status(self) -> Dict[str, Dict[str, Any]]:
        """
        모든 거래소 상태 정보 가져오기
        
        Returns:
            Dict: 거래소별 상태 정보
        """
        result = {}
        for exchange_code in self.metrics.keys():
            result[exchange_code] = self.get_exchange_status(exchange_code)
        return result
    
    def reset_metrics(self, exchange_code: Optional[str] = None) -> None:
        """
        메트릭 초기화
        
        Args:
            exchange_code: 거래소 코드 (None이면 모든 거래소)
        """
        if exchange_code:
            # 특정 거래소 메트릭만 초기화
            exchange_code = exchange_code.lower()
            self.metrics[exchange_code] = {}
            self.errors[exchange_code] = []
            if exchange_code in self.previous_counts:
                del self.previous_counts[exchange_code]
        else:
            # 모든 거래소 메트릭 초기화
            self.metrics = defaultdict(lambda: defaultdict(dict))
            self.errors = defaultdict(list)
            self.previous_counts = {}
        
        logger.info(f"{LOG_SYSTEM} 메트릭 초기화: {'모든 거래소' if not exchange_code else EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)}")
    
    def initialize_metrics(self, exchange_code: str) -> None:
        """
        거래소별 메트릭 초기화
        
        Args:
            exchange_code: 거래소 코드
        """
        # 모든 스레드에서 동시 접근 방지
        with self._lock:
            # 이미 해당 거래소 메트릭이 초기화되어 있으면 무시
            if exchange_code in self.metrics and 'message_count' in self.metrics[exchange_code]:
                return
                
            # 기본 메트릭 초기화
            self.metrics[exchange_code] = {
                "message_count": 0,
                "error_count": 0,
                "processing_time": 0,
                "last_error_time": 0,
                "last_error_message": "",
                "first_message_time": None,
                "message_rate": 0
            }
            
            # 오류 목록 초기화
            self.errors[exchange_code] = []
            
            # 이전 카운트 초기화
            self.previous_counts[exchange_code] = 0
            
            logger.debug(f"{LOG_SYSTEM} {EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 메트릭 초기화됨")

    async def _handle_metric_update(self, event_data):
        """
        METRIC_UPDATE 이벤트 처리
        
        모든 메트릭 업데이트 이벤트는 MetricManager에서 중앙 처리합니다.
        
        Args:
            event_data: 이벤트 데이터 (exchange_code, metric_name, value 포함)
        """
        try:
            # 필수 데이터 추출
            exchange_code = event_data.get("exchange_code")
            metric_name = event_data.get("metric_name")
            value = event_data.get("value")
            
            if not exchange_code or not metric_name or value is None:
                logger.warning(f"{LOG_SYSTEM} 메트릭 업데이트 이벤트 데이터 누락: {event_data}")
                return
            
            # 추가 데이터(kwargs) 추출
            kwargs = {k: v for k, v in event_data.items() 
                    if k not in ["exchange_code", "metric_name", "value", "timestamp"]}
            
            # 메트릭 업데이트
            self.update_metric(exchange_code, metric_name, value, **kwargs)
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 메트릭 업데이트 이벤트 처리 중 오류: {str(e)}")

# 편의를 위한 글로벌 함수
def get_metric_manager() -> MetricManager:
    """중앙 메트릭 관리자 인스턴스 반환"""
    return MetricManager.get_instance() 