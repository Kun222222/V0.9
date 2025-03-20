"""
시스템 이벤트 관리자 모듈

이 모듈은 이벤트 버스를 통해 시스템 이벤트를 관리하고 메트릭을 유지합니다.
주요 목적은 거래소별 메트릭을 수집, 저장하고 요약 정보를 제공하는 것입니다.
"""

import asyncio
import time
from typing import Dict, List, Any, Optional
from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.util.event_bus import EventBus, EVENT_TYPES
from crosskimp.config.constants_v3 import EXCHANGE_NAMES_KR

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class SystemEventManager:
    """
    시스템 이벤트 관리 클래스
    
    이벤트 버스를 통해 모든 시스템 이벤트를 수신하고 관리합니다.
    핵심 메트릭을 저장하고 주기적으로 요약 정보를 출력합니다.
    """
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """싱글톤 인스턴스 반환"""
        if cls._instance is None:
            cls._instance = SystemEventManager()
        return cls._instance
    
    def __init__(self):
        """초기화"""
        if SystemEventManager._instance is not None:
            raise Exception("SystemEventManager는 싱글톤입니다. get_instance()를 사용하세요.")
        
        # 메트릭 저장소
        self.metrics = {}  # exchange_code -> metrics
        
        # 연결 상태 저장소
        self.connection_status = {}  # exchange_code -> status
        
        # 구독 상태 저장소
        self.subscriptions = {}  # exchange_code -> subscription_status
        
        # 이벤트 버스 초기화
        self.event_bus = EventBus.get_instance()
        
        # 메시지 카운터 저장소
        self.message_counters = {}
        
        # 이벤트 핸들러 등록
        self.event_bus.subscribe("system_event", self.handle_event)
        
        # 최근 오류 저장소 (거래소별 최대 5개)
        self.errors = {}  # exchange_code -> [(timestamp, error_msg)]
        
        # 현재 거래소 설정
        self.current_exchange = None
        
        # 메트릭 요약 로그 주기적 출력을 위한 변수
        self.last_summary_log_time = time.time()
        self.summary_log_interval = 1.0  # 1초마다 요약 로그 출력
        self.previous_message_counts = {}  # 이전 카운트 저장용
        
        # 메트릭 요약 로그 태스크 시작
        self._start_summary_log_task()

        logger.info("[SystemEventManager] 초기화 완료")
    
    def _start_summary_log_task(self):
        """주기적 메트릭 요약 로그 태스크 시작"""
        try:
            asyncio.create_task(self._summary_log_loop())
        except RuntimeError:
            logger.debug("이벤트 루프가 실행 중이 아니므로 요약 로그 태스크를 시작할 수 없습니다.")
    
    async def _summary_log_loop(self):
        """메트릭 요약 로그 출력 루프"""
        try:
            while True:
                await asyncio.sleep(self.summary_log_interval)
                self._log_metrics_summary()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"메트릭 요약 로그 루프 오류: {e}")
    
    def _log_metrics_summary(self):
        """각 거래소별 메트릭 요약 로그 출력"""
        current_time = time.time()
        elapsed = current_time - self.last_summary_log_time
        
        if elapsed < self.summary_log_interval:
            return  # 아직 시간이 충분히 지나지 않음
        
        # 각 거래소별 메트릭 정보 출력
        for exchange_code in sorted(self.metrics.keys()):
            # 메시지 카운트 정보
            current_count = self.metrics[exchange_code].get("message_count", 0)
            previous_count = self.previous_message_counts.get(exchange_code, 0)
            
            # 1. 총 메시지 수
            total_count = current_count
            
            # 2. 이번 수신 갯수 (이번 1초 동안)
            current_diff = current_count - previous_count
            
            # 3. 평균 수신 갯수 계산
            # 초기화 시간 또는 첫 메시지 시간을 기준으로 계산
            start_time = self.metrics[exchange_code].get("first_message_time")
            if start_time is None and current_count > 0:
                # 첫 메시지 수신 시간 기록
                start_time = current_time
                self.metrics[exchange_code]["first_message_time"] = start_time
            
            total_elapsed = current_time - (start_time or current_time)
            avg_per_second = 0
            if total_elapsed > 0 and current_count > 0:
                avg_per_second = current_count / total_elapsed
            
            # 4. 이번 수신 속도 (1초 동안)
            current_rate = self.metrics[exchange_code].get("message_rate", 0)
            if current_rate == 0 and elapsed > 0:
                current_rate = current_diff / elapsed
            
            # 한글 거래소명 변환
            exchange_name_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code.upper())
            
            # 로그 출력 (INFO 레벨로 출력하여 항상 보이도록 함)
            logger.info(f"[메트릭] {exchange_name_kr:15} | 총: {total_count:8d}건 | 수신: {current_diff:6d}건/1초 | 평균: {avg_per_second:.2f}건/초 | 속도: {current_rate:.2f}건/초")
            
            # 이전 카운트 업데이트
            self.previous_message_counts[exchange_code] = current_count
        
        # 마지막 로그 시간 업데이트
        self.last_summary_log_time = current_time
    
    def initialize_exchange(self, exchange_code: str) -> None:
        """
        거래소 초기화 - 거래소 관련 메트릭과 상태 정보 초기화
        
        Args:
            exchange_code: 거래소 코드
        """
        if exchange_code not in self.connection_status:
            self.connection_status[exchange_code] = "disconnected"
            
        if exchange_code not in self.metrics:
            self.metrics[exchange_code] = {
                "message_count": 0,
                "error_count": 0,
                "processing_time": 0,
                "last_message_time": 0,
                "last_error_time": 0,
                "last_error_message": "",
                "first_message_time": None,  # 첫 메시지 시간 추가
                "message_rate": 0  # 현재 메시지 속도
            }
            
        if exchange_code not in self.errors:
            self.errors[exchange_code] = []
            
        if exchange_code not in self.subscriptions:
            self.subscriptions[exchange_code] = {
                "symbols": [],
                "status": "unsubscribed"
            }
    
    def increment_message_count(self, exchange_code: str) -> None:
        """
        메시지 카운트 증가
        
        Args:
            exchange_code: 거래소 코드
        """
        # 거래소 초기화 확인
        if exchange_code not in self.metrics:
            self.initialize_exchange(exchange_code)
            
        # 현재 시간
        current_time = time.time()
        
        # 메시지 카운트 증가
        message_count = self.metrics[exchange_code].get("message_count", 0)
        self.metrics[exchange_code]["message_count"] = message_count + 1
        
        # 첫 메시지 시간 설정 (첫 메시지인 경우에만)
        if message_count == 0:
            self.metrics[exchange_code]["first_message_time"] = current_time
            
        # 마지막 메시지 시간 업데이트
        self.metrics[exchange_code]["last_message_time"] = current_time
    
    async def handle_event(self, event) -> None:
        """
        이벤트 처리 - 이벤트 버스로부터 받은 이벤트를 처리하고 적절한 메트릭을 업데이트
        
        Args:
            event: 이벤트 객체
        """
        try:
            event_type = event.get("event_type")
            exchange_code = event.get("exchange_code")
            timestamp = event.get("timestamp", time.time())
            data = event.get("data", {})
            
            # 거래소 초기화 확인
            if exchange_code and exchange_code not in self.connection_status:
                self.initialize_exchange(exchange_code)
            
            # 이벤트 타입별 처리
            if event_type == EVENT_TYPES["CONNECTION_STATUS"]:
                await self._handle_connection_status(exchange_code, data, timestamp)
            elif event_type == EVENT_TYPES["METRIC_UPDATE"]:
                await self._handle_metric_update(exchange_code, data, timestamp)
            elif event_type == EVENT_TYPES["ERROR_EVENT"]:
                await self._handle_error_event(exchange_code, data, timestamp)
            elif event_type == EVENT_TYPES["SUBSCRIPTION_STATUS"]:
                await self._handle_subscription_status(exchange_code, data, timestamp)
        except Exception as e:
            logger.error(f"이벤트 처리 중 오류: {str(e)}")
    
    async def _handle_connection_status(self, exchange_code, data, timestamp) -> None:
        """연결 상태 이벤트 처리 - 메트릭 업데이트"""
        status = data.get("status", "disconnected")
        self.connection_status[exchange_code] = status
        
        if status == "connected":
            self.metrics[exchange_code]["connection_time"] = timestamp
            
        logger.debug(f"{exchange_code} 연결 상태 변경: {status}")
    
    async def _handle_metric_update(self, exchange_code, data, timestamp) -> None:
        """메트릭 업데이트 이벤트 처리 - 메트릭 저장"""
        metric_name = data.get("metric_name")
        value = data.get("value")
        
        if not metric_name or value is None:
            return
            
        self.metrics[exchange_code][metric_name] = value
        
        if metric_name == "message_count":
            self.metrics[exchange_code]["last_message_time"] = timestamp
        
        if metric_name == "processing_time":
            current = self.metrics[exchange_code].get("processing_time", 0)
            self.metrics[exchange_code]["processing_time"] = (current * 0.8) + (value * 0.2)
    
    async def _handle_error_event(self, exchange_code, data, timestamp) -> None:
        """오류 이벤트 처리 - 오류 정보 저장"""
        error_type = data.get("error_type", "unknown")
        message = data.get("message", "")
        severity = data.get("severity", "error")
        
        self.metrics[exchange_code]["error_count"] = self.metrics[exchange_code].get("error_count", 0) + 1
        self.metrics[exchange_code]["last_error_time"] = timestamp
        self.metrics[exchange_code]["last_error_message"] = message
        
        self.errors[exchange_code].append({
            "timestamp": timestamp,
            "type": error_type,
            "message": message,
            "severity": severity
        })
        
        # 최대 5개만 유지
        if len(self.errors[exchange_code]) > 5:
            self.errors[exchange_code] = self.errors[exchange_code][-5:]
        
        # 심각한 오류만 로깅
        if severity in ["error", "critical"]:
            logger.error(f"{exchange_code} {error_type}: {message}")
    
    async def _handle_subscription_status(self, exchange_code, data, timestamp) -> None:
        """구독 상태 이벤트 처리 - 구독 정보 저장"""
        status = data.get("status", "unknown")
        symbols = data.get("symbols", [])
        
        self.subscriptions[exchange_code] = {
            "status": status,
            "symbols": symbols,
            "timestamp": timestamp
        }
        
        logger.debug(f"{exchange_code} 구독 상태: {status}, 심볼: {len(symbols)}개")
    
    def get_status(self, exchange_code: str = None) -> dict:
        """
        시스템 상태 정보 조회
        
        Args:
            exchange_code: 거래소 코드 (None이면 모든 거래소)
            
        Returns:
            dict: 상태 정보
        """
        if exchange_code:
            if exchange_code not in self.metrics:
                return {}
                
            result = self.metrics[exchange_code].copy()
            
            # 배치 필드는 제거
            for key in list(result.keys()):
                if key.startswith("_"):
                    del result[key]
                    
            # 상태 이모지 추가
            connection_status = result.get("status", "disconnected")
            result["emoji"] = self._get_status_emoji(connection_status)
            
            # 최근 오류 추가
            if exchange_code in self.errors:
                result["recent_errors"] = self.errors[exchange_code][-3:]  # 최근 3개 오류만
                
            return result
        else:
            # 모든 거래소 상태 반환
            result = {}
            for ex_code in self.metrics:
                result[ex_code] = self.get_status(ex_code)
            return result
    
    def _get_status_emoji(self, status: str) -> str:
        """상태에 따른 이모지 반환"""
        emojis = {
            "connected": "🟢",
            "disconnected": "🔴",
            "reconnecting": "🟠",
            "error": "⚠️",
            "unknown": "⚪"
        }
        return emojis.get(status, "⚪")
    
    def get_connection_status(self, exchange_code: str) -> str:
        """
        연결 상태 가져오기
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            str: 연결 상태
        """
        return self.connection_status.get(exchange_code, "unknown")
    
    def get_metrics(self, exchange_code: str) -> dict:
        """
        메트릭 정보 가져오기
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            dict: 메트릭 정보
        """
        return self.metrics.get(exchange_code, {})
    
    def get_errors(self, exchange_code: str, limit: int = None) -> List:
        """
        오류 목록 가져오기
        
        Args:
            exchange_code: 거래소 코드
            limit: 최대 개수 (None이면 모두 반환)
            
        Returns:
            List: 오류 목록
        """
        errors = self.errors.get(exchange_code, [])
        if limit:
            return errors[-limit:]
        return errors
    
    def _update_metric(self, exchange_code: str, metric_name: str, count: int = 1) -> None:
        """
        메트릭 업데이트 (내부 메서드)
        
        Args:
            exchange_code: 거래소 코드
            metric_name: 메트릭 이름
            count: 증가량 (기본값 1)
        """
        # 거래소 초기화 확인
        if exchange_code not in self.metrics:
            self.initialize_exchange(exchange_code)
            
        # 메트릭 업데이트
        current_value = self.metrics[exchange_code].get(metric_name, 0)
        self.metrics[exchange_code][metric_name] = current_value + count
        
        # 타임스탬프 업데이트
        current_time = time.time()
        self.metrics[exchange_code]["last_update_time"] = current_time
        
        # 특수 메트릭인 경우 추가 처리
        if metric_name == "message_count":
            self.metrics[exchange_code]["last_message_time"] = current_time
    
    def record_metric(self, exchange_code: str, metric_name: str, **data) -> None:
        """
        메트릭 기록 메서드
        
        모든 메트릭을 기록하는 중앙 메서드입니다.
        
        Args:
            exchange_code: 거래소 코드
            metric_name: 메트릭 이름
            **data: 추가 데이터
        """
        # 거래소 초기화 확인
        if exchange_code not in self.metrics:
            self.initialize_exchange(exchange_code)
            
        # 값 추출 (기본값은 1)
        value = 1
            
        # 특수 메트릭 처리
        if metric_name == "processing_time" or "processing_time" in data:
            value = data.get("processing_time", 0)
            metric_name = "processing_time"
        elif metric_name == "data_size" or "byte_size" in data:
            value = data.get("byte_size", 0)
            metric_name = "data_size"
            
        # 메트릭 업데이트
        self._update_metric(exchange_code, metric_name, value)
    
    async def publish_system_event(self, event_type: str, exchange_code: str = None, **data) -> None:
        """
        시스템 이벤트 발행 (비동기)
        
        Args:
            event_type: 이벤트 타입 (EVENT_TYPES 사용)
            exchange_code: 거래소 코드 (없으면 현재 설정된 거래소 사용)
            **data: 이벤트 데이터
        """
        try:
            # 거래소 코드가 없으면 현재 설정된 거래소 사용
            if not exchange_code:
                exchange_code = self.current_exchange
                
            if not exchange_code:
                logger.warning("시스템 이벤트 발행 실패: 거래소 코드가 없습니다.")
                return
                
            # 거래소가 초기화되지 않았으면 초기화
            if exchange_code not in self.metrics:
                self.initialize_exchange(exchange_code)
            
            # 타임스탬프 처리 (data에 timestamp가 있으면 해당 값 사용, 없으면 현재 시간)
            timestamp = time.time()
            if "timestamp" in data:
                timestamp = data.pop("timestamp")
            
            # 이벤트 발행 - 수정: event_type 중복 제거
            await self.event_bus.publish(
                event_type,  # 첫 번째 매개변수로만 event_type 전달
                exchange_code=exchange_code,
                timestamp=timestamp,
                data=data
            )
            
        except Exception as e:
            logger.error(f"시스템 이벤트 발행 실패: {str(e)}")
    
    async def publish_system_event_sync(self, event_type: str, exchange_code: str = None, **data) -> None:
        """
        시스템 이벤트 발행 (동기식)
        
        과거에는 비동기 컨텍스트 외부에서 사용할 수 있었지만, 
        이제는 비동기 함수로 변경되었습니다. 이름은 호환성을 위해 유지합니다.
        
        Args:
            event_type: 이벤트 타입
            exchange_code: 거래소 코드 (없으면 현재 설정된 거래소 사용)
            **data: 이벤트 데이터
        """
        try:
            # 거래소 코드가 없으면 현재 설정된 거래소 사용
            if not exchange_code:
                exchange_code = self.current_exchange
                
            if not exchange_code:
                logger.warning("시스템 이벤트 발행 실패: 거래소 코드가 없습니다.")
                return
                
            # 거래소가 초기화되지 않았으면 초기화
            if exchange_code not in self.metrics:
                self.initialize_exchange(exchange_code)
            
            # 타임스탬프 처리 (data에 timestamp가 있으면 해당 값 사용, 없으면 현재 시간)
            timestamp = time.time()
            if "timestamp" in data:
                timestamp = data.pop("timestamp")
            
            # 이벤트 데이터 준비
            event_data = {
                "exchange_code": exchange_code,
                "timestamp": timestamp,
                **data  # 나머지 데이터 포함
            }
            
            # 이벤트 버스를 통해 비동기로 이벤트 발행 (이전에는 동기식이었음)
            await self.event_bus.publish_sync(event_type, **event_data)
            
        except Exception as e:
            logger.error(f"시스템 이벤트 발행 실패: {str(e)}")
            
    def set_current_exchange(self, exchange_code: str) -> None:
        """
        현재 컨텍스트의 거래소 코드 설정
        
        Args:
            exchange_code: 거래소 코드
        """
        self.current_exchange = exchange_code
        
        # 거래소 초기화 확인
        if exchange_code and exchange_code not in self.metrics:
            self.initialize_exchange(exchange_code)
    
    def clear_current_exchange(self) -> None:
        """현재 컨텍스트의 거래소 코드 초기화"""
        self.current_exchange = None 