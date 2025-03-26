import asyncio
import time
from typing import Dict, Optional, Any, List, Callable

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, EXCHANGE_NAMES_KR, normalize_exchange_code
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector

# 로거 설정
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

class ConnectionManager:
    """
    웹소켓 연결 관리자
    
    여러 거래소의 웹소켓 연결 상태를 관리하는 클래스입니다.
    ObCollector에서 분리된 연결 관리 기능을 담당합니다.
    """
    
    def __init__(self, metrics_manager=None):
        """
        초기화
        
        Args:
            metrics_manager: 메트릭 관리자 (선택 사항)
        """
        # 로거 설정
        self.logger = logger
        
        # 거래소 연결 객체 및 상태 딕셔너리
        self.connectors = {}  # 거래소 코드 -> 연결 객체
        self.exchange_status = {}  # 거래소 코드 -> 연결 상태
        
        # 연결 통계
        self.connected_exchanges_count = 0
        self.total_exchanges_count = 0
        
        # 메트릭 관리자 (외부에서 주입)
        self.metric_manager = metrics_manager
        
        # 모니터링 태스크
        self.monitoring_task = None
        self.is_monitoring = False
        
    def register_connector(self, exchange_code: str, connector: BaseWebsocketConnector) -> None:
        """
        연결 객체 등록
        
        Args:
            exchange_code: 거래소 코드
            connector: 웹소켓 연결 객체
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        self.connectors[exchange_code] = connector
        self.exchange_status[exchange_code] = False  # 초기 상태는 연결 안됨
        self.total_exchanges_count = len(self.connectors)
        self.logger.info(f"{exchange_kr} 연결 객체가 등록되었습니다")

    def create_connector(self, exchange_code: str, settings: Dict[str, Any], connector_class) -> Optional[BaseWebsocketConnector]:
        """
        거래소별 웹소켓 연결 객체 생성
        
        Args:
            exchange_code: 거래소 코드
            settings: 설정 딕셔너리
            connector_class: 연결 클래스
            
        Returns:
            BaseWebsocketConnector: 웹소켓 연결 객체 또는 None (실패 시)
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        
        try:
            # 연결 객체 생성 (콜백 함수 전달)
            self.logger.debug(f"{exchange_kr} 연결 객체 생성 시도 (클래스: {connector_class.__name__})")
            connector = connector_class(settings, exchange_code, on_status_change=self.on_connection_status_change)
            self.logger.info(f"{exchange_kr} 연결 객체 생성 성공")
            
            # 객체 등록
            self.register_connector(exchange_code, connector)
            
            return connector
            
        except Exception as e:
            self.logger.error(f"{exchange_kr} 연결 객체 생성 실패: {str(e)}", exc_info=True)
            if self.metric_manager:
                self.metric_manager.update_error_counter(exchange_code, "connector_creation_errors")
            return None
        
    async def on_connection_status_change(self, status: str, exchange_code: str, timestamp: float = None, initial_connection: bool = False, **kwargs):
        """
        연결 상태 변경 콜백 함수
        
        Args:
            status: 연결 상태 ("connected" 또는 "disconnected")
            exchange_code: 거래소 코드
            timestamp: 상태 변경 시간
            initial_connection: 최초 연결 여부
            **kwargs: 기타 추가 데이터
        """
        if not exchange_code:
            self.logger.warning(f"연결 상태 변경 콜백에 exchange_code가 없음: {kwargs}")
            return
        
        # 상태 업데이트
        self.update_exchange_status(exchange_code, status == "connected")
        
        # 타임스탬프 기본값 설정
        if timestamp is None:
            timestamp = time.time()
        
        if initial_connection and status == "connected":
            self.logger.info(f"🟢 [{exchange_code}] 최초 연결 성공!")

    def update_exchange_status(self, exchange_code: str, is_connected: bool):
        """
        거래소 연결 상태 업데이트
        
        모든 연결 상태 변경은 이 메서드를 통해 처리하여 일관성 유지
        
        Args:
            exchange_code: 거래소 코드
            is_connected: 연결 상태
        """
        # 한글 거래소명 가져오기
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        
        # 이전 상태와 비교하여 변경된 경우에만 처리
        old_status = self.exchange_status.get(exchange_code, False)
        if old_status == is_connected:
            return  # 상태 변경 없음
        
        # 상태 업데이트
        self.exchange_status[exchange_code] = is_connected
        
        # 메트릭 업데이트 (메트릭 관리자가 있는 경우)
        if self.metric_manager:
            self.metric_manager.update_exchange_status(exchange_code, is_connected)
        
        # 연결된 거래소 수 업데이트
        if is_connected and not old_status:
            self.connected_exchanges_count += 1
            self.logger.info(f"🟢 [{exchange_kr}] 연결됨 (총 {self.connected_exchanges_count}/{self.total_exchanges_count})")
        elif not is_connected and old_status:
            self.connected_exchanges_count = max(0, self.connected_exchanges_count - 1)
            self.logger.info(f"🔴 [{exchange_kr}] 연결 끊김 (총 {self.connected_exchanges_count}/{self.total_exchanges_count})")

    def is_exchange_connected(self, exchange_code: str) -> bool:
        """
        거래소 연결 상태 확인
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            bool: 연결 상태
        """
        return self.exchange_status.get(exchange_code, False)
    
    def get_connected_exchanges_count(self) -> int:
        """
        연결된 거래소 수 반환
        
        Returns:
            int: 연결된 거래소 수
        """
        return self.connected_exchanges_count
    
    def get_total_exchanges_count(self) -> int:
        """
        전체 거래소 수 반환
        
        Returns:
            int: 전체 거래소 수
        """
        return self.total_exchanges_count
    
    def get_connector(self, exchange_code: str) -> Optional[BaseWebsocketConnector]:
        """
        거래소 연결 객체 가져오기
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            BaseWebsocketConnector: 웹소켓 연결 객체 또는 None
        """
        return self.connectors.get(exchange_code)

    def start_monitoring(self, interval: int = 30):
        """
        연결 상태 모니터링 시작
        
        Args:
            interval: 점검 간격 (초)
        """
        if self.is_monitoring:
            return
            
        self.is_monitoring = True
        self.monitoring_task = asyncio.create_task(self._monitor_connection_health(interval))
        self.logger.info(f"연결 상태 모니터링 시작 (점검 간격: {interval}초)")

    def stop_monitoring(self):
        """연결 상태 모니터링 중지"""
        if not self.is_monitoring:
            return
            
        self.is_monitoring = False
        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()
        self.logger.info("연결 상태 모니터링 중지")

    async def _monitor_connection_health(self, interval: int = 30):
        """
        주기적으로 모든 거래소 연결 상태를 확인하고 필요시 재연결하는 태스크
        
        Args:
            interval: 점검 간격 (초)
        """
        self.logger.info(f"연결 상태 모니터링 태스크 시작 (점검 간격: {interval}초)")
        
        while self.is_monitoring:
            try:
                await self._check_all_connections()
                
                # 지정된 간격만큼 대기
                await asyncio.sleep(interval)
                
            except asyncio.CancelledError:
                self.logger.debug("연결 상태 모니터링 태스크 취소됨")
                break
            except Exception as e:
                self.logger.error(f"연결 상태 모니터링 중 오류: {str(e)}")
                await asyncio.sleep(interval)  # 오류 발생 시에도 계속 진행
                
        self.logger.debug("연결 상태 모니터링 종료")
    
    async def _check_all_connections(self):
        """모든 거래소의 연결 상태를 확인하고 필요시 재연결 시도"""
        for exchange_code, connector in self.connectors.items():
            # 실제 상태와 내부 상태가 일치하는지 확인
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
            
            try:
                # 연결 객체의 실제 상태 확인
                connector_state = connector.is_connected
                saved_state = self.exchange_status.get(exchange_code, False)
                
                # 웹소켓 객체 확인
                websocket_valid = False
                if connector_state:
                    ws = await connector.get_websocket()
                    websocket_valid = ws is not None
                
                # 상태 불일치 확인
                if connector_state != saved_state:
                    self.logger.warning(f"{exchange_kr} 연결 상태 불일치: 커넥터={connector_state}, ConnectionManager={saved_state}")
                    # 커넥터 상태로 통합 상태 업데이트
                    self.update_exchange_status(exchange_code, connector_state)
                
                # 커넥터는 연결됨으로 표시되었지만 실제 웹소켓이 없는 경우
                if connector_state and not websocket_valid:
                    self.logger.warning(f"{exchange_kr} 웹소켓 불일치: 상태는 연결됨이지만 웹소켓 객체가 없음")
                    # 재연결 시도
                    await self._reconnect_exchange(exchange_code, "websocket_inconsistent")
                    
            except Exception as e:
                self.logger.error(f"{exchange_kr} 연결 상태 확인 중 오류: {str(e)}")
                # 오류 발생 시 재연결 시도
                await self._reconnect_exchange(exchange_code, f"check_error: {str(e)}")
    
    async def _reconnect_exchange(self, exchange_code: str, reason: str) -> bool:
        """
        특정 거래소에 재연결 시도
        
        Args:
            exchange_code: 거래소 코드
            reason: 재연결 이유
            
        Returns:
            bool: 재연결 성공 여부
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        
        # 재연결 카운터 증가 (메트릭 관리자가 있는 경우)
        if self.metric_manager:
            self.metric_manager.increment_reconnect_counter(exchange_code)
        
        self.logger.info(f"🔄 [{exchange_kr}] 재연결 시도 (이유: {reason})")
        
        try:
            # 1. 연결 객체 확인
            connector = self.connectors.get(exchange_code)
            if not connector:
                self.logger.error(f"{exchange_kr} 연결 객체가 없습니다")
                return False
            
            # 2. 재연결 시도 (커넥터의 reconnect 메서드 호출)
            reconnect_success = await connector.reconnect()
            
            # 3. 재연결 실패 처리
            if not reconnect_success:
                self.logger.error(f"❌ [{exchange_kr}] 재연결 실패")
                if self.metric_manager:
                    self.metric_manager.update_error_counter(exchange_code, "reconnect_errors")
                return False
                
            # 4. 재연결 성공
            self.logger.info(f"✅ [{exchange_kr}] 재연결 성공")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ [{exchange_kr}] 재연결 처리 중 오류: {str(e)}", exc_info=True)
            if self.metric_manager:
                self.metric_manager.update_error_counter(exchange_code, "reconnect_errors")
            return False
            
    async def close_all_connections(self):
        """모든 거래소 연결 종료"""
        for exchange_code, connector in list(self.connectors.items()):
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
            try:
                await connector.disconnect()
                self.logger.info(f"{exchange_kr} 연결이 종료되었습니다")
                self.update_exchange_status(exchange_code, False)
            except Exception as e:
                self.logger.error(f"{exchange_kr} 연결 종료 중 오류: {str(e)}")
                
        # 모니터링 중지
        self.stop_monitoring()

    # 퍼블릭 메서드 추가
    async def reconnect_exchange(self, exchange_code: str, reason: str) -> bool:
        """
        특정 거래소에 재연결 시도 (퍼블릭 메서드)
        
        Args:
            exchange_code: 거래소 코드
            reason: 재연결 이유
            
        Returns:
            bool: 재연결 성공 여부
        """
        return await self._reconnect_exchange(exchange_code, reason) 