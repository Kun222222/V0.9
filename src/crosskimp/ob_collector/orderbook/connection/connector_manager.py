import asyncio
import time
from typing import Dict, Optional, Any, List, Callable

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, EXCHANGE_NAMES_KR, normalize_exchange_code, Exchange

# 로거 설정
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

class ConnectionManager:
    """
    연결 상태 관리자
    
    여러 거래소의 연결 상태를 모니터링하는 클래스입니다.
    역할:
    1. 거래소 연결 객체 관리
    2. 연결 상태 추적 (단일 진실 공급원)
    3. 연결 상태 모니터링
    4. 구독 상태 관리 (추가됨)
    5. 연결 시작 시간 및 업타임 트래킹 (추가됨)
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
        self.exchange_status = {}  # 거래소 코드 -> 연결 상태 (단일 진실 공급원)
        
        # 연결 통계
        self.connected_exchanges_count = 0
        self.total_exchanges_count = 0
        
        # 메트릭 관리자 (외부에서 주입)
        self.metric_manager = metrics_manager
        
        # 모니터링 태스크
        self.monitoring_task = None
        self.is_monitoring = False

        # 재연결 카운터 초기화
        self.reconnect_count = {}
        
        # 재연결 콜백 (obcollector_manager의 _connect_and_subscribe 함수를 저장)
        self.reconnect_callback = None
        
        # 재연결 락 시스템 - 거래소별 락으로 동시 재연결 방지
        self.reconnect_locks = {}
        
        # 연결 시작 시간 추적 (추가됨)
        self.connection_start_time = {}
        
        # 구독 상태 관리 (추가됨)
        self.subscription_status = {}
        
        # 심볼별 타임스탬프 (추가됨)
        self.symbol_timestamps = {}

        # 로깅 추가 - 객체 생성 완료
        self.logger.debug("ConnectionManager 객체 생성 완료")
        
    def register_connector(self, exchange_code: str, connector) -> None:
        """
        연결 객체 등록
        
        Args:
            exchange_code: 거래소 코드
            connector: 거래소 커넥터 객체
        """
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        self.connectors[exchange_code] = connector
        self.exchange_status[exchange_code] = False  # 초기 상태는 연결 안됨
        self.total_exchanges_count = len(self.connectors)
        
        # 구독 상태 초기화 (추가됨)
        if exchange_code not in self.subscription_status:
            self.subscription_status[exchange_code] = {
                "active": False,
                "symbol_count": 0,
                "symbols": [],
                "last_update": time.time()
            }
        
        # 거래소별 락 생성
        if exchange_code not in self.reconnect_locks:
            self.reconnect_locks[exchange_code] = asyncio.Lock()
        
        # 바이낸스 선물인 경우 로깅 생략
        if exchange_code == Exchange.BINANCE_FUTURE.value:
            pass  # 로깅 생략
        else:
            self.logger.info(f"{exchange_kr} 연결 객체가 등록되었습니다")

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
        
        # 로깅 추가
        self.logger.debug(f"ConnectionManager.update_exchange_status({exchange_code}, {is_connected}) 호출됨")
        
        # 이전 상태와 비교하여 변경된 경우에만 처리
        old_status = self.exchange_status.get(exchange_code, False)
        if old_status == is_connected:
            return  # 상태 변경 없음
        
        # 상태 업데이트
        self.exchange_status[exchange_code] = is_connected
        
        # 연결 시작 시간 관리 (추가됨)
        current_time = time.time()
        if is_connected and not old_status:
            # 새롭게 연결된 경우 시작 시간 기록
            self.connection_start_time[exchange_code] = current_time
        elif not is_connected and old_status:
            # 연결이 끊긴 경우 시작 시간 제거
            if exchange_code in self.connection_start_time:
                del self.connection_start_time[exchange_code]
        
        # 메트릭 업데이트 (메트릭 관리자가 있는 경우)
        if self.metric_manager:
            self.metric_manager.update_exchange_status(exchange_code, is_connected)
        
        # 연결된 거래소 수 업데이트
        if is_connected and not old_status:
            self.connected_exchanges_count += 1
            self.logger.info(f"🟢 {exchange_kr} 연결됨 (총 {self.connected_exchanges_count}/{self.total_exchanges_count})")
            # 연결 성공 시 재연결 카운터 리셋
            self.reconnect_count[exchange_code] = 0
        elif not is_connected and old_status:
            self.connected_exchanges_count = max(0, self.connected_exchanges_count - 1)
            self.logger.info(f"🔴 {exchange_kr} 연결 끊김 (총 {self.connected_exchanges_count}/{self.total_exchanges_count})")

    def get_connection_status(self, exchange_code: str = None) -> Dict[str, bool]:
        """
        거래소 연결 상태 조회
        
        Args:
            exchange_code: 거래소 코드 (None이면 모든 거래소 상태 반환)
            
        Returns:
            Dict[str, bool]: 거래소 코드 -> 연결 상태 딕셔너리
        """
        # 로깅 추가
        self.logger.debug(f"ConnectionManager.get_connection_status({exchange_code}) 호출됨")
        
        if exchange_code:
            return {exchange_code: self.is_exchange_connected(exchange_code)}
        else:
            return self.exchange_status.copy()  # 복사본 반환하여 원본 보호

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
    
    def get_connector(self, exchange_code: str) -> Optional[Any]:
        """
        거래소 연결 객체 가져오기
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            Any: 거래소 커넥터 객체 또는 None
        """
        return self.connectors.get(exchange_code)

    # 업타임 계산 메서드 (추가됨)
    def calculate_uptime(self, exchange_code: str) -> float:
        """
        거래소 연결 업타임 계산
        
        Args:
            exchange_code: 거래소 코드
            
        Returns:
            float: 업타임 (초 단위)
        """
        if not self.is_exchange_connected(exchange_code):
            return 0.0
            
        if exchange_code not in self.connection_start_time:
            return 0.0
            
        return time.time() - self.connection_start_time.get(exchange_code, time.time())
    
    def format_uptime(self, seconds: float) -> str:
        """
        업타임을 가독성 있는 형태로 포맷팅
        
        Args:
            seconds: 초 단위 시간
            
        Returns:
            str: 포맷팅된 업타임 문자열 (예: "2시간 30분")
        """
        if seconds < 60:
            return f"{int(seconds)}초"
        elif seconds < 3600:
            return f"{int(seconds / 60)}분 {int(seconds % 60)}초"
        else:
            hours = int(seconds / 3600)
            minutes = int((seconds % 3600) / 60)
            return f"{hours}시간 {minutes}분"
    
    # 구독 상태 관리 메서드 (추가됨)
    def update_subscription_status(self, exchange_code: str, active: bool, 
                                 symbols: List[str], symbol_count: Optional[int] = None):
        """
        거래소 구독 상태 업데이트
        
        Args:
            exchange_code: 거래소 코드
            active: 구독 활성화 여부
            symbols: 구독 중인 심볼 목록
            symbol_count: 구독 중인 심볼 수 (None이면 len(symbols)로 계산)
        """
        if exchange_code not in self.subscription_status:
            self.subscription_status[exchange_code] = {
                "active": False,
                "symbol_count": 0,
                "symbols": [],
                "last_update": time.time()
            }
        
        if symbol_count is None:
            symbol_count = len(symbols)
            
        self.subscription_status[exchange_code]["active"] = active
        self.subscription_status[exchange_code]["symbol_count"] = symbol_count
        self.subscription_status[exchange_code]["symbols"] = symbols
        self.subscription_status[exchange_code]["last_update"] = time.time()
        
        # 심볼별 타임스탬프 업데이트
        current_time = time.time()
        for symbol in symbols:
            key = f"{exchange_code}:{symbol}"
            self.symbol_timestamps[key] = current_time
        
        # 로깅
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        self.logger.debug(f"{exchange_kr} 구독 상태 업데이트: {symbol_count}개 심볼, 활성화={active}")
    
    def get_subscription_status(self, exchange_code: Optional[str] = None):
        """
        구독 상태 조회
        
        Args:
            exchange_code: 거래소 코드 (None이면 모든 거래소 반환)
            
        Returns:
            Dict: 구독 상태 정보
        """
        if exchange_code:
            return self.subscription_status.get(exchange_code, {
                "active": False,
                "symbol_count": 0,
                "symbols": [],
                "last_update": 0
            })
        else:
            return self.subscription_status.copy()
    
    def update_symbol_timestamp(self, exchange_code: str, symbol: str):
        """
        특정 심볼의 마지막 업데이트 시간 기록
        
        Args:
            exchange_code: 거래소 코드
            symbol: 심볼명
        """
        key = f"{exchange_code}:{symbol}"
        self.symbol_timestamps[key] = time.time()
    
    def get_symbol_timestamp(self, exchange_code: str, symbol: str) -> float:
        """
        특정 심볼의 마지막 업데이트 시간 조회
        
        Args:
            exchange_code: 거래소 코드
            symbol: 심볼명
            
        Returns:
            float: 마지막 업데이트 시간 (타임스탬프)
        """
        key = f"{exchange_code}:{symbol}"
        return self.symbol_timestamps.get(key, 0)
    
    # 메트릭 수집용 인터페이스 메서드 (추가됨)
    def get_connection_metrics(self) -> Dict[str, Any]:
        """
        연결 관련 메트릭 데이터 수집
        
        Returns:
            Dict: 각 거래소별 연결 상태 및 업타임 정보
        """
        result = {}
        
        for exchange, connected in self.exchange_status.items():
            # 업타임 계산
            uptime = self.calculate_uptime(exchange)
            
            result[exchange] = {
                "connected": connected,
                "uptime": uptime,
                "uptime_formatted": self.format_uptime(uptime) if uptime > 0 else "연결 안됨",
                "reconnect_count": self.reconnect_count.get(exchange, 0),
                "last_connected": self.connection_start_time.get(exchange, 0)
            }
            
        return result
    
    def get_subscription_metrics(self) -> Dict[str, Any]:
        """
        구독 관련 메트릭 데이터 수집
        
        Returns:
            Dict: 각 거래소별 구독 상태 정보
        """
        result = {}
        
        for exchange, status in self.subscription_status.items():
            # 심볼별 타임스탬프 정보 수집
            symbols_data = {}
            for symbol in status["symbols"]:
                key = f"{exchange}:{symbol}"
                symbols_data[symbol] = {
                    "last_update": self.symbol_timestamps.get(key, 0)
                }
            
            result[exchange] = {
                "active": status["active"],
                "total_symbols": status["symbol_count"],
                "symbols": symbols_data,
                "last_update": status["last_update"]
            }
            
        return result

    def start_monitoring(self, interval: int = 1):
        """
        연결 상태 모니터링 시작
        
        Args:
            interval: 점검 간격 (초) - 1초로 감소
        """
        if self.is_monitoring:
            return
            
        # 로깅 추가
        self.logger.debug(f"ConnectionManager.start_monitoring({interval}) 호출됨")
            
        self.is_monitoring = True
        self.monitoring_task = asyncio.create_task(self._monitor_connection_health(interval))
        self.logger.info(f"연결 상태 모니터링 시작 (점검 간격: {interval}초)")

    def stop_monitoring(self):
        """연결 상태 모니터링 중지"""
        if not self.is_monitoring:
            return
            
        # 로깅 추가
        self.logger.debug("ConnectionManager.stop_monitoring() 호출됨")
            
        self.is_monitoring = False
        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()
        self.logger.info("연결 상태 모니터링 중지")

    async def _monitor_connection_health(self, interval: int = 1):
        """
        주기적으로 모든 거래소 연결 상태를 확인하는 태스크
        
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
    
    def set_reconnect_callback(self, callback):
        """
        재연결 콜백 설정 - obcollector_manager의 _connect_and_subscribe 함수 연결
        
        Args:
            callback: 재연결 콜백 함수 (exchange_code를 인자로 받는 비동기 함수)
        """
        self.reconnect_callback = callback
        self.logger.debug("재연결 콜백이 설정되었습니다")
        
    async def _check_all_connections(self):
        """모든 거래소의 연결 상태를 확인"""
        for exchange_code, connector in self.connectors.items():
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
            
            try:
                # 커넥터의 연결 상태 확인 (is_connected 속성 사용)
                if hasattr(connector, 'is_connected'):
                    connector_state = connector.is_connected
                    saved_state = self.exchange_status.get(exchange_code, False)
                    
                    # 상태 불일치 확인 (커넥터 상태를 신뢰하지 않고 ConnectionManager 상태를 유지)
                    if connector_state != saved_state:
                        # 1. 커넥터 상태가 True, 저장된 상태가 False인 경우: ConnectionManager 상태 업데이트
                        if connector_state and not saved_state:
                            self.logger.info(f"{exchange_kr} 연결 감지됨: 커넥터={connector_state}, ConnectionManager={saved_state}")
                            self.update_exchange_status(exchange_code, True)  # 단일 진실 소스 업데이트
                        # 2. 커넥터 상태가 False, 저장된 상태가 True인 경우: 연결 끊김 감지
                        elif not connector_state and saved_state:
                            self.logger.warning(f"{exchange_kr} 연결 끊김 감지: 커넥터={connector_state}, ConnectionManager={saved_state}")
                            self.update_exchange_status(exchange_code, False)  # 연결 끊김으로 상태 업데이트
                            
                    # 연결이 끊어진 경우 재연결 시도
                    if not saved_state:
                        # 재연결 시도 횟수 확인 - 50회 미만인 경우만 재시도
                        if exchange_code not in self.reconnect_count or self.reconnect_count[exchange_code] < 50:
                            self.logger.info(f"{exchange_kr} 연결 끊김 상태 감지, 재연결 시도 예약...")
                            # 재연결 태스크 생성
                            asyncio.create_task(self.reconnect_exchange(exchange_code))
                    
            except Exception as e:
                self.logger.error(f"{exchange_kr} 연결 상태 확인 중 오류: {str(e)}")
            
    async def close_all_connections(self):
        """모든 거래소 연결 종료"""
        # 로깅 추가
        self.logger.debug("ConnectionManager.close_all_connections() 호출됨")
        
        success_count = 0
        error_count = 0
        
        for exchange_code, connector in list(self.connectors.items()):
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
            try:
                # 커넥터의 disconnect 메서드 호출
                if hasattr(connector, 'disconnect'):
                    await connector.disconnect()
                    self.logger.info(f"{exchange_kr} 연결이 종료되었습니다")
                    self.update_exchange_status(exchange_code, False)
                    success_count += 1
                else:
                    self.logger.warning(f"{exchange_kr} disconnect 메서드가 없습니다")
                    error_count += 1
            except Exception as e:
                self.logger.error(f"{exchange_kr} 연결 종료 중 오류: {str(e)}")
                error_count += 1
                
        # 모니터링 중지
        self.stop_monitoring()
        
        # 결과 요약 로깅
        self.logger.info(f"모든 거래소 연결 종료 완료 (성공: {success_count}, 실패: {error_count})")
        return success_count > 0

    # 중앙화된 재연결 메서드
    async def reconnect_exchange(self, exchange_code: str) -> bool:
        """
        거래소 재연결 시도 - 모든 재연결의 단일 진입점
        
        Args:
            exchange_code: 재연결할 거래소 코드
            
        Returns:
            bool: 재연결 성공 여부
        """
        # 거래소 한글명 가져오기
        exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
        
        # 해당 거래소에 대해 이미 재연결 진행 중인지 확인 (락 사용)
        if exchange_code not in self.reconnect_locks:
            self.reconnect_locks[exchange_code] = asyncio.Lock()
            
        # 비동기 락 획득 시도 (다른 재연결 시도와 충돌 방지)
        if self.reconnect_locks[exchange_code].locked():
            self.logger.debug(f"{exchange_kr} 이미 재연결 진행 중입니다. 중복 요청 무시")
            return False
            
        async with self.reconnect_locks[exchange_code]:
            # 재연결 횟수 증가
            self.reconnect_count[exchange_code] = self.reconnect_count.get(exchange_code, 0) + 1
            count = self.reconnect_count[exchange_code]
            
            # 너무 많은 재연결 시도 방지 (최대 50회까지 허용)
            if count > 50:
                self.logger.warning(f"{exchange_kr} 재연결 최대 시도 횟수 초과 (50회), 5분 후 다시 시도")
                await asyncio.sleep(300)  # 5분 후 다시 시도할 수 있도록 카운터 리셋
                self.reconnect_count[exchange_code] = 0
                return False
            
            # 항상 1초 후에 재연결 시도
            wait_time = 1.0
            
            self.logger.info(f"{exchange_kr} 재연결 {count}번째 시도 예정 ({wait_time}초 후)...")
            await asyncio.sleep(wait_time)
            
            # 1. 커넥터 객체 가져오기
            connector = self.get_connector(exchange_code)
            if not connector:
                self.logger.error(f"{exchange_kr} 재연결 실패: 커넥터를 찾을 수 없음")
                return False
            
            # 2. 기존 연결 정리
            try:
                if hasattr(connector, 'disconnect'):
                    await connector.disconnect()
                    self.logger.info(f"{exchange_kr} 기존 연결 정리 완료")
                else:
                    self.logger.warning(f"{exchange_kr} disconnect 메서드가 없음")
            except Exception as e:
                self.logger.error(f"{exchange_kr} 연결 정리 중 오류: {str(e)}")
            
            # 3. 재연결 콜백이 설정된 경우 사용 (obcollector_manager._connect_and_subscribe)
            if self.reconnect_callback:
                try:
                    self.logger.info(f"{exchange_kr} 재연결 콜백 실행...")
                    result = await self.reconnect_callback(exchange_code)
                    
                    if result:
                        self.logger.info(f"{exchange_kr} 재연결 성공 (콜백 사용)")
                        self.reconnect_count[exchange_code] = 0  # 성공 시 카운터 리셋
                        return True
                    else:
                        self.logger.error(f"{exchange_kr} 재연결 실패 (콜백 사용)")
                        return False
                except Exception as e:
                    self.logger.error(f"{exchange_kr} 재연결 콜백 실행 중 오류: {str(e)}")
                    return False
            
            # 4. 콜백이 없는 경우 기본 연결만 시도
            else:
                try:
                    # 직접 connect 호출
                    if hasattr(connector, 'connect'):
                        connect_result = await connector.connect()
                        if connect_result:
                            self.logger.info(f"{exchange_kr} 연결 성공 (구독은 수행되지 않음)")
                            self.update_exchange_status(exchange_code, True)
                            self.reconnect_count[exchange_code] = 0  # 성공 시 카운터 리셋
                            return True
                        else:
                            self.logger.error(f"{exchange_kr} 연결 실패")
                            return False
                    else:
                        self.logger.error(f"{exchange_kr} connect 메서드가 없음")
                        return False
                except Exception as e:
                    self.logger.error(f"{exchange_kr} 재연결 중 오류: {str(e)}")
                    return False 