"""
오더북 프로세스 핸들러

ProcessComponent를 상속받아 오더북 프로세스의 시작/종료를 관리합니다.
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List, Set
from datetime import datetime, timedelta

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.handler.process_component import ProcessComponent
from crosskimp.common.config.common_constants import SystemComponent, EXCHANGE_NAMES_KR
from crosskimp.common.events.system_types import EventChannels, EventValues

# 메트릭 모듈 임포트 제거 (사용하지 않음)

# 오더북 수집기 임포트
from crosskimp.ob_collector.obcollector import OrderbookCollectorManager

# 로거 설정
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

class OrderbookProcess(ProcessComponent):
    """
    오더북 수집 프로세스
    
    오더북 수집을 위한 프로세스 컴포넌트입니다.
    ProcessComponent를 상속받아 프로세스 생명주기와 상태 관리를 담당합니다.
    기술적 작업은 OrderbookCollectorManager에 위임하고, 이 클래스는 상태 관리만 담당합니다.
    """

    def __init__(self, eventbus=None, config=None):
        """
        초기화
        
        Args:
            eventbus: 이벤트 버스 객체 (기본값: None, None일 경우 get_event_bus()로 가져옴)
            config: 설정 딕셔너리 (기본값: None, None일 경우 get_config()로 가져옴)
        """
        # 부모 클래스 초기화 (process_name과 eventbus 전달)
        super().__init__(process_name="ob_collector", event_bus=eventbus)
        
        # 설정 저장
        from crosskimp.common.config.app_config import get_config
        self.config = config if config is not None else get_config()
        
        # 데이터 수집을 위한 OrderbookCollectorManager 인스턴스 생성
        self.collector = OrderbookCollectorManager()
        
        # 메트릭 수집 관련 변수
        self.metric_interval = self.config.get("metrics", {}).get("interval", 20)  # 기본값 20초
        self.metric_collection_task = None  # 메트릭 수집 태스크 참조
        
        # 거래소 상태 추적용 변수
        self.exchange_uptimes = {}  # 각 거래소별 연결 시작 시간
        self.connected_exchanges = set()  # 현재 연결된 거래소
        self.all_connected = False  # 모든 거래소 연결 여부
        
        # 모니터링 태스크 참조
        self._monitoring_task = None

        # 텔레그램 주기적 알림 관련 설정
        self.telegram_status_interval = 10 * 60  # 10분(초 단위)
        self.last_telegram_status_time = 0  # 마지막 알림 시간
        
        # 이전 메트릭 저장 (변경점 계산용)
        self.previous_metrics = {}
        self.last_metric_time = None

    async def _do_start(self) -> bool:
        """
        오더북 프로세스 시작
        
        Returns:
            bool: 시작 성공 여부
        """
        try:
            self.logger.info("오더북 프로세스 시작 중...")
            self.logger.info("[디버깅] _do_start 메서드 시작")
            
            # 1. 초기화 필요성 확인 및 수행
            if not self.collector.initialization_complete:
                self.logger.info("오더북 수집기 초기화 시작")
                self.logger.info("[디버깅] collector.initialize 호출 전")
                
                # 초기화 수행
                init_success = await self.collector.initialize()
                self.logger.info(f"[디버깅] collector.initialize 결과: {init_success}")
                
                if not init_success:
                    self.logger.error("오더북 수집기 초기화 실패")
                    return False
                    
                self.logger.info("오더북 수집기 초기화 완료")
            else:
                self.logger.info("오더북 수집기가 이미 초기화되어 있음")
            
            # 2. 기술적 작업 시작
            self.logger.info("오더북 수집기 시작 요청")
            self.logger.info("[디버깅] collector.start 호출 전")
            success = await self.collector.start()
            self.logger.info(f"[디버깅] collector.start 결과: {success}")
            
            if not success:
                self.logger.error("오더북 수집기 시작 실패")
                return False
                
            # 3. 메트릭 수집 시작 - 변경: 직접 메트릭 수집 및 발행
            try:
                self.logger.info("메트릭 수집 시작")
                # ObcMetricManager 초기화 코드 제거
                
                # 메트릭 수집 태스크 시작
                self.metric_collection_task = asyncio.create_task(
                    self._collect_and_publish_metrics(self.metric_interval)
                )
                self.logger.info(f"메트릭 수집 태스크 시작됨 (간격: {self.metric_interval}초)")
                
            except Exception as e:
                self.logger.error(f"메트릭 수집 시작 실패: {str(e)}")
                # 메트릭 실패는 치명적이지 않으므로 계속 진행
            
            # 4. 상태 모니터링 태스크 시작 (백그라운드)
            self.logger.info("[디버깅] 상태 모니터링 태스크 시작 전")
            self._monitoring_task = asyncio.create_task(self._monitor_collector_status())
            self.logger.info("[디버깅] 상태 모니터링 태스크 생성 완료")
            
            self.logger.info("오더북 프로세스가 성공적으로 시작되었습니다. (거래소 연결은 백그라운드에서 계속됩니다)")
            self.logger.info("[디버깅] _do_start 메서드 종료, 성공")
            return True
            
        except Exception as e:
            self.logger.error(f"오더북 프로세스 시작 중 오류 발생: {str(e)}")
            self.logger.error("[디버깅] _do_start 메서드 종료, 예외 발생", exc_info=True)
            return False

    def _format_uptime(self, seconds: float) -> str:
        """거래소 업타임을 가독성 있는 형태로 포맷팅"""
        if seconds < 60:
            return f"{int(seconds)}초"
        elif seconds < 3600:
            return f"{int(seconds / 60)}분 {int(seconds % 60)}초"
        else:
            hours = int(seconds / 3600)
            minutes = int((seconds % 3600) / 60)
            return f"{hours}시간 {minutes}분"

    def _get_formatted_exchange_status(self, exchange_status: Dict[str, bool]) -> List[Dict[str, Any]]:
        """모든 거래소 상태 정보를 포맷팅"""
        exchanges_info = []
        
        # 지원하는 거래소 목록 (필터링된 거래소 목록이 있으면 사용)
        supported_exchanges = list(self.collector.filtered_symbols.keys() if hasattr(self.collector, 'filtered_symbols') else exchange_status.keys())
        
        # ConnectionManager에서 정보 가져오기
        connection_metrics = self.collector.connection_manager.get_connection_metrics()
        subscription_metrics = self.collector.connection_manager.get_subscription_metrics()
        
        for exchange in supported_exchanges:
            is_connected = exchange_status.get(exchange, False)
            
            # ConnectionManager에서 업타임 정보 가져오기
            uptime_seconds = 0
            uptime_formatted = "연결 안됨"
            
            if exchange in connection_metrics:
                uptime_seconds = connection_metrics[exchange].get("uptime", 0)
                uptime_formatted = connection_metrics[exchange].get("uptime_formatted", "연결 안됨")
                
            # 거래소 표시명 가져오기 (한글명 있으면 사용)
            exchange_name = EXCHANGE_NAMES_KR.get(exchange, exchange)
            
            # 구독 심볼 수 정보 가져오기
            subscribed_symbols_count = 0
            
            # ConnectionManager에서 구독 정보 가져오기
            if exchange in subscription_metrics:
                subscribed_symbols_count = subscription_metrics[exchange].get("total_symbols", 0)
            
            exchanges_info.append({
                "name": exchange,
                "display_name": exchange_name,
                "connected": is_connected,
                "uptime_seconds": uptime_seconds,
                "uptime_formatted": uptime_formatted,
                "subscribed_symbols_count": subscribed_symbols_count
            })
            
        return exchanges_info

    async def _monitor_collector_status(self):
        """오더북 수집기의 상태를 모니터링하고 이벤트 발행"""
        self.logger.info("오더북 수집기 상태 모니터링 태스크 시작")
        
        # 초기 상태 저장
        prev_exchange_status = {}
        check_count = 0
        all_connected_event_sent = False
        current_time = time.time()
        
        # 텔레그램 노티파이어 가져오기
        from crosskimp.telegram_bot.notify import get_telegram_notifier
        from crosskimp.telegram_bot.notify_formatter import NotificationLevel
        telegram_notifier = get_telegram_notifier()
        
        # 상태 모니터링 루프
        while True:
            try:
                # 프로세스가 시작 중이 아니거나 이미 실행 중인 경우 (상태가 변경된 경우) 모니터링 중단
                if not self.is_starting and not self.is_running:
                    self.logger.info("프로세스가 시작 중 또는 실행 중 상태가 아니므로 모니터링 종료")
                    break
                
                check_count += 1
                current_time = time.time()
                
                # 거래소 연결 상태 가져오기 - ConnectionManager를 단일 진실 원천으로 사용
                exchange_status = self.collector.get_exchange_status()
                
                # 거래소 연결 상태에 따라 self.exchange_uptimes 업데이트
                for exchange, is_connected in exchange_status.items():
                    # 연결된 거래소의 경우 시작 시간 기록
                    if is_connected:
                        if exchange not in self.exchange_uptimes:
                            self.logger.debug(f"거래소 '{exchange}' 첫 연결 감지, 업타임 기록 시작")
                            self.exchange_uptimes[exchange] = current_time
                        elif exchange not in self.connected_exchanges:
                            self.logger.debug(f"거래소 '{exchange}' 재연결 감지, 업타임 갱신")
                            self.exchange_uptimes[exchange] = current_time
                        self.connected_exchanges.add(exchange)
                    else:
                        # 연결 끊긴 거래소는 연결된 거래소 목록에서 제거
                        if exchange in self.connected_exchanges:
                            self.logger.debug(f"거래소 '{exchange}' 연결 끊김 감지")
                            self.connected_exchanges.discard(exchange)
                            if exchange in self.exchange_uptimes:
                                self.exchange_uptimes.pop(exchange)
                
                # 전체 거래소 상태 포맷팅
                exchanges_info = self._get_formatted_exchange_status(exchange_status)
                
                # 연결된 거래소 수와 총 거래소 수
                connected_count = sum(1 for ex in exchanges_info if ex["connected"])
                total_count = len(exchanges_info)
                
                # 모든 거래소 연결 여부 확인
                prev_all_connected = self.all_connected
                self.all_connected = (connected_count == total_count) and (total_count > 0)
                
                # 주기적 로깅 (5회마다)
                if check_count % 5 == 0:
                    self.logger.debug(f"오더북 수집기 상태: {connected_count}/{total_count} 거래소 연결됨")
                
                # 거래소 상태 변경 감지 - 더 효율적인 비교
                status_changed = False
                
                # 상태 변경 감지 방법 개선
                for exchange, status in exchange_status.items():
                    prev_status = prev_exchange_status.get(exchange, False)
                    if prev_status != status:
                        status_changed = True
                        break
                
                # 모든 거래소 처음 연결된 경우도 상태 변경으로 간주
                if self.all_connected and not prev_all_connected:
                    status_changed = True
                
                # 상태 변경이 있을 때만 이벤트 발행
                if status_changed:
                    # 이벤트 데이터 준비
                    event_data = {
                        "process_name": self.process_name,
                        "timestamp": current_time,
                        "details": {
                            "exchanges": exchanges_info,
                            "connected_count": connected_count,
                            "total_count": total_count,
                            "all_connected": self.all_connected
                        }
                    }
                    
                    # 모든 거래소 연결된 경우 이벤트 발행 (RUNNING 이벤트 제거)
                    if self.all_connected and not all_connected_event_sent:
                        all_connected_event_sent = True
                        
                        # 프로세스 상태 변경만 유지 (RUNNING 이벤트 발행하지 않음)
                        await self._publish_status(EventValues.PROCESS_RUNNING)
                        self.logger.info("모든 거래소 연결 완료, 프로세스 상태를 RUNNING으로 변경")
                    
                    # 개별 거래소 상태 변경 이벤트 발행
                    for exchange_code, is_connected in exchange_status.items():
                        prev_state = prev_exchange_status.get(exchange_code, False)
                        
                        # 연결 상태가 변경된 경우에만 이벤트 발행
                        if prev_state != is_connected:
                            exchange_event_data = {
                                "timestamp": current_time,
                                "exchange": exchange_code,
                                "status": is_connected,
                                "exchanges_info": exchanges_info,
                                "connected_count": connected_count,
                                "total_count": total_count,
                                "all_connected": self.all_connected  # 모든 거래소 연결 상태 추가
                            }
                            
                            # 거래소 연결 끊김
                            if prev_state and not is_connected:
                                exchange_event_data["reason"] = f"{EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)} 연결이 끊겼습니다"
                                await self.event_bus.publish(EventChannels.Component.ObCollector.CONNECTION_LOST, exchange_event_data)
                                self.logger.info(f"거래소 '{exchange_code}' 연결 끊김 이벤트 발행됨")
                            
                            # 거래소 연결 성공
                            elif not prev_state and is_connected:
                                await self.event_bus.publish(EventChannels.Component.ObCollector.EXCHANGE_STATUS, exchange_event_data)
                                self.logger.info(f"거래소 '{exchange_code}' 연결 성공 이벤트 발행됨")
                
                # 현재 상태를 이전 상태로 저장
                prev_exchange_status = exchange_status.copy()
                
                # 2초마다 확인 (1초에서 2초로 변경하여 부하 감소)
                await asyncio.sleep(2)
                
            except asyncio.CancelledError:
                self.logger.info("오더북 수집기 상태 모니터링 태스크가 취소되었습니다.")
                break
            except Exception as e:
                self.logger.error(f"상태 모니터링 중 오류: {str(e)}")
                await asyncio.sleep(5)  # 오류 발생 시 더 긴 간격으로 재시도
                
        self.logger.info("오더북 수집기 상태 모니터링 태스크 종료")
        
    async def _do_stop(self) -> bool:
        """
        오더북 프로세스 중지
        
        Returns:
            bool: 중지 성공 여부
        """
        try:
            self.logger.info("오더북 프로세스 중지 중...")
            
            # 1. 메트릭 수집 태스크 중지
            if self.metric_collection_task:
                try:
                    self.metric_collection_task.cancel()
                    self.logger.info("메트릭 수집 태스크 취소됨")
                except Exception as e:
                    self.logger.warning(f"메트릭 수집 태스크 취소 중 오류: {str(e)}")
                self.metric_collection_task = None
            
            # 2. 수집기 중지
            stop_success = await self.collector.stop()
            if not stop_success:
                self.logger.error("오더북 수집기 중지 실패")
                return False
            
            self.logger.info("오더북 프로세스가 성공적으로 중지되었습니다.")
            return True
            
        except Exception as e:
            self.logger.error(f"오더북 프로세스 중지 중 오류 발생: {str(e)}")
            return False

    # 메트릭 수집 및 발행 관련 메서드들 - 단순화
    async def _collect_and_publish_metrics(self, interval: int = 20):
        """
        메트릭 데이터 주기적 수집 및 이벤트 발행
        
        Args:
            interval: 수집 간격(초)
        """
        try:
            self.logger.info(f"🚀 메트릭 수집 태스크 실행 시작 (간격: {interval}초)")
            self.last_metric_time = time.time()
            
            while self.is_running or self.is_starting:
                try:
                    current_time = time.time()
                    self.logger.debug(f"메트릭 데이터 수집 시작")
                    
                    # 연결 상태 가져오기
                    connection_metrics = self.collector.connection_manager.get_connection_metrics()
                    
                    # 메시지 통계 가져오기 (data_manager에서 직접 가져옴)
                    stats = self.collector.data_manager.get_statistics()
                    
                    # 메시지 메트릭 형식으로 변환
                    message_metrics = {}
                    for exchange, data in stats["exchanges"].items():
                        message_metrics[exchange] = {
                            "total_count": data["raw_messages"],
                            "rate": data["interval_rate"],
                            "error_count": data.get("errors", 0)
                        }
                    
                    # 메트릭 발행
                    await self._publish_connection_metrics(connection_metrics)
                    await self._publish_message_metrics(message_metrics)
                    
                    # 시스템 메트릭 생성 및 발행
                    system_metrics = {
                        "status": "process/running" if self.all_connected else "process/starting",
                        "uptime": time.time() - self.start_time if hasattr(self, 'start_time') else 0,
                        "connected_exchanges": len(self.connected_exchanges),
                        "total_exchanges": len(self.collector.filtered_symbols) if hasattr(self.collector, 'filtered_symbols') else 0
                    }
                    await self._publish_system_metrics(system_metrics)
                    
                    # 구독 메트릭 발행
                    subscription_metrics = self.collector.connection_manager.get_subscription_metrics()
                    await self._publish_subscription_metrics(subscription_metrics)
                    
                    # 로깅
                    self.logger.debug(f"메트릭 데이터 수집 및 발행 완료")
                    
                    # 통계 요약 로깅
                    total_raw_messages = sum(data["raw_messages"] for data in stats["exchanges"].values())
                    total_interval_count = sum(data["interval_count"] for data in stats["exchanges"].values())
                    total_rate = sum(data["interval_rate"] for data in stats["exchanges"].values())
                    
                    self.logger.info(f"📊 [메트릭 합계] {'전체 거래소':15} | 총: {total_raw_messages:8d}건 | "
                                 f"수신: {total_interval_count:6d}건/{stats['interval_seconds']:.1f}초 | "
                                 f"속도: {total_rate:.2f}건/초")
                    
                    # 지정된 간격만큼 대기
                    self.logger.debug(f"다음 메트릭 수집까지 {interval}초 대기")
                    await asyncio.sleep(interval)
                    
                except asyncio.CancelledError:
                    self.logger.debug("메트릭 수집 태스크가 취소되었습니다.")
                    break
                except Exception as e:
                    self.logger.error(f"메트릭 수집 중 오류 발생: {str(e)}", exc_info=True)
                    await asyncio.sleep(10)  # 오류 발생 시 10초 후 재시도
        
        except asyncio.CancelledError:
            self.logger.debug("메트릭 수집 태스크가 취소되었습니다.")
        except Exception as e:
            self.logger.error(f"메트릭 수집 태스크 실행 중 오류 발생: {str(e)}", exc_info=True)
        finally:
            self.logger.debug("메트릭 수집 태스크 종료")

    async def _publish_connection_metrics(self, connection_metrics):
        """연결 메트릭 발행"""
        if not connection_metrics:
            return
            
        event_data = {
            "process_id": self.process_name,
            "timestamp": time.time(),
            "metric_type": "connection",
            "metrics": connection_metrics
        }
        
        await self.event_bus.publish(
            event_path=f"{EventChannels.Component.ObCollector.METRICS}/connection",
            data=event_data
        )
            
    async def _publish_message_metrics(self, message_metrics):
        """메시지 메트릭 발행"""
        if not message_metrics:
            return
            
        event_data = {
            "process_id": self.process_name,
            "timestamp": time.time(),
            "metric_type": "message",
            "metrics": message_metrics
        }
        
        await self.event_bus.publish(
            event_path=f"{EventChannels.Component.ObCollector.METRICS}/message",
            data=event_data
        )
    
    async def _publish_error_metrics(self, error_metrics):
        """오류 메트릭 발행"""
        if not error_metrics:
            return
            
        event_data = {
            "process_id": self.process_name,
            "timestamp": time.time(),
            "metric_type": "error",
            "metrics": error_metrics
        }
        
        await self.event_bus.publish(
            event_path=f"{EventChannels.Component.ObCollector.METRICS}/error",
            data=event_data
        )
        
    async def _publish_system_metrics(self, system_metrics):
        """시스템 메트릭 발행"""
        if not system_metrics:
            return
            
        # 하위 컴포넌트 상태 제거 (불필요함)
        if "components" in system_metrics:
            del system_metrics["components"]
            
        event_data = {
            "process_id": self.process_name,
            "timestamp": time.time(),
            "metric_type": "system",
            "metrics": system_metrics
        }
        
        await self.event_bus.publish(
            event_path=f"{EventChannels.Component.ObCollector.METRICS}/system",
            data=event_data
        )
        
    async def _publish_subscription_metrics(self, subscription_metrics):
        """구독 메트릭 발행"""
        if not subscription_metrics:
            return
            
        # 각 거래소에 대해 심볼 상세 정보 제거하고 총 수만 유지
        simplified_metrics = {}
        for exchange, data in subscription_metrics.items():
            simplified_metrics[exchange] = {
                "active": data.get("active", False),
                "total_symbols": data.get("total_symbols", 0)
            }
            
        event_data = {
            "process_id": self.process_name,
            "timestamp": time.time(),
            "metric_type": "subscription",
            "metrics": simplified_metrics
        }
        
        await self.event_bus.publish(
            event_path=f"{EventChannels.Component.ObCollector.METRICS}/subscription",
            data=event_data
        )

# 싱글톤 인스턴스
_instance = None

def get_orderbook_process(eventbus, config=None) -> OrderbookProcess:
    """
    오더북 프로세스 컴포넌트 인스턴스를 반환합니다.
    
    싱글톤 패턴을 사용하여 하나의 인스턴스만 생성합니다.
    
    Args:
        eventbus: 이벤트 버스 객체
        config: 설정 딕셔너리 (기본값: None)
        
    Returns:
        OrderbookProcess: 오더북 프로세스 컴포넌트 인스턴스
    """
    global _instance
    if _instance is None:
        _instance = OrderbookProcess(eventbus=eventbus, config=config)
    return _instance

async def initialize_orderbook_process(eventbus, config=None):
    """
    오더북 프로세스 컴포넌트를 초기화합니다.
    
    Args:
        eventbus: 이벤트 버스 객체
        config: 설정 딕셔너리 (기본값: None)
        
    Returns:
        OrderbookProcess: 초기화된 오더북 프로세스 인스턴스
    """
    process = get_orderbook_process(eventbus=eventbus, config=config)
    await process.register_event_handlers()
    return process