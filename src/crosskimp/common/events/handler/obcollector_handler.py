"""
오더북 프로세스 핸들러

ProcessComponent를 상속받아 오더북 프로세스의 시작/종료를 관리합니다.
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.domains.process_component import ProcessComponent
from crosskimp.common.config.common_constants import SystemComponent, ProcessStatus
from crosskimp.common.events.system_types import EventCategory, SystemEventType, ObCollectorEventType

# 오더북 수집기 임포트
from crosskimp.ob_collector.ob_collector import ObCollector

# 로거 설정
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

class OrderbookProcess(ProcessComponent):
    """
    오더북 프로세스 컴포넌트
    
    오더북 데이터 수집 프로세스의 시작/종료를 관리합니다.
    """
    
    def __init__(self, process_name: str = "ob_collector"):
        """
        오더북 프로세스 초기화
        
        Args:
            process_name: 프로세스 이름 (기본값: "ob_collector")
        """
        super().__init__(process_name)
        # self.is_running = False  # 부모 클래스의 self.status로 대체
        self.collector = ObCollector()  # 오더북 수집기 인스턴스 생성
        
        # 메트릭 수집 설정
        self.metric_reporter_task = None
        self.metric_intervals = {
            "connection": 30,  # 연결 상태 30초마다
            "message": 10,     # 메시지 통계 10초마다
            "subscription": 60, # 구독 상태 60초마다
            "error": 30,       # 오류 통계 30초마다
            "system": 60       # 시스템 상태 60초마다
        }
        self.last_metric_times = {k: 0 for k in self.metric_intervals}
        
    async def _do_start(self) -> bool:
        """
        오더북 프로세스 실제 시작 작업
        
        오더북 수집기의 start 메서드를 호출하여 오더북 수집을 시작합니다.
        모든 상태 관리는 부모 클래스가 처리합니다.
        
        Returns:
            bool: 시작 성공 여부
        """
        try:
            self.logger.info("오더북 프로세스 시작 중...")
            self.logger.debug("OrderbookProcess._do_start() 메서드 호출됨 - ObCollector.start_collection() 호출 전")
            
            # 오더북 수집기를 사용하여 시작
            success = await self.collector.start_collection()
            
            self.logger.debug(f"ObCollector.start_collection() 함수 호출 결과: {success}")
            
            if success:
                self.logger.info("오더북 프로세스가 성공적으로 시작되었습니다.")
                
                # 메트릭 수집 태스크 시작
                self.metric_reporter_task = asyncio.create_task(self._start_metric_reporter())
                self.logger.debug("메트릭 수집 태스크가 시작되었습니다.")
                
                return True
            else:
                self.logger.error("오더북 프로세스 시작 실패")
                return False
            
        except Exception as e:
            self.logger.error(f"오더북 프로세스 시작 중 오류 발생: {str(e)}")
            self.logger.error(f"오류 상세: {e.__class__.__name__}: {str(e)}")
            import traceback
            self.logger.error(f"스택 트레이스: {traceback.format_exc()}")
            return False
    
    async def _do_stop(self) -> bool:
        """
        오더북 프로세스 실제 종료 작업
        
        오더북 수집기의 stop 메서드를 호출하여 오더북 수집을 종료합니다.
        모든 상태 관리는 부모 클래스가 처리합니다.
        
        Returns:
            bool: 종료 성공 여부
        """
        try:
            self.logger.info("오더북 프로세스 종료 중...")
            
            # 메트릭 수집 태스크 종료
            if self.metric_reporter_task and not self.metric_reporter_task.done():
                self.logger.debug("메트릭 수집 태스크를 종료합니다.")
                self.metric_reporter_task.cancel()
                try:
                    await self.metric_reporter_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    self.logger.warning(f"메트릭 태스크 종료 중 오류: {str(e)}")
            
            # 오더북 수집기를 사용하여 종료
            success = await self.collector.stop_collection()
            
            if success:
                self.logger.info("오더북 프로세스가 성공적으로 종료되었습니다.")
            else:
                self.logger.warning("오더북 프로세스 종료 중 일부 오류가 발생했습니다.")
                
            return True
            
        except Exception as e:
            self.logger.error(f"오더북 프로세스 종료 중 오류 발생: {str(e)}")
            return False

    # 메트릭 수집 및 발행 관련 메서드
    # ======================================================
            
    async def _start_metric_reporter(self):
        """
        정기적으로 메트릭을 수집하고 발행하는 태스크
        
        각 메트릭 유형별로 다른 주기로 수집 및 발행합니다.
        """
        try:
            self.logger.info("메트릭 수집 태스크 시작")
            
            # 초기 메트릭 발행 (시작 시)
            await self.collect_and_publish_all_metrics()
            
            # 메트릭 수집 루프
            while self.status == ProcessStatus.RUNNING:
                try:
                    # 현재 시간
                    current_time = time.time()
                    
                    # 각 메트릭 유형별로 주기 확인 및 발행
                    for metric_type, interval in self.metric_intervals.items():
                        if current_time - self.last_metric_times.get(metric_type, 0) >= interval:
                            await self.collect_and_publish_metric(metric_type)
                            self.last_metric_times[metric_type] = current_time
                    
                    # 짧은 대기 후 다시 체크
                    await asyncio.sleep(1.0)
                    
                except Exception as e:
                    self.logger.error(f"메트릭 수집 중 오류: {str(e)}")
                    await asyncio.sleep(5.0)  # 오류 발생 시 더 긴 대기 시간
            
            self.logger.info("메트릭 수집 태스크 종료")
            
        except asyncio.CancelledError:
            self.logger.info("메트릭 수집 태스크가 취소되었습니다.")
            raise
            
        except Exception as e:
            self.logger.error(f"메트릭 수집 태스크 오류: {str(e)}")
    
    async def collect_and_publish_all_metrics(self):
        """모든 메트릭 수집 및 발행"""
        try:
            metrics = self.collector.get_metrics()
            
            for metric_type, data in metrics.items():
                await self.publish_metric_data(metric_type, data)
                self.last_metric_times[metric_type] = time.time()
        except Exception as e:
            self.logger.error(f"메트릭 수집 및 발행 중 오류: {str(e)}")
    
    async def collect_and_publish_metric(self, metric_type):
        """
        특정 유형의 메트릭 수집 및 발행
        
        Args:
            metric_type: 메트릭 유형 ('connection', 'message', 'subscription', 'error', 'system')
        """
        try:
            # 모든 메트릭 수집
            metrics = self.collector.get_metrics()
            
            # 특정 유형 메트릭만 발행
            if metric_type in metrics:
                await self.publish_metric_data(metric_type, metrics[metric_type])
                
        except Exception as e:
            self.logger.error(f"{metric_type} 메트릭 수집/발행 중 오류: {str(e)}")
    
    async def publish_metric_data(self, metric_type, data):
        """
        메트릭 데이터 발행
        
        Args:
            metric_type: 메트릭 유형
            data: 메트릭 데이터
        """
        try:
            # 메트릭 유형에 따른 이벤트 타입 매핑
            event_mapping = {
                "connection": "connection_status",
                "message": "message_rate",
                "subscription": "subscription_status",
                "error": "error_count",
                "system": "component_status"
            }
            
            # 해당 이벤트 타입 확인 (기본값은 일반 메트릭)
            event_type = event_mapping.get(metric_type, "metric")
            
            # 이벤트 데이터 구성
            event_data = {
                "category": "ob_collector",
                "process_name": self.process_name,
                "event_type": event_type,
                "timestamp": time.time(),
                "data": data
            }
            
            # 이벤트 버스 발행 - 문자열 직접 전달
            await self.event_bus.publish(event_type, event_data)
            
            # 메트릭 유형과 데이터 크기 로깅 (디버그 레벨)
            if self.logger.isEnabledFor(logging.DEBUG):
                data_size = len(str(data))
                self.logger.debug(f"메트릭 발행: {event_type}, 데이터 크기: {data_size} 바이트")
            
            # 메트릭 데이터를 가공하여 INFO 레벨로 출력
            if event_type == "connection_status":
                # 연결 상태 정보 가공
                connected_exchanges = []
                for exchange, status in data.items():
                    if status.get('connected', False):
                        uptime = status.get('uptime', 0)
                        uptime_str = f"{uptime:.1f}초" if uptime < 60 else f"{uptime/60:.1f}분"
                        connected_exchanges.append(f"{exchange}({uptime_str})")
                
                if connected_exchanges:
                    self.logger.info(f"🔌 연결 상태: {', '.join(connected_exchanges)}")
                else:
                    self.logger.info("🔌 연결 상태: 모든 거래소 연결 끊김")
                
            elif event_type == "message_rate":
                # 메시지 속도 정보 가공
                active_exchanges = []
                for exchange, stats in data.items():
                    rate = stats.get('rate', 0)
                    total_count = stats.get('total_count', 0)
                    if rate > 0:
                        active_exchanges.append(f"{exchange}({total_count:,}개, {rate:.1f}/s)")
                
                if active_exchanges:
                    self.logger.info(f"📨 메시지 수신: {', '.join(active_exchanges)}")
                else:
                    self.logger.info("📨 메시지 수신: 메시지 없음")
                
            elif event_type == "subscription_status":
                # 구독 상태 정보 가공
                total_symbols = 0
                active_exchanges = []
                
                for exchange, status in data.items():
                    symbols_count = status.get('total_symbols', 0)
                    total_symbols += symbols_count
                    if status.get('active', False) and symbols_count > 0:
                        active_exchanges.append(f"{exchange}({symbols_count})")
                
                self.logger.info(f"📊 구독 상태: 총 {total_symbols}개 심볼, 활성 거래소: {', '.join(active_exchanges)}")
                
            elif event_type == "error_count":
                # 오류 정보 가공
                total_errors = 0
                error_exchanges = []
                
                for exchange, errors in data.items():
                    error_count = errors.get('total', 0)
                    total_errors += error_count
                    if error_count > 0:
                        error_exchanges.append(f"{exchange}({error_count})")
                
                if total_errors > 0:
                    self.logger.info(f"⚠️ 오류 현황: 총 {total_errors}개, 거래소별: {', '.join(error_exchanges)}")
                else:
                    self.logger.info("✅ 오류 현황: 없음")
                
            elif event_type == "component_status":
                # 컴포넌트 상태 정보 가공
                status = data.get('status', '알 수 없음')
                uptime = data.get('uptime', 0)
                uptime_str = f"{uptime:.1f}초" if uptime < 60 else f"{uptime/60:.1f}분"
                
                components = []
                for comp, comp_status in data.get('components', {}).items():
                    components.append(f"{comp}({comp_status})")
                
                self.logger.info(f"🔄 시스템 상태: {status}, 가동시간: {uptime_str}, 컴포넌트: {', '.join(components)}")
                
        except Exception as e:
            self.logger.error(f"메트릭 데이터 발행 중 오류: {str(e)}")
            import traceback
            self.logger.error(f"오류 상세: {traceback.format_exc()}")

# 싱글톤 인스턴스
_instance = None

def get_orderbook_process() -> OrderbookProcess:
    """
    오더북 프로세스 컴포넌트 인스턴스를 반환합니다.
    
    싱글톤 패턴을 사용하여 하나의 인스턴스만 생성합니다.
    
    Returns:
        OrderbookProcess: 오더북 프로세스 컴포넌트 인스턴스
    """
    global _instance
    if _instance is None:
        _instance = OrderbookProcess()
    return _instance

async def initialize_orderbook_process():
    """
    오더북 프로세스 컴포넌트를 초기화합니다.
    
    이 함수는 시스템 시작 시 호출되어야 합니다.
    """
    process = get_orderbook_process()
    await process.setup()
    await process.collector.setup()  # 오더북 수집기의 setup 메서드도 호출
    return process