"""
메트릭 리포터

주기적으로 메트릭 데이터를 수집하고 이벤트 버스를 통해 전송하는 컴포넌트입니다.
"""

import asyncio
import time
from typing import Dict, Any, Optional, Callable, List

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.events.system_types import EventPaths
from crosskimp.common.events.system_eventbus import get_event_bus

class MetricReporter:
    """메트릭 데이터 수집 및 보고 클래스"""
    
    def __init__(self, metric_collector: Callable, component: str, event_path: str, interval: int = 30):
        """
        초기화
        
        Args:
            metric_collector: 메트릭 데이터를 수집하는 콜백 함수
            component: 컴포넌트 이름
            event_path: 이벤트 발행 경로
            interval: 수집 간격(초)
        """
        self.logger = get_unified_logger(component=component)
        self.component = component
        self.event_path = event_path
        self.metric_interval = interval
        self.metric_collector = metric_collector
        
        # 이벤트 버스 가져오기
        self.event_bus = get_event_bus()
        
        # 태스크 및 상태 관리
        self.reporter_task = None
        self.is_running = False
        
        # 이전 메트릭 저장 (변경점 계산용)
        self.previous_metrics = {}
        self.last_metric_time = None
        
    async def start(self):
        """메트릭 리포터 시작"""
        if self.is_running:
            self.logger.debug("메트릭 리포터가 이미 실행 중입니다")
            return
            
        self.is_running = True
        self.reporter_task = asyncio.create_task(self._run_reporter())
        self.logger.debug(f"메트릭 리포터 시작 (수집 간격: {self.metric_interval}초)")
        
    async def stop(self):
        """메트릭 리포터 중지"""
        if not self.is_running:
            return
            
        self.is_running = False
        if self.reporter_task:
            try:
                self.reporter_task.cancel()
                self.logger.debug("메트릭 리포터 태스크 취소됨")
            except Exception as e:
                self.logger.warning(f"메트릭 리포터 태스크 취소 중 오류: {str(e)}")
            self.reporter_task = None
    
    async def _run_reporter(self):
        """메트릭 데이터 주기적 수집 및 이벤트 발행"""
        try:
            self.logger.info(f"🚀 메트릭 리포터 실행 시작 (컴포넌트: {self.component}, 간격: {self.metric_interval}초)")
            self.last_metric_time = time.time()
            
            while self.is_running:
                try:
                    # 메트릭 데이터 수집 시작 로그
                    self.logger.debug(f"메트릭 데이터 수집 시작")
                    
                    # 메트릭 데이터 수집
                    metrics = self.metric_collector()
                    current_time = time.time()
                    
                    # 수집된 메트릭 데이터 확인
                    if metrics:
                        self.logger.debug(f"메트릭 데이터 수집 완료 - 처리 시작")
                        
                        # 메트릭 간격 계산 - 첫 실행이면 기본 간격 사용
                        elapsed = current_time - self.last_metric_time if self.last_metric_time else self.metric_interval
                        
                        # 각 메트릭 타입별 처리 및 발행
                        await self._process_and_publish_metrics(metrics, elapsed)
                        
                        # 이전 메트릭 저장
                        self.previous_metrics = metrics
                        self.last_metric_time = current_time
                    else:
                        self.logger.warning(f"⚠️ 수집된 메트릭 데이터가 없습니다")
                    
                    # 지정된 간격만큼 대기
                    self.logger.debug(f"다음 메트릭 수집까지 {self.metric_interval}초 대기")
                    await asyncio.sleep(self.metric_interval)
                    
                except asyncio.CancelledError:
                    self.logger.debug("메트릭 수집 태스크가 취소되었습니다.")
                    break
                except Exception as e:
                    self.logger.error(f"메트릭 수집 중 오류 발생: {str(e)}", exc_info=True)
                    await asyncio.sleep(10)  # 오류 발생 시 10초 후 재시도
        
        except asyncio.CancelledError:
            self.logger.debug("메트릭 리포터가 취소되었습니다.")
        except Exception as e:
            self.logger.error(f"메트릭 리포터 실행 중 오류 발생: {str(e)}", exc_info=True)
        finally:
            self.is_running = False
            self.logger.debug("메트릭 리포터 종료")
            
    async def _process_and_publish_metrics(self, metrics, elapsed_time):
        """메트릭 처리 및 타입별 발행"""
        try:
            # 메트릭 요약 생성 및 로깅
            self._log_metric_summary(metrics, elapsed_time)
            
            # 메트릭 타입별로 분리하여 발행
            await self._publish_connection_metrics(metrics.get("connection", {}))
            await self._publish_message_metrics(metrics.get("message", {}))
            await self._publish_error_metrics(metrics.get("error", {}))
            await self._publish_system_metrics(metrics.get("system", {}))
            await self._publish_subscription_metrics(metrics.get("subscription", {}))
            
            # 전체 통계 요약 로깅
            total_stats = self._get_total_statistics(metrics)
            self.logger.info(f"📊 [메트릭 합계] {'전체 거래소':15} | 총: {total_stats['total']:8d}건 | 수신: {total_stats['interval']:6d}건/{self.metric_interval:.0f}초 | 속도: {total_stats['rate']:.2f}건/초")
            
        except Exception as e:
            self.logger.error(f"메트릭 처리 및 발행 중 오류: {str(e)}", exc_info=True)
    
    async def _publish_connection_metrics(self, connection_metrics):
        """연결 메트릭 발행"""
        if not connection_metrics:
            return
            
        event_data = {
            "process_id": self.component,
            "timestamp": time.time(),
            "metric_type": "connection",
            "metrics": connection_metrics
        }
        
        await self.event_bus.publish(
            event_path=f"{self.event_path}/connection",
            data=event_data
        )
            
    async def _publish_message_metrics(self, message_metrics):
        """메시지 메트릭 발행"""
        if not message_metrics:
            return
            
        event_data = {
            "process_id": self.component,
            "timestamp": time.time(),
            "metric_type": "message",
            "metrics": message_metrics
        }
        
        await self.event_bus.publish(
            event_path=f"{self.event_path}/message",
            data=event_data
        )
    
    async def _publish_error_metrics(self, error_metrics):
        """오류 메트릭 발행"""
        if not error_metrics:
            return
            
        event_data = {
            "process_id": self.component,
            "timestamp": time.time(),
            "metric_type": "error",
            "metrics": error_metrics
        }
        
        await self.event_bus.publish(
            event_path=f"{self.event_path}/error",
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
            "process_id": self.component,
            "timestamp": time.time(),
            "metric_type": "system",
            "metrics": system_metrics
        }
        
        await self.event_bus.publish(
            event_path=f"{self.event_path}/system",
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
            "process_id": self.component,
            "timestamp": time.time(),
            "metric_type": "subscription",
            "metrics": simplified_metrics
        }
        
        await self.event_bus.publish(
            event_path=f"{self.event_path}/subscription",
            data=event_data
        )
    
    def _log_metric_summary(self, metrics, elapsed_time):
        """수집된 메트릭 데이터 요약하여 로깅"""
        try:
            message_metrics = metrics.get("message", {})
            prev_message_metrics = self.previous_metrics.get("message", {})
            
            # 시스템 상태 정보 로깅
            system_metrics = metrics.get("system", {})
            if system_metrics:
                status = system_metrics.get("status", "unknown")
                uptime = system_metrics.get("uptime", 0)
                connected_exchanges = system_metrics.get("connected_exchanges", 0)
                total_exchanges = system_metrics.get("total_exchanges", 0)
                
                # 상태 이모지 선택
                status_emoji = "🟢" if status == "process/started" else "🟡" if status == "process/starting" else "🔴"
                
                # 시스템 상태 로깅
                self.logger.info(
                    f"{status_emoji} [시스템 상태] 상태: {status} | "
                    f"실행시간: {int(uptime//60):02d}:{int(uptime%60):02d} | "
                    f"연결된 거래소: {connected_exchanges}/{total_exchanges}"
                )
            
            # 거래소별 메시지 처리량 요약
            for exchange, data in message_metrics.items():
                # 현재 총 메시지 수
                total_count = data.get("total_count", 0)
                
                # 이전 총 메시지 수 (첫 실행이면 0)
                prev_total = 0
                if exchange in prev_message_metrics:
                    prev_total = prev_message_metrics[exchange].get("total_count", 0)
                
                # 구간 메시지 수 계산
                interval_count = total_count - prev_total
                
                # 초당 메시지 처리율 (rate는 이미 계산되어 있음)
                rate = data.get("rate", 0)
                
                # 오류 정보
                error_metrics = metrics.get("error", {}).get(exchange, {})
                error_count = error_metrics.get("total", 0)
                
                # 연결 상태
                connection_metrics = metrics.get("connection", {}).get(exchange, {})
                connected = connection_metrics.get("connected", False)
                
                # 상태 이모지 선택
                status_emoji = "🟢" if connected else "🔴"
                
                # 거래소별 요약 로깅 (에러가 있으면 경고 표시)
                error_text = f" | 오류: {error_count}건" if error_count > 0 else ""
                self.logger.info(
                    f"{status_emoji} [메트릭 요약] {exchange:15} | "
                    f"총: {int(total_count):8d}건 | "
                    f"수신: {int(interval_count):6d}건/{elapsed_time:.0f}초 | "
                    f"속도: {rate:.2f}건/초{error_text}"
                )
                
        except Exception as e:
            self.logger.error(f"메트릭 요약 로깅 중 오류: {str(e)}")
    
    def _get_total_statistics(self, metrics):
        """전체 통계 계산"""
        try:
            message_metrics = metrics.get("message", {})
            prev_message_metrics = self.previous_metrics.get("message", {})
            
            # 전체 합계 계산을 위한 변수 초기화
            total_count = 0
            interval_count = 0
            total_rate = 0
            
            # 각 거래소별 합산
            for exchange, data in message_metrics.items():
                # 현재 총 메시지 수
                total_count += data.get("total_count", 0)
                
                # 이전 총 메시지 수 (첫 실행이면 0)
                prev_total = 0
                if exchange in prev_message_metrics:
                    prev_total = prev_message_metrics[exchange].get("total_count", 0)
                
                # 구간 메시지 수 계산
                interval_count += (data.get("total_count", 0) - prev_total)
                
                # 초당 메시지 처리율
                total_rate += data.get("rate", 0)
            
            return {
                "total": total_count,
                "interval": interval_count,
                "rate": total_rate
            }
                
        except Exception as e:
            self.logger.error(f"전체 통계 계산 중 오류: {str(e)}")
            return {"total": 0, "interval": 0, "rate": 0}
            
class ObcMetricReporter(MetricReporter):
    """오더북 수집기 전용 메트릭 리포터"""
    
    def __init__(self, collector, interval: int = 20):
        """
        초기화
        
        Args:
            collector: ObCollector 인스턴스
            interval: 수집 간격(초)
        """
        super().__init__(
            metric_collector=collector.get_metrics,
            component=SystemComponent.OB_COLLECTOR.value,
            event_path=EventPaths.OB_COLLECTOR_METRICS,
            interval=interval
        )
        self.collector = collector 