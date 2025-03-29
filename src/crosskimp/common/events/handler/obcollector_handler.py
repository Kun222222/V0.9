"""
오더북 프로세스 핸들러

ProcessComponent를 상속받아 오더북 프로세스의 시작/종료를 관리합니다.
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.handler.process_component import ProcessComponent
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.events.system_types import EventPaths
from crosskimp.ob_collector.metric.reporter import ObcMetricReporter

# 오더북 수집기 임포트
from crosskimp.ob_collector.orderbook._legacy.ob_collector import ObCollector

# 로거 설정
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

class OrderbookProcess(ProcessComponent):
    """
    오더북 수집 프로세스
    
    오더북 수집을 위한 프로세스 컴포넌트입니다.
    ProcessComponent를 상속받아 프로세스 생명주기와 상태 관리를 담당합니다.
    기술적 작업은 ObCollector에 위임하고, 이 클래스는 상태 관리만 담당합니다.
    """

    def __init__(self, eventbus=None, config=None):
        """
        초기화
        
        Args:
            eventbus: 이벤트 버스 객체 (기본값: None, None일 경우 get_event_bus()로 가져옴)
            config: 설정 딕셔너리 (기본값: None, None일 경우 get_config()로 가져옴)
        """
        # 부모 클래스 초기화 (process_name만 전달)
        super().__init__(process_name="ob_collector")
        
        # 이벤트 버스와 설정 저장
        from crosskimp.common.events.system_eventbus import get_event_bus
        from crosskimp.common.config.app_config import get_config
        
        self.eventbus = eventbus if eventbus is not None else get_event_bus()
        self.config = config if config is not None else get_config()
        
        # 데이터 수집을 위한 ObCollector 인스턴스 생성
        self.collector = ObCollector()
        
        # 메트릭 수집 관련 변수
        self.metric_interval = self.config.get("metrics", {}).get("interval", 20)  # 기본값 20초 (60초에서 변경)
        self.metric_reporter = None

    async def _do_setup(self) -> bool:
        """
        프로세스 설정
        
        Returns:
            bool: 설정 성공 여부
        """
        try:
            self.logger.info("오더북 프로세스 설정 중...")
            
            # ObCollector 설정
            setup_success = await self.collector.setup()
            if not setup_success:
                self.logger.error("오더북 수집기 설정 실패")
                return False
                
            self.logger.info("오더북 프로세스 설정 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"오더북 프로세스 설정 중 오류 발생: {str(e)}")
            return False

    async def _do_start(self) -> bool:
        """
        오더북 프로세스 시작
        
        Returns:
            bool: 시작 성공 여부
        """
        try:
            self.logger.info("오더북 프로세스 시작 중...")
            
            # 1. 초기화 필요성 확인 및 수행
            if not self.collector.is_initialized():
                self.logger.debug("오더북 수집기 초기화 시작")
                
                # 초기화 수행
                init_success = await self.collector.initialize()
                if not init_success:
                    self.logger.error("오더북 수집기 초기화 실패")
                    
                    # 상태 변경: 초기화 실패 (error 상태 사용)
                    await self._publish_status(EventPaths.PROCESS_STATUS_ERROR, 
                                             error_message="오더북 수집기 초기화 실패")
                    return False
                    
                self.logger.debug("오더북 수집기 초기화 완료")
            else:
                self.logger.debug("오더북 수집기가 이미 초기화되어 있음")
            
            # 2. 기술적 작업 시작
            self.logger.debug("오더북 수집기 시작 요청")
            success = await self.collector.start_collection()
            
            if not success:
                self.logger.error("오더북 수집기 시작 실패")
                
                # 상태 변경: 시작 실패 (error 상태 사용)
                await self._publish_status(EventPaths.PROCESS_STATUS_ERROR, 
                                         error_message="오더북 수집기 시작 실패")
                return False
                
            # 수집기 상태 확인 - 단순화된 상태에 맞게 수정
            collector_status = self.collector.get_status()
            self.logger.debug(f"오더북 수집기 현재 상태: {collector_status}")
            
            # 상태 모니터링 태스크 시작 (백그라운드)
            asyncio.create_task(self._monitor_collector_status())
            
            # 3. 메트릭 수집 시작 (백그라운드에서)
            self.logger.info("메트릭 수집 시작")
            self.metric_reporter = ObcMetricReporter(self.collector, self.metric_interval)
            await self.metric_reporter.start()
            
            # 오더북 수집기의 상태에 맞게 프로세스 상태 이벤트 발행
            # 시작되었으므로 running 상태 이벤트 발행
            self.logger.info("오더북 프로세스가 성공적으로 시작되었습니다. (거래소 연결은 백그라운드에서 계속됩니다)")
            return True
            
        except Exception as e:
            self.logger.error(f"오더북 프로세스 시작 중 오류 발생: {str(e)}")
            await self._publish_status(EventPaths.PROCESS_STATUS_ERROR, 
                                     error_message=f"시작 중 오류 발생: {str(e)}")
            return False

    async def _monitor_collector_status(self):
        """오더북 수집기의 상태를 모니터링하고 running 상태가 되면 이벤트 발행"""
        self.logger.info("📊 오더북 수집기 상태 모니터링 태스크 시작")
        
        # 초기 상태 저장
        last_status = None
        check_count = 0
        event_publishing_attempted = False
        
        # 상태가 running이 될 때까지 모니터링
        while True:
            try:
                check_count += 1
                current_status = self.collector.get_status()
                
                # 주기적 로깅
                if check_count % 5 == 0:
                    self.logger.debug(f"📊 오더북 수집기 상태 확인 중: {current_status} (확인 횟수: {check_count})")
                
                # 상태 변경 로깅
                if current_status != last_status:
                    self.logger.info(f"📊 오더북 수집기 상태 변경 감지: {last_status} → {current_status}")
                
                # 처음으로 running 상태가 되었을 때
                if current_status == "running" and last_status != "running" and not event_publishing_attempted:
                    self.logger.info("🎉 오더북 수집기가 성공적으로 구동 완료되었습니다!")
                    event_publishing_attempted = True
                    
                    # 이벤트 경로 상수 사용
                    event_path = EventPaths.OB_COLLECTOR_RUNNING
                    
                    # 이벤트 발행 전 이벤트 버스 상태 확인
                    self.logger.info(f"ℹ️ 이벤트 버스 인스턴스 ID: {id(self.eventbus)}")
                    handlers = self.eventbus._handlers.get(event_path, [])
                    self.logger.info(f"⏱️ 이벤트 발행 전 핸들러 수: {len(handlers)}")
                    
                    if handlers:
                        handler_names = [f"{h.__module__.split('.')[-1]}.{h.__qualname__}" for h in handlers]
                        self.logger.info(f"📑 등록된 핸들러: {handler_names}")
                    else:
                        self.logger.warning("⚠️ 오더북 수집기 구동 완료 이벤트에 핸들러가 등록되지 않았습니다!")
                    
                    # 이벤트 데이터 준비
                    event_data = {
                        "message": "오더북 수집기가 성공적으로 구동되었습니다.",
                        "process_name": self.process_name,
                        "status": EventPaths.PROCESS_STATUS_RUNNING,
                        "details": {"collector_status": current_status}
                    }
                    
                    # 이벤트 발행
                    self.logger.info(f"📣 이벤트 발행 시작: {event_path}")
                    try:
                        await self.eventbus.publish(event_path, event_data)
                        self.logger.info(f"✅ 이벤트 발행 완료: {event_path}")
                    except Exception as e:
                        self.logger.error(f"❌ 이벤트 발행 실패: {str(e)}", exc_info=True)
                        # 스택 트레이스 출력
                        import traceback
                        self.logger.error(f"🔍 스택 트레이스: {traceback.format_exc()}")
                    
                    # 이후 상태 확인
                    try:
                        await asyncio.sleep(0.5)  # 이벤트 처리 시간 확보
                        post_handlers = self.eventbus._handlers.get(event_path, [])
                        self.logger.info(f"⏱️ 이벤트 발행 후 핸들러 수: {len(post_handlers)}")
                    except Exception as e:
                        self.logger.error(f"❌ 후속 확인 실패: {str(e)}")
                    
                    # 모니터링 계속 (여러 번 확인을 위해 종료하지 않음)
                    
                last_status = current_status
                await asyncio.sleep(1)  # 1초마다 확인
                
            except Exception as e:
                self.logger.error(f"❌ 상태 모니터링 중 오류: {str(e)}", exc_info=True)
                await asyncio.sleep(5)  # 오류 발생 시 더 긴 간격으로 재시도
                
        self.logger.info("📊 오더북 수집기 상태 모니터링 태스크 종료")

    async def _do_stop(self) -> bool:
        """
        오더북 프로세스 중지
        
        Returns:
            bool: 중지 성공 여부
        """
        try:
            self.logger.info("오더북 프로세스 중지 중...")
            
            # 상태 변경: 중지 중
            await self._publish_status(EventPaths.PROCESS_STATUS_STOPPING)
            
            # 1. 메트릭 수집 중지
            if self.metric_reporter:
                try:
                    await self.metric_reporter.stop()
                    self.logger.debug("메트릭 리포터 종료됨")
                except Exception as e:
                    self.logger.warning(f"메트릭 리포터 종료 중 오류: {str(e)}")
                self.metric_reporter = None
            
            # 2. 수집기 중지
            stop_success = await self.collector.stop_collection()
            if not stop_success:
                self.logger.error("오더북 수집기 중지 실패")
                
                # 상태 변경: 중지 실패 (error 상태 사용)
                await self._publish_status(EventPaths.PROCESS_STATUS_ERROR, 
                                         error_message="오더북 수집기 중지 실패")
            
            # 상태 변경: 중지됨
            await self._publish_status(EventPaths.PROCESS_STATUS_STOPPED)
            
            self.logger.info("오더북 프로세스가 성공적으로 중지되었습니다.")
            return True
            
        except Exception as e:
            self.logger.error(f"오더북 프로세스 중지 중 오류 발생: {str(e)}")
            await self._publish_status(EventPaths.PROCESS_STATUS_ERROR, 
                                     error_message=f"중지 중 오류 발생: {str(e)}")
            return False

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
    await process.setup()
    return process