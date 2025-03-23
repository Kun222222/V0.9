# file: main.py

import os, asyncio, time
import logging
from datetime import datetime
import signal
import sys
import shutil

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.constants_v3 import LOG_SYSTEM, EXCHANGE_NAMES_KR, get_settings
from crosskimp.common.events.sys_event_bus import SimpleEventBus, EventType

from crosskimp.ob_collector.eventbus.handler import get_orderbook_event_bus, EventHandler, get_event_handler
from crosskimp.ob_collector.eventbus.types import EventTypes, EventPriority
from crosskimp.ob_collector.core.aggregator import Aggregator
from crosskimp.ob_collector.core.ws_usdtkrw import WsUsdtKrwMonitor
from crosskimp.ob_collector.orderbook.order_manager import create_order_manager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# 환경에 따른 로깅 설정
logger.setLevel(logging.DEBUG)
logger.info(f"{LOG_SYSTEM} 디버깅을 위해 로그 레벨을 DEBUG로 설정했습니다.")

if os.getenv("CROSSKIMP_ENV") == "production":
    # 프로덕션 환경 로그
    logger.info(f"{LOG_SYSTEM} 배포 환경에서 실행 중입니다.")
else:
    # 개발 환경 로그
    logger.warning(f"{LOG_SYSTEM} 개발 환경에서 실행 중입니다. 배포 환경에서는 'CROSSKIMP_ENV=production' 환경 변수를 설정하세요.")

# 이벤트 버스 인스턴스 가져오기
event_bus = None
# 시스템 이벤트 버스 인스턴스 추가
sys_event_bus = None

class OrderbookCollector:
    """
    오더북 수집기 클래스
    
    Aggregator에서 필터링된 심볼을 받아 OrderManager로 전달하는 
    데이터 흐름을 관리합니다.
    """
    
    def __init__(self, settings: dict):
        """
        초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        self.settings = settings
        self.aggregator = None
        self.order_managers = {}  # 거래소별 OrderManager 저장
        self.stop_event = asyncio.Event()
        self.usdtkrw_monitor = None  # USDT/KRW 모니터링 객체
        
        # 중앙 메트릭 관리자 대신 시스템 이벤트 버스 사용
        self.sys_event_bus = SimpleEventBus()
        
        # 내부 이벤트 버스 초기화
        self.event_bus = get_orderbook_event_bus()
        
        # 초기 시스템 메트릭 이벤트 발행
        self._update_system_metric("init_time", time.time())
        self._update_system_metric("collector_status", "initializing")
        
        # 환경 정보 메트릭 설정
        env = "production" if os.getenv("CROSSKIMP_ENV") == "production" else "development"
        self._update_system_metric("environment", env)
        self._update_system_metric("python_version", sys.version.split()[0])
        
        # 독립 실행 모드에서만 시그널 핸들러 설정
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"{LOG_SYSTEM} 오더북 수집기 초기화 완료")
    
    def _update_system_metric(self, metric_name, value):
        """시스템 메트릭을 이벤트 버스를 통해 업데이트"""
        if self.sys_event_bus:
            asyncio.create_task(self.sys_event_bus.publish(
                EventType.STATUS_UPDATE, 
                {
                    "component": "orderbook",
                    "metric": f"system.{metric_name}",
                    "value": value,
                    "timestamp": time.time()
                }
            ))
    
    def _signal_handler(self, sig, frame):
        """시그널 핸들러"""
        logger.info(f"{LOG_SYSTEM} 종료 시그널 수신: {sig}")
        if not self.stop_event.is_set():
            self.stop_event.set()
            # 이벤트 루프에 종료 태스크 스케줄링
            asyncio.create_task(self._handle_shutdown())
        else:
            logger.info(f"{LOG_SYSTEM} 강제 종료")
            sys.exit(1)
    
    async def _handle_shutdown(self):
        """종료 처리 핸들러"""
        try:
            logger.info(f"{LOG_SYSTEM} 프로그램 종료 처리 시작")
            
            # OrderManager 종료
            await self._stop_order_managers()
            
            # USDT/KRW 모니터 종료
            if self.usdtkrw_monitor:
                await self.usdtkrw_monitor.stop()
                logger.info(f"{LOG_SYSTEM} USDT/KRW 가격 모니터링 종료")
                
            # 시스템 종료 메트릭 업데이트
            self._update_system_metric("collector_status", "stopped")
            self._update_system_metric("stop_time", time.time())
            
            # 시스템 이벤트 버스 정리 (비동기 함수이므로 await 필요)
            if self.sys_event_bus and hasattr(self.sys_event_bus, 'stop'):
                await self.sys_event_bus.stop()
            
            # 프로그램 종료 이벤트 발행
            if self.event_bus:
                await self.event_bus.publish(EventTypes.SYSTEM_SHUTDOWN, {
                    "type": "orderbook_shutdown",
                    "message": "오더북 수집기가 종료되었습니다",
                    "timestamp": time.time()
                })
            
            logger.info(f"{LOG_SYSTEM} 프로그램 종료 처리 완료")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 종료 처리 중 오류 발생: {str(e)}")
            
        finally:
            # 로거 종료
            logging.shutdown()
    
    async def run(self):
        """실행"""
        try:
            # 시스템 이벤트 버스 시작
            if self.sys_event_bus and hasattr(self.sys_event_bus, 'start'):
                await self.sys_event_bus.start()
                logger.info(f"{LOG_SYSTEM} 시스템 이벤트 버스 시작됨")
            
            # 기본 시스템 메트릭 초기화
            self._update_system_metric("startup_time", time.time())
            self._update_system_metric("collector_status", "starting")
            
            # 프로그램 시작 이벤트 발행
            if self.event_bus:
                await self.event_bus.publish(EventTypes.SYSTEM_STARTUP, {
                    "type": "orderbook_startup",
                    "message": "오더북 수집기가 시작되었습니다",
                    "timestamp": time.time(),
                    "environment": "production" if os.getenv("CROSSKIMP_ENV") == "production" else "development"
                })
            
            # USDT/KRW 모니터 시작
            self.usdtkrw_monitor = WsUsdtKrwMonitor()
            usdtkrw_task = asyncio.create_task(self.usdtkrw_monitor.start())
            logger.info(f"{LOG_SYSTEM} USDT/KRW 가격 모니터링 시작")
            
            # Aggregator 초기화 및 심볼 필터링
            logger.info(f"{LOG_SYSTEM} Aggregator 초기화 및 심볼 필터링 시작")
            self.aggregator = Aggregator(self.settings)
            filtered_data = await self.aggregator.run_filtering()
            
            if not filtered_data:
                logger.error(f"{LOG_SYSTEM} 필터링된 심볼이 없습니다.")
                self._update_system_metric("collector_status", "error")
                self._update_system_metric("error_reason", "no_filtered_symbols")
                return
            
            # 필터링 결과 로그 출력
            log_msg = f"{LOG_SYSTEM} 필터링된 심볼: "
            for exchange_code, symbols in filtered_data.items():
                log_msg += f"\n{EXCHANGE_NAMES_KR[exchange_code]} - {len(symbols)}개: {', '.join(symbols[:5])}"
                if len(symbols) > 5:
                    log_msg += f" 외 {len(symbols)-5}개"
            logger.info(log_msg)
            
            # OrderManager 초기화 및 시작
            await self._start_order_managers(filtered_data)
            
            # 시스템 상태 업데이트 - 정상 실행 중
            self._update_system_metric("collector_status", "running")
            
            # 종료 이벤트 대기 - 독립 실행 모드에서만 직접 안내
            logger.info(f"{LOG_SYSTEM} 프로그램을 종료하려면 Ctrl+C를 누르세요")
            # 여기서 대기하지만, signal_handler에서 종료 처리를 병렬로 시작함
            await self.stop_event.wait()
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 실행 중 오류 발생: {str(e)}")
            # 오류 상태 기록
            self._update_system_metric("collector_status", "error")
            self._update_system_metric("error_reason", str(e))
            
            await self._stop_order_managers()
            
            # USDT/KRW 모니터 종료
            if self.usdtkrw_monitor:
                await self.usdtkrw_monitor.stop()
                
            # 시스템 이벤트 버스 정리
            if self.sys_event_bus and hasattr(self.sys_event_bus, 'stop'):
                await self.sys_event_bus.stop()
    
    async def _start_order_managers(self, filtered_data):
        """
        OrderManager 초기화 및 시작
        
        Args:
            filtered_data: 필터링된 심볼 데이터
        """
        # 결과 값을 체크하기 위한 리스트
        order_manager_tasks = []
        
        async def init_and_start_manager(exchange, symbols):
            """단일 거래소에 대한 OrderManager 초기화 및 시작 함수"""
            try:
                if not symbols:
                    logger.info(f"{EXCHANGE_NAMES_KR[exchange]} 심볼이 없어 OrderManager를 초기화하지 않습니다.")
                    return None
                
                # OrderManager 생성
                manager = create_order_manager(exchange, self.settings)
                if not manager:
                    logger.error(f"{EXCHANGE_NAMES_KR[exchange]} OrderManager 생성 실패")
                    return None
                
                # 초기화 및 시작
                await manager.initialize()
                await manager.start(symbols)
                
                logger.info(f"{EXCHANGE_NAMES_KR[exchange]} OrderManager 시작 완료")
                return exchange, manager
            except Exception as e:
                logger.error(f"{EXCHANGE_NAMES_KR[exchange]} OrderManager 초기화/시작 중 오류: {str(e)}", exc_info=True)
                return None
                
        # 모든 거래소 OrderManager 초기화 및 시작 태스크 생성
        for exchange, symbols in filtered_data.items():
            order_manager_tasks.append(init_and_start_manager(exchange, symbols))
        
        # 모든 태스크 동시 실행
        results = await asyncio.gather(*order_manager_tasks, return_exceptions=True)
        
        # 결과 처리
        for result in results:
            if result and isinstance(result, tuple):
                exchange, manager = result
                self.order_managers[exchange] = manager
            elif isinstance(result, Exception):
                logger.error(f"OrderManager 시작 중 예외 발생: {str(result)}")
        
        logger.info(f"{LOG_SYSTEM} 오더북 수집기 시작 완료")
    
    async def _stop_order_managers(self):
        """OrderManager 종료"""
        # 시스템 상태 업데이트
        self._update_system_metric("collector_status", "stopping")
        
        # 먼저 모든 이벤트 핸들러에 종료 상태 설정
        for exchange_code in list(EventHandler._instances.keys()):
            try:
                event_handler = get_event_handler(exchange_code)
                if event_handler:
                    try:
                        # 종료 상태 설정
                        event_handler.set_shutting_down()
                    except AttributeError:
                        logger.warning(f"{EXCHANGE_NAMES_KR[exchange_code]} set_shutting_down 메서드 없음")
                    except Exception as e:
                        logger.error(f"{EXCHANGE_NAMES_KR[exchange_code]} 종료 상태 설정 중 오류: {str(e)}", exc_info=True)
                    
                    try:
                        # 메트릭 요약 로그 태스크 중지
                        if hasattr(event_handler, 'stop_summary_log_task'):
                            await event_handler.stop_summary_log_task()
                    except AttributeError:
                        logger.warning(f"{EXCHANGE_NAMES_KR[exchange_code]} stop_summary_log_task 메서드 없음")
                    except Exception as e:
                        logger.error(f"{EXCHANGE_NAMES_KR[exchange_code]} 요약 로그 태스크 중지 중 오류: {str(e)}", exc_info=True)
                    
                    logger.info(f"{EXCHANGE_NAMES_KR[exchange_code]} 메트릭 수집 중지 완료")
            except Exception as e:
                logger.error(f"이벤트 핸들러 정리 중 오류: {str(e)}", exc_info=True)
                
        # 이제 OrderManager들 종료
        for exchange, manager in self.order_managers.items():
            try:
                await manager.stop()
                logger.info(f"{EXCHANGE_NAMES_KR[exchange]} OrderManager 중지 완료")
            except Exception as e:
                logger.error(f"{EXCHANGE_NAMES_KR[exchange]} OrderManager 중지 중 오류: {str(e)}", exc_info=True)
        
        self.order_managers.clear()
        
        # 시스템 상태 최종 업데이트
        self._update_system_metric("collector_status", "stopped")
        self._update_system_metric("stop_time", time.time())
        
        logger.info(f"{LOG_SYSTEM} 오더북 수집기 중지 완료")

async def async_main():
    """비동기 메인 함수"""
    global event_bus, sys_event_bus
    
    try:
        # 설정 로드
        logger.info(f"{LOG_SYSTEM} 설정 로드 시작")
        start_time = time.time()
        settings = get_settings()
        elapsed = time.time() - start_time
        logger.info(f"{LOG_SYSTEM} 설정 로드 완료 (소요 시간: {elapsed:.3f}초)")
        
        # 로그 폴더 확인 및 생성
        log_path = "/Users/kun/Desktop/CrossKimpArbitrage/v0.6/src/logs"
        try:
            # 로그 폴더 생성 (이미 존재해도 오류 발생하지 않음)
            os.makedirs(log_path, exist_ok=True)
            
            # 필요한 하위 폴더만 생성
            os.makedirs(os.path.join(log_path, "raw_data"), exist_ok=True)
            os.makedirs(os.path.join(log_path, "orderbook_data"), exist_ok=True)
            
            print(f"로그 폴더 확인 완료: {log_path}")
        except Exception as e:
            print(f"로그 폴더 초기화 중 오류 발생: {str(e)}")
        
        # 시스템 이벤트 버스 초기화 및 시작
        sys_event_bus = SimpleEventBus()
        await sys_event_bus.start()
        logger.info(f"{LOG_SYSTEM} 시스템 이벤트 버스 초기화 및 시작 완료")
        
        # 이벤트 버스 초기화
        try:
            # 내부 이벤트 버스 사용으로 변경
            event_bus = get_orderbook_event_bus()
            
            # 이벤트 버스가 실행 중이 아니면 시작
            if hasattr(event_bus, 'is_running') and not event_bus.is_running:
                asyncio.create_task(event_bus.start())
            
            logger.info(f"{LOG_SYSTEM} 이벤트 버스 초기화 완료")
        except Exception as e:
            logger.warning(f"{LOG_SYSTEM} 이벤트 버스 초기화 실패, 이벤트 발행 기능이 동작하지 않습니다: {str(e)}")
        
        # 오더북 수집기 생성 및 실행
        collector = OrderbookCollector(settings)
        await collector.run()
        
    except KeyboardInterrupt:
        logger.info(f"{LOG_SYSTEM} 키보드 인터럽트 수신")
        
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 메인 함수 오류: {str(e)}")
        
    finally:
        try:
            # 시스템 이벤트 버스 종료
            if sys_event_bus:
                try:
                    await sys_event_bus.stop()
                    logger.info(f"{LOG_SYSTEM} 시스템 이벤트 버스 종료 완료")
                except Exception as e:
                    logger.error(f"{LOG_SYSTEM} 시스템 이벤트 버스 종료 중 오류: {str(e)}")
            
            # 내부 이벤트 버스 종료
            if event_bus:
                try:
                    await event_bus.stop()
                    logger.info(f"{LOG_SYSTEM} 이벤트 버스 종료 완료")
                except Exception as e:
                    logger.error(f"{LOG_SYSTEM} 이벤트 버스 종료 중 오류: {str(e)}")
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 메인 함수 종료 처리 중 오류: {str(e)}")

def main():
    """동기 메인 함수 - 독립 실행 모드에서만 사용"""
    try:
        # 직접 실행 시에만 경고 메시지 출력
        print("""
주의: 이 파일은 이제 단독으로 실행하지 않는 것이 권장됩니다.
통합 시스템을 사용하려면 다음 명령어를 사용하세요:
  python -m crosskimp.crosskimp_system

오더북 수집기만 실행하려면:
  python -m crosskimp.crosskimp_system --collector-only
        """)
        
        if not os.getenv("CROSSKIMP_ENV"):
            logger.warning(f"{LOG_SYSTEM} 개발 환경에서 실행 중입니다. 배포 환경에서는 'CROSSKIMP_ENV=production' 환경 변수를 설정하세요.")
            
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info(f"{LOG_SYSTEM} 키보드 인터럽트 감지")
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 예상치 못한 오류 발생: {e}", exc_info=True)

if __name__ == "__main__":
    main()