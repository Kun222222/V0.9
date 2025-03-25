"""
오더북 수집 실행 모듈

이 모듈은 여러 거래소에서 오더북 데이터를 수집하는 독립 실행 스크립트입니다.
"""

import os, asyncio, time
import logging
import signal
import sys
from typing import Dict, List, Any, Optional

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, COMPONENT_NAMES_KR, EXCHANGE_NAMES_KR
from crosskimp.common.config.app_config import get_config
from crosskimp.common.events.system_eventbus import get_event_bus, EventType

# 오더북 수집기 클래스 임포트
from crosskimp.ob_collector.ob_collector import ObCollector

# 로거 인스턴스 가져오기
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

# 환경에 따른 로깅 설정
logger.setLevel(logging.DEBUG)
logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 디버깅을 위해 로그 레벨을 DEBUG로 설정했습니다.")

if os.getenv("CROSSKIMP_ENV") == "production":
    # 프로덕션 환경 로그
    logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 배포 환경에서 실행 중입니다.")
else:
    # 개발 환경 로그
    logger.warning(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 개발 환경에서 실행 중입니다. 배포 환경에서는 'CROSSKIMP_ENV=production' 환경 변수를 설정하세요.")

class ObCollectorRunner:
    """
    오더북 수집기 실행 클래스
    
    시스템 독립 실행 시 사용되는 래퍼 클래스입니다.
    """
    
    def __init__(self):
        """초기화"""
        self.event_bus = get_event_bus()
        self.collector = None
        self.stop_event = asyncio.Event()
        
        # 독립 실행 모드에서만 시그널 핸들러 설정
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 오더북 수집기 러너 초기화 완료")
    
    def _signal_handler(self, sig, frame):
        """시그널 핸들러"""
        logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 종료 시그널 수신: {sig}")
        if not self.stop_event.is_set():
            self.stop_event.set()
            # 이벤트 루프에 종료 태스크 스케줄링
            asyncio.create_task(self._handle_shutdown())
        else:
            logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 강제 종료")
            sys.exit(1)
    
    async def _handle_shutdown(self):
        """종료 처리 핸들러"""
        try:
            logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 프로그램 종료 처리 시작")
            
            # 오더북 수집기 중지
            if self.collector:
                await self.collector.stop()
                logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 오더북 수집기 종료 완료")
            
            # 이벤트 버스 정리
            if self.event_bus and hasattr(self.event_bus, 'stop'):
                await self.event_bus.stop()
                logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 이벤트 버스 종료 완료")
            
            logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 프로그램 종료 처리 완료")
            
        except Exception as e:
            logger.error(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 종료 처리 중 오류 발생: {str(e)}")
            
        finally:
            # 로거 종료
            logging.shutdown()
    
    async def run(self):
        """오더북 수집기 실행"""
        try:
            # 이벤트 버스 시작
            if self.event_bus and hasattr(self.event_bus, 'start'):
                await self.event_bus.start()
                logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 이벤트 버스 시작됨")
            
            # 오더북 수집기 생성 및 초기화
            self.collector = ObCollector()
            
            # 셋업 및 초기화
            await self.collector.setup()
            init_success = await self.collector.initialize()
            
            if not init_success:
                logger.error(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 오더북 수집기 초기화 실패")
                return
                
            # 오더북 수집기 시작
            start_success = await self.collector.start()
            
            if not start_success:
                logger.error(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 오더북 수집기 시작 실패")
                return
                
            logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 오더북 수집기 실행 중")
            
            # 종료 이벤트 대기 - 독립 실행 모드에서만 직접 안내
            logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 프로그램을 종료하려면 Ctrl+C를 누르세요")
            await self.stop_event.wait()
            
        except Exception as e:
            logger.error(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 실행 중 오류 발생: {str(e)}")
            
            # 오류 발생 시 수집기 종료
            if self.collector:
                try:
                    await self.collector.stop()
                except Exception as stop_error:
                    logger.error(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 오더북 수집기 종료 중 오류: {str(stop_error)}")
            
            # 이벤트 버스 정리
            if self.event_bus and hasattr(self.event_bus, 'stop'):
                try:
                    await self.event_bus.stop()
                except Exception as stop_error:
                    logger.error(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 이벤트 버스 종료 중 오류: {str(stop_error)}")

async def async_main():
    """비동기 메인 함수"""
    try:
        # 설정 로드
        logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 설정 로드 시작")
        start_time = time.time()
        settings = get_config()
        elapsed = time.time() - start_time
        logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 설정 로드 완료 (소요 시간: {elapsed:.3f}초)")
        
        # 로그 폴더 확인 및 생성
        log_path = settings.get_path("logs_dir")
        try:
            # 로그 폴더 생성 (이미 존재해도 오류 발생하지 않음)
            os.makedirs(log_path, exist_ok=True)
            
            # 필요한 하위 폴더만 생성
            os.makedirs(os.path.join(log_path, "raw_data"), exist_ok=True)
            os.makedirs(os.path.join(log_path, "orderbook_data"), exist_ok=True)
            
            logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 로그 폴더 확인 완료: {log_path}")
        except Exception as e:
            logger.error(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 로그 폴더 초기화 중 오류 발생: {str(e)}")
        
        # 오더북 수집기 러너 생성 및 실행
        runner = ObCollectorRunner()
        await runner.run()
        
    except KeyboardInterrupt:
        logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 키보드 인터럽트 수신")
        
    except Exception as e:
        logger.error(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 메인 함수 오류: {str(e)}")

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
            logger.warning(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 개발 환경에서 실행 중입니다. 배포 환경에서는 'CROSSKIMP_ENV=production' 환경 변수를 설정하세요.")
            
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 키보드 인터럽트 감지")
    except Exception as e:
        logger.error(f"{COMPONENT_NAMES_KR[SystemComponent.SYSTEM.value]} 예상치 못한 오류 발생: {e}", exc_info=True)

if __name__ == "__main__":
    main()