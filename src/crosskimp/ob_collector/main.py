# file: main.py

import os, asyncio, time
import logging
from datetime import datetime
import signal
import sys

from crosskimp.logger.logger import get_unified_logger
from crosskimp.ob_collector.core.aggregator import Aggregator
from crosskimp.config.constants_v3 import LOG_SYSTEM, EXCHANGE_NAMES_KR, get_settings
from crosskimp.ob_collector.orderbook.order_manager import create_order_manager
from crosskimp.telegrambot.telegram_notification import send_telegram_message
from crosskimp.ob_collector.core.ws_usdtkrw import WsUsdtKrwMonitor

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

async def send_startup_message():
    """시작 메시지 전송"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"""🚀 <b>오더북 수집기 시작</b>

<b>시간:</b> {current_time}
<b>환경:</b> {'프로덕션' if os.getenv('CROSSKIMP_ENV') == 'production' else '개발'}
<b>상태:</b> 시스템 초기화 중...

⏳ 오더북 데이터 수집을 시작합니다. 모든 거래소 연결이 완료되면 알려드립니다.
"""
    # 비동기 태스크로 실행하여 초기화 과정을 블로킹하지 않도록 함
    asyncio.create_task(send_telegram_message(message))

async def send_shutdown_message():
    """종료 메시지 전송"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"""🔴 <b>오더북 수집기 종료</b>

<b>시간:</b> {current_time}
<b>상태:</b> 안전하게 종료되었습니다

📊 오더북 데이터 수집이 중단되었습니다. 
⚙️ 모든 리소스가 정상적으로 정리되었습니다.
"""
    await send_telegram_message(message)

async def shutdown():
    """종료 처리"""
    try:
        logger.info(f"{LOG_SYSTEM} 프로그램 종료 처리 시작")
        
        # 텔레그램 종료 메시지 전송 (await로 완료될 때까지 기다림)
        await send_shutdown_message()
        
        # 메시지가 실제로 전송될 시간을 추가로 확보
        await asyncio.sleep(1.5)
        
        logger.info(f"{LOG_SYSTEM} 프로그램 종료 처리 완료")
        
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 종료 처리 중 오류 발생: {str(e)}")
        
    finally:
        # 로거 종료
        logging.shutdown()

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
        
        # 시그널 핸들러 설정
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"{LOG_SYSTEM} 오더북 수집기 초기화 완료")
    
    def _signal_handler(self, sig, frame):
        """시그널 핸들러"""
        logger.info(f"{LOG_SYSTEM} 종료 시그널 수신: {sig}")
        if not self.stop_event.is_set():
            self.stop_event.set()
        else:
            logger.info(f"{LOG_SYSTEM} 강제 종료")
            sys.exit(1)
    
    async def run(self):
        """실행"""
        try:
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
            
            # 종료 이벤트 대기
            logger.info(f"{LOG_SYSTEM} 프로그램을 종료하려면 Ctrl+C를 누르세요")
            await self.stop_event.wait()
            
            # 종료 처리
            await self._stop_order_managers()
            
            # USDT/KRW 모니터 종료
            if self.usdtkrw_monitor:
                await self.usdtkrw_monitor.stop()
                logger.info(f"{LOG_SYSTEM} USDT/KRW 가격 모니터링 종료")
            
            # 태스크 취소
            usdtkrw_task.cancel()
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 실행 중 오류 발생: {str(e)}")
            await self._stop_order_managers()
            
            # USDT/KRW 모니터 종료
            if self.usdtkrw_monitor:
                await self.usdtkrw_monitor.stop()
    
    async def _start_order_managers(self, filtered_data):
        """
        OrderManager 초기화 및 시작
        
        Args:
            filtered_data: 필터링된 심볼 데이터
        """
        for exchange, symbols in filtered_data.items():
            if not symbols:
                logger.info(f"{EXCHANGE_NAMES_KR[exchange]} 심볼이 없어 OrderManager를 초기화하지 않습니다.")
                continue
            
            # OrderManager 생성
            manager = create_order_manager(exchange, self.settings)
            if not manager:
                logger.error(f"{EXCHANGE_NAMES_KR[exchange]} OrderManager 생성 실패")
                continue
            
            # 초기화 및 시작
            await manager.initialize()
            await manager.start(symbols)
            
            # OrderManager 저장
            self.order_managers[exchange] = manager
            
            logger.info(f"{EXCHANGE_NAMES_KR[exchange]} OrderManager 시작 완료")
        
        logger.info(f"{LOG_SYSTEM} 오더북 수집기 시작 완료")
    
    async def _stop_order_managers(self):
        """OrderManager 종료"""
        for exchange, manager in self.order_managers.items():
            await manager.stop()
            logger.info(f"{EXCHANGE_NAMES_KR[exchange]} OrderManager 중지 완료")
        
        self.order_managers.clear()
        logger.info(f"{LOG_SYSTEM} 오더북 수집기 중지 완료")

async def async_main():
    """비동기 메인 함수"""
    try:
        # 설정 로드
        logger.info(f"{LOG_SYSTEM} 설정 로드 시작")
        start_time = time.time()
        settings = get_settings()
        elapsed = time.time() - start_time
        logger.info(f"{LOG_SYSTEM} 설정 로드 완료 (소요 시간: {elapsed:.3f}초)")
        
        # 텔레그램 시작 메시지 전송
        await send_startup_message()
        
        # 오더북 수집기 생성 및 실행
        collector = OrderbookCollector(settings)
        await collector.run()
        
    except KeyboardInterrupt:
        logger.info(f"{LOG_SYSTEM} 키보드 인터럽트 수신")
        
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 메인 함수 오류: {str(e)}")
        
    finally:
        # 종료 처리
        await shutdown()

def main():
    """동기 메인 함수"""
    try:
        # 직접 실행 시에만 경고 메시지 출력
        if __name__ == "__main__":
            if not os.getenv("CROSSKIMP_ENV"):
                logger.warning(f"{LOG_SYSTEM} 개발 환경에서 실행 중입니다. 배포 환경에서는 'python -m crosskimp.ob_collector.main' 명령어를 사용하세요.")
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info(f"{LOG_SYSTEM} 키보드 인터럽트 감지")
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 예상치 못한 오류 발생: {e}", exc_info=True)

if __name__ == "__main__":
    main()