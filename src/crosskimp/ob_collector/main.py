# file: main.py

import os, asyncio, time
import logging
from datetime import datetime, timedelta
from typing import Tuple, Dict, Optional
import signal
import sys

from crosskimp.logger.logger import get_unified_logger, get_logger
from crosskimp.ob_collector.core.aggregator import Aggregator
from crosskimp.ob_collector.core.ws_usdtkrw import WsUsdtKrwMonitor
from crosskimp.config.config_loader import get_settings, initialize_config, add_config_observer, shutdown_config
from crosskimp.config.constants import LOG_SYSTEM, WEBSOCKET_CONFIG, LOAD_TIMEOUT, SAVE_TIMEOUT, Exchange, EXCHANGE_NAMES_KR
from crosskimp.ob_collector.orderbook.order_manager import OrderManager, create_order_manager

from crosskimp.telegrambot.telegram_notification import send_telegram_message, send_error_notification, send_system_status_notification
from crosskimp.telegrambot.bot_constants import MessageType, TELEGRAM_START_MESSAGE, TELEGRAM_STOP_MESSAGE

# 시스템 관리 모듈 가져오기
from crosskimp.system_manager.scheduler import calculate_next_midnight, format_remaining_time, schedule_task

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# 환경에 따른 로깅 설정
# 디버깅을 위해 항상 DEBUG 레벨로 설정
logger.setLevel(logging.DEBUG)
logger.info(f"{LOG_SYSTEM} 디버깅을 위해 로그 레벨을 DEBUG로 설정했습니다.")

if os.getenv("CROSSKIMP_ENV") == "production":
    # 프로덕션 환경 로그
    logger.info(f"{LOG_SYSTEM} 배포 환경에서 실행 중입니다.")
else:
    # 개발 환경 로그
    logger.warning(f"{LOG_SYSTEM} 개발 환경에서 실행 중입니다. 배포 환경에서는 'CROSSKIMP_ENV=production' 환경 변수를 설정하세요.")

# 거래소별 로거 초기화
def initialize_exchange_loggers():
    """거래소별 로거 초기화"""
    # 거래소별 로거를 생성하지 않도록 수정
    # 통합 로거만 사용
    logger.debug(f"{LOG_SYSTEM} 거래소별 로거 초기화 생략 (통합 로거만 사용)")

# 거래소 로거 초기화 실행
initialize_exchange_loggers()

async def websocket_callback(exchange_name: str, data: dict):
    """웹소켓 메시지 수신 콜백"""
    try:
        sym = data.get("symbol")
        if sym:
            # DEBUG 레벨이 필요한 경우에만 로깅 (개발 환경에서만)
            if logger.level <= logging.DEBUG:
                logger.debug(f"{exchange_name} 메시지 수신: {sym}")
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 콜백 오류: {e}")

async def reset_websockets(aggregator, settings):
    """웹소켓 재시작 및 심볼 필터링 재실행"""
    try:
        logger.info(f"{LOG_SYSTEM} 웹소켓 재시작 시작")
        async with asyncio.timeout(300):  # 5분 타임아웃
            
            # 심볼 필터링 재실행
            filtered_data = await aggregator.run_filtering()
            logger.info(f"{LOG_SYSTEM} 필터링된 심볼: {filtered_data}")
            
            # 다음 자정에 다시 실행되도록 예약
            tomorrow = calculate_next_midnight()
            await schedule_task(
                "daily_websocket_reset",
                tomorrow,
                reset_websockets,
                aggregator, settings
            )
            logger.info(f"{LOG_SYSTEM} 다음 웹소켓 재시작 예약됨: {tomorrow.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 웹소켓 재시작 중 오류 발생: {e}")
        # 오류 발생 시 1시간 후 다시 시도
        next_try = datetime.now() + timedelta(hours=1)
        await schedule_task(
            "retry_websocket_reset",
            next_try,
            reset_websockets,
            aggregator, settings
        )
        logger.info(f"{LOG_SYSTEM} 웹소켓 재시작 재시도 예약됨: {next_try.strftime('%Y-%m-%d %H:%M:%S')}")

async def on_settings_changed(new_settings: dict):
    """설정 파일 변경 감지 콜백"""
    logger.info(f"{LOG_SYSTEM} 설정 변경 감지, 새 설정 적용 중...")
    # 여기에 설정 변경 시 필요한 작업 추가
    # 예: 웹소켓 재연결, 필터링 재실행 등

async def shutdown():
    """종료 처리"""
    try:
        logger.info(f"{LOG_SYSTEM} 프로그램 종료 처리 시작")
        
        # 텔레그램 종료 메시지 전송 (settings 파라미터 없이)
        await send_telegram_message(None, MessageType.SHUTDOWN, TELEGRAM_STOP_MESSAGE)
        
        logger.info(f"{LOG_SYSTEM} 프로그램 종료 처리 완료")
        
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 종료 처리 중 오류 발생: {str(e)}")
        
    finally:
        # 로거 종료
        logging.shutdown()

class OrderbookCollector:
    """
    오더북 수집기 클래스
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
    
    async def initialize(self):
        """초기화"""
        try:
            # Aggregator 초기화
            self.aggregator = Aggregator(self.settings)
            
            logger.info(f"{LOG_SYSTEM} 오더북 수집기 초기화 완료")
            return True
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 초기화 실패: {str(e)}")
            return False
    
    async def start(self):
        """시작"""
        try:
            # 심볼 필터링
            filtered_data = await self.aggregator.run_filtering()
            if not filtered_data:
                logger.error(f"{LOG_SYSTEM} 필터링된 심볼이 없습니다.")
                return False
            
            # 각 거래소별 OrderManager 초기화 및 시작
            for exchange, symbols in filtered_data.items():
                if not symbols:
                    logger.info(f"{exchange} 심볼이 없어 OrderManager를 초기화하지 않습니다.")
                    continue
                
                # OrderManager 생성
                manager = create_order_manager(exchange, self.settings)
                if not manager:
                    logger.error(f"{exchange} OrderManager 생성 실패")
                    continue
                
                # 초기화 및 시작
                await manager.initialize()
                await manager.start(symbols)
                
                # OrderManager 저장
                self.order_managers[exchange] = manager
                
                logger.info(f"{exchange} OrderManager 시작 완료")
            
            logger.info(f"{LOG_SYSTEM} 오더북 수집기 시작 완료")
            return True
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 시작 실패: {str(e)}")
            return False
    
    async def stop(self):
        """중지"""
        try:
            # 모든 OrderManager 중지
            for exchange, manager in self.order_managers.items():
                await manager.stop()
                logger.info(f"{exchange} OrderManager 중지 완료")
            
            self.order_managers.clear()
            logger.info(f"{LOG_SYSTEM} 오더북 수집기 중지 완료")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 중지 실패: {str(e)}")
    
    async def run(self):
        """실행"""
        try:
            # 초기화
            if not await self.initialize():
                return
            
            # 시작
            if not await self.start():
                await self.stop()
                return
            
            # 종료 이벤트 대기
            logger.info(f"{LOG_SYSTEM} 프로그램을 종료하려면 Ctrl+C를 누르세요")
            await self.stop_event.wait()
            
            # 중지
            await self.stop()
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 실행 중 오류 발생: {str(e)}")
            await self.stop()

async def async_main():
    """비동기 메인 함수"""
    try:
        # 설정 로드
        logger.info(f"{LOG_SYSTEM} 설정 로드 시작")
        start_time = time.time()
        settings = get_settings()
        elapsed = time.time() - start_time
        logger.info(f"{LOG_SYSTEM} 설정 로드 완료 (소요 시간: {elapsed:.3f}초)")
        
        # 텔레그램 시작 메시지 전송 (settings 파라미터 없이)
        asyncio.create_task(send_telegram_message(None, MessageType.STARTUP, TELEGRAM_START_MESSAGE))
        
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