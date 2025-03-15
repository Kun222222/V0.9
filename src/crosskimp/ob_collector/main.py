# file: main.py

import os, asyncio, time
import logging
from datetime import datetime, timedelta
from typing import Tuple, Dict, Optional

from crosskimp.logger.logger import get_unified_logger, get_logger
from crosskimp.ob_collector.core.aggregator import Aggregator
from crosskimp.ob_collector.core.ws_usdtkrw import WsUsdtKrwMonitor
from crosskimp.config.config_loader import get_settings, initialize_config, add_config_observer, shutdown_config
from crosskimp.config.constants import LOG_SYSTEM, WEBSOCKET_CONFIG, LOAD_TIMEOUT, SAVE_TIMEOUT, Exchange, EXCHANGE_NAMES_KR
from crosskimp.ob_collector.orderbook.websocket.base_ws_manager import WebsocketManager

from crosskimp.telegrambot.telegram_notification import send_telegram_message, send_system_status, send_market_status, send_error
from crosskimp.config.bot_constants import MessageType, TELEGRAM_START_MESSAGE, TELEGRAM_STOP_MESSAGE

# 시스템 관리 모듈 가져오기
from crosskimp.system_manager.scheduler import calculate_next_midnight, format_remaining_time, schedule_task

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# 환경에 따른 로깅 설정
if os.getenv("CROSSKIMP_ENV") == "production":
    logger.setLevel(logging.INFO)
    logger.info(f"{LOG_SYSTEM} 배포 환경에서 실행 중입니다.")
else:
    logger.setLevel(logging.DEBUG)
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

async def reset_websockets(aggregator, ws_manager, settings):
    """웹소켓 재시작 및 심볼 필터링 재실행"""
    try:
        logger.info(f"{LOG_SYSTEM} 웹소켓 재시작 시작")
        async with asyncio.timeout(300):  # 5분 타임아웃
            await ws_manager.shutdown()
            logger.info(f"{LOG_SYSTEM} 웹소켓 종료 완료")
            filtered_data = await aggregator.run_filtering()
            logger.info(f"{LOG_SYSTEM} 필터링된 심볼: {filtered_data}")
            await ws_manager.start_all_websockets(filtered_data)
            logger.info(f"{LOG_SYSTEM} 웹소켓 재시작 완료")
            
            # 다음 자정에 다시 실행되도록 예약
            tomorrow = calculate_next_midnight()
            await schedule_task(
                "daily_websocket_reset",
                tomorrow,
                reset_websockets,
                aggregator, ws_manager, settings
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
            aggregator, ws_manager, settings
        )
        logger.info(f"{LOG_SYSTEM} 웹소켓 재시작 재시도 예약됨: {next_try.strftime('%Y-%m-%d %H:%M:%S')}")

async def on_settings_changed(new_settings: dict):
    """설정 파일 변경 감지 콜백"""
    logger.info(f"{LOG_SYSTEM} 설정 변경 감지, 새 설정 적용 중...")
    # 여기에 설정 변경 시 필요한 작업 추가
    # 예: 웹소켓 재연결, 필터링 재실행 등

async def cleanup(settings: Optional[Dict] = None, ws_manager: Optional[WebsocketManager] = None, 
                 usdt_monitor: Optional[WsUsdtKrwMonitor] = None):
    """시스템 종료 처리"""
    try:
        # 텔레그램 종료 메시지 전송 (설정이 있는 경우에만)
        if settings:
            await send_telegram_message(settings, MessageType.SHUTDOWN, TELEGRAM_STOP_MESSAGE)
        
        # 종료 처리
        if ws_manager:
            await ws_manager.shutdown()
        if usdt_monitor:
            await usdt_monitor.stop()
        shutdown_config()
        logger.info(f"{LOG_SYSTEM} 프로그램 종료")

    except Exception as e:
        # 에러 메시지 전송 (설정이 있는 경우에만)
        if settings:
            await send_error(settings, "OrderBook Collector", f"종료 처리 중 오류 발생: {str(e)}")
        logger.error(f"{LOG_SYSTEM} 종료 처리 중 오류 발생: {e}")

async def async_main():
    """비동기 메인 함수"""
    settings = None
    ws_manager = None
    usdt_monitor = None
    aggregator = None
    
    try:
        # 다음 자정 시간 계산
        now = datetime.now()
        tomorrow = calculate_next_midnight()
        next_restart = tomorrow.timestamp()
        
        # 남은 시간 계산
        remaining_time = format_remaining_time(tomorrow)
        logger.info(f"{LOG_SYSTEM} 프로그램 시작 | 다음 재시작까지 {remaining_time} 남음")

        # 설정 초기화 및 로드
        await initialize_config()
        settings = get_settings()
        if not settings:
            logger.error(f"{LOG_SYSTEM} 설정 로드 실패")
            return

        # 텔레그램 시작 메시지 전송 (비동기 태스크로 실행)
        logger.info(f"{LOG_SYSTEM} 텔레그램 시작 메시지 전송 시작")
        asyncio.create_task(send_telegram_message(settings, MessageType.STARTUP, TELEGRAM_START_MESSAGE))
        
        # 웹소켓 매니저 초기화
        logger.info(f"{LOG_SYSTEM} 웹소켓 매니저 초기화 시작")
        start_time = time.time()
        ws_manager = WebsocketManager(settings)
        ws_manager.register_callback(websocket_callback)
        elapsed = time.time() - start_time
        logger.info(f"{LOG_SYSTEM} 웹소켓 매니저 초기화 완료 (소요 시간: {elapsed:.3f}초)")
        
        # Aggregator 초기화
        logger.info(f"{LOG_SYSTEM} Aggregator 초기화 시작")
        start_time = time.time()
        aggregator = Aggregator(settings)
        elapsed = time.time() - start_time
        logger.info(f"{LOG_SYSTEM} Aggregator 초기화 완료 (소요 시간: {elapsed:.3f}초)")
        
        # USDT/KRW 모니터 초기화
        logger.info(f"{LOG_SYSTEM} USDT/KRW 모니터 초기화 시작")
        usdt_monitor = WsUsdtKrwMonitor()
        usdt_monitor.add_price_callback(ws_manager.update_usdt_rate)
        ws_manager.usdt_monitor = usdt_monitor
        logger.info(f"{LOG_SYSTEM} USDT/KRW 모니터 초기화 완료")
        
        # 설정 변경 옵저버 등록
        add_config_observer(on_settings_changed)
        
        # 심볼 필터링
        logger.info(f"{LOG_SYSTEM} 심볼 필터링 시작")
        start_time = time.time()
        filtered_data = await aggregator.run_filtering()
        elapsed = time.time() - start_time
        logger.info(f"{LOG_SYSTEM} 심볼 필터링 완료 (소요 시간: {elapsed:.3f}초)")
        
        # 웹소켓 시작
        logger.info(f"{LOG_SYSTEM} 웹소켓 연결 시작")
        await ws_manager.start_all_websockets(filtered_data)
        
        # USDT/KRW 모니터링 시작 (비동기 태스크로 실행)
        logger.info(f"{LOG_SYSTEM} USDT/KRW 모니터링 시작")
        asyncio.create_task(usdt_monitor.start())
        
        # 다음 자정에 웹소켓 재시작 예약
        await schedule_task(
            "daily_websocket_reset",
            tomorrow,
            reset_websockets,
            aggregator, ws_manager, settings
        )
        logger.info(f"{LOG_SYSTEM} 웹소켓 재시작 예약됨: {tomorrow.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 백그라운드 태스크 시작
        background_tasks = [
            # 웹소켓 상태 확인 태스크
            asyncio.create_task(ws_manager.check_message_delays()),
            asyncio.create_task(ws_manager.check_alerts())
        ]
        logger.info(f"{LOG_SYSTEM} 백그라운드 태스크 시작됨")
        logger.info(f"{LOG_SYSTEM} 프로그램을 종료하려면 Ctrl+C를 누르세요")
        
        # 태스크 완료 대기
        await asyncio.gather(*background_tasks)
        
    except KeyboardInterrupt:
        logger.info(f"{LOG_SYSTEM} Ctrl+C 감지됨, 프로그램 종료 중...")
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 메인 루프 오류: {str(e)}", exc_info=True)
        if settings:
            await send_error(settings, "OrderBook Collector", f"실행 중 오류 발생: {str(e)}")
    finally:
        # 종료 처리 통합
        await cleanup(settings, ws_manager, usdt_monitor)

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