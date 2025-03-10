# file: main.py

import os, asyncio, time
from datetime import datetime, timedelta
from typing import Tuple, Dict, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, EXCHANGE_LOGGER_MAP
from crosskimp.ob_collector.core.aggregator import Aggregator
from crosskimp.ob_collector.core.ws_usdtkrw import WsUsdtKrwMonitor
from crosskimp.ob_collector.utils.config.config_loader import get_settings, initialize_config, add_config_observer, shutdown_config
from crosskimp.ob_collector.utils.config.constants import LOG_SYSTEM, WEBSOCKET_CONFIG, LOAD_TIMEOUT, SAVE_TIMEOUT
from crosskimp.ob_collector.server.backend.monitor_server import MonitoringServer
from crosskimp.ob_collector.orderbook.websocket.base_ws_manager import WebsocketManager

from crosskimp.telegrambot.telegram_notification import send_telegram_message, send_system_status, send_market_status, send_error
from crosskimp.telegrambot.bot_constants import MessageType, TELEGRAM_START_MESSAGE, TELEGRAM_STOP_MESSAGE

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# 환경 변수에서 프론트엔드 URL 가져오기
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

async def websocket_callback(exchange_name: str, data: dict):
    """웹소켓 메시지 수신 콜백"""
    try:
        sym = data.get("symbol")
        if sym:
            exchange_logger = EXCHANGE_LOGGER_MAP.get(exchange_name.lower(), logger)
            # 여기서 필요한 로깅 작업 수행
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 콜백 오류: {e}")

async def daily_reset_loop(aggregator, ws_manager, settings):
    """매일 자정에 웹소켓 재시작 및 심볼 필터링 재실행"""
    while True:
        try:
            now = datetime.now()
            tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            wait_seconds = (tomorrow - now).total_seconds()
            logger.info(f"{LOG_SYSTEM} 다음 재시작까지 {wait_seconds:.0f}초 대기")
            await asyncio.sleep(wait_seconds)

            logger.info(f"{LOG_SYSTEM} 자정 도달 → 재시작 시작")
            async with asyncio.timeout(300):  # 5분 타임아웃
                await ws_manager.shutdown()
                logger.info(f"{LOG_SYSTEM} 웹소켓 종료 완료")
                filtered_data = await aggregator.run_filtering()
                logger.info(f"{LOG_SYSTEM} 필터링된 심볼: {filtered_data}")
                await ws_manager.start_all_websockets(filtered_data)
                logger.info(f"{LOG_SYSTEM} 웹소켓 재시작 완료")
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 재시작 중 오류 발생: {e}")
            await asyncio.sleep(60)

async def on_settings_changed(new_settings: dict):
    """설정 파일 변경 감지 콜백"""
    logger.info(f"{LOG_SYSTEM} 설정 변경 감지")

async def initialize_system() -> Tuple[Dict, Aggregator, WebsocketManager, WsUsdtKrwMonitor]:
    """시스템 초기화"""
    try:
        # 설정 초기화 및 로드
        await initialize_config()
        settings = get_settings()
        if not settings:
            logger.error(f"{LOG_SYSTEM} 설정 로드 실패")
            return

        # 텔레그램 시작 메시지 전송 (비동기 태스크로 실행)
        logger.info(f"{LOG_SYSTEM} 텔레그램 시작 메시지 전송 시작")
        # 비동기 태스크로 실행하여 초기화 과정을 블로킹하지 않도록 함
        asyncio.create_task(send_telegram_message(settings, MessageType.STARTUP, TELEGRAM_START_MESSAGE))
        logger.info(f"{LOG_SYSTEM} 텔레그램 시작 메시지 전송 태스크 생성 완료")

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
        
        # USDT/KRW 모니터 초기화 - 비동기 태스크로 실행
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
        usdt_monitor_task = asyncio.create_task(usdt_monitor.start())
        logger.info(f"{LOG_SYSTEM} USDT/KRW 모니터링 태스크 시작됨")
        
        logger.info(f"{LOG_SYSTEM} 초기 웹소켓 연결 완료")
        
        return settings, aggregator, ws_manager, usdt_monitor

    except Exception as e:
        # 에러 메시지 전송
        await send_error(settings, "OrderBook Collector", f"초기화 중 오류 발생: {str(e)}")
        raise

async def cleanup(settings: Optional[Dict] = None, aggregator: Optional[Aggregator] = None, 
                 ws_manager: Optional[WebsocketManager] = None, 
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
    try:
        # 다음 자정 시간 계산
        now = datetime.now()
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        next_restart = tomorrow.timestamp()
        
        # 남은 시간 계산
        remaining = int(next_restart - time.time())
        hours = remaining // 3600
        minutes = (remaining % 3600) // 60
        seconds = remaining % 60
        logger.info(f"{LOG_SYSTEM} 프로그램 시작 | 다음 재시작까지 {hours:02d}:{minutes:02d}:{seconds:02d} 남음")

        # 설정 초기화 및 로드
        await initialize_config()
        settings = get_settings()
        if not settings:
            logger.error(f"{LOG_SYSTEM} 설정 로드 실패")
            return

        # 텔레그램 시작 메시지 전송 (비동기 태스크로 실행)
        logger.info(f"{LOG_SYSTEM} 텔레그램 시작 메시지 전송 시작")
        # 비동기 태스크로 실행하여 초기화 과정을 블로킹하지 않도록 함
        asyncio.create_task(send_telegram_message(settings, MessageType.STARTUP, TELEGRAM_START_MESSAGE))
        logger.info(f"{LOG_SYSTEM} 텔레그램 시작 메시지 전송 태스크 생성 완료")

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
        
        # USDT/KRW 모니터 초기화 - 비동기 태스크로 실행
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
        usdt_monitor_task = asyncio.create_task(usdt_monitor.start())
        logger.info(f"{LOG_SYSTEM} USDT/KRW 모니터링 태스크 시작됨")
        
        logger.info(f"{LOG_SYSTEM} 초기 웹소켓 연결 완료")
        
        # 백그라운드 태스크 시작
        background_tasks = [
            asyncio.create_task(check_restart(next_restart)),
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
    finally:
        if 'ws_manager' in locals():
            await ws_manager.shutdown()
        if 'usdt_monitor' in locals():
            await usdt_monitor.stop()
        logger.info(f"{LOG_SYSTEM} 프로그램 종료 완료")

async def check_restart(next_restart: float):
    """재시작 시간 체크"""
    try:
        while True:
            now = time.time()
            if now >= next_restart:
                logger.info(f"{LOG_SYSTEM} 예정된 재시작 시간 도달")
                os._exit(0)  # 강제 재시작
            
            remaining = int(next_restart - now)
            hours = remaining // 3600
            minutes = (remaining % 3600) // 60
            seconds = remaining % 60
            
            logger.info(f"{LOG_SYSTEM} 다음 재시작까지 {hours:02d}:{minutes:02d}:{seconds:02d} 남음")
            await asyncio.sleep(60)  # 1분마다 체크
            
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 재시작 체크 중 오류: {str(e)}", exc_info=True)

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