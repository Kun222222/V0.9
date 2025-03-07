# file: main.py

import os
import sys
import time
import signal
import asyncio
import subprocess
import webbrowser
from datetime import datetime, timedelta
from typing import Tuple, Dict, Optional

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.."))

from server.backend.monitor_server import MonitoringServer
from orderbook.websocket.websocket_manager import WebsocketManager, EXCHANGE_LOGGER_MAP
from config.config_loader import get_settings, initialize_config, add_config_observer, shutdown_config
from utils.logging.logger import unified_logger
from core.aggregator import Aggregator
from core.ws_usdtkrw import WsUsdtKrwMonitor
from crosskimp.telegrambot.notification.telegram_bot import (
    send_telegram_message,
    MessageType,
    send_system_status,
    send_market_status,
    send_error
)

FRONTEND_URL = "http://localhost:3000"

# 시작 메시지 상수 정의
TELEGRAM_START_MESSAGE = {
    "component": "OrderBook Collector",
    "status": "시스템 초기화 시작"
}

# 종료 메시지 상수 정의
TELEGRAM_STOP_MESSAGE = {
    "component": "OrderBook Collector",
    "reason": "사용자 요청으로 인한 종료"
}

async def websocket_callback(exchange_name: str, data: dict):
    """웹소켓 메시지 수신 콜백"""
    try:
        sym = data.get("symbol")
        if sym:
            logger = EXCHANGE_LOGGER_MAP.get(exchange_name.lower(), unified_logger)
    except Exception as e:
        unified_logger.error(f"[Callback] 오류: {e}")

async def daily_reset_loop(aggregator, ws_manager, settings):
    """매일 자정에 웹소켓 재시작 및 심볼 필터링 재실행"""
    while True:
        try:
            now = datetime.now()
            tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            wait_seconds = (tomorrow - now).total_seconds()
            unified_logger.info(f"[DailyReset] 다음 재시작까지 {wait_seconds:.0f}초 대기")
            await asyncio.sleep(wait_seconds)

            unified_logger.info("[DailyReset] 자정 도달 → 재시작 시작")
            async with asyncio.timeout(300):  # 5분 타임아웃
                await ws_manager.shutdown()
                unified_logger.info("[DailyReset] 웹소켓 종료 완료")
                filtered_data = await aggregator.run_filtering()
                unified_logger.info(f"[DailyReset] 필터링된 심볼: {filtered_data}")
                await ws_manager.start_all_websockets(filtered_data)
                unified_logger.info("[DailyReset] 웹소켓 재시작 완료")
        except Exception as e:
            unified_logger.error(f"[DailyReset] 오류 발생: {e}")
            await asyncio.sleep(60)

async def on_settings_changed(new_settings: dict):
    """설정 파일 변경 감지 콜백"""
    unified_logger.info("[Config] 설정 변경 감지")

async def initialize_system() -> Tuple[Dict, Aggregator, WebsocketManager, WsUsdtKrwMonitor]:
    """시스템 초기화"""
    try:
        # 설정 초기화 및 로드
        await initialize_config()
        settings = get_settings()
        if not settings:
            raise ValueError("설정을 불러올 수 없습니다.")

        # 텔레그램 시작 메시지 전송
        await send_telegram_message(settings, MessageType.STARTUP, TELEGRAM_START_MESSAGE)

        # Aggregator 및 WebsocketManager 초기화
        aggregator = Aggregator(settings)
        ws_manager = WebsocketManager(settings)
        ws_manager.register_callback(websocket_callback)
        
        # USDT/KRW 모니터 초기화 (독립적으로 실행)
        usdt_monitor = WsUsdtKrwMonitor()
        usdt_monitor.add_price_callback(ws_manager.update_usdt_rate)  # 가격 변경 콜백 등록
        
        # 설정 변경 옵저버 등록
        add_config_observer(on_settings_changed)
        
        # 초기 심볼 필터링 및 웹소켓 연결
        filtered_data = await aggregator.run_filtering()
        await ws_manager.start_all_websockets(filtered_data)
        unified_logger.info("[Main] 초기 웹소켓 연결 완료")
        
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
        unified_logger.info("[Main] 프로그램 종료")

    except Exception as e:
        # 에러 메시지 전송 (설정이 있는 경우에만)
        if settings:
            await send_error(settings, "OrderBook Collector", f"종료 처리 중 오류 발생: {str(e)}")
        unified_logger.error(f"[Main] 종료 처리 중 오류 발생: {e}")

async def amain():
    """비동기 메인 함수"""
    settings = None
    aggregator = None
    ws_manager = None
    usdt_monitor = None
    
    try:
        settings, aggregator, ws_manager, usdt_monitor = await initialize_system()
        
        # 백그라운드 태스크 시작
        background_tasks = [
            asyncio.create_task(ws_manager.process_queue()),
            asyncio.create_task(ws_manager.check_message_delays()),
            asyncio.create_task(daily_reset_loop(aggregator, ws_manager, settings)),
            asyncio.create_task(usdt_monitor.start())  # USDT/KRW 모니터링 시작
        ]
        
        unified_logger.info("[Main] 백그라운드 태스크 시작됨")
        unified_logger.info("[Main] 프로그램을 종료하려면 Ctrl+C를 누르세요")
        
        # 태스크 실행 대기
        await asyncio.gather(*background_tasks)
        
    except KeyboardInterrupt:
        unified_logger.info("[Main] 키보드 인터럽트 감지")
    except Exception as e:
        unified_logger.error(f"[Main] 예상치 못한 오류 발생: {e}", exc_info=True)
    finally:
        await cleanup(settings, aggregator, ws_manager, usdt_monitor)

def main():
    """동기 메인 함수"""
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        unified_logger.info("[Main] 키보드 인터럽트 감지")
    except Exception as e:
        unified_logger.error(f"[Main] 예상치 못한 오류 발생: {e}", exc_info=True)

if __name__ == "__main__":
    main()