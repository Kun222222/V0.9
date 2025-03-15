#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
바이낸스 선물 웹소켓 재연결 테스트 스크립트

이 스크립트는 바이낸스 선물 웹소켓의 재연결 기능을 테스트합니다.
- 웹소켓 연결 수립
- 임의로 연결 끊기 (강제 종료)
- 재연결 확인
- 메시지 수신 확인
- 재연결 후 스냅샷 수신 확인

사용법:
    python test_binance_f_reconnect.py
"""

import asyncio
import json
import logging
import random
import signal
import sys
import time
from typing import Dict, List, Optional, Set

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("binance_future_reconnect_test")

# 바이낸스 선물 웹소켓 클래스 임포트
from crosskimp.ob_collector.orderbook.websocket.binance_f_ws import BinanceFutureWebsocket
from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants import Exchange, EXCHANGE_NAMES_KR

# 테스트 설정
TEST_SYMBOLS = ["BTC", "ETH"]  # 테스트할 심볼 목록
RECONNECT_INTERVAL_MIN = 5  # 최소 재연결 간격 (초)
RECONNECT_INTERVAL_MAX = 10  # 최대 재연결 간격 (초)
TEST_DURATION = 30  # 테스트 지속 시간 (초)
FORCE_DISCONNECT_METHODS = ["close", "error"]  # 강제 연결 끊기 방법
MAX_RECONNECT_TESTS = 2  # 최대 테스트 횟수

class BinanceFutureReconnectTester:
    """바이낸스 선물 웹소켓 재연결 테스트 클래스"""
    
    def __init__(self):
        """초기화"""
        self.logger = logger
        
        # 웹소켓 및 큐 초기화
        self.binance_ws = None
        self.message_queue = asyncio.Queue()
        self.stop_event = asyncio.Event()
        
        # 설정
        self.settings = {
            "depth": 100
        }
        
        # 거래소 한글 이름
        self.exchange_name_kr = EXCHANGE_NAMES_KR[Exchange.BINANCE_FUTURE.value]
        
        # 테스트 통계
        self.stats = {
            "test_start_time": 0,
            "connections": 0,
            "reconnections": 0,
            "disconnections": 0,
            "messages_received": 0,
            "errors": 0,
            "last_message_time": 0,
            "last_connection_time": 0,
            "last_disconnection_time": 0,
            "last_error_time": 0,
            "last_error_message": "",
            "forced_disconnects": 0,
            "pong_received": 0
        }
        
        # 스냅샷 추적
        self.snapshot_tracking = {
            "symbols_with_snapshot": set(),  # 스냅샷을 받은 심볼 목록
            "last_snapshot_time": 0,  # 마지막 스냅샷 수신 시간
            "total_snapshots": 0,  # 총 스냅샷 수신 횟수
            "reconnection_snapshots": 0,  # 재연결 후 스냅샷 수신 횟수
            "reconnection_pending": False  # 재연결 후 스냅샷 수신 대기 중 여부
        }
        
        # 시그널 핸들러 등록
        signal.signal(signal.SIGINT, handle_sigint)
    
    def connection_status_callback(self, exchange: str, status: str):
        """연결 상태 콜백"""
        now = time.time()
        
        if status == "connect_attempt":
            self.logger.info(f"{self.exchange_name_kr} 연결 시도 중...")
            
        elif status == "connect":
            self.logger.info(f"{self.exchange_name_kr} 연결 성공")
            self.stats["connections"] += 1
            self.stats["last_connection_time"] = now
            
            # 첫 연결이 아닌 경우 재연결로 간주
            if self.stats["connections"] > 1 or self.stats["forced_disconnects"] > 0:
                self.stats["reconnections"] += 1
                self.logger.info(f"{self.exchange_name_kr} 재연결 성공 ({self.stats['reconnections']}번째)")
                
                # 재연결 후 스냅샷 수신 대기 상태로 설정
                self.snapshot_tracking["reconnection_pending"] = True
                self.snapshot_tracking["symbols_with_snapshot"].clear()
            else:
                self.logger.info(f"{self.exchange_name_kr} 초기 연결 성공")
                
        elif status == "disconnect":
            self.logger.info(f"{self.exchange_name_kr} 연결 끊김")
            self.stats["disconnections"] += 1
            self.stats["last_disconnection_time"] = now
            self.logger.info(f"{self.exchange_name_kr} 연결 끊김 ({self.stats['disconnections']}번째)")
            
            # 재연결 대기 상태로 설정
            if self.stats["reconnection_pending"]:
                self.logger.warning(f"{self.exchange_name_kr} 이미 재연결 대기 중입니다.")
            else:
                self.stats["reconnection_pending"] = True
                self.logger.info(f"{self.exchange_name_kr} 재연결 대기 상태로 설정")
            
        elif status == "message":
            # 메시지 수신 시간 업데이트
            self.stats["messages_received"] += 1
            self.stats["last_message_time"] = now
            
        elif status == "error":
            self.logger.error(f"{self.exchange_name_kr} 오류 발생")
            self.stats["errors"] += 1
            self.stats["last_error_time"] = now
            
        elif status == "snapshot_request":
            self.logger.info(f"{self.exchange_name_kr} 스냅샷 요청 중...")
            
        elif status == "snapshot_parsed":
            # 스냅샷 수신 시간 업데이트
            self.snapshot_tracking["last_snapshot_time"] = now
            self.snapshot_tracking["total_snapshots"] += 1
            
            # 재연결 후 스냅샷 수신인 경우
            if self.snapshot_tracking["reconnection_pending"]:
                self.snapshot_tracking["reconnection_snapshots"] += 1
                self.logger.info(f"{self.exchange_name_kr} 재연결 후 스냅샷 수신 ({self.snapshot_tracking['reconnection_snapshots']}번째)")
    
    async def process_queue(self):
        """메시지 큐 처리"""
        while not self.stop_event.is_set():
            try:
                # 큐에서 메시지 가져오기 (0.1초 타임아웃)
                try:
                    message = await asyncio.wait_for(self.message_queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue
                
                # 메시지 처리
                self.stats["messages_received"] += 1
                
                # 메시지 타입 확인
                if isinstance(message, dict):
                    msg_type = message.get("type", "unknown")
                    symbol = message.get("symbol", "unknown")
                    
                    # 스냅샷 메시지 처리
                    if msg_type == "snapshot":
                        self.snapshot_tracking["symbols_with_snapshot"].add(symbol)
                        self.snapshot_tracking["last_snapshot_time"] = time.time()
                        
                        # 재연결 후 스냅샷 수신인 경우
                        if self.snapshot_tracking["reconnection_pending"] and self.stats["reconnections"] > 0:
                            self.logger.info(f"{self.exchange_name_kr} 재연결 후 {symbol} 스냅샷 수신")
                            
                            # 모든 심볼에 대해 스냅샷을 받았는지 확인
                            if len(self.snapshot_tracking["symbols_with_snapshot"]) == len(TEST_SYMBOLS):
                                self.snapshot_tracking["reconnection_pending"] = False
                                self.logger.info(f"{self.exchange_name_kr} 모든 심볼({len(TEST_SYMBOLS)}개)에 대한 스냅샷 수신 완료")
                
            except Exception as e:
                self.logger.error(f"{self.exchange_name_kr} 메시지 처리 중 오류: {e}")
                self.stats["errors"] += 1
                self.stats["last_error_time"] = time.time()
                self.stats["last_error_message"] = str(e)
    
    async def force_disconnect(self):
        """강제로 연결 끊기"""
        if not self.binance_ws or not self.binance_ws.is_connected:
            self.logger.warning(f"{self.exchange_name_kr} 웹소켓이 연결되어 있지 않습니다.")
            return
        
        # 연결 끊기 방법 선택
        method = random.choice(FORCE_DISCONNECT_METHODS)
        self.logger.info(f"{self.exchange_name_kr} 강제 연결 끊기 시도 (방법: {method})")
        
        try:
            if method == "close":
                # 정상적인 방법으로 연결 종료
                await self.binance_ws.ws.close()
                self.logger.info(f"{self.exchange_name_kr} 웹소켓 정상 종료")
                
            elif method == "error":
                # 웹소켓 객체를 None으로 설정하여 오류 발생
                self.binance_ws.ws = None
                self.logger.info(f"{self.exchange_name_kr} 웹소켓 객체를 None으로 설정하여 오류 발생시킴")
                
            self.stats["forced_disconnects"] += 1
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 강제 연결 끊기 실패: {e}")
    
    async def check_snapshot_recovery(self):
        """스냅샷 복구 확인"""
        while not self.stop_event.is_set():
            await asyncio.sleep(1)  # 1초마다 확인
            
            # 재연결 후 스냅샷 복구 확인
            if self.stats["reconnections"] > 0 and self.snapshot_tracking["reconnection_pending"]:
                current_snapshot_count = len(self.snapshot_tracking["symbols_with_snapshot"])
                expected_snapshot_count = len(TEST_SYMBOLS)
                
                # 모든 심볼에 대해 스냅샷이 수신되었는지 확인
                if current_snapshot_count == expected_snapshot_count:
                    time_since_reconnect = time.time() - self.stats["last_connection_time"]
                    self.logger.info(
                        f"{self.exchange_name_kr} 스냅샷 복구 완료: {current_snapshot_count}/{expected_snapshot_count} 심볼 "
                        f"(재연결 후 {time_since_reconnect:.1f}초)"
                    )
                    self.snapshot_tracking["reconnection_pending"] = False
                elif current_snapshot_count > 0:
                    self.logger.info(
                        f"{self.exchange_name_kr} 스냅샷 복구 진행 중: {current_snapshot_count}/{expected_snapshot_count} 심볼"
                    )
    
    async def print_stats(self):
        """통계 출력"""
        while not self.stop_event.is_set():
            await asyncio.sleep(5)  # 5초마다 통계 출력
            
            now = time.time()
            test_duration = now - self.stats["test_start_time"]
            
            # 마지막 메시지 수신 이후 경과 시간
            last_msg_elapsed = "N/A"
            if self.stats["last_message_time"] > 0:
                last_msg_elapsed = f"{now - self.stats['last_message_time']:.1f}초"
            
            # 마지막 연결 이후 경과 시간
            last_conn_elapsed = "N/A"
            if self.stats["last_connection_time"] > 0:
                last_conn_elapsed = f"{now - self.stats['last_connection_time']:.1f}초"
            
            # 통계 출력
            self.logger.info(
                f"{self.exchange_name_kr} 테스트 진행 중 ({test_duration:.1f}초): "
                f"연결 {self.stats['connections']}회, "
                f"재연결 {self.stats['reconnections']}회, "
                f"강제 종료 {self.stats['forced_disconnects']}회, "
                f"메시지 {self.stats['messages_received']}개, "
                f"스냅샷 {self.snapshot_tracking['total_snapshots']}개, "
                f"재연결 후 스냅샷 {self.snapshot_tracking['reconnection_snapshots']}개"
            )
            self.logger.info(
                f"{self.exchange_name_kr} 마지막 메시지: {last_msg_elapsed} 전, "
                f"{self.exchange_name_kr} 마지막 연결: {last_conn_elapsed} 전"
            )
    
    async def run_test(self):
        """테스트 실행"""
        self.logger.info(f"{self.exchange_name_kr} 웹소켓 재연결 테스트 시작")
        
        # 테스트 시작 시간 기록
        self.stats["test_start_time"] = time.time()
        
        try:
            # 바이낸스 선물 웹소켓 인스턴스 생성
            self.binance_ws = BinanceFutureWebsocket(self.settings)
            
            # 콜백 및 큐 설정
            self.binance_ws.connection_status_callback = self.connection_status_callback
            self.binance_ws.set_output_queue(self.message_queue)
            
            # 테스트 작업 시작
            tasks = [
                asyncio.create_task(self.binance_ws.start({"binancefuture": TEST_SYMBOLS})),
                asyncio.create_task(self.process_queue()),
                asyncio.create_task(self.force_disconnect()),
                asyncio.create_task(self.check_snapshot_recovery()),
                asyncio.create_task(self.print_stats())
            ]
            
            # 테스트 지속 시간 설정
            if TEST_DURATION > 0:
                self.logger.info(f"{self.exchange_name_kr} 테스트는 {TEST_DURATION}초 동안 실행됩니다.")
                try:
                    await asyncio.sleep(TEST_DURATION)
                except asyncio.CancelledError:
                    self.logger.info(f"{self.exchange_name_kr} 테스트가 취소되었습니다.")
                finally:
                    self.logger.info(f"{self.exchange_name_kr} 테스트 시간이 종료되어 중지합니다.")
                    self.stop_event.set()
            
            # 모든 작업이 완료될 때까지 대기
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as gather_e:
                self.logger.error(f"{self.exchange_name_kr} 작업 실행 중 오류: {gather_e}")
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 테스트 실행 중 오류: {e}")
        finally:
            # 테스트 종료 처리
            self.logger.info(f"{self.exchange_name_kr} 테스트 종료 처리 중...")
            self.stop_event.set()
            
            if self.binance_ws:
                try:
                    await self.binance_ws.stop()
                    self.logger.info(f"{self.exchange_name_kr} 웹소켓 정상 종료")
                except Exception as stop_e:
                    self.logger.error(f"{self.exchange_name_kr} 웹소켓 종료 중 오류: {stop_e}")
            
            # 최종 통계 출력
            test_duration = time.time() - self.stats["test_start_time"]
            self.logger.info(
                f"{self.exchange_name_kr} 테스트 종료 (총 {test_duration:.1f}초): "
                f"{self.exchange_name_kr} 연결 {self.stats['connections']}회, "
                f"{self.exchange_name_kr} 재연결 {self.stats['reconnections']}회, "
                f"{self.exchange_name_kr} 강제 종료 {self.stats['forced_disconnects']}회, "
                f"{self.exchange_name_kr} 메시지 {self.stats['messages_received']}개, "
                f"{self.exchange_name_kr} 스냅샷 {self.snapshot_tracking['total_snapshots']}개, "
                f"{self.exchange_name_kr} 재연결 후 스냅샷 {self.snapshot_tracking['reconnection_snapshots']}개"
            )
            
            # 테스트 결과 평가
            if self.stats["reconnections"] > 0 and self.snapshot_tracking["reconnection_snapshots"] > 0:
                self.logger.info(f"{self.exchange_name_kr} 테스트 결과: 성공 (재연결 후 스냅샷 수신 확인)")
            else:
                self.logger.warning(f"{self.exchange_name_kr} 테스트 결과: 실패 (재연결 후 스냅샷 수신 실패)")


def handle_sigint(signum, frame):
    """SIGINT 핸들러"""
    logger.info(f"{EXCHANGE_NAMES_KR[Exchange.BINANCE_FUTURE.value]} 테스트를 종료합니다...")
    # 전역 변수로 stop_event에 접근할 수 없으므로 시그널 핸들러에서는 아무 작업도 수행하지 않음


if __name__ == "__main__":
    # 테스트 실행
    tester = BinanceFutureReconnectTester()
    asyncio.run(tester.run_test()) 