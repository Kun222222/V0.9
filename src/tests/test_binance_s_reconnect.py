#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
바이낸스 현물 웹소켓 재연결 테스트 스크립트

이 스크립트는 바이낸스 현물 웹소켓의 재연결 기능을 테스트합니다.
- 웹소켓 연결 수립
- 임의로 연결 끊기 (강제 종료)
- 재연결 확인
- 메시지 수신 확인
- 재연결 후 스냅샷 수신 확인

사용법:
    python test_binance_s_reconnect.py
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
logger = logging.getLogger("binance_reconnect_test")

# 바이낸스 웹소켓 클래스 임포트
from crosskimp.ob_collector.orderbook.websocket.binance_s_ws import BinanceSpotWebsocket
from crosskimp.logger.logger import get_unified_logger

# 테스트 설정
TEST_SYMBOLS = ["BTC", "ETH"]  # 테스트할 심볼 목록
RECONNECT_INTERVAL_MIN = 5  # 최소 재연결 간격 (초)
RECONNECT_INTERVAL_MAX = 10  # 최대 재연결 간격 (초)
TEST_DURATION = 30  # 테스트 지속 시간 (초)
FORCE_DISCONNECT_METHODS = ["close", "error"]  # 강제 연결 끊기 방법
MAX_RECONNECT_TESTS = 3  # 최대 테스트 횟수

class BinanceReconnectTester:
    """바이낸스 웹소켓 재연결 테스트 클래스"""
    
    def __init__(self):
        """초기화"""
        self.logger = logger
        self.binance_ws = None
        self.message_queue = asyncio.Queue()
        self.stop_event = asyncio.Event()
        
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
        
        # 설정
        self.settings = {
            "api_key": "",
            "api_secret": "",
            "connection": {
                "websocket": {
                    "depth_level": 100
                }
            }
        }
        
        # 시그널 핸들러 등록
        signal.signal(signal.SIGINT, handle_sigint)
    
    def connection_status_callback(self, exchange: str, status: str):
        """연결 상태 콜백"""
        now = time.time()
        
        if status == "connect_attempt":
            self.logger.info(f"연결 시도 중...")
            
        elif status == "connect":
            self.stats["connections"] += 1
            self.stats["last_connection_time"] = now
            
            # 첫 연결이 아닌 경우 재연결로 간주
            if self.stats["connections"] > 1:
                self.stats["reconnections"] += 1
                self.logger.info(f"재연결 성공 ({self.stats['reconnections']}번째)")
                
                # 재연결 후 스냅샷 수신 대기 상태로 설정
                self.snapshot_tracking["reconnection_pending"] = True
                self.snapshot_tracking["symbols_with_snapshot"].clear()
            else:
                self.logger.info("초기 연결 성공")
                
        elif status == "disconnect":
            self.stats["disconnections"] += 1
            self.stats["last_disconnection_time"] = now
            self.logger.info(f"연결 끊김 ({self.stats['disconnections']}번째)")
            
        elif status == "message":
            # 메시지 수신 시간 업데이트
            self.stats["last_message_time"] = now
            
        elif status == "error":
            self.stats["errors"] += 1
            self.stats["last_error_time"] = now
            self.logger.error(f"오류 발생: {status}")
            
        elif status == "snapshot":
            # 스냅샷 수신 시간 업데이트
            self.snapshot_tracking["last_snapshot_time"] = now
            self.snapshot_tracking["total_snapshots"] += 1
            
            # 재연결 후 스냅샷 수신인 경우
            if self.stats["reconnections"] > 0:
                self.snapshot_tracking["reconnection_snapshots"] += 1
                self.logger.info(f"재연결 후 스냅샷 수신 ({self.snapshot_tracking['reconnection_snapshots']}번째)")
                
                # 모든 심볼에 대해 스냅샷을 받았는지 확인
                if self.snapshot_tracking["reconnection_snapshots"] >= len(TEST_SYMBOLS):
                    self.logger.info(f"모든 심볼({len(TEST_SYMBOLS)}개)에 대한 재연결 후 스냅샷 수신 완료")
    
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
                            self.logger.info(f"재연결 후 {symbol} 스냅샷 수신")
                            
                            # 모든 심볼에 대해 스냅샷을 받았는지 확인
                            if len(self.snapshot_tracking["symbols_with_snapshot"]) == len(TEST_SYMBOLS):
                                self.snapshot_tracking["reconnection_pending"] = False
                                self.logger.info(f"모든 심볼({len(TEST_SYMBOLS)}개)에 대한 스냅샷 수신 완료")
                
            except Exception as e:
                self.logger.error(f"메시지 처리 중 오류: {e}")
                self.stats["errors"] += 1
                self.stats["last_error_time"] = time.time()
                self.stats["last_error_message"] = str(e)
    
    async def force_disconnect(self):
        """강제 연결 끊기"""
        while not self.stop_event.is_set():
            try:
                # 첫 연결 후 일정 시간 대기
                if self.stats["connections"] == 0:
                    await asyncio.sleep(1)
                    continue
                
                # 최대 테스트 횟수에 도달한 경우 중지
                if self.stats["forced_disconnects"] >= MAX_RECONNECT_TESTS:
                    self.logger.info(f"최대 테스트 횟수({MAX_RECONNECT_TESTS}회)에 도달하여 테스트를 종료합니다.")
                    self.stop_event.set()
                    break
                
                # 랜덤 시간 대기
                wait_time = random.uniform(RECONNECT_INTERVAL_MIN, RECONNECT_INTERVAL_MAX)
                await asyncio.sleep(wait_time)
                
                # 이미 연결이 끊긴 상태인 경우 스킵
                if not self.binance_ws or not self.binance_ws.is_connected:
                    continue
                
                # 강제 연결 끊기 방법 선택
                disconnect_method = random.choice(FORCE_DISCONNECT_METHODS)
                self.stats["forced_disconnects"] += 1
                
                self.logger.info(f"강제 연결 끊기 시도 ({self.stats['forced_disconnects']}번째, 방법: {disconnect_method})")
                
                if disconnect_method == "close":
                    # 정상적인 방법으로 연결 끊기
                    if self.binance_ws and self.binance_ws.ws:
                        await self.binance_ws.ws.close()
                        self.logger.info("웹소켓 close() 메소드로 연결 끊기 성공")
                
                elif disconnect_method == "error":
                    # 오류 발생시켜 연결 끊기
                    if self.binance_ws and self.binance_ws.ws:
                        # 웹소켓 객체를 직접 조작하여 오류 발생
                        self.binance_ws.ws = None
                        self.logger.info("웹소켓 객체를 None으로 설정하여 오류 발생시킴")
                
            except Exception as e:
                self.logger.error(f"강제 종료 중 오류: {e}")
    
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
                        f"스냅샷 복구 완료: {current_snapshot_count}/{expected_snapshot_count} 심볼 "
                        f"(재연결 후 {time_since_reconnect:.1f}초)"
                    )
                    self.snapshot_tracking["reconnection_pending"] = False
                elif current_snapshot_count > 0:
                    self.logger.info(
                        f"스냅샷 복구 진행 중: {current_snapshot_count}/{expected_snapshot_count} 심볼"
                    )
    
    async def print_stats(self):
        """테스트 통계 출력"""
        while not self.stop_event.is_set():
            await asyncio.sleep(5)  # 5초마다 통계 출력
            
            now = time.time()
            elapsed = now - self.stats["test_start_time"]
            
            # 현재 연결 상태 확인
            is_connected = self.binance_ws and self.binance_ws.is_connected
            connection_state = "연결됨" if is_connected else "연결 끊김"
            
            # 마지막 메시지 수신 시간
            last_msg_time = "없음"
            if self.stats["last_message_time"] > 0:
                last_msg_time = f"{now - self.stats['last_message_time']:.1f}초 전"
            
            # 마지막 스냅샷 수신 시간
            last_snapshot_time = "없음"
            if self.snapshot_tracking["last_snapshot_time"] > 0:
                last_snapshot_time = f"{now - self.snapshot_tracking['last_snapshot_time']:.1f}초 전"
            
            # 통계 출력
            self.logger.info(
                f"=== 테스트 통계 (경과: {elapsed:.1f}초) ===\n"
                f"상태: {connection_state}\n"
                f"연결: {self.stats['connections']}회\n"
                f"재연결: {self.stats['reconnections']}회\n"
                f"강제 종료: {self.stats['forced_disconnects']}회\n"
                f"메시지 수신: {self.stats['messages_received']}개\n"
                f"스냅샷 수신: {self.snapshot_tracking['total_snapshots']}개\n"
                f"재연결 후 스냅샷: {self.snapshot_tracking['reconnection_snapshots']}개\n"
                f"오류: {self.stats['errors']}개\n"
                f"마지막 메시지: {last_msg_time}\n"
                f"마지막 스냅샷: {last_snapshot_time}\n"
                f"================================="
            )
    
    async def run_test(self):
        """테스트 실행"""
        self.logger.info("바이낸스 현물 웹소켓 재연결 테스트 시작")
        self.stats["test_start_time"] = time.time()
        
        # 바이낸스 웹소켓 인스턴스 생성
        self.binance_ws = BinanceSpotWebsocket(self.settings)
        self.binance_ws.set_output_queue(self.message_queue)
        
        # 바이낸스 오더북 매니저의 output_queue 설정
        if hasattr(self.binance_ws, 'manager') and self.binance_ws.manager:
            self.binance_ws.manager.set_output_queue(self.message_queue)
            self.logger.info("바이낸스 오더북 매니저의 output_queue 설정 완료")
        
        # 연결 상태 콜백 설정
        self.binance_ws.set_connection_status_callback(self.connection_status_callback)
        
        # 테스트 태스크 생성
        tasks = [
            asyncio.create_task(self.process_queue()),
            asyncio.create_task(self.force_disconnect()),
            asyncio.create_task(self.print_stats()),
            asyncio.create_task(self.check_snapshot_recovery())
        ]
        
        # 웹소켓 시작 태스크 생성
        symbols_by_exchange = {"binance": TEST_SYMBOLS}
        ws_task = asyncio.create_task(self.binance_ws.start(symbols_by_exchange))
        
        try:
            # 지정된 시간 동안 테스트 실행 또는 최대 테스트 횟수 도달까지
            await asyncio.sleep(TEST_DURATION)
            if not self.stop_event.is_set():
                self.logger.info(f"테스트 기간 {TEST_DURATION}초 완료")
                self.stop_event.set()
            
        except asyncio.CancelledError:
            self.logger.info("테스트가 취소되었습니다")
            
        finally:
            # 테스트 종료 처리
            self.stop_event.set()
            
            # 웹소켓 종료
            if self.binance_ws:
                try:
                    await self.binance_ws.stop()
                except Exception as e:
                    self.logger.error(f"웹소켓 종료 중 오류: {e}")
            
            # 태스크 취소
            for task in tasks:
                task.cancel()
            
            # 최종 통계 출력
            self.logger.info(
                f"=== 최종 테스트 결과 ===\n"
                f"총 테스트 시간: {time.time() - self.stats['test_start_time']:.1f}초\n"
                f"총 연결 횟수: {self.stats['connections']}회\n"
                f"총 재연결 횟수: {self.stats['reconnections']}회\n"
                f"총 강제 종료 횟수: {self.stats['forced_disconnects']}회\n"
                f"총 메시지 수신: {self.stats['messages_received']}개\n"
                f"총 스냅샷 수신: {self.snapshot_tracking['total_snapshots']}개\n"
                f"재연결 후 스냅샷 수신: {self.snapshot_tracking['reconnection_snapshots']}개\n"
                f"총 오류 발생: {self.stats['errors']}개\n"
                f"=========================="
            )

def handle_sigint(signum, frame):
    """SIGINT 핸들러"""
    logger.info("Ctrl+C 감지, 테스트를 종료합니다...")
    # 메인 이벤트 루프에서 처리하도록 함

async def main():
    """메인 함수"""
    tester = BinanceReconnectTester()
    await tester.run_test()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("프로그램이 사용자에 의해 중단되었습니다.")
    except Exception as e:
        logger.error(f"프로그램 실행 중 오류 발생: {e}", exc_info=True)
    finally:
        logger.info("프로그램 종료") 