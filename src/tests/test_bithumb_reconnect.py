#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
빗썸 웹소켓 재연결 테스트 스크립트

이 스크립트는 빗썸 웹소켓의 재연결 기능을 테스트합니다.
특히 다음 사항을 중점적으로 확인합니다:
- 웹소켓 연결 수립
- 임의로 연결 끊기 (강제 종료)
- 재연결 확인
- 재연결 시 스냅샷 수신부터 다시 작동되는지 확인
- 메시지 수신 확인

사용법:
    python test_bithumb_reconnect.py
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
logger = logging.getLogger("bithumb_reconnect_test")

# 빗썸 웹소켓 클래스 임포트
from crosskimp.ob_collector.orderbook.websocket.bithumb_s_ws import BithumbSpotWebsocket
from crosskimp.logger.logger import get_unified_logger

# 테스트 설정
TEST_SYMBOLS = ["BTC", "ETH", "XRP", "SOL", "DOGE"]  # 테스트할 심볼 목록
RECONNECT_INTERVAL_MIN = 5  # 최소 재연결 간격 (초)
RECONNECT_INTERVAL_MAX = 10  # 최대 재연결 간격 (초)
TEST_DURATION = 30  # 테스트 지속 시간 (초)
FORCE_DISCONNECT_METHODS = ["close"]  # 강제 연결 끊기 방법 (에러 방식 제외)

class BithumbReconnectTester:
    """빗썸 웹소켓 재연결 테스트 클래스"""
    
    def __init__(self):
        """테스터 초기화"""
        self.logger = logger
        self.message_queue = asyncio.Queue()
        self.stop_event = asyncio.Event()
        
        # 테스트 통계
        self.stats = {
            "connection_attempts": 0,
            "successful_connections": 0,
            "reconnections": 0,
            "messages_received": 0,
            "errors": 0,
            "test_start_time": 0,
            "last_connection_time": 0,
            "last_message_time": 0,
            "forced_disconnects": 0,
            "snapshot_requests": 0,
            "snapshot_received": 0
        }
        
        # 스냅샷 추적
        self.snapshot_tracking = {
            "last_snapshot_time": 0,
            "symbols_with_snapshot": set(),
            "reconnect_snapshot_count": 0
        }
        
        # 빗썸 웹소켓 설정
        self.settings = {
            "depth": 10,
            "reconnect": {
                "initial_delay": 1.0,
                "max_delay": 10.0,
                "multiplier": 1.5,
                "max_attempts": 0  # 무제한 재시도
            },
            "logging": {
                "log_raw_data": True
            }
        }
        
        # 빗썸 웹소켓 인스턴스 생성
        self.bithumb_ws = None
    
    def connection_status_callback(self, exchange: str, status: str):
        """연결 상태 콜백 함수"""
        now = time.time()
        
        if status == "connect_attempt":
            self.stats["connection_attempts"] += 1
            self.logger.info(f"연결 시도 #{self.stats['connection_attempts']}")
            
        elif status == "connect":
            self.stats["successful_connections"] += 1
            self.stats["last_connection_time"] = now
            if self.stats["successful_connections"] > 1:
                self.stats["reconnections"] += 1
                self.logger.info(f"재연결 성공 #{self.stats['reconnections']}")
                # 재연결 시 스냅샷 추적 초기화
                self.snapshot_tracking["symbols_with_snapshot"].clear()
            else:
                self.logger.info("최초 연결 성공")
                
        elif status == "disconnect":
            self.logger.info("연결 종료됨")
            
        elif status == "message":
            self.stats["messages_received"] += 1
            self.stats["last_message_time"] = now
            if self.stats["messages_received"] % 100 == 0:
                self.logger.info(f"메시지 {self.stats['messages_received']}개 수신됨")
                
        elif status == "error":
            self.stats["errors"] += 1
            self.logger.warning(f"오류 발생 #{self.stats['errors']}")
            
        elif status == "snapshot_request":
            self.stats["snapshot_requests"] += 1
            self.logger.info(f"스냅샷 요청 #{self.stats['snapshot_requests']}")
            
        elif status == "snapshot_received":
            self.stats["snapshot_received"] += 1
            self.snapshot_tracking["last_snapshot_time"] = now
            
            # 재연결 후 스냅샷 수신 추적
            if self.stats["reconnections"] > 0 and len(self.snapshot_tracking["symbols_with_snapshot"]) == 0:
                self.snapshot_tracking["reconnect_snapshot_count"] += 1
                self.logger.info(f"재연결 후 첫 스냅샷 수신 #{self.snapshot_tracking['reconnect_snapshot_count']}")
            
            # 심볼별 스냅샷 수신 추적 - 이 부분은 process_queue에서 처리하므로 여기서는 제거
            self.logger.info(f"스냅샷 수신 #{self.stats['snapshot_received']}")
    
    async def process_queue(self):
        """메시지 큐 처리"""
        while not self.stop_event.is_set():
            try:
                exchange, data = await self.message_queue.get()
                
                # 스냅샷 여부 확인 (빗썸 오더북 데이터 구조 확인 필요)
                if isinstance(data, dict) and "type" in data:
                    if data["type"] == "snapshot":
                        symbol = data.get("symbol", "UNKNOWN")
                        if symbol != "UNKNOWN":
                            self.snapshot_tracking["symbols_with_snapshot"].add(symbol)
                            self.logger.info(f"큐에서 스냅샷 감지: {symbol}, 현재 {len(self.snapshot_tracking['symbols_with_snapshot'])}/{len(TEST_SYMBOLS)} 심볼")
                
                self.message_queue.task_done()
            except Exception as e:
                self.logger.error(f"큐 처리 오류: {e}")
            await asyncio.sleep(0.01)
    
    async def print_stats(self):
        """테스트 통계 출력"""
        while not self.stop_event.is_set():
            await asyncio.sleep(5)  # 5초마다 통계 출력
            
            now = time.time()
            elapsed = now - self.stats["test_start_time"]
            
            # 현재 연결 상태 확인
            is_connected = self.bithumb_ws and self.bithumb_ws.is_connected
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
                f"연결 시도: {self.stats['connection_attempts']}회\n"
                f"성공한 연결: {self.stats['successful_connections']}회\n"
                f"재연결: {self.stats['reconnections']}회\n"
                f"강제 종료: {self.stats['forced_disconnects']}회\n"
                f"수신 메시지: {self.stats['messages_received']}개\n"
                f"스냅샷 요청: {self.stats['snapshot_requests']}회\n"
                f"스냅샷 수신: {self.stats['snapshot_received']}회\n"
                f"재연결 후 스냅샷: {self.snapshot_tracking['reconnect_snapshot_count']}회\n"
                f"현재 스냅샷 심볼: {len(self.snapshot_tracking['symbols_with_snapshot'])}/{len(TEST_SYMBOLS)}개\n"
                f"오류: {self.stats['errors']}개\n"
                f"마지막 메시지: {last_msg_time}\n"
                f"마지막 스냅샷: {last_snapshot_time}\n"
                f"================================="
            )
    
    async def check_snapshot_recovery(self):
        """스냅샷 복구 확인"""
        while not self.stop_event.is_set():
            await asyncio.sleep(1)  # 1초마다 확인
            
            # 재연결 후 스냅샷 복구 확인
            if self.stats["reconnections"] > 0:
                current_snapshot_count = len(self.snapshot_tracking["symbols_with_snapshot"])
                expected_snapshot_count = len(TEST_SYMBOLS)
                
                # 모든 심볼에 대해 스냅샷이 수신되었는지 확인
                if current_snapshot_count == expected_snapshot_count:
                    time_since_reconnect = time.time() - self.stats["last_connection_time"]
                    self.logger.info(
                        f"스냅샷 복구 완료: {current_snapshot_count}/{expected_snapshot_count} 심볼 "
                        f"(재연결 후 {time_since_reconnect:.1f}초)"
                    )
                elif current_snapshot_count > 0:
                    self.logger.info(
                        f"스냅샷 복구 진행 중: {current_snapshot_count}/{expected_snapshot_count} 심볼"
                    )
    
    async def run_test(self):
        """테스트 실행"""
        self.logger.info("빗썸 웹소켓 재연결 테스트 시작")
        self.stats["test_start_time"] = time.time()
        
        # 빗썸 웹소켓 인스턴스 생성
        self.bithumb_ws = BithumbSpotWebsocket(self.settings)
        self.bithumb_ws.set_output_queue(self.message_queue)
        self.bithumb_ws.connection_status_callback = self.connection_status_callback
        
        # 테스트 태스크 생성
        tasks = [
            asyncio.create_task(self.process_queue()),
            asyncio.create_task(self.print_stats()),
            asyncio.create_task(self.check_snapshot_recovery())
        ]
        
        # 웹소켓 시작 태스크 생성
        symbols_by_exchange = {"bithumb": TEST_SYMBOLS}
        ws_task = asyncio.create_task(self.bithumb_ws.start(symbols_by_exchange))
        
        try:
            # 10초 대기 후 강제 연결 종료
            await asyncio.sleep(10)
            self.logger.info("10초 경과, 강제 연결 종료 시도")
            
            if self.bithumb_ws and self.bithumb_ws.is_connected:
                self.logger.info("웹소켓 강제 종료 (close 메서드 호출)")
                if self.bithumb_ws.ws:
                    await self.bithumb_ws.ws.close()
                    self.stats["forced_disconnects"] += 1
            
            # 나머지 시간 동안 테스트 실행
            remaining_time = TEST_DURATION - 10
            self.logger.info(f"강제 종료 후 {remaining_time}초 동안 재연결 테스트 실행")
            await asyncio.sleep(remaining_time)
            
        except asyncio.CancelledError:
            self.logger.info("테스트가 취소되었습니다")
            
        finally:
            # 테스트 종료 처리
            self.stop_event.set()
            
            # 웹소켓 종료
            if self.bithumb_ws:
                await self.bithumb_ws.stop()
            
            # 모든 태스크 취소
            for task in tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            # 웹소켓 태스크 취소
            ws_task.cancel()
            try:
                await ws_task
            except asyncio.CancelledError:
                pass
            
            # 최종 통계 출력
            elapsed = time.time() - self.stats["test_start_time"]
            self.logger.info(
                f"=== 테스트 최종 결과 (총 {elapsed:.1f}초) ===\n"
                f"연결 시도: {self.stats['connection_attempts']}회\n"
                f"성공한 연결: {self.stats['successful_connections']}회\n"
                f"재연결: {self.stats['reconnections']}회\n"
                f"강제 종료: {self.stats['forced_disconnects']}회\n"
                f"수신 메시지: {self.stats['messages_received']}개\n"
                f"스냅샷 요청: {self.stats['snapshot_requests']}회\n"
                f"스냅샷 수신: {self.stats['snapshot_received']}회\n"
                f"재연결 후 스냅샷: {self.snapshot_tracking['reconnect_snapshot_count']}회\n"
                f"오류: {self.stats['errors']}개\n"
                f"================================="
            )

def handle_sigint(signum, frame):
    """SIGINT(Ctrl+C) 처리"""
    logger.info("Ctrl+C 감지, 테스트를 종료합니다...")
    # asyncio 이벤트 루프 중지
    if asyncio.get_event_loop().is_running():
        asyncio.get_event_loop().stop()
    sys.exit(0)

async def main():
    """메인 함수"""
    # SIGINT(Ctrl+C) 핸들러 등록
    signal.signal(signal.SIGINT, handle_sigint)
    
    # 테스터 생성 및 실행
    tester = BithumbReconnectTester()
    await tester.run_test()

if __name__ == "__main__":
    # 비동기 이벤트 루프 실행
    asyncio.run(main()) 