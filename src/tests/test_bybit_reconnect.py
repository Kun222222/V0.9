#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
바이비트 웹소켓 재연결 테스트 스크립트

이 스크립트는 바이비트 웹소켓의 재연결 기능을 테스트합니다.
- 웹소켓 연결 수립
- 임의로 연결 끊기 (강제 종료)
- 재연결 확인
- 메시지 수신 확인

사용법:
    python test_bybit_reconnect.py
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
logger = logging.getLogger("bybit_reconnect_test")

# 바이비트 웹소켓 클래스 임포트
from crosskimp.ob_collector.orderbook.websocket.bybit_s_ws import BybitSpotWebsocket
from crosskimp.ob_collector.utils.logging.logger import get_unified_logger

# 테스트 설정
TEST_SYMBOLS = ["BTC", "ETH", "XRP", "SOL", "DOGE"]  # 테스트할 심볼 목록
RECONNECT_INTERVAL_MIN = 5  # 최소 재연결 간격 (초)
RECONNECT_INTERVAL_MAX = 10  # 최대 재연결 간격 (초)
TEST_DURATION = 60  # 테스트 지속 시간 (초)
FORCE_DISCONNECT_METHODS = ["close", "error"]  # 강제 연결 끊기 방법
MAX_RECONNECT_TESTS = 10  # 최대 테스트 횟수

class BybitReconnectTester:
    """바이비트 웹소켓 재연결 테스트 클래스"""
    
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
            "ping_sent": 0,
            "pong_received": 0,
            "snapshot_received": 0
        }
        
        # 스냅샷 추적
        self.snapshot_tracking = {
            "last_snapshot_time": 0,
            "symbols_with_snapshot": set(),
            "reconnect_snapshot_count": 0
        }
        
        # 바이비트 웹소켓 설정
        self.settings = {
            "connection": {
                "websocket": {
                    "depth_level": 50
                }
            },
            "websocket": {
                "orderbook_depth": 10,
                "reconnect": {
                    "initial_delay": 1.0,
                    "max_delay": 10.0,
                    "multiplier": 1.5,
                    "max_attempts": 0  # 무제한 재시도
                }
            },
            "logging": {
                "log_raw_data": True
            }
        }
        
        # 바이비트 웹소켓 인스턴스 생성
        self.bybit_ws = None
    
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
            
        elif status == "ping":
            self.stats["ping_sent"] += 1
            self.logger.debug(f"핑 전송 #{self.stats['ping_sent']}")
            
        elif status == "pong":
            self.stats["pong_received"] += 1
            self.logger.debug(f"퐁 수신 #{self.stats['pong_received']}")
            
        elif status == "snapshot_received":
            self.stats["snapshot_received"] += 1
            self.snapshot_tracking["last_snapshot_time"] = now
            self.logger.info(f"스냅샷 수신 #{self.stats['snapshot_received']}")
    
    async def process_queue(self):
        """메시지 큐 처리"""
        while not self.stop_event.is_set():
            try:
                exchange, data = await self.message_queue.get()
                
                # 디버그 로깅 추가 (모든 메시지 구조 확인)
                if isinstance(data, dict):
                    # 모든 메시지의 구조를 로깅
                    self.logger.info(f"큐 메시지: {exchange}, 타입: {type(data)}, 키: {list(data.keys())}")
                    self.logger.info(f"메시지 내용: {data}")
                
                # 스냅샷 여부 확인 (바이비트 오더북 데이터 구조 확인)
                if isinstance(data, dict):
                    # 바이비트 웹소켓 클래스에서 파싱된 메시지 구조 확인
                    if exchange == "bybit" and "type" in data:
                        msg_type = data["type"].lower()
                        if msg_type == "snapshot":
                            symbol = data.get("symbol", "")
                            self.logger.info(f"스냅샷 메시지 감지: {symbol}, type={msg_type}")
                            if symbol in TEST_SYMBOLS:
                                self.snapshot_tracking["symbols_with_snapshot"].add(symbol)
                                self.logger.info(f"큐에서 스냅샷 감지: {symbol}, 현재 {len(self.snapshot_tracking['symbols_with_snapshot'])}/{len(TEST_SYMBOLS)} 심볼")
                                
                                # 연결 상태 콜백 호출
                                self.connection_status_callback(self.bybit_ws.exchangename, "snapshot_received")
                
                self.message_queue.task_done()
            except Exception as e:
                self.logger.error(f"큐 처리 오류: {e}")
            await asyncio.sleep(0.01)
    
    async def force_disconnect(self):
        """임의로 연결 강제 종료"""
        while not self.stop_event.is_set():
            # 최대 테스트 횟수 확인
            if self.stats["reconnections"] >= MAX_RECONNECT_TESTS:
                self.logger.info(f"최대 테스트 횟수({MAX_RECONNECT_TESTS}회) 도달, 테스트 종료")
                self.stop_event.set()
                break
                
            # 일정 시간 대기 후 연결 강제 종료
            wait_time = random.randint(RECONNECT_INTERVAL_MIN, RECONNECT_INTERVAL_MAX)
            self.logger.info(f"{wait_time}초 후 연결 강제 종료 예정...")
            await asyncio.sleep(wait_time)
            
            if self.stop_event.is_set():
                break
                
            if not self.bybit_ws or not self.bybit_ws.is_connected:
                self.logger.info("연결이 이미 끊어져 있어 강제 종료 건너뜀")
                continue
            
            # 강제 종료 방법 선택
            method = random.choice(FORCE_DISCONNECT_METHODS)
            self.stats["forced_disconnects"] += 1
            
            try:
                if method == "close":
                    # 정상적인 방법으로 연결 종료
                    self.logger.info(f"강제 종료 #{self.stats['forced_disconnects']}: close() 메서드 호출")
                    if self.bybit_ws.ws:
                        await self.bybit_ws.ws.close()
                        
                elif method == "error":
                    # 에러 발생시키기 (웹소켓 객체 조작)
                    self.logger.info(f"강제 종료 #{self.stats['forced_disconnects']}: 에러 발생시키기")
                    if self.bybit_ws.ws:
                        # ws 객체를 None으로 설정하여 다음 메시지 수신/전송 시 에러 발생
                        self.bybit_ws.ws = None
                        
            except Exception as e:
                self.logger.error(f"강제 종료 중 오류: {e}")
    
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
    
    async def print_stats(self):
        """테스트 통계 출력"""
        while not self.stop_event.is_set():
            await asyncio.sleep(5)  # 5초마다 통계 출력
            
            now = time.time()
            elapsed = now - self.stats["test_start_time"]
            
            # 현재 연결 상태 확인
            is_connected = self.bybit_ws and self.bybit_ws.is_connected
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
                f"재연결: {self.stats['reconnections']}/{MAX_RECONNECT_TESTS}회\n"
                f"강제 종료: {self.stats['forced_disconnects']}회\n"
                f"수신 메시지: {self.stats['messages_received']}개\n"
                f"스냅샷 수신: {self.stats['snapshot_received']}회\n"
                f"재연결 후 스냅샷: {self.snapshot_tracking['reconnect_snapshot_count']}회\n"
                f"현재 스냅샷 심볼: {len(self.snapshot_tracking['symbols_with_snapshot'])}/{len(TEST_SYMBOLS)}개\n"
                f"핑 전송: {self.stats['ping_sent']}회\n"
                f"퐁 수신: {self.stats['pong_received']}회\n"
                f"오류: {self.stats['errors']}개\n"
                f"마지막 메시지: {last_msg_time}\n"
                f"마지막 스냅샷: {last_snapshot_time}\n"
                f"================================="
            )
    
    async def run_test(self):
        """테스트 실행"""
        self.logger.info("바이비트 웹소켓 재연결 테스트 시작")
        self.stats["test_start_time"] = time.time()
        
        # 바이비트 웹소켓 인스턴스 생성
        self.bybit_ws = BybitSpotWebsocket(self.settings)
        self.bybit_ws.set_output_queue(self.message_queue)
        
        # 바이비트 오더북 매니저의 output_queue 설정
        if hasattr(self.bybit_ws, 'ob_manager') and self.bybit_ws.ob_manager:
            self.bybit_ws.ob_manager.output_queue = self.message_queue
            self.logger.info("바이비트 오더북 매니저의 output_queue 설정 완료")
        
        self.bybit_ws.connection_status_callback = self.connection_status_callback
        
        # 테스트 태스크 생성
        tasks = [
            asyncio.create_task(self.process_queue()),
            asyncio.create_task(self.force_disconnect()),
            asyncio.create_task(self.print_stats()),
            asyncio.create_task(self.check_snapshot_recovery())
        ]
        
        # 웹소켓 시작 태스크 생성
        symbols_by_exchange = {"bybit": TEST_SYMBOLS}
        ws_task = asyncio.create_task(self.bybit_ws.start(symbols_by_exchange))
        
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
            if self.bybit_ws:
                await self.bybit_ws.stop()
            
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
                f"\n=== 테스트 최종 결과 (총 {elapsed:.1f}초) ===\n"
                f"연결 시도: {self.stats['connection_attempts']}회\n"
                f"성공한 연결: {self.stats['successful_connections']}회\n"
                f"재연결: {self.stats['reconnections']}회\n"
                f"강제 종료: {self.stats['forced_disconnects']}회\n"
                f"수신 메시지: {self.stats['messages_received']}개\n"
                f"스냅샷 수신: {self.stats['snapshot_received']}회\n"
                f"재연결 후 스냅샷: {self.snapshot_tracking['reconnect_snapshot_count']}회\n"
                f"핑 전송: {self.stats['ping_sent']}회\n"
                f"퐁 수신: {self.stats['pong_received']}회\n"
                f"오류: {self.stats['errors']}개\n"
                f"================================="
            )

def handle_sigint(signum, frame):
    """SIGINT(Ctrl+C) 핸들러"""
    logger.info("\n테스트가 사용자에 의해 중단되었습니다.")
    sys.exit(0)

async def main():
    """메인 함수"""
    signal.signal(signal.SIGINT, handle_sigint)
    
    try:
        tester = BybitReconnectTester()
        await tester.run_test()
    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main()) 