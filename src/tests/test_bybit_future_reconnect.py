#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
바이비트 선물 웹소켓 재연결 테스트 스크립트

이 스크립트는 바이비트 선물 웹소켓의 재연결 기능을 테스트합니다.
- 웹소켓 연결 수립
- 임의로 연결 끊기 (강제 종료)
- 재연결 확인
- 메시지 수신 확인
- 오더북 복구 확인

사용법:
    python test_bybit_future_reconnect.py
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
logger = logging.getLogger("bybit_future_reconnect_test")

# 바이비트 선물 웹소켓 클래스 임포트
from crosskimp.ob_collector.orderbook.websocket.bybit_f_ws import BybitFutureWebsocket
from crosskimp.ob_collector.utils.logging.logger import get_unified_logger

# 테스트 설정
TEST_SYMBOLS = ["BTC", "ETH", "XRP", "SOL", "DOGE"]  # 테스트할 심볼 목록
RECONNECT_INTERVAL_MIN = 5  # 최소 재연결 간격 (초)
RECONNECT_INTERVAL_MAX = 10  # 최대 재연결 간격 (초)
TEST_DURATION = 60  # 테스트 지속 시간 (초)
FORCE_DISCONNECT_METHODS = ["close", "error"]  # 강제 연결 끊기 방법
MAX_RECONNECT_TESTS = 10  # 최대 테스트 횟수

class BybitFutureReconnectTester:
    """바이비트 선물 웹소켓 재연결 테스트 클래스"""
    
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
            "reconnect_snapshot_count": 0,
            "last_reconnect_time": 0,  # 추가: 마지막 재연결 시간
            "reconnect_snapshot_symbols": set()  # 추가: 재연결 후 스냅샷 수신한 심볼 추적
        }
        
        # 바이비트 선물 웹소켓 설정
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
        
        # 바이비트 선물 웹소켓 인스턴스 생성
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
                self.snapshot_tracking["reconnect_snapshot_symbols"].clear()
                self.snapshot_tracking["last_reconnect_time"] = now
                self.logger.info(f"재연결 시간 기록: {now}")
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
            
            # 재연결 후 스냅샷 수신 확인
            if self.stats["reconnections"] > 0:
                time_since_reconnect = now - self.snapshot_tracking["last_reconnect_time"]
                
                # 디버깅을 위한 상세 로그 추가
                self.logger.info(f"스냅샷 수신 시간: {now}, 마지막 재연결 시간: {self.snapshot_tracking['last_reconnect_time']}")
                self.logger.info(f"재연결 후 경과 시간: {time_since_reconnect:.1f}초")
                
                # 재연결 후 스냅샷 수신 조건 확인 (60초 이내)
                if time_since_reconnect <= 60:
                    self.snapshot_tracking["reconnect_snapshot_count"] += 1
                    self.logger.info(f"재연결 후 스냅샷 수신 카운트 증가: {self.snapshot_tracking['reconnect_snapshot_count']} (경과: {time_since_reconnect:.1f}초)")
    
    async def process_queue(self):
        """메시지 큐 처리"""
        while not self.stop_event.is_set():
            try:
                # 큐에서 메시지 가져오기 (타임아웃 설정)
                message = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)
                
                # 메시지 타입 확인
                if isinstance(message, dict):
                    msg_type = message.get("type", "unknown")
                    symbol = message.get("symbol", "unknown")
                    
                    # 스냅샷 메시지 처리
                    if msg_type == "snapshot":
                        self.stats["snapshot_received"] += 1
                        self.snapshot_tracking["symbols_with_snapshot"].add(symbol)
                        self.snapshot_tracking["last_snapshot_time"] = time.time()
                        
                        # 재연결 후 스냅샷 수신 확인
                        if self.stats["reconnections"] > 0:
                            now = time.time()
                            last_reconnect_time = self.snapshot_tracking["last_reconnect_time"]
                            time_since_reconnect = now - last_reconnect_time
                            
                            # 디버깅을 위한 상세 로그 추가
                            self.logger.info(f"스냅샷 수신 시간: {now}, 마지막 재연결 시간: {last_reconnect_time}")
                            self.logger.info(f"재연결 후 경과 시간: {time_since_reconnect:.1f}초")
                            
                            # 재연결 후 스냅샷 수신 조건 확인 (60초 이내)
                            if time_since_reconnect <= 60 and symbol not in self.snapshot_tracking["reconnect_snapshot_symbols"]:
                                self.snapshot_tracking["reconnect_snapshot_count"] += 1
                                self.snapshot_tracking["reconnect_snapshot_symbols"].add(symbol)
                                self.logger.info(f"재연결 후 스냅샷 수신 카운트 증가: {self.snapshot_tracking['reconnect_snapshot_count']} (심볼: {symbol}, 경과: {time_since_reconnect:.1f}초)")
                            else:
                                if time_since_reconnect > 60:
                                    self.logger.warning(f"재연결 후 스냅샷 수신 시간 초과: {time_since_reconnect:.1f}초 > 60초")
                                elif symbol in self.snapshot_tracking["reconnect_snapshot_symbols"]:
                                    self.logger.info(f"이미 카운트된 심볼의 스냅샷: {symbol}")
                    
                    # 오더북 메시지 처리
                    elif msg_type == "orderbook":
                        # 오더북 상태 확인
                        bids = message.get("bids", [])
                        asks = message.get("asks", [])
                        if bids and asks:
                            self.logger.debug(
                                f"[{symbol}] 오더북 상태 | "
                                f"최고매수가={bids[0][0]}, "
                                f"최저매도가={asks[0][0]}, "
                                f"스프레드={(asks[0][0] - bids[0][0]):.2f}"
                            )
                
                # 큐 작업 완료 표시
                self.message_queue.task_done()
                
            except asyncio.TimeoutError:
                # 타임아웃은 정상적인 상황
                continue
            except Exception as e:
                self.logger.error(f"메시지 처리 오류: {str(e)}")
                await asyncio.sleep(0.1)
    
    async def force_disconnect(self):
        """강제 연결 끊기 테스트"""
        reconnect_count = 0
        
        while not self.stop_event.is_set():
            # 최대 테스트 횟수 확인
            if reconnect_count >= MAX_RECONNECT_TESTS:
                self.logger.info(f"최대 테스트 횟수({MAX_RECONNECT_TESTS}회) 도달")
                self.stop_event.set()
                break
                
            # 랜덤 대기 시간
            wait_time = random.uniform(RECONNECT_INTERVAL_MIN, RECONNECT_INTERVAL_MAX)
            self.logger.info(f"다음 강제 연결 끊기까지 {wait_time:.1f}초 대기")
            
            # 대기
            for _ in range(int(wait_time * 10)):
                if self.stop_event.is_set():
                    break
                await asyncio.sleep(0.1)
            
            if self.stop_event.is_set():
                break
                
            # 웹소켓 객체 확인
            if not self.bybit_ws or not self.bybit_ws.is_connected:
                self.logger.warning("웹소켓이 연결되어 있지 않음, 강제 연결 끊기 건너뜀")
                await asyncio.sleep(1)
                continue
                
            # 강제 연결 끊기 방법 선택
            method = random.choice(FORCE_DISCONNECT_METHODS)
            self.logger.warning(f"강제 연결 끊기 시작 | 방법: {method}")
            
            try:
                if method == "close":
                    # 정상 종료 (close 메서드 호출)
                    await self.bybit_ws.ws.close()
                elif method == "error":
                    # 에러 발생 (잘못된 메시지 전송)
                    await self.bybit_ws.ws.send("invalid_message_to_trigger_error")
                
                self.stats["forced_disconnects"] += 1
                reconnect_count += 1
                self.logger.warning(f"강제 연결 끊기 완료 | 총 {reconnect_count}회")
                
            except Exception as e:
                self.logger.error(f"강제 연결 끊기 실패: {str(e)}")
            
            # 재연결 대기
            await asyncio.sleep(2)
    
    async def check_snapshot_recovery(self):
        """스냅샷 복구 확인"""
        while not self.stop_event.is_set():
            try:
                # 현재 스냅샷 상태 확인
                if self.bybit_ws and hasattr(self.bybit_ws, 'orderbook_manager'):
                    ob_manager = self.bybit_ws.orderbook_manager
                    
                    # 초기화된 심볼 확인
                    initialized_symbols = []
                    for symbol in TEST_SYMBOLS:
                        if ob_manager.is_initialized(symbol):
                            initialized_symbols.append(symbol)
                    
                    if initialized_symbols:
                        self.logger.info(f"초기화된 오더북: {len(initialized_symbols)}/{len(TEST_SYMBOLS)} 심볼")
                
                await asyncio.sleep(5)
                
            except Exception as e:
                self.logger.error(f"스냅샷 복구 확인 오류: {str(e)}")
                await asyncio.sleep(1)
    
    async def print_stats(self):
        """통계 출력"""
        while not self.stop_event.is_set():
            try:
                # 현재 시간 기준 통계 계산
                now = time.time()
                elapsed = now - self.stats["test_start_time"]
                
                # 마지막 메시지/연결 시간
                last_msg_time = "없음"
                if self.stats["last_message_time"] > 0:
                    last_msg_time = f"{now - self.stats['last_message_time']:.1f}초 전"
                
                last_conn_time = "없음"
                if self.stats["last_connection_time"] > 0:
                    last_conn_time = f"{now - self.stats['last_connection_time']:.1f}초 전"
                
                last_snapshot_time = "없음"
                if self.snapshot_tracking["last_snapshot_time"] > 0:
                    last_snapshot_time = f"{now - self.snapshot_tracking['last_snapshot_time']:.1f}초 전"
                
                # 통계 출력
                self.logger.info(
                    f"\n=== 테스트 통계 ({elapsed:.1f}초 경과) ===\n"
                    f"연결 시도: {self.stats['connection_attempts']}회\n"
                    f"성공한 연결: {self.stats['successful_connections']}회\n"
                    f"재연결: {self.stats['reconnections']}회\n"
                    f"강제 종료: {self.stats['forced_disconnects']}회\n"
                    f"수신 메시지: {self.stats['messages_received']}개\n"
                    f"스냅샷 수신: {self.stats['snapshot_received']}회\n"
                    f"재연결 후 스냅샷: {self.snapshot_tracking['reconnect_snapshot_count']}회\n"
                    f"마지막 연결: {last_conn_time}\n"
                    f"핑 전송: {self.stats['ping_sent']}회\n"
                    f"퐁 수신: {self.stats['pong_received']}회\n"
                    f"오류: {self.stats['errors']}개\n"
                    f"마지막 메시지: {last_msg_time}\n"
                    f"마지막 스냅샷: {last_snapshot_time}\n"
                    f"================================="
                )
                
                # 5초마다 출력
                await asyncio.sleep(5)
                
            except Exception as e:
                self.logger.error(f"통계 출력 오류: {str(e)}")
                await asyncio.sleep(1)
    
    async def run_test(self):
        """테스트 실행"""
        self.logger.info("바이비트 선물 웹소켓 재연결 테스트 시작")
        self.stats["test_start_time"] = time.time()
        
        # 바이비트 선물 웹소켓 인스턴스 생성
        self.bybit_ws = BybitFutureWebsocket(self.settings)
        self.bybit_ws.set_output_queue(self.message_queue)
        
        # 바이비트 오더북 매니저의 output_queue 설정
        if hasattr(self.bybit_ws, 'orderbook_manager') and self.bybit_ws.orderbook_manager:
            self.bybit_ws.orderbook_manager.output_queue = self.message_queue
            self.logger.info("바이비트 선물 오더북 매니저의 output_queue 설정 완료")
        
        self.bybit_ws.connection_status_callback = self.connection_status_callback
        
        # 테스트 태스크 생성
        tasks = [
            asyncio.create_task(self.process_queue()),
            asyncio.create_task(self.force_disconnect()),
            asyncio.create_task(self.print_stats()),
            asyncio.create_task(self.check_snapshot_recovery())
        ]
        
        # 웹소켓 시작 태스크 생성
        symbols_by_exchange = {"bybitfuture": TEST_SYMBOLS}
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
        tester = BybitFutureReconnectTester()
        await tester.run_test()
    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main()) 