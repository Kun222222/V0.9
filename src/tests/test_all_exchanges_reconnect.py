#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
모든 거래소 웹소켓 동시 연결 및 재연결 테스트 스크립트

이 스크립트는 다음 거래소들의 웹소켓 연결 및 재연결 기능을 동시에 테스트합니다:
- 바이낸스 현물
- 바이낸스 선물
- 바이비트 현물
- 바이비트 선물
- 업비트 현물
- 빗썸 현물

테스트 내용:
- 모든 거래소 웹소켓 동시 연결
- 각 거래소별로 10번씩 랜덤하게 연결 끊기 테스트
- 재연결 확인
- 메시지 수신 확인
- 재연결 후 스냅샷 수신 확인

사용법:
    python test_all_exchanges_reconnect.py
"""

import asyncio
import json
import logging
import random
import signal
import sys
import time
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_reconnect.log'),  # 파일로 로깅
        logging.StreamHandler(sys.stdout)  # 터미널에는 중요 정보만 출력
    ]
)
logger = logging.getLogger("all_exchanges_reconnect_test")

# raw 데이터 로깅 비활성화
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# 웹소켓 클래스 임포트
from crosskimp.ob_collector.orderbook.websocket.binance_s_ws import BinanceSpotWebsocket
from crosskimp.ob_collector.orderbook.websocket.binance_f_ws import BinanceFutureWebsocket
from crosskimp.ob_collector.orderbook.websocket.bybit_s_ws import BybitSpotWebsocket
from crosskimp.ob_collector.orderbook.websocket.bybit_f_ws import BybitFutureWebsocket
from crosskimp.ob_collector.orderbook.websocket.upbit_s_ws import UpbitWebsocket
from crosskimp.ob_collector.orderbook.websocket.bithumb_s_ws import BithumbSpotWebsocket
from crosskimp.ob_collector.orderbook.websocket.base_ws_connector import BaseWebsocketConnector
from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.utils.config.constants import Exchange, EXCHANGE_NAMES_KR

# 실제 사용되는 심볼 목록
EXCHANGE_SYMBOLS = {
    "binancespot": ['ADA', 'ANIME', 'AUCTION', 'BERA', 'DOGE', 'ENS', 'ETC', 'HBAR', 'JUP', 'KAITO', 'LAYER', 'LINK', 'NEAR', 'SEI', 'SHELL', 'SOL', 'STX', 'SUI', 'TRUMP', 'VANA', 'WLD', 'XLM', 'XRP'],
    "binancefuture": ['ADA', 'ANIME', 'AUCTION', 'BERA', 'DOGE', 'ENS', 'ETC', 'HBAR', 'JUP', 'KAITO', 'LAYER', 'LINK', 'NEAR', 'SEI', 'SHELL', 'SOL', 'STX', 'SUI', 'TRUMP', 'VANA', 'WLD', 'XLM', 'XRP'],
    "bybitspot": ['ADA', 'ANIME', 'ATH', 'AVL', 'BERA', 'DOGE', 'ENS', 'ETC', 'HBAR', 'JUP', 'LINK', 'NEAR', 'ONDO', 'SEI', 'SOL', 'STX', 'SUI', 'TRUMP'],
    "bybitfuture": ['ADA', 'ANIME', 'ATH', 'AVL', 'BERA', 'DOGE', 'ENS', 'ETC', 'HBAR', 'JUP', 'LINK', 'NEAR', 'ONDO', 'SEI', 'SOL', 'STX', 'SUI', 'TRUMP'],
    "upbit": ['ADA', 'ANIME', 'ATH', 'AUCTION', 'BERA', 'DOGE', 'ENS', 'ETC', 'HBAR', 'JUP', 'KAITO', 'LAYER', 'LINK', 'NEAR', 'ONDO', 'SEI', 'SOL', 'STX', 'SUI', 'TRUMP', 'USDT', 'VANA', 'XLM', 'XRP'],
    "bithumb": ['ADA', 'AVL', 'DOGE', 'ONDO', 'SHELL', 'SOL', 'TRUMP', 'USDT', 'WLD', 'XRP']
}

# 테스트 설정
RECONNECT_INTERVAL_MIN = 60  # 최소 재연결 간격 (초)
RECONNECT_INTERVAL_MAX = 120  # 최대 재연결 간격 (초)
TEST_DURATION = 1800  # 테스트 지속 시간 (초) - 30분
FORCE_DISCONNECT_METHODS = ["close", "error"]  # 강제 연결 끊기 방법
MAX_RECONNECT_TESTS = 10  # 각 거래소별 최대 재연결 테스트 횟수

@dataclass
class ExchangeStats:
    """거래소별 통계 데이터"""
    name: str
    connections: int = 0
    reconnections: int = 0
    disconnections: int = 0
    messages_received: int = 0
    errors: int = 0
    last_message_time: float = 0.0
    last_connection_time: float = 0.0
    last_disconnection_time: float = 0.0
    last_error_time: float = 0.0
    last_error_message: str = ""
    forced_disconnects: int = 0
    snapshot_count: int = 0
    reconnection_snapshot_count: int = 0
    symbols_with_snapshot: Set[str] = field(default_factory=set)
    reconnection_pending: bool = False
    is_connected: bool = False
    
    # 한글 이름 추가
    kr_name: str = ""

class AllExchangesReconnectTester:
    """모든 거래소 웹소켓 재연결 테스트 클래스"""
    
    def __init__(self):
        """초기화"""
        self.logger = logger
        
        # 웹소켓 및 큐 초기화
        self.websockets = {}
        self.message_queue = asyncio.Queue()
        self.stop_event = asyncio.Event()
        
        # 설정
        self.settings = {
            "depth": 100
        }
        
        # 거래소별 통계
        self.exchange_stats: Dict[str, ExchangeStats] = {
            "binancespot": ExchangeStats(name="binancespot", kr_name=EXCHANGE_NAMES_KR[Exchange.BINANCE.value]),
            "binancefuture": ExchangeStats(name="binancefuture", kr_name=EXCHANGE_NAMES_KR[Exchange.BINANCE_FUTURE.value]),
            "bybitspot": ExchangeStats(name="bybitspot", kr_name=EXCHANGE_NAMES_KR[Exchange.BYBIT.value]),
            "bybitfuture": ExchangeStats(name="bybitfuture", kr_name=EXCHANGE_NAMES_KR[Exchange.BYBIT_FUTURE.value]),
            "upbit": ExchangeStats(name="upbit", kr_name=EXCHANGE_NAMES_KR[Exchange.UPBIT.value]),
            "bithumb": ExchangeStats(name="bithumb", kr_name=EXCHANGE_NAMES_KR[Exchange.BITHUMB.value])
        }
        
        # 테스트 통계
        self.test_stats = {
            "test_start_time": 0,
            "total_connections": 0,
            "total_reconnections": 0,
            "total_disconnections": 0,
            "total_messages": 0,
            "total_errors": 0,
            "total_snapshots": 0,
            "total_reconnection_snapshots": 0
        }
        
        # 시그널 핸들러 등록
        signal.signal(signal.SIGINT, self.handle_sigint)
    
    def handle_sigint(self, signum, frame):
        """SIGINT 핸들러"""
        self.logger.info("Ctrl+C 감지, 테스트를 종료합니다...")
        self.stop_event.set()
    
    def connection_status_callback(self, exchange: str, status: str):
        """연결 상태 콜백"""
        now = time.time()
        
        # 해당 거래소 통계 가져오기
        stats = self.exchange_stats.get(exchange)
        if not stats:
            self.logger.warning(f"알 수 없는 거래소: {exchange}")
            return
        
        kr_name = stats.kr_name
        
        if status == "connect_attempt":
            self.logger.info(f"{kr_name} 연결 시도 중...")
            
        elif status == "connect":
            stats.connections += 1
            stats.last_connection_time = now
            stats.is_connected = True
            self.test_stats["total_connections"] += 1
            
            # 첫 연결이 아닌 경우 재연결로 간주
            if stats.connections > 1 or stats.forced_disconnects > 0:
                stats.reconnections += 1
                self.test_stats["total_reconnections"] += 1
                self.logger.info(f"{kr_name} 재연결 성공 ({stats.reconnections}번째)")
                
                # 재연결 후 스냅샷 수신 대기 상태로 설정
                stats.reconnection_pending = True
                stats.symbols_with_snapshot.clear()

                # 업비트의 경우 재연결 시 자동으로 모든 심볼의 스냅샷을 수신
                if exchange == "upbit":
                    self.logger.info(f"{kr_name} 재연결 후 모든 심볼의 스냅샷 자동 수신 대기")
            else:
                self.logger.info(f"{kr_name} 초기 연결 성공")
                
        elif status == "disconnect":
            stats.disconnections += 1
            stats.last_disconnection_time = now
            stats.is_connected = False
            self.test_stats["total_disconnections"] += 1
            self.logger.info(f"{kr_name} 연결 끊김 ({stats.disconnections}번째)")
            
            # 재연결 대기 상태로 설정
            if stats.reconnection_pending:
                self.logger.warning(f"{kr_name} 이미 재연결 대기 중입니다.")
            else:
                stats.reconnection_pending = True
                self.logger.info(f"{kr_name} 재연결 대기 상태로 설정")
            
        elif status == "message":
            # 메시지 수신 시간 업데이트
            stats.last_message_time = now
            stats.messages_received += 1
            self.test_stats["total_messages"] += 1
            
        elif status == "error":
            stats.errors += 1
            stats.last_error_time = now
            self.test_stats["total_errors"] += 1
            self.logger.error(f"{kr_name} 오류 발생")
            
        elif status == "snapshot_request":
            self.logger.info(f"{kr_name} 스냅샷 요청 중...")
            
        elif status == "snapshot_parsed":
            # 스냅샷 수신 시간 업데이트
            stats.snapshot_count += 1
            self.test_stats["total_snapshots"] += 1
            
            # 재연결 후 스냅샷 수신인 경우
            if stats.reconnection_pending:
                stats.reconnection_snapshot_count += 1
                self.test_stats["total_reconnection_snapshots"] += 1
                self.logger.info(f"{kr_name} 재연결 후 스냅샷 수신 ({stats.reconnection_snapshot_count}번째)")
    
    async def process_queue(self):
        """메시지 큐 처리"""
        while not self.stop_event.is_set():
            try:
                # 큐에서 메시지 가져오기 (0.1초 타임아웃)
                try:
                    message = await asyncio.wait_for(self.message_queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue
                
                # 메시지 타입 확인
                if isinstance(message, dict):
                    exchange = message.get("exchangename", "unknown")
                    msg_type = message.get("type", "unknown")
                    symbol = message.get("symbol", "unknown")
                    
                    # 거래소 통계 업데이트
                    stats = self.exchange_stats.get(exchange)
                    if stats:
                        stats.messages_received += 1
                        stats.last_message_time = time.time()
                        
                        # 스냅샷 메시지 처리
                        if msg_type == "snapshot":
                            stats.symbols_with_snapshot.add(symbol)
                            
                            # 재연결 후 스냅샷 수신인 경우
                            if stats.reconnection_pending:
                                self.logger.info(f"{stats.kr_name} 재연결 후 {symbol} 스냅샷 수신")
                                
                                # 업비트의 경우 모든 메시지가 스냅샷이므로 별도 처리
                                if exchange == "upbit":
                                    stats.reconnection_snapshot_count += 1
                                    self.test_stats["total_reconnection_snapshots"] += 1
                                
                                # 모든 심볼에 대해 스냅샷을 받았는지 확인
                                if len(stats.symbols_with_snapshot) == len(EXCHANGE_SYMBOLS[exchange]):
                                    stats.reconnection_pending = False
                                    self.logger.info(f"{stats.kr_name} 모든 심볼({len(EXCHANGE_SYMBOLS[exchange])}개)에 대한 스냅샷 수신 완료")
                
            except Exception as e:
                self.logger.error(f"메시지 처리 중 오류: {e}")
                self.test_stats["total_errors"] += 1
    
    async def force_disconnect(self):
        """강제 연결 끊기"""
        while not self.stop_event.is_set():
            try:
                # 최대 테스트 횟수에 도달한 경우 중지
                total_forced_disconnects = sum(stats.forced_disconnects for stats in self.exchange_stats.values())
                if total_forced_disconnects >= MAX_RECONNECT_TESTS:
                    self.logger.info(f"최대 테스트 횟수({MAX_RECONNECT_TESTS}회)에 도달하여 테스트를 종료합니다.")
                    self.stop_event.set()
                    break
                
                # 랜덤 시간 대기
                wait_time = random.uniform(5, 10)
                await asyncio.sleep(wait_time)
                
                # 연결된 거래소 목록 가져오기
                connected_exchanges = [
                    exchange for exchange, stats in self.exchange_stats.items()
                    if stats.is_connected and exchange in self.websockets
                ]
                
                if not connected_exchanges:
                    self.logger.info("연결된 거래소가 없어 강제 연결 끊기를 스킵합니다.")
                    continue
                
                # 랜덤으로 거래소 선택
                target_exchange = random.choice(connected_exchanges)
                ws = self.websockets.get(target_exchange)
                stats = self.exchange_stats[target_exchange]
                kr_name = stats.kr_name
                
                if not ws or not ws.is_connected:
                    self.logger.info(f"{kr_name} 이미 연결이 끊긴 상태이므로 강제 연결 끊기를 스킵합니다.")
                    continue
                
                # 강제 연결 끊기 방법 선택
                disconnect_method = random.choice(FORCE_DISCONNECT_METHODS)
                stats.forced_disconnects += 1
                
                self.logger.info(f"{kr_name} 강제 연결 끊기 시도 ({stats.forced_disconnects}번째, 방법: {disconnect_method})")
                
                if disconnect_method == "close":
                    # 정상적인 방법으로 연결 끊기
                    if ws and ws.ws:
                        try:
                            await ws.ws.close()
                            self.logger.info(f"{kr_name} 웹소켓 close() 메소드로 연결 끊기 성공")
                        except Exception as close_e:
                            self.logger.error(f"{kr_name} 웹소켓 close() 메소드 호출 중 오류: {close_e}")
                    
                elif disconnect_method == "error":
                    # 오류 발생시켜 연결 끊기
                    if ws and ws.ws:
                        try:
                            # 웹소켓 객체를 직접 조작하여 오류 발생
                            ws.ws = None
                            self.logger.info(f"{kr_name} 웹소켓 객체를 None으로 설정하여 오류 발생시킴")
                        except Exception as err_e:
                            self.logger.error(f"{kr_name} 웹소켓 객체 조작 중 오류: {err_e}")
                
            except Exception as e:
                self.logger.error(f"강제 종료 중 오류: {e}")
                await asyncio.sleep(1)  # 오류 발생 시 잠시 대기
    
    async def print_stats(self):
        """주기적으로 통계 출력"""
        while not self.stop_event.is_set():
            try:
                await asyncio.sleep(5)  # 5초마다 통계 출력
                
                # 테스트 지속 시간 계산
                test_duration = time.time() - self.test_stats["test_start_time"]
                
                # 거래소별 통계 출력
                for exchange, stats in self.exchange_stats.items():
                    if exchange in self.websockets:
                        kr_name = stats.kr_name
                        
                        # 마지막 메시지 수신 시간 계산
                        last_msg_elapsed = "N/A"
                        if stats.last_message_time > 0:
                            last_msg_elapsed = f"{time.time() - stats.last_message_time:.1f}초"
                            
                        # 마지막 연결 시간 계산
                        last_conn_elapsed = "N/A"
                        if stats.last_connection_time > 0:
                            last_conn_elapsed = f"{time.time() - stats.last_connection_time:.1f}초"
                        
                        # 통계 출력
                        self.logger.info(
                            f"{kr_name} 통계 ({test_duration:.1f}초): "
                            f"연결 {stats.connections}회, "
                            f"재연결 {stats.reconnections}회, "
                            f"메시지 {stats.messages_received}개, "
                            f"스냅샷 {stats.snapshot_count}개, "
                            f"재연결 후 스냅샷 {stats.reconnection_snapshot_count}개"
                        )
                        
                        # 마지막 메시지/연결 시간 출력
                        self.logger.info(
                            f"{kr_name} 마지막 메시지: {last_msg_elapsed} 전, "
                            f"마지막 연결: {last_conn_elapsed} 전"
                        )
                
            except Exception as e:
                self.logger.error(f"통계 출력 중 오류: {e}")
    
    async def create_websockets(self) -> Dict[str, Tuple[str, List[str]]]:
        """웹소켓 인스턴스 생성 및 심볼 매핑 반환"""
        # 바이낸스 현물
        self.websockets["binancespot"] = BinanceSpotWebsocket(self.settings)
        self.websockets["binancespot"].connection_status_callback = self.connection_status_callback
        self.websockets["binancespot"].set_output_queue(self.message_queue)
        
        # 바이낸스 선물
        self.websockets["binancefuture"] = BinanceFutureWebsocket(self.settings)
        self.websockets["binancefuture"].connection_status_callback = self.connection_status_callback
        self.websockets["binancefuture"].set_output_queue(self.message_queue)
        
        # 바이비트 현물
        self.websockets["bybitspot"] = BybitSpotWebsocket(self.settings)
        self.websockets["bybitspot"].connection_status_callback = self.connection_status_callback
        self.websockets["bybitspot"].set_output_queue(self.message_queue)
        
        # 바이비트 선물
        self.websockets["bybitfuture"] = BybitFutureWebsocket(self.settings)
        self.websockets["bybitfuture"].connection_status_callback = self.connection_status_callback
        self.websockets["bybitfuture"].set_output_queue(self.message_queue)
        
        # 업비트
        self.websockets["upbit"] = UpbitWebsocket(self.settings)
        self.websockets["upbit"].connection_status_callback = self.connection_status_callback
        self.websockets["upbit"].set_output_queue(self.message_queue)
        
        # 빗썸
        self.websockets["bithumb"] = BithumbSpotWebsocket(self.settings)
        self.websockets["bithumb"].connection_status_callback = self.connection_status_callback
        self.websockets["bithumb"].set_output_queue(self.message_queue)
        
        # 심볼 매핑 (거래소별로 동일한 심볼 사용)
        symbols_by_exchange = {
            "binancespot": EXCHANGE_SYMBOLS["binancespot"],
            "binancefuture": EXCHANGE_SYMBOLS["binancefuture"],
            "bybitspot": EXCHANGE_SYMBOLS["bybitspot"],
            "bybitfuture": EXCHANGE_SYMBOLS["bybitfuture"],
            "upbit": EXCHANGE_SYMBOLS["upbit"],
            "bithumb": EXCHANGE_SYMBOLS["bithumb"]
        }
        
        return symbols_by_exchange
    
    async def run_test(self):
        """테스트 실행"""
        self.logger.info(f"모든 거래소 웹소켓 재연결 테스트 시작")
        self.logger.info(f"테스트 심볼: {', '.join(EXCHANGE_SYMBOLS['binancespot'][:5])}... 외 {len(EXCHANGE_SYMBOLS['binancespot'])-5}개")
        self.logger.info(f"테스트 지속 시간: {TEST_DURATION}초, 최대 재연결 테스트: {MAX_RECONNECT_TESTS}회")
        
        try:
            # 테스트 시작 시간 기록
            self.test_stats["test_start_time"] = time.time()
            
            # 웹소켓 인스턴스 생성 및 심볼 매핑 가져오기
            symbols_by_exchange = await self.create_websockets()
            
            # 테스트 작업 시작
            tasks = []
            
            # 각 거래소별 웹소켓 연결 태스크 추가
            for exchange_name, ws in self.websockets.items():
                tasks.append(asyncio.create_task(
                    ws.start(symbols_by_exchange),
                    name=f"{exchange_name}_websocket"
                ))
            
            # 공통 태스크 추가
            tasks.extend([
                asyncio.create_task(self.process_queue(), name="process_queue"),
                asyncio.create_task(self.force_disconnect(), name="force_disconnect"),
                asyncio.create_task(self.print_stats(), name="print_stats")
            ])
            
            # 테스트 지속 시간 동안 실행
            self.logger.info(f"테스트는 {TEST_DURATION}초 동안 실행됩니다.")
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=TEST_DURATION)
            except asyncio.TimeoutError:
                self.logger.info(f"테스트 지속 시간({TEST_DURATION}초)이 경과하여 테스트를 종료합니다.")
                self.stop_event.set()
            
            # 모든 작업이 완료될 때까지 대기
            for task in tasks:
                try:
                    task.cancel()
                except:
                    pass
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
        except Exception as e:
            self.logger.error(f"테스트 실행 중 오류: {e}")
        finally:
            # 테스트 종료 처리
            for exchange_name, ws in self.websockets.items():
                try:
                    await ws.stop()
                    self.logger.info(f"[{exchange_name}] 웹소켓 정상 종료")
                except Exception as e:
                    self.logger.error(f"[{exchange_name}] 웹소켓 종료 중 오류: {e}")
            
            # 최종 통계 출력
            await self.print_final_report()
    
    async def print_final_report(self):
        """최종 테스트 보고서 출력"""
        # 테스트 경과 시간
        elapsed = time.time() - self.test_stats["test_start_time"]
        
        self.logger.info(f"\n{'=' * 80}")
        self.logger.info(f"테스트 최종 보고서 (총 실행 시간: {elapsed:.1f}초)")
        self.logger.info(f"{'=' * 80}")
        
        # 전체 통계 출력
        self.logger.info(f"총 연결: {self.test_stats['total_connections']}, "
                        f"총 재연결: {self.test_stats['total_reconnections']}, "
                        f"총 연결 끊김: {self.test_stats['total_disconnections']}")
        self.logger.info(f"총 메시지: {self.test_stats['total_messages']}, "
                        f"총 오류: {self.test_stats['total_errors']}")
        self.logger.info(f"총 스냅샷: {self.test_stats['total_snapshots']}, "
                        f"재연결 후 스냅샷: {self.test_stats['total_reconnection_snapshots']}")
        
        # 거래소별 상세 통계 출력
        self.logger.info(f"\n거래소별 상세 통계:")
        for exchange, stats in self.exchange_stats.items():
            self.logger.info(f"\n[{exchange}]")
            self.logger.info(f"  - 연결 횟수: {stats.connections}")
            self.logger.info(f"  - 재연결 횟수: {stats.reconnections}")
            self.logger.info(f"  - 연결 끊김 횟수: {stats.disconnections}")
            self.logger.info(f"  - 강제 연결 끊기 횟수: {stats.forced_disconnects}")
            self.logger.info(f"  - 수신 메시지 수: {stats.messages_received}")
            self.logger.info(f"  - 오류 수: {stats.errors}")
            self.logger.info(f"  - 스냅샷 수: {stats.snapshot_count}")
            self.logger.info(f"  - 재연결 후 스냅샷 수: {stats.reconnection_snapshot_count}")
            self.logger.info(f"  - 스냅샷 수신 심볼 수: {len(stats.symbols_with_snapshot)}/{len(EXCHANGE_SYMBOLS[exchange])}")
        
        # 테스트 결과 평가
        self.logger.info(f"\n테스트 결과 평가:")
        
        # 각 거래소별 평가
        for exchange, stats in self.exchange_stats.items():
            result = "성공" if stats.reconnections > 0 and stats.reconnection_snapshot_count > 0 else "실패"
            self.logger.info(f"[{exchange}] 재연결 테스트: {result}")
            
            # 상세 평가
            if stats.reconnections > 0:
                if stats.reconnection_snapshot_count > 0:
                    self.logger.info(f"  - 재연결 후 스냅샷 수신 확인됨 ({stats.reconnection_snapshot_count}개)")
                else:
                    self.logger.warning(f"  - 재연결은 되었으나 스냅샷 수신 실패")
            else:
                if stats.forced_disconnects > 0:
                    self.logger.warning(f"  - 연결 끊김 후 재연결 실패")
                else:
                    self.logger.info(f"  - 연결 끊김이 발생하지 않음")
        
        self.logger.info(f"{'=' * 80}")


async def main():
    """메인 함수"""
    # 테스터 인스턴스 생성 및 실행
    tester = AllExchangesReconnectTester()
    await tester.run_test()


if __name__ == "__main__":
    # 테스트 실행
    asyncio.run(main()) 