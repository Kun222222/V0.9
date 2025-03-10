#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
바이낸스 선물 웹소켓 재연결 테스트 스크립트 (간소화 버전)

이 스크립트는 바이낸스 선물 웹소켓의 재연결 기능을 테스트합니다.
- 웹소켓 연결 수립
- 임의로 연결 끊기 (강제 종료)
- 재연결 확인
- 재연결 후 스냅샷 수신 확인

사용법:
    python test_binance_f_reconnect_simple.py
"""

import asyncio
import logging
import sys
import time
from typing import Dict, List

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

# 테스트 설정
TEST_SYMBOLS = ["BTC", "ETH"]  # 테스트할 심볼 목록

class SimpleReconnectTester:
    """간단한 재연결 테스트 클래스"""
    
    def __init__(self, testnet: bool = False):
        """초기화"""
        self.logger = logger
        self.testnet = testnet
        self.binance_ws = None
        self.message_queue = asyncio.Queue()
        self.stop_event = asyncio.Event()
        
        # 스냅샷 추적
        self.snapshot_count = 0
        self.reconnection_snapshot_count = 0
        self.is_reconnecting = False
        
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
    
    def connection_status_callback(self, exchange: str, status: str):
        """연결 상태 콜백"""
        if status == "connect_attempt":
            self.logger.info(f"연결 시도 중...")
            
        elif status == "connect":
            self.logger.info(f"연결 성공")
            
        elif status == "disconnect":
            self.logger.info(f"연결 끊김")
            self.is_reconnecting = True
            
        elif status == "snapshot_request":
            self.logger.info(f"스냅샷 요청 중...")
            
        elif status == "snapshot_parsed":
            self.snapshot_count += 1
            if self.is_reconnecting:
                self.reconnection_snapshot_count += 1
                self.logger.info(f"재연결 후 스냅샷 수신 ({self.reconnection_snapshot_count}번째)")
            else:
                self.logger.info(f"초기 스냅샷 수신 ({self.snapshot_count}번째)")
    
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
                    msg_type = message.get("type", "unknown")
                    symbol = message.get("symbol", "unknown")
                    
                    # 스냅샷 메시지 처리
                    if msg_type == "snapshot":
                        self.logger.info(f"{symbol} 스냅샷 메시지 수신")
                
            except Exception as e:
                self.logger.error(f"메시지 처리 중 오류: {e}")
    
    async def force_disconnect(self):
        """강제 연결 끊기"""
        # 초기 연결 후 5초 대기
        await asyncio.sleep(5)
        
        if not self.binance_ws or not self.binance_ws.is_connected:
            self.logger.info("웹소켓이 연결되어 있지 않습니다.")
            return
        
        self.logger.info("강제 연결 끊기 시도")
        
        # 웹소켓 객체를 None으로 설정하여 오류 발생
        self.binance_ws.ws = None
        self.logger.info("웹소켓 객체를 None으로 설정하여 오류 발생시킴")
    
    async def run_test(self):
        """테스트 실행"""
        self.logger.info(f"바이낸스 선물 웹소켓 재연결 테스트 시작 (testnet: {self.testnet})")
        self.logger.info(f"테스트 심볼: {', '.join(TEST_SYMBOLS)}")
        
        try:
            # 바이낸스 선물 웹소켓 인스턴스 생성
            self.binance_ws = BinanceFutureWebsocket(self.settings, testnet=self.testnet)
            
            # 콜백 및 큐 설정
            self.binance_ws.connection_status_callback = self.connection_status_callback
            self.binance_ws.set_output_queue(self.message_queue)
            
            # 테스트 작업 시작
            tasks = [
                asyncio.create_task(self.binance_ws.start({"binancefuture": TEST_SYMBOLS})),
                asyncio.create_task(self.process_queue()),
                asyncio.create_task(self.force_disconnect())
            ]
            
            # 20초 동안 실행
            self.logger.info("테스트는 20초 동안 실행됩니다.")
            await asyncio.sleep(20)
            self.stop_event.set()
            
            # 모든 작업이 완료될 때까지 대기
            await asyncio.gather(*tasks, return_exceptions=True)
            
        except Exception as e:
            self.logger.error(f"테스트 실행 중 오류: {e}")
        finally:
            # 테스트 종료 처리
            if self.binance_ws:
                await self.binance_ws.stop()
            
            # 최종 통계 출력
            self.logger.info(
                f"테스트 종료: "
                f"총 스냅샷 {self.snapshot_count}개, "
                f"재연결 후 스냅샷 {self.reconnection_snapshot_count}개"
            )
            
            # 테스트 결과 평가
            if self.reconnection_snapshot_count > 0:
                self.logger.info("테스트 결과: 성공 (재연결 후 스냅샷 수신 확인)")
            else:
                self.logger.warning("테스트 결과: 실패 (재연결 후 스냅샷 수신 실패)")


async def main():
    """메인 함수"""
    # 테스트넷 사용 여부 (기본값: False)
    use_testnet = False
    
    # 명령행 인수 처리
    if len(sys.argv) > 1 and sys.argv[1].lower() == "testnet":
        use_testnet = True
        logger.info("테스트넷 모드로 실행합니다.")
    
    # 테스터 인스턴스 생성 및 실행
    tester = SimpleReconnectTester(testnet=use_testnet)
    await tester.run_test()


if __name__ == "__main__":
    # 이벤트 루프 실행
    asyncio.run(main()) 