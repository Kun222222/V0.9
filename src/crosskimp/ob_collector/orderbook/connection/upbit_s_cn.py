# file: orderbook/connection/upbit_s_cn.py

import asyncio
import json
import time
from websockets import connect
import websockets.exceptions
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG
from crosskimp.ob_collector.orderbook.connection.base_ws_connector import BaseWebsocketConnector

# ============================
# 업비트 웹소켓 연결 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.UPBIT.value  # 거래소 코드
UPBIT_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 업비트 설정

# 웹소켓 연결 설정
WS_URL = UPBIT_CONFIG["ws_url"]  # 웹소켓 URL
PING_INTERVAL = UPBIT_CONFIG["ping_interval"]  # 핑 전송 간격 (초)
PING_TIMEOUT = UPBIT_CONFIG["ping_timeout"]  # 핑 응답 타임아웃 (초)
PONG_RESPONSE = UPBIT_CONFIG["pong_response"]  # 업비트 PONG 응답 형식

class UpbitWebSocketConnector(BaseWebsocketConnector):
    """
    업비트 웹소켓 연결 관리 클래스
    
    업비트 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    
    특징:
    - URL: wss://api.upbit.com/websocket/v1
    - 핑/퐁: 60초 간격으로 PING 전송, 10초 내 응답 필요
    """
    def __init__(self, settings: dict):
        """
        업비트 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, EXCHANGE_CODE)
        self.ws_url = WS_URL
        
        # 연결 관련 설정
        self.is_connected = False
        
        # 핑/퐁 설정
        self.last_ping_time = 0
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        
        # 메시지 처리 통계
        self.message_stats = {
            "total_received": 0,
            "ping_pong": 0
        }

    async def _do_connect(self):
        """
        실제 웹소켓 연결 수행 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        # 웹소켓 연결 수립
        self.ws = await connect(
            self.ws_url,
            ping_interval=None,  # 자체 핑 메커니즘 사용
            compression=None     # 압축 비활성화
        )

    async def subscribe(self, symbols: List[str]) -> None:
        """
        지정된 심볼에 대한 오더북 구독
        
        Args:
            symbols: 구독할 심볼 목록
        
        Raises:
            ConnectionError: 웹소켓이 연결되지 않은 경우
            Exception: 구독 처리 중 오류 발생
        """
        try:
            # 연결 상태 확인
            if not self.ws or not self.is_connected:
                error_msg = "웹소켓이 연결되지 않음"
                self.log_error(error_msg)
                raise ConnectionError(error_msg)
            
            # 심볼 형식 변환 (KRW-{symbol})
            markets = [f"KRW-{s.upper()}" for s in symbols]
            
            # 구독 메시지 생성
            sub_message = [
                {"ticket": f"upbit_orderbook_{int(time.time())}"},
                {
                    "type": "orderbook",
                    "codes": markets,
                    "isOnlyRealtime": False  # 스냅샷 포함 요청
                }
            ]
            
            # 구독 시작 상태 콜백
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe")
                
            # 구독 메시지 전송
            await self.ws.send(json.dumps(sub_message))
            
            # 구독 완료 상태 콜백
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe_complete")
                
            self.log_info(f"오더북 구독 완료: {len(symbols)}개 심볼")
            
        except Exception as e:
            error_msg = f"구독 처리 중 오류 발생: {str(e)}"
            self.log_error(error_msg)
            raise

    async def _send_ping(self) -> None:
        """
        PING 메시지 전송
        
        업비트 서버에 PING 메시지를 전송하여 연결 상태를 유지합니다.
        """
        try:
            if self.ws and self.is_connected:
                # 일반 PING 메시지 전송 방식 사용
                await self.ws.send("PING")
                self.last_ping_time = time.time()
                self.stats.last_ping_time = self.last_ping_time
                self.log_debug("PING 전송")
        except Exception as e:
            self.log_error(f"PING 전송 실패: {str(e)}")

    async def _handle_pong(self, message: str) -> bool:
        """
        PONG 메시지 처리
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: PONG 메시지인 경우 True, 아니면 False
        """
        try:
            if message == PONG_RESPONSE:
                now = time.time()
                self.stats.last_pong_time = now
                self.message_stats["ping_pong"] += 1
                
                # 레이턴시 계산 (PING-PONG 간격)
                if self.stats.last_ping_time > 0:
                    self.stats.latency_ms = (now - self.stats.last_ping_time) * 1000
                
                self.log_debug(f"PONG 수신 (레이턴시: {self.stats.latency_ms:.2f}ms)")
                return True
            return False
        except Exception as e:
            self.log_error(f"PONG 처리 실패: {str(e)}")
            return False

    async def _run_message_loop(self, symbols: List[str], tasks: List[asyncio.Task]) -> None:
        """
        메시지 처리 루프 실행 (BaseWebsocketConnector 템플릿 메서드 구현)
        
        Args:
            symbols: 구독한 심볼 목록
            tasks: 실행 중인 백그라운드 태스크 목록
        """
        
        while not self.stop_event.is_set() and self.is_connected:
            try:
                # PING 전송 체크
                now = time.time()
                if now - self.last_ping_time >= self.ping_interval:
                    await self._send_ping()

                # 메시지 수신
                message = await asyncio.wait_for(
                    self.ws.recv(),
                    timeout=self.ping_timeout
                )
                
                # 메시지 수신 통계 업데이트
                self.message_stats["total_received"] += 1
                self.stats.last_message_time = time.time()
                self.stats.message_count += 1
                
                # PONG 메시지 체크
                if await self._handle_pong(message):
                    continue

                # 메시지 처리 (자식 클래스에서 구현)
                await self.process_message(message)
                
            except asyncio.TimeoutError:
                # PONG 응답 체크
                if time.time() - self.stats.last_pong_time > self.ping_timeout:
                    self.log_error("PONG 응답 타임아웃")
                    break
                continue
            except websockets.exceptions.ConnectionClosed as e:
                error_msg = f"{self.exchange_korean_name} 웹소켓 연결 끊김: {str(e)}"
                self.log_error(error_msg)
                await self._send_telegram_notification("error", error_msg)
                self.is_connected = False
                if not self.stop_event.is_set():
                    await self.reconnect()
                break
            except Exception as e:
                self.log_error(f"메시지 처리 실패: {str(e)}")
                break

    async def process_message(self, message: str) -> None:
        """
        수신된 메시지 처리 (자식 클래스에서 구현)
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        # 이 메서드는 자식 클래스에서 구현해야 함
        pass

    async def _cleanup_connection(self) -> None:
        """연결 종료 및 정리"""
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                self.log_error(f"웹소켓 종료 실패: {e}")

        self.is_connected = False
        
        # 연결 종료 상태 콜백
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "disconnect") 