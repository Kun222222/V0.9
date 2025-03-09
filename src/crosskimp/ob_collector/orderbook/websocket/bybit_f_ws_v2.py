# file: orderbook/websocket/bybit_f_ws_v2.py

import asyncio
import json
import time
import aiohttp
from fastapi import websockets
from websockets import connect
from typing import Dict, List, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, get_raw_logger
from crosskimp.ob_collector.orderbook.websocket.base_ws import BaseWebsocket
from crosskimp.ob_collector.orderbook.orderbook.bybit_f_ob_v2 import BybitFutureOrderBookManagerV2

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class BybitFutureWebSocketV2(BaseWebsocket):
    """
    Bybit 선물(Linear) WebSocket V2
    - wss://stream.bybit.com/v5/public/linear
    - 공식 문서 기반 구현
    - 핑퐁 메커니즘 (20초)
    - 스냅샷/델타 처리
    """
    def __init__(self, settings: dict):
        super().__init__(settings, "bybitfuture2")
        self.ws_url = "wss://stream.bybit.com/v5/public/linear"
        self.depth_level = settings.get("connection", {}).get("websocket", {}).get("depth_level", 50)
        self.orderbook_manager = BybitFutureOrderBookManagerV2(self.depth_level)
        
        # Bybit specific settings
        self.ping_interval = 20  # 20초마다 ping
        self.ping_timeout = 10
        self.max_symbols_per_subscription = 10  # 한 번에 최대 10개 심볼만 구독 가능
        
        # 연결 상태 관리
        self.is_connected = False
        self.current_retry = 0
        self.initial_connection = True  # 초기 연결 여부를 확인하기 위한 플래그
        self.initial_retry_delay = 0.5  # 초기 연결 시 0.5초 간격
        self.normal_retry_delay = 1.0   # 일반적인 재연결 시 1초 간격

        # raw 로거 초기화
        self.raw_logger = get_raw_logger("bybit_future")

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        super().set_output_queue(queue)  # 부모 클래스의 메서드 호출
        self.orderbook_manager.set_output_queue(queue)  # 오더북 매니저 큐 설정

    async def connect(self) -> None:
        while True:
            try:
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "connect_attempt")
                
                # 초기 연결 시도인지 여부에 따라 타임아웃 설정
                timeout = 0.5 if self.initial_connection else 30
                retry_delay = self.initial_retry_delay if self.initial_connection else self.normal_retry_delay
                
                logger.info(
                    f"[Bybit] 웹소켓 연결 시도 | "
                    f"시도={self.current_retry + 1}회차, "
                    f"timeout={timeout}초, "
                    f"초기연결={'예' if self.initial_connection else '아니오'}"
                )
                
                self.ws = await asyncio.wait_for(
                    connect(
                        self.ws_url,
                        ping_interval=self.ping_interval,
                        ping_timeout=self.ping_timeout,
                        compression=None
                    ),
                    timeout=timeout
                )
                
                self.is_connected = True
                self.current_retry = 0
                self.stats.connection_start_time = time.time()
                self.initial_connection = False  # 연결 성공 시 초기 연결 플래그 해제
                
                logger.info(f"[Bybit] 웹소켓 연결 성공")
                
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "connect")
                break
                
            except asyncio.TimeoutError:
                self.current_retry += 1
                self.log_error(
                    f"웹소켓 연결 타임아웃 | "
                    f"시도={self.current_retry}회차, "
                    f"timeout={timeout}초, "
                    f"초기연결={'예' if self.initial_connection else '아니오'}"
                )
                await asyncio.sleep(retry_delay)
                continue
                
            except Exception as e:
                self.current_retry += 1
                self.log_error(
                    f"웹소켓 연결 실패 | "
                    f"시도={self.current_retry}회차, "
                    f"초기연결={'예' if self.initial_connection else '아니오'}, "
                    f"error={str(e)}"
                )
                await asyncio.sleep(retry_delay)
                continue

    async def subscribe(self, symbols: List[str]) -> None:
        try:
            total_batches = (len(symbols) + self.max_symbols_per_subscription - 1) // self.max_symbols_per_subscription
            logger.info(f"[Bybit] 구독 시작 | 총 {len(symbols)}개 심볼, {total_batches}개 배치로 나눔")
            
            # 심볼을 10개씩 나누어 구독
            for i in range(0, len(symbols), self.max_symbols_per_subscription):
                batch_symbols = symbols[i:i + self.max_symbols_per_subscription]
                batch_num = (i // self.max_symbols_per_subscription) + 1
                
                # 현재 배치의 args 리스트 생성
                args = []
                for sym in batch_symbols:
                    market = f"{sym}USDT"
                    args.append(f"orderbook.{self.depth_level}.{market}")

                # 구독 메시지 전송
                msg = {
                    "op": "subscribe",
                    "args": args
                }
                
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe")
                    
                await self.ws.send(json.dumps(msg))
                logger.info(f"[Bybit] 구독 요청 전송 | 배치 {batch_num}/{total_batches}, symbols={batch_symbols}")
                
                # 각 배치 사이에 짧은 딜레이
                await asyncio.sleep(0.1)
            
            logger.info(f"[Bybit] 전체 구독 요청 완료 | 총 {len(symbols)}개 심볼")
            
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe_complete")
            
        except Exception as e:
            self.log_error(f"구독 요청 실패: {str(e)}")
            raise

    async def parse_message(self, message: str) -> Optional[dict]:
        try:
            data = json.loads(message)
            
            # 구독 응답 처리
            if data.get("op") == "subscribe":
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe_response")
                return None

            # 오더북 메시지 처리
            if "topic" in data and "data" in data:
                topic = data.get("topic", "")
                if "orderbook" in topic:
                    parts = topic.split(".")
                    if len(parts) >= 3:
                        symbol = parts[-1].replace("USDT", "")
                        # raw 데이터 로깅 추가
                        msg_type = data.get("type", "delta")
                        self.log_raw_message(msg_type, message, symbol)
                        return data
            return None
            
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}")
            return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        try:
            if not parsed or "topic" not in parsed:
                return

            topic = parsed["topic"]
            if not topic.startswith("orderbook"):
                return

            # 심볼 추출 (예: orderbook.50.BTCUSDT -> BTC)
            symbol = topic.split(".")[-1].replace("USDT", "")
            msg_type = parsed.get("type", "unknown")
            
            # 오더북 매니저로 메시지 전달
            start_time = time.time()
            await self.orderbook_manager.update(symbol, parsed)
            processing_time = (time.time() - start_time) * 1000  # ms로 변환
            
            # 처리 시간 메트릭 기록
            if self.stats:
                self.stats.message_count += 1
                if not hasattr(self.stats, 'processing_times'):
                    self.stats.processing_times = []
                self.stats.processing_times.append(processing_time)
                
                # 로깅 주기 체크 (1000개 메시지마다)
                if self.stats.message_count % 1000 == 0:
                    avg_time = sum(self.stats.processing_times[-1000:]) / min(1000, len(self.stats.processing_times))
                    logger.info(
                        f"[Bybit] {symbol} 처리 성능 | "
                        f"평균={avg_time:.2f}ms, "
                        f"총={self.stats.message_count:,}개"
                    )
            
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "message")
                
        except Exception as e:
            self.log_error(f"메시지 처리 실패: {str(e)}")

    async def _send_ping(self) -> None:
        """Bybit 공식 핑 메시지 전송"""
        try:
            if self.ws and self.is_connected:
                ping_message = {
                    "req_id": str(int(time.time() * 1000)),  # 현재 시간을 req_id로 사용
                    "op": "ping"
                }
                await self.ws.send(json.dumps(ping_message))
                self.last_ping_time = time.time()
                logger.debug(f"[Bybit] PING 전송: {ping_message}")
        except Exception as e:
            self.log_error(f"PING 전송 실패: {str(e)}")

    async def _handle_pong(self, message: str) -> bool:
        """Bybit 공식 퐁 메시지 처리"""
        try:
            data = json.loads(message)
            if data.get("op") == "pong":
                self.last_pong_time = time.time()
                latency = (self.last_pong_time - self.last_ping_time) * 1000
                logger.debug(f"[Bybit] PONG 수신 | 레이턴시: {latency:.2f}ms")
                return True
            return False
        except Exception as e:
            self.log_error(f"PONG 처리 실패: {str(e)}")
            return False

    async def start_ping(self):
        """핑 전송 태스크 시작"""
        while not self.stop_event.is_set():
            try:
                await self._send_ping()
                await asyncio.sleep(self.ping_interval)
                
                # 퐁 타임아웃 체크
                if time.time() - self.last_pong_time > self.ping_timeout:
                    logger.error(f"[Bybit] PONG 응답 타임아웃 ({self.ping_timeout}초)")
                    await self.reconnect()
                    
            except Exception as e:
                self.log_error(f"PING 태스크 오류: {str(e)}")
                await asyncio.sleep(1)

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        while not self.stop_event.is_set():
            try:
                symbols = symbols_by_exchange.get(self.exchangename.lower(), [])
                if not symbols:
                    self.log_error("구독할 심볼이 없습니다")
                    return

                # 1. 연결
                await self.connect()
                
                # 2. 핑 태스크 시작
                self.ping_task = asyncio.create_task(self.start_ping())
                
                # 3. 구독 시작
                await self.subscribe(symbols)

                # 4. 메시지 처리 루프
                while not self.stop_event.is_set():
                    try:
                        message = await asyncio.wait_for(self.ws.recv(), timeout=30)
                        self.stats.last_message_time = time.time()
                        self.stats.message_count += 1
                        
                        # PONG 메시지 체크
                        if await self._handle_pong(message):
                            continue

                        parsed = await self.parse_message(message)
                        if parsed:
                            await self.handle_parsed_message(parsed)
                            
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        self.log_error(f"메시지 루프 오류: {str(e)}")
                        break

            except Exception as e:
                self.current_retry += 1
                self.log_error(f"연결 오류: {str(e)}, retry={self.current_retry}")
                
                if self.current_retry > 5:
                    self.log_error("최대 재시도 횟수 초과 -> 중지")
                    break
                    
                await asyncio.sleep(self.retry_delay)
                continue
                
            finally:
                if self.ping_task:
                    self.ping_task.cancel()
                    try:
                        await self.ping_task
                    except asyncio.CancelledError:
                        pass
                if self.ws:
                    await self.ws.close()
                self.is_connected = False
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "disconnect")

    async def stop(self):
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "stop")
        self.stop_event.set()
        if self.ws:
            await self.ws.close()
        self.is_connected = False
        self.orderbook_manager.clear_all()

    def log_raw_message(self, msg_type: str, message: str, symbol: str) -> None:
        """
        Raw 메시지 로깅
        Args:
            msg_type: 메시지 타입 (snapshot/depthUpdate)
            message: raw 메시지
            symbol: 심볼명
        """
        try:
            # unified_logger에 로깅
            logger.debug(f"[BybitFuture] {symbol} {msg_type} 메시지 수신")
            self.raw_logger.info(f"{msg_type}|{symbol}|{message}")
        except Exception as e:
            self.log_error(f"Raw 로깅 실패: {str(e)}")