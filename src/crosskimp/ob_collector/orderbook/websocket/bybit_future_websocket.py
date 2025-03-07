# file: orderbook/websocket/bybit_future_websocket.py

import asyncio
import json
import time
import aiohttp
from fastapi import websockets
from websockets import connect
from typing import Dict, List, Optional

from utils.logging.logger import get_unified_logger
from orderbook.websocket.base_websocket import BaseWebsocket
from orderbook.orderbook.base_orderbook import ValidationResult
from orderbook.orderbook.bybit_future_orderbook_manager import BybitFutureOrderBookManager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class BybitFutureWebsocket(BaseWebsocket):
    """
    Bybit 선물(Linear) WebSocket 클라이언트
    - wss://stream.bybit.com/v5/public/linear
    - snapshot + delta 방식 오더북
    """
    def __init__(self, settings: dict):
        super().__init__(settings, "bybitfuture")
        self.ws_url = "wss://stream.bybit.com/v5/public/linear"
        depth = settings.get("websocket", {}).get("orderbook_depth", 10)
        self.orderbook_manager = BybitFutureOrderBookManager(depth)
        self.session: Optional[aiohttp.ClientSession] = None
        self.is_connected = False
        self.ping_interval = 20
        self.ping_timeout = 10
        self.connection_timeout = 30
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5
        self.current_reconnect_attempt = 0
        self.message_timeout = 30  # 메시지 타임아웃 추가
        self._last_message_time = time.time()

        # 부모 클래스의 설정 사용
        reconnect_cfg = settings.get("websocket", {}).get("reconnect", {})
        self.max_reconnect_attempts = reconnect_cfg.get("max_attempts", 5)
        self.reconnect_delay = reconnect_cfg.get("delay", 5)
        self.current_reconnect_attempt = 0

        self.logger = logger  # bybit_future_logger 대신 unified_logger 사용

    async def connect(self) -> bool:
        max_retries = 3
        retry_delay = 1.0
        
        for attempt in range(max_retries):
            try:
                self.ws = await connect(
                    self.ws_url,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                    compression=None,
                    open_timeout=10
                )
                if not self.session:
                    self.session = aiohttp.ClientSession()
                self.is_connected = True
                self.current_reconnect_attempt = 0
                self.logger.info(
                    f"[{self.exchangename}] WebSocket 연결 성공 | "
                    f"url={self.ws_url}, "
                    f"ping_interval={self.ping_interval}s"
                )
                return True
                
            except Exception as e:
                self.logger.error(
                    f"[{self.exchangename}] WebSocket 연결 실패 | "
                    f"attempt={attempt + 1}/{max_retries}, "
                    f"error={str(e)}",
                    exc_info=True
                )
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                else:
                    raise

    async def subscribe(self, symbols: List[str]):
        try:
            self.logger.info(f"[{self.exchangename}] 구독 시작 | symbols={symbols}")
            
            if not self.ws:
                self.logger.error(f"[{self.exchangename}] WebSocket이 연결되지 않음")
                return
            
            # 심볼을 10개씩 나누어 구독
            chunk_size = 10
            for i in range(0, len(symbols), chunk_size):
                chunk = symbols[i:i + chunk_size]
                
                sub_msg = {
                    "op": "subscribe",
                    "args": [f"orderbook.200.{symbol}USDT" for symbol in chunk]
                }
                
                try:
                    self.logger.info(f"[{self.exchangename}] 구독 메시지 전송: {sub_msg}")
                    await self.ws.send(json.dumps(sub_msg))
                    
                    # 구독 응답 대기를 위한 짧은 지연
                    try:
                        async with asyncio.timeout(2.0):  # 2초 타임아웃
                            await asyncio.sleep(0.5)
                    except asyncio.TimeoutError:
                        self.logger.warning(f"[{self.exchangename}] 구독 지연 타임아웃")
                        continue
                        
                except Exception as e:
                    self.logger.error(f"[{self.exchangename}] 구독 메시지 전송 실패: {e}")
                    continue
                
                # 청크 간 지연
                try:
                    async with asyncio.timeout(1.0):
                        await asyncio.sleep(0.2)
                except asyncio.TimeoutError:
                    pass
                
            self.logger.info(f"[{self.exchangename}] 구독 완료")
            
        except Exception as e:
            self.logger.error(
                f"[{self.exchangename}] 구독 처리 중 오류 발생 | "
                f"error={str(e)}",
                exc_info=True
            )
            raise

    async def parse_message(self, message: str) -> Optional[dict]:
        try:
            data = json.loads(message)
            if data.get("op") == "subscribe":
                self.logger.info(f"구독 응답: {data}")
                return None
            if "topic" in data and "data" in data and "type" in data:
                topic = data["topic"]
                raw_symbol = topic.split(".")[-1]
                symbol = raw_symbol.replace("USDT", "")
                self.log_raw_message("depthUpdate", message, symbol)
                msg_type = data["type"].lower()
                ob_data = data["data"]
                seq = ob_data.get("seq", 0)
                ts = ob_data.get("ts", int(time.time()*1000))
                bids = []
                for b in ob_data.get("b", []):
                    px, qty = float(b[0]), float(b[1])
                    bids.append([px, qty if qty > 0 else 0.0])
                asks = []
                for a in ob_data.get("a", []):
                    px, qty = float(a[0]), float(a[1])
                    asks.append([px, qty if qty > 0 else 0.0])
                return {
                    "exchangename": "bybitfuture",
                    "symbol": symbol,
                    "bids": bids,
                    "asks": asks,
                    "timestamp": ts,
                    "sequence": seq,
                    "type": msg_type
                }
            return None
        except Exception as e:
            self.logger.error(f"[{self.exchangename}] BybitFuture 메시지 파싱 실패: {e}, raw={message}")
            return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        symbol = parsed["symbol"]
        if not self.orderbook_manager.is_initialized(symbol):
            snap = await self.request_snapshot(f"{symbol.upper()}USDT")
            if snap:
                await self.orderbook_manager.initialize_orderbook(symbol, snap)
            return
        orderbook = self.orderbook_manager.get_orderbook(symbol)
        if not orderbook:
            self.logger.warning(f"[{self.exchangename}] {symbol} 오더북 객체 없음")
            return
        result = await orderbook.update(parsed)
        if result.is_valid:
            self.orderbook_manager.update_sequence(symbol, parsed["sequence"])
            ob_dict = orderbook.to_dict()
            # self.logger.info(f"[{self.exchangename}] {symbol} 오더북 업데이트 성공")
            if self.output_queue:
                await self.output_queue.put(("bybitfuture", ob_dict))
        else:
            self.logger.error(f"[{self.exchangename}] {symbol} 오더북 업데이트 실패: {result.error_messages}")
            snap = await self.request_snapshot(f"{symbol.upper()}USDT")
            if snap:
                await self.orderbook_manager.initialize_orderbook(symbol, snap)

    async def request_snapshot(self, market: str) -> Optional[dict]:
        try:
            if market.lower().startswith(self.exchangename.lower()):
                return None
            if not self.session:
                self.session = aiohttp.ClientSession()
            url = (f"https://api.bybit.com/v5/market/orderbook"
                   f"?category=linear&symbol={market}"
                   f"&limit={self.orderbook_manager.depth}")
            self.logger.info(f"[{self.exchangename}] {market} 스냅샷 요청: {url}")
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.log_raw_message("snapshot", json.dumps(data), market)
                    if data.get("retCode") == 0:
                        result = data.get("result", {})
                        raw_symbol = market.replace("USDT", "")
                        parsed = self._parse_snapshot_data(result, raw_symbol)
                        if parsed:
                            self.logger.info(f"[{self.exchangename}] {raw_symbol} 스냅샷 파싱 완료")
                        return parsed
                    else:
                        self.logger.error(f"[{self.exchangename}] {market} 스냅샷 응답 실패: {data}")
                else:
                    self.logger.error(f"[{self.exchangename}] {market} 스냅샷 HTTP 에러: {resp.status}")
        except Exception as e:
            self.logger.error(f"[{self.exchangename}] {market} 스냅샷 요청 실패: {e}")
        return None

    def _parse_snapshot_data(self, data: dict, symbol: str) -> dict:
        seq = data.get("seq", 0)
        ts = data.get("ts", int(time.time()*1000))
        b_raw = data.get("b", [])
        a_raw = data.get("a", [])
        bids = [ [float(b[0]), float(b[1])] for b in b_raw if float(b[1]) > 0 ][:self.orderbook_manager.depth]
        asks = [ [float(a[0]), float(a[1])] for a in a_raw if float(a[1]) > 0 ][:self.orderbook_manager.depth]
        return {
            "exchangename": "bybitfuture",
            "symbol": symbol,
            "bids": bids,
            "asks": asks,
            "timestamp": ts,
            "sequence": seq,
            "type": "snapshot"
        }

    async def _reconnect(self):
        """재연결 처리"""
        try:
            self.current_reconnect_attempt += 1
            if self.current_reconnect_attempt > self.max_reconnect_attempts:
                self.logger.error("최대 재연결 시도 횟수 초과")
                return
                
            self.logger.info(f"[{self.exchangename}] 재연결 시도 #{self.current_reconnect_attempt}")
            
            # 기존 연결 정리
            if self.ws:
                try:
                    await self.ws.close()
                except Exception as e:
                    self.logger.error(f"[{self.exchangename}] 연결 종료 중 오류: {e}")
                self.ws = None
                
            # 연결 상태 업데이트
            self.is_connected = False
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "disconnect")
                
            # 재연결 전 대기
            await asyncio.sleep(self.reconnect_delay)
            
        except Exception as e:
            self.logger.error(f"[{self.exchangename}] 재연결 처리 중 오류: {e}")

    async def _handle_message(self, message: str):
        """메시지 처리"""
        try:
            self._last_message_time = time.time()
            
            # 메트릭 업데이트
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "message")
            
            # 메시지 파싱 및 처리
            parsed = await self.parse_message(message)
            if parsed:
                await self.handle_parsed_message(parsed)
                
        except Exception as e:
            self.logger.error(f"[{self.exchangename}] 메시지 처리 중 오류: {e}")

    async def start(self, symbols_by_exchange: Dict[str, List[str]]):
        try:
            symbols = symbols_by_exchange.get("bybitfuture", [])
            if not symbols:
                self.logger.warning("구독할 심볼이 없음")
                return
                
            while not self.stop_event.is_set():
                try:
                    if not await self.connect():
                        self.logger.error("연결 실패")
                        await asyncio.sleep(5)
                        continue
                    
                    # 연결 성공 시 콜백 호출
                    if self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "connect")
                    
                    # 구독 시도
                    try:
                        async with asyncio.timeout(300):
                            await self.subscribe(symbols)
                    except asyncio.TimeoutError:
                        self.logger.error("구독 타임아웃")
                        await self._reconnect()
                        continue
                    
                    # 메시지 수신 루프
                    while not self.stop_event.is_set():
                        try:
                            async with asyncio.timeout(self.message_timeout):
                                message = await self.ws.recv()
                                await self._handle_message(message)
                        except asyncio.TimeoutError:
                            if not await self._check_connection():
                                break
                        except Exception as e:
                            self.logger.error(f"[{self.exchangename}] 메시지 처리 중 오류: {e}")
                            break
                            
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger.error(f"[{self.exchangename}] 연결 오류: {e}")
                    await self._reconnect()
                    
        except asyncio.CancelledError:
            self.logger.info("작업 취소됨")
        finally:
            await self.stop()

    async def _check_connection(self) -> bool:
        """연결 상태 확인 및 필요시 ping 전송"""
        try:
            if not self.ws:
                return False
                
            await self.ws.ping()
            return True
            
        except Exception as e:
            logger.error(f"[BybitFuture] 연결 확인 중 오류: {e}")
            return False

    async def stop(self) -> None:
        """종료 처리"""
        try:
            self.logger.info("종료 처리 시작")
            self.stop_event.set()
            
            if self.ws:
                try:
                    await self.ws.close()
                    self.logger.info("WebSocket 연결 종료")
                except Exception as e:
                    self.logger.warning(f"연결 종료 오류: {e}")
                self.ws = None
                
            if self.session:
                await self.session.close()
                self.session = None
                
            self.orderbook_manager.clear_all()
            self.is_connected = False
            
            # 연결 상태 업데이트
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "disconnect")
                
            self.logger.info("종료 처리 완료")
            
        except Exception as e:
            self.logger.error(f"종료 처리 중 오류: {e}")