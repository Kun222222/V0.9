# file: orderbook/websocket/bybit_future_websocket.py

import asyncio
import json
import time
import aiohttp
from fastapi import websockets
from websockets import connect
from typing import Dict, List, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, get_raw_logger
from crosskimp.ob_collector.orderbook.websocket.base_ws import BaseWebsocket
from crosskimp.ob_collector.orderbook.orderbook.base_ob import ValidationResult
from crosskimp.ob_collector.orderbook.orderbook.bybit_f_ob import BybitFutureOrderBookManager

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
        self.initial_connection = True  # 초기 연결 여부를 확인하기 위한 플래그
        depth = settings.get("websocket", {}).get("orderbook_depth", 10)
        self.orderbook_manager = BybitFutureOrderBookManager(depth)
        self.session: Optional[aiohttp.ClientSession] = None
        self.is_connected = False
        
        # Ping/Pong 설정
        self.ping_interval = 20
        self.ping_timeout = 10
        
        # 재연결 설정
        self.retry_delay = 1  # 초기 재연결 딜레이 1초
        self.current_retry = 0
        self.message_timeout = 30
        self._last_message_time = time.time()
        
        # raw 로거 초기화
        self.raw_logger = get_raw_logger("bybit_future")

    def log_raw_message(self, msg_type: str, message: str, symbol: str) -> None:
        """
        Raw 메시지 로깅
        Args:
            msg_type: 메시지 타입 (snapshot/depthUpdate)
            message: raw 메시지
            symbol: 심볼명
        """
        try:
            self.raw_logger.info(f"{msg_type}|{symbol}|{message}")
        except Exception as e:
            self.log_error(f"Raw 로깅 실패: {str(e)}")

    async def connect(self) -> bool:
        while True:  # 무한 재시도 루프
            try:
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "connect_attempt")
                
                # 초기 연결 시에만 0.5초 타임아웃 설정
                timeout = 0.5 if self.initial_connection else 30
                
                self.ws = await asyncio.wait_for(
                    connect(
                        self.ws_url,
                        ping_interval=self.ping_interval,
                        ping_timeout=self.ping_timeout,
                        compression=None
                    ),
                    timeout=timeout
                )
                
                if not self.session:
                    self.session = aiohttp.ClientSession()
                    
                self.is_connected = True
                self.current_retry = 0
                self.stats.connection_start_time = time.time()
                self.initial_connection = False  # 초기 연결 완료 표시
                
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "connect")
                return True
                
            except asyncio.TimeoutError:
                self.current_retry += 1
                self.log_error(f"웹소켓 연결 타임아웃 | 시도={self.current_retry}회차, timeout={timeout}초, 초기연결={'예' if self.initial_connection else '아니오'}")
                await asyncio.sleep(self.retry_delay)
                continue
                
            except Exception as e:
                self.current_retry += 1
                self.log_error(f"웹소켓 연결 실패 | 시도={self.current_retry}회차, 초기연결={'예' if self.initial_connection else '아니오'}, error={str(e)}")
                await asyncio.sleep(self.retry_delay)
                continue

    async def subscribe(self, symbols: List[str]):
        try:
            if not self.ws:
                self.log_error("WebSocket이 연결되지 않음")
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
                    if self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "subscribe")
                    await self.ws.send(json.dumps(sub_msg))
                    
                    # 구독 응답 대기를 위한 짧은 지연
                    try:
                        async with asyncio.timeout(2.0):  # 2초 타임아웃
                            await asyncio.sleep(0.5)
                    except asyncio.TimeoutError:
                        self.log_error("구독 지연 타임아웃")
                        continue
                        
                except Exception as e:
                    self.log_error(f"구독 메시지 전송 실패: {e}")
                    continue
                
                # 청크 간 지연
                try:
                    async with asyncio.timeout(1.0):
                        await asyncio.sleep(0.2)
                except asyncio.TimeoutError:
                    pass
                
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe_complete")
            
        except Exception as e:
            self.log_error(f"구독 처리 중 오류 발생: {str(e)}", exc_info=True)
            raise

    async def parse_message(self, message: str) -> Optional[dict]:
        try:
            data = json.loads(message)
            if data.get("op") == "subscribe":
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe_response")
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
            self.log_error(f"메시지 파싱 실패: {e}, raw={message}")
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
            self.log_error(f"{symbol} 오더북 객체 없음")
            return
        result = await orderbook.update(parsed)
        if result.is_valid:
            self.orderbook_manager.update_sequence(symbol, parsed["sequence"])
            ob_dict = orderbook.to_dict()
            if self.output_queue:
                await self.output_queue.put(("bybitfuture", ob_dict))
        else:
            self.log_error(f"{symbol} 오더북 업데이트 실패: {result.error_messages}")
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
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "snapshot_request")
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.log_raw_message("snapshot", json.dumps(data), market)
                    if data.get("retCode") == 0:
                        result = data.get("result", {})
                        if self.connection_status_callback:
                            self.connection_status_callback(self.exchangename, "snapshot_received")
                        return self._parse_snapshot_data(result, market)
                    else:
                        self.log_error(f"{market} 스냅샷 응답 에러: {data}")
                else:
                    self.log_error(f"{market} 스냅샷 요청 실패: status={resp.status}")
        except Exception as e:
            self.log_error(f"{market} 스냅샷 요청 예외: {e}")
        return None

    def _parse_snapshot_data(self, data: dict, symbol: str) -> dict:
        try:
            ts = data.get("ts", int(time.time()*1000))
            seq = data.get("seq", 0)
            bids = [[float(b[0]), float(b[1])] for b in data.get("b", [])]
            asks = [[float(a[0]), float(a[1])] for a in data.get("a", [])]
            return {
                "exchangename": "bybitfuture",
                "symbol": symbol.replace("USDT", ""),
                "bids": bids,
                "asks": asks,
                "timestamp": ts,
                "sequence": seq,
                "type": "snapshot"
            }
        except Exception as e:
            self.log_error(f"{symbol} 스냅샷 파싱 실패: {e}")
            return {}

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        while not self.stop_event.is_set():
            try:
                symbols = symbols_by_exchange.get(self.exchangename.lower(), [])
                if not symbols:
                    self.log_error("구독할 심볼 없음")
                    return

                # 공통 로깅을 위한 부모 클래스 start 호출
                await super().start(symbols_by_exchange)

                # 연결
                await self.connect()
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "connect")

                # 구독
                await self.subscribe(symbols)

                # 메시지 처리 루프
                while not self.stop_event.is_set():
                    try:
                        message = await asyncio.wait_for(
                            self.ws.recv(),
                            timeout=self.message_timeout
                        )
                        self.stats.last_message_time = time.time()
                        self.stats.message_count += 1
                        
                        parsed = await self.parse_message(message)
                        if parsed:
                            await self.handle_parsed_message(parsed)
                            if self.connection_status_callback:
                                self.connection_status_callback(self.exchangename, "message")
                            
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        self.log_error(f"메시지 처리 실패: {str(e)}")
                        break

            except Exception as e:
                self.current_retry += 1
                self.log_error(f"연결 실패: {str(e)}, retry={self.current_retry}")
                
                if self.current_retry > 5:
                    self.log_error("최대 재시도 횟수 초과")
                    break
                    
                await asyncio.sleep(self.retry_delay)
                continue
                
            finally:
                if self.ws:
                    await self.ws.close()
                self.is_connected = False
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "disconnect")

    async def stop(self) -> None:
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "stop")
        self.stop_event.set()
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                self.log_error(f"웹소켓 종료 실패: {e}")

        self.is_connected = False
        self.orderbook_manager.clear_all()
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "disconnect")