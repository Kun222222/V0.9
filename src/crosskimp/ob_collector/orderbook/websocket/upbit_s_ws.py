# file: orderbook/websocket/upbit_websocket.py

import asyncio
import json
import time
from websockets import connect
from typing import Dict, List, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.websocket.base_ws_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.orderbook.upbit_s_ob import UpbitOrderBookManager

logger = get_unified_logger()

class UpbitWebsocket(BaseWebsocketConnector):
    """
    Upbit WebSocket 클라이언트
    - URL: wss://api.upbit.com/websocket/v1
    - 구독: orderbook (심볼별 "KRW-{symbol}")
    """
    def __init__(self, settings: dict):
        super().__init__(settings, "upbit")
        self.ws_url = "wss://api.upbit.com/websocket/v1"
        depth = settings.get("websocket", {}).get("orderbook_depth", 10)
        self.orderbook_manager = UpbitOrderBookManager(depth=depth)
        self.is_connected = False
        self.initialized_symbols = set()  # 초기화된 심볼 추적
        self.last_ping_time = 0
        self.ping_interval = 60  # 60초 (서버 타임아웃 120초의 절반)
        self.ping_timeout = 10   # 10초 내에 응답 필요

    async def connect(self) -> bool:
        try:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect_attempt")
                
            self.ws = await connect(
                self.ws_url,
                ping_interval=None,
                compression=None
            )
            self.is_connected = True
            
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect")
            return True
            
        except Exception as e:
            self.log_error(f"웹소켓 연결 실패: {str(e)}", exc_info=True)
            return False

    async def subscribe(self, symbols: List[str]) -> None:
        try:
            if not self.ws:
                self.log_error("웹소켓이 연결되지 않음")
                return
            
            markets = [f"KRW-{s.upper()}" for s in symbols]
            
            # 웹소켓 구독 메시지 전송
            sub_message = [
                {"ticket": f"upbit_orderbook_{int(time.time())}"},
                {
                    "type": "orderbook",
                    "codes": markets,
                    "isOnlyRealtime": False  # 스냅샷 포함 요청
                }
            ]
            
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe")
            await self.ws.send(json.dumps(sub_message))
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe_complete")
            
        except Exception as e:
            self.log_error(f"구독 처리 중 오류 발생: {str(e)}")
            raise

    async def parse_message(self, message: str) -> Optional[dict]:
        try:
            if isinstance(message, bytes):
                message = message.decode("utf-8")
            
            # PING 응답 처리
            if message == '{"status":"UP"}':
                return None
            
            data = json.loads(message)
            if data.get("type") != "orderbook":
                return None

            symbol = data.get("code", "").replace("KRW-","")
            if not symbol:
                logger.error("심볼 정보 누락")
                return None

            ts = data.get("timestamp", int(time.time()*1000))
            msg_type = "delta"  # 업비트 웹소켓은 실시간 델타 업데이트
            units = data.get("orderbook_units", [])
            bids = []
            asks = []
            for unit in units:
                bid_price = float(unit.get("bid_price",0))
                bid_size = float(unit.get("bid_size",0))
                ask_price = float(unit.get("ask_price",0))
                ask_size = float(unit.get("ask_size",0))
                if bid_price > 0:
                    bids.append({'price': bid_price, 'size': bid_size})
                if ask_price > 0:
                    asks.append({'price': ask_price, 'size': ask_size})

            # bids와 asks가 비어 있는 경우 로깅
            if not bids:
                logger.error(f"{symbol} bids가 비어 있습니다: {units}")
            if not asks:
                logger.error(f"{symbol} asks가 비어 있습니다: {units}")

            return {
                "exchangename": "upbit",
                "symbol": symbol.upper(),
                "bids": bids,
                "asks": asks,
                "timestamp": ts,
                "sequence": ts,
                "type": msg_type
            }
        except Exception as e:
            logger.error(f"메시지 파싱 실패: {str(e)}, raw={{message[:200]}}...", exc_info=True)
            return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        try:
            symbol = parsed["symbol"]
            
            # 업비트는 매 메시지가 전체 오더북이므로, 항상 스냅샷으로 처리
            result = await self.orderbook_manager.initialize_orderbook(symbol, parsed)
            if result.is_valid:
                self.orderbook_manager.update_sequence(symbol, parsed["timestamp"])
                ob_dict = self.orderbook_manager.get_orderbook(symbol).to_dict()
                if self.output_queue:
                    await self.output_queue.put((self.exchangename, ob_dict))
            else:
                self.log_error(f"{symbol} 오더북 업데이트 실패")

        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {e}")

    async def _send_ping(self) -> None:
        """PING 메시지 전송"""
        try:
            if self.ws and self.is_connected:
                # 일반 PING 메시지 전송 방식 사용
                await self.ws.send("PING")
                self.last_ping_time = time.time()
                self.logger.debug(f"[Upbit] PING 전송")
        except Exception as e:
            self.log_error(f"PING 전송 실패: {str(e)}")

    async def _handle_pong(self, message: str) -> bool:
        """PONG 메시지 처리"""
        try:
            if message == '{"status":"UP"}':
                self.stats.last_pong_time = time.time()
                self.logger.debug(f"[Upbit] PONG 수신")
                return True
            return False
        except Exception as e:
            self.log_error(f"PONG 처리 실패: {str(e)}")
            return False

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
                if not self.is_connected:
                    delay = self.reconnect_strategy.next_delay()
                    self.log_error(f"연결 실패, {delay}초 후 재시도")
                    await asyncio.sleep(delay)
                    continue

                # 구독
                await self.subscribe(symbols)

                # 메시지 처리 루프
                while not self.stop_event.is_set():
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
                        
                        # PONG 메시지 체크
                        if await self._handle_pong(message):
                            continue

                        # 일반 메시지 처리
                        self.stats.last_message_time = time.time()
                        self.stats.message_count += 1

                        parsed = await self.parse_message(message)
                        if parsed:
                            await self.handle_parsed_message(parsed)
                            if self.connection_status_callback:
                                self.connection_status_callback(self.exchangename, "message")

                    except asyncio.TimeoutError:
                        # PONG 응답 체크
                        if time.time() - self.stats.last_pong_time > self.ping_timeout:
                            self.log_error("PONG 응답 타임아웃")
                            break
                        continue
                    except Exception as e:
                        self.log_error(f"메시지 처리 실패: {str(e)}")
                        break

            except Exception as e:
                delay = self.reconnect_strategy.next_delay()
                self.log_error(f"연결 실패: {str(e)}, {delay}초 후 재연결")
                await asyncio.sleep(delay)
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