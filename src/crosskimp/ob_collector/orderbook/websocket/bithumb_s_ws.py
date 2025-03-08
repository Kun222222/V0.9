# file: orderbook/websocket/bithumb_spot_websocket.py

import asyncio
import json
import time
from websockets import connect
from typing import Dict, List, Optional

from crosskimp.ob_collector.utils.logging.logger import get_raw_logger
from crosskimp.ob_collector.orderbook.websocket.base_ws import BaseWebsocket
from crosskimp.ob_collector.orderbook.orderbook.bithumb_s_ob import BithumbSpotOrderBookManager

def parse_bithumb_depth_update(msg_data: dict) -> Optional[dict]:
    """
    빗썸 웹소켓의 orderbookdepth 메시지를 공통 포맷으로 변환 (바이낸스 현물의 parse_binance_depth_update와 유사)
    """
    if msg_data.get("type") != "orderbookdepth":
        return None
    content = msg_data.get("content", {})
    order_list = content.get("list", [])
    if not order_list:
        return None

    raw_symbol = order_list[0].get("symbol", "")
    symbol = raw_symbol.replace("_KRW", "").upper()

    dt_str = content.get("datetime", str(int(time.time() * 1000)))
    try:
        dt_val = int(dt_str)
    except ValueError:
        dt_val = int(time.time() * 1000)

    # 파싱: 바이낸스와 유사하게 숫자형 변환 및 0 필터링
    bids = []
    asks = []
    for order in order_list:
        otype = order.get("orderType", "").lower()
        try:
            price = float(order["price"])
            qty = float(order["quantity"])
            if price <= 0 or qty <= 0:
                continue
            if otype == "bid":
                bids.append([price, qty])
            elif otype == "ask":
                asks.append([price, qty])
        except Exception:
            continue

    bids.sort(key=lambda x: x[0], reverse=True)
    asks.sort(key=lambda x: x[0])

    return {
        "exchangename": "bithumb",
        "symbol": symbol,
        "bids": bids,
        "asks": asks,
        "timestamp": dt_val,
        "sequence": dt_val,  # 빗썸은 timestamp를 sequence로 사용
        "type": "delta"
    }

class BithumbSpotWebsocket(BaseWebsocket):
    """
    빗썸 현물 웹소켓 클라이언트 (바이낸스 현물과 동일한 데이터 흐름)
    - 연결, 구독, 스냅샷 요청, 델타 메시지 수신 및 처리
    """
    def __init__(self, settings: dict):
        super().__init__(settings, "bithumb")
        self.ws_url = "wss://pubwss.bithumb.com/pub/ws"
        self.depth = settings.get("depth", 500)  # 내부 depth를 500으로 설정
        self.manager = BithumbSpotOrderBookManager(depth=self.depth)
        self.subscribed_symbols: List[str] = []
        self.ws = None
        
        # raw 로거 초기화
        self.raw_logger = get_raw_logger("bithumb_spot")

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        super().set_output_queue(queue)  # 부모 클래스의 메서드 호출 (기본 큐 설정 및 로깅)
        self.manager.output_queue = queue  # 오더북 매니저 큐 설정

    async def connect(self):
        try:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect_attempt")
                
            self.ws = await connect(
                self.ws_url,
                ping_interval=20,  # 20초마다 ping
                ping_timeout=10,   # 10초 내에 pong이 오지 않으면 연결 종료
                close_timeout=30,
                max_size=None,
                compression=None
            )
            self.is_connected = True
            self.stats.connection_start_time = time.time()
            
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect")
                
        except Exception as e:
            self.log_error(f"connect() 예외: {e}", exc_info=True)
            raise

    async def subscribe(self, symbols: List[str]):
        if not symbols:
            self.log_error("구독할 심볼이 없음")
            return

        # 각 심볼별로 스냅샷 요청 및 오더북 초기화를 수행
        for sym in symbols:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "snapshot_request")
            snap = await self.manager.fetch_snapshot(sym)
            if snap:
                init_res = await self.manager.initialize_orderbook(sym, snap)
                if not init_res.is_valid:
                    self.log_error(f"{sym} 스냅샷 적용 실패: {init_res.error_messages}")
                else:
                    if self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "snapshot_received")
                    self.subscribed_symbols.append(sym.upper())
            else:
                self.log_error(f"{sym} 스냅샷 요청 실패")

        # 구독 메시지 전송
        if self.subscribed_symbols:
            symbol_list = [f"{s.upper()}_KRW" for s in self.subscribed_symbols]
            sub_msg = {
                "type": "orderbookdepth",
                "symbols": symbol_list,
                "tickTypes": ["1M"]
            }
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe")
            await self.ws.send(json.dumps(sub_msg))
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe_complete")

    async def _parse_message(self, data: dict) -> Optional[dict]:
        """빗썸 특화 파싱 로직"""
        # 구독 응답 처리
        if "status" in data and "resmsg" in data:
            if data["status"] == "0000":
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe_response")
            else:
                self.log_error(f"구독 실패: {data['resmsg']}")
            return None

        # Delta 메시지 처리
        if data.get("type") == "orderbookdepth":
            return data

        return None

    def _extract_symbol(self, parsed: dict) -> str:
        """빗썸 특화 심볼 추출"""
        content = parsed.get("content", {})
        order_list = content.get("list", [])
        if order_list:
            raw_symbol = order_list[0].get("symbol", "")
            return raw_symbol.replace("_KRW", "").upper()
        return "UNKNOWN"

    async def handle_parsed_message(self, parsed: dict) -> None:
        try:
            evt = parse_bithumb_depth_update(parsed)
            if evt:
                symbol = evt["symbol"]
                res = await self.manager.update(symbol, evt)
                if not res.is_valid:
                    self.log_error(f"{symbol} 업데이트 실패: {res.error_messages}")
        except Exception as e:
            self.log_error(f"handle_parsed_message 예외: {e}", exc_info=True)

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        syms = symbols_by_exchange.get(self.exchangename.lower(), [])
        if not syms:
            self.log_error("구독할 심볼이 없음")
            return

        # 공통 로깅을 위한 부모 클래스 start 호출
        await super().start(symbols_by_exchange)

        while not self.stop_event.is_set():
            try:
                await self.connect()
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "connect")

                await self.subscribe(syms)

                last_message_time = time.time()
                while not self.stop_event.is_set():
                    try:
                        message = await asyncio.wait_for(
                            self.ws.recv(), 
                            timeout=self.health_check_interval
                        )
                        current_time = time.time()
                        message_delay = current_time - last_message_time
                        last_message_time = current_time
                        
                        # 메시지 지연이 30초 이상이면 연결 재설정
                        if message_delay > 30:
                            self.log_error(f"높은 메시지 지연 감지: {message_delay:.1f}초 -> 연결 재설정")
                            break

                        self.stats.last_message_time = current_time
                        self.stats.message_count += 1

                        parsed = await self.parse_message(message)
                        if parsed:
                            await self.handle_parsed_message(parsed)
                            if self.connection_status_callback:
                                self.connection_status_callback(self.exchangename, "message")

                    except asyncio.TimeoutError:
                        current_time = time.time()
                        if current_time - last_message_time > 30:
                            self.log_error(f"메시지 수신 타임아웃: {current_time - last_message_time:.1f}초 -> 연결 재설정")
                            break
                        continue
                    except Exception as e:
                        self.log_error(f"메시지 수신 오류: {e}", exc_info=True)
                        break

            except Exception as conn_e:
                delay = self.reconnect_strategy.next_delay()
                self.log_error(f"연결 실패: {conn_e}, {delay}s 후 재연결", exc_info=False)
                await asyncio.sleep(delay)
            finally:
                if self.ws:
                    try:
                        await self.ws.close()
                    except:
                        pass
                self.is_connected = False
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "disconnect")

    async def stop(self) -> None:
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "stop")
        self.stop_event.set()
        if self.ws:
            await self.ws.close()
        self.is_connected = False
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "disconnect")

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