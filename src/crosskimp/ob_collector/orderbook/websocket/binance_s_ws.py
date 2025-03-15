# file: orderbook/websocket/binance_spot_websocket.py

import asyncio
import json
import time
import aiohttp
from websockets import connect
from typing import Dict, List, Optional
from crosskimp.config.ob_constants import Exchange, WebSocketState, STATUS_EMOJIS, WEBSOCKET_CONFIG

from crosskimp.ob_collector.orderbook.websocket.base_ws_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.orderbook.binance_s_ob import BinanceSpotOrderBookManager

# ============================
# 바이낸스 현물 웹소켓 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BINANCE.value  # 거래소 코드
BINANCE_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이낸스 설정

# 웹소켓 연결 설정
WS_URL = BINANCE_CONFIG["ws_url"]  # 웹소켓 URL
PING_INTERVAL = BINANCE_CONFIG["ping_interval"]  # 핑 전송 간격 (초)
PING_TIMEOUT = BINANCE_CONFIG["ping_timeout"]    # 핑 응답 타임아웃 (초)
HEALTH_CHECK_INTERVAL = BINANCE_CONFIG["health_check_interval"]  # 헬스 체크 간격 (초)

# 오더북 관련 설정
DEFAULT_DEPTH = BINANCE_CONFIG["default_depth"]  # 기본 오더북 깊이
DEPTH_UPDATE_STREAM = BINANCE_CONFIG["depth_update_stream"]  # 깊이 업데이트 스트림 형식

# 구독 관련 설정
SUBSCRIBE_CHUNK_SIZE = BINANCE_CONFIG["subscribe_chunk_size"]  # 한 번에 구독할 심볼 수
SUBSCRIBE_DELAY = BINANCE_CONFIG["subscribe_delay"]  # 구독 요청 간 딜레이 (초)

def parse_binance_depth_update(msg_data: dict) -> Optional[dict]:
    if msg_data.get("e") != "depthUpdate":
        return None
    symbol_raw = msg_data.get("s", "")
    symbol = symbol_raw.replace("USDT", "").upper()

    b_data = msg_data.get("b", [])
    a_data = msg_data.get("a", [])
    event_time = msg_data.get("E", 0)
    first_id = msg_data.get("U", 0)
    final_id = msg_data.get("u", 0)

    bids, asks = [], []
    for b in b_data:
        px, qty = float(b[0]), float(b[1])
        if px > 0 and qty != 0:
            bids.append([px, qty])
    for a in a_data:
        px, qty = float(a[0]), float(a[1])
        if px > 0 and qty != 0:
            asks.append([px, qty])

    return {
        "exchangename": "binance",
        "symbol": symbol,
        "bids": bids,
        "asks": asks,
        "timestamp": event_time,
        "first_update_id": first_id,
        "final_update_id": final_id,
        "sequence": final_id,
        "type": "delta"
    }

class BinanceSpotWebsocket(BaseWebsocketConnector):
    """
    바이낸스 현물 웹소켓
    - depth=500으로 사용하려면: settings["depth"] = 500
    - subscribe -> snapshot -> orderbook_manager
    - gap 발생 시 재스냅샷 X
    """
    def __init__(self, settings: dict):
        super().__init__(settings, "binance")
        self.manager = BinanceSpotOrderBookManager(depth=settings.get("depth", DEFAULT_DEPTH))
        self.manager.set_websocket(self)  # 웹소켓 연결 설정
        self.subscribed_symbols = set()
        self.ws = None
        self.session = None
        self.ws_url = WS_URL

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """출력 큐 설정"""
        # 부모 클래스의 output_queue 설정
        super().set_output_queue(queue)
        
        # 오더북 매니저의 output_queue 설정
        if hasattr(self, 'manager') and self.manager:
            self.manager.set_output_queue(queue)
            
        # 로깅 추가
        self.log_info(f"웹소켓 출력 큐 설정 완료 (큐 ID: {id(queue)})")
        
        # 큐 설정 확인
        if not hasattr(self.manager, '_output_queue') or self.manager._output_queue is None:
            self.log_error("오더북 매니저 큐 설정 실패!")
        else:
            self.log_info(f"오더북 매니저 큐 설정 확인 (큐 ID: {id(self.manager._output_queue)})")

    def set_connection_status_callback(self, callback):
        """연결 상태 콜백 설정 (오더북 매니저에도 전달)"""
        self.connection_status_callback = callback
        if hasattr(self, 'manager') and self.manager:
            self.manager.connection_status_callback = callback

    async def _do_connect(self):
        """
        실제 연결 로직 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        self.session = aiohttp.ClientSession()
        self.ws = await connect(
            self.ws_url,
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_TIMEOUT,
            compression=None
        )
        # is_connected와 connection_start_time은 부모 클래스의 connect 메소드에서 설정됨

    async def _after_connect(self):
        """
        연결 후 처리 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        # 재연결 성공 알림
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "connect")
            
        # 재연결 시 이미 구독된 심볼들에 대해 스냅샷 다시 요청
        if self.subscribed_symbols:
            self.log_info(f"재연결 후 스냅샷 다시 요청 (심볼: {len(self.subscribed_symbols)}개)")
            for sym in self.subscribed_symbols:
                snapshot = await self.manager.fetch_snapshot(sym)
                if snapshot:
                    init_res = await self.manager.initialize_orderbook(sym, snapshot)
                    if init_res.is_valid:
                        self.log_info(f"{sym} 재연결 후 스냅샷 초기화 성공")
                        if self.connection_status_callback:
                            self.connection_status_callback(self.exchangename, "snapshot")
                    else:
                        self.log_error(f"{sym} 재연결 후 스냅샷 초기화 실패: {init_res.error_messages}")
                else:
                    self.log_error(f"{sym} 재연결 후 스냅샷 요청 실패")

    async def _prepare_start(self, symbols: List[str]) -> None:
        """
        시작 전 초기화 및 설정 (BaseWebsocketConnector 템플릿 메서드 구현)
        
        Args:
            symbols: 구독할 심볼 목록
        """
        # 필요한 초기화 작업 수행
        pass

    async def _run_message_loop(self, symbols: List[str], tasks: List[asyncio.Task]) -> None:
        """
        메시지 처리 루프 실행 (BaseWebsocketConnector 템플릿 메서드 구현)
        
        Args:
            symbols: 구독한 심볼 목록
            tasks: 실행 중인 백그라운드 태스크 목록
        """
        while not self.stop_event.is_set() and self.is_connected:
            try:
                msg = await asyncio.wait_for(
                    self.ws.recv(), timeout=HEALTH_CHECK_INTERVAL
                )
                self.stats.last_message_time = time.time()
                self.stats.message_count += 1

                parsed = await self.parse_message(msg)
                if parsed and self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "message")
                if parsed:
                    await self.handle_parsed_message(parsed)
                    
            except asyncio.TimeoutError:
                # 타임아웃은 정상적인 상황일 수 있음 (메시지가 없는 경우)
                continue
                
            except Exception as e:
                self.log_error(f"메시지 루프 오류: {str(e)}")
                # 연결 오류 발생 시 루프 종료 (부모 클래스의 start 메소드에서 재연결 처리)
                break

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        """
        웹소켓 연결 시작 및 심볼 구독
        부모 클래스의 템플릿 메소드 패턴을 활용
        
        Args:
            symbols_by_exchange: 거래소별 구독할 심볼 목록
        """
        exchange_symbols = symbols_by_exchange.get("binance", [])
        if not exchange_symbols:
            self.log_error(f"{STATUS_EMOJIS['ERROR']} 구독할 심볼이 없습니다.")
            return

        # 부모 클래스의 start 메소드 호출 (템플릿 메소드 패턴)
        # 이 메소드는 _prepare_start, connect, subscribe, start_background_tasks, _run_message_loop 순서로 호출
        await super().start({"binance": exchange_symbols})

    async def subscribe(self, symbols: List[str]):
        if not symbols:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "warning")
            return

        chunk_size = SUBSCRIBE_CHUNK_SIZE
        for i in range(0, len(symbols), chunk_size):
            chunk = symbols[i:i+chunk_size]
            sub_params = [f"{sym.lower()}usdt{DEPTH_UPDATE_STREAM}" for sym in chunk]
            msg = {
                "method": "SUBSCRIBE",
                "params": sub_params,
                "id": int(time.time() * 1000)
            }
            await self.ws.send(json.dumps(msg))
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe")
            self.log_info(f"{len(chunk)}개 심볼 구독 요청 전송")
            await asyncio.sleep(SUBSCRIBE_DELAY)

        # 스냅샷
        for sym in symbols:
            snapshot = await self.manager.fetch_snapshot(sym)
            if snapshot:
                init_res = await self.manager.initialize_orderbook(sym, snapshot)
                if init_res.is_valid:
                    # 구독 성공한 심볼 추적을 위해 집합에 추가
                    self.subscribed_symbols.add(sym)
                    self.log_info(f"{sym} 스냅샷 초기화 성공 {STATUS_EMOJIS['CONNECTED']}")
                    if self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "snapshot")
                else:
                    self.log_error(f"{sym} 스냅샷 초기화 실패: {init_res.error_messages} {STATUS_EMOJIS['ERROR']}")
            else:
                self.log_error(f"{sym} 스냅샷 요청 실패 {STATUS_EMOJIS['ERROR']}")

    async def parse_message(self, message: str) -> Optional[dict]:
        try:
            data = json.loads(message)
            if "result" in data and "id" in data:
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe_response")
                return None
            if data.get("e") == "depthUpdate":
                symbol = data["s"].replace("USDT","").upper()
                # Raw 메시지 로깅
                self.log_raw_message("depthUpdate", message, symbol)
                return data
            return None
        except Exception as e:
            self.log_error(f"메시지 파싱 오류: {e} {STATUS_EMOJIS['ERROR']}")
            return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        try:
            evt = parse_binance_depth_update(parsed)
            if evt:
                symbol = evt["symbol"]
                res = await self.manager.update(symbol, evt)
                if not res.is_valid:
                    self.log_error(f"{symbol} 업데이트 실패: {res.error_messages} {STATUS_EMOJIS['ERROR']}")
        except Exception as e:
            self.log_error(f"메시지 처리 오류: {e} {STATUS_EMOJIS['ERROR']}")

    async def stop(self) -> None:
        """
        웹소켓 연결 종료
        """
        self.log_info(f"웹소켓 연결 종료 중... {STATUS_EMOJIS['DISCONNECTING']}")
        await super().stop()
        self.log_info(f"웹소켓 연결 종료 완료 {STATUS_EMOJIS['DISCONNECTED']}")