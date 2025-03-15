# file: orderbook/websocket/binance_spot_websocket.py

import asyncio
import json
import time
import aiohttp
from typing import Dict, List, Optional

from crosskimp.config.ob_constants import Exchange, WebSocketState, STATUS_EMOJIS, WEBSOCKET_CONFIG

from crosskimp.ob_collector.orderbook.connection.binance_s_cn import BinanceWebSocketConnector
from crosskimp.ob_collector.orderbook.orderbook.binance_s_ob import BinanceSpotOrderBookManager

# ============================
# 바이낸스 현물 웹소켓 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BINANCE.value  # 거래소 코드
BINANCE_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이낸스 설정

# 오더북 관련 설정
DEFAULT_DEPTH = BINANCE_CONFIG["default_depth"]  # 기본 오더북 깊이
DEPTH_UPDATE_STREAM = BINANCE_CONFIG["depth_update_stream"]  # 깊이 업데이트 스트림 형식

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

class BinanceSpotWebsocket(BinanceWebSocketConnector):
    """
    바이낸스 현물 웹소켓
    - 메시지 파싱 및 처리
    - 오더북 업데이트 관리
    
    연결 관리는 BinanceWebSocketConnector 클래스에서 처리합니다.
    """
    def __init__(self, settings: dict):
        super().__init__(settings)
        self.manager = BinanceSpotOrderBookManager(depth=settings.get("depth", DEFAULT_DEPTH))
        self.manager.set_websocket(self)  # 웹소켓 연결 설정
        self.set_manager(self.manager)  # 부모 클래스에 매니저 설정

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

    async def subscribe(self, symbols: List[str]):
        """
        지정된 심볼 목록을 구독
        
        1. 웹소켓을 통해 실시간 업데이트 구독
        2. 각 심볼별로 REST API를 통해 스냅샷 요청
        3. 스냅샷을 오더북 매니저에 적용
        
        Args:
            symbols: 구독할 심볼 목록
        """
        if not symbols:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "warning")
            return

        # 부모 클래스의 subscribe 호출하여 웹소켓 구독 수행
        await super().subscribe(symbols)

        # 스냅샷
        for sym in symbols:
            snapshot = await self.manager.fetch_snapshot(sym)
            if snapshot:
                init_res = await self.manager.initialize_orderbook(sym, snapshot)
                if init_res.is_valid:
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

    async def process_message(self, message: str) -> None:
        """
        수신된 메시지 처리 (BinanceWebSocketConnector 클래스의 추상 메서드 구현)
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        parsed = await self.parse_message(message)
        if parsed and self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "message")
        if parsed:
            await self.handle_parsed_message(parsed)