# file: orderbook/websocket/bybit_spot_websocket.py

import asyncio
import json
import time
from websockets import connect
from typing import Dict, List, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, get_raw_logger
from crosskimp.ob_collector.orderbook.websocket.base_ws import BaseWebsocket
from crosskimp.ob_collector.orderbook.orderbook.bybit_spot_orderbook_manager import BybitSpotOrderBookManager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

def parse_bybit_depth_update(data: dict) -> Optional[dict]:
    """
    Bybit에서 온 depth 메시지를 파싱 (바이낸스와 유사한 형태로 리턴)
    """
    try:
        topic = data.get("topic", "")
        if "orderbook" not in topic:
            return None

        parts = topic.split(".")
        if len(parts) < 3:
            # unified_logger.debug(f"[BybitSpot] parse_bybit_depth_update: invalid topic format={topic}")
            return None

        sym_str = parts[-1]  # 예: "BTCUSDT"
        symbol = sym_str.replace("USDT", "")  # 예: "BTC"
        msg_type = data.get("type", "delta").lower()  # "snapshot" or "delta"
        ob_data = data.get("data", {})

        seq = ob_data.get("u", 0)
        ts = ob_data.get("ts", int(time.time() * 1000))

        bids = []
        for b in ob_data.get("b", []):
            px, qty = float(b[0]), float(b[1])
            # qty가 0이면 제거 로직
            bids.append([px, qty if qty > 0 else 0.0])

        asks = []
        for a in ob_data.get("a", []):
            px, qty = float(a[0]), float(a[1])
            asks.append([px, qty if qty > 0 else 0.0])

        # unified_logger.debug(
        #     f"[BybitSpot] parse_bybit_depth_update: symbol={symbol}, seq={seq}, "
        #     f"bids_len={len(bids)}, asks_len={len(asks)}, msg_type={msg_type}"
        # )

        return {
            "exchangename": "bybit",
            "symbol": symbol,
            "bids": bids,
            "asks": asks,
            "timestamp": ts,
            "sequence": seq,
            "type": msg_type
        }
    except Exception as e:
        logger.error(f"[BybitSpot] parse_bybit_depth_update error: {e}", exc_info=True)
        return None

class BybitSpotWebsocket(BaseWebsocket):
    """
    Bybit 현물 WebSocket
    """
    def __init__(self, settings: dict):
        super().__init__(settings, "bybit")
        self.ws_url = "wss://stream.bybit.com/v5/public/spot"
        self.initial_connection = True  # 초기 연결 여부를 확인하기 위한 플래그
        
        # depth 설정 통합 (settings.json의 depth_level 사용)
        self.depth_level = settings.get("connection", {}).get("websocket", {}).get("depth_level", 50)
        self.orderbook_manager = BybitSpotOrderBookManager(self.depth_level)

        # 재연결 설정은 부모 클래스에서 처리하므로 제거
        self.current_retry = 0
        self.retry_delay = 1  # 초기 재연결 딜레이 1초

        # Ping/Pong 설정 추가
        self.ping_interval = 20
        self.ping_timeout = 10

        self.logger = logger
        
        # raw 로거 초기화
        self.raw_logger = get_raw_logger("bybit_spot")

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        super().set_output_queue(queue)  # 부모 클래스의 메서드 호출 (기본 큐 설정 및 로깅)
        self.orderbook_manager.set_output_queue(queue)  # 오더북 매니저 큐 설정
        # 부모 클래스에서 이미 로깅하므로 여기서는 생략

    async def connect(self) -> None:
        while True:  # 무한 재시도 루프
            try:
                logger.info(
                    f"[BybitSpot] 웹소켓 연결 시도 | "
                    f"시도={self.current_retry + 1}회차, "
                    f"초기연결={'예' if self.initial_connection else '아니오'}, "
                    f"url={self.ws_url}"
                )
                
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
                
                self.is_connected = True
                self.current_retry = 0
                self.stats.connection_start_time = time.time()
                self.initial_connection = False  # 초기 연결 완료 표시
                
                logger.info(
                    f"[BybitSpot] 웹소켓 연결 성공 | "
                    f"시도={self.current_retry + 1}회차, "
                    f"url={self.ws_url}, "
                    f"ping_interval={self.ping_interval}초"
                )
                break  # 연결 성공시 루프 종료
                
            except asyncio.TimeoutError:
                self.current_retry += 1
                logger.warning(
                    f"[BybitSpot] 웹소켓 연결 타임아웃 | "
                    f"시도={self.current_retry}회차, "
                    f"timeout={timeout}초, "
                    f"초기연결={'예' if self.initial_connection else '아니오'}"
                )
                await asyncio.sleep(self.retry_delay)
                continue
                
            except Exception as e:
                self.current_retry += 1
                logger.error(
                    f"[BybitSpot] 웹소켓 연결 실패 | "
                    f"시도={self.current_retry}회차, "
                    f"초기연결={'예' if self.initial_connection else '아니오'}, "
                    f"error={str(e)}"
                )
                await asyncio.sleep(self.retry_delay)
                continue

    async def subscribe(self, symbols: List[str]) -> None:
        """
        바이비트 웹소켓 구독 - 심볼을 10개씩 나누어 구독
        """
        try:
            # 심볼을 10개씩 나누어 구독
            BATCH_SIZE = 10
            for i in range(0, len(symbols), BATCH_SIZE):
                batch_symbols = symbols[i:i + BATCH_SIZE]
                
                # 현재 배치의 args 리스트 생성
                args = []
                for sym in batch_symbols:
                    market = f"{sym}USDT"
                    args.append(f"orderbook.{self.depth_level}.{market}")  # depth_level 사용

                # 구독 메시지 전송
                msg = {
                    "op": "subscribe",
                    "args": args
                }
                logger.info(f"[BybitSpot] subscribing batch {i//BATCH_SIZE + 1}: symbols={batch_symbols}, depth={self.depth_level}, msg={msg}")
                await self.ws.send(json.dumps(msg))
                
                # 각 배치 사이에 짧은 딜레이
                await asyncio.sleep(0.1)
            
            logger.info(f"[BybitSpot] Successfully subscribed to {len(symbols)} symbols in {(len(symbols)-1)//BATCH_SIZE + 1} batches")
            
        except Exception as e:
            self.log_error(f"[BybitSpot] subscribe error: {str(e)}")
            raise

    async def parse_message(self, message: str) -> Optional[dict]:
        """
        바이낸스와 동일하게 raw json -> dict 변환
        """
        try:
            data = json.loads(message)
            if data.get("op") == "subscribe":
                logger.debug(f"[BybitSpot] subscribe response: {data}")
                return None

            if "topic" in data and "data" in data:
                topic = data.get("topic", "")
                if "orderbook" in topic:
                    parts = topic.split(".")
                    if len(parts) >= 3:
                        symbol = parts[-1].replace("USDT", "")
                        self.log_raw_message("depthUpdate", message, symbol)
                return data
            return None
        except Exception as e:
            self.log_error(f"[BybitSpot] parse_message error: {e}")
            return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        """
        파싱된 메시지를 실제 orderbook_manager에 전달
        """
        try:
            evt = parse_bybit_depth_update(parsed)
            if not evt:
                return

            symbol = evt["symbol"]
            res = await self.orderbook_manager.update(symbol, evt)
            if not res.is_valid:
                logger.error(
                    f"[BybitSpot] {symbol} orderbook update fail: {res.error_messages}"
                )
        except Exception as e:
            self.log_error(f"[BybitSpot] handle_parsed_message error: {e}")

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        """
        웹소켓 시작 및 초기화
        1. 연결
        2. 스냅샷 요청 (병렬)
        3. 구독 시작
        4. 메시지 처리 루프
        """
        while not self.stop_event.is_set():
            try:
                symbols = symbols_by_exchange.get(self.exchangename.lower(), [])
                if not symbols:
                    logger.warning(f"[BybitSpot] No symbols to subscribe for {self.exchangename}")
                    return

                # 1. 연결
                await self.connect()
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "connect")
                
                # 2. 스냅샷 요청 (병렬 처리)
                snapshot_tasks = []
                for sym in symbols:
                    task = asyncio.create_task(self._initialize_symbol(sym))
                    snapshot_tasks.append(task)
                
                # 모든 스냅샷 요청 완료 대기
                results = await asyncio.gather(*snapshot_tasks, return_exceptions=True)
                valid_symbols = []
                
                for sym, result in zip(symbols, results):
                    if isinstance(result, Exception):
                        logger.error(f"[BybitSpot] Failed to initialize {sym}: {str(result)}")
                        continue
                    if result:  # True인 경우만 구독 대상에 포함
                        valid_symbols.append(sym)
                
                if not valid_symbols:
                    logger.error("[BybitSpot] No valid symbols to subscribe")
                    return
                    
                # 3. 구독 시작
                await self.subscribe(valid_symbols)
                logger.info(f"[BybitSpot] Successfully subscribed to {len(valid_symbols)} symbols")

                # 4. 메시지 처리 루프
                while not self.stop_event.is_set():
                    try:
                        message = await asyncio.wait_for(self.ws.recv(), timeout=30)
                        self.stats.last_message_time = time.time()
                        self.stats.message_count += 1
                        
                        parsed = await self.parse_message(message)
                        if parsed:
                            await self.handle_parsed_message(parsed)
                            
                    except asyncio.TimeoutError:
                        logger.debug("[BybitSpot] recv timeout - checking connection health")
                        continue
                    except Exception as e:
                        logger.error(f"[BybitSpot] message loop error: {str(e)}")
                        break

            except Exception as e:
                self.current_retry += 1
                logger.error(
                    f"[BybitSpot] connection error: {str(e)}, retry={self.current_retry}"
                )
                
                if self.current_retry > 5:
                    logger.error("[BybitSpot] max retries exceeded -> stop")
                    break
                    
                await asyncio.sleep(self.retry_delay)
                continue
                
            finally:
                if self.ws:
                    await self.ws.close()
                self.is_connected = False
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "disconnect")

        logger.info("[BybitSpot] websocket stopped")

    async def _initialize_symbol(self, symbol: str) -> bool:
        """
        단일 심볼에 대한 스냅샷 초기화
        """
        try:
            # unified_logger.info(f"[BybitSpot] requesting snapshot for {symbol}...")
            snapshot = await self.orderbook_manager.fetch_snapshot(symbol)
            
            if not snapshot:
                logger.error(f"[BybitSpot] {symbol} snapshot request fail")
                return False
                
            init_res = await self.orderbook_manager.initialize_orderbook(symbol, snapshot)
            if not init_res.is_valid:
                logger.error(f"[BybitSpot] {symbol} snapshot init fail: {init_res.error_messages}")
                return False
                
            logger.info(f"[BybitSpot] {symbol} orderbook initialized with snapshot seq={snapshot.get('sequence')}")
            return True
            
        except Exception as e:
            logger.error(f"[BybitSpot] {symbol} initialization error: {str(e)}")
            return False

    async def stop(self):
        logger.info("[BybitSpot] stop() called")
        self.stop_event.set()
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                logger.warning(f"[BybitSpot] websocket close error: {e}")

        self.is_connected = False
        self.orderbook_manager.clear_all()
        logger.info("[BybitSpot] stopped & cleared.")

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
            logger.error(f"[{self.exchangename}] Raw 로깅 실패: {str(e)}")