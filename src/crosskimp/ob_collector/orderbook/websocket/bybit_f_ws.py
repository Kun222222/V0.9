import asyncio
import json
import time
import aiohttp
from websockets import connect
from typing import Dict, List, Optional, Any

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, get_raw_logger
from crosskimp.ob_collector.orderbook.websocket.base_ws_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.orderbook.bybit_f_ob import BybitFutureOrderBookManager

# 바이빗 선물 웹소켓 전용 상수
BYBIT_FUTURE_PING_INTERVAL = 20  # 20초마다 ping (공식 문서 권장)
BYBIT_FUTURE_PING_TIMEOUT = 10   # 10초 이내 pong 응답 없으면 재연결

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class BybitFutureWebsocket(BaseWebsocketConnector):
    """
    Bybit 선물(Linear) WebSocket
    - wss://stream.bybit.com/v5/public/linear
    - 공식 문서 기반 구현
    - 핑퐁 메커니즘 (20초)
    - 스냅샷/델타 처리
    - 지수 백오프 재연결 전략
    - 공통 모듈 활용 강화
    """
    def __init__(self, settings: dict):
        super().__init__(settings, "bybitfuture")
        
        # 웹소켓 URL 설정
        self.ws_url = "wss://stream.bybit.com/v5/public/linear"
        if settings.get("testnet", False):
            self.ws_url = "wss://stream-testnet.bybit.com/v5/public/linear"
            
        # 오더북 설정
        self.depth_level = settings.get("connection", {}).get("websocket", {}).get("depth_level", 50)
        self.orderbook_manager = BybitFutureOrderBookManager(self.depth_level)
        
        # Bybit 특화 설정 - 상단 상수 사용
        self.ping_interval = BYBIT_FUTURE_PING_INTERVAL
        self.ping_timeout = BYBIT_FUTURE_PING_TIMEOUT
        self.max_symbols_per_subscription = 10  # 한 번에 최대 10개 심볼 구독
        
        # 연결 상태 관리
        self.is_connected = False
        self.last_ping_time = 0
        self.last_pong_time = 0
        self.ping_task = None
        self.health_check_task = None
        
        # 스냅샷 관리
        self.snapshot_received = set()  # 스냅샷을 받은 심볼 목록
        self.snapshot_pending = set()   # 스냅샷 요청 대기 중인 심볼 목록
        self.session: Optional[aiohttp.ClientSession] = None
        
        # raw 로거 초기화
        self.raw_logger = get_raw_logger("bybit_future")

    async def _do_connect(self):
        """실제 연결 로직 (BaseWebsocketConnector 템플릿 메서드 구현)"""
        timeout = min(30, 5 * (1 + self.reconnect_strategy.attempt * 0.5))
        logger.info(
            f"[Bybit] 웹소켓 연결 시도 | 시도={self.reconnect_strategy.attempt + 1}회차, timeout={timeout}초"
        )
        try:
            self.ws = await asyncio.wait_for(
                connect(
                    self.ws_url,
                    ping_interval=None,  # 자체 핑 메커니즘 사용
                    ping_timeout=None,
                    compression=None
                ),
                timeout=timeout
            )
            if not self.session:
                self.session = aiohttp.ClientSession()
        except asyncio.TimeoutError:
            logger.warning(
                f"[{self.exchangename}] 연결 타임아웃 발생 (무시됨) | 시도={self.reconnect_strategy.attempt + 1}회차, timeout={timeout}초"
            )
            return await self._do_connect()

    async def _after_connect(self):
        """
        연결 후 처리 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        self.ping_task = asyncio.create_task(self._ping_loop())
        self.health_check_task = asyncio.create_task(self.health_check())
        if self.output_queue:
            self.orderbook_manager.set_output_queue(self.output_queue)
        logger.info(f"[Bybit] 웹소켓 연결 성공")

    async def _handle_connection_failure(self, reason: str, exception: Optional[Exception] = None) -> bool:
        """
        연결 실패 처리 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        result = await super()._handle_connection_failure(reason, exception)
        logger.warning(
            f"[Bybit] 연결 실패 처리 | 이유={reason}, 재연결 시도={result}, 다음 시도까지 대기={self.reconnect_strategy.next_delay():.1f}초"
        )
        return result

    async def subscribe(self, symbols: List[str]) -> None:
        """
        심볼 구독
        """
        try:
            total_batches = (len(symbols) + self.max_symbols_per_subscription - 1) // self.max_symbols_per_subscription
            logger.info(f"[Bybit] 구독 시작 | 총 {len(symbols)}개 심볼, {total_batches}개 배치로 나눔")
            self.snapshot_pending = set(symbols)
            for i in range(0, len(symbols), self.max_symbols_per_subscription):
                batch_symbols = symbols[i:i + self.max_symbols_per_subscription]
                batch_num = (i // self.max_symbols_per_subscription) + 1
                args = []
                for sym in batch_symbols:
                    market = f"{sym}USDT"
                    args.append(f"orderbook.{self.depth_level}.{market}")
                msg = {
                    "op": "subscribe",
                    "args": args
                }
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe")
                await self.ws.send(json.dumps(msg))
                logger.info(f"[Bybit] 구독 요청 전송 | 배치 {batch_num}/{total_batches}, symbols={batch_symbols}")
                await asyncio.sleep(0.1)
            logger.info(f"[Bybit] 전체 구독 요청 완료 | 총 {len(symbols)}개 심볼")
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe_complete")
        except Exception as e:
            self.log_error(f"구독 요청 실패: {str(e)}")
            raise

    async def parse_message(self, message: str) -> Optional[dict]:
        """
        메시지 파싱
        """
        try:
            try:
                data = json.loads(message)
            except json.JSONDecodeError as e:
                logger.error(f"[Bybit] JSON 파싱 실패: {str(e)}, 메시지: {message[:100]}...")
                return None

            if data.get("op") == "subscribe":
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe_response")
                logger.debug(f"[Bybit] 구독 응답 수신: {data}")
                return None

            if data.get("op") == "pong" or (data.get("ret_msg") == "pong" and data.get("op") == "ping"):
                self._handle_pong(data)
                return None

            if "topic" in data and "data" in data:
                topic = data.get("topic", "")
                if "orderbook" in topic:
                    parts = topic.split(".")
                    if len(parts) >= 3:
                        symbol = parts[-1].replace("USDT", "")
                        msg_type = data.get("type", "delta")
                        if msg_type == "snapshot":
                            logger.info(f"[Bybit] 스냅샷 메시지 수신: {symbol}")
                            self.snapshot_received.add(symbol)
                            self.snapshot_pending.discard(symbol)
                            if self.connection_status_callback:
                                self.connection_status_callback(self.exchangename, "snapshot_received")
                        self.log_raw_message(msg_type, message, symbol)
                        return data
            return None
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}")
            return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        """
        파싱된 메시지 처리
        """
        try:
            if not parsed or "topic" not in parsed:
                return
            topic = parsed["topic"]
            if not topic.startswith("orderbook"):
                return
            symbol = topic.split(".")[-1].replace("USDT", "")
            msg_type = parsed.get("type", "unknown")
            start_time = time.time()
            result = await self.orderbook_manager.update(symbol, parsed)
            processing_time = (time.time() - start_time) * 1000  # ms 변환

            if not result.is_valid:
                logger.warning(f"[Bybit] {symbol} 오더북 업데이트 실패: {result.error_messages}")
                if symbol not in self.snapshot_pending:
                    logger.info(f"[Bybit] {symbol} 스냅샷 요청 시작")
                    self.snapshot_pending.add(symbol)
                    snapshot = await self.request_snapshot(f"{symbol}USDT")
                    if snapshot:
                        await self.orderbook_manager.initialize_orderbook(symbol, snapshot)
                        logger.info(f"[Bybit] {symbol} 스냅샷 초기화 완료")
                    else:
                        logger.error(f"[Bybit] {symbol} 스냅샷 요청 실패")
                        self.snapshot_pending.discard(symbol)

            if self.stats:
                self.stats.message_count += 1
                if not hasattr(self.stats, 'processing_times'):
                    self.stats.processing_times = []
                self.stats.processing_times.append(processing_time)
                if self.stats.message_count % 1000 == 0:
                    avg_time = sum(self.stats.processing_times[-1000:]) / min(1000, len(self.stats.processing_times))
                    logger.info(
                        f"[Bybit] {symbol} 처리 성능 | 평균={avg_time:.2f}ms, 총={self.stats.message_count:,}개"
                    )
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "message")
        except Exception as e:
            self.log_error(f"메시지 처리 실패: {str(e)}")

    async def request_snapshot(self, market: str) -> Optional[dict]:
        """
        REST API를 통해 스냅샷 요청
        """
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            url = (f"https://api.bybit.com/v5/market/orderbook"
                   f"?category=linear&symbol={market}"
                   f"&limit={self.depth_level}")
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "snapshot_request")
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.log_raw_message("snapshot", json.dumps(data), market.replace("USDT", ""))
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
        """
        스냅샷 데이터 파싱
        """
        try:
            ts = data.get("ts", int(time.time()*1000))
            seq = data.get("u", 0)
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

    async def _ping_loop(self) -> None:
        """
        핑 전송 루프 (20초 간격)
        """
        self.last_ping_time = time.time()
        self.last_pong_time = time.time()
        while not self.stop_event.is_set() and self.is_connected:
            try:
                await self._send_ping()
                await asyncio.sleep(self.ping_interval)
                current_time = time.time()
                if self.last_pong_time <= 0:
                    self.last_pong_time = current_time
                    continue
                pong_diff = current_time - self.last_pong_time
                if pong_diff < self.ping_interval:
                    continue
                if pong_diff > self.ping_timeout:
                    logger.error(f"[Bybit] PONG 응답 타임아웃 ({self.ping_timeout}초) | 마지막 PONG: {pong_diff:.1f}초 전")
                    await self.reconnect()
                    break
            except Exception as e:
                self.log_error(f"PING 루프 오류: {str(e)}")
                await asyncio.sleep(1)

    async def _send_ping(self) -> None:
        """Bybit 공식 핑 메시지 전송"""
        try:
            if self.ws and self.is_connected:
                ping_message = {
                    "req_id": str(int(time.time() * 1000)),
                    "op": "ping"
                }
                await self.ws.send(json.dumps(ping_message))
                self.last_ping_time = time.time()
                logger.debug(f"[Bybit] PING 전송: {ping_message}")
        except Exception as e:
            self.log_error(f"PING 전송 실패: {str(e)}")

    def _handle_pong(self, data: dict) -> None:
        """
        Bybit 공식 퐁 메시지 처리
        """
        try:
            self.last_pong_time = time.time()
            latency = (self.last_pong_time - self.last_ping_time) * 1000
            self.stats.latency_ms = latency
            self.stats.last_pong_time = self.last_pong_time
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "heartbeat")
            logger.debug(f"[Bybit] PONG 수신 | 레이턴시: {latency:.2f}ms | req_id: {data.get('req_id', 'N/A')}")
        except Exception as e:
            self.log_error(f"PONG 처리 실패: {str(e)}")

    async def _prepare_start(self, symbols: List[str]) -> None:
        """
        시작 전 초기화 및 설정
        """
        self.snapshot_received.clear()
        self.snapshot_pending.clear()
        self.orderbook_manager.clear_all()
        logger.info(f"[Bybit] 시작 준비 완료 | 심볼 수: {len(symbols)}개")

    async def _run_message_loop(self, symbols: List[str], tasks: List[asyncio.Task]) -> None:
        """
        메시지 처리 루프 실행
        """
        try:
            while not self.stop_event.is_set() and self.is_connected:
                try:
                    message = await asyncio.wait_for(self.ws.recv(), timeout=30)
                    self.stats.last_message_time = time.time()
                    parsed = await self.parse_message(message)
                    if parsed:
                        await self.handle_parsed_message(parsed)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    self.log_error(f"메시지 루프 오류: {str(e)}")
                    await self.reconnect()
                    break
        except Exception as e:
            self.log_error(f"메시지 루프 실행 실패: {str(e)}")
            await self.reconnect()
        finally:
            # 공통 태스크 취소 처리
            for task in tasks:
                await self._cancel_task(task, "백그라운드 태스크")
            if not self.stop_event.is_set():
                logger.info(f"[Bybit] 메시지 루프 종료 후 재시작 시도")
                asyncio.create_task(self.start({"bybitfuture": symbols}))

    async def start_background_tasks(self) -> List[asyncio.Task]:
        """
        백그라운드 태스크 시작
        """
        tasks = []
        if not self.ping_task or self.ping_task.done():
            self.ping_task = asyncio.create_task(self._ping_loop())
            tasks.append(self.ping_task)
        if not self.health_check_task or self.health_check_task.done():
            self.health_check_task = asyncio.create_task(self.health_check())
            tasks.append(self.health_check_task)
        return tasks

    async def _cancel_task(self, task: Optional[asyncio.Task], name: str) -> None:
        """공통 태스크 취소 함수"""
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning(f"[Bybit] {name} 취소 중 오류 (무시됨): {str(e)}")

    async def stop(self) -> None:
        """
        웹소켓 연결 및 모든 태스크 종료
        """
        await super().stop()
        self.orderbook_manager.clear_all()
        if self.session:
            await self.session.close()
            self.session = None
        logger.info(f"[Bybit] 웹소켓 종료 완료")

    async def reconnect(self) -> None:
        """
        웹소켓 연결 재설정
        """
        logger.info(f"[Bybit] 재연결 시작")
        try:
            if self.ws:
                try:
                    await self.ws.close()
                except Exception as e:
                    logger.warning(f"[Bybit] 웹소켓 종료 중 오류 (무시됨): {str(e)}")
            await self._cancel_task(self.ping_task, "PING 태스크")
            await self._cancel_task(self.health_check_task, "헬스 체크 태스크")
            self.is_connected = False
            self.stats.connected = False
            self.stats.reconnect_count += 1
            await super().reconnect()
        except Exception as e:
            self.log_error(f"재연결 실패: {str(e)}")
            delay = self.reconnect_strategy.next_delay()
            logger.info(f"[Bybit] 재연결 대기 중 ({delay:.1f}초) | 시도={self.reconnect_strategy.attempt}회차")
            await asyncio.sleep(delay)

    def log_raw_message(self, msg_type: str, message: str, symbol: str) -> None:
        """
        Raw 메시지 로깅
        """
        try:
            self.raw_logger.info(f"{msg_type}|{symbol}|{message}")
        except Exception as e:
            self.log_error(f"Raw 로깅 실패: {str(e)}")
        try:
            if isinstance(message, dict):
                data = message
            else:
                data = json.loads(message)
            if "topic" in data and "data" in data:
                topic = data.get("topic", "")
                if "orderbook" in topic:
                    ob_data = data.get("data", {})
                    msg_type_parsed = data.get("type", "delta").lower()
                    seq = ob_data.get("u", 0)
                    ts = ob_data.get("ts", int(time.time() * 1000))
                    bids = [[float(b[0]), float(b[1])] for b in ob_data.get("b", [])]
                    asks = [[float(a[0]), float(a[1])] for a in ob_data.get("a", [])]
                    parsed_data = {
                        "exchangename": "bybitfuture",
                        "symbol": symbol,
                        "bids": bids,
                        "asks": asks,
                        "timestamp": ts,
                        "sequence": seq,
                        "type": msg_type_parsed
                    }
                    parsed_json = json.dumps(parsed_data)
                    self.raw_logger.info(f"parsedData|{symbol}|{parsed_json}")
                    if not hasattr(self, '_parsed_log_count'):
                        self._parsed_log_count = 0
                    self._parsed_log_count += 1
                    if self._parsed_log_count <= 5:
                        logger.debug(f"[BybitFuture] 파싱된 데이터 로깅 #{self._parsed_log_count}: {symbol}")
            if hasattr(self.stats, 'raw_logged_messages'):
                self.stats.raw_logged_messages += 1
            else:
                self.stats.raw_logged_messages = 1
        except Exception as e:
            logger.error(f"[BybitFuture] 파싱된 데이터 로깅 실패: {str(e)}", exc_info=True)