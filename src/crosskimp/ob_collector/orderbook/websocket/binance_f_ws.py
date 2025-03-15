# file: orderbook/websocket/binance_future_websocket.py


import asyncio
import json
import time
import aiohttp
import websockets
from websockets import connect
from typing import Dict, List, Optional

from crosskimp.ob_collector.orderbook.websocket.base_ws_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.orderbook.binance_f_ob_v2 import BinanceFutureOrderBookManagerV2, parse_binance_future_depth_update
from crosskimp.config.constants import Exchange


# ============================
# 바이낸스 선물 웹소켓 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BINANCE_FUTURE.value  # 거래소 코드

# 웹소켓 연결 설정
WS_BASE_URL = "wss://fstream.binance.com/stream?streams="  # 웹소켓 기본 URL
PING_INTERVAL = 20  # 핑 전송 간격 (초)
PING_TIMEOUT = 20    # 핑 응답 타임아웃 (초)

# REST API 설정 (스냅샷 요청용)
REST_BASE_URL = "https://fapi.binance.com/fapi/v1/depth"  # REST API 기본 URL

# 오더북 관련 설정
UPDATE_SPEED = "100ms"  # 웹소켓 업데이트 속도 (100ms, 250ms, 500ms 중 선택)
DEFAULT_DEPTH = 500     # 기본 오더북 깊이
MAX_RETRY_COUNT = 3     # 최대 재시도 횟수

# 스냅샷 요청 설정
SNAPSHOT_RETRY_DELAY = 1  # 스냅샷 요청 재시도 초기 딜레이 (초)
SNAPSHOT_REQUEST_TIMEOUT = 10  # 스냅샷 요청 타임아웃 (초)

# DNS 캐시 설정
DNS_CACHE_TTL = 300  # DNS 캐시 TTL (초)

class BinanceFutureWebsocket(BaseWebsocketConnector):
    """
    바이낸스 선물 웹소켓
    - 바이낸스 현물과 유사한 흐름
    - orderbook_manager를 통해 오더북 업데이트 & output_queue 전달
    """

    def __init__(self, settings: dict):
        """
        바이낸스 선물 웹소켓 클라이언트 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, Exchange.BINANCE_FUTURE.value)
        self.ws_base = WS_BASE_URL
        self.snapshot_base = REST_BASE_URL

        self.update_speed = UPDATE_SPEED
        self.snapshot_depth = settings.get("depth", DEFAULT_DEPTH)
        self.orderbook_manager = BinanceFutureOrderBookManagerV2(self.snapshot_depth)

        self.subscribed_symbols: set = set()
        self.instance_key: Optional[str] = None

        # 실시간 url
        self.wsurl = ""
        self.is_connected = False
        # Ping/Pong 설정 추가
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """
        BaseWebsocket에도 저장 + orderbook_manager에도 동일하게 저장
        """
        super().set_output_queue(queue)
        if self.orderbook_manager:
            self.orderbook_manager.set_output_queue(queue)
            self.log_info(f"오더북 매니저에 출력 큐 설정 완료 (큐 ID: {id(queue)})")
            
            # 각 오더북 객체에 큐가 제대로 설정되었는지 확인
            for symbol in self.subscribed_symbols:
                orderbook = self.orderbook_manager.get_orderbook(symbol)
                if orderbook:
                    if orderbook.output_queue is None:
                        orderbook.output_queue = queue
                        self.log_info(f"{symbol} 오더북 객체에 출력 큐 직접 설정 완료")
                    else:
                        self.log_info(f"{symbol} 오더북 객체 출력 큐 확인: {id(orderbook.output_queue)}")

    async def connect(self):
        """
        바이낸스 현물과 유사한 로직
        """
        try:
            if not self.wsurl:
                raise ValueError("WebSocket URL is not set. Call subscribe() first.")
                
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect_attempt")
                
            self.session = aiohttp.ClientSession()
            self.ws = await connect(
                self.wsurl,
                ping_interval=self.ping_interval,  # 150초
                ping_timeout=self.ping_timeout,    # 10초
                compression=None
            )
            self.is_connected = True
            self.stats.connection_start_time = time.time()
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect")
            self.log_info("웹소켓 연결 성공")
        except Exception as e:
            self.log_error(f"connect() 예외: {e}", exc_info=True)
            raise

    async def subscribe(self, symbols: List[str]):
        if not symbols:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "warning")
            return

        streams = []
        for sym in symbols:
            sym_lower = sym.lower()
            if not sym_lower.endswith("usdt"):
                sym_lower += "usdt"
            streams.append(f"{sym_lower}@depth@{self.update_speed}")
            self.subscribed_symbols.add(sym.upper())

        combined = "/".join(streams)
        self.wsurl = self.ws_base + combined
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "subscribe")

        await self.connect()

        # 출력 큐가 설정되어 있는지 확인
        if self.output_queue and self.orderbook_manager:
            self.log_info(f"출력 큐 확인: {id(self.output_queue)}")
            self.orderbook_manager.set_output_queue(self.output_queue)

        # 스냅샷
        for sym in symbols:
            snapshot = await self.request_snapshot(sym)
            if snapshot:
                res = await self.orderbook_manager.initialize_orderbook(sym, snapshot)
                if not res.is_valid:
                    self.log_error(f"{sym} 스냅샷 적용 실패: {res.error_messages}")
                else:
                    self.log_info(f"{sym} 스냅샷 적용 성공")
                    
                    # 초기 오더북 데이터 큐에 전송 시도
                    orderbook = self.orderbook_manager.get_orderbook(sym)
                    if orderbook:
                        # 오더북 객체에 큐가 설정되어 있는지 확인
                        if orderbook.output_queue is None and self.output_queue:
                            orderbook.output_queue = self.output_queue
                            self.log_info(f"{sym} 오더북 객체에 출력 큐 설정 (subscribe 내)")
                        
                        # 큐로 데이터 전송
                        if self.output_queue:
                            try:
                                data = orderbook.to_dict()
                                await self.output_queue.put((self.exchangename, data))
                                self.log_info(f"{sym} 초기 오더북 데이터 큐 전송 성공")
                                
                                # 직접 send_to_queue 메서드 호출 시도
                                try:
                                    await orderbook.send_to_queue()
                                    self.log_info(f"{sym} send_to_queue 메서드 호출 성공")
                                except Exception as e:
                                    self.log_error(f"{sym} send_to_queue 메서드 호출 실패: {e}")
                            except Exception as e:
                                self.log_error(f"{sym} 초기 오더북 데이터 큐 전송 실패: {e}")
            else:
                self.log_error(f"{sym} 스냅샷 요청 실패")

    async def request_snapshot(self, symbol: str) -> Optional[dict]:
        """
        바이낸스 현물과 유사한 스냅샷 요청
        """
        max_retries = MAX_RETRY_COUNT
        retry_delay = SNAPSHOT_RETRY_DELAY
        
        # 심볼 형식 처리
        symbol_upper = symbol.upper()
        if not symbol_upper.endswith("USDT"):
            symbol_upper += "USDT"
        
        for attempt in range(max_retries):
            try:
                url = f"{self.snapshot_base}?symbol={symbol_upper}&limit={self.snapshot_depth}"
                self.log_info(f"스냅샷 요청 URL: {url}")
                
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "snapshot_request")
                
                # TCP 커넥터에 DNS 캐시 설정 추가
                connector = aiohttp.TCPConnector(
                    ttl_dns_cache=DNS_CACHE_TTL,  # DNS 캐시 TTL
                    ssl=False  # SSL 검증 비활성화 (필요한 경우)
                )
                
                async with aiohttp.ClientSession(connector=connector) as sess:
                    async with sess.get(url, timeout=SNAPSHOT_REQUEST_TIMEOUT) as resp:
                        if resp.status == 200:
                            raw_data = await resp.json()
                            return self.parse_snapshot(raw_data, symbol)
                        else:
                            self.log_error(f"{symbol} 스냅샷 status={resp.status}")
                            
            except Exception as e:
                self.log_error(f"스냅샷 요청 실패: {e}", exc_info=True)
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                
        self.log_error(f"{symbol} ME 스냅샷 요청 최대 재시도 횟수 초과")
        return None

    def parse_snapshot(self, data: dict, symbol: str) -> Optional[dict]:
        """
        바이낸스 현물 parse_snapshot과 유사
        """
        try:
            if "lastUpdateId" not in data:
                self.log_error(f"{symbol} 'lastUpdateId' 없음")
                return None

            last_id = data["lastUpdateId"]
            bids = [[float(b[0]), float(b[1])] for b in data.get("bids", []) if float(b[0])>0 and float(b[1])>0]
            asks = [[float(a[0]), float(a[1])] for a in data.get("asks", []) if float(a[0])>0 and float(a[1])>0]

            snapshot = {
                "exchangename": "binancefuture",
                "symbol": symbol,
                "bids": bids,
                "asks": asks,
                "timestamp": int(time.time()*1000),
                "lastUpdateId": last_id,
                "type": "snapshot"
            }
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "snapshot_parsed")
            return snapshot
        except Exception as e:
            self.log_error(f"parse_snapshot() 예외: {e}", exc_info=True)
            return None

    async def parse_message(self, message: str) -> Optional[dict]:
        try:
            data = json.loads(message)
            if "stream" in data and "data" in data:
                data = data["data"]

            # 구독 응답
            if "result" in data and "id" in data:
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe_response")
                return None

            # depthUpdate
            if data.get("e") == "depthUpdate":
                symbol = data["s"].replace("USDT","").upper()
                # Raw 메시지 로깅
                self.log_raw_message("depthUpdate", message, symbol)
                return data

            return None
        except json.JSONDecodeError as je:
            self.log_error(f"JSON 파싱실패: {je}, message={message[:200]}")
        except Exception as e:
            self.log_error(f"parse_message() 예외: {e}")
        return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        """파싱된 메시지 처리"""
        try:
            start_time = time.time()
            
            # depthUpdate 메시지 처리
            if parsed.get("e") == "depthUpdate":
                symbol = parsed.get("s", "").replace("USDT", "").upper()
                
                # 메시지 크기 메트릭 기록 - 안전하게 호출
                try:
                    # metrics_manager 속성이 있는지 확인
                    if hasattr(self, 'metrics_manager') and self.metrics_manager:
                        message_size = len(str(parsed))
                        await self.record_metric("bytes", size=message_size, message_type="orderbook", symbol=symbol)
                except Exception:
                    # 오류 로그를 출력하지 않음
                    pass
                
                # 오더북 업데이트
                if self.orderbook_manager:
                    try:
                        # 오더북 업데이트
                        result = await self.orderbook_manager.update(symbol, parsed)
                        
                        # 업데이트 성공 시 큐로 직접 전송 시도 (백업 메커니즘)
                        if result.is_valid and self.output_queue:
                            try:
                                orderbook = self.orderbook_manager.get_orderbook(symbol)
                                if orderbook:
                                    # 오더북 객체에 큐가 설정되어 있는지 확인
                                    if orderbook.output_queue is None:
                                        orderbook.output_queue = self.output_queue
                                        self.log_info(f"{symbol} 오더북 객체에 출력 큐 설정 (handle_parsed_message 내)")
                                    
                                    # 큐에 직접 전송
                                    data = orderbook.to_dict()
                                    await self.output_queue.put((self.exchangename, data))
                                    self.log_info(f"{symbol} 오더북 데이터 큐 직접 전송 성공")
                                    
                                    # 직접 send_to_queue 메서드 호출 시도
                                    try:
                                        await orderbook.send_to_queue()
                                        self.log_info(f"{symbol} send_to_queue 메서드 호출 성공")
                                    except Exception as e:
                                        self.log_error(f"{symbol} send_to_queue 메서드 호출 실패: {e}")
                            except Exception as e:
                                self.log_error(f"{symbol} 오더북 데이터 큐 직접 전송 실패: {e}")
                        
                        # 처리 시간 메트릭 기록 - 안전하게 호출
                        try:
                            # metrics_manager 속성이 있는지 확인
                            if hasattr(self, 'metrics_manager') and self.metrics_manager:
                                processing_time = (time.time() - start_time) * 1000  # ms 단위
                                await self.record_metric("processing_time", time_ms=processing_time, operation="orderbook_update", symbol=symbol)
                        except Exception:
                            # 오류 로그를 출력하지 않음
                            pass
                        
                        # 업데이트 결과에 따른 메트릭 기록 - 안전하게 호출
                        try:
                            # metrics_manager 속성이 있는지 확인
                            if hasattr(self, 'metrics_manager') and self.metrics_manager:
                                if result.is_valid:
                                    await self.record_metric("orderbook", symbol=symbol, operation="update")
                                else:
                                    await self.record_metric("error", error_type="update_failed", symbol=symbol, error=str(result.error_messages))
                        except Exception:
                            # 오류 로그를 출력하지 않음
                            pass
                    except Exception as e:
                        self.log_error(f"오더북 업데이트 실패: {e}")
            
            # 바이빗 스타일 메시지 처리 (이전 코드 유지)
            elif "topic" in parsed and "data" in parsed:
                topic = parsed["topic"]
                data = parsed["data"]
                
                # 오더북 메시지 처리
                if topic.startswith("orderbook."):
                    # 심볼 추출
                    symbol_with_suffix = topic.split(".")[-1]
                    symbol = symbol_with_suffix.replace("USDT", "")
                    
                    # 메시지 크기 메트릭 기록 - 안전하게 호출
                    try:
                        # metrics_manager 속성이 있는지 확인
                        if hasattr(self, 'metrics_manager') and self.metrics_manager:
                            message_size = len(str(parsed))
                            await self.record_metric("bytes", size=message_size, message_type="orderbook", symbol=symbol)
                    except Exception:
                        # 오류 로그를 출력하지 않음
                        pass
                    
                    # 오더북 업데이트
                    if self.orderbook_manager:
                        try:
                            # 데이터 변환
                            converted_data = self._convert_orderbook_data(data, symbol)
                            
                            # 오더북 업데이트
                            result = await self.orderbook_manager.update(symbol, converted_data)
                            
                            # 처리 시간 메트릭 기록 - 안전하게 호출
                            try:
                                # metrics_manager 속성이 있는지 확인
                                if hasattr(self, 'metrics_manager') and self.metrics_manager:
                                    processing_time = (time.time() - start_time) * 1000  # ms 단위
                                    await self.record_metric("processing_time", time_ms=processing_time, operation="orderbook_update", symbol=symbol)
                            except Exception:
                                # 오류 로그를 출력하지 않음
                                pass
                            
                            # 업데이트 결과에 따른 메트릭 기록 - 안전하게 호출
                            try:
                                # metrics_manager 속성이 있는지 확인
                                if hasattr(self, 'metrics_manager') and self.metrics_manager:
                                    if result.is_valid:
                                        await self.record_metric("orderbook", symbol=symbol, operation="update")
                                    else:
                                        await self.record_metric("error", error_type="update_failed", symbol=symbol, error=str(result.error_messages))
                            except Exception:
                                # 오류 로그를 출력하지 않음
                                pass
                        except Exception as e:
                            self.log_error(f"오더북 업데이트 실패: {e}")
                        
                # 핑/퐁 메시지 처리
                elif "op" in parsed and parsed["op"] == "pong":
                    # 퐁 수신 시간 기록
                    self.stats.last_pong_time = time.time()
                    
                    # 핑-퐁 지연시간 계산
                    latency = (self.stats.last_pong_time - self.stats.last_ping_time) * 1000  # ms 단위
                    self.stats.latency_ms = latency
                    
                    # 핑-퐁 메트릭 기록 - 안전하게 호출
                    try:
                        # metrics_manager 속성이 있는지 확인
                        if hasattr(self, 'metrics_manager') and self.metrics_manager:
                            await self.record_metric("pong", latency_ms=latency)
                    except Exception:
                        # 오류 로그를 출력하지 않음
                        pass
                    
                    self.log_debug(f"퐁 수신 | 지연시간: {latency:.2f}ms")
                
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {e}")
            
            # 오류 메트릭 기록 - 안전하게 호출
            try:
                # metrics_manager 속성이 있는지 확인
                if hasattr(self, 'metrics_manager') and self.metrics_manager:
                    await self.record_metric("error", error_type="message_handling", error=str(e))
            except Exception:
                # 오류 로그를 출력하지 않음
                pass

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        exchange_symbols = symbols_by_exchange.get(self.exchangename.lower(), [])
        if not exchange_symbols:
            self.log_error("구독할 심볼 없음.")
            return

        # 부모 클래스의 start 메소드를 호출하지 않고 직접 필요한 로직 구현
        # await super().start(symbols_by_exchange)
        
        # 1. 초기화 및 설정
        await self._prepare_start(exchange_symbols)

        while not self.stop_event.is_set():
            try:
                # 2. 단일 연결로 모든 심볼 구독 (이 과정에서 wsurl이 설정되고 connect가 호출됨)
                await self.subscribe(exchange_symbols)
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "connect")

                while not self.stop_event.is_set():
                    try:
                        message = await asyncio.wait_for(
                            self.ws.recv(),
                            timeout=self.health_check_interval
                        )
                        self.stats.last_message_time = time.time()
                        self.stats.message_count += 1
                        parsed = await self.parse_message(message)
                        if parsed and self.connection_status_callback:
                            self.connection_status_callback(self.exchangename, "message")

                        if parsed:
                            await self.handle_parsed_message(parsed)

                    except asyncio.TimeoutError:
                        # 헬스체크 timeout
                        continue
                    except Exception as e:
                        self.log_error(f"메시지 수신 실패: {str(e)}")
                        break

            except Exception as conn_e:
                # 연결 실패 시 백오프
                delay = self.reconnect_strategy.next_delay()
                self.log_error(f"연결 실패: {conn_e}, 재연결 {delay}s 후 재시도")
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
        """
        웹소켓 연결 종료
        """
        self.log_info("웹소켓 연결 종료 중...")
        await super().stop()
        self.log_info("웹소켓 연결 종료 완료")

    async def record_metric(self, metric_type: str, **kwargs):
        """
        메트릭 기록 메서드 (metrics_manager가 없을 때 호출되는 더미 메서드)
        
        이 메서드는 metrics_manager 속성이 없을 때 호출되어도 오류가 발생하지 않도록 합니다.
        실제 메트릭 기록은 수행하지 않습니다.
        
        Args:
            metric_type: 메트릭 유형
            **kwargs: 추가 매개변수
        """
        # metrics_manager가 없으면 아무 작업도 수행하지 않음
        pass