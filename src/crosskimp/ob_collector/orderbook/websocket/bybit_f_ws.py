import asyncio
import json
import time
import aiohttp
from websockets import connect
from typing import Dict, List, Optional, Any

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG

from crosskimp.ob_collector.orderbook.websocket.base_ws_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.orderbook.bybit_f_ob import BybitFutureOrderBookManager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이빗 선물 웹소켓 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BYBIT_FUTURE.value  # 거래소 코드
BYBIT_FUTURE_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이빗 선물 설정

# 웹소켓 연결 설정
WS_URL = BYBIT_FUTURE_CONFIG["ws_url"]  # 웹소켓 URL
PING_INTERVAL = BYBIT_FUTURE_CONFIG["ping_interval"]  # 핑 전송 간격 (초)
PING_TIMEOUT = BYBIT_FUTURE_CONFIG["ping_timeout"]  # 핑 응답 타임아웃 (초)

# 오더북 관련 설정
DEFAULT_DEPTH = BYBIT_FUTURE_CONFIG["default_depth"]  # 기본 오더북 깊이
MAX_SYMBOLS_PER_BATCH = BYBIT_FUTURE_CONFIG["max_symbols_per_batch"]  # 배치당 최대 심볼 수

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
        super().__init__(settings, Exchange.BYBIT_FUTURE.value)
        
        # 웹소켓 URL 설정
        self.ws_url = WS_URL
        if settings.get("testnet", False):
            self.ws_url = "wss://stream-testnet.bybit.com/v5/public/linear"
            
        # 오더북 설정
        self.depth_level = settings.get("depth", DEFAULT_DEPTH)
        self.orderbook_manager = BybitFutureOrderBookManager(self.depth_level)
        self.orderbook_manager.set_websocket(self)  # 웹소켓 연결 설정
        
        # Bybit 특화 설정 - 상단 상수 사용
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        self.max_symbols_per_subscription = MAX_SYMBOLS_PER_BATCH  # 한 번에 최대 10개 심볼 구독
        
        # 연결 상태 관리
        self.is_connected = False
        self.last_ping_time = 0
        self.last_pong_time = 0
        self.ping_task = None
        self.health_check_task = None
        self.ws = None
        
        # 스냅샷 관리
        self.snapshot_received = set()  # 스냅샷을 받은 심볼 목록
        self.snapshot_pending = set()   # 스냅샷 요청 대기 중인 심볼 목록
        self.session: Optional[aiohttp.ClientSession] = None

    async def _do_connect(self):
        """실제 연결 수행"""
        try:
            # 타임아웃 설정 - 재연결 시도 횟수에 따라 증가
            timeout = min(30, 5 * (1 + self.reconnect_strategy.attempt * 0.5))
            
            # 연결 파라미터 설정
            self.ws = await connect(
                self.ws_url,
                ping_interval=None,  # 자체 핑 구현
                ping_timeout=None,   # 자체 핑 구현
                close_timeout=timeout,
                max_size=2**24,      # 24MB 최대 메시지 크기
                compression=None
            )
            
            # 연결 성공 로깅
            self.log_info("웹소켓 연결 성공")
            
            return True
        except Exception as e:
            self.log_error(f"웹소켓 연결 실패: {e}")
            return False

    async def _after_connect(self):
        """
        연결 후 처리 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        self.ping_task = asyncio.create_task(self._ping_loop())
        self.health_check_task = asyncio.create_task(self.health_check())
        if self.output_queue:
            self.orderbook_manager.set_output_queue(self.output_queue)
        self.log_info("웹소켓 연결 성공")

    async def _handle_connection_failure(self, reason: str, exception: Optional[Exception] = None) -> bool:
        """
        연결 실패 처리 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        result = await super()._handle_connection_failure(reason, exception)
        self.log_warning(
            f"연결 실패 처리 | 이유={reason}, 재연결 시도={result}, 다음 시도까지 대기={self.reconnect_strategy.next_delay():.1f}초"
        )
        return result

    async def subscribe(self, symbols: List[str]) -> None:
        """심볼 구독"""
        if not symbols:
            self.log_warning("구독할 심볼이 없습니다")
            return
        
        if not self.ws or not self.is_connected:
            self.log_error("웹소켓이 연결되지 않았습니다")
            return
        
        self.log_info(f"구독 시작 | 총 {len(symbols)}개 심볼, {MAX_SYMBOLS_PER_BATCH}개 배치로 나눔")
        
        # 심볼을 청크로 나누어 구독
        chunks = [symbols[i:i+MAX_SYMBOLS_PER_BATCH] for i in range(0, len(symbols), MAX_SYMBOLS_PER_BATCH)]
        
        for i, chunk in enumerate(chunks):
            try:
                # 구독 메시지 생성
                msg = {
                    "op": "subscribe",
                    "args": [f"orderbook.{self.depth_level}.{sym}USDT" for sym in chunk]
                }
                
                # 구독 요청 전송
                if self.ws and self.is_connected:
                    await self.ws.send(json.dumps(msg))
                    self.log_info(f"구독 요청 전송 | 배치 {i+1}/{len(chunks)}, {len(chunk)}개 심볼")
                    
                    # 요청 간 딜레이
                    await asyncio.sleep(0.1)
                else:
                    self.log_error("구독 중 웹소켓 연결이 끊겼습니다")
                    break
                
            except Exception as e:
                self.log_error(f"구독 요청 실패: {e}")
                
                # 연결이 끊겼을 가능성이 있으므로 재연결 시도
                if not self.is_connected:
                    await self.reconnect()
                    break

    async def parse_message(self, message: str) -> Optional[dict]:
        """
        메시지 파싱
        """
        try:
            try:
                data = json.loads(message)
            except json.JSONDecodeError as e:
                self.log_error(f"JSON 파싱 실패: {str(e)}, 메시지: {message[:100]}...")
                return None

            if data.get("op") == "subscribe":
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe_response")
                self.log_debug(f"구독 응답 수신: {data}")
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
                        
                        # 원본 메시지 로깅
                        self.log_raw_message(msg_type, message, symbol)
                        
                        # ELX 코인에 대해 추가 로깅
                        if symbol == "ELX":
                            ob_data = data.get("data", {})
                            bids_count = len(ob_data.get("b", []))
                            asks_count = len(ob_data.get("a", []))
                            self.log_info(
                                f"ELX 메시지 수신 | "
                                f"타입: {msg_type}, 매수: {bids_count}건, 매도: {asks_count}건, "
                                f"시퀀스: {ob_data.get('u', 'N/A')}"
                            )
                        
                        if msg_type == "snapshot":
                            self.log_info(f"스냅샷 메시지 수신: {symbol}")
                            self.snapshot_received.add(symbol)
                            self.snapshot_pending.discard(symbol)
                            if self.connection_status_callback:
                                self.connection_status_callback(self.exchangename, "snapshot_received")
                        return data
            return None
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}")
            return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        """파싱된 메시지 처리"""
        try:
            if not parsed or "topic" not in parsed:
                return

            topic = parsed["topic"]
            if not topic.startswith("orderbook."):
                return

            start_time = time.time()
            
            # 심볼 추출
            symbol_with_suffix = topic.split(".")[-1]
            symbol = symbol_with_suffix.replace("USDT", "")
            
            # 메시지 타입 확인
            msg_type = parsed.get("type", "delta")
            
            # 원본 데이터 로깅
            self.log_debug(f"{symbol} 원본 데이터: {str(parsed.get('data', {}))[:200]}")
            
            # 데이터 변환
            try:
                # 바이낸스 형식으로 변환
                data = parsed.get("data", {})
                timestamp = data.get("ts", int(time.time() * 1000))
                sequence = data.get("u", 0)
                
                # 매수/매도 데이터 추출
                bids_raw = data.get("b", [])
                asks_raw = data.get("a", [])
                
                # 변환된 데이터 생성
                converted_data = {
                    "exchangename": self.exchangename,
                    "symbol": symbol,
                    "bids": bids_raw,  # 원본 형식 그대로 전달
                    "asks": asks_raw,  # 원본 형식 그대로 전달
                    "timestamp": timestamp,
                    "sequence": sequence,
                    "type": msg_type
                }
                
                self.log_debug(f"{symbol} 변환 데이터: 매수={len(bids_raw)}건, 매도={len(asks_raw)}건")
                
                # 오더북 업데이트
                if self.orderbook_manager:
                    result = await self.orderbook_manager.update(symbol, converted_data)
                    
                    # 처리 시간 로깅
                    processing_time = (time.time() - start_time) * 1000  # ms 단위
                    self.log_debug(f"{symbol} 처리 시간: {processing_time:.2f}ms")
                    
                    if not result.is_valid:
                        self.log_error(f"{symbol} 오더북 업데이트 실패: {result.error_messages}")
                
                # 통계 업데이트
                if self.stats:
                    self.stats.message_count += 1
                    if not hasattr(self.stats, 'processing_times'):
                        self.stats.processing_times = []
                    self.stats.processing_times.append(processing_time)
                    
                    if self.stats.message_count % 1000 == 0:
                        avg_time = sum(self.stats.processing_times[-1000:]) / min(1000, len(self.stats.processing_times))
                        self.log_info(
                            f"{symbol} 처리 성능 | 평균={avg_time:.2f}ms, 총={self.stats.message_count:,}개"
                        )
                
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "message")
                
            except Exception as e:
                self.log_error(f"{symbol} 데이터 변환/처리 오류: {e}, 원본={parsed}")
                
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {e}")

    async def request_snapshot(self, market: str) -> Optional[dict]:
        """
        REST API를 통해 스냅샷 요청
        """
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            # 요청 뎁스를 200으로 증가 (최대값)
            request_depth = 200
            
            url = (f"https://api.bybit.com/v5/market/orderbook"
                   f"?category=linear&symbol={market}"
                   f"&limit={request_depth}")
            
            self.log_info(f"{market} 스냅샷 요청 | 요청 뎁스: {request_depth}")
            
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "snapshot_request")
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.log_raw_message("snapshot", json.dumps(data), market.replace("USDT", ""))
                    if data.get("retCode") == 0:
                        result = data.get("result", {})
                        
                        # 받은 데이터의 뎁스 로깅
                        bids_count = len(result.get("b", []))
                        asks_count = len(result.get("a", []))
                        self.log_info(
                            f"{market} 스냅샷 응답 | "
                            f"매수: {bids_count}건, 매도: {asks_count}건"
                        )
                        
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
            
            # 원본 데이터 로깅
            bids_count = len(data.get("b", []))
            asks_count = len(data.get("a", []))
            
            # 심볼에서 USDT 제거
            clean_symbol = symbol.replace("USDT", "")
            
            self.log_info(
                f"{clean_symbol} 스냅샷 파싱 | "
                f"원본 매수: {bids_count}건, 원본 매도: {asks_count}건, "
                f"시퀀스: {seq}"
            )
            
            bids = [[float(b[0]), float(b[1])] for b in data.get("b", [])]
            asks = [[float(a[0]), float(a[1])] for a in data.get("a", [])]
            
            # 파싱 후 데이터 로깅
            self.log_info(
                f"{clean_symbol} 스냅샷 파싱 완료 | "
                f"파싱 후 매수: {len(bids)}건, 파싱 후 매도: {len(asks)}건"
            )
            
            return {
                "exchangename": "bybitfuture",
                "symbol": clean_symbol,
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
                    self.log_error(f"PONG 응답 타임아웃 ({self.ping_timeout}초) | 마지막 PONG: {pong_diff:.1f}초 전")
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
                self.log_debug(f"PING 전송: {ping_message}")
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
            self.log_debug(f"PONG 수신 | 레이턴시: {latency:.2f}ms | req_id: {data.get('req_id', 'N/A')}")
        except Exception as e:
            self.log_error(f"PONG 처리 실패: {str(e)}")

    async def _prepare_start(self, symbols: List[str]) -> None:
        """
        시작 전 초기화 및 설정
        """
        self.snapshot_received.clear()
        self.snapshot_pending.clear()
        self.orderbook_manager.clear_all()
        self.log_info(f"시작 준비 완료 | 심볼 수: {len(symbols)}개")

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
                self.log_info("메시지 루프 종료 후 재시작 시도")
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
                self.log_warning(f"{name} 취소 중 오류 (무시됨): {str(e)}")

    async def stop(self) -> None:
        """
        웹소켓 연결 종료
        """
        self.log_info("웹소켓 연결 종료 중...")
        await super().stop()
        self.log_info("웹소켓 연결 종료 완료")

    async def reconnect(self) -> None:
        """
        웹소켓 재연결
        """
        try:
            await self.disconnect()
            await asyncio.sleep(1)  # 재연결 전 잠시 대기
            await self._do_connect()
            if self.is_connected:
                await self._after_connect()
                self.log_info("재연결 성공")
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "reconnected")
            else:
                self.log_error("재연결 실패")
        except Exception as e:
            self.log_error(f"재연결 오류: {str(e)}")

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """
        출력 큐 설정
        - 부모 클래스의 output_queue 설정
        - 오더북 매니저의 output_queue 설정
        """
        # 부모 클래스의 output_queue 설정
        super().set_output_queue(queue)
        
        # 오더북 매니저의 output_queue 설정
        self.orderbook_manager.set_output_queue(queue)
        
        # 로깅 추가
        self.log_info(f"웹소켓 출력 큐 설정 완료 (큐 ID: {id(queue)})")
        
        # 큐 설정 확인
        if not hasattr(self.orderbook_manager, '_output_queue') or self.orderbook_manager._output_queue is None:
            self.log_error("오더북 매니저 큐 설정 실패!")
        else:
            self.log_info(f"오더북 매니저 큐 설정 확인 (큐 ID: {id(self.orderbook_manager._output_queue)})")