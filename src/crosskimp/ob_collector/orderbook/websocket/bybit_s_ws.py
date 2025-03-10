import asyncio
import json
import time
from websockets import connect
import websockets  # 전체 websockets 모듈 임포트 추가
from typing import Dict, List, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, get_raw_logger
from crosskimp.ob_collector.orderbook.websocket.base_ws_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.orderbook.bybit_s_ob import BybitSpotOrderBookManager

# 바이빗 현물 웹소켓 전용 상수
BYBIT_SPOT_PING_INTERVAL = 20  # 20초마다 ping (공식 문서 권장)
BYBIT_SPOT_PING_TIMEOUT = 10   # 10초 이내 pong 응답 없으면 재연결

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
            bids.append([px, qty if qty > 0 else 0.0])

        asks = []
        for a in ob_data.get("a", []):
            px, qty = float(a[0]), float(a[1])
            asks.append([px, qty if qty > 0 else 0.0])

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

class BybitSpotWebsocket(BaseWebsocketConnector):
    """
    Bybit 현물 WebSocket
    """
    def __init__(self, settings: dict):
        super().__init__(settings, "bybit")
        self.ws_url = "wss://stream.bybit.com/v5/public/spot"
        self.depth_level = settings.get("connection", {}).get("websocket", {}).get("depth_level", 50)
        self.ob_manager = BybitSpotOrderBookManager(self.depth_level)
        
        # Bybit specific settings - 파일 상단에 정의된 상수 사용
        self.ping_interval = BYBIT_SPOT_PING_INTERVAL
        self.ping_timeout = BYBIT_SPOT_PING_TIMEOUT
        self.max_symbols_per_subscription = 10
        
        # 연결 상태 관리
        self.is_connected = False
        self.current_retry = 0
        self.retry_delay = 1
        self.last_ping_time = 0
        self.last_pong_time = 0
        self.ping_task = None
        self.health_check_task = None

        # 로거 초기화 부분 제거 - 부모 클래스의 로깅 설정 사용
        """
        # 로거 초기화
        self.raw_logger = get_raw_logger("bybit")
        
        # 로그 파일 경로 설정 (직접 로깅용)
        try:
            import os
            import glob
            from datetime import datetime
            
            log_dir = os.path.join("src", "logs", "raw")
            os.makedirs(log_dir, exist_ok=True)
            
            today = datetime.now().strftime("%y%m%d")
            existing_logs = glob.glob(os.path.join(log_dir, f"{today}_*_bybit_raw_logger.log"))
            
            if existing_logs:
                self.log_file_path = sorted(existing_logs)[-1]
                logger.info(f"[Bybit] 기존 로그 파일 사용: {self.log_file_path}")
                
                with open(self.log_file_path, "a", encoding="utf-8") as f:
                    now = datetime.now()
                    f.write(f"\n{now.strftime('%Y-%m-%d %H:%M:%S')} - INIT|SYSTEM|바이빗 로거 재시작\n")
            else:
                now = datetime.now()
                date_str = now.strftime("%y%m%d")
                time_str = now.strftime("%H%M%S")
                self.log_file_path = os.path.join(log_dir, f"{date_str}_{time_str}_bybit_raw_logger.log")
                
                with open(self.log_file_path, "w", encoding="utf-8") as f:
                    f.write(f"{now.strftime('%Y-%m-%d %H:%M:%S')} - INIT|SYSTEM|바이빗 로거 초기화 완료\n")
                
                logger.info(f"[Bybit] 새 로그 파일 생성 완료: {self.log_file_path}")
            
        except Exception as e:
            logger.error(f"[Bybit] 로그 파일 설정 실패: {str(e)}", exc_info=True)
        """
        
        # 오더북 관리자 초기화
        self.ob_manager = BybitSpotOrderBookManager(self.depth_level)

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        super().set_output_queue(queue)
        self.ob_manager.set_output_queue(queue)

    async def connect(self) -> None:
        """
        바이빗 웹소켓 연결
        초기 연결 및 재연결 시 타임아웃을 점진적으로 증가시키며 시도
        """
        while True:
            try:
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "connect_attempt")
                
                timeout = min(30, 5 * (1 + self.current_retry * 0.5))
                
                logger.info(
                    f"[Bybit] 웹소켓 연결 시도 | 시도={self.current_retry + 1}회차, timeout={timeout}초"
                )
                
                self.ws = await asyncio.wait_for(
                    connect(
                        self.ws_url,
                        ping_interval=None,
                        ping_timeout=None,
                        compression=None
                    ),
                    timeout=timeout
                )
                
                self.is_connected = True
                self.current_retry = 0
                self.stats.connected = True
                self.stats.connection_start_time = time.time()
                self.last_pong_time = time.time()
                self.stats.last_pong_time = self.last_pong_time
                
                logger.info(f"[Bybit] 웹소켓 연결 성공")
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "connect")
                break
                
            except asyncio.TimeoutError:
                self.current_retry += 1
                logger.warning(
                    f"[Bybit] 연결 타임아웃 발생 (무시됨) | 시도={self.current_retry}회차, timeout={timeout}초"
                )
                await asyncio.sleep(1)
                
            except Exception as e:
                self.current_retry += 1
                self.stats.error_count += 1
                self.stats.last_error_time = time.time()
                self.stats.last_error_message = str(e)
                logger.error(
                    f"[Bybit] 연결 오류: {str(e)} | 시도={self.current_retry}회차"
                )
                retry_delay = min(30, self.retry_delay * (2 ** (self.current_retry - 1)))
                logger.info(f"[Bybit] 재연결 대기 중 ({retry_delay}초)")
                await asyncio.sleep(retry_delay)

    async def subscribe(self, symbols: List[str]) -> None:
        try:
            total_batches = (len(symbols) + self.max_symbols_per_subscription - 1) // self.max_symbols_per_subscription
            logger.info(f"[Bybit] 구독 시작 | 총 {len(symbols)}개 심볼, {total_batches}개 배치로 나눔")
            
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
                        
                        # 원본 메시지 로깅 강화
                        self.log_raw_message("orderbook", message, symbol)
                        
                        # 로깅 성공 메시지 (처음 5개만)
                        if not hasattr(self, '_raw_log_count'):
                            self._raw_log_count = 0
                        self._raw_log_count += 1
                        
                        if self._raw_log_count <= 5:
                            logger.debug(f"[Bybit] 원본 데이터 로깅 성공 #{self._raw_log_count}: {symbol}")
                        
                        if msg_type == "snapshot":
                            logger.info(f"[Bybit] 스냅샷 메시지 수신: {symbol}, type={msg_type}")
                        return data
            return None
            
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}")
            return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        try:
            if not parsed or "topic" not in parsed:
                return

            topic = parsed["topic"]
            if not topic.startswith("orderbook"):
                return

            symbol = topic.split(".")[-1].replace("USDT", "")
            msg_type = parsed.get("type", "unknown")
            
            try:
                parsed_data = parse_bybit_depth_update(parsed)
                if parsed_data and not hasattr(self, '_parsed_log_count'):
                    self._parsed_log_count = 0
                    self._parsed_log_count += 1
                    
                    if self._parsed_log_count <= 5:
                        logger.debug(f"[Bybit] 데이터 파싱 성공 #{self._parsed_log_count}: {symbol}")
            except Exception as e:
                logger.error(f"[Bybit] 데이터 파싱 실패: {str(e)}", exc_info=True)
            
            start_time = time.time()
            await self.ob_manager.update(symbol, parsed)
            processing_time = (time.time() - start_time) * 1000
            
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

    def _handle_pong(self, data: dict) -> bool:
        """Bybit 공식 퐁 메시지 처리. 파싱된 데이터(dict)를 인자로 받습니다."""
        try:
            self.last_pong_time = time.time()
            self.stats.last_pong_time = self.last_pong_time
            latency = (self.last_pong_time - self.last_ping_time) * 1000
            self.stats.latency_ms = latency
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "heartbeat")
            logger.debug(f"[Bybit] PONG 수신 | 레이턴시: {latency:.2f}ms | req_id: {data.get('req_id', 'N/A')}")
            return True
        except Exception as e:
            self.log_error(f"PONG 처리 실패: {str(e)}")
            return False

    async def _ping_loop(self):
        """
        Bybit 공식 핑 태스크
        공식 문서에 따르면 20초마다 ping을 보내는 것이 권장됨
        """
        self.last_ping_time = time.time()
        self.last_pong_time = time.time()
        self.stats.last_ping_time = self.last_ping_time
        self.stats.last_pong_time = self.last_pong_time
        
        while not self.stop_event.is_set() and self.is_connected:
            try:
                await self._send_ping()
                await asyncio.sleep(self.ping_interval)
                current_time = time.time()
                
                if self.last_pong_time <= 0:
                    self.last_pong_time = current_time
                    self.stats.last_pong_time = self.last_pong_time
                    continue
                
                pong_diff = current_time - self.last_pong_time
                if pong_diff < self.ping_interval:
                    continue
                
                if pong_diff > self.ping_timeout:
                    logger.error(f"[Bybit] PONG 응답 타임아웃 ({self.ping_timeout}초) | 마지막 PONG: {pong_diff:.1f}초 전")
                    if self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "heartbeat_timeout")
                    self.is_connected = False
                    self.stats.connected = False
                    await self.reconnect()
                    break
                    
            except Exception as e:
                self.log_error(f"PING 태스크 오류: {str(e)}")
                await asyncio.sleep(1)

    async def _prepare_start(self, symbols: List[str]) -> None:
        """
        시작 전 초기화 및 설정 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        if self.current_retry > 0:
            logger.info(f"[Bybit] 재연결 감지 (retry={self.current_retry}), 오더북 초기화 수행")
            self.ob_manager.clear_all()
            logger.info(f"[Bybit] 재연결 후 오더북 매니저 초기화 완료")
        
        logger.info(f"[Bybit] 시작 준비 완료 | 심볼 수: {len(symbols)}개")

    async def _run_message_loop(self, symbols: List[str], tasks: List[asyncio.Task]) -> None:
        """
        메시지 처리 루프 실행 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        try:
            while not self.stop_event.is_set() and self.is_connected:
                try:
                    message = await asyncio.wait_for(self.ws.recv(), timeout=30)
                    self.stats.last_message_time = time.time()
                    self.stats.message_count += 1
                    
                    parsed = await self.parse_message(message)
                    if parsed:
                        await self.handle_parsed_message(parsed)
                        
                except asyncio.TimeoutError:
                    logger.debug(f"[Bybit] 30초 동안 메시지 없음, 연결 상태 확인을 위해 PING 전송")
                    try:
                        await self._send_ping()
                    except Exception as e:
                        self.log_error(f"PING 전송 실패: {str(e)}")
                        await self.reconnect()
                        break
                        
                except websockets.exceptions.ConnectionClosed as e:
                    self.log_error(f"웹소켓 연결 종료: {str(e)}")
                    await self.reconnect()
                    break
                    
                except Exception as e:
                    self.log_error(f"메시지 루프 오류: {str(e)}")
                    await self.reconnect()
                    break
                    
        except Exception as e:
            self.log_error(f"메시지 루프 실행 실패: {str(e)}")
            await self.reconnect()
            
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            
            if not self.stop_event.is_set():
                logger.info(f"[Bybit] 메시지 루프 종료 후 재시작 시도")
                asyncio.create_task(self.start({"bybit": symbols}))

    async def start_background_tasks(self) -> List[asyncio.Task]:
        """
        백그라운드 태스크 시작 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        tasks = []
        self.ping_task = asyncio.create_task(self._ping_loop())
        tasks.append(self.ping_task)
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
            
            self.current_retry += 1
            self.stats.reconnect_count += 1
            
            await super().reconnect()
            
        except Exception as e:
            self.log_error(f"재연결 실패: {str(e)}")
            delay = min(30, self.retry_delay * (2 ** (self.current_retry - 1)))
            logger.info(f"[Bybit] 재연결 대기 중 ({delay}초) | 시도={self.current_retry}회차")
            await asyncio.sleep(delay)

    async def stop(self) -> None:
        """
        웹소켓 연결 종료
        """
        self.logger.info("바이빗 웹소켓 연결 종료 중...")
        await super().stop()
        self.logger.info("바이빗 웹소켓 연결 종료 완료")