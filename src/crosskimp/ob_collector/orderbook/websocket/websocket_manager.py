# file: orderbook/websocket/websocket_manager.py

import asyncio
import time
from typing import Callable, List, Dict, Any, Optional

from fastapi import WebSocket, WebSocketDisconnect
from config.config_loader import get_settings
from utils.logging.logger import (
    unified_logger, queue_logger, EXCHANGE_LOGGER_MAP
)
from orderbook.websocket.binance_spot_websocket import BinanceSpotWebsocket
from orderbook.websocket.binance_future_websocket import BinanceFutureWebsocket
from orderbook.websocket.bybit_spot_websocket import BybitSpotWebsocket
from orderbook.websocket.bybit_future_websocket import BybitFutureWebsocket
from orderbook.websocket.upbit_websocket import UpbitWebsocket
from orderbook.websocket.bithumb_spot_websocket import BithumbSpotWebsocket
from crosskimp.telegrambot.notification.telegram_bot import send_telegram_message
import psutil
from datetime import datetime
from orderbook.manager.websocket_metrics_manager import WebsocketMetricsManager
from core.ws_usdtkrw import WsUsdtKrwMonitor
from core.market_price_monitor import MarketPriceMonitor
from config.constants import EXCHANGE_NAMES_KR

EXCHANGE_CLASS_MAP = {
    "binance": BinanceSpotWebsocket,
    "binancefuture": BinanceFutureWebsocket,
    "bybit": BybitSpotWebsocket,
    "bybitfuture": BybitFutureWebsocket,
    "upbit": UpbitWebsocket,
    "bithumb": BithumbSpotWebsocket
}

class WebsocketManager:
    """
    여러 거래소 웹소켓 연결을 중앙에서 관리
    """
    def __init__(self, settings: dict):
        self.settings = settings
        self.websockets: Dict[str, Any] = {}  # 인스턴스별 ws
        self.tasks: Dict[str, asyncio.Task] = {}
        self.output_queue = asyncio.Queue()
        self.stop_event = asyncio.Event()
        self.callback: Optional[Callable[[str, dict], None]] = None
        self.start_time = time.time()  # 시작 시간 추가

        # logger 초기화 추가
        self.logger = unified_logger  # 기본 로거로 unified_logger 사용

        # 메트릭 매니저 초기화
        self.metrics_manager = WebsocketMetricsManager()

        # 시장가격 모니터 초기화
        self.market_price_monitor = MarketPriceMonitor()
        self.current_usdt_rate = 0.0  # USDT 환율 캐시

        # 지연 감지 설정
        self.delay_threshold_ms = settings.get("websocket", {}).get("delay_threshold_ms", 1000)  # 1초
        self.last_delay_alert = {}  # 마지막 알림 시간 기록
        self.alert_cooldown = 300  # 알림 쿨다운 5분
        self.metric_update_interval = 1.0  # 메트릭 업데이트 주기 (초)

        # 연결 상태 관리 추가
        self._active_connections = set()
        self._connection_lock = asyncio.Lock()

        # 메트릭 히스토리 제한
        self.max_metric_history = 3600  # 1시간치 데이터만 보관
        self.metric_cleanup_interval = 300  # 5분마다 정리
        
        # 메모리 사용량 모니터링
        self.memory_stats = {
            'peak_usage': 0,
            'current_usage': 0,
            'last_cleanup': time.time()
        }

        # 처리율 계산 주기 (초)
        self.rate_calculation_interval = 1.0

        # 거래소 초기화
        current_time = time.time()
        for exchange in EXCHANGE_CLASS_MAP.keys():
            self.metrics_manager.initialize_exchange(exchange)
            self.metrics_manager.metrics["connection_status"][exchange] = False
            self.metrics_manager.metrics["message_counts"][exchange] = 0
            self.metrics_manager.metrics["error_counts"][exchange] = 0
            self.metrics_manager.metrics["latencies"][exchange] = []
            self.metrics_manager.metrics["last_ping_times"][exchange] = current_time * 1000
            self.metrics_manager.metrics["message_rates"][exchange] = 0.0
            self.metrics_manager.metrics["last_message_times"][exchange] = current_time * 1000
            self.logger.info(f"[{EXCHANGE_NAMES_KR.get(exchange, exchange)}] 메트릭 초기화 완료")

    def register_callback(self, callback: Callable[[str, dict], None]):
        self.callback = callback

    def update_metrics(self, key: str, event_type: str):
        self.metrics_manager.update_metric(key, event_type)

    def get_aggregated_metrics(self) -> Dict:
        return self.metrics_manager.get_metrics()

    def update_usdt_rate(self, rate: float):
        """USDT 환율 업데이트"""
        self.current_usdt_rate = rate
        if hasattr(self, 'market_price_monitor'):
            self.market_price_monitor.update_usdt_rate(rate)

    async def process_queue(self):
        """메시지 큐 처리 -> 메시지 카운트 증가, 콜백 호출"""
        while not self.stop_event.is_set():
            try:
                key, data = await self.output_queue.get()
                current_time = time.time() * 1000
                
                # 거래소별 로거 선택
                logger = EXCHANGE_LOGGER_MAP.get(key.lower(), unified_logger)

                # 큐 데이터 로깅
                queue_logger.debug(
                    f"[Queue] Raw Data | exchange={key}, "
                    f"time={datetime.fromtimestamp(current_time/1000).strftime('%H:%M:%S.%f')}, "
                    f"data={data}"
                )

                # 시장가격 모니터에 데이터 전달
                self.market_price_monitor.update_orderbook(key, data)
                self.market_price_monitor.calculate_market_price()

                # 핑/퐁 메시지인 경우 레이턴시 계산
                if isinstance(data, dict) and data.get('type') == 'pong':
                    last_ping = self.metrics_manager.metrics["last_ping_times"].get(key)
                    if last_ping:
                        latency = current_time - last_ping
                        if key not in self.metrics_manager.metrics["latencies"]:
                            self.metrics_manager.metrics["latencies"][key] = []
                        self.metrics_manager.metrics["latencies"][key].append(latency)
                        self.metrics_manager.metrics["latencies"][key] = self.metrics_manager.metrics["latencies"][key][-100:]
                        logger.debug(
                            f"[Queue] {key} 레이턴시 업데이트 | "
                            f"latency={latency:.2f}ms, "
                            f"avg={sum(self.metrics_manager.metrics['latencies'][key])/len(self.metrics_manager.metrics['latencies'][key]):.2f}ms"
                        )

                # 메시지 타입 결정
                msg_type = "orderbook"  # 기본값
                if isinstance(data, dict):
                    msg_type = (
                        data.get("type", "orderbook")  # 일반적인 type 필드
                        if data.get("type") not in ["snapshot", "delta"]  # snapshot/delta가 아닌 경우
                        else data["type"]  # snapshot/delta인 경우
                    )

                # 거래소별 로거로 메시지 로깅
                # logger.debug(
                #     f"[Queue] 메시지 수신 | "
                #     f"type={msg_type}, "
                #     f"symbol={data.get('symbol', 'unknown')}"
                # )

                self.update_metrics(key, "message")

                if self.callback:
                    await self.callback(key, data)

                self.output_queue.task_done()
                
            except Exception as e:
                self.update_metrics("unknown", "error")
                logger = unified_logger  # 에러 상황에서는 unified_logger 사용
                logger.error(
                    f"[Queue] 메시지 처리 실패 | error={str(e)}",
                    exc_info=True
                )

    async def start_exchange_websocket(self, exchange_name: str, symbols: List[str]):
        try:
            exchange_name_lower = exchange_name.lower()
            ws_class = EXCHANGE_CLASS_MAP.get(exchange_name_lower)
            
            # 거래소별 로거 선택
            logger = EXCHANGE_LOGGER_MAP.get(exchange_name_lower, unified_logger)
            
            if not ws_class:
                logger.error(f"[WebsocketManager] 지원하지 않는 거래소: {exchange_name}")
                return

            if not symbols:
                logger.warning(f"[WebsocketManager] 구독할 심볼이 없음: {exchange_name}")
                return

            logger.info(
                f"[WebsocketManager][{exchange_name}] 웹소켓 시작 | "
                f"symbols={len(symbols)}개: {symbols}"
            )

            # 시작 시간 기록
            current_time = time.time()
            self.metrics_manager.initialize_exchange(exchange_name_lower)
            self.metrics_manager.metrics["connection_status"][exchange_name_lower] = False
            self.metrics_manager.metrics["message_counts"][exchange_name_lower] = 0
            self.metrics_manager.metrics["error_counts"][exchange_name_lower] = 0
            self.metrics_manager.metrics["latencies"][exchange_name_lower] = []
            self.metrics_manager.metrics["last_ping_times"][exchange_name_lower] = current_time * 1000

            # binancefuture: 여러 그룹으로 나누어 처리
            if exchange_name_lower == "binancefuture":
                group_size = 10
                symbol_groups = [symbols[i:i+group_size] for i in range(0, len(symbols), group_size)]
                
                # 기존 인스턴스가 있다면 정리
                for key in list(self.websockets.keys()):
                    if key.startswith(exchange_name_lower):
                        if self.websockets[key]:
                            await self.websockets[key].shutdown()
                        self.websockets.pop(key, None)
                        self.tasks.pop(key, None)
                
                for idx, group_syms in enumerate(symbol_groups):
                    instance_key = f"{exchange_name_lower}_{idx}"
                    logger.info(
                        f"[WebsocketManager] {exchange_name} 그룹 {idx+1}/{len(symbol_groups)} 시작 | "
                        f"symbols={group_syms}"
                    )
                    
                    ws_instance = ws_class(self.settings)
                    ws_instance.instance_key = "binancefuture"
                    ws_instance.set_output_queue(self.output_queue)
                    ws_instance.connection_status_callback = lambda _, status: self.update_metrics("binancefuture", status)
                    ws_instance.start_time = time.time()

                    self.websockets[instance_key] = ws_instance
                    self.tasks[instance_key] = asyncio.create_task(
                        ws_instance.start({exchange_name_lower: group_syms})
                    )
            else:
                # 기존 인스턴스가 있다면 정리
                if exchange_name_lower in self.websockets:
                    if self.websockets[exchange_name_lower]:
                        await self.websockets[exchange_name_lower].shutdown()
                    self.websockets.pop(exchange_name_lower, None)
                    self.tasks.pop(exchange_name_lower, None)
                
                ws_instance = ws_class(self.settings)
                ws_instance.set_output_queue(self.output_queue)
                ws_instance.connection_status_callback = lambda _, status: self.update_metrics(exchange_name_lower, status)
                ws_instance.start_time = time.time()

                self.websockets[exchange_name_lower] = ws_instance
                self.tasks[exchange_name_lower] = asyncio.create_task(
                    ws_instance.start({exchange_name_lower: symbols})
                )

        except Exception as e:
            logger = EXCHANGE_LOGGER_MAP.get(exchange_name_lower, unified_logger)
            logger.error(
                f"[WebsocketManager][{exchange_name}] 시작 실패 | "
                f"error={str(e)}, symbols={symbols}",
                exc_info=True
            )
            self.metrics_manager.metrics["error_counts"][exchange_name_lower] = self.metrics_manager.metrics["error_counts"].get(exchange_name_lower, 0) + 1

    async def start_all_websockets(self, filtered_data: Dict[str, List[str]]):
        try:
            unified_logger.info(f"[WebsocketManager] 웹소켓 시작 | filtered_data={filtered_data}")
            
            self.tasks['queue'] = asyncio.create_task(self.process_queue())
            unified_logger.info("[WebsocketManager] 큐 처리 태스크 생성 완료")
            
            for exchange, syms in filtered_data.items():
                logger = EXCHANGE_LOGGER_MAP.get(exchange.lower(), unified_logger)
                logger.info(f"[WebsocketManager][{exchange}] 시작 준비 | symbols={syms}")
                await self.start_exchange_websocket(exchange, syms)
                logger.info(f"[WebsocketManager][{exchange}] 시작 완료")
                
        except Exception as e:
            unified_logger.error(f"[WebsocketManager] 웹소켓 시작 실패: {e}", exc_info=True)

    async def shutdown(self):
        self.stop_event.set()
        
        tasks = [ws.shutdown() for ws in self.websockets.values()]
        await asyncio.gather(*tasks, return_exceptions=True)
        for t in self.tasks.values():
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        self.websockets.clear()
        self.tasks.clear()
        unified_logger.info("모든 웹소켓 종료 완료")

    async def add_connection(self, websocket):
        async with self._connection_lock:
            self._active_connections.add(websocket)
            self.logger.info(f"[WebsocketManager] New connection added. Total: {len(self._active_connections)}")
    
    async def remove_connection(self, websocket):
        async with self._connection_lock:
            self._active_connections.discard(websocket)
            self.logger.info(f"[WebsocketManager] Connection removed. Remaining: {len(self._active_connections)}")
    
    async def is_connected(self, websocket) -> bool:
        async with self._connection_lock:
            return websocket in self._active_connections

    async def check_message_delays(self):
        while not self.stop_event.is_set():
            try:
                current_time = time.time() * 1000
                for exchange, last_msg_time in self.metrics_manager.metrics["last_message_times"].items():
                    if not last_msg_time:
                        continue
                        
                    delay = current_time - last_msg_time
                    connection_status = self.metrics_manager.metrics["connection_status"].get(exchange, False)
                    message_count = self.metrics_manager.metrics["message_counts"].get(exchange, 0)
                    
                    self.logger.info(
                        f"[{exchange}] 연결 상태 체크 | "
                        f"연결={connection_status}, "
                        f"지연={delay/1000:.1f}초, "
                        f"총 메시지={message_count:,}개"
                    )
                    
                await asyncio.sleep(60)  # 1분마다 체크
                
            except Exception as e:
                self.logger.error(f"연결 상태 체크 중 오류: {e}")

    async def cleanup_old_metrics(self):
        self.metrics_manager.cleanup_old_metrics()

    async def monitor_memory_usage(self):
        while not self.stop_event.is_set():
            try:
                process = psutil.Process()
                memory_info = process.memory_info()
                
                self.logger.info(
                    f"메모리 사용량 | "
                    f"RSS={memory_info.rss/1024/1024:.1f}MB, "
                    f"VMS={memory_info.vms/1024/1024:.1f}MB"
                )
                
                await asyncio.sleep(300)  # 5분마다 체크
                
            except Exception as e:
                self.logger.error(f"메모리 모니터링 중 오류: {e}")

    def get_usdt_price(self) -> float:
        """현재 USDT/KRW 가격 조회"""
        return self.current_usdt_rate

    async def _send_metrics_to_client(self, websocket: WebSocket) -> None:
        try:
            # 시스템 메트릭
            cpu_percent = psutil.cpu_percent(interval=None)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            # 네트워크 메트릭
            network_metrics = await self.collect_network_metrics()
            
            current_time = time.time()
            formatted_time = datetime.fromtimestamp(current_time).strftime('%H:%M:%S')
            
            # USDT/KRW 가격 추가
            usdt_prices = {"average": self.current_usdt_rate}
            
            # 시스템 메트릭 데이터 구조화
            system_metrics = {
                "timestamp": int(current_time * 1000),
                "time": formatted_time,
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "disk_percent": disk.percent,
                "network_in": network_metrics.get('network_in', 0),
                "network_out": network_metrics.get('network_out', 0)
            }
            
            # 웹소켓 상태
            websocket_stats = {}
            try:
                aggregated_metrics = self.metrics_manager.get_metrics()
                
                for exchange, metrics in aggregated_metrics.items():
                    websocket_stats[exchange] = {
                        "connected": metrics.get("connected", False),
                        "message_count": metrics.get("message_count", 0),
                        "messages_per_second": metrics.get("messages_per_second", 0.0),
                        "error_count": metrics.get("error_count", 0),
                        "latency_ms": metrics.get("latency_ms", 0.0),
                        "last_message_time": metrics.get("last_message_time", 0)
                    }
            except Exception as e:
                self.logger.error(f"메트릭 수집 중 오류: {str(e)}", exc_info=True)
            
            # 업타임 계산
            uptime_seconds = int(current_time - self.start_time)
            hours = uptime_seconds // 3600
            minutes = (uptime_seconds % 3600) // 60
            seconds = uptime_seconds % 60
            
            # 클라이언트에 전송할 데이터 구성
            data = {
                "uptime": {
                    "formatted": f"{hours:02d}:{minutes:02d}:{seconds:02d}",
                    "seconds": uptime_seconds
                },
                "system_metrics": system_metrics,
                "websocket_stats": websocket_stats,
                "usdt_prices": usdt_prices  # USDT 가격 데이터
            }
            
            # 데이터 전송 전 로깅
            self.logger.debug(
                f"[WebsocketManager] 클라이언트 데이터 전송 | "
                f"USDT 가격: 업비트={usdt_prices.get('upbit', 0):,.2f}, "
                f"빗썸={usdt_prices.get('bithumb', 0):,.2f}, "
                f"평균={usdt_prices.get('average', 0):,.2f}"
            )
            
            await websocket.send_json(data)
            
        except WebSocketDisconnect:
            raise
        except Exception as e:
            self.logger.error(f"클라이언트 데이터 전송 중 오류: {str(e)}", exc_info=True)
            raise