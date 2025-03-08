"""
웹소켓 관리자 모듈

이 모듈은 여러 거래소의 웹소켓 연결을 중앙에서 관리하는 기능을 제공합니다.
주요 기능:
1. 거래소별 웹소켓 연결 관리
2. 메시지 큐 처리
3. 연결 상태 모니터링
4. 메트릭 수집 및 관리
5. 메모리 사용량 모니터링

작성자: CrossKimp Arbitrage Bot 개발팀
최종수정: 2024.03
"""

import asyncio
import time
import psutil
from datetime import datetime
from typing import Callable, List, Dict, Any, Optional
from fastapi import WebSocket, WebSocketDisconnect
from collections import defaultdict
import json
import os

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, get_queue_logger
from crosskimp.ob_collector.core.ws_usdtkrw import WsUsdtKrwMonitor
from crosskimp.ob_collector.config.config_loader import get_settings
from crosskimp.ob_collector.config.constants import EXCHANGE_NAMES_KR, LOG_SYSTEM
from crosskimp.ob_collector.orderbook.manager.websocket_metrics_manager import WebsocketMetricsManager
from crosskimp.ob_collector.orderbook.websocket.binance_spot_websocket import BinanceSpotWebsocket
from crosskimp.ob_collector.orderbook.websocket.binance_future_websocket import BinanceFutureWebsocket
from crosskimp.ob_collector.orderbook.websocket.bybit_spot_websocket import BybitSpotWebsocket
from crosskimp.ob_collector.orderbook.websocket.bybit_future_websocket import BybitFutureWebsocket
from crosskimp.ob_collector.orderbook.websocket.upbit_websocket import UpbitWebsocket
from crosskimp.ob_collector.orderbook.websocket.bithumb_spot_websocket import BithumbSpotWebsocket

from crosskimp.telegrambot.notification.telegram_bot import send_telegram_message

# ============================
# 상수 정의
# ============================
EXCHANGE_CLASS_MAP = {
    "binance": BinanceSpotWebsocket,
    "binancefuture": BinanceFutureWebsocket,
    "bybit": BybitSpotWebsocket,
    "bybitfuture": BybitFutureWebsocket,
    "upbit": UpbitWebsocket,
    "bithumb": BithumbSpotWebsocket
}

# ============================
# 로깅 설정
# ============================
logger = get_unified_logger()
queue_logger = get_queue_logger()

class ExchangeMetrics:
    def __init__(self):
        self.connection_status = False
        self.last_message_time = 0
        self.message_count = 0
        self.message_rates = defaultdict(int)  # 분당 메시지 수
        self.orderbook_count = 0
        self.orderbook_rates = defaultdict(int)  # 분당 오더북 수
        self.error_count = 0
        self.reconnect_count = 0
        self.errors = []
        self.latency_ms = 0
        self.last_latency_check = 0
        self.last_minute = ""
        
        # 성능 메트릭
        self.processing_times = []  # 메시지 처리 시간 기록
        self.memory_usage = 0
        self.thread_count = 0
        
        # 네트워크 메트릭
        self.bytes_sent = 0
        self.bytes_received = 0
        self.last_network_check = 0

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
        self.start_time = time.time()

        # 메트릭 매니저 초기화
        self.metrics = {
            "binance": ExchangeMetrics(),
            "binancefuture": ExchangeMetrics(),
            "bybit": ExchangeMetrics(),
            "bybitfuture": ExchangeMetrics(),
            "upbit": ExchangeMetrics(),
            "bithumb": ExchangeMetrics()
        }

        # 메트릭 저장 경로 설정 (절대 경로 사용)
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
        self.metrics_dir = os.path.join(self.base_dir, "logs", "metrics")
        
        # 메트릭 디렉토리 생성
        try:
            os.makedirs(self.metrics_dir, exist_ok=True)
            logger.info(f"메트릭 디렉토리 생성/확인 완료: {self.metrics_dir}")
        except Exception as e:
            logger.error(f"메트릭 디렉토리 생성 실패: {str(e)}", exc_info=True)
        
        self.last_save_time = 0  # 초기값을 0으로 설정하여 첫 실행시 즉시 저장되도록 함
        self.save_interval = 60  # 1분마다 저장

        # 시장가격 모니터 초기화
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
            self.metrics[exchange].last_message_time = current_time * 1000

    def update_connection_status(self, exchange: str, status: bool):
        """연결 상태 업데이트"""
        metrics = self.metrics.get(exchange)
        if metrics:
            if metrics.connection_status != status:
                if status:
                    logger.info(f"[{exchange}] 연결됨")
                else:
                    metrics.reconnect_count += 1
                    logger.warning(f"[{exchange}] 연결 끊김 (재연결 {metrics.reconnect_count}회)")
            metrics.connection_status = status
            metrics.last_message_time = time.time()
            logger.debug(f"[{exchange}] 연결 상태 업데이트: {status}")

    def record_message(self, exchange: str, size: int = 0):
        """메시지 수신 기록"""
        metrics = self.metrics.get(exchange)
        if metrics:
            current_minute = datetime.now().strftime("%Y-%m-%d %H:%M")
            if current_minute != metrics.last_minute:
                metrics.message_rates[current_minute] = 0
                metrics.last_minute = current_minute
            
            metrics.message_count += 1
            metrics.message_rates[current_minute] += 1
            metrics.last_message_time = time.time()
            metrics.bytes_received += size

    def record_orderbook(self, exchange: str, processing_time: float):
        """오더북 처리 기록"""
        metrics = self.metrics.get(exchange)
        if metrics:
            current_minute = datetime.now().strftime("%Y-%m-%d %H:%M")
            metrics.orderbook_count += 1
            metrics.orderbook_rates[current_minute] += 1
            metrics.processing_times.append(processing_time)
            
            # 최근 100개의 처리 시간만 유지
            if len(metrics.processing_times) > 100:
                metrics.processing_times.pop(0)

    def record_error(self, exchange: str, error: str):
        """에러 기록"""
        metrics = self.metrics.get(exchange)
        if metrics:
            metrics.error_count += 1
            metrics.errors.append({
                "timestamp": datetime.now().isoformat(),
                "error": error
            })
            # 최근 100개의 에러만 유지
            if len(metrics.errors) > 100:
                metrics.errors.pop(0)

    def record_latency(self, exchange: str, latency_ms: float):
        """레이턴시 기록"""
        metrics = self.metrics.get(exchange)
        if metrics:
            metrics.latency_ms = latency_ms
            metrics.last_latency_check = time.time()

    def register_callback(self, callback: Callable[[str, dict], None]):
        """콜백 함수 등록"""
        self.callback = callback

    def update_usdt_rate(self, rate: float):
        """USDT 환율 업데이트"""
        self.current_usdt_rate = rate

    async def add_connection(self, websocket):
        """웹소켓 연결 추가"""
        async with self._connection_lock:
            self._active_connections.add(websocket)
            logger.info(f"{LOG_SYSTEM} 새 연결 추가됨. 총 {len(self._active_connections)}개")
    
    async def remove_connection(self, websocket):
        """웹소켓 연결 제거"""
        async with self._connection_lock:
            self._active_connections.discard(websocket)
            logger.info(f"{LOG_SYSTEM} 연결 제거됨. 남은 연결 {len(self._active_connections)}개")
    
    async def is_connected(self, websocket) -> bool:
        """웹소켓 연결 상태 확인"""
        async with self._connection_lock:
            return websocket in self._active_connections

    async def check_message_delays(self):
        """메시지 지연 체크"""
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                for exchange, metrics in self.metrics.items():
                    if not metrics.last_message_time:
                        continue
                        
                    delay = current_time - metrics.last_message_time
                    logger.info(
                        f"{LOG_SYSTEM} [{EXCHANGE_NAMES_KR.get(exchange, exchange)}] 연결 상태 체크 | "
                        f"연결={'예' if metrics.connection_status else '아니오'}, "
                        f"지연={delay:.1f}초, "
                        f"총 메시지={metrics.message_count:,}개"
                    )
                    
                await asyncio.sleep(60)  # 1분마다 체크
                
            except Exception as e:
                logger.error(f"{LOG_SYSTEM} 연결 상태 체크 중 오류: {e}")

    def get_usdt_price(self) -> float:
        """현재 USDT/KRW 가격 조회"""
        return self.current_usdt_rate

    def save_metrics(self):
        """메트릭 저장"""
        try:
            current_time = time.time()
            
            # 저장 간격 체크 및 로깅
            time_since_last_save = current_time - self.last_save_time
            logger.debug(f"마지막 저장 후 경과 시간: {time_since_last_save:.1f}초")
            
            if time_since_last_save < self.save_interval:
                logger.debug(f"저장 간격({self.save_interval}초)이 지나지 않았습니다.")
                return

            # 현재 날짜와 시간으로 파일명 생성
            current_date = datetime.now().strftime("%Y%m%d")
            current_time_str = datetime.now().strftime("%H%M%S")
            metrics_file = os.path.join(self.metrics_dir, f"metrics_{current_date}.json")
            
            # 새로운 메트릭 데이터 생성
            new_metric = {
                "timestamp": datetime.now().isoformat(),
                "time": current_time_str,
                "uptime": current_time - self.start_time,
                "system": self.get_system_metrics(),
                "exchanges": {}
            }
            
            # 거래소 데이터 수집
            for exchange, metrics in self.metrics.items():
                current_minute = datetime.now().strftime("%Y-%m-%d %H:%M")
                new_metric["exchanges"][exchange] = {
                    "connection_status": metrics.connection_status,
                    "message_count": metrics.message_count,
                    "message_rate": metrics.message_rates[current_minute],
                    "orderbook_count": metrics.orderbook_count,
                    "orderbook_rate": metrics.orderbook_rates[current_minute],
                    "error_count": metrics.error_count,
                    "reconnect_count": metrics.reconnect_count,
                    "latency_ms": metrics.latency_ms,
                    "avg_processing_time": sum(metrics.processing_times) / len(metrics.processing_times) if metrics.processing_times else 0,
                    "bytes_received": metrics.bytes_received,
                    "bytes_sent": metrics.bytes_sent
                }
            
            # 기존 데이터 로드 또는 새로운 데이터 구조 생성
            try:
                if os.path.exists(metrics_file):
                    with open(metrics_file, 'r') as f:
                        data = json.load(f)
                        if not isinstance(data, dict):
                            data = {"metrics": []}
                        if "metrics" not in data:
                            data["metrics"] = []
                else:
                    data = {"metrics": []}
                
                # 새로운 메트릭 추가
                data["metrics"].append(new_metric)
                
                # 최대 1440개(24시간)의 데이터만 유지
                if len(data["metrics"]) > 1440:
                    data["metrics"] = data["metrics"][-1440:]
                
                # 파일 저장
                with open(metrics_file, 'w') as f:
                    json.dump(data, f, indent=2)
                
                logger.info(f"메트릭 추가 완료: {metrics_file} (총 {len(data['metrics'])}개 데이터)")
                
            except Exception as e:
                logger.error(f"메트릭 파일 처리 중 오류: {str(e)}", exc_info=True)
                return
            
            self.last_save_time = current_time
            
        except Exception as e:
            logger.error(f"메트릭 저장 중 오류 발생: {str(e)}", exc_info=True)

    def get_system_metrics(self):
        """시스템 메트릭 수집"""
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        network = psutil.net_io_counters()
        
        return {
            "cpu_percent": cpu_percent,
            "memory_percent": memory.percent,
            "memory_used": memory.used,
            "memory_total": memory.total,
            "disk_percent": disk.percent,
            "disk_used": disk.used,
            "disk_total": disk.total,
            "network_bytes_sent": network.bytes_sent,
            "network_bytes_recv": network.bytes_recv
        }

    async def process_queue(self):
        """메시지 큐 처리"""
        while not self.stop_event.is_set():
            try:
                start_time = time.time()
                exchange, data = await self.output_queue.get()
                
                # 메시지 처리 시간 측정
                processing_time = (time.time() - start_time) * 1000  # ms 단위
                
                # 모든 메시지 기록
                self.record_message(exchange, len(str(data)))
                
                # 메시지 타입 판별
                msg_type = "other"
                if isinstance(data, dict):
                    # 바이낸스 선물/현물
                    if 'stream' in data and 'depth' in data['stream'] and 'data' in data:
                        actual_data = data['data']
                        if actual_data.get('e') == 'depthUpdate':
                            msg_type = "orderbook_delta"
                    elif 'e' in data and data['e'] == 'depthUpdate':
                        msg_type = "orderbook_delta"
                    # 빗썸
                    elif 'type' in data and data['type'] == 'orderbookdepth':
                        msg_type = "orderbook_delta"
                    # 바이빗 선물
                    elif 'result' in data and isinstance(data['result'], dict):
                        result = data['result']
                        if 'b' in result and 'a' in result:
                            msg_type = "orderbook_delta"
                    # 바이빗
                    elif 'topic' in data and 'orderbook' in data['topic']:
                        if data.get('type') == 'delta':
                            msg_type = "orderbook_delta"
                        elif data.get('type') == 'snapshot':
                            msg_type = "orderbook_snapshot"
                    # 업비트
                    elif 'type' in data and data['type'] == 'orderbook':
                        msg_type = "orderbook_delta"
                    # 일반적인 오더북 형식 (bids/asks)
                    elif 'bids' in data and 'asks' in data:
                        msg_type = "orderbook_delta"
                
                # 큐 데이터 로깅 (메시지 타입 포함)
                queue_logger.info(f"큐 데이터 [{msg_type}] - exchange: {exchange}, data: {data}")
                
                # 오더북 메시지인 경우 처리 (스냅샷과 델타 모두 포함)
                if "orderbook" in msg_type:
                    self.record_orderbook(exchange, processing_time)
                    logger.debug(f"오더북 데이터 처리 - exchange: {exchange}, type: {msg_type}, processing_time: {processing_time:.2f}ms")

                if self.callback:
                    await self.callback(exchange, data)

                self.output_queue.task_done()
                
            except Exception as e:
                self.record_error("unknown", str(e))
                logger.error(f"{LOG_SYSTEM} 큐 처리 실패: {str(e)}", exc_info=True)

    async def start_exchange_websocket(self, exchange_name: str, symbols: List[str]):
        try:
            exchange_name_lower = exchange_name.lower()
            ws_class = EXCHANGE_CLASS_MAP.get(exchange_name_lower)
            
            if not ws_class:
                logger.error(f"{LOG_SYSTEM} [{EXCHANGE_NAMES_KR.get(exchange_name, exchange_name)}] 지원하지 않는 거래소")
                return

            if not symbols:
                logger.warning(f"{LOG_SYSTEM} [{EXCHANGE_NAMES_KR.get(exchange_name, exchange_name)}] 구독할 심볼이 없음")
                return

            logger.info(f"[{EXCHANGE_NAMES_KR.get(exchange_name, exchange_name)}] 웹소켓 시작 | symbols={len(symbols)}개: {symbols}")

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
                    logger.info(f"[{EXCHANGE_NAMES_KR.get(exchange_name, exchange_name)}] 그룹 {idx+1}/{len(symbol_groups)} 시작 | symbols={group_syms}")
                    
                    ws_instance = ws_class(self.settings)
                    ws_instance.instance_key = "binancefuture"
                    ws_instance.set_output_queue(self.output_queue)
                    ws_instance.connection_status_callback = self.update_connection_status
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
                ws_instance.connection_status_callback = self.update_connection_status
                ws_instance.start_time = time.time()

                self.websockets[exchange_name_lower] = ws_instance
                self.tasks[exchange_name_lower] = asyncio.create_task(
                    ws_instance.start({exchange_name_lower: symbols})
                )

        except Exception as e:
            self.record_error(exchange_name_lower, str(e))
            logger.error(f"{LOG_SYSTEM} [{exchange_name}] 시작 실패: {str(e)}", exc_info=True)

    async def start_all_websockets(self, filtered_data: Dict[str, List[str]]):
        try:
            logger.info(f"{LOG_SYSTEM} 웹소켓 시작 | filtered_data={filtered_data}")
            
            self.tasks['queue'] = asyncio.create_task(self.process_queue())
            logger.info(f"{LOG_SYSTEM} 큐 처리 태스크 생성 완료")
            
            # 메트릭 모니터링 태스크 시작
            self.tasks['metrics'] = asyncio.create_task(self.monitor_metrics())
            logger.info(f"{LOG_SYSTEM} 메트릭 모니터링 태스크 생성 완료")
            
            for exchange, syms in filtered_data.items():
                logger.info(f"[{EXCHANGE_NAMES_KR.get(exchange, exchange)}] 웹소켓 시작 준비 | symbols={syms}")
                await self.start_exchange_websocket(exchange, syms)
                logger.info(f"[{EXCHANGE_NAMES_KR.get(exchange, exchange)}] 웹소켓 시작 완료")
                
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 웹소켓 시작 실패: {e}", exc_info=True)

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
        logger.info(f"{LOG_SYSTEM} 모든 웹소켓 종료 완료")

    async def check_alerts(self):
        """경고 상태 체크"""
        current_time = time.time()
        system = self.get_system_metrics()
        
        # 시스템 리소스 체크
        if system["cpu_percent"] > 90:
            logger.warning(f"CPU 사용량 높음: {system['cpu_percent']}%")
        if system["memory_percent"] > 90:
            logger.warning(f"메모리 사용량 높음: {system['memory_percent']}%")
        if system["disk_percent"] > 90:
            logger.warning(f"디스크 사용량 높음: {system['disk_percent']}%")
            
        # 거래소별 체크
        for exchange, metrics in self.metrics.items():
            if not metrics.connection_status:
                logger.warning(f"{exchange} 웹소켓 연결 끊김")
            elif current_time - metrics.last_message_time > 60:
                logger.warning(f"{exchange} 1분 이상 메시지 수신 없음")
            if metrics.latency_ms > 1000:  # 1초 이상
                logger.warning(f"{exchange} 높은 레이턴시: {metrics.latency_ms:.2f}ms")

    async def monitor_metrics(self):
        """메트릭 모니터링 태스크"""
        logger.info(f"{LOG_SYSTEM} 메트릭 모니터링 시작 (저장 간격: {self.save_interval}초)")
        while not self.stop_event.is_set():
            try:
                logger.debug("메트릭 저장 시도 중...")
                self.save_metrics()
                await self.check_alerts()
                await asyncio.sleep(self.metric_update_interval)
            except Exception as e:
                logger.error(f"메트릭 모니터링 중 오류: {str(e)}", exc_info=True)
                await asyncio.sleep(60)  # 오류 발생시 1분 대기 후 재시도