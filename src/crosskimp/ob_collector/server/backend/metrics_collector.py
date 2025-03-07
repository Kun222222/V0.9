# file: server/backend/metrics_collector.py

import psutil
import asyncio
import aiohttp
import time
from typing import Dict
from utils.logging.logger import unified_logger  # 반드시 utils.logger에서 가져옴

class MetricsCollector:
    def __init__(self, api_url: str):
        """
        api_url 예: http://localhost:8000
        """
        self.api_url = api_url
        self.session = None
        
    async def start(self):
        try:
            unified_logger.info(
                f"[MetricsCollector] 메트릭 수집 시작 | "
                f"API={self.api_url}, "
                f"수집 주기(초)=시스템({self.metric_intervals['system']:.1f}), "
                f"네트워크({self.metric_intervals['network']:.1f}), "
                f"상세({self.metric_intervals['detailed']:.1f})"
            )

            self.session = aiohttp.ClientSession()
            
            # 여러 태스크 동시 실행
            tasks = [
                self.collect_system_metrics(),
                self.collect_websocket_metrics(),
                self.collect_orderbook_metrics()
            ]
            
            unified_logger.debug("[MetricsCollector] 메트릭 수집 태스크 시작")
            await asyncio.gather(*tasks)
            
        except Exception as e:
            unified_logger.error(
                f"[MetricsCollector] 시작 실패 | error={str(e)}",
                exc_info=True
            )
            raise

    async def collect_system_metrics(self):
        unified_logger.info("[MetricsCollector] 시스템 메트릭 수집 시작")
        
        while True:
            try:
                # CPU 사용량 측정
                cpu_percent = psutil.cpu_percent(interval=1)
                cpu_times = psutil.cpu_times_percent()
                cpu_freq = psutil.cpu_freq()
                
                # 메모리 사용량
                mem = psutil.virtual_memory()
                swap = psutil.swap_memory()
                
                # 메트릭 구성
                metrics = {
                    "timestamp": int(time.time() * 1000),
                    "system": {
                        "cpu": {
                            "total": cpu_percent,
                            "user": cpu_times.user,
                            "system": cpu_times.system,
                            "idle": cpu_times.idle,
                            "frequency": cpu_freq.current if cpu_freq else None
                        },
                        "memory": {
                            "total": mem.total,
                            "used": mem.used,
                            "free": mem.free,
                            "percent": mem.percent,
                            "swap_used": swap.used,
                            "swap_free": swap.free,
                            "swap_percent": swap.percent
                        }
                    }
                }
                
                # 리소스 사용량이 높은 경우 경고
                if cpu_percent > 80:
                    unified_logger.warning(
                        f"[MetricsCollector] 높은 CPU 사용량 감지 | "
                        f"usage={cpu_percent}%, user={cpu_times.user}%, system={cpu_times.system}%"
                    )
                if mem.percent > 85:
                    unified_logger.warning(
                        f"[MetricsCollector] 높은 메모리 사용량 감지 | "
                        f"usage={mem.percent}%, used={mem.used/1024/1024/1024:.1f}GB, "
                        f"free={mem.free/1024/1024/1024:.1f}GB"
                    )
                
                # API 서버로 전송
                try:
                    async with self.session.post(
                        f"{self.api_url}/metrics/system",
                        json=metrics
                    ) as resp:
                        if resp.status != 200:
                            unified_logger.error(
                                f"[MetricsCollector] 시스템 메트릭 전송 실패 | "
                                f"status={resp.status}, url={self.api_url}/metrics/system"
                            )
                except Exception as e:
                    unified_logger.error(
                        f"[MetricsCollector] 시스템 메트릭 전송 오류 | error={str(e)}",
                        exc_info=True
                    )
                
                # 상세 디버그 로깅
                unified_logger.debug(
                    f"[MetricsCollector] 시스템 메트릭 수집 완료 | "
                    f"cpu={cpu_percent}%, mem={mem.percent}%, "
                    f"swap={swap.percent}%"
                )
                
                await asyncio.sleep(1)
                
            except Exception as e:
                unified_logger.error(
                    f"[MetricsCollector] 시스템 메트릭 수집 실패 | error={str(e)}",
                    exc_info=True
                )
                await asyncio.sleep(5)

    async def collect_websocket_metrics(self):
        unified_logger.info("[MetricsCollector] 웹소켓 메트릭 수집 시작")
        exchanges = ["binance", "binancefuture", "bybit", "bybitfuture", "upbit", "bithumb"]
        
        while True:
            try:
                for exchange in exchanges:
                    metrics = {
                        "connected": True,
                        "last_message_time": time.time(),
                        "message_count": 0,
                        "error_count": 0
                    }
                    url = f"{self.api_url}/metrics/websocket/{exchange}"
                    
                    try:
                        async with self.session.post(url, json=metrics) as resp:
                            if resp.status == 200:
                                unified_logger.debug(
                                    f"[MetricsCollector] 웹소켓 메트릭 전송 성공 | "
                                    f"exchange={exchange}"
                                )
                            else:
                                unified_logger.warning(
                                    f"[MetricsCollector] 웹소켓 메트릭 전송 실패 | "
                                    f"exchange={exchange}, status={resp.status}"
                                )
                    except Exception as e:
                        unified_logger.error(
                            f"[MetricsCollector] 웹소켓 메트릭 전송 오류 | "
                            f"exchange={exchange}, error={str(e)}"
                        )
                        
                await asyncio.sleep(1)
                
            except Exception as e:
                unified_logger.error(
                    f"[MetricsCollector] 웹소켓 메트릭 수집 실패 | error={str(e)}",
                    exc_info=True
                )
                await asyncio.sleep(5)

    async def collect_orderbook_metrics(self):
        unified_logger.info("[MetricsCollector] 오더북 메트릭 수집 시작")
        exchanges = ["binance", "binancefuture", "bybit", "bybitfuture", "upbit", "bithumb"]
        
        while True:
            try:
                for exchange in exchanges:
                    metrics = {
                        "update_count": 0,
                        "symbols": [],
                        "depth": 0,
                        "last_update": time.time()
                    }
                    url = f"{self.api_url}/metrics/orderbook/{exchange}"
                    
                    try:
                        async with self.session.post(url, json=metrics) as resp:
                            if resp.status == 200:
                                unified_logger.debug(
                                    f"[MetricsCollector] 오더북 메트릭 전송 성공 | "
                                    f"exchange={exchange}"
                                )
                            else:
                                unified_logger.warning(
                                    f"[MetricsCollector] 오더북 메트릭 전송 실패 | "
                                    f"exchange={exchange}, status={resp.status}"
                                )
                    except Exception as e:
                        unified_logger.error(
                            f"[MetricsCollector] 오더북 메트릭 전송 오류 | "
                            f"exchange={exchange}, error={str(e)}"
                        )
                        
                await asyncio.sleep(1)
                
            except Exception as e:
                unified_logger.error(
                    f"[MetricsCollector] 오더북 메트릭 수집 실패 | error={str(e)}",
                    exc_info=True
                )
                await asyncio.sleep(5)

    async def collect_network_metrics(self) -> dict:
        """네트워크 사용량 수집"""
        try:
            current_net = psutil.net_io_counters()
            current_time = time.time()
            
            if hasattr(self, '_last_net_io') and hasattr(self, '_last_net_time'):
                time_delta = current_time - self._last_net_time
                
                # 초당 바이트 계산
                bytes_sent = (current_net.bytes_sent - self._last_net_io.bytes_sent) / time_delta
                bytes_recv = (current_net.bytes_recv - self._last_net_io.bytes_recv) / time_delta
                
                # Mbps 변환
                mbps_sent = (bytes_sent * 8) / 1_000_000
                mbps_recv = (bytes_recv * 8) / 1_000_000
                
                network_metrics = {
                    'timestamp': int(current_time * 1000),
                    'mbps_sent': round(mbps_sent, 2),
                    'mbps_recv': round(mbps_recv, 2),
                    'total_sent_gb': round(current_net.bytes_sent / 1_000_000_000, 2),
                    'total_recv_gb': round(current_net.bytes_recv / 1_000_000_000, 2)
                }
            else:
                network_metrics = {
                    'timestamp': int(current_time * 1000),
                    'mbps_sent': 0,
                    'mbps_recv': 0,
                    'total_sent_gb': 0,
                    'total_recv_gb': 0
                }
            
            self._last_net_io = current_net
            self._last_net_time = current_time
            
            return network_metrics
            
        except Exception as e:
            unified_logger.error(f"네트워크 메트릭 수집 실패: {e}")
            return {}