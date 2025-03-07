# file: server/backend/monitor_server.py

from fastapi import FastAPI, WebSocket, HTTPException, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import uvicorn
from typing import Dict, List, Set
import psutil
from datetime import datetime, timedelta
import json
from utils.logging.logger import unified_logger, LOG_DIRS, get_current_time_str
import time
import os
import websockets

# MetricsStore 클래스 정의
class MetricsStore:
    def __init__(self):
        self.websocket_stats = {}
        self.orderbook_stats = {}
        self.system_stats_history = []  # 시계열 데이터 저장
        self.max_history_size = 3600  # 1시간치 데이터 보관
        self.settings = {}
        self.profit_stats = {
            'daily': [],
            'monthly': [],
            'yearly': []
        }
        self.system_metrics_history = []
        self.max_metrics_history = 30  # 최대 30개 데이터 포인트 저장

    def add_system_metric(self, metric: dict):
        """시스템 메트릭 추가"""
        try:
            # 타임스탬프 확인
            if 'timestamp' not in metric:
                metric['timestamp'] = int(time.time() * 1000)
            
            # 메트릭 저장
            self.system_metrics_history.append(metric)
            
            # 최대 개수 제한
            if len(self.system_metrics_history) > self.max_metrics_history:
                self.system_metrics_history = self.system_metrics_history[-self.max_metrics_history:]
                
        except Exception as e:
            unified_logger.error(f"[MetricsStore] Failed to add system metric: {e}")

class MonitoringServer:
    def __init__(self, websocket_manager, host="0.0.0.0", port=8000):
        self.app = FastAPI()
        self.websocket_manager = websocket_manager
        self.host = host
        self.port = port
        self.clients: Set[WebSocket] = set()
        self.running = False
        
        # Logger 초기화 추가
        self.logger = unified_logger
        
        # MetricsStore 인스턴스 초기화 추가
        self.metrics = MetricsStore()
        
        self.setup_routes()
        
        # 클라이언트 연결 관리를 위한 락 추가
        self._clients_lock = asyncio.Lock()
        
        # 메트릭 수집 주기 설정
        self.metric_intervals = {
            'system': 1.0,      # 시스템 메트릭 (1초)
            'network': 1.0,     # 네트워크 메트릭 (1초)
            'detailed': 1.0     # 상세 메트릭 (1초)
        }
        
        # 네트워크 사용량 추적
        self._last_net_io = None
        self._last_net_time = None
        
        # 병목 감지 임계값
        self.bottleneck_thresholds = {
            'cpu_percent': 80.0,
            'memory_percent': 85.0,
            'network_usage_mbps': 100.0,  # 100Mbps
            'message_queue_size': 1000
        }
        
        # 프로그램 시작 시간 추가
        self.start_time = time.time()
        
    def setup_routes(self):
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        @self.app.get("/api/settings")
        async def get_settings():
            try:
                with open("config/settings.json", "r") as f:
                    settings = json.load(f)
                return settings
            except Exception as e:
                raise HTTPException(status_code=500, detail="설정을 불러올 수 없습니다.")

        @self.app.post("/api/settings")
        async def update_settings(settings: dict):
            try:
                # 백업 생성
                if os.path.exists("config/settings.json"):
                    with open("config/settings.json.bak", "w") as f:
                        json.dump(settings, f, indent=2)
                
                # 새 설정 저장
                with open("config/settings.json", "w") as f:
                    json.dump(settings, f, indent=2)
                return {"message": "설정이 업데이트되었습니다."}
            except Exception as e:
                raise HTTPException(status_code=500, detail="설정을 저장할 수 없습니다.")

        @self.app.get("/api/api-keys")
        async def get_api_keys():
            try:
                with open("config/api_keys.json", "r") as f:
                    api_keys = json.load(f)
                # API 시크릿 키는 마스킹 처리
                masked_keys = {}
                for exchange, keys in api_keys.items():
                    masked_keys[exchange] = {
                        "api_key": keys["api_key"][:4] + "*" * (len(keys["api_key"]) - 4),
                        "api_secret": "*" * len(keys["api_secret"])
                    }
                return masked_keys
            except Exception as e:
                raise HTTPException(status_code=500, detail="API 키를 불러올 수 없습니다.")

        @self.app.post("/api/api-keys")
        async def update_api_keys(api_keys: dict):
            try:
                # 백업 생성
                if os.path.exists("config/api_keys.json"):
                    with open("config/api_keys.json.bak", "w") as f:
                        json.dump(api_keys, f, indent=2)
                
                # 새 API 키 저장
                with open("config/api_keys.json", "w") as f:
                    json.dump(api_keys, f, indent=2)
                return {"message": "API 키가 업데이트되었습니다."}
            except Exception as e:
                raise HTTPException(status_code=500, detail="API 키를 저장할 수 없습니다.")
        
        @self.app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            try:
                await websocket.accept()
                async with self._clients_lock:
                    self.clients.add(websocket)
                await self.websocket_manager.add_connection(websocket)
                
                while True:
                    try:
                        # 메트릭 데이터 수집
                        metrics = self.websocket_manager.get_aggregated_metrics()
                        
                        # 시스템 메트릭 수집
                        current_time = time.time()
                        process = psutil.Process()
                        
                        # CPU, 메모리 사용량
                        cpu_percent = psutil.cpu_percent(interval=None)
                        memory = psutil.virtual_memory()
                        
                        # 네트워크 사용량
                        network_metrics = await self.collect_network_metrics()
                        
                        # 업타임 계산
                        uptime_seconds = int(current_time - self.websocket_manager.start_time)
                        hours = uptime_seconds // 3600
                        minutes = (uptime_seconds % 3600) // 60
                        seconds = uptime_seconds % 60
                        
                        # USDT 가격 데이터 가져오기
                        usdt_prices = self.websocket_manager.usdt_monitor.get_all_prices()
                        
                        # 웹소켓 상태 데이터 구성
                        websocket_stats = {}
                        for exchange in ["binance", "binancefuture", "bybit", "bybitfuture", "upbit", "bithumb"]:
                            exchange_metrics = metrics.get(exchange, {})
                            websocket_stats[exchange] = {
                                "connected": exchange_metrics.get("connected", False),
                                "message_count": exchange_metrics.get("message_count", 0),
                                "messages_per_second": exchange_metrics.get("messages_per_second", 0.0),
                                "error_count": exchange_metrics.get("error_count", 0),
                                "latency_ms": exchange_metrics.get("latency_ms", 0.0),
                                "last_message_time": exchange_metrics.get("last_message_time", 0)
                            }
                            # 디버그 로깅 추가
                            self.logger.debug(
                                f"[Monitor] {exchange} 메트릭 | "
                                f"rate={exchange_metrics.get('messages_per_second', 0.0):.1f} msg/s, "
                                f"total={exchange_metrics.get('message_count', 0):,}"
                            )
                        
                        # 데이터 구성
                        data = {
                            "uptime": {
                                "formatted": f"{hours:02d}:{minutes:02d}:{seconds:02d}",
                                "seconds": uptime_seconds
                            },
                            "websocket_stats": websocket_stats,
                            "usdt_prices": usdt_prices,  # USDT 가격 데이터
                            "system_metrics": {
                                "cpu_percent": cpu_percent,
                                "memory_percent": memory.percent,
                                "network_in": network_metrics.get('network_in', 0),
                                "network_out": network_metrics.get('network_out', 0)
                            }
                        }
                        
                        # 데이터 전송 전 로깅
                        self.logger.debug(
                            f"[Monitor] 클라이언트 데이터 전송 | "
                            f"USDT 가격: 업비트={usdt_prices.get('upbit', 0):,.2f}, "
                            f"빗썸={usdt_prices.get('bithumb', 0):,.2f}, "
                            f"평균={usdt_prices.get('average', 0):,.2f} | "
                            f"연결된 거래소: {sum(1 for stat in websocket_stats.values() if stat['connected'])}"
                        )
                        
                        # 데이터 전송
                        await websocket.send_json(data)
                        
                        # 1초 대기
                        await asyncio.sleep(1.0)
                        
                    except WebSocketDisconnect:
                        break
                    except Exception as e:
                        self.logger.error(f"웹소켓 데이터 전송 중 오류: {str(e)}", exc_info=True)
                        break
                    
            except Exception as e:
                self.logger.error(f"웹소켓 연결 처리 중 오류: {str(e)}", exc_info=True)
            finally:
                async with self._clients_lock:
                    self.clients.remove(websocket)
                await self.websocket_manager.remove_connection(websocket)

        @self.app.get("/api/trades")
        async def get_trades():
            try:
                current_time = int(time.time() * 1000)  # 밀리초 단위 타임스탬프
                
                # 예시 데이터 - 실제로는 DB나 다른 소스에서 가져와야 함
                summary_trades = [
                    {
                        "date": datetime.now().strftime("%Y-%m-%d"),
                        "profit": 0.0,
                        "trade_count": 0,
                        "win_rate": 0.0
                    },
                    {
                        "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
                        "profit": 0.0,
                        "trade_count": 0,
                        "win_rate": 0.0
                    }
                ]
                
                recent_trades = [
                    {
                        "id": "trade1",
                        "timestamp": current_time,
                        "symbol": "BTC",
                        "type": "LONG",
                        "entry_price": 50000000.0,
                        "exit_price": 50100000.0,
                        "profit": 100000.0
                    }
                ]
                
                return {
                    "summary": summary_trades,
                    "recent": recent_trades
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail="거래 데이터를 불러올 수 없습니다.")

    async def collect_network_metrics(self) -> dict:
        """네트워크 사용량 수집"""
        try:
            current_net = psutil.net_io_counters()
            current_time = time.time()
            
            if self._last_net_io and self._last_net_time:
                time_delta = current_time - self._last_net_time
                
                # 초당 바이트 계산
                bytes_sent = (current_net.bytes_sent - self._last_net_io.bytes_sent) / time_delta
                bytes_recv = (current_net.bytes_recv - self._last_net_io.bytes_recv) / time_delta
                
                # MB/s 변환 (MBps로 변경)
                mbps_sent = bytes_sent / (1024 * 1024)  # bytes to MB/s
                mbps_recv = bytes_recv / (1024 * 1024)  # bytes to MB/s
                
                network_metrics = {
                    'timestamp': current_time,
                    'network_out': round(mbps_sent, 2),
                    'network_in': round(mbps_recv, 2),
                    'total_sent_gb': round(current_net.bytes_sent / (1024**3), 2),
                    'total_recv_gb': round(current_net.bytes_recv / (1024**3), 2)
                }
            else:
                network_metrics = {
                    'timestamp': current_time,
                    'network_out': 0,
                    'network_in': 0,
                    'total_sent_gb': 0,
                    'total_recv_gb': 0
                }
            
            self._last_net_io = current_net
            self._last_net_time = current_time
            
            return network_metrics
            
        except Exception as e:
            return {}

    def detect_bottlenecks(self, metrics: dict) -> list:
        """병목 현상 감지"""
        bottlenecks = []
        
        # CPU 병목
        if metrics.get('cpu_percent', 0) > self.bottleneck_thresholds['cpu_percent']:
            bottlenecks.append({
                'type': 'cpu',
                'value': metrics['cpu_percent'],
                'threshold': self.bottleneck_thresholds['cpu_percent']
            })
            
        # 메모리 병목
        if metrics.get('memory_percent', 0) > self.bottleneck_thresholds['memory_percent']:
            bottlenecks.append({
                'type': 'memory',
                'value': metrics['memory_percent'],
                'threshold': self.bottleneck_thresholds['memory_percent']
            })
            
        # 네트워크 병목
        network = metrics.get('network', {})
        if network.get('network_out', 0) > self.bottleneck_thresholds['network_usage_mbps']:
            bottlenecks.append({
                'type': 'network_out',
                'value': network['network_out'],
                'threshold': self.bottleneck_thresholds['network_usage_mbps']
            })
            
        return bottlenecks

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
            
            # 메트릭 저장
            self.metrics.add_system_metric(system_metrics)
            
            # 웹소켓 상태
            websocket_stats = {}
            try:
                aggregated_metrics = self.websocket_manager.get_aggregated_metrics()
                
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
                pass
            
            # 업타임 계산
            uptime_seconds = int(time.time() - self.start_time)
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
                "websocket_stats": websocket_stats
            }
            
            await websocket.send_json(data)
            
        except WebSocketDisconnect:
            raise
        except Exception as e:
            raise

    def run_in_thread(self):
        try:
            if not self.running:
                self.running = True
                config = uvicorn.Config(
                    app=self.app,
                    host=self.host,
                    port=self.port,
                    log_level="info",
                    loop="asyncio"
                )
                server = uvicorn.Server(config)
                import threading
                thread = threading.Thread(target=server.run, daemon=True)
                thread.start()
                return thread
        except Exception as e:
            raise