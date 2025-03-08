import psutil
import time
import logging
import json
import os
from datetime import datetime
from collections import defaultdict
from typing import Dict, List
import asyncio

# 현재 스크립트의 절대 경로
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# 프로젝트 루트 디렉토리
ROOT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
# 로그 디렉토리
LOG_DIR = os.path.join(ROOT_DIR, "logs")

# 로그 디렉토리 생성
os.makedirs(LOG_DIR, exist_ok=True)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "monitor.log")),
        logging.StreamHandler()
    ]
)

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

class MetricsCollector:
    def __init__(self, check_interval=60):
        self.check_interval = check_interval
        self.metrics = {
            "binance": ExchangeMetrics(),
            "binancefuture": ExchangeMetrics(),
            "bybit": ExchangeMetrics(),
            "bybitfuture": ExchangeMetrics(),
            "upbit": ExchangeMetrics(),
            "bithumb": ExchangeMetrics()
        }
        
        # 메트릭 저장 경로
        self.metrics_dir = os.path.join(LOG_DIR, "metrics")
        os.makedirs(self.metrics_dir, exist_ok=True)
        
        self.start_time = time.time()
        self.last_save_time = 0
        self.save_interval = 300  # 5분마다 저장
        
    def update_connection_status(self, exchange: str, status: bool):
        """연결 상태 업데이트"""
        metrics = self.metrics.get(exchange)
        if metrics:
            if metrics.connection_status != status:
                if status:
                    logging.info(f"[{exchange}] 연결됨")
                else:
                    metrics.reconnect_count += 1
                    logging.warning(f"[{exchange}] 연결 끊김 (재연결 {metrics.reconnect_count}회)")
            metrics.connection_status = status
            metrics.last_message_time = time.time()

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

    def save_metrics(self):
        """메트릭 저장"""
        current_time = time.time()
        if current_time - self.last_save_time < self.save_interval:
            return

        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            metrics_file = os.path.join(self.metrics_dir, f"metrics_{timestamp}.json")
            
            data = {
                "timestamp": datetime.now().isoformat(),
                "uptime": current_time - self.start_time,
                "system": self.get_system_metrics(),
                "exchanges": {}
            }
            
            for exchange, metrics in self.metrics.items():
                current_minute = datetime.now().strftime("%Y-%m-%d %H:%M")
                data["exchanges"][exchange] = {
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
            
            with open(metrics_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            self.last_save_time = current_time
            logging.info(f"메트릭 저장 완료: {metrics_file}")
            
        except Exception as e:
            logging.error(f"메트릭 저장 중 오류 발생: {str(e)}")

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

    def print_report(self):
        """현재 메트릭 출력"""
        current_time = datetime.now()
        uptime = time.time() - self.start_time
        
        print("\n" + "="*50)
        print(f"모니터링 리포트 ({current_time.strftime('%Y-%m-%d %H:%M:%S')})")
        print(f"구동 시간: {int(uptime//3600)}시간 {int((uptime%3600)//60)}분 {int(uptime%60)}초")
        print("-"*50)
        
        # 시스템 메트릭
        system = self.get_system_metrics()
        print("\n[시스템 리소스]")
        print(f"CPU: {system['cpu_percent']}%")
        print(f"메모리: {system['memory_percent']}% ({system['memory_used']/1024/1024/1024:.1f}GB / {system['memory_total']/1024/1024/1024:.1f}GB)")
        print(f"디스크: {system['disk_percent']}% ({system['disk_used']/1024/1024/1024:.1f}GB / {system['disk_total']/1024/1024/1024:.1f}GB)")
        print(f"네트워크: 송신={system['network_bytes_sent']/1024/1024:.1f}MB, 수신={system['network_bytes_recv']/1024/1024:.1f}MB")
        
        # 거래소별 메트릭
        print("\n[거래소별 통계]")
        current_minute = current_time.strftime("%Y-%m-%d %H:%M")
        for exchange, metrics in self.metrics.items():
            print(f"\n{exchange}:")
            print(f"- 연결 상태: {'연결됨' if metrics.connection_status else '끊김'}")
            print(f"- 총 메시지 수: {metrics.message_count:,}개")
            print(f"- 현재 분 메시지 수: {metrics.message_rates[current_minute]:,}개/분")
            print(f"- 총 오더북 수: {metrics.orderbook_count:,}개")
            print(f"- 현재 분 오더북 수: {metrics.orderbook_rates[current_minute]:,}개/분")
            print(f"- 평균 처리 시간: {sum(metrics.processing_times) / len(metrics.processing_times):.2f}ms" if metrics.processing_times else "N/A")
            print(f"- 레이턴시: {metrics.latency_ms:.2f}ms")
            print(f"- 에러 수: {metrics.error_count}개")
            print(f"- 재연결 수: {metrics.reconnect_count}회")
            if metrics.errors:
                print("- 최근 에러:")
                for error in metrics.errors[-3:]:
                    print(f"  * {error['timestamp']}: {error['error']}")
        
        print("="*50 + "\n")

    def check_alerts(self):
        """경고 상태 체크"""
        current_time = time.time()
        system = self.get_system_metrics()
        
        # 시스템 리소스 체크
        if system["cpu_percent"] > 90:
            logging.warning(f"CPU 사용량 높음: {system['cpu_percent']}%")
        if system["memory_percent"] > 90:
            logging.warning(f"메모리 사용량 높음: {system['memory_percent']}%")
        if system["disk_percent"] > 90:
            logging.warning(f"디스크 사용량 높음: {system['disk_percent']}%")
            
        # 거래소별 체크
        for exchange, metrics in self.metrics.items():
            if not metrics.connection_status:
                logging.warning(f"{exchange} 웹소켓 연결 끊김")
            elif current_time - metrics.last_message_time > 60:
                logging.warning(f"{exchange} 1분 이상 메시지 수신 없음")
            if metrics.latency_ms > 1000:  # 1초 이상
                logging.warning(f"{exchange} 높은 레이턴시: {metrics.latency_ms:.2f}ms")

async def main():
    collector = MetricsCollector(check_interval=60)
    
    while True:
        try:
            collector.check_alerts()
            collector.save_metrics()
            collector.print_report()
            await asyncio.sleep(collector.check_interval)
        except Exception as e:
            logging.error(f"모니터링 중 오류 발생: {str(e)}")
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main()) 