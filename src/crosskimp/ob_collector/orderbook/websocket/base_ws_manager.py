"""
웹소켓 관리자 모듈

이 모듈은 여러 거래소의 웹소켓 연결을 중앙에서 관리하는 기능을 제공합니다.
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
from crosskimp.ob_collector.utils.config.constants import EXCHANGE_NAMES_KR, LOG_SYSTEM, STATUS_EMOJIS
from crosskimp.ob_collector.core.metrics_manager import WebsocketMetricsManager
from crosskimp.ob_collector.orderbook.websocket.binance_f_ws import BinanceFutureWebsocket
from crosskimp.ob_collector.orderbook.websocket.binance_s_ws import BinanceSpotWebsocket
from crosskimp.ob_collector.orderbook.websocket.bithumb_s_ws import BithumbSpotWebsocket
from crosskimp.ob_collector.orderbook.websocket.bybit_f_ws import BybitFutureWebsocket
from crosskimp.ob_collector.orderbook.websocket.bybit_s_ws import BybitSpotWebsocket
from crosskimp.ob_collector.orderbook.websocket.upbit_s_ws import UpbitWebsocket

from crosskimp.telegrambot.telegram_notification import send_telegram_message

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

        # 메트릭 매니저로 통합
        self.metrics_manager = WebsocketMetricsManager()
        
        # 메트릭 매니저의 설정값 사용
        self.delay_threshold_ms = self.metrics_manager.delay_threshold_ms
        self.ping_interval = self.metrics_manager.ping_interval
        self.pong_timeout = self.metrics_manager.pong_timeout
        self.health_threshold = self.metrics_manager.health_threshold

        # 메트릭 저장 경로 설정 (절대 경로 사용)
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))
        self.metrics_dir = os.path.join(self.base_dir, "logs", "metrics")
        
        # 시장가격 모니터 초기화
        self.current_usdt_rate = 0.0  # USDT 환율 캐시

        # 거래소 초기화
        for exchange in EXCHANGE_CLASS_MAP.keys():
            self.metrics_manager.initialize_exchange(exchange)

    async def initialize_metrics_dir(self):
        """메트릭 디렉토리 초기화 (비동기)"""
        try:
            # 디렉토리가 이미 존재하는지 확인 (비동기)
            exists = await asyncio.to_thread(os.path.exists, self.metrics_dir)
            if exists:
                logger.debug(f"메트릭 디렉토리 이미 존재: {self.metrics_dir}")
                return
                
            # 디렉토리 생성 (비동기)
            await asyncio.to_thread(os.makedirs, self.metrics_dir, exist_ok=True)
            logger.info(f"메트릭 디렉토리 생성 완료: {self.metrics_dir}")
        except Exception as e:
            logger.error(f"메트릭 디렉토리 생성 실패: {str(e)}", exc_info=True)

    def update_connection_status(self, exchange: str, status: str):
        """연결 상태 업데이트"""
        try:
            if status == "connect":
                self.metrics_manager.update_metric(
                    exchange=exchange,
                    event_type="connect"
                )
                logger.info(f"{EXCHANGE_NAMES_KR.get(exchange, exchange)} {STATUS_EMOJIS['CONNECTED']} 웹소켓 연결됨")
                # 연결 상태 변경 시에만 전체 상태 표시
                self._display_all_connection_status()
            
            elif status == "disconnect":
                self.metrics_manager.update_metric(
                    exchange=exchange,
                    event_type="disconnect"
                )
                logger.info(f"{EXCHANGE_NAMES_KR.get(exchange, exchange)} {STATUS_EMOJIS['DISCONNECTED']} 웹소켓 연결 해제됨")
                # 연결 상태 변경 시에만 전체 상태 표시
                self._display_all_connection_status()
            
            elif status == "message":
                self.metrics_manager.update_metric(
                    exchange=exchange,
                    event_type="message"
                )
                
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 연결 상태 업데이트 오류: {str(e)}")

    def _display_all_connection_status(self):
        """전체 거래소 연결 상태 표시"""
        try:
            metrics = self.metrics_manager.get_metrics()
            status_lines = []
            
            # 모든 거래소에 대해 상태 표시 (EXCHANGE_CLASS_MAP의 순서 유지)
            for exchange in EXCHANGE_CLASS_MAP.keys():
                metric = metrics.get(exchange, {})
                status_emoji = "🟢" if metric.get('connected', False) else "⚪"
                msg_rate = metric.get('messages_per_second', 0.0)
                status_lines.append(
                    f"{EXCHANGE_NAMES_KR.get(exchange, exchange)}: {status_emoji} "
                    f"({msg_rate:.1f}/s)"
                )
            
            logger.info(f"{LOG_SYSTEM} 거래소 연결 | {' | '.join(status_lines)}")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 전체 상태 표시 오류: {str(e)}", exc_info=True)

    def record_message(self, exchange: str, size: int = 0):
        """메시지 수신 기록 - 메트릭 매니저 위임"""
        self.metrics_manager.update_metric(
            exchange=exchange,
            event_type="message"
        )

    def record_error(self, exchange: str, error: str):
        """에러 기록 - 메트릭 매니저 위임"""
        self.metrics_manager.update_metric(
            exchange=exchange,
            event_type="error"
        )

    def register_callback(self, callback: Callable[[str, dict], None]):
        """콜백 함수 등록"""
        self.callback = callback

    def update_usdt_rate(self, rate: float):
        """USDT 환율 업데이트"""
        self.current_usdt_rate = rate

    async def monitor_metrics(self):
        """메트릭 모니터링 태스크"""
        logger.info(f"{LOG_SYSTEM} 메트릭 모니터링 시작")
        last_periodic_log = time.time()  # 마지막 주기적 로그 시간을 현재 시간으로 초기화
        
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                
                # 메트릭 정리
                self.metrics_manager.cleanup_old_metrics()
                
                # 1분마다 주기적으로 상태 표시
                if current_time - last_periodic_log >= 60:
                    self._display_all_connection_status()
                    last_periodic_log = current_time
                
                await asyncio.sleep(1)  # 1초마다 체크
                
            except Exception as e:
                logger.error(f"{LOG_SYSTEM} 메트릭 모니터링 오류: {str(e)}", exc_info=True)
                await asyncio.sleep(5)  # 오류 발생시 5초 대기

    async def process_queue(self):
        """메시지 큐 처리"""
        # 큐 로거 초기화 (모듈 레벨 변수가 아닌 로컬 변수로 사용)
        queue_logger = get_queue_logger()
        
        # 큐 처리 시작 로깅
        logger.info(f"{LOG_SYSTEM} 큐 처리 시작 (큐 ID: {id(self.output_queue)})")
        
        # 큐 처리 카운터 초기화
        processed_count = 0
        
        while not self.stop_event.is_set():
            try:
                start_time = time.time()
                
                # 큐에서 데이터 가져오기 (5초마다 큐 상태 로깅)
                if processed_count % 100 == 0:
                    logger.debug(f"{LOG_SYSTEM} 큐 상태: {self.output_queue.qsize()} 항목 대기 중")
                
                queue_item = await self.output_queue.get()
                processed_count += 1
                
                # 큐 아이템 형식 검증
                if not isinstance(queue_item, tuple) or len(queue_item) != 2:
                    logger.error(f"{LOG_SYSTEM} 큐 데이터 형식 오류: {queue_item}")
                    self.output_queue.task_done()
                    continue
                    
                exchange, data = queue_item
                
                # 메시지 처리 시간 측정
                processing_time = (time.time() - start_time) * 1000  # ms 단위
                
                # 모든 메시지 기록
                self.record_message(exchange, len(str(data)))
                
                # 메시지 타입 판별 및 큐 데이터 로깅
                msg_type = self._determine_message_type(data)
                
                # 큐 로깅 (1000개마다 로깅 상태 출력)
                queue_logger.info(f"{exchange} {data}")
                if processed_count % 1000 == 0:
                    logger.info(f"{LOG_SYSTEM} 큐 처리 진행 상황: {processed_count}개 처리됨")
                
                if self.callback:
                    await self.callback(exchange, data)

                self.output_queue.task_done()
                
            except ValueError as e:
                # "too many values to unpack" 오류 처리
                logger.error(f"{LOG_SYSTEM} 큐 데이터 언패킹 오류: {e}")
                try:
                    # 큐에서 가져온 항목을 로깅하여 디버깅
                    item = await self.output_queue.get()
                    logger.error(f"{LOG_SYSTEM} 문제가 있는 큐 항목: {item}")
                    self.output_queue.task_done()
                except Exception:
                    pass
                
            except Exception as e:
                self.record_error("unknown", str(e))
                logger.error(f"{LOG_SYSTEM} 큐 처리 실패: {e}", exc_info=True)
                await asyncio.sleep(0.1)  # 오류 발생 시 짧은 대기 후 재시도

    def _determine_message_type(self, data: dict) -> str:
        """메시지 타입 판별"""
        if isinstance(data, dict):
            if 'stream' in data and 'depth' in data['stream'] and 'data' in data:
                actual_data = data['data']
                if actual_data.get('e') == 'depthUpdate':
                    return "orderbook_delta"
            elif 'e' in data and data['e'] == 'depthUpdate':
                return "orderbook_delta"
            elif 'type' in data and data['type'] == 'orderbookdepth':
                return "orderbook_delta"
            elif 'result' in data and isinstance(data['result'], dict):
                result = data['result']
                if 'b' in result and 'a' in result:
                    return "orderbook_delta"
            elif 'topic' in data and 'orderbook' in data['topic']:
                if data.get('type') == 'delta':
                    return "orderbook_delta"
                elif data.get('type') == 'snapshot':
                    return "orderbook_snapshot"
            elif 'type' in data and data['type'] == 'orderbook':
                return "orderbook_delta"
            elif 'bids' in data and 'asks' in data:
                return "orderbook_delta"
        return "other"

    async def add_connection(self, websocket):
        """웹소켓 연결 추가"""
        async with self._connection_lock:
            self._active_connections.add(websocket)
            logger.info(f"새 연결 추가됨. 총 {len(self._active_connections)}개")
    
    async def remove_connection(self, websocket):
        """웹소켓 연결 제거"""
        async with self._connection_lock:
            self._active_connections.discard(websocket)
            logger.info(f"연결 제거됨. 남은 연결 {len(self._active_connections)}개")
    
    async def is_connected(self, websocket) -> bool:
        """웹소켓 연결 상태 확인"""
        async with self._connection_lock:
            return websocket in self._active_connections

    async def check_message_delays(self):
        """메시지 지연 체크"""
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                for exchange, metrics in self.metrics_manager.get_metrics().items():
                    if not metrics['last_message_time']:
                        continue
                        
                    delay = current_time - metrics['last_message_time']  # 초 단위로 계산
                    if metrics['connected'] and delay > (self.delay_threshold_ms / 1000):  # ms를 초로 변환하여 비교
                        # 연결이 끊어진 것으로 판단하고 상태 업데이트
                        self.metrics_manager.update_metric(exchange, "disconnect")
                        
                    logger.info(
                        f"{EXCHANGE_NAMES_KR.get(exchange, exchange)} 연결 상태 체크 | "
                        f"연결={'예' if metrics['connected'] else '아니오'}, "
                        f"지연={delay:.1f}초, "
                        f"총 메시지={format(metrics['message_count'], ',')}개, "
                        f"재연결={metrics['reconnect_count']}회"
                    )
                    
                await asyncio.sleep(60)  # 1분마다 체크
                
            except Exception as e:
                logger.error(f"연결 상태 체크 중 오류: {e}", exc_info=True)
                await asyncio.sleep(5)  # 오류 발생시 5초 대기

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
            for exchange, metrics in self.metrics_manager.get_metrics().items():
                current_minute = datetime.now().strftime("%Y-%m-%d %H:%M")
                new_metric["exchanges"][exchange] = {
                    "connection_status": metrics['connected'],
                    "message_count": metrics['message_count'],
                    "message_rate": metrics['messages_per_second'],
                    "orderbook_count": metrics['orderbook_count'],
                    "orderbook_rate": metrics['orderbook_per_second'],
                    "error_count": metrics['error_count'],
                    "reconnect_count": metrics['reconnect_count'],
                    "latency_ms": metrics['latency_ms'],
                    "avg_processing_time": metrics['avg_processing_time'],
                    "bytes_received": metrics['bytes_received'],
                    "bytes_sent": metrics['bytes_sent']
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

    async def start_exchange_websocket(self, exchange_name: str, symbols: List[str]):
        try:
            exchange_name_lower = exchange_name.lower()
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_name_lower, exchange_name_lower)
            ws_class = EXCHANGE_CLASS_MAP.get(exchange_name_lower)
            
            if not ws_class:
                logger.error(f"{exchange_kr} {STATUS_EMOJIS['ERROR']} 지원하지 않는 거래소")
                return

            if not symbols:
                logger.warning(f"{exchange_kr} {STATUS_EMOJIS['ERROR']} 구독할 심볼이 없음")
                return

            logger.info(f"{exchange_kr} 웹소켓 초기화 시작 | symbols={len(symbols)}개: {symbols}")

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
            logger.error(f"[{exchange_name}] 시작 실패: {str(e)}", exc_info=True)

    async def start_all_websockets(self, filtered_data: Dict[str, List[str]]):
        try:
            # 메트릭 디렉토리 초기화
            await self.initialize_metrics_dir()
            
            self.tasks['queue'] = asyncio.create_task(self.process_queue())
            logger.info(f"{LOG_SYSTEM} 큐 처리 태스크 생성 완료")
            
            # 메트릭 모니터링 태스크 시작
            self.tasks['metrics'] = asyncio.create_task(self.monitor_metrics())
            logger.info(f"{LOG_SYSTEM} 메트릭 모니터링 태스크 생성 완료")
            
            for exchange, syms in filtered_data.items():
                await self.start_exchange_websocket(exchange, syms)
                logger.info(f"{EXCHANGE_NAMES_KR.get(exchange, exchange)} {STATUS_EMOJIS['CONNECTING']} 웹소켓 초기화 완료")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 웹소켓 시작 실패: {e}", exc_info=True)

    async def shutdown(self):
        self.stop_event.set()
        
        # 모든 웹소켓 종료
        shutdown_tasks = []
        for exchange, ws in self.websockets.items():
            try:
                if hasattr(ws, 'shutdown'):
                    shutdown_tasks.append(ws.shutdown())
                else:
                    # shutdown 메서드가 없는 경우 stop 메서드 시도
                    if hasattr(ws, 'stop'):
                        shutdown_tasks.append(ws.stop())
                    else:
                        logger.warning(f"{LOG_SYSTEM} {exchange} 웹소켓에 shutdown/stop 메서드가 없습니다")
            except Exception as e:
                logger.error(f"{LOG_SYSTEM} {exchange} 웹소켓 종료 실패: {e}", exc_info=True)
        
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        
        # 모든 태스크 취소
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
            logger.warning(f"{LOG_SYSTEM} {STATUS_EMOJIS['ERROR']} CPU 사용량 높음: {system['cpu_percent']}%")
        if system["memory_percent"] > 90:
            logger.warning(f"{LOG_SYSTEM} {STATUS_EMOJIS['ERROR']} 메모리 사용량 높음: {system['memory_percent']}%")
        if system["disk_percent"] > 90:
            logger.warning(f"{LOG_SYSTEM} {STATUS_EMOJIS['ERROR']} 디스크 사용량 높음: {system['disk_percent']}%")
        
        # 거래소별 체크
        for exchange, metrics in self.metrics_manager.get_metrics().items():
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            # 초기 연결 시도 전인지 확인
            is_initial_state = metrics['last_message_time'] == 0 or metrics['last_message_time'] == current_time

            # 이미 연결 시도가 있었고 현재 연결이 끊긴 상태일 때만 경고
            if not is_initial_state and not metrics['connected']:
                self.update_connection_status(exchange, "disconnect")  # 연결 끊김 상태 업데이트
            elif not is_initial_state and current_time - metrics['last_message_time'] > 60:
                self.update_connection_status(exchange, "disconnect")  # 1분 이상 메시지 없으면 연결 끊김으로 처리
            if metrics['latency_ms'] > 1000:  # 1초 이상
                logger.warning(f"{LOG_SYSTEM} {exchange_kr} {STATUS_EMOJIS['ERROR']} 높은 레이턴시: {metrics['latency_ms']:.2f}ms")

    # ============================
    # 공통 로깅 메서드
    # ============================
    def log_error(self, exchange: str, msg: str, exc_info: bool = True):
        """에러 로깅 공통 메서드"""
        try:
            self.metrics_manager.update_metric(
                exchange=exchange,
                event_type="error"
            )
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            logger.error(
                f"{LOG_SYSTEM} {exchange_kr} {STATUS_EMOJIS['ERROR']} {msg}",
                exc_info=exc_info
            )
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 에러 로깅 중 오류 발생: {str(e)}")

    def log_connect_attempt(self, exchange: str):
        """연결 시도 로깅 공통 메서드"""
        try:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            logger.info(
                f"{LOG_SYSTEM} {exchange_kr} {STATUS_EMOJIS['CONNECTING']} "
                f"웹소켓 연결 시도"
            )
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 연결 시도 로깅 중 오류 발생: {str(e)}")

    def log_connect_success(self, exchange: str):
        """연결 성공 로깅 공통 메서드"""
        try:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            logger.info(
                f"{LOG_SYSTEM} {exchange_kr} {STATUS_EMOJIS['CONNECTED']} "
                f"웹소켓 연결 성공"
            )
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 연결 성공 로깅 중 오류 발생: {str(e)}")

    def log_disconnect(self, exchange: str):
        """연결 종료 로깅 공통 메서드"""
        try:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            logger.info(
                f"{LOG_SYSTEM} {exchange_kr} {STATUS_EMOJIS['DISCONNECTED']} "
                f"웹소켓 연결 종료"
            )
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 연결 종료 로깅 중 오류 발생: {str(e)}")

    def log_start(self, exchange: str, symbols: List[str]):
        """웹소켓 시작 로깅 공통 메서드"""
        try:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            logger.info(
                f"{LOG_SYSTEM} {exchange_kr} {STATUS_EMOJIS['CONNECTING']} "
                f"웹소켓 시작 | symbols={len(symbols)}개: {symbols}"
            )
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 시작 로깅 중 오류 발생: {str(e)}")

    def log_stop(self, exchange: str):
        """웹소켓 정지 로깅 공통 메서드"""
        try:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            logger.info(
                f"{LOG_SYSTEM} {exchange_kr} {STATUS_EMOJIS['DISCONNECTED']} "
                f"웹소켓 정지 요청"
            )
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 정지 로깅 중 오류 발생: {str(e)}")

    def log_message_performance(self, exchange: str, avg_time: float, max_time: float, sample_count: int):
        """메시지 처리 성능 로깅 공통 메서드"""
        try:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            logger.info(
                f"{LOG_SYSTEM} {exchange_kr} 메시지 처리 성능 | "
                f"평균={avg_time:.2f}ms, "
                f"최대={max_time:.2f}ms, "
                f"샘플수={sample_count:,}개"
            )
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 성능 로깅 중 오류 발생: {str(e)}")

    def log_subscribe(self, exchange: str, symbols: List[str]):
        """구독 로깅 공통 메서드"""
        try:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            logger.info(
                f"{LOG_SYSTEM} {exchange_kr} 구독 시작 | "
                f"symbols={len(symbols)}개: {symbols}"
            )
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 구독 로깅 중 오류 발생: {str(e)}")

    def log_subscribe_success(self, exchange: str, count: int):
        """구독 성공 로깅 공통 메서드"""
        try:
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            logger.info(
                f"{LOG_SYSTEM} {exchange_kr} 구독 완료 | "
                f"총 {count}개 심볼"
            )
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 구독 완료 로깅 중 오류 발생: {str(e)}")