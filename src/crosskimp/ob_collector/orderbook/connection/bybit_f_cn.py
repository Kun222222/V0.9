# file: orderbook/connection/bybit_f_cn.py

import asyncio
import json
import time
from websockets import connect
import websockets
from typing import Dict, List, Optional, Set, Any

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG
from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, WebSocketError
from crosskimp.ob_collector.orderbook.parser.bybit_f_pa import BybitFutureParser

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이빗 선물 웹소켓 연결 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BYBIT_FUTURE.value  # 거래소 코드
BYBIT_FUTURE_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이빗 선물 설정

# 웹소켓 연결 설정
WS_URL = BYBIT_FUTURE_CONFIG["ws_url"]  # 웹소켓 URL
PING_INTERVAL = BYBIT_FUTURE_CONFIG["ping_interval"]  # 핑 전송 간격 (초)
PING_TIMEOUT = BYBIT_FUTURE_CONFIG["ping_timeout"]  # 핑 응답 타임아웃 (초)

# 오더북 관련 설정
MAX_SYMBOLS_PER_BATCH = BYBIT_FUTURE_CONFIG["max_symbols_per_batch"]  # 배치당 최대 심볼 수

class BybitFutureWebSocketConnector(BaseWebsocketConnector):
    """
    바이빗 선물 웹소켓 연결 관리 클래스
    
    바이빗 선물 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    
    특징:
    - 커스텀 핑/퐁 메커니즘 사용
    - 재연결 전략 구현
    - 배치 구독 지원
    """
    def __init__(self, settings: dict):
        """
        바이빗 선물 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, EXCHANGE_CODE)
        
        # 웹소켓 URL 설정
        self.ws_url = WS_URL
        if settings.get("testnet", False):
            self.ws_url = "wss://stream-testnet.bybit.com/v5/public/linear"
        
        # 연결 관련 설정
        self.is_connected = False
        self.current_retry = 0
        self.retry_delay = 0.5
        
        # 핑/퐁 설정
        self.last_ping_time = 0
        self.last_pong_time = 0
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        
        # 구독 관련 설정
        self.max_symbols_per_subscription = MAX_SYMBOLS_PER_BATCH
        self.subscribed_symbols: List[str] = []
        
        # 백그라운드 태스크
        self.ping_task = None
        self.health_check_task = None
        
        # 메시지 처리 통계
        self.message_stats = {
            "total_received": 0,
            "ping_pong": 0
        }
        
        # 파서 초기화
        self.parser = BybitFutureParser()

    async def _do_connect(self):
        """
        실제 웹소켓 연결 수행 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        try:
            # 타임아웃 설정 - 재연결 시도 횟수에 따라 증가
            if self.current_retry < 3:
                timeout = min(30, (0.2 + self.current_retry * 0.3))  # 0.2초, 0.5초, 0.8초
            else:
                timeout = min(30, 5 * (1 + self.current_retry * 0.5))  # 5초, 7.5초, 10초...
            
            self.log_info(
                f"웹소켓 연결 시도 | 시도={self.current_retry + 1}회차, timeout={timeout}초"
            )
            
            # 연결 파라미터 설정
            self.ws = await asyncio.wait_for(
                connect(
                    self.ws_url,
                    ping_interval=None,  # 자체 핑 구현
                    ping_timeout=None,   # 자체 핑 구현
                    close_timeout=timeout,
                    max_size=2**24,      # 24MB 최대 메시지 크기
                    compression=None
                ),
                timeout=timeout
            )
            
            self.last_pong_time = time.time()
            self.stats.last_pong_time = self.last_pong_time
            
            return True
        except asyncio.TimeoutError:
            # 초기 연결 타임아웃은 에러로 처리하지 않고 로깅만 수행
            self.log_info(f"초기 연결 타임아웃 발생 (무시됨) | 시도={self.current_retry + 1}회차, timeout={timeout}초")
            # 상위 메서드에서 재시도 로직을 처리할 수 있도록 예외를 다시 발생시킴
            raise
        except Exception as e:
            self.log_error(f"웹소켓 연결 실패: {e}")
            raise

    async def _handle_timeout_error(self, exception: asyncio.TimeoutError) -> bool:
        """
        바이빗 전용 타임아웃 에러 처리 (BaseWebsocketConnector 메서드 오버라이드)
        
        바이빗의 경우 초기 연결 타임아웃은 정상적인 상황으로 처리합니다.
        
        Args:
            exception: 타임아웃 예외 객체
            
        Returns:
            bool: 재연결 시도 여부
        """
        # 초기 연결 시도 중 타임아웃은 정상적인 상황으로 처리
        is_initial_timeout = self.current_retry <= 3
        
        if is_initial_timeout:
            # 초기 연결 타임아웃은 정보 로그만 남김
            self.log_info(f"{self.exchange_korean_name} 초기 연결 타임아웃 발생 (정상적인 상황으로 처리)")
            
            # 짧은 대기 후 재연결 시도
            await asyncio.sleep(1)
            return await self.connect()  # 재귀적으로 다시 연결 시도
        else:
            # 초기 연결이 아닌 경우 기본 타임아웃 처리 사용
            return await super()._handle_timeout_error(exception)

    async def subscribe(self, symbols: List[str]) -> None:
        """
        지정된 심볼에 대한 오더북 구독
        
        Args:
            symbols: 구독할 심볼 목록
        
        Raises:
            ConnectionError: 웹소켓이 연결되지 않은 경우
            Exception: 구독 처리 중 오류 발생
        """
        if not symbols:
            self.log_warning("구독할 심볼이 없습니다")
            return
        
        if not self.ws or not self.is_connected:
            error_msg = "웹소켓이 연결되지 않았습니다"
            self.log_error(error_msg)
            raise ConnectionError(error_msg)
        
        self.log_info(f"구독 시작 | 총 {len(symbols)}개 심볼, {self.max_symbols_per_subscription}개 배치로 나눔")
        
        # 심볼을 청크로 나누어 구독
        chunks = [symbols[i:i+self.max_symbols_per_subscription] for i in range(0, len(symbols), self.max_symbols_per_subscription)]
        
        for i, chunk in enumerate(chunks):
            try:
                # 파서를 사용하여 구독 메시지 생성
                msg = self.parser.create_subscribe_message(chunk)
                
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
                error_msg = f"구독 요청 실패: {str(e)}"
                self.log_error(error_msg)
                raise

    async def _send_ping(self) -> None:
        """
        PING 메시지 전송
        
        바이빗 서버에 PING 메시지를 전송하여 연결 상태를 유지합니다.
        """
        try:
            if self.ws and self.is_connected:
                ping_message = {
                    "req_id": str(int(time.time() * 1000)),
                    "op": "ping"
                }
                await self.ws.send(json.dumps(ping_message))
                self.last_ping_time = time.time()
                self.stats.last_ping_time = self.last_ping_time
                self.log_debug(f"PING 전송: {ping_message}")
        except Exception as e:
            self.log_error(f"PING 전송 실패: {str(e)}")

    def _handle_pong(self, data: dict) -> bool:
        """
        PONG 메시지 처리
        
        Args:
            data: 수신된 PONG 메시지 데이터
            
        Returns:
            bool: PONG 메시지 처리 성공 여부
        """
        try:
            # 파서를 사용하여 PONG 메시지 확인
            if self.parser.is_pong_message(data):
                self.last_pong_time = time.time()
                self.stats.last_pong_time = self.last_pong_time
                self.message_stats["ping_pong"] += 1
                
                latency = (self.last_pong_time - self.last_ping_time) * 1000
                self.stats.latency_ms = latency
                
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "heartbeat")
                    
                self.log_debug(f"PONG 수신 (레이턴시: {latency:.2f}ms)")
                return True
            return False
        except Exception as e:
            self.log_error(f"PONG 처리 실패: {str(e)}")
            return False

    async def _ping_loop(self):
        """
        바이빗 공식 핑 태스크
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
                    error_msg = f"{self.exchange_korean_name} PONG 응답 타임아웃 ({self.ping_timeout}초) | 마지막 PONG: {pong_diff:.1f}초 전"
                    self.log_error(error_msg)
                    await self._send_telegram_notification("error", error_msg)
                    
                    if self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "heartbeat_timeout")
                        
                    self.is_connected = False
                    self.stats.connected = False
                    await self.reconnect()
                    break
                    
            except Exception as e:
                self.log_error(f"PING 태스크 오류: {str(e)}")
                await asyncio.sleep(1)

    async def _run_message_loop(self, symbols: List[str], tasks: List[asyncio.Task]) -> None:
        """
        메시지 처리 루프 실행 (BaseWebsocketConnector 템플릿 메서드 구현)
        
        Args:
            symbols: 구독한 심볼 목록
            tasks: 실행 중인 백그라운드 태스크 목록
        """
        try:
            while not self.stop_event.is_set() and self.is_connected:
                try:
                    message = await asyncio.wait_for(self.ws.recv(), timeout=30)
                    self.stats.last_message_time = time.time()
                    self.stats.message_count += 1
                    self.message_stats["total_received"] += 1
                    
                    # 메시지 수신 로깅 (1000개마다)
                    if self.message_stats["total_received"] % 1000 == 0:
                        self.log_info(f"메시지 수신 중 | 총 {self.message_stats['total_received']:,}개")
                    
                    # 메시지 처리 (자식 클래스에서 구현)
                    await self.process_message(message)
                        
                except asyncio.TimeoutError:
                    self.log_debug(f"30초 동안 메시지 없음, 연결 상태 확인을 위해 PING 전송")
                    try:
                        await self._send_ping()
                    except Exception as e:
                        self.log_error(f"PING 전송 실패: {str(e)}")
                        await self.reconnect()
                        break
                        
                except websockets.exceptions.ConnectionClosed as e:
                    error_msg = f"{self.exchange_korean_name} 웹소켓 연결 끊김: {str(e)}"
                    self.log_error(error_msg)
                    await self._send_telegram_notification("error", error_msg)
                    self.is_connected = False
                    if not self.stop_event.is_set():
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
                self.log_info(f"메시지 루프 종료 후 재시작 시도")
                asyncio.create_task(self.start({self.exchangename.lower(): symbols}))

    async def process_message(self, message: str) -> None:
        """
        수신된 메시지 처리
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        try:
            # 메시지 파싱
            data = json.loads(message)
            
            # 핑/퐁 메시지 처리
            if data.get("op") == "pong" or (data.get("ret_msg") == "pong" and data.get("op") == "ping"):
                self._handle_pong(data)
                return
                
            # 구독 응답 처리
            if data.get("op") == "subscribe":
                self.log_debug(f"구독 응답 수신: {data}")
                return
                
            # 오더북 메시지 처리
            if "topic" in data and "data" in data:
                topic = data.get("topic", "")
                if "orderbook" in topic:
                    parts = topic.split(".")
                    if len(parts) >= 3:
                        symbol = parts[-1].replace("USDT", "")
                        
                        # 파서를 사용하여 메시지 파싱
                        parsed_data = self.parser.parse_message(message)
                        if not parsed_data:
                            self.log_debug(f"{symbol} 파싱 실패 또는 무시된 메시지")
                            return
                            
                        # 오더북 업데이트
                        if self.orderbook_manager:
                            result = await self.orderbook_manager.update(symbol, parsed_data)
                            if not result.is_valid:
                                self.log_error(f"{symbol} 오더북 업데이트 실패: {result.error_messages}")
                        else:
                            self.log_warning(f"{symbol} 오더북 매니저가 설정되지 않았습니다")
                        
                        # 로깅 (1000개마다)
                        if self.message_stats["total_received"] % 1000 == 0:
                            self.log_info(
                                f"메시지 처리 통계 | 총 수신={self.message_stats['total_received']:,}개, "
                                f"핑퐁={self.message_stats['ping_pong']:,}개"
                            )
        except json.JSONDecodeError:
            self.log_error(f"JSON 파싱 실패: {message[:100]}...")
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {str(e)}", exc_info=True)

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
                self.log_warning(f"{name} 취소 중 오류 (무시됨): {str(e)}")

    async def reconnect(self) -> None:
        """
        웹소켓 연결 재설정
        
        바이빗의 경우 초기 연결 시도 중 타임아웃은 정상적인 상황으로 처리합니다.
        """
        # 재연결 시도 횟수 증가
        self.current_retry += 1
        self.stats.reconnect_count += 1
        
        # 초기 연결 시도 중 타임아웃은 정상적인 상황으로 처리
        is_initial_timeout = self.current_retry <= 3
        
        # 로그 및 알림 처리 - 초기 연결 여부에 따라 다르게 처리
        if is_initial_timeout:
            # 초기 연결 타임아웃은 정보 로그만 남김
            self.log_info(f"{self.exchange_korean_name} 초기 연결 재시도 중 (시도 횟수: {self.current_retry})")
        else:
            # 초기 연결 타임아웃이 아닌 경우에만 텔레그램 알림 전송
            reconnect_msg = f"{self.exchange_korean_name} 웹소켓 재연결 시도 중 (시도 횟수: {self.stats.reconnect_count})"
            self.log_info(reconnect_msg)
            await self._send_telegram_notification("reconnect", reconnect_msg)
        
        try:
            # 웹소켓 및 태스크 정리
            if self.ws:
                try:
                    await self.ws.close()
                except Exception as e:
                    self.log_warning(f"웹소켓 종료 중 오류 (무시됨): {str(e)}")
            
            await self._cancel_task(self.ping_task, "PING 태스크")
            await self._cancel_task(self.health_check_task, "헬스 체크 태스크")
            
            self.is_connected = False
            self.stats.connected = False
            
            # 초기 연결 시도 중 타임아웃인 경우 직접 connect 호출, 그 외에는 부모 클래스의 reconnect 호출
            if is_initial_timeout:
                await asyncio.sleep(1)  # 짧은 대기 후 재연결 시도
                await self.connect()
            else:
                await super().reconnect()
            
        except Exception as e:
            # 오류 메시지 및 로깅 - 초기 연결 여부에 따라 다르게 처리
            if is_initial_timeout:
                # 초기 연결 타임아웃은 정보 로그만 남김
                self.log_info(f"{self.exchange_korean_name} 초기 연결 재시도 실패 (무시됨): {str(e)}")
            else:
                # 초기 연결 타임아웃이 아닌 경우에만 에러 로그 및 텔레그램 알림
                error_msg = f"{self.exchange_korean_name} 웹소켓 재연결 실패: {str(e)}"
                self.log_error(error_msg)
                await self._send_telegram_notification("error", error_msg)
            
            delay = min(30, self.retry_delay * (2 ** (self.current_retry - 1)))
            self.log_info(f"재연결 대기 중 ({delay}초) | 시도={self.current_retry}회차")
            await asyncio.sleep(delay)

    async def _cleanup_connection(self) -> None:
        """연결 종료 및 정리"""
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                self.log_error(f"웹소켓 종료 실패: {e}")

        self.is_connected = False
        
        # 연결 종료 상태 콜백
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "disconnect") 