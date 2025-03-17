# file: orderbook/connection/bybit_s_cn.py

import asyncio
import json
import time
from websockets import connect
import websockets
from typing import Dict, List, Optional, Set, Any

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG
from crosskimp.ob_collector.orderbook.connection.base_ws_connector import BaseWebsocketConnector, WebSocketError
from crosskimp.ob_collector.orderbook.parser.bybit_s_pa import BybitParser

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이빗 현물 웹소켓 연결 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BYBIT.value  # 거래소 코드
BYBIT_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이빗 설정

# 웹소켓 연결 설정
WS_URL = BYBIT_CONFIG["ws_url"]  # 웹소켓 URL
PING_INTERVAL = BYBIT_CONFIG["ping_interval"]  # 핑 전송 간격 (초)
PING_TIMEOUT = BYBIT_CONFIG["ping_timeout"]  # 핑 응답 타임아웃 (초)

class BybitWebSocketConnector(BaseWebsocketConnector):
    """
    바이빗 웹소켓 연결 관리 클래스
    
    바이빗 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    
    특징:
    - 커스텀 핑/퐁 메커니즘 사용
    - 재연결 전략 구현
    - 연결 상태 관리
    """
    def __init__(self, settings: dict):
        """
        바이빗 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, EXCHANGE_CODE)
        self.ws_url = WS_URL
        
        # 연결 관련 설정
        self.is_connected = False
        self.current_retry = 0
        self.retry_delay = 0.5
        
        # 핑/퐁 설정
        self.last_ping_time = 0
        self.last_pong_time = 0
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        
        # 백그라운드 태스크
        self.ping_task = None
        self.health_check_task = None
        
        # 메시지 처리 통계
        self.message_stats = {
            "total_received": 0,
            "ping_pong": 0
        }
        
        # 파서 초기화
        self.parser = BybitParser()

    async def _do_connect(self):
        """
        실제 웹소켓 연결 수행 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        # 초기 3번은 매우 공격적으로 시도
        if self.current_retry < 3:
            timeout = min(30, (0.2 + self.current_retry * 0.3))  # 0.2초, 0.5초, 0.8초
        else:
            # 3번 이후부터는 좀 더 보수적으로
            timeout = min(30, 2 * (1 + self.current_retry * 0.5))  # 2초, 3.6초, 4.2초...
        
        self.log_info(
            f"웹소켓 연결 시도 | 시도={self.current_retry + 1}회차, timeout={timeout}초"
        )
        
        try:
            self.ws = await asyncio.wait_for(
                connect(
                    self.ws_url,
                    ping_interval=None,
                    ping_timeout=None,
                    compression=None
                ),
                timeout=timeout
            )
            
            self.last_pong_time = time.time()
            self.stats.last_pong_time = self.last_pong_time
            
        except asyncio.TimeoutError:
            # 초기 연결 타임아웃은 에러로 처리하지 않고 로깅만 수행
            self.log_info(f"초기 연결 타임아웃 발생 (무시됨) | 시도={self.current_retry + 1}회차, timeout={timeout}초")
            # 상위 메서드에서 재시도 로직을 처리할 수 있도록 예외를 다시 발생시킴
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
                self.log_debug(f"PING 메시지 전송")
                self.message_stats["ping_pong"] += 1
        except Exception as e:
            self.log_error(f"PING 메시지 전송 실패: {str(e)}")

    def _handle_pong(self, data: dict) -> bool:
        """
        PONG 메시지 처리
        
        Args:
            data: PONG 메시지 데이터
            
        Returns:
            bool: PONG 메시지 처리 성공 여부
        """
        try:
            self.last_pong_time = time.time()
            self.stats.last_pong_time = self.last_pong_time
            self.log_debug(f"PONG 응답 수신")
            return True
        except Exception as e:
            self.log_error(f"PONG 메시지 처리 실패: {str(e)}")
            return False

    async def _ping_loop(self):
        """
        PING 루프 실행
        
        주기적으로 PING 메시지를 전송하여 연결 상태를 유지합니다.
        """
        try:
            self.log_info(f"PING 루프 시작 | 간격: {self.ping_interval}초")
            
            while self.is_connected and not self.stop_event.is_set():
                try:
                    # PING 메시지 전송
                    await self._send_ping()
                    
                    # 다음 PING까지 대기
                    await asyncio.sleep(self.ping_interval)
                    
                except asyncio.CancelledError:
                    self.log_info(f"PING 루프 취소됨")
                    break
                    
                except Exception as e:
                    self.log_error(f"PING 루프 오류: {str(e)}")
                    await asyncio.sleep(1)  # 오류 발생 시 짧게 대기 후 재시도
            
            self.log_info(f"PING 루프 종료")
            
        except asyncio.CancelledError:
            self.log_info(f"PING 루프 취소됨")
            
        except Exception as e:
            self.log_error(f"PING 루프 실행 중 오류 발생: {str(e)}")

    async def _run_message_loop(self, symbols: List[str], tasks: List[asyncio.Task]) -> None:
        """
        메시지 수신 루프 실행
        
        웹소켓으로부터 메시지를 수신하고 처리합니다.
        
        Args:
            symbols: 구독할 심볼 목록
            tasks: 백그라운드 태스크 목록
        """
        try:
            self.log_info(f"메시지 수신 루프 시작")
            
            # 연결 상태 콜백 호출
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connected")
            
            # 메시지 수신 루프
            while self.is_connected and not self.stop_event.is_set():
                try:
                    # 메시지 수신
                    message = await asyncio.wait_for(
                        self.ws.recv(),
                        timeout=self.ping_interval + self.ping_timeout
                    )
                    
                    # 메시지 처리
                    await self.process_message(message)
                    
                except asyncio.TimeoutError:
                    # 타임아웃 발생 시 연결 상태 확인
                    current_time = time.time()
                    if self.last_pong_time > 0 and (current_time - self.last_pong_time) > self.ping_timeout:
                        self.log_warning(f"PONG 응답 타임아웃 | 마지막 PONG: {current_time - self.last_pong_time:.1f}초 전")
                        break
                    
                except websockets.exceptions.ConnectionClosed as e:
                    self.log_error(f"웹소켓 연결이 닫혔습니다: {e}")
                    break
                    
                except asyncio.CancelledError:
                    self.log_info(f"메시지 루프가 취소되었습니다.")
                    break
                    
                except Exception as e:
                    self.log_error(f"메시지 처리 중 오류 발생: {e}")
                    continue
                    
        except asyncio.CancelledError:
            self.log_info(f"메시지 루프가 취소되었습니다.")
            
        except Exception as e:
            self.log_error(f"메시지 루프 실행 중 오류 발생: {e}")
            
        finally:
            self.log_info(f"메시지 수신 루프 종료")
            
            # 연결 상태 콜백 호출
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "disconnected")
            
            # 모든 태스크 취소
            for task in tasks:
                if not task.done():
                    task.cancel()
            
            # 연결 종료
            await self._cleanup_connection()
            
            # 재연결 시도
            if not self.stop_event.is_set():
                self.log_info(f"재연결 시도")
                asyncio.create_task(self.reconnect())

    async def process_message(self, message: str) -> None:
        """
        수신된 메시지 처리
        
        이 메서드는 하위 클래스에서 구현해야 합니다.
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        pass

    async def start_background_tasks(self) -> List[asyncio.Task]:
        """
        백그라운드 태스크 시작
        
        Returns:
            List[asyncio.Task]: 시작된 태스크 목록
        """
        tasks = []
        
        # PING 루프 시작
        self.ping_task = asyncio.create_task(self._ping_loop())
        tasks.append(self.ping_task)
        
        return tasks

    async def _cancel_task(self, task: Optional[asyncio.Task], name: str) -> None:
        """
        태스크 취소
        
        Args:
            task: 취소할 태스크
            name: 태스크 이름
        """
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                self.log_debug(f"{name} 태스크 취소됨")

    async def reconnect(self) -> None:
        """
        재연결 시도
        
        연결이 끊어진 경우 재연결을 시도합니다.
        """
        try:
            # 이미 재연결 중인 경우 무시
            if self.is_reconnecting:
                return
                
            self.is_reconnecting = True
            
            # 재연결 시도 횟수 증가
            self.current_retry += 1
            
            # 최대 재시도 횟수 초과 시 중단
            if self.max_retries > 0 and self.current_retry > self.max_retries:
                self.log_error(
                    f"최대 재연결 시도 횟수 초과 | 시도={self.current_retry}회, 최대={self.max_retries}회"
                )
                self.stop_event.set()
                return
                
            # 지수 백오프 적용
            delay = min(30, self.retry_delay * (2 ** (self.current_retry - 1)))
            self.log_info(f"재연결 대기 | 시도={self.current_retry}회, 대기={delay:.1f}초")
            await asyncio.sleep(delay)
            
            # 연결 시도
            success = await self.connect()
            
            if success:
                self.log_info(f"재연결 성공 | 시도={self.current_retry}회")
                self.current_retry = 0
                
                # 백그라운드 태스크 시작
                tasks = await self.start_background_tasks()
                
                # 메시지 루프 실행
                await self._run_message_loop(self.subscribed_symbols, tasks)
            else:
                self.log_error(f"재연결 실패 | 시도={self.current_retry}회")
                # 다시 재연결 시도
                asyncio.create_task(self.reconnect())
                
        except Exception as e:
            self.log_error(f"재연결 중 오류 발생: {str(e)}")
            # 다시 재연결 시도
            asyncio.create_task(self.reconnect())
            
        finally:
            self.is_reconnecting = False

    async def _cleanup_connection(self) -> None:
        """
        연결 종료 및 정리
        """
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                self.log_error(f"웹소켓 종료 실패: {e}")

        self.is_connected = False
        
        # 연결 종료 상태 콜백
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "disconnect")

    async def receive_raw(self) -> Optional[str]:
        """
        웹소켓에서 원시 메시지 수신
        
        Returns:
            Optional[str]: 수신된 원시 메시지 또는 None
        """
        try:
            if not self.ws or not self.is_connected:
                self.log_debug("웹소켓 연결이 없거나 연결되지 않음")
                return None
                
            # 메시지 수신
            message = await self.ws.recv()
            
            # 메시지 로깅 (너무 길면 일부만)
            if message:
                # 메시지 통계 업데이트
                self.stats.total_messages += 1
                self.stats.message_count += 1
                self.stats.last_message_time = time.time()
                
                # 일반 메시지는 DEBUG 레벨로 로깅
                try:
                    # JSON 형식인 경우 파싱하여 타입 확인
                    data = json.loads(message)
                    if "topic" in data and "orderbook" in data.get("topic", ""):
                        # 오더북 메시지는 DEBUG 레벨로 로깅
                        self.log_debug(f"오더북 메시지 수신: 길이={len(message)}")
                    else:
                        # 기타 메시지는 INFO 레벨로 로깅
                        self.log_info(f"메시지 수신: {message[:200]}...")
                except:
                    # 파싱 실패 시 원시 메시지 일부만 로깅
                    if len(message) > 200:
                        self.log_info(f"메시지 수신 (길이: {len(message)}): {message[:100]}...{message[-100:]}")
                    else:
                        self.log_info(f"메시지 수신: {message}")
                
            return message
        except websockets.exceptions.ConnectionClosed as e:
            self.log_error(f"웹소켓 연결 끊김: {e}")
            self.is_connected = False
            return None
        except Exception as e:
            self.log_error(f"메시지 수신 실패: {e}")
            return None