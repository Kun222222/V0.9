"""
바이빗 현물 커넥터 모듈

바이빗 현물 거래소에 대한 웹소켓 연결 및 데이터 처리를 담당하는 커넥터를 제공합니다.
"""

import json
import time
import asyncio
import websockets
from typing import Dict, List, Any, Optional, Union, Callable, Set
import logging

from crosskimp.common.config.app_config import get_config
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR

from crosskimp.ob_collector.orderbook.connection.connector_interface import ExchangeConnectorInterface
from crosskimp.ob_collector.orderbook.data_handlers.exchanges.bybit_s_handler import BybitSpotDataHandler

class BybitSpotConnector(ExchangeConnectorInterface):
    """
    바이빗 현물 커넥터 클래스
    
    바이빗 현물 거래소의 웹소켓 연결 및 데이터 처리를 담당합니다.
    """
    
    # 전략에서 이동한 상수
    BASE_WS_URL = "wss://stream.bybit.com/v5/public/spot"
    
    def __init__(self, connection_manager=None):
        """
        초기화
        
        Args:
            connection_manager: 연결 관리자 객체
        """
        # 로거 설정
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        
        # 설정 로드
        self.config = get_config()
        
        # 거래소 코드 및 한글 이름 설정
        self.exchange_code = Exchange.BYBIT_SPOT.value
        self.exchange_name_kr = EXCHANGE_NAMES_KR[self.exchange_code]
        
        # 연결 관리자 저장
        self.connection_manager = connection_manager
        
        # 전략 클래스 제거하고 필요한 변수들 직접 선언
        self.id_counter = 1  # 요청 ID 카운터
        
        # 구독 정보 통합 (subscriptions 제거)
        self._subscribed_symbols = set()  # 구독 중인 심볼 집합
        
        # 데이터 핸들러 초기화
        self.data_handler = BybitSpotDataHandler()
        
        # 웹소켓 연결 객체
        self.ws = None
        
        # 핑 전송 타이머 (연결 유지를 위한 타이머)
        self.ping_task = None
        self._ping_interval = 20  # 핑 전송 간격 (초)
        
        # 연결 상태 플래그 추가
        self._is_connected = False
        self._stopping = False
        self.connecting = False  # 연결 중 상태 플래그 추가
        self.is_shutting_down = False  # 종료 중 상태 플래그 추가
        
        # 메시지 및 오더북 콜백
        self.message_callbacks = []
        self.orderbook_callbacks = []
        
        # 메시지 처리 태스크
        self._message_handler_task = None
        
        # 메시지 핸들러 락 추가 - 동시 실행 방지
        self._message_handler_lock = asyncio.Lock()
        
        # stop_event 추가 - 메시지 처리 루프 제어
        self.stop_event = asyncio.Event()
        
        # 스냅샷 캐시 추가
        self.snapshot_cache = {}  # 심볼 -> 스냅샷 데이터
        
        self.logger.debug(f"{self.exchange_name_kr} 커넥터 초기화 완료")
    
    @property
    def is_connected(self) -> bool:
        """
        현재 연결 상태 확인
        
        Returns:
            bool: 연결 상태 (True: 연결됨, False: 연결 안됨)
        """
        if self.ws is None:
            return False
            
        return self._is_connected
        
    @is_connected.setter
    def is_connected(self, value: bool):
        """
        연결 상태 설정
        
        Args:
            value: 연결 상태 (True: 연결됨, False: 연결 안됨)
        """
        old_value = self._is_connected
        self._is_connected = value
        
        # 연결 관리자에게 상태 변경 알림
        if self.connection_manager and old_value != value:
            self.connection_manager.update_exchange_status(self.exchange_code, value)
    
    async def connect(self) -> bool:
        """
        거래소 웹소켓 서버에 연결
        
        최대 60회(약 1분) 재시도를 포함한 연결 로직
        
        Returns:
            bool: 연결 성공 여부
        """
        # 이미 연결된 경우 바로 성공 반환
        if self.is_connected:
            return True
            
        # 이미 연결 중인 경우 대기
        if self.connecting:
            self.logger.debug(f"{self.exchange_name_kr} 이미 연결 중")
            return False
            
        # 연결 중 상태로 설정
        self.connecting = True
        self.stop_event.clear()
            
        # 연결 시도 카운터 및 결과 변수
        attempt_count = 0
        max_attempts = 60  # 최대 60회 시도 (약 1분)
        
        try:
            while attempt_count < max_attempts:
                attempt_count += 1
                
                try:
                    self.logger.debug(f"{self.exchange_name_kr} 연결 시도 {attempt_count}/{max_attempts}")
                    
                    # 기존 메시지 처리 태스크 취소
                    if self._message_handler_task and not self._message_handler_task.done():
                        self._message_handler_task.cancel()
                        try:
                            await self._message_handler_task
                        except asyncio.CancelledError:
                            pass
                        self._message_handler_task = None
                        
                    # 기존 연결 정리
                    if self.ws:
                        try:
                            await self.ws.close()
                        except:
                            pass
                        self.ws = None
                    
                    # 웹소켓 연결 수립 - 전략 클래스 대신 직접 구현
                    self.ws = await self._connect_websocket()
                    
                    # 연결 상태 업데이트
                    self.is_connected = True
                    
                    # 연결 후 설정
                    await self._on_connected()
                    
                    # 메시지 처리 태스크 시작
                    if self._message_handler_task is None or self._message_handler_task.done():
                        self._message_handler_task = asyncio.create_task(self._message_handler())
                        
                    # 핑 전송 태스크 시작 (바이빗은 직접 핑 전송 필요)
                    if self._requires_ping():
                        self._start_ping_task()
                        
                    # 연결 중 상태 해제
                    self.connecting = False
                        
                    self.logger.info(f"{self.exchange_name_kr} 웹소켓 연결 성공 (시도 {attempt_count}/{max_attempts})")
                    return True
                    
                except Exception as e:
                    if attempt_count == max_attempts:
                        self.logger.error(f"{self.exchange_name_kr} 웹소켓 연결 실패: {str(e)} (최대 시도 횟수 도달)")
                    else:
                        # self.logger.debug(f"{self.exchange_name_kr} 연결 시도 {attempt_count} 실패: {str(e)}")
                        pass
                    
                    # 마지막 시도가 아니면 0.1초 대기 후 재시도 (1초에서 0.1초로 변경)
                    if attempt_count < max_attempts:
                        await asyncio.sleep(0.1)
            
            # 모든 시도 실패
            self.is_connected = False
            self.logger.error(f"{self.exchange_name_kr} 60회 연결 시도 후 연결 실패")
            
            # 연결 중 상태 해제
            self.connecting = False
            return False
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 연결 프로세스 오류: {str(e)}")
            self.is_connected = False
            
            # 연결 중 상태 해제
            self.connecting = False
            return False
            
    async def _connect_websocket(self) -> websockets.WebSocketClientProtocol:
        """
        웹소켓 연결 수립 (전략에서 이전)
        
        Returns:
            websockets.WebSocketClientProtocol: 웹소켓 연결 객체
        """
        try:
            # 설정된 URI로 웹소켓 연결 수립
            # 내장 핑퐁 메커니즘 비활성화 (ping_interval=None)
            # 자체 핑퐁 메커니즘을 구현하여 사용
            ws = await websockets.connect(
                self.BASE_WS_URL,
                ping_interval=None,  # 내장 핑퐁 비활성화
                ping_timeout=None,   # 내장 핑퐁 타임아웃 비활성화
                close_timeout=2.0,   # 빠른 종료를 위한 타임아웃 설정
                max_size=1024 * 1024 * 10,  # 최대 메시지 크기 증가 (10MB)
                open_timeout=1.0      # 1초 연결 타임아웃 추가 - 1초 안에 연결 안되면 예외 발생
            )
            
            # 연결 성공
            return ws
            
        except Exception as e:
            # self.logger.error(f"{self.exchange_name_kr} 웹소켓 연결 실패: {str(e)}")
            raise
            
    def _requires_ping(self) -> bool:
        """바이빗은 클라이언트 측에서 20초마다 ping이 필요함"""
        return True
        
    async def _on_connected(self) -> None:
        """
        연결 후 초기화 작업 수행 (전략에서 이전)
        
        Args:
            없음
        """
        # 기존 구독이 있으면 다시 구독
        if self._subscribed_symbols:
            await self.subscribe(list(self._subscribed_symbols))

    async def _message_handler(self):
        """웹소켓 메시지 수신 및 처리 루프"""
        try:
            # 락을 사용하여 동시에 여러 태스크가 메시지 핸들러를 실행하지 않도록 함
            async with self._message_handler_lock:
                self.logger.debug(f"{self.exchange_name_kr} 메시지 핸들러 시작")
                
                while not self.stop_event.is_set() and self.is_connected and self.ws:
                    try:
                        # 메시지 수신
                        message = await self.ws.recv()
                        
                        # 메시지를 데이터 핸들러로 직접 전달하여 처리 (중복 파싱 제거)
                        processed_message = self.data_handler.process_message(message)
                        
                        # 메시지 타입 확인
                        if processed_message:
                            msg_type = processed_message.get("type", "unknown")
                                
                            # 구독 응답 처리
                            if msg_type == "subscription_response":
                                self.logger.info(f"{self.exchange_name_kr} 구독 응답: {processed_message['data']}")
                                continue
                            
                            # 핑퐁 응답 처리
                            elif msg_type == "pong":
                                # 로깅 제거 - 불필요한 로그 출력 방지
                                continue    
                                
                            # 오더북 데이터 처리 (스냅샷 또는 델타)
                            elif msg_type in ["snapshot", "delta"]:
                                # 스냅샷 캐시 업데이트
                                symbol = processed_message.get("symbol", "").replace("usdt", "").lower()
                                if symbol and msg_type == "snapshot":
                                    self.snapshot_cache[symbol] = processed_message
                                
                                self._on_message(processed_message)
                                
                            # 기타 메시지 - 디버그 로그만 남김
                            elif msg_type == "unknown" and self.config.get("debug_mode", False):
                                self.logger.debug(f"{self.exchange_name_kr} 기타 메시지: {msg_type}")
                            
                    except websockets.exceptions.ConnectionClosed as e:
                        self.logger.warning(f"{self.exchange_name_kr} 웹소켓 연결 끊김: {str(e)}")
                        self.is_connected = False
                        break
                        
                    except asyncio.CancelledError:
                        self.logger.info(f"{self.exchange_name_kr} 메시지 핸들러 태스크 취소됨")
                        break
                        
                    except Exception as e:
                        self.logger.error(f"{self.exchange_name_kr} 메시지 처리 중 오류: {str(e)}")
                        await asyncio.sleep(1)
                        
                self.logger.debug(f"{self.exchange_name_kr} 메시지 핸들러 종료")
        except asyncio.CancelledError:
            self.logger.info(f"{self.exchange_name_kr} 메시지 핸들러 태스크 취소됨")
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 핸들러 태스크 오류: {str(e)}")
            self.is_connected = False
                
    async def subscribe(self, symbols: List[str]) -> bool:
        """
        심볼 목록 구독
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            bool: 구독 성공 여부
        """
        if not symbols:
            self.logger.warning("구독할 심볼이 없습니다.")
            return False
            
        # 연결 확인
        if not self.is_connected:
            # 자동 연결 시도
            if not await self.connect():
                self.logger.error("웹소켓 연결이 없어 구독할 수 없습니다.")
                return False
                
        try:
            # 심볼 목록 정규화 (중복 제거)
            symbols_to_subscribe = list(set(symbols))
            
            # 구독 요청 - 직접 구현
            success = await self._subscribe_symbols(symbols_to_subscribe)
            
            if success:
                # 성공한 심볼 저장
                for symbol in symbols_to_subscribe:
                    self._subscribed_symbols.add(symbol)
                    
                # self.logger.info(f"{self.exchange_name_kr} 구독 성공: {len(symbols_to_subscribe)}개 심볼")
                return True
            else:
                self.logger.error(f"{self.exchange_name_kr} 구독 실패")
                return False
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 구독 중 오류: {str(e)}")
            return False
            
    async def _subscribe_symbols(self, symbols: List[str]) -> bool:
        """
        심볼 구독 (전략에서 이전)
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            bool: 구독 성공 여부
        """
        if not self.ws:
            self.logger.error(f"{self.exchange_name_kr} 심볼 구독 실패: 웹소켓 연결 없음")
            return False
            
        try:
            # 심볼 형식 변환 (핸들러의 normalize_symbol 활용)
            formatted_symbols = [self.data_handler.normalize_symbol(s) for s in symbols]
            
            # 바이빗은 한 번에 최대 10개의 심볼만 구독 가능
            MAX_SYMBOLS_PER_BATCH = 10
            success = True
            
            # 심볼을 10개씩 배치로 나누어 구독
            for i in range(0, len(formatted_symbols), MAX_SYMBOLS_PER_BATCH):
                batch_symbols = formatted_symbols[i:i + MAX_SYMBOLS_PER_BATCH]
                batch_num = (i // MAX_SYMBOLS_PER_BATCH) + 1
                total_batches = (len(formatted_symbols) + MAX_SYMBOLS_PER_BATCH - 1) // MAX_SYMBOLS_PER_BATCH
                
                # 구독 메시지 생성 (핸들러의 create_subscription_message 활용)
                subscribe_msg = self.data_handler.create_subscription_message(batch_symbols)
                # req_id 추가
                subscribe_msg["req_id"] = str(self._get_next_id())
                
                # 구독 요청 전송
                self.logger.info(f"{self.exchange_name_kr} 구독 요청: 배치 {batch_num}/{total_batches}, {len(batch_symbols)}개 심볼")
                
                await self.ws.send(json.dumps(subscribe_msg))
                
                # 연속 요청 시 짧은 지연 추가
                if i + MAX_SYMBOLS_PER_BATCH < len(formatted_symbols):
                    await asyncio.sleep(0.1)
                    
                # 심볼별 초기화 오더북 생성
                for symbol in batch_symbols:
                    if symbol not in self.snapshot_cache:
                        # 초기 스냅샷 없는 경우 빈 객체로 초기화
                        self.snapshot_cache[symbol] = {}
                        await self._initialize_orderbook(symbol)
            
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 구독 중 오류: {str(e)}")
            return False
            
    async def _initialize_orderbook(self, symbol: str) -> bool:
        """
        심볼의 오더북 초기화
        
        Args:
            symbol: 초기화할 심볼
            
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            # 현재 오더북 스냅샷이 있는지 확인
            snapshot = await self.get_orderbook_snapshot(symbol)
            
            # 스냅샷이 없는 경우 오더북 빈 상태로 초기화
            if not snapshot or not snapshot.get("bids") or not snapshot.get("asks"):
                self.logger.debug(f"{symbol} 오더북 초기화: 스냅샷 대기")
                # 스냅샷이 도착할 때까지 기다리지 않고 빈 상태로 시작
            else:
                self.logger.debug(f"{symbol} 오더북 초기화 완료")
                
            return True
            
        except Exception as e:
            self.logger.error(f"{symbol} 오더북 초기화 오류: {str(e)}")
            return False

    async def send_message(self, message: Union[str, Dict, List]) -> bool:
        """
        웹소켓을 통해 메시지 전송
        
        Args:
            message: 전송할 메시지 (문자열, 딕셔너리 또는 리스트)
            
        Returns:
            bool: 메시지 전송 성공 여부
        """
        # 연결 확인
        if not self.is_connected:
            self.logger.error("웹소켓 연결이 없어 메시지를 전송할 수 없습니다.")
            return False
            
        try:
            # 메시지 형식 변환
            if isinstance(message, (dict, list)):
                message = json.dumps(message)
                
            # 메시지 전송
            await self.ws.send(message)
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 전송 중 오류: {str(e)}")
            # 연결 문제 감지 시 상태만 업데이트 (재연결 시도하지 않음)
            self.is_connected = False
            return False
            
    def add_message_callback(self, callback: Callable) -> None:
        """
        웹소켓 메시지 콜백 함수 등록
        
        Args:
            callback: 메시지 수신 시 호출될 콜백 함수
        """
        if callback not in self.message_callbacks:
            self.message_callbacks.append(callback)
            
    def remove_message_callback(self, callback: Callable) -> bool:
        """
        웹소켓 메시지 콜백 함수 제거
        
        Args:
            callback: 제거할 콜백 함수
            
        Returns:
            bool: 콜백 제거 성공 여부
        """
        if callback in self.message_callbacks:
            self.message_callbacks.remove(callback)
            return True
        return False
        
    def add_orderbook_callback(self, callback: Callable) -> None:
        """
        오더북 업데이트 콜백 함수 등록
        
        Args:
            callback: 오더북 업데이트 수신 시 호출될 콜백 함수
        """
        if callback not in self.orderbook_callbacks:
            self.orderbook_callbacks.append(callback)
            
    def remove_orderbook_callback(self, callback: Callable) -> bool:
        """
        오더북 업데이트 콜백 함수 제거
        
        Args:
            callback: 제거할 콜백 함수
            
        Returns:
            bool: 콜백 제거 성공 여부
        """
        if callback in self.orderbook_callbacks:
            self.orderbook_callbacks.remove(callback)
            return True
        return False
    
    def _on_message(self, message: Dict[str, Any]) -> None:
        """
        메시지 수신 처리
        
        Args:
            message: 수신된 메시지
        """
        try:
            # 메시지 타입 확인
            msg_type = message.get("type", "unknown")
            
            # 오더북 업데이트 메시지 처리 (스냅샷 또는 델타)
            if msg_type in ["snapshot", "delta"]:
                # 심볼 확인
                symbol = message.get("symbol", "").replace("usdt", "").lower()
                
                # 콜백 호출
                for callback in self.message_callbacks:
                    try:
                        callback(message)
                    except Exception as cb_error:
                        self.logger.error(f"{self.exchange_name_kr} 메시지 콜백 호출 중 오류: {str(cb_error)}")
                
                # 오더북 콜백 호출 - 핸들러에서 스냅샷/델타 처리 완료 후 오더북 객체 가져오기
                if symbol in self.data_handler.orderbooks:
                    # 완전한 오더북 데이터 조회
                    current_orderbook = self.data_handler.get_orderbook(symbol)
                    
                    if current_orderbook:
                        # 콜백 호출
                        for callback in self.orderbook_callbacks:
                            try:
                                callback({
                                    "exchange": self.exchange_code,
                                    "symbol": symbol,
                                    "timestamp": time.time() * 1000,
                                    "current_orderbook": current_orderbook
                                })
                            except Exception as cb_error:
                                self.logger.error(f"{self.exchange_name_kr} 오더북 콜백 호출 중 오류: {str(cb_error)}")
                    
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 처리 중 오류: {str(e)}")
            
    async def get_exchange_info(self) -> Dict[str, Any]:
        """
        거래소 정보 조회
        
        Returns:
            Dict[str, Any]: 거래소 정보
        """
        return await self.data_handler.get_exchange_info()
        
    async def get_websocket(self) -> Optional[websockets.WebSocketClientProtocol]:
        """
        웹소켓 객체 반환
        
        Returns:
            Optional[websockets.WebSocketClientProtocol]: 웹소켓 객체
        """
        return self.ws
        
    def _start_ping_task(self):
        """핑 전송 태스크 시작"""
        if self.ping_task is None or self.ping_task.done():
            self.ping_task = asyncio.create_task(self._ping_loop())
            
    def _stop_ping_task(self):
        """핑 전송 태스크 중지"""
        if self.ping_task and not self.ping_task.done():
            self.ping_task.cancel()
            
    async def _ping_loop(self):
        """
        주기적으로 핑 메시지를 전송하는 루프
        바이빗은 20초마다 ping 메시지를 전송해야 합니다.
        """
        try:
            while not self.stop_event.is_set() and self.is_connected:
                try:
                    # 핑 메시지 전송
                    await self.send_ping()
                    self.logger.debug(f"{self.exchange_name_kr} 핑 전송")
                        
                except Exception as e:
                    self.logger.error(f"{self.exchange_name_kr} 핑 전송 중 오류: {str(e)}")
                    # 연결에 문제가 있는 경우 상태만 업데이트
                    self.is_connected = False
                    break
                
                # 다음 핑 전송까지 대기
                await asyncio.sleep(self._ping_interval)
                
        except asyncio.CancelledError:
            # 태스크 취소 시 조용히 종료
            self.logger.debug(f"{self.exchange_name_kr} 핑 루프 태스크 취소됨")
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 핑 루프 중 오류: {str(e)}")
            # 연결 문제가 있을 수 있으므로 상태만 업데이트
            if not self.is_shutting_down:
                self.is_connected = False

    def _get_next_id(self) -> int:
        """
        다음 요청 ID 생성 (전략에서 이전)
        
        Returns:
            int: 요청 ID
        """
        current_id = self.id_counter
        self.id_counter += 1
        return current_id
        
    async def send_ping(self) -> bool:
        """
        Ping 메시지 전송
        
        Returns:
            bool: 전송 성공 여부
        """
        if not self.ws or not self.is_connected:
            return False
            
        try:
            # 바이빗은 직접 ping 메시지 필요
            ping_msg = {
                "op": "ping",
                "req_id": str(self._get_next_id())
            }
            await self.ws.send(json.dumps(ping_msg))
            return True
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 핑 메시지 전송 실패: {str(e)}")
            return False
                
    async def get_orderbook_snapshot(self, symbol: str) -> Dict[str, Any]:
        """
        특정 심볼의 오더북 스냅샷 조회
        
        현재 메모리에 있는 오더북 데이터를 반환합니다.
        
        Args:
            symbol: 조회할 심볼
            
        Returns:
            Dict[str, Any]: 오더북 스냅샷 데이터
        """
        # 현재 메모리에 있는 오더북 데이터 반환
        return self.data_handler.get_orderbook(symbol)

    async def disconnect(self) -> bool:
        """
        거래소 웹소켓 연결 종료
        
        Returns:
            bool: 연결 종료 성공 여부
        """
        self._stopping = True
        self.is_shutting_down = True  # 종료 중 플래그 설정
        self.stop_event.set()  # stop_event 설정으로 메시지 핸들러 루프 중단
        
        try:
            # 핑 태스크 정지
            self._stop_ping_task()
            
            # 메시지 핸들러 종료 대기
            if self._message_handler_task and not self._message_handler_task.done():
                self._message_handler_task.cancel()
                try:
                    await self._message_handler_task
                except asyncio.CancelledError:
                    pass
                self._message_handler_task = None
                
            # 웹소켓 연결 종료
            if self.ws:
                await self._disconnect_websocket()
                self.ws = None
                
            # 연결 상태 업데이트
            self.is_connected = False
            
            # 구독 목록 초기화
            self._subscribed_symbols.clear()
            
            # 스냅샷 캐시 초기화
            self.snapshot_cache.clear()
            
            # self.logger.info(f"{self.exchange_name_kr} 웹소켓 연결 종료됨")
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 웹소켓 연결 종료 중 오류: {str(e)}")
            return False
        finally:
            self._stopping = False
            self.is_shutting_down = False  # 종료 완료 후 플래그 해제

    async def _disconnect_websocket(self) -> None:
        """웹소켓 연결 종료 (전략에서 이전)"""
        if self.ws:
            try:
                await self.ws.close()
                self.logger.info(f"{self.exchange_name_kr} 웹소켓 연결 종료됨")
            except Exception as e:
                self.logger.error(f"{self.exchange_name_kr} 웹소켓 연결 종료 중 오류: {str(e)}")
                
    async def unsubscribe(self, symbols: List[str]) -> bool:
        """
        심볼 목록 구독 해제
        
        Args:
            symbols: 구독 해제할 심볼 목록
            
        Returns:
            bool: 구독 해제 성공 여부
        """
        if not symbols:
            return True
            
        # 연결 확인
        if not self.is_connected:
            # 구독 해제는 연결이 없어도 로컬 상태만 업데이트
            for symbol in symbols:
                if symbol in self._subscribed_symbols:
                    self._subscribed_symbols.remove(symbol)
            return True
            
        try:
            # 구독 해제 요청 - 직접 구현
            success = await self._unsubscribe_symbols(symbols)
            
            if success:
                # 구독 목록에서 제거
                for symbol in symbols:
                    if symbol in self._subscribed_symbols:
                        self._subscribed_symbols.remove(symbol)
                        
                # self.logger.info(f"{self.exchange_name_kr} 구독 해제 성공: {len(symbols)}개 심볼")
                return True
            else:
                self.logger.error(f"{self.exchange_name_kr} 구독 해제 실패")
                return False
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 구독 해제 중 오류: {str(e)}")
            return False
            
    async def _unsubscribe_symbols(self, symbols: List[str]) -> bool:
        """
        심볼 구독 해제 (전략에서 이전)
        
        Args:
            symbols: 구독 해제할 심볼 목록
            
        Returns:
            bool: 구독 해제 성공 여부
        """
        if not self.ws:
            self.logger.error(f"{self.exchange_name_kr} 구독 해제 실패: 웹소켓 연결 없음")
            return False
            
        try:
            # 심볼 형식 변환 (핸들러의 normalize_symbol 활용)
            formatted_symbols = [self.data_handler.normalize_symbol(s) for s in symbols]
            
            # 바이빗은 한 번에 최대 10개의 심볼만 처리 가능
            MAX_SYMBOLS_PER_BATCH = 10
            success = True
            
            # 심볼을 10개씩 배치로 나누어 구독 해제
            for i in range(0, len(formatted_symbols), MAX_SYMBOLS_PER_BATCH):
                batch_symbols = formatted_symbols[i:i + MAX_SYMBOLS_PER_BATCH]
                batch_num = (i // MAX_SYMBOLS_PER_BATCH) + 1
                total_batches = (len(formatted_symbols) + MAX_SYMBOLS_PER_BATCH - 1) // MAX_SYMBOLS_PER_BATCH
                
                # 심볼별로 depth 스트림 구독 해제 - args만 생성
                args = [f"orderbook.50.{symbol.upper()}" for symbol in batch_symbols]
                
                # 구독 해제 메시지 생성
                request_id = self._get_next_id()
                unsubscribe_msg = {
                    "op": "unsubscribe",
                    "args": args,
                    "req_id": str(request_id)
                }
                
                # 구독 해제 요청 전송
                self.logger.info(f"{self.exchange_name_kr} 구독 해제 요청: 배치 {batch_num}/{total_batches}, {len(args)}개 심볼")
                
                await self.ws.send(json.dumps(unsubscribe_msg))
                
                # 연속 요청 시 짧은 지연 추가
                if i + MAX_SYMBOLS_PER_BATCH < len(formatted_symbols):
                    await asyncio.sleep(0.1)
            
            # 구독 목록에서 제거
            self._subscribed_symbols = {s for s in self._subscribed_symbols if s not in symbols}
            
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 구독 해제 중 오류: {str(e)}")
            return False 