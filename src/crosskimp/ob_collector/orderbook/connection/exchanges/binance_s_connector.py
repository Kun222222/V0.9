"""
바이낸스 현물 커넥터 모듈

바이낸스 현물 거래소 연결 및 데이터 처리를 담당하는 커넥터 클래스를 제공합니다.
"""

import asyncio
import time
import json
import websockets
from typing import Dict, List, Any, Optional, Union, Callable
import logging

from crosskimp.common.config.app_config import get_config
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR

from crosskimp.ob_collector.orderbook.connection.connector_interface import ExchangeConnectorInterface
from crosskimp.ob_collector.orderbook.data_handlers.exchanges.binance_s_handler import BinanceSpotDataHandler

class BinanceSpotConnector(ExchangeConnectorInterface):
    """
    바이낸스 현물 거래소 커넥터
    
    바이낸스 현물 거래소와의 웹소켓 연결, 메시지 처리, 오더북 데이터 관리를 담당합니다.
    """
    
    # 상수 정의 (전략 클래스에서 이동)
    BASE_WS_URL = "wss://stream.binance.com/ws"
    BASE_REST_URL = "https://api.binance.com"
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    
    def __init__(self, connection_manager=None):
        """
        초기화
        
        Args:
            connection_manager: 연결 관리자 객체 (선택 사항)
        """
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        self.config = get_config()
        
        # 거래소 코드 및 한글 이름 설정
        self.exchange_code = Exchange.BINANCE_SPOT.value
        self.exchange_name_kr = EXCHANGE_NAMES_KR[self.exchange_code]
        
        # 연결 관리자 참조 저장
        self.connection_manager = connection_manager
        
        # 상태 변수
        self._is_connected = False
        self.ws = None
        self.stop_event = asyncio.Event()
        self.message_task = None
        
        # 전략 클래스 대신 직접 변수 관리
        self.id_counter = 1      # 요청 ID 카운터
        
        # 데이터 핸들러 초기화
        self.data_handler = BinanceSpotDataHandler()
        
        # 콜백 함수 저장소
        self.message_callbacks = []
        self.orderbook_callbacks = []
        
        # 마지막 메시지 수신 시간 추적
        self.last_message_time = time.time()
        
        # 데이터 무응답 시간 임계값 (초)
        self.data_timeout = 30  # 30초 이상 데이터가 오지 않으면 재연결
        
        # 메시지 핸들러 락 추가 - 동시 실행 방지
        self._message_handler_lock = asyncio.Lock()
        
        # 재연결 관련 플래그
        self.connecting = False
        self.is_shutting_down = False
        
        # 구독 상태
        self.subscribed_symbols = set()  # 리스트에서 세트로 변경
        self.snapshot_cache = {}         # 스냅샷 캐시 추가
        
        # 메시지 콜백 등록
        self.add_message_callback(self._on_message)
        
    @property
    def is_connected(self) -> bool:
        """
        현재 연결 상태 반환
        
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
            value: 설정할 연결 상태
        """
        if self._is_connected != value:
            self._is_connected = value
            
            # 연결 관리자가 있으면 상태 업데이트
            if self.connection_manager:
                self.connection_manager.update_exchange_status(self.exchange_code, value)
    
    async def connect(self) -> bool:
        """
        바이낸스 현물 거래소 웹소켓 서버에 연결
        
        Returns:
            bool: 연결 성공 여부
        """
        # 이미 연결되어 있는지 확인
        if self.is_connected or self.connecting:
            self.logger.warning(f"{self.exchange_name_kr} 이미 연결 중이거나 연결됨")
            return self.is_connected
            
        try:
            # 연결 시작 플래그 설정
            self.connecting = True
            self.stop_event.clear()
            
            # 기존 태스크 정리
            if self.message_task and not self.message_task.done():
                self.message_task.cancel()
                self.message_task = None
            
            # 웹소켓 연결 - 빗썸과 유사한 재시도 로직 적용
            retry_count = 0
            max_retries = 6  # 최대 6회 재시도
            
            while retry_count < max_retries:
                try:
                    retry_count += 1
                    self.logger.debug(f"{self.exchange_name_kr} 웹소켓 연결 시도 #{retry_count}")
                    
                    # 웹소켓 연결 (5초 타임아웃 설정)
                    self.ws = await asyncio.wait_for(
                        websockets.connect(
                            self.BASE_WS_URL,
                            ping_interval=None,  # 바이낸스 서버가 20초마다 ping을 보내므로 클라이언트 ping은 비활성화
                            ping_timeout=60,     # ping 응답 대기 시간 60초(1분)로 설정 (바이낸스 정책에 맞춤)
                            close_timeout=10,    # 닫기 타임아웃
                            max_size=10 * 1024 * 1024,  # 최대 메시지 크기 10MB로 설정
                            max_queue=1024,      # 수신 큐 크기 증가
                            compression=None,    # 압축 비활성화하여 CPU 사용량 감소
                        ),
                        timeout=5.0  # 5초 타임아웃
                    )
                    
                    self.logger.info(f"{self.exchange_name_kr} 웹소켓 연결 성공 (시도 #{retry_count})")
                    break  # 연결 성공
                
                except asyncio.TimeoutError:
                    self.logger.warning(f"{self.exchange_name_kr} 웹소켓 연결 타임아웃 (시도 #{retry_count}/{max_retries})")
                    if retry_count >= max_retries:
                        self.logger.error(f"{self.exchange_name_kr} 최대 재시도 횟수({max_retries}회) 초과")
                        raise
                    await asyncio.sleep(0.5)  # 0.5초 후 재시도
                
                except Exception as e:
                    self.logger.error(f"{self.exchange_name_kr} 웹소켓 연결 실패 (시도 #{retry_count}/{max_retries}): {str(e)}")
                    if retry_count >= max_retries:
                        self.logger.error(f"{self.exchange_name_kr} 최대 재시도 횟수({max_retries}회) 초과")
                        raise
                    await asyncio.sleep(0.5)  # 0.5초 후 재시도
            
            # 연결 성공 시 상태 업데이트
            self.is_connected = True
            self.connecting = False
            self.last_message_time = time.time()
            
            # 메시지 처리 태스크 시작
            self.message_task = asyncio.create_task(self._message_handler())
            
            # 초기화 작업 수행 (기존 구독 복원)
            if self.subscribed_symbols:
                symbols_list = list(self.subscribed_symbols)
                await self.subscribe(symbols_list)
                self.logger.info(f"{self.exchange_name_kr} 재연결 후 {len(symbols_list)}개 심볼 구독 복원")
            
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 연결 실패: {str(e)}")
            self.is_connected = False
            self.connecting = False
            return False
    
    async def disconnect(self) -> bool:
        """
        바이낸스 현물 거래소 웹소켓 연결 종료
        
        Returns:
            bool: 연결 종료 성공 여부
        """
        try:
            # 종료 이벤트 설정
            self.stop_event.set()
            self.is_shutting_down = True
            
            # 메시지 처리 태스크 종료
            if self.message_task and not self.message_task.done():
                self.message_task.cancel()
                self.message_task = None
                
            # 웹소켓 닫기
            if self.ws:
                await self.ws.close()
                self.ws = None
                
            # 상태 업데이트
            self.is_connected = False
            self.connecting = False
            self.is_shutting_down = False
            
            self.logger.info(f"{self.exchange_name_kr} 연결 종료됨")
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 연결 종료 중 오류: {str(e)}")
            self.is_connected = False
            return False
    
    async def _message_handler(self):
        """
        웹소켓 메시지 수신 및 처리 루프
        """
        try:
            # 락을 사용하여 동시에 여러 태스크가 메시지 핸들러를 실행하지 않도록 함
            async with self._message_handler_lock:
                self.logger.debug(f"{self.exchange_name_kr} 메시지 핸들러 시작")
                
                while not self.stop_event.is_set() and self.ws:
                    try:
                        # 메시지 수신
                        message = await self.ws.recv()
                        self.last_message_time = time.time()
                        
                        # 메시지 처리 (통합 process_message 메서드 사용)
                        processed = self.data_handler.process_message(message)
                        
                        # 콜백 실행
                        for callback in self.message_callbacks:
                            try:
                                callback(processed)
                            except Exception as cb_error:
                                self.logger.error(f"{self.exchange_name_kr} 콜백 처리 중 오류: {str(cb_error)}")
                                
                    except websockets.exceptions.ConnectionClosed as cc:
                        # 연결 종료 감지 시 상태만 업데이트 (재연결 시도 없음)
                        self.logger.warning(f"{self.exchange_name_kr} 연결 종료됨 (코드: {cc.code}, 사유: {cc.reason})")
                        self.is_connected = False  # 상태 업데이트만 수행 (ConnectionManager가 감지)
                        break
                        
                    except Exception as e:
                        # 기타 예외 처리
                        self.logger.error(f"{self.exchange_name_kr} 메시지 처리 중 오류: {str(e)}")
                        await asyncio.sleep(1)
                        
                self.logger.debug(f"{self.exchange_name_kr} 메시지 핸들러 종료")
                        
        except asyncio.CancelledError:
            self.logger.debug(f"{self.exchange_name_kr} 메시지 처리 태스크 취소됨")
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 처리 태스크 오류: {str(e)}")
            self.is_connected = False
    
    async def subscribe(self, symbols: List[str]) -> bool:
        """
        심볼 목록 구독
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            bool: 구독 성공 여부
        """
        if not self.is_connected:
            self.logger.error(f"{self.exchange_name_kr} 구독 실패: 연결되지 않음")
            return False
            
        try:
            # 정규화된 심볼 목록
            normalized_symbols = [self.data_handler.normalize_symbol(s) for s in symbols]
            
            # 모든 심볼 구독 (필터링 로직 제거)
            symbols_to_subscribe = normalized_symbols
            
            # 구독 요청
            self.logger.debug(f"{self.exchange_name_kr} {len(symbols_to_subscribe)}개 심볼 구독 중...")
            
            # 심볼 형식 변환 - 핸들러의 normalize_symbol 사용
            formatted_symbols = []
            for s in symbols_to_subscribe:
                # REST API용 형식으로 변환 (소문자 + usdt)
                formatted = s.lower() + 'usdt'
                formatted_symbols.append(formatted)
            
            # 심볼별로 depth 스트림 구독
            params = [f"{symbol}@depth@100ms" for symbol in formatted_symbols]
            
            # 구독 메시지 생성
            request_id = self._get_next_id()
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": params,
                "id": request_id
            }
            
            # 구독 요청 전송
            self.logger.debug(f"{self.exchange_name_kr} 구독 요청: {len(params)}개 심볼")
            
            await self.ws.send(json.dumps(subscribe_msg))
            
            # 구독 심볼 저장
            self.subscribed_symbols.update(symbols_to_subscribe)
            
            # 각 심볼에 대한 오더북 스냅샷 가져오기
            for symbol in symbols_to_subscribe:
                asyncio.create_task(self._initialize_orderbook(symbol))
                
            self.logger.debug(f"{self.exchange_name_kr} {len(symbols_to_subscribe)}개 심볼 구독 성공")
            return True
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 구독 중 오류: {str(e)}")
            return False
    
    async def unsubscribe(self, symbols: List[str]) -> bool:
        """
        심볼 목록 구독 해제
        
        Args:
            symbols: 구독 해제할 심볼 목록
            
        Returns:
            bool: 구독 해제 성공 여부
        """
        if not self.is_connected:
            self.logger.error(f"{self.exchange_name_kr} 구독 해제 실패: 연결되지 않음")
            return False
            
        try:
            # 정규화된 심볼 목록
            normalized_symbols = [self.data_handler.normalize_symbol(s) for s in symbols]
            
            # 구독 중인 심볼만 필터링
            symbols_to_unsub = [s for s in normalized_symbols if s in self.subscribed_symbols]
            
            if not symbols_to_unsub:
                self.logger.info(f"{self.exchange_name_kr} 구독 해제할 심볼 없음")
                return True
                
            # 구독 해제 요청
            self.logger.info(f"{self.exchange_name_kr} {len(symbols_to_unsub)}개 심볼 구독 해제 중...")
            
            # 심볼 형식 변환
            formatted_symbols = []
            for s in symbols_to_unsub:
                # REST API용 형식으로 변환 (소문자 + usdt)
                formatted = s.lower() + 'usdt'
                formatted_symbols.append(formatted)
            
            # 심볼별로 depth 스트림 구독 해제
            params = [f"{symbol}@depth@100ms" for symbol in formatted_symbols]
            
            # 구독 해제 메시지 생성
            request_id = self._get_next_id()
            unsubscribe_msg = {
                "method": "UNSUBSCRIBE",
                "params": params,
                "id": request_id
            }
            
            # 구독 해제 요청 전송
            self.logger.debug(f"{self.exchange_name_kr} 구독 해제 요청: {len(params)}개 심볼")
            
            await self.ws.send(json.dumps(unsubscribe_msg))
            
            # 구독 목록에서 제거
            for s in symbols_to_unsub:
                self.subscribed_symbols.discard(s)
                # 스냅샷 캐시에서 제거
                if s in self.snapshot_cache:
                    del self.snapshot_cache[s]
            
            self.logger.info(f"{self.exchange_name_kr} {len(symbols_to_unsub)}개 심볼 구독 해제 성공")
            return True
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 구독 해제 중 오류: {str(e)}")
            return False
    
    async def send_message(self, message: Union[str, Dict, List]) -> bool:
        """
        웹소켓을 통해 메시지 전송
        
        Args:
            message: 전송할 메시지 (문자열, 딕셔너리 또는 리스트)
            
        Returns:
            bool: 메시지 전송 성공 여부
        """
        if not self.is_connected or not self.ws:
            self.logger.error(f"{self.exchange_name_kr} 메시지 전송 실패: 연결되지 않음")
            return False
            
        try:
            # 메시지가 dict 또는 list인 경우 JSON 문자열로 변환
            if isinstance(message, (dict, list)):
                message = json.dumps(message)
                
            # 메시지 전송
            await self.ws.send(message)
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 전송 중 오류: {str(e)}")
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
    
    async def _initialize_orderbook(self, symbol: str) -> None:
        """
        특정 심볼의 오더북 초기화
        
        REST API를 통한 스냅샷 요청 및 처리
        
        Args:
            symbol: 초기화할 심볼
        """
        try:            
            # 데이터 핸들러를 통한 스냅샷 요청
            snapshot = await self.data_handler.get_orderbook_snapshot(symbol)
            
            # 스냅샷 캐시에 저장
            if not snapshot.get("error", False):
                self.snapshot_cache[symbol] = snapshot
                self.logger.debug(f"{self.exchange_name_kr} {symbol} 오더북 스냅샷 수신 완료")
                
                # 콜백 호출
                for callback in self.orderbook_callbacks:
                    try:
                        callback({
                            "type": "snapshot",
                            "exchange": self.exchange_code,
                            "symbol": symbol,
                            "data": snapshot
                        })
                    except Exception as e:
                        self.logger.error(f"{self.exchange_name_kr} 오더북 스냅샷 콜백 처리 중 오류: {str(e)}")
            else:
                self.logger.error(f"{self.exchange_name_kr} {symbol} 오더북 스냅샷 요청 실패: {snapshot.get('message', '알 수 없는 오류')}")
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} {symbol} 오더북 초기화 중 오류: {str(e)}")
    
    def _on_message(self, message: Dict[str, Any]) -> None:
        """
        웹소켓 메시지 수신 처리
        
        Args:
            message: 수신된 메시지
        """
        try:
            # 메시지 유형에 따라 처리
            message_type = message.get("type", "unknown")
            
            # PONG 응답이면 무시
            if message_type == "pong":
                return
                
            # 구독 응답 처리
            if message_type == "subscription_response":
                return
                
            # 오류 메시지 처리
            if message_type == "error":
                error_type = message.get("error", "unknown_error")
                error_msg = message.get("message", "")
                self.logger.error(f"{self.exchange_name_kr} 오류 메시지: {error_type} - {error_msg}")
                return
                
            # 오더북 업데이트인 경우
            if message_type == "depth_update":
                # 오더북 콜백 구성
                callback_data = {
                    "type": "update",
                    "exchange": self.exchange_code,
                    "symbol": message.get("symbol", ""),
                    "data": message
                }
                
                # 시퀀스 정보 추가
                if "sequence" in message:
                    callback_data["sequence"] = message["sequence"]
                
                # 현재 오더북 상태 정보가 있으면 추가
                if "current_orderbook" in message:
                    callback_data["current_orderbook"] = message["current_orderbook"]
                    
                    # 필요한 경우에만 시퀀스 정보 복사
                    if "sequence" in message and (
                        "sequence" not in message["current_orderbook"] or 
                        message["current_orderbook"]["sequence"] == 0
                    ):
                        message["current_orderbook"]["sequence"] = message["sequence"]
                
                # 오더북 콜백 호출
                for callback in self.orderbook_callbacks:
                    try:
                        callback(callback_data)
                    except Exception as e:
                        self.logger.error(f"{self.exchange_name_kr} 오더북 업데이트 콜백 처리 중 오류: {str(e)}")
                        
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 처리 중 오류: {str(e)}")
    
    async def refresh_snapshots(self, symbols: List[str] = None) -> None:
        """
        오더북 스냅샷 갱신
        
        Args:
            symbols: 갱신할 심볼 목록 (None이면 모든 구독 중인 심볼)
        """
        if symbols is None:
            symbols = list(self.subscribed_symbols)
            
        if not symbols:
            return
            
        self.logger.info(f"{self.exchange_name_kr} 오더북 스냅샷 갱신 시작: {len(symbols)}개 심볼")
        
        for symbol in symbols:
            asyncio.create_task(self._initialize_orderbook(symbol))
            
    async def get_exchange_info(self) -> Dict[str, Any]:
        """
        거래소 정보 조회
        
        Returns:
            Dict[str, Any]: 거래소 정보
        """
        return {
            "exchange": self.exchange_code,
            "name": self.exchange_name_kr,
            "connected": self.is_connected,
            "symbols": list(self.subscribed_symbols)
        }
        
    async def get_websocket(self) -> Optional[websockets.WebSocketClientProtocol]:
        """
        웹소켓 객체 반환 (ConnectionManager 호환용)
        
        Returns:
            Optional[websockets.WebSocketClientProtocol]: 웹소켓 객체
        """
        return self.ws
        
    async def get_orderbook_snapshot(self, symbol: str) -> Dict[str, Any]:
        """
        특정 심볼의 오더북 스냅샷 조회
        
        Args:
            symbol: 조회할 심볼
            
        Returns:
            Dict[str, Any]: 오더북 스냅샷 데이터
        """
        if symbol in self.snapshot_cache:
            return self.snapshot_cache[symbol]
            
        # 스냅샷이 없으면 새로 가져오기
        try:
            snapshot = await self.data_handler.get_orderbook_snapshot(symbol)
            if snapshot and not snapshot.get("error", False):
                self.snapshot_cache[symbol] = snapshot
                return snapshot
            else:
                self.logger.error(f"{self.exchange_name_kr} {symbol} 오더북 스냅샷 가져오기 실패")
                return {}
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} {symbol} 오더북 스냅샷 요청 중 오류: {str(e)}")
            return {}

    def _get_next_id(self) -> int:
        """
        다음 요청 ID 생성
        
        Returns:
            int: 요청 ID
        """
        current_id = self.id_counter
        self.id_counter += 1
        return current_id