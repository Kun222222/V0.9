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
from crosskimp.ob_collector.orderbook.connection.strategies.binance_s_strategie import BinanceSpotConnectionStrategy
from crosskimp.ob_collector.orderbook.data_handlers.binance_s_handler import BinanceSpotDataHandler

class BinanceSpotConnector(ExchangeConnectorInterface):
    """
    바이낸스 현물 거래소 커넥터
    
    바이낸스 현물 거래소와의 웹소켓 연결, 메시지 처리, 오더북 데이터 관리를 담당합니다.
    """
    
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
        self.message_loop_task = None
        
        # 전략 및 데이터 핸들러 초기화
        self.connection_strategy = BinanceSpotConnectionStrategy()
        self.data_handler = BinanceSpotDataHandler()
        
        # 콜백 함수 저장소
        self.message_callbacks = []
        self.orderbook_callbacks = []
        
        # 연결 재시도 설정
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 1.0  # 초기 재연결 지연 시간
        
        # 연결 설정
        self.connect_timeout = self.config.get_system("connection.connect_timeout", 30)
        
        # 구독 상태
        self.subscribed_symbols = []
        
        # self.logger.info("바이낸스 현물 커넥터 초기화 완료")
        
    @property
    def is_connected(self) -> bool:
        """
        현재 연결 상태 반환
        
        Returns:
            bool: 연결 상태 (True: 연결됨, False: 연결 안됨)
        """
        return self._is_connected and self.ws is not None
    
    @is_connected.setter
    def is_connected(self, value: bool):
        """
        연결 상태 설정
        
        Args:
            value: 설정할 연결 상태
        """
        if self._is_connected != value:
            self._is_connected = value
            if value:
                self.logger.info(f"{self.exchange_name_kr} 연결 상태 변경: 연결됨")
                self.reconnect_attempts = 0  # 연결 성공 시 재시도 카운터 초기화
            else:
                self.logger.info(f"{self.exchange_name_kr} 연결 상태 변경: 연결 끊김")
    
    async def connect(self) -> bool:
        """
        바이낸스 현물 거래소 웹소켓 서버에 연결
        
        Returns:
            bool: 연결 성공 여부
        """
        if self.is_connected:
            self.logger.info(f"{self.exchange_name_kr} 이미 연결되어 있음")
            return True
            
        try:
            # self.logger.info("바이낸스 현물 연결 시작...")
            
            # ConnectionStrategy를 통한 웹소켓 연결 (타임아웃 추가)
            self.ws = await self.connection_strategy.connect(self.connect_timeout)
            
            # 연결 성공 시 상태 업데이트
            self.is_connected = True
            
            # ConnectionManager에 상태 업데이트
            if self.connection_manager:
                self.connection_manager.update_exchange_status(self.exchange_code, True)
            
            # 기존에 메시지 수신 루프가 실행 중이지 않으면 시작
            if not self.message_loop_task or self.message_loop_task.done():
                self.stop_event.clear()
                self.message_loop_task = asyncio.create_task(self._message_handler())
                
            # 연결 후 초기화 작업
            await self.connection_strategy.on_connected(self.ws)
            
            self.logger.info(f"{self.exchange_name_kr} 연결 성공")
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 연결 실패: {str(e)}")
            self.is_connected = False
            return False
    
    async def disconnect(self) -> bool:
        """
        바이낸스 현물 거래소 웹소켓 연결 종료
        
        Returns:
            bool: 연결 종료 성공 여부
        """
        try:
            self.logger.info(f"{self.exchange_name_kr} 연결 종료 시작...")
            
            # 메시지 처리 루프 중지
            self.stop_event.set()
            
            if self.message_loop_task and not self.message_loop_task.done():
                try:
                    # 5초 타임아웃으로 메시지 루프 종료 대기
                    await asyncio.wait_for(self.message_loop_task, timeout=5.0)
                except asyncio.TimeoutError:
                    self.logger.warning(f"{self.exchange_name_kr} 메시지 루프 종료 타임아웃")
                    self.message_loop_task.cancel()
                    
            # 웹소켓 연결 종료
            if self.ws:
                await self.connection_strategy.disconnect(self.ws)
                self.ws = None
                
            # 연결 상태 업데이트
            self.is_connected = False
            
            self.logger.info(f"{self.exchange_name_kr} 연결 종료 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 연결 종료 중 오류: {str(e)}")
            self.is_connected = False
            return False
    
    async def _message_handler(self):
        """
        웹소켓 메시지 수신 및 처리 루프
        """
        self.logger.info(f"{self.exchange_name_kr} 메시지 수신 루프 시작")
        
        last_ping_time = time.time()
        ping_interval = 30  # 30초 간격으로 ping 전송
        
        try:
            while not self.stop_event.is_set() and self.ws:
                try:
                    # 메시지 수신 (0.1초 타임아웃)
                    message = await asyncio.wait_for(self.ws.recv(), timeout=0.1)
                    
                    # 메시지 전처리
                    processed = self.connection_strategy.preprocess_message(message)
                    
                    # 메시지 콜백 호출
                    for callback in self.message_callbacks:
                        try:
                            callback(processed)
                        except Exception as callback_error:
                            self.logger.error(f"메시지 콜백 실행 중 오류: {str(callback_error)}")
                    
                    # 오더북 업데이트 메시지인 경우 데이터 핸들러로 처리
                    if processed.get("type") == "depth_update":
                        # 데이터 핸들러 처리
                        orderbook_data = self.data_handler.process_orderbook_update(processed)
                        
                        # 오더북 콜백 호출
                        for callback in self.orderbook_callbacks:
                            try:
                                callback(orderbook_data)
                            except Exception as callback_error:
                                self.logger.error(f"오더북 콜백 실행 중 오류: {str(callback_error)}")
                
                except asyncio.TimeoutError:
                    # 주기적으로 ping 전송 (필요한 경우)
                    current_time = time.time()
                    if self.connection_strategy.requires_ping() and (current_time - last_ping_time) >= ping_interval:
                        await self.connection_strategy.send_ping(self.ws)
                        last_ping_time = current_time
                    continue
                
                except websockets.exceptions.ConnectionClosed as e:
                    self.logger.warning(f"{self.exchange_name_kr} 연결 닫힘: {e.code} {e.reason}")
                    self.is_connected = False
                    await self._reconnect()
                    break
                
                except Exception as e:
                    self.logger.error(f"{self.exchange_name_kr} 메시지 수신 중 오류: {str(e)}")
                    
        except asyncio.CancelledError:
            self.logger.info(f"{self.exchange_name_kr} 메시지 수신 루프 취소됨")
        
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 루프 오류: {str(e)}")
            
        finally:
            self.logger.info(f"{self.exchange_name_kr} 메시지 수신 루프 종료")
    
    async def _reconnect(self):
        """
        연결 종료 시 재연결 시도
        """
        if self.stop_event.is_set():
            return
            
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            self.logger.error(f"{self.exchange_name_kr} 최대 재연결 시도 횟수 초과 ({self.max_reconnect_attempts})")
            return
            
        self.reconnect_attempts += 1
        delay = min(self.reconnect_delay * (2 ** (self.reconnect_attempts - 1)), 60)
        
        self.logger.info(f"{self.exchange_name_kr} {delay:.1f}초 후 재연결 시도 ({self.reconnect_attempts}/{self.max_reconnect_attempts})")
        await asyncio.sleep(delay)
        
        success = await self.connect()
        if success and self.subscribed_symbols:
            # 재연결 성공 시 기존 구독 심볼 복구
            await self.subscribe(self.subscribed_symbols)
    
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
            self.logger.info(f"{self.exchange_name_kr} 구독 시작: {len(symbols)}개 심볼")
            
            # 연결 전략을 통한 구독
            success = await self.connection_strategy.subscribe(self.ws, symbols)
            
            if success:
                # 구독 심볼 목록 저장
                self.subscribed_symbols = symbols
                
                # 각 심볼에 대해 스냅샷 요청
                for symbol in symbols:
                    asyncio.create_task(self._initialize_orderbook(symbol))
                    
                self.logger.info(f"{self.exchange_name_kr} 구독 성공: {len(symbols)}개 심볼")
                return True
            else:
                self.logger.error(f"{self.exchange_name_kr} 구독 실패")
                return False
                
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
            self.logger.info(f"{self.exchange_name_kr} 구독 해제 시작: {len(symbols)}개 심볼")
            
            # 연결 전략을 통한 구독 해제
            success = await self.connection_strategy.unsubscribe(self.ws, symbols)
            
            if success:
                # 구독 심볼 목록에서 제거
                self.subscribed_symbols = [s for s in self.subscribed_symbols if s not in symbols]
                self.logger.info(f"{self.exchange_name_kr} 구독 해제 성공: {len(symbols)}개 심볼")
                return True
            else:
                self.logger.error(f"{self.exchange_name_kr} 구독 해제 실패")
                return False
                
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
        if not self.is_connected:
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
            # 개별 심볼 로그 제거 (refresh_snapshots에서 한 번에 출력)
            
            # 데이터 핸들러를 통한 스냅샷 요청
            snapshot = await self.data_handler.get_orderbook_snapshot(symbol)
            
            # 스냅샷 응답에 오류가 있는 경우
            if snapshot.get("error", False):
                self.logger.error(f"{self.exchange_name_kr} {symbol} 오더북 스냅샷 요청 실패: {snapshot.get('message', '')}")
                return
                
            # self.logger.info(f"{self.exchange_name_kr} {symbol} 오더북 초기화 완료")
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} {symbol} 오더북 초기화 중 오류: {str(e)}")
    
    def _on_message(self, message: Dict[str, Any]) -> None:
        """
        메시지 수신 콜백
        
        Args:
            message: 수신된 메시지
        """
        try:
            # 메시지 유형에 따른 처리
            msg_type = message.get("type", "unknown")
            
            if msg_type == "depth_update":
                # 오더북 업데이트 메시지
                symbol = message.get("symbol", "")
                exchange = message.get("exchange", "")
                
                # DEBUG 레벨로 간단한 메시지만 로깅
                self.logger.debug(f"{self.exchange_name_kr} {symbol} 오더북 업데이트 수신")
                
                # 오더북 콜백 호출은 메시지 핸들러에서 처리
                
            elif msg_type == "subscription_response":
                # 구독 응답
                response_data = message.get("data", {})
                request_id = response_data.get("id", 0)
                result = response_data.get("result", None)
                
                if result is None:
                    self.logger.info(f"{self.exchange_name_kr} 구독 응답 (ID: {request_id}): 성공")
                else:
                    error = response_data.get("error", {})
                    self.logger.error(f"{self.exchange_name_kr} 구독 응답 (ID: {request_id}): 실패 - {error}")
                    
            elif msg_type == "error":
                # 오류 메시지
                error_type = message.get("error", "unknown_error")
                error_msg = message.get("message", "")
                self.logger.error(f"{self.exchange_name_kr} 오류 메시지: {error_type} - {error_msg}")
                
            else:
                # 기타 메시지는 디버그 레벨로 로깅
                self.logger.debug(f"{self.exchange_name_kr} 기타 메시지 수신: {msg_type}")
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 처리 중 오류: {str(e)}")
    
    async def refresh_snapshots(self, symbols: List[str] = None) -> None:
        """
        오더북 스냅샷 갱신
        
        Args:
            symbols: 갱신할 심볼 목록 (None이면 모든 구독 중인 심볼)
        """
        if symbols is None:
            symbols = self.subscribed_symbols
            
        if symbols:
            self.logger.info(f"{self.exchange_name_kr} 오더북 스냅샷 갱신 시작: {', '.join(symbols)}")
            
        for symbol in symbols:
            await self._initialize_orderbook(symbol)
            
    async def get_exchange_info(self) -> Dict[str, Any]:
        """
        거래소 정보 조회
        
        Returns:
            Dict[str, Any]: 거래소 정보
        """
        return await self.data_handler.get_exchange_info()
        
    async def get_websocket(self) -> Optional[websockets.WebSocketClientProtocol]:
        """
        웹소켓 객체 반환 (ConnectionManager 호환용)
        
        Returns:
            Optional[websockets.WebSocketClientProtocol]: 웹소켓 객체
        """
        return self.ws
        
    async def reconnect(self) -> bool:
        """
        재연결 수행
        
        Returns:
            bool: 재연결 성공 여부
        """
        await self.disconnect()
        success = await self.connect()
        
        if success and self.subscribed_symbols:
            # 재연결 성공 시 기존 구독 심볼 복구
            await self.subscribe(self.subscribed_symbols)
            
        return success
        
    async def get_orderbook_snapshot(self, symbol: str) -> Dict[str, Any]:
        """
        특정 심볼의 오더북 스냅샷 조회
        
        Args:
            symbol: 조회할 심볼
            
        Returns:
            Dict[str, Any]: 오더북 스냅샷 데이터
        """
        return await self.data_handler.get_orderbook_snapshot(symbol)