"""
바이낸스 현물 커넥터 모듈

바이낸스 현물 거래소 연결 및 데이터 처리를 담당하는 커넥터 클래스를 제공합니다.
"""

import asyncio
import time
import json
import websockets
from typing import Dict, List, Any, Optional, Union, Callable, Set

from crosskimp.common.config.app_config import get_config
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR
from crosskimp.common.events.system_eventbus import get_component_event_bus
from crosskimp.common.events.system_types import EventChannels

from crosskimp.ob_collector.orderbook.connection.connector_interface import ExchangeConnectorInterface
from crosskimp.ob_collector.orderbook.connection.strategies.binance_s_strategie import BinanceSpotConnectionStrategy
from crosskimp.ob_collector.orderbook.data_handlers.exchanges.binance_s_handler import BinanceSpotDataHandler

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
        self.reconnect_delay = 1.0  # 초기 재연결 지연 시간
        
        # 연결 설정
        self.connect_timeout = self.config.get_system("connection.connect_timeout", 30)
        
        # 구독 상태 (Set으로 변경)
        self.subscribed_symbols: Set[str] = set()
        
        # 스냅샷 캐시
        self.snapshot_cache: Dict[str, Dict[str, Any]] = {}
        
        # 이벤트 버스 추가
        self.event_bus = get_component_event_bus(SystemComponent.OB_COLLECTOR)
        
    @property
    def is_connected(self) -> bool:
        """
        현재 연결 상태 반환
        
        Returns:
            bool: 연결 상태
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
            
            # 연결 관리자에 상태 업데이트
            if self.connection_manager:
                self.connection_manager.update_exchange_status(self.exchange_code, value)
            
            if value:
                self.reconnect_attempts = 0  # 연결 성공 시 재시도 카운터 초기화
    
    async def connect(self) -> bool:
        """
        바이낸스 현물 거래소 웹소켓 서버에 연결
        
        Returns:
            bool: 연결 성공 여부
        """
        if self.is_connected:
            return True
            
        try:
            # ConnectionStrategy를 통한 웹소켓 연결
            self.ws = await self.connection_strategy.connect(self.connect_timeout)
            
            # 연결 성공 시 상태 업데이트
            self.is_connected = True
            
            # 기존에 메시지 수신 루프가 실행 중이지 않으면 시작
            if not self.message_loop_task or self.message_loop_task.done():
                self.stop_event.clear()
                self.message_loop_task = asyncio.create_task(self._message_handler())
                
            # 연결 후 초기화 작업
            await self.connection_strategy.on_connected(self.ws)
            
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
            # 메시지 처리 루프 중지
            self.stop_event.set()
            
            if self.message_loop_task and not self.message_loop_task.done():
                try:
                    # 5초 타임아웃으로 메시지 루프 종료 대기
                    await asyncio.wait_for(self.message_loop_task, timeout=5.0)
                except asyncio.TimeoutError:
                    self.message_loop_task.cancel()
                    
            # 웹소켓 연결 종료
            if self.ws:
                await self.connection_strategy.disconnect(self.ws)
                self.ws = None
                
            # 연결 상태 업데이트
            self.is_connected = False
            
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
            while not self.stop_event.is_set() and self.ws:
                try:
                    # 메시지 수신 (타임아웃 5초)
                    message = await asyncio.wait_for(self.ws.recv(), timeout=5)
                    
                    # 메시지 전처리
                    processed = self.connection_strategy.preprocess_message(message)
                    
                    # 메시지 콜백 호출
                    for callback in self.message_callbacks:
                        try:
                            callback(processed)
                        except Exception:
                            pass
                    
                    # 오더북 업데이트 메시지인 경우 데이터 핸들러로 처리
                    if processed.get("type") == "depth_update":
                        # 데이터 핸들러 처리
                        orderbook_data = self.data_handler.process_orderbook_update(processed)
                        
                        # 오더북 콜백 호출
                        for callback in self.orderbook_callbacks:
                            try:
                                callback(orderbook_data)
                            except Exception:
                                pass
            
                except asyncio.TimeoutError:
                    continue
                
                except websockets.exceptions.ConnectionClosed:
                    if not self.is_shutting_down:
                        self.is_connected = False
                        # 이벤트 버스를 통해 연결 끊김 알림
                        await self.event_bus.publish(EventChannels.Component.ObCollector.CONNECTION_LOST, {
                            "exchange_code": self.exchange_code,
                            "exchange_name": self.exchange_name_kr,
                            "timestamp": time.time()
                        })
                    break
                
                except Exception as e:
                    if not self.is_shutting_down:
                        self.logger.error(f"바이낸스 현물 메시지 핸들러 오류: {str(e)}")
                        self.is_connected = False
                        # 이벤트 버스를 통해 연결 끊김 알림
                        await self.event_bus.publish(EventChannels.Component.ObCollector.CONNECTION_LOST, {
                            "exchange_code": self.exchange_code,
                            "exchange_name": self.exchange_name_kr,
                            "error": str(e),
                            "timestamp": time.time()
                        })
                
        except asyncio.CancelledError:
            pass
    
    async def subscribe(self, symbols: List[str]) -> bool:
        """
        심볼 목록 구독
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            bool: 구독 성공 여부
        """
        if not self.is_connected:
            return False
            
        try:
            # 이미 구독 중인 심볼 필터링
            new_symbols = [s for s in symbols if s not in self.subscribed_symbols]
            
            if not new_symbols:
                return True
                
            # 연결 전략을 통한 구독
            success = await self.connection_strategy.subscribe(self.ws, new_symbols)
            
            if success:
                # 구독 심볼 목록 저장 (Set 연산 사용)
                self.subscribed_symbols.update(new_symbols)
                
                # 각 심볼에 대해 스냅샷 요청
                for symbol in new_symbols:
                    asyncio.create_task(self._initialize_orderbook(symbol))
                    
                return True
            else:
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
            return False
            
        try:
            # 실제로 구독 중인 심볼 필터링
            symbols_to_unsub = [s for s in symbols if s in self.subscribed_symbols]
            
            if not symbols_to_unsub:
                return True
                
            # 연결 전략을 통한 구독 해제
            success = await self.connection_strategy.unsubscribe(self.ws, symbols_to_unsub)
            
            if success:
                # 구독 심볼 목록에서 제거 (Set 연산 사용)
                self.subscribed_symbols.difference_update(symbols_to_unsub)
                
                # 스냅샷 캐시에서 제거
                for symbol in symbols_to_unsub:
                    self.snapshot_cache.pop(symbol, None)
                    
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 구독 해제 중 오류: {str(e)}")
            return False
    
    def add_orderbook_callback(self, callback: Callable) -> None:
        """
        오더북 업데이트 콜백 함수 등록
        
        Args:
            callback: 오더북 업데이트 수신 시 호출될 콜백 함수
        """
        if callback not in self.orderbook_callbacks:
            self.orderbook_callbacks.append(callback)
    
    async def _initialize_orderbook(self, symbol: str) -> None:
        """
        특정 심볼의 오더북 초기화
        
        REST API를 통한 스냅샷 요청 및 처리
        
        Args:
            symbol: 초기화할 심볼
        """
        try:
            # 데이터 핸들러를 통한 스냅샷 요청
            await self.data_handler.get_orderbook_snapshot(symbol)
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} {symbol} 오더북 초기화 중 오류: {str(e)}")
    
    async def get_orderbook_snapshot(self, symbol: str) -> Dict[str, Any]:
        """
        특정 심볼의 오더북 스냅샷 조회
        
        Args:
            symbol: 조회할 심볼
            
        Returns:
            Dict[str, Any]: 오더북 스냅샷 데이터
        """
        return await self.data_handler.get_orderbook_snapshot(symbol)
    
    async def refresh_snapshots(self, symbols: List[str] = None) -> None:
        """
        오더북 스냅샷 갱신
        """
        if symbols is None:
            symbols = list(self.subscribed_symbols)
            
        for symbol in symbols:
            await self._initialize_orderbook(symbol)
            
    async def get_websocket(self) -> Optional[websockets.WebSocketClientProtocol]:
        """웹소켓 객체 반환"""
        return self.ws
    
    # 선택적 메서드 구현
    def add_message_callback(self, callback: Callable) -> None:
        """메시지 콜백 함수 등록"""
        if callback not in self.message_callbacks:
            self.message_callbacks.append(callback)
    
    def remove_message_callback(self, callback: Callable) -> bool:
        """메시지 콜백 함수 제거"""
        if callback in self.message_callbacks:
            self.message_callbacks.remove(callback)
            return True
        return False
    
    def remove_orderbook_callback(self, callback: Callable) -> bool:
        """오더북 콜백 함수 제거"""
        if callback in self.orderbook_callbacks:
            self.orderbook_callbacks.remove(callback)
            return True
        return False
        
    async def send_message(self, message: Union[str, Dict, List]) -> bool:
        """웹소켓으로 메시지 전송"""
        if not self.is_connected:
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