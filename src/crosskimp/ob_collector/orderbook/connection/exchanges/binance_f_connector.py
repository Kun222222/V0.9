"""
바이낸스 선물 커넥터 모듈

바이낸스 선물 거래소의 웹소켓 연결 및 데이터 처리를 통합한 커넥터를 제공합니다.
"""

import json
import time
import asyncio
import websockets
from typing import Dict, List, Any, Optional, Union, Callable, Set

from crosskimp.common.config.app_config import get_config
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR

from crosskimp.ob_collector.orderbook.connection.strategies.binance_f_strategie import BinanceFutureConnectionStrategy
from crosskimp.ob_collector.orderbook.data_handlers.exchanges.binance_f_handler import BinanceFutureDataHandler
from crosskimp.ob_collector.orderbook.connection.connector_interface import ExchangeConnectorInterface

class BinanceFutureConnector(ExchangeConnectorInterface):
    """
    바이낸스 선물 커넥터 클래스
    
    바이낸스 선물 거래소의 웹소켓 연결 및 데이터 처리를 통합합니다.
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
        self.exchange_code = Exchange.BINANCE_FUTURE.value
        self.exchange_name_kr = EXCHANGE_NAMES_KR[self.exchange_code]
        
        # 연결 관리자 참조 저장
        self.connection_manager = connection_manager
        
        # 바이낸스 선물 연결 전략 및 데이터 핸들러 생성
        self.connection_strategy = BinanceFutureConnectionStrategy()
        self.data_handler = BinanceFutureDataHandler()
        
        # 웹소켓 관련 상태 관리
        self.ws = None
        self.message_callbacks = []
        self.message_task = None
        self.last_message_time = 0
        self.connect_timeout = self.config.get_system("connection.connect_timeout", 30)
        self.is_shutting_down = False
        
        # 구독 중인 심볼 목록 (Set으로 변경)
        self.subscribed_symbols: Set[str] = set()
        
        # 스냅샷 캐시 (심볼 -> 스냅샷 데이터)
        self.snapshot_cache: Dict[str, Dict[str, Any]] = {}
        
        # 오더북 업데이트 콜백
        self.orderbook_callbacks = []
        
        # 연결 상태
        self._is_connected = False
        
        # 메시지 콜백 등록
        self.add_message_callback(self._on_message)
        
    @property
    def is_connected(self) -> bool:
        """
        현재 연결 상태 반환
        
        Returns:
            bool: 연결 상태
        """
        return self._is_connected

    @is_connected.setter
    def is_connected(self, value: bool):
        """
        연결 상태 설정
        
        Args:
            value: 새 연결 상태
        """
        if self._is_connected != value:
            self._is_connected = value
            
            # 연결 관리자에 상태 업데이트
            if self.connection_manager:
                self.connection_manager.update_exchange_status(self.exchange_code, value)
        
    async def connect(self) -> bool:
        """
        바이낸스 선물 웹소켓 연결
        
        Returns:
            bool: 연결 성공 여부
        """
        if self.is_connected:
            return True
            
        try:
            # 웹소켓 연결
            self.ws = await self.connection_strategy.connect(self.connect_timeout)
            
            if self.ws:
                # 연결 성공 시 상태 업데이트
                self.is_connected = True
                self.last_message_time = time.time()
                
                # 메시지 처리 작업 시작
                if not self.message_task or self.message_task.done():
                    self.message_task = asyncio.create_task(self._message_handler())
                
                # 초기화 작업 수행
                await self.connection_strategy.on_connected(self.ws)
                
                return True
            else:
                self.is_connected = False
                return False
                
        except Exception as e:
            self.logger.error(f"바이낸스 선물 웹소켓 연결 실패: {str(e)}")
            self.is_connected = False
            return False
            
    async def disconnect(self) -> bool:
        """
        바이낸스 선물 웹소켓 연결 종료
        
        Returns:
            bool: 연결 종료 성공 여부
        """
        try:
            self.is_shutting_down = True
            
            # 작업 취소
            if self.message_task and not self.message_task.done():
                self.message_task.cancel()
                
            # 웹소켓 닫기
            if self.ws:
                await self.connection_strategy.disconnect(self.ws)
                self.ws = None
                
            self.is_connected = False
            self.is_shutting_down = False
            return True
            
        except Exception as e:
            self.logger.error(f"바이낸스 선물 웹소켓 연결 종료 실패: {str(e)}")
            return False
    
    async def _message_handler(self):
        """웹소켓 메시지 처리 루프"""
        if not self.ws:
            return
            
        try:
            async for message in self.ws:
                try:
                    self.last_message_time = time.time()
                    
                    # 메시지 전처리
                    processed_message = self.connection_strategy.preprocess_message(message)
                    
                    # 콜백 실행
                    for callback in self.message_callbacks:
                        try:
                            callback(processed_message)
                        except Exception:
                            pass
                            
                except Exception as e:
                    self.logger.error(f"바이낸스 선물 메시지 처리 중 오류: {str(e)}")
                    await asyncio.sleep(1)
                    
        except websockets.exceptions.ConnectionClosed:
            if not self.is_shutting_down:
                self.is_connected = False
                await self.handle_reconnect()
                
        except asyncio.CancelledError:
            pass
            
        except Exception as e:
            if not self.is_shutting_down:
                self.logger.error(f"바이낸스 선물 메시지 핸들러 오류: {str(e)}")
                self.is_connected = False
                await self.handle_reconnect()
                
    async def handle_reconnect(self):
        """연결 종료 시 ConnectionManager에 재연결 요청"""
        if self.is_shutting_down:
            return
            
        # 연결 관리자에 재연결 요청
        if self.connection_manager:
            asyncio.create_task(self.connection_manager.reconnect_exchange(self.exchange_code))
        
    async def subscribe(self, symbols: List[str]) -> bool:
        """
        심볼 구독
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            bool: 구독 성공 여부
        """
        if not self.is_connected:
            return False
            
        # 이미 구독 중인 심볼 필터링 (Set 연산 사용)
        new_symbols = [s for s in symbols if s not in self.subscribed_symbols]
        
        if not new_symbols:
            return True
            
        try:
            # 웹소켓 구독
            success = await self.connection_strategy.subscribe(self.ws, new_symbols)
            
            if success:
                # 구독 목록에 추가 (Set 연산 사용)
                self.subscribed_symbols.update(new_symbols)
                
                # 각 심볼에 대한 오더북 스냅샷 가져오기
                for symbol in new_symbols:
                    asyncio.create_task(self._initialize_orderbook(symbol))
                
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"바이낸스 선물 구독 중 오류: {str(e)}")
            return False
            
    async def unsubscribe(self, symbols: List[str]) -> bool:
        """
        심볼 구독 해제
        
        Args:
            symbols: 구독 해제할 심볼 목록
            
        Returns:
            bool: 구독 해제 성공 여부
        """
        if not self.is_connected:
            return False
            
        # 실제로 구독 중인 심볼 필터링 (Set 연산 사용)
        symbols_to_unsub = [s for s in symbols if s in self.subscribed_symbols]
        
        if not symbols_to_unsub:
            return True
            
        try:
            # 웹소켓 구독 해제
            success = await self.connection_strategy.unsubscribe(self.ws, symbols_to_unsub)
            
            if success:
                # 구독 목록에서 제거 (Set 연산 사용)
                self.subscribed_symbols.difference_update(symbols_to_unsub)
                
                # 스냅샷 캐시에서 제거
                for symbol in symbols_to_unsub:
                    self.snapshot_cache.pop(symbol, None)
                
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"바이낸스 선물 구독 해제 중 오류: {str(e)}")
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
        심볼의 오더북 초기화 (스냅샷 요청)
        
        Args:
            symbol: 초기화할 심볼
        """
        try:
            # 오더북 스냅샷 요청
            snapshot = await self.data_handler.get_orderbook_snapshot(symbol)
            
            # 스냅샷 캐시에 저장
            if not snapshot.get("error", False):
                self.snapshot_cache[symbol] = snapshot
                
                # 콜백 호출
                for callback in self.orderbook_callbacks:
                    try:
                        callback({
                            "type": "snapshot",
                            "exchange": Exchange.BINANCE_FUTURE.value,
                            "symbol": symbol,
                            "data": snapshot
                        })
                    except Exception:
                        pass
            else:
                self.logger.error(f"바이낸스 선물 {symbol} 오더북 스냅샷 요청 실패: {snapshot.get('message', '알 수 없는 오류')}")
                
        except Exception as e:
            self.logger.error(f"바이낸스 선물 {symbol} 오더북 초기화 중 오류: {str(e)}")
            
    def _on_message(self, message: Dict[str, Any]) -> None:
        """
        웹소켓 메시지 수신 처리
        
        Args:
            message: 수신된 메시지
        """
        try:
            # 메시지 유형에 따라 처리
            if message.get("type") == "depth_update":
                # 오더북 업데이트 처리
                processed_update = self.data_handler.process_orderbook_update(message)
                
                # 콜백 데이터 준비
                callback_data = {
                    "type": "update",
                    "exchange": Exchange.BINANCE_FUTURE.value,
                    "symbol": processed_update.get("symbol", ""),
                    "data": processed_update
                }
                
                # 시퀀스 정보 추가
                if "sequence" in processed_update:
                    callback_data["sequence"] = processed_update["sequence"]
                
                # 현재 오더북 상태 추가
                if "current_orderbook" in processed_update:
                    callback_data["current_orderbook"] = processed_update["current_orderbook"]
                    
                    # 시퀀스 복사
                    if "sequence" in processed_update and (
                        "sequence" not in processed_update["current_orderbook"] or 
                        processed_update["current_orderbook"]["sequence"] == 0
                    ):
                        processed_update["current_orderbook"]["sequence"] = processed_update["sequence"]
                
                # 콜백 호출
                for callback in self.orderbook_callbacks:
                    try:
                        callback(callback_data)
                    except Exception:
                        pass
                        
        except Exception as e:
            self.logger.error(f"바이낸스 선물 메시지 처리 중 오류: {str(e)}")
            
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
            if snapshot:
                self.snapshot_cache[symbol] = snapshot
                return snapshot
            else:
                return {}
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 선물 {symbol} 오더북 스냅샷 요청 중 오류: {str(e)}")
            return {}
    
    async def refresh_snapshots(self, symbols: List[str] = None) -> None:
        """
        오더북 스냅샷 갱신
        """
        if symbols is None:
            symbols = list(self.subscribed_symbols)
            
        if not symbols:
            return
            
        for symbol in symbols:
            asyncio.create_task(self._initialize_orderbook(symbol))
    
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
        if not self.is_connected or not self.ws:
            return False
            
        try:
            # 메시지가 딕셔너리나 리스트인 경우 JSON 문자열로 변환
            if isinstance(message, (dict, list)):
                message = json.dumps(message)
                
            await self.ws.send(message)
            return True
            
        except Exception as e:
            self.logger.error(f"바이낸스 선물 메시지 전송 중 오류: {str(e)}")
            return False 