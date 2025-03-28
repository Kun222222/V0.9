"""
바이낸스 선물 커넥터 모듈

바이낸스 선물 거래소의 웹소켓 연결 및 데이터 처리를 통합한 커넥터를 제공합니다.
"""

import json
import time
import asyncio
import websockets
from typing import Dict, List, Any, Optional, Union, Callable
import logging

from crosskimp.common.config.app_config import get_config
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR

from crosskimp.ob_collector.orderbook.connection.strategies.binance_f_strategie import BinanceFutureConnectionStrategy
from crosskimp.ob_collector.orderbook.data_handlers.binance_f_handler import BinanceFutureDataHandler
from crosskimp.ob_collector.orderbook.connection.connector_interface import ExchangeConnectorInterface

class BinanceFutureConnector(ExchangeConnectorInterface):
    """
    바이낸스 선물 커넥터 클래스
    
    바이낸스 선물 거래소의 웹소켓 연결 및 데이터 처리를 통합합니다.
    ExchangeConnectorInterface를 구현하여 표준화된 인터페이스를 제공합니다.
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
        
        # WebsocketConnector 대신 직접 웹소켓 관련 상태 관리
        self.ws = None
        self.name = "바이낸스선물"
        self.message_callbacks = []
        self.message_task = None
        self.ping_task = None
        self.last_message_time = 0
        self.connect_timeout = self.config.get_system("connection.connect_timeout", 30)
        self.is_shutting_down = False
        
        # 구독 중인 심볼 목록
        self.subscribed_symbols = []
        
        # 스냅샷 캐시 (심볼 -> 스냅샷 데이터)
        self.snapshot_cache = {}
        
        # 오더북 업데이트 콜백
        self.orderbook_callbacks = []
        
        # 연결 상태
        self._is_connected = False
        
        # 메시지 콜백 등록
        self.add_message_callback(self._on_message)
        
        # self.logger.info("바이낸스 선물 커넥터 초기화 완료")
        
    @property
    def is_connected(self) -> bool:
        """
        현재 연결 상태 반환
        
        Returns:
            bool: 연결 상태 (True: 연결됨, False: 연결 안됨)
        """
        return self._is_connected

    @is_connected.setter
    def is_connected(self, value: bool):
        """
        연결 상태 설정
        
        Args:
            value: 새 연결 상태
        """
        self._is_connected = value
        
    async def connect(self) -> bool:
        """
        바이낸스 선물 웹소켓 연결
        
        Returns:
            bool: 연결 성공 여부
        """
        # 이미 연결되어 있는지 확인 (현물 커넥터와 일관성)
        if self.is_connected:
            self.logger.info("바이낸스 선물 이미 연결되어 있음")
            return True
            
        try:
            # self.logger.info("바이낸스 선물 웹소켓 연결 시작")
            
            # 웹소켓 연결 직접 생성 (타임아웃 유지)
            self.ws = await self.connection_strategy.connect(self.connect_timeout)
            
            if self.ws:
                # 연결 성공 시 상태 업데이트
                self.is_connected = True
                self.last_message_time = time.time()
                self.logger.info("바이낸스 선물 웹소켓 연결 성공")
                
                # ConnectionManager에 상태 업데이트 (hasattr 제거)
                if self.connection_manager:
                    self.connection_manager.update_exchange_status(Exchange.BINANCE_FUTURE.value, True)
                
                # 메시지 처리 작업 시작 (현물 커넥터와 일관성)
                if not self.message_task or self.message_task.done():
                    self.message_task = asyncio.create_task(self._message_handler())
                
                # 초기화 작업 수행
                await self.connection_strategy.on_connected(self.ws)
                
                return True
            else:
                self.logger.error("바이낸스 선물 웹소켓 연결 실패: 웹소켓 객체가 생성되지 않음")
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
        self.logger.info("바이낸스 선물 웹소켓 연결 종료 시작")
        
        try:
            self.is_shutting_down = True
            
            # 작업 취소
            if self.message_task and not self.message_task.done():
                self.message_task.cancel()
                
            if self.ping_task and not self.ping_task.done():
                self.ping_task.cancel()
                
            # 웹소켓 닫기
            if self.ws:
                await self.connection_strategy.disconnect(self.ws)
                self.ws = None
                
            self.is_connected = False
            self.is_shutting_down = False
            self.logger.info("바이낸스 선물 웹소켓 연결 종료 성공")
            return True
            
        except Exception as e:
            self.logger.error(f"바이낸스 선물 웹소켓 연결 종료 실패: {str(e)}")
            return False
    
    async def _message_handler(self):
        """웹소켓 메시지 처리 루프"""
        if not self.ws:
            self.logger.error(f"{self.name} 메시지 핸들러 실행 불가: 웹소켓 연결 없음")
            return
            
        try:
            async for message in self.ws:
                try:
                    self.last_message_time = time.time()
                    
                    # 거래소별 전략으로 메시지 전처리
                    processed_message = self.connection_strategy.preprocess_message(message)
                    
                    # 콜백 실행
                    for callback in self.message_callbacks:
                        try:
                            callback(processed_message)
                        except Exception as cb_error:
                            self.logger.error(f"{self.name} 콜백 처리 중 오류: {str(cb_error)}")
                            
                except Exception as msg_error:
                    self.logger.error(f"{self.name} 메시지 처리 중 오류: {str(msg_error)}")
                    
        except websockets.exceptions.ConnectionClosed as e:
            if not self.is_shutting_down:
                self.logger.warning(f"{self.name} 연결 종료됨: {e.code} {e.reason}")
                self.is_connected = False
                asyncio.create_task(self._reconnect())
                
        except asyncio.CancelledError:
            self.logger.info(f"{self.name} 메시지 핸들러 작업 취소됨")
            
        except Exception as e:
            if not self.is_shutting_down:
                self.logger.error(f"{self.name} 메시지 핸들러 오류: {str(e)}")
                self.is_connected = False
                asyncio.create_task(self._reconnect())
                
    async def _reconnect(self):
        """내부 재연결 함수"""
        if self.is_shutting_down:
            return
            
        reconnect_delay = 1.0
        self.logger.info(f"{self.name} {reconnect_delay}초 후 재연결 시도...")
        await asyncio.sleep(reconnect_delay)
        
        # 재연결 시도
        await self.disconnect()
        await self.connect()
        
        # 재연결 후 구독 복원
        if self.is_connected and self.subscribed_symbols:
            await self.subscribe(self.subscribed_symbols)
            
    async def subscribe(self, symbols: List[str]) -> bool:
        """
        심볼 구독
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            bool: 구독 성공 여부
        """
        if not self.is_connected:
            self.logger.error("바이낸스 선물 심볼 구독 실패: 연결되어 있지 않음")
            return False
            
        # 이미 구독 중인 심볼 필터링
        new_symbols = [s for s in symbols if s not in self.subscribed_symbols]
        
        if not new_symbols:
            self.logger.info("바이낸스 선물 구독: 모든 심볼이 이미 구독 중")
            return True
            
        self.logger.info(f"바이낸스 선물 구독 시작: {len(new_symbols)}개 심볼")
        
        try:
            # 웹소켓 구독
            success = await self.connection_strategy.subscribe(self.ws, new_symbols)
            
            if success:
                # 구독 목록에 추가
                self.subscribed_symbols.extend(new_symbols)
                
                # 각 심볼에 대한 오더북 스냅샷 가져오기
                for symbol in new_symbols:
                    asyncio.create_task(self._initialize_orderbook(symbol))
                    
                self.logger.info(f"바이낸스 선물 구독 성공: {len(new_symbols)}개 심볼")
                return True
            else:
                self.logger.error("바이낸스 선물 구독 실패")
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
            self.logger.error("바이낸스 선물 심볼 구독 해제 실패: 연결되어 있지 않음")
            return False
            
        # 실제로 구독 중인 심볼 필터링
        symbols_to_unsub = [s for s in symbols if s in self.subscribed_symbols]
        
        if not symbols_to_unsub:
            self.logger.info("바이낸스 선물 구독 해제: 구독 중인 심볼 없음")
            return True
            
        self.logger.info(f"바이낸스 선물 구독 해제 시작: {len(symbols_to_unsub)}개 심볼")
        
        try:
            # 웹소켓 구독 해제
            success = await self.connection_strategy.unsubscribe(self.ws, symbols_to_unsub)
            
            if success:
                # 구독 목록에서 제거
                self.subscribed_symbols = [s for s in self.subscribed_symbols if s not in symbols_to_unsub]
                
                # 스냅샷 캐시에서 제거
                for symbol in symbols_to_unsub:
                    if symbol in self.snapshot_cache:
                        del self.snapshot_cache[symbol]
                        
                self.logger.info(f"바이낸스 선물 구독 해제 성공: {len(symbols_to_unsub)}개 심볼")
                return True
            else:
                self.logger.error("바이낸스 선물 구독 해제 실패")
                return False
                
        except Exception as e:
            self.logger.error(f"바이낸스 선물 구독 해제 중 오류: {str(e)}")
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
            self.logger.error("바이낸스 선물 메시지 전송 실패: 연결되어 있지 않음")
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
        심볼의 오더북 초기화 (스냅샷 요청)
        
        Args:
            symbol: 초기화할 심볼
        """
        # 개별 심볼 로그 제거 (refresh_snapshots에서 한 번에 출력)
        
        try:
            # 오더북 스냅샷 요청
            snapshot = await self.data_handler.get_orderbook_snapshot(symbol)
            
            # 스냅샷 캐시에 저장
            if not snapshot.get("error", False):
                self.snapshot_cache[symbol] = snapshot
                # self.logger.info(f"바이낸스 선물 {symbol} 오더북 스냅샷 수신 완료")
                
                # 콜백 호출
                for callback in self.orderbook_callbacks:
                    try:
                        callback({
                            "type": "snapshot",
                            "exchange": Exchange.BINANCE_FUTURE.value,
                            "symbol": symbol,
                            "data": snapshot
                        })
                    except Exception as e:
                        self.logger.error(f"바이낸스 선물 오더북 스냅샷 콜백 처리 중 오류: {str(e)}")
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
                
                # current_orderbook 정보가 있으면 콜백 데이터에 포함시킵니다
                callback_data = {
                    "type": "update",
                    "exchange": Exchange.BINANCE_FUTURE.value,
                    "symbol": processed_update.get("symbol", ""),
                    "data": processed_update
                }
                
                # 시퀀스 정보가 processed_update에 직접 있는 경우 이를 콜백에 포함시킵니다
                if "sequence" in processed_update:
                    callback_data["sequence"] = processed_update["sequence"]
                
                # 현재 오더북 상태 정보가 있으면 추가합니다
                if "current_orderbook" in processed_update:
                    callback_data["current_orderbook"] = processed_update["current_orderbook"]
                    
                    # 현재 오더북에 시퀀스 정보가 있는지 확인하고, 없으면 processed_update의 시퀀스 사용
                    if "sequence" in processed_update and (
                        "sequence" not in processed_update["current_orderbook"] or 
                        processed_update["current_orderbook"]["sequence"] == 0
                    ):
                        processed_update["current_orderbook"]["sequence"] = processed_update["sequence"]
                
                # 콜백 호출
                for callback in self.orderbook_callbacks:
                    try:
                        callback(callback_data)
                    except Exception as e:
                        self.logger.error(f"바이낸스 선물 오더북 업데이트 콜백 처리 중 오류: {str(e)}")
                        
        except Exception as e:
            self.logger.error(f"바이낸스 선물 메시지 처리 중 오류: {str(e)}")
            
    async def refresh_snapshots(self, symbols: List[str] = None) -> None:
        """
        오더북 스냅샷 갱신
        
        Args:
            symbols: 갱신할 심볼 목록 (None이면 구독 중인 모든 심볼)
        """
        if symbols is None:
            symbols = self.subscribed_symbols
            
        if not symbols:
            return
            
        self.logger.info(f"{self.exchange_name_kr} 선물 오더북 스냅샷 갱신 시작: {', '.join(symbols)}")
        
        for symbol in symbols:
            asyncio.create_task(self._initialize_orderbook(symbol))
            
    async def get_exchange_info(self) -> Dict[str, Any]:
        """
        거래소 정보 조회
        
        Returns:
            Dict[str, Any]: 거래소 정보
        """
        return await self.data_handler.get_exchange_info()
        
    # ConnectionManager와의 호환성을 위한 메서드
    async def get_websocket(self) -> Optional[websockets.WebSocketClientProtocol]:
        """
        웹소켓 객체 반환 (ConnectionManager 호환용)
        
        Returns:
            Optional[websockets.WebSocketClientProtocol]: 웹소켓 객체
        """
        return self.ws
        
    async def reconnect(self) -> bool:
        """
        재연결 수행 (ConnectionManager 호환용)
        
        Returns:
            bool: 재연결 성공 여부
        """
        await self.disconnect()
        return await self.connect()

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
                self.logger.error(f"{self.exchange_name_kr} 선물 {symbol} 오더북 스냅샷 가져오기 실패")
                return {}
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 선물 {symbol} 오더북 스냅샷 요청 중 오류: {str(e)}")
            return {} 