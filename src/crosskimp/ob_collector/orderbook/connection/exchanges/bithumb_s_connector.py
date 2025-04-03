"""
빗썸 현물 커넥터 모듈

빗썸 현물 거래소의 웹소켓 연결 및 오더북 데이터 처리를 담당하는 커넥터 클래스를 제공합니다.
"""

import json
import time
import asyncio
import websockets
from typing import Dict, List, Any, Optional, Union, Callable
from urllib.parse import urlencode

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR
from crosskimp.common.config.app_config import get_config
from crosskimp.common.events.system_eventbus import get_component_event_bus
from crosskimp.common.events.system_types import EventChannels

from crosskimp.ob_collector.orderbook.connection.connector_interface import ExchangeConnectorInterface
from crosskimp.ob_collector.orderbook.connection.strategies.bithumb_s_strategie import BithumbSpotConnectionStrategy
from crosskimp.ob_collector.orderbook.data_handlers.exchanges.bithumb_s_handler import BithumbSpotDataHandler
from crosskimp.ob_collector.orderbook.data_handlers.ob_data_manager import get_orderbook_data_manager

class BithumbSpotConnector(ExchangeConnectorInterface):
    """
    빗썸 현물 커넥터 클래스
    
    빗썸 현물 거래소의 웹소켓 연결 및 오더북 데이터 처리를 담당합니다.
    """
    
    def __init__(self, connection_manager=None):
        """
        초기화
        
        Args:
            connection_manager: 연결 관리자 객체 (선택 사항)
        """
        # 로거 설정
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        self.config = get_config()
        
        # 거래소 코드 및 한글 이름 설정
        self.exchange_code = Exchange.BITHUMB.value
        self.exchange_name_kr = EXCHANGE_NAMES_KR[self.exchange_code]
        
        # 연결 관리자 저장
        self.connection_manager = connection_manager
        
        # 웹소켓 연결 관련 속성
        self.ws = None
        self._is_connected = False
        self.connecting = False
        self.subscribed_symbols = set()
        
        # 종료 중 플래그 추가
        self.is_shutting_down = False
        
        # 핸들러 및 전략 객체 생성
        self.connection_strategy = BithumbSpotConnectionStrategy()
        self.data_handler = BithumbSpotDataHandler()
        
        # 데이터 관리자 - 원본 데이터 로깅용
        self.data_manager = get_orderbook_data_manager()
        
        # 콜백 저장
        self.message_callbacks = []
        self.orderbook_callbacks = []
        
        # 메시지 처리 태스크
        self.message_task = None
        self.ping_task = None
        
        # 종료 이벤트
        self.stop_event = asyncio.Event()
        
        # 이벤트 버스 추가
        self.event_bus = get_component_event_bus(SystemComponent.OB_COLLECTOR)
        
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
        # 상태가 변경된 경우에만 처리
        if self._is_connected != value:
            self._is_connected = value
            
            # 연결 관리자가 있으면 상태 업데이트
            if self.connection_manager:
                self.connection_manager.update_exchange_status(self.exchange_code, value)
        
    async def connect(self) -> bool:
        """
        거래소 웹소켓 서버에 연결
        
        Returns:
            bool: 연결 성공 여부
        """
        if self.is_connected or self.connecting:
            self.logger.warning(f"{self.exchange_name_kr} 이미 연결 중이거나 연결됨")
            return self.is_connected
            
        try:
            self.connecting = True
            self.stop_event.clear()
            
            # 연결 전략을 통해 웹소켓 연결
            self.ws = await self.connection_strategy.connect()
            
            # 웹소켓 연결 성공
            self.is_connected = True
            self.connecting = False
            
            # 메시지 처리 태스크 시작
            self.message_task = asyncio.create_task(self._message_handler())
            
            # 핑 전송 태스크 시작
            if self.connection_strategy.requires_ping():
                self.ping_task = asyncio.create_task(self._ping_handler())
            
            # 연결 후 초기화 작업 수행
            await self.connection_strategy.on_connected(self.ws)
            
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 연결 실패: {str(e)}")
            self.is_connected = False
            self.connecting = False
            return False
            
    async def disconnect(self) -> bool:
        """
        거래소 웹소켓 연결 종료
        
        Returns:
            bool: 연결 종료 성공 여부
        """
        try:
            # 종료 이벤트 설정
            self.stop_event.set()
            
            # 태스크 정리
            if self.message_task and not self.message_task.done():
                self.message_task.cancel()
                try:
                    await self.message_task
                except asyncio.CancelledError:
                    pass
            
            # 핑 태스크 정리
            if self.ping_task and not self.ping_task.done():
                self.ping_task.cancel()
                try:
                    await self.ping_task
                except asyncio.CancelledError:
                    pass
            
            # 웹소켓 종료
            if self.ws:
                await self.connection_strategy.disconnect(self.ws)
                self.ws = None
                
            # 상태 업데이트
            self.is_connected = False
            self.connecting = False
            self.subscribed_symbols.clear()
            
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 연결 종료 중 오류: {str(e)}")
            self.is_connected = False
            return False
            
    async def _message_handler(self):
        """메시지 수신 및 처리 태스크"""
        try:
            while not self.stop_event.is_set() and self.ws:
                try:
                    # 메시지 수신 - 타임아웃 5초로 줄임
                    message = await asyncio.wait_for(self.ws.recv(), timeout=5)
                    self._on_message(message)
                    
                except asyncio.TimeoutError:
                    # 타임아웃 발생 - 핑을 보내 연결 확인
                    if self.connection_strategy.requires_ping():
                        # 이제 핑 전송은 ConnectionClosed를 발생시킬 수 있어 추가 try-except 불필요
                        await self.connection_strategy.send_ping(self.ws)
                    
                except websockets.exceptions.ConnectionClosed as e:
                    if not self.is_shutting_down:
                        self.is_connected = False
                        # 이벤트 버스를 통해 연결 끊김 알림
                        await self.event_bus.publish(EventChannels.Component.ObCollector.CONNECTION_LOST, {
                            "exchange_code": self.exchange_code,
                            "exchange_name": self.exchange_name_kr,
                            "error": str(e),
                            "timestamp": time.time()
                        })
                    break
                    
                except Exception as e:
                    # 기타 예외 처리
                    if not self.is_shutting_down:
                        self.logger.error(f"{self.exchange_name_kr} 메시지 처리 중 오류: {str(e)}")
                        self.is_connected = False
                        # 이벤트 버스를 통해 연결 끊김 알림
                        await self.event_bus.publish(EventChannels.Component.ObCollector.CONNECTION_LOST, {
                            "exchange_code": self.exchange_code,
                            "exchange_name": self.exchange_name_kr,
                            "error": str(e),
                            "timestamp": time.time()
                        })
                    await asyncio.sleep(1)
                    
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
            self.logger.error(f"{self.exchange_name_kr} 구독 실패: 연결되지 않음")
            return False
            
        try:
            # 정규화된 심볼 목록
            normalized_symbols = [self.data_handler.normalize_symbol(s) for s in symbols]
            
            # 이미 구독 중인 심볼 제외
            new_symbols = [s for s in normalized_symbols if s not in self.subscribed_symbols]
            
            if not new_symbols:
                return True
                
            # 구독 요청
            self.logger.info(f"{self.exchange_name_kr} {len(new_symbols)}개 심볼 구독 중...")
            
            # 연결 전략을 통해 구독
            result = await self.connection_strategy.subscribe(self.ws, new_symbols)
            
            if result:
                # 구독 심볼 저장
                self.subscribed_symbols.update(new_symbols)
            
            return result
            
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
            # 정규화된 심볼 목록
            normalized_symbols = [self.data_handler.normalize_symbol(s) for s in symbols]
            
            # 구독 중인 심볼만 필터링
            symbols_to_unsub = [s for s in normalized_symbols if s in self.subscribed_symbols]
            
            if not symbols_to_unsub:
                return True
                
            # 연결 전략을 통해 구독 해제
            result = await self.connection_strategy.unsubscribe(self.ws, symbols_to_unsub)
            
            if result:
                # 구독 심볼 제거
                for s in symbols_to_unsub:
                    self.subscribed_symbols.discard(s)
                
            return result
            
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
            
    def _on_message(self, message: str) -> None:
        """
        웹소켓 메시지 처리
        
        Args:
            message: 수신된 웹소켓 메시지 (JSON 문자열)
        """
        try:
            # 원본 메시지 로깅 (오더북 메시지만)
            try:
                data = json.loads(message)
                if "type" in data and data.get("type") == "orderbook" and "code" in data:
                    self.data_manager.log_raw_message(self.exchange_code, message)
            except Exception:
                pass
            
            # 메시지 콜백 호출
            for callback in self.message_callbacks:
                try:
                    callback(message)
                except Exception:
                    pass
                    
            # 연결 전략을 통해 메시지 전처리
            processed_message = self.connection_strategy.preprocess_message(message)
            
            # 오더북 메시지 처리
            if processed_message.get("type") == "orderbook":
                # 데이터 핸들러를 통해 오더북 데이터 처리
                orderbook_data = self.data_handler.process_orderbook_update(processed_message)
                
                # 오더북 콜백 호출
                for callback in self.orderbook_callbacks:
                    try:
                        callback(orderbook_data)
                    except Exception:
                        pass
                        
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 처리 중 오류: {str(e)}")
            
    async def get_orderbook_snapshot(self, symbol: str) -> Dict[str, Any]:
        """
        특정 심볼의 오더북 데이터 조회
        
        Args:
            symbol: 조회할 심볼
            
        Returns:
            Dict[str, Any]: 오더북 데이터
        """
        return await self.data_handler.get_orderbook_snapshot(symbol)
            
    async def refresh_snapshots(self, symbols: List[str] = None) -> None:
        """
        오더북 스냅샷 갱신 - 빗썸은 웹소켓 연결만으로 전체 오더북 제공
        """
        # 빗썸은 웹소켓 재연결 시 최신 오더북을 자동으로 제공하므로 필요 없음
        pass
            
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
        """웹소켓을 통해 메시지 전송"""
        if not self.is_connected or not self.ws:
            return False
            
        try:
            # 메시지 형식 변환 (필요 시)
            if isinstance(message, (dict, list)):
                message = json.dumps(message)
                
            await self.ws.send(message)
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 전송 실패: {str(e)}")
            return False

    async def _ping_handler(self):
        """핑 메시지 전송 태스크"""
        try:
            ping_interval = 10  # 10초마다 핑 전송
            last_ping_time = time.time()
            
            while not self.stop_event.is_set() and self.ws:
                current_time = time.time()
                
                # 핑 전송 시간이 되었는지 확인
                if current_time - last_ping_time >= ping_interval:
                    try:
                        # 핑 전송
                        await self.connection_strategy.send_ping(self.ws)
                        last_ping_time = current_time
                    except Exception as e:
                        self.logger.error(f"{self.exchange_name_kr} 핑 전송 실패: {str(e)}")
                        # 실패 시 더 자주 재시도
                        last_ping_time = current_time - (ping_interval - 2)
                
                # 1초마다 이벤트 루프 활성화 (테스트 코드와 같은 패턴)
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            pass 