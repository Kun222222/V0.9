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

from crosskimp.ob_collector.orderbook.connection.connector_interface import ExchangeConnectorInterface
from crosskimp.ob_collector.orderbook.connection.strategies.bithumb_s_strategie import BithumbSpotConnectionStrategy
from crosskimp.ob_collector.orderbook.data_handlers.bithumb_s_handler import BithumbSpotDataHandler
from crosskimp.ob_collector.orderbook.data_handlers.ob_data_manager import get_orderbook_data_manager

class BithumbSpotConnector(ExchangeConnectorInterface):
    """
    빗썸 현물 커넥터 클래스
    
    빗썸 현물 거래소의 웹소켓 연결 및 오더북 데이터 처리를 담당합니다.
    ExchangeConnectorInterface를 구현하여 시스템과 일관된 방식으로 통합됩니다.
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
        
        # 구독 중인 심볼 목록
        self._subscribed_symbols = set()
        
        # 메시지 처리 태스크
        self._message_handler_task = None
        
        # self.logger.info(f"빗썸 현물 커넥터 초기화 완료")
        
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
        # 상태가 변경된 경우에만 처리
        if self._is_connected != value:
            self._is_connected = value
            
            # 연결 관리자가 있으면 상태 업데이트
            if self.connection_manager:
                self.connection_manager.update_exchange_status(self.exchange_code, value)
                
            # 이벤트 로깅
            if value:
                self.logger.info(f"🟢 {self.exchange_name_kr} 연결됨")
            else:
                self.logger.info(f"🔴 {self.exchange_name_kr} 연결 끊김")
        
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
            
            # 연결 후 초기화 작업 수행 (전략 객체에 위임)
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
            
            # 웹소켓 종료
            if self.ws:
                await self.connection_strategy.disconnect(self.ws)
                self.ws = None
                
            # 상태 업데이트
            self.is_connected = False
            self.connecting = False
            self.subscribed_symbols.clear()
            
            self.logger.info(f"{self.exchange_name_kr} 연결 종료됨")
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
                    # 메시지 수신
                    message = await asyncio.wait_for(self.ws.recv(), timeout=60)
                    
                    # 메시지 처리
                    self._on_message(message)
                    
                except asyncio.TimeoutError:
                    # 타임아웃 발생 - 연결 확인
                    self.logger.warning(f"{self.exchange_name_kr} 메시지 수신 타임아웃, 연결 확인 중...")
                    try:
                        # 웹소켓 확인
                        if self.ws and self.ws.open:
                            continue
                        else:
                            self.logger.error(f"{self.exchange_name_kr} 웹소켓 닫힘 감지, 재연결 시도...")
                            await self._reconnect()
                            break
                    except Exception as check_e:
                        self.logger.error(f"{self.exchange_name_kr} 연결 확인 중 오류: {str(check_e)}")
                        await self._reconnect()
                        break
                        
                except websockets.exceptions.ConnectionClosed as cc:
                    # 연결 종료 처리
                    self.logger.warning(f"{self.exchange_name_kr} 연결 종료됨 (코드: {cc.code}, 사유: {cc.reason}), 재연결 시도...")
                    await self._reconnect()
                    break
                    
                except Exception as e:
                    # 기타 예외 처리
                    self.logger.error(f"{self.exchange_name_kr} 메시지 처리 중 오류: {str(e)}")
                    await asyncio.sleep(1)
                    
        except asyncio.CancelledError:
            self.logger.info(f"{self.exchange_name_kr} 메시지 처리 태스크 취소됨")
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 처리 태스크 오류: {str(e)}")
            self.is_connected = False
            
    async def _reconnect(self):
        """재연결 수행"""
        if self.connecting:
            self.logger.warning(f"{self.exchange_name_kr} 이미 재연결 중...")
            return
            
        try:
            # 상태 업데이트
            self.is_connected = False
            self.connecting = True
            
            # 이전 웹소켓 정리
            if self.ws:
                try:
                    await self.ws.close()
                except:
                    pass
                self.ws = None
                
            # 잠시 대기 후 재연결
            self.logger.info(f"{self.exchange_name_kr} 재연결 시도 중...")
            await asyncio.sleep(0.5)  # 0.5초 대기
            
            # 재연결
            self.ws = await self.connection_strategy.connect()
            
            # 상태 업데이트
            self.is_connected = True
            self.connecting = False
            
            # 메시지 처리 태스크 재시작
            self.message_task = asyncio.create_task(self._message_handler())
            
            # 기존 구독 복원
            if self.subscribed_symbols:
                symbols = list(self.subscribed_symbols)
                self.logger.info(f"{self.exchange_name_kr} {len(symbols)}개 심볼 재구독 중...")
                await self.connection_strategy.subscribe(self.ws, symbols)
                
            self.logger.info(f"{self.exchange_name_kr} 재연결 완료")
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 재연결 실패: {str(e)}")
            self.is_connected = False
            self.connecting = False
            
            # 잠시 후 다시 시도
            self.logger.info(f"{self.exchange_name_kr} 1초 후 재연결 재시도...")
            await asyncio.sleep(1)
            asyncio.create_task(self._reconnect())
            
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
                self.logger.info(f"{self.exchange_name_kr} 구독할 새로운 심볼 없음")
                return True
                
            # 구독 요청
            self.logger.info(f"{self.exchange_name_kr} {len(new_symbols)}개 심볼 구독 중...")
            
            # 연결 전략을 통해 구독
            result = await self.connection_strategy.subscribe(self.ws, new_symbols)
            
            if result:
                # 구독 심볼 저장
                self.subscribed_symbols.update(new_symbols)
                self.logger.info(f"{self.exchange_name_kr} {len(new_symbols)}개 심볼 구독 성공")
            else:
                self.logger.error(f"{self.exchange_name_kr} 심볼 구독 실패")
                
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
            
            # 연결 전략을 통해 구독 해제
            result = await self.connection_strategy.unsubscribe(self.ws, symbols_to_unsub)
            
            if result:
                # 구독 심볼 제거
                for s in symbols_to_unsub:
                    self.subscribed_symbols.discard(s)
                self.logger.info(f"{self.exchange_name_kr} {len(symbols_to_unsub)}개 심볼 구독 해제 성공")
            else:
                self.logger.error(f"{self.exchange_name_kr} 심볼 구독 해제 실패")
                
            return result
            
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
            # 메시지 형식 변환 (필요 시)
            if isinstance(message, (dict, list)):
                message = json.dumps(message)
                
            # 메시지 전송
            await self.ws.send(message)
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 전송 실패: {str(e)}")
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
        
    def _on_message(self, message: str) -> None:
        """
        웹소켓 메시지 처리
        
        Args:
            message: 수신된 웹소켓 메시지 (JSON 문자열)
        """
        try:
            # 원본 메시지 로깅 추가
            try:
                # 메시지 파싱
                data = json.loads(message)
                # 메시지에 타입과 코드가 있는 경우 (오더북 메시지인 경우) 로깅
                if "type" in data and data.get("type") == "orderbook" and "code" in data:
                    # 심볼 추출
                    symbol = data.get("code", "").replace("KRW-", "").lower()
                    # 로깅 - 첫 번째 인자는 거래소 코드, 두 번째 인자는 메시지
                    self.data_manager.log_raw_message(self.exchange_code, message)
            except Exception as e:
                self.logger.error(f"{self.exchange_name_kr} 원본 메시지 로깅 중 오류: {str(e)}")
            
            # 메시지 콜백 호출
            for callback in self.message_callbacks:
                try:
                    callback(message)
                except Exception as e:
                    self.logger.error(f"{self.exchange_name_kr} 메시지 콜백 실행 중 오류: {str(e)}")
                    
            # 연결 전략을 통해 메시지 전처리
            processed_message = self.connection_strategy.preprocess_message(message)
            message_type = processed_message.get("type")
            
            # 오더북 메시지 처리
            if message_type == "orderbook":
                # 데이터 핸들러를 통해 오더북 데이터 처리
                orderbook_data = self.data_handler.process_orderbook_update(processed_message)
                
                # 오더북 콜백 호출
                for callback in self.orderbook_callbacks:
                    try:
                        callback(orderbook_data)
                    except Exception as e:
                        self.logger.error(f"{self.exchange_name_kr} 오더북 콜백 실행 중 오류: {str(e)}")
                        
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 메시지 처리 중 오류: {str(e)}")
            
    async def refresh_snapshots(self, symbols: List[str] = None) -> None:
        """
        오더북 스냅샷 갱신
        
        Args:
            symbols: 갱신할 심볼 목록 (None이면 모든 구독 중인 심볼)
        """
        # 필요한 경우 재연결을 통해 스냅샷 갱신
        if self.is_connected:
            await self.reconnect()
            
    async def get_orderbook_snapshot(self, symbol: str) -> Dict[str, Any]:
        """
        특정 심볼의 오더북 데이터 조회
        
        Args:
            symbol: 조회할 심볼
            
        Returns:
            Dict[str, Any]: 오더북 데이터
        """
        # 데이터 핸들러에서 현재 캐시된 오더북 반환
        return await self.data_handler.get_orderbook_snapshot(symbol)
            
    async def get_exchange_info(self) -> Dict[str, Any]:
        """
        구독 중인 심볼 정보 반환
        
        Returns:
            Dict[str, Any]: 심볼 정보
        """
        return {"symbols": [s for s in self.subscribed_symbols]}
            
    async def get_websocket(self) -> Optional[websockets.WebSocketClientProtocol]:
        """
        웹소켓 객체 반환
        
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
        self.logger.info(f"{self.exchange_name_kr} 수동 재연결 시작")
        await self.disconnect()
        await asyncio.sleep(0.5)
        return await self.connect() 