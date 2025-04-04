"""
업비트 현물 커넥터 모듈

업비트 현물 거래소의 웹소켓 연결 및 오더북 데이터 처리를 담당하는 커넥터 클래스를 제공합니다.
"""

import json
import time
import asyncio
import websockets
from typing import Dict, List, Any, Optional, Union, Callable
from urllib.parse import urlencode
import logging

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR
from crosskimp.common.config.app_config import get_config

from crosskimp.ob_collector.orderbook.connection.connector_interface import ExchangeConnectorInterface
from crosskimp.ob_collector.orderbook.data_handlers.exchanges.upbit_s_handler import UpbitSpotDataHandler
from crosskimp.ob_collector.orderbook.data_handlers.ob_data_manager import get_orderbook_data_manager

class UpbitSpotConnector(ExchangeConnectorInterface):
    """
    업비트 현물 커넥터 클래스
    
    업비트 현물 거래소의 웹소켓 연결 및 오더북 데이터 처리를 담당합니다.
    ExchangeConnectorInterface를 구현하여 시스템과 일관된 방식으로 통합됩니다.
    """
    
    # 상수 정의
    BASE_WS_URL = "wss://api.upbit.com/websocket/v1"
    PING_INTERVAL = 60  # 60초마다 PING 메시지 전송 (업비트 120초 타임아웃 기준으로 충분한 간격)
    PING_RESPONSE = '{"status":"UP"}'  # 업비트 핑 응답 메시지
    
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
        self.exchange_code = Exchange.UPBIT.value
        self.exchange_name_kr = EXCHANGE_NAMES_KR[self.exchange_code]
        
        # 연결 관리자 저장
        self.connection_manager = connection_manager
        
        # 웹소켓 연결 관련 속성
        self.ws = None
        self._is_connected = False
        self.connecting = False
        self.subscribed_symbols = set()
        
        # 핸들러 객체 생성
        self.data_handler = UpbitSpotDataHandler()
        
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
        
        # 메시지 핸들러 락 추가 - 동시 실행 방지
        self._message_handler_lock = asyncio.Lock()
        
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
            
            # 기존 태스크 정리
            if self.message_task and not self.message_task.done():
                self.message_task.cancel()
                self.message_task = None
                
            if self.ping_task and not self.ping_task.done():
                self.ping_task.cancel()
                self.ping_task = None
            
            # 웹소켓 연결
            retry_count = 0
            max_retries = 6  # 6번 재시도 (빗썸과 동일)
            
            while retry_count < max_retries:
                try:
                    retry_count += 1
                    # 웹소켓 연결 (5초 타임아웃 설정)
                    self.logger.debug(f"{self.exchange_name_kr} 웹소켓 연결 시도 #{retry_count}")
                    self.ws = await asyncio.wait_for(
                        websockets.connect(
                            self.BASE_WS_URL,
                            close_timeout=10,
                            ping_interval=None,  # 자체 핑 메커니즘 사용
                            ping_timeout=None
                        ),
                        timeout=5.0  # 5초 연결 타임아웃
                    )
                    self.logger.info(f"{self.exchange_name_kr} 웹소켓 연결 성공 (시도 #{retry_count})")
                    break  # 연결 성공
                
                except asyncio.TimeoutError:
                    self.logger.warning(f"{self.exchange_name_kr} 웹소켓 연결 타임아웃 (시도 #{retry_count}/{max_retries})")
                    if retry_count >= max_retries:
                        self.logger.error(f"{self.exchange_name_kr} 최대 재시도 횟수({max_retries}회) 초과")
                        raise
                    await asyncio.sleep(0.5)  # 0.5초 후 재시도 (빗썸과 동일)
                
                except Exception as e:
                    self.logger.error(f"{self.exchange_name_kr} 웹소켓 연결 실패 (시도 #{retry_count}/{max_retries}): {str(e)}")
                    if retry_count >= max_retries:
                        self.logger.error(f"{self.exchange_name_kr} 최대 재시도 횟수({max_retries}회) 초과")
                        raise
                    await asyncio.sleep(0.5)  # 0.5초 후 재시도 (빗썸과 동일)
            
            # 연결 성공 처리
            self.is_connected = True
            self.connecting = False
            
            # 메시지 처리 및 핑 태스크 시작
            self.message_task = asyncio.create_task(self._message_handler())
            self.ping_task = asyncio.create_task(self._ping_sender())
            
            # 기존 구독이 있으면 다시 구독
            if self.subscribed_symbols:
                await self.subscribe(list(self.subscribed_symbols))
            
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
                self.message_task = None
                
            if self.ping_task and not self.ping_task.done():
                self.ping_task.cancel()
                self.ping_task = None
            
            # 웹소켓 종료
            if self.ws:
                await self.ws.close()
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
    
    async def _ping_sender(self):
        """
        업비트 연결 유지를 위한 PING 메시지 전송 태스크
        업비트 공식 문서에 따라 텍스트 "PING" 메시지를 주기적으로 전송
        """
        try:
            while not self.stop_event.is_set() and self.ws:
                try:
                    if self.is_connected:
                        # 업비트 공식 문서에 따라 "PING" 문자열 전송
                        await self.ws.send("PING")
                    
                    # PING_INTERVAL 대기
                    await asyncio.sleep(self.PING_INTERVAL)
                    
                except Exception as e:
                    self.logger.error(f"{self.exchange_name_kr} PING 전송 중 오류: {str(e)}")
                    await asyncio.sleep(5)  # 오류 발생 시 잠시 대기 후 재시도
                    
        except asyncio.CancelledError:
            pass
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} PING 전송 태스크 오류: {str(e)}")
            
    async def _message_handler(self):
        """메시지 수신 및 처리 태스크"""
        try:
            async with self._message_handler_lock:
                self.logger.debug(f"{self.exchange_name_kr} 메시지 핸들러 시작")
                
                while not self.stop_event.is_set() and self.ws:
                    try:
                        # 메시지 수신
                        message = await self.ws.recv()
                        
                        # PING 응답 확인
                        if message == self.PING_RESPONSE:
                            self.logger.debug(f"{self.exchange_name_kr} PING 응답 수신")
                            continue
                        
                        # 메시지 처리
                        self._on_message(message)
                        
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
            
    async def subscribe(self, symbols: List[str], prices: Dict[str, float] = None) -> bool:
        """
        심볼 목록 구독
        
        Args:
            symbols: 구독할 심볼 목록
            prices: 심볼별 가격 정보 (사용 안 함)
            
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
            # INFO -> DEBUG로 수준 낮춤 (중복 로그 제거)
            self.logger.debug(f"{self.exchange_name_kr} {len(symbols_to_subscribe)}개 심볼 구독 중...")
            
            # 구독 메시지 생성
            subscribe_msg = self._create_subscription_message(symbols_to_subscribe)
            
            # 구독 요청 전송
            # INFO -> DEBUG로 수준 낮춤 (중복 로그 제거)
            self.logger.debug(f"{self.exchange_name_kr} 구독 요청: {len(symbols_to_subscribe)}개 심볼")
            
            await self.ws.send(json.dumps(subscribe_msg))
            
            # 구독 심볼 저장
            self.subscribed_symbols.update(symbols_to_subscribe)
            # INFO -> DEBUG로 수준 낮춤 (중복 로그 제거)
            self.logger.debug(f"{self.exchange_name_kr} {len(symbols_to_subscribe)}개 심볼 구독 성공")
            
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 구독 중 오류: {str(e)}")
            return False
            
    def _create_subscription_message(self, symbols: List[str]) -> List[Dict[str, Any]]:
        """
        구독 메시지 생성
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            List[Dict[str, Any]]: 구독 메시지
        """
        # 심볼 형식 변환 (KRW-BTC 형식으로 변경)
        formatted_symbols = [f"KRW-{s.upper()}" for s in symbols]
        
        # 업비트 구독 메시지 형식
        ticket = f"upbit-ticket-{int(time.time() * 1000)}"
        
        # 구독 메시지 생성
        subscribe_msg = [
            {"ticket": ticket},
            {"type": "orderbook", "codes": formatted_symbols},
            {"format": "DEFAULT"}
        ]
        
        return subscribe_msg
            
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
            
            # 업비트는 별도의 구독 해제 메시지가 없으므로 구독 목록에서만 제거하고 재연결 시 필요한 것만 구독
            for s in symbols_to_unsub:
                self.subscribed_symbols.discard(s)
                
            self.logger.info(f"{self.exchange_name_kr} {len(symbols_to_unsub)}개 심볼 구독 해제 성공 (내부적으로만 처리됨)")
            
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
        
    def _on_message(self, message: Union[str, bytes]) -> None:
        """
        웹소켓 메시지 처리
        
        Args:
            message: 수신된 웹소켓 메시지 (JSON 문자열 또는 바이트)
        """
        try:
            # 원본 메시지 로깅 최적화
            # config.debug_mode 대신 logging.debug_logging 설정 사용
            debug_logging = self.config.get_system("logging.debug_logging", False)
            
            # 바이트 메시지를 문자열로 변환하지 않고 키워드 확인 (필요한 경우에만 디코딩)
            if debug_logging:
                msg_for_check = message
                if isinstance(message, bytes):
                    try:
                        # 로깅 목적으로만 디코딩 (실제 처리는 핸들러에서 함)
                        msg_for_check = message.decode('utf-8')
                    except:
                        # 디코딩 오류가 발생하면 로깅하지 않음
                        pass
                
                # 문자열에서 키워드 확인 후 로깅
                if isinstance(msg_for_check, str) and '"type":"orderbook"' in msg_for_check and '"code":"KRW-' in msg_for_check:
                    # 디버그 로깅이 활성화된 경우에만 원본 메시지 로깅
                    # 바이트는 그대로 전달 (log_raw_message가 처리하도록)
                    self.data_manager.log_raw_message(self.exchange_code, message)
            
            # 메시지 콜백 호출
            for callback in self.message_callbacks:
                try:
                    callback(message)
                except Exception as e:
                    self.logger.error(f"{self.exchange_name_kr} 메시지 콜백 실행 중 오류: {str(e)}")
            
            # 핸들러의 process_message 메서드를 직접 호출하여 메시지 처리
            orderbook_data = self.data_handler.process_message(message)
            
            # 오더북 데이터가 있는 경우에만 콜백 호출
            if orderbook_data:
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
        # 업비트는 웹소켓 재연결 시 최신 오더북을 자동으로 제공하므로 필요 없음
        self.logger.info(f"{self.exchange_name_kr} 스냅샷 갱신 요청 (재연결로 대체)")
        
        # 연결 관리자가 재연결을 처리하므로 별도 로직 불필요
        pass
            
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
        거래소 정보 조회 (심볼 정보 등)
        
        Returns:
            Dict[str, Any]: 거래소 정보
        """
        # 구독 중인 심볼 정보만 반환
        return {"symbols": [s for s in self.subscribed_symbols]}
            
    async def get_websocket(self) -> Optional[websockets.WebSocketClientProtocol]:
        """
        웹소켓 객체 반환
        
        Returns:
            Optional[websockets.WebSocketClientProtocol]: 웹소켓 객체
        """
        return self.ws 