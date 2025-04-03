"""
바이빗 선물 커넥터 모듈

바이빗 선물 거래소에 대한 웹소켓 연결 및 데이터 처리를 담당하는 커넥터를 제공합니다.
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
from crosskimp.common.events.system_eventbus import get_component_event_bus
from crosskimp.common.events.system_types import EventChannels

from crosskimp.ob_collector.orderbook.connection.connector_interface import ExchangeConnectorInterface
from crosskimp.ob_collector.orderbook.connection.strategies.bybit_f_strategie import BybitFutureConnectionStrategy
from crosskimp.ob_collector.orderbook.data_handlers.exchanges.bybit_f_handler import BybitFutureDataHandler

class BybitFutureConnector(ExchangeConnectorInterface):
    """
    바이빗 선물 커넥터 클래스
    
    바이빗 선물 거래소의 웹소켓 연결 및 데이터 처리를 담당합니다.
    """
    
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
        self.exchange_code = Exchange.BYBIT_FUTURE.value
        self.exchange_name_kr = EXCHANGE_NAMES_KR[self.exchange_code]
        
        # 연결 관리자 저장
        self.connection_manager = connection_manager
        
        # 연결 전략 및 데이터 핸들러 초기화
        self.connection_strategy = BybitFutureConnectionStrategy()
        self.data_handler = BybitFutureDataHandler()
        
        # 웹소켓 연결 객체
        self.ws = None
        
        # 핑 전송 타이머 (연결 유지를 위한 타이머)
        self.ping_task = None
        self._ping_interval = 20  # 핑 전송 간격 (초)
        
        # 연결 상태 및 중지 설정
        self._connected = False
        self._stopping = False
        
        # 메시지 및 오더북 콜백
        self.message_callbacks = []
        self.orderbook_callbacks = []
        
        # 구독 중인 심볼 목록
        self._subscribed_symbols = set()
        
        # 메시지 처리 태스크
        self._message_handler_task = None
        
        # 메시지 핸들러 락 추가 - 동시 실행 방지
        self._message_handler_lock = asyncio.Lock()
        
        # 이벤트 버스 추가
        self.event_bus = get_component_event_bus(SystemComponent.OB_COLLECTOR)
        
        # self.logger.info(f"바이빗 선물 커넥터 초기화 완료")
    
    @property
    def is_connected(self) -> bool:
        """
        현재 연결 상태 반환
        
        Returns:
            bool: 연결 상태
        """
        return self._connected and self.ws is not None and not self.ws.closed
        
    @is_connected.setter
    def is_connected(self, value: bool):
        """
        연결 상태 설정
        
        Args:
            value: 연결 상태 (True: 연결됨, False: 연결 안됨)
        """
        old_value = self._connected
        self._connected = value
        
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
                    
                    # 웹소켓 연결 수립
                    self.ws = await self.connection_strategy.connect()
                    
                    # 연결 상태 업데이트
                    self.is_connected = True
                    
                    # 연결 후 설정
                    await self.connection_strategy.on_connected(self.ws)
                    
                    # 메시지 처리 태스크 시작
                    if self._message_handler_task is None or self._message_handler_task.done():
                        self._message_handler_task = asyncio.create_task(self._message_handler())
                        
                    # 핑 전송 태스크 시작 (바이빗은 직접 핑 전송 필요)
                    if self.connection_strategy.requires_ping():
                        self._start_ping_task()
                        
                    self.logger.info(f"{self.exchange_name_kr} 웹소켓 연결 성공 (시도 {attempt_count}/{max_attempts})")
                    return True
                    
                except Exception as e:
                    if attempt_count == max_attempts:
                        self.logger.error(f"{self.exchange_name_kr} 웹소켓 연결 실패: {str(e)} (최대 시도 횟수 도달)")
                    else:
                        # self.logger.debug(f"{self.exchange_name_kr} 연결 시도 {attempt_count} 실패: {str(e)}")
                        pass
                    
                    # 마지막 시도가 아니면 1초 대기 후 재시도
                    if attempt_count < max_attempts:
                        await asyncio.sleep(1)
            
            # 모든 시도 실패
            self.is_connected = False
            self.logger.error(f"{self.exchange_name_kr} 60회 연결 시도 후 연결 실패")
            return False
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 연결 프로세스 오류: {str(e)}")
            self.is_connected = False
            return False

    async def _reconnect(self):
        """
        연결 재시도 로직은 이벤트 버스로 대체됨
        """
        pass

    async def _message_handler(self):
        """웹소켓 메시지 수신 및 처리 루프"""
        try:
            # 락을 사용하여 동시에 여러 태스크가 메시지 핸들러를 실행하지 않도록 함
            async with self._message_handler_lock:
                self.logger.debug(f"{self.exchange_name_kr} 선물 메시지 핸들러 시작")
                
                while not self._stopping and self.is_connected and self.ws:
                    try:
                        # 메시지 수신
                        message = await self.ws.recv()
                        
                        # JSON 파싱 (문자열인 경우만)
                        if isinstance(message, str):
                            try:
                                parsed_message = json.loads(message)
                            except:
                                parsed_message = message
                        else:
                            parsed_message = message
                            
                        # 핑퐁 메시지 처리 (다양한 퐁 응답 형식 고려)
                        if isinstance(parsed_message, dict) and (
                                parsed_message.get("op") == "pong" or 
                                parsed_message.get("ret_msg") == "pong"):
                            self.logger.debug(f"{self.exchange_name_kr} 선물 퐁 응답 수신")
                            continue
                        
                        # 전처리
                        processed_message = self.connection_strategy.preprocess_message(message)
                        
                        # 메시지 타입 확인
                        msg_type = processed_message.get("type", "unknown")
                            
                        # 구독 응답 처리
                        if msg_type == "subscription_response":
                            self.logger.info(f"{self.exchange_name_kr} 선물 구독 응답: {processed_message['data']}")
                            continue
                            
                        # 오더북 데이터 처리 - snapshot, delta 타입도 처리하도록 추가
                        elif msg_type in ["depth_update", "snapshot", "delta"]:
                            self._on_message(processed_message)
                            
                        # 기타 메시지
                        else:
                            self.logger.debug(f"{self.exchange_name_kr} 선물 기타 메시지: {msg_type}, 내용: {json.dumps(processed_message['data'], ensure_ascii=False)[:200]}")
                        
                    except websockets.exceptions.ConnectionClosed as e:
                        self.logger.warning(f"{self.exchange_name_kr} 선물 웹소켓 연결 끊김: {str(e)}")
                        self.is_connected = False
                        
                        # 이벤트 버스를 통해 연결 끊김 알림
                        if not self._stopping:
                            await self.event_bus.publish(EventChannels.Component.ObCollector.CONNECTION_LOST, {
                                "exchange_code": self.exchange_code,
                                "exchange_name": self.exchange_name_kr,
                                "error": str(e),
                                "timestamp": time.time()
                            })
                        break
                        
                    except asyncio.CancelledError:
                        self.logger.info(f"{self.exchange_name_kr} 선물 메시지 핸들러 태스크 취소됨")
                        break
                        
                    except Exception as e:
                        self.logger.error(f"{self.exchange_name_kr} 선물 메시지 처리 중 오류: {str(e)}")
                        await asyncio.sleep(1)  # 오류 발생 시 잠시 대기 후 계속
                        
                self.logger.debug(f"{self.exchange_name_kr} 선물 메시지 핸들러 종료")
        except asyncio.CancelledError:
            self.logger.info(f"{self.exchange_name_kr} 선물 메시지 핸들러 태스크 취소됨")
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 선물 메시지 핸들러 태스크 오류: {str(e)}")
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
            
            # 구독 요청
            success = await self.connection_strategy.subscribe(self.ws, symbols_to_subscribe)
            
            if success:
                # 성공한 심볼 저장
                for symbol in symbols_to_subscribe:
                    self._subscribed_symbols.add(symbol)
                    
                self.logger.info(f"{self.exchange_name_kr} 선물 구독 성공: {len(symbols_to_subscribe)}개 심볼")
                return True
            else:
                self.logger.error(f"{self.exchange_name_kr} 선물 구독 실패")
                return False
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 선물 구독 중 오류: {str(e)}")
            return False
            
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
            # 구독 해제 요청
            success = await self.connection_strategy.unsubscribe(self.ws, symbols)
            
            if success:
                # 구독 목록에서 제거
                for symbol in symbols:
                    if symbol in self._subscribed_symbols:
                        self._subscribed_symbols.remove(symbol)
                        
                self.logger.info(f"{self.exchange_name_kr} 선물 구독 해제 성공: {len(symbols)}개 심볼")
                return True
            else:
                self.logger.error(f"{self.exchange_name_kr} 선물 구독 해제 실패")
                return False
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 선물 구독 해제 중 오류: {str(e)}")
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
            self.logger.error(f"{self.exchange_name_kr} 선물 메시지 전송 중 오류: {str(e)}")
            # 연결 문제 감지 시 이벤트 버스로 알림
            if not self._stopping:
                asyncio.create_task(self.event_bus.publish(EventChannels.Component.ObCollector.CONNECTION_LOST, {
                    "exchange_code": self.exchange_code,
                    "exchange_name": self.exchange_name_kr,
                    "error": f"메시지 전송 오류: {str(e)}",
                    "timestamp": time.time()
                }))
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
            
            # 오더북 업데이트 메시지 처리
            if msg_type in ["depth_update", "snapshot", "delta"]:
                # 심볼 확인
                symbol = message.get("symbol", "").replace("usdt", "").lower()
                
                # 데이터 핸들러에서 처리
                orderbook_data = self.data_handler.process_orderbook_update(message)
                
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
                
            # 일반 메시지 콜백 호출
            for callback in self.message_callbacks:
                try:
                    callback(message)
                except Exception as cb_error:
                    self.logger.error(f"{self.exchange_name_kr} 메시지 콜백 호출 중 오류: {str(cb_error)}")
                    
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 선물 메시지 처리 중 오류: {str(e)}")
            
    async def refresh_snapshots(self, symbols: List[str] = None) -> None:
        """
        오더북 스냅샷 갱신
        
        웹소켓을 통해 오더북 스냅샷을 새로 구독하여 갱신합니다.
        이 방법은 심볼을 다시 구독하여 새로운 스냅샷이 전달되도록 합니다.
        
        Args:
            symbols: 갱신할 심볼 목록 (None이면 모든 구독 중인 심볼)
        """
        # 심볼 목록 선택
        target_symbols = symbols if symbols else list(self._subscribed_symbols)
        if not target_symbols:
            return
            
        try:
            self.logger.info(f"{self.exchange_name_kr} 웹소켓을 통한 오더북 스냅샷 갱신 시작 ({len(target_symbols)}개 심볼)")
            
            # 웹소켓 연결 확인
            if not self.is_connected:
                if not await self.connect():
                    self.logger.error("웹소켓 연결이 없어 스냅샷 갱신이 불가능합니다.")
                    return
                    
            # 구독 해제 후 재구독
            if self.ws:
                # 먼저 구독 해제
                await self.connection_strategy.unsubscribe(self.ws, target_symbols)
                await asyncio.sleep(0.5)  # 약간의 지연
                
                # 재구독
                await self.connection_strategy.subscribe(self.ws, target_symbols)
                self.logger.info(f"{self.exchange_name_kr} 스냅샷 갱신을 위한 재구독 완료 ({len(target_symbols)}개 심볼)")
            else:
                self.logger.error("웹소켓 연결이 없어 스냅샷 갱신이 불가능합니다.")
                
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 오더북 스냅샷 갱신 중 오류: {str(e)}")
            
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
            
    async def _ping_loop(self):
        """
        주기적으로 핑 메시지를 전송하는 루프
        바이빗은 20초마다 ping 메시지를 전송해야 합니다.
        """
        PING_INTERVAL = 20  # 핑 전송 간격 (초)
        
        self.logger.info(f"{self.exchange_name_kr} 핑 루프 시작 (간격: {PING_INTERVAL}초)")
        
        try:
            while not self._stopping and self.is_connected:
                try:
                    # 핑 전송 전 연결 상태 확인
                    if self.ws is None or self.ws.closed:
                        self.logger.warning(f"{self.exchange_name_kr} 웹소켓이 닫혀 있어 핑 전송 중단")
                        self.is_connected = False
                        await self.event_bus.publish(EventChannels.Component.ObCollector.CONNECTION_LOST, {
                            "exchange_code": self.exchange_code,
                            "exchange_name": self.exchange_name_kr,
                            "error": "웹소켓이 닫힌 상태에서 핑 전송 시도",
                            "timestamp": time.time()
                        })
                        break
                    
                    # 핑 전송 (이제 ConnectionClosed를 발생시킬 수 있음)
                    await self.connection_strategy.send_ping(self.ws)
                    
                    # 핑 간격만큼 대기
                    await asyncio.sleep(PING_INTERVAL)
                    
                except websockets.exceptions.ConnectionClosed as e:
                    # 연결 끊김 감지
                    self.logger.warning(f"{self.exchange_name_kr} 핑 전송 중 연결 끊김: {str(e)}")
                    self.is_connected = False
                    
                    # 이벤트 버스를 통해 연결 끊김 알림
                    await self.event_bus.publish(EventChannels.Component.ObCollector.CONNECTION_LOST, {
                        "exchange_code": self.exchange_code,
                        "exchange_name": self.exchange_name_kr,
                        "error": f"핑 전송 중 연결 끊김: {str(e)}",
                        "timestamp": time.time()
                    })
                    break
                    
                except Exception as e:
                    # 기타 예외
                    self.logger.error(f"{self.exchange_name_kr} 핑 루프 오류: {str(e)}")
                    await asyncio.sleep(1)  # 짧은 대기 후 재시도
                
        except asyncio.CancelledError:
            self.logger.info(f"{self.exchange_name_kr} 핑 루프 취소됨")
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 핑 루프 크리티컬 오류: {str(e)}")
            
        self.logger.info(f"{self.exchange_name_kr} 핑 루프 종료")
        
    async def get_orderbook_snapshot(self, symbol: str) -> Dict[str, Any]:
        """
        특정 심볼의 오더북 스냅샷 조회
        
        현재 메모리에 있는 오더북 데이터를 반환합니다.
        웹소켓을 통해 스냅샷을 받으려면 구독을 통해 시작해야 합니다.
        
        Args:
            symbol: 조회할 심볼
            
        Returns:
            Dict[str, Any]: 오더북 스냅샷 데이터
        """
        # 현재 메모리에 있는 오더북 데이터 반환
        snapshot = self.data_handler.get_orderbook(symbol)
        
        # 스냅샷이 없으면 로그 남기고 None 반환
        if not snapshot:
            self.logger.info(f"[{symbol}] 스냅샷 데이터 없음. 구독을 통해 스냅샷을 받아야 합니다.")
            
        return snapshot 

    async def disconnect(self) -> bool:
        """
        거래소 웹소켓 연결 종료
        
        Returns:
            bool: 연결 종료 성공 여부
        """
        self._stopping = True
        
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
                await self.connection_strategy.disconnect(self.ws)
                self.ws = None
                
            # 연결 상태 업데이트
            self.is_connected = False
            
            # 구독 목록 초기화
            self._subscribed_symbols.clear()
            
            self.logger.info(f"{self.exchange_name_kr} 웹소켓 연결 종료됨")
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 웹소켓 연결 종료 중 오류: {str(e)}")
            return False
        finally:
            self._stopping = False

    def _stop_ping_task(self):
        """핑 전송 태스크 중지"""
        if self.ping_task and not self.ping_task.done():
            self.ping_task.cancel() 