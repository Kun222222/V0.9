# file: orderbook/connection/bithumb_s_cn.py

import asyncio
import json
import time
from typing import Dict, List, Optional, Any
import websockets
from websockets import connect

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG
from crosskimp.ob_collector.orderbook.connection.base_ws_connector import BaseWebsocketConnector

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 빗썸 웹소켓 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BITHUMB.value  # 거래소 코드
BITHUMB_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 빗썸 설정

# 웹소켓 연결 설정
WS_URL = BITHUMB_CONFIG["ws_url"]  # 웹소켓 URL
PING_INTERVAL = BITHUMB_CONFIG["ping_interval"]  # 핑 전송 간격 (초)
PING_TIMEOUT = BITHUMB_CONFIG["ping_timeout"]  # 핑 응답 타임아웃 (초)

# 오더북 관련 설정
DEFAULT_DEPTH = BITHUMB_CONFIG["default_depth"]  # 기본 오더북 깊이
SYMBOL_SUFFIX = BITHUMB_CONFIG["symbol_suffix"]  # 심볼 접미사

# 빗썸 특화 설정
BITHUMB_MESSAGE_TYPE = BITHUMB_CONFIG["message_type_depth"]  # 오더북 메시지 타입

class BithumbWebSocketConnector(BaseWebsocketConnector):
    """
    빗썸 현물 웹소켓 연결 관리 클래스
    
    빗썸 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    
    특징:
    - 빗썸 전용 핑/퐁 메커니즘 사용
    - 재연결 전략 구현
    - 구독 관리
    """
    def __init__(self, settings: dict):
        """
        빗썸 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, EXCHANGE_CODE)
        
        # 웹소켓 URL 설정
        self.ws_url = WS_URL
        
        # 연결 관련 설정
        self.depth = settings.get("depth", DEFAULT_DEPTH)  # 내부 depth 설정
        self.subscribed_symbols: List[str] = []
        self.ws = None
        
        # 재연결 관련 속성 추가
        self.reconnect_count = 0
        self.last_reconnect_time = 0

    async def connect(self):
        """
        빗썸 웹소켓 서버에 연결
        
        설정된 URL로 웹소켓 연결을 수립하고, 연결 상태를 업데이트합니다.
        연결 실패 시 예외를 발생시킵니다.
        """
        try:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect_attempt")
                
            # 웹소켓 연결 수립
            self.ws = await connect(
                self.ws_url,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT
            )
            self.is_connected = True
            self.stats.connection_start_time = time.time()
            
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect")
                
            self.log_info("웹소켓 연결 성공")
            
            # 텔레그램 알림 전송
            await self._send_telegram_notification("connect", f"{self.exchange_korean_name} 웹소켓 연결 성공")
            
            return True
                
        except Exception as e:
            self.log_error(f"웹소켓 연결 중 예외 발생: {e}")
            return False

    async def subscribe(self, symbols: List[str]):
        """
        지정된 심볼 목록을 구독
        
        Args:
            symbols: 구독할 심볼 목록
        """
        if not symbols:
            self.log_error("구독할 심볼이 없음")
            return

        # 구독 심볼 목록 초기화
        self.subscribed_symbols = []

        # 구독 메시지 전송
        if symbols and self.ws and self.is_connected:
            try:
                # 빗썸 형식으로 심볼 변환 (예: BTC -> BTC_KRW)
                symbol_list = [f"{s.upper()}{SYMBOL_SUFFIX}" for s in symbols]
                # 구독 메시지 구성
                sub_msg = {
                    "type": BITHUMB_MESSAGE_TYPE,
                    "symbols": symbol_list,
                    "tickTypes": BITHUMB_CONFIG["tick_types"]
                }
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe")
                # 웹소켓을 통해 구독 메시지 전송
                await self.ws.send(json.dumps(sub_msg))
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe_complete")
                
                # 구독 심볼 목록에 추가
                self.subscribed_symbols = [s.upper() for s in symbols]
                
                self.log_info(f"구독 완료: {len(self.subscribed_symbols)}개 심볼")
            except Exception as e:
                self.log_error(f"구독 메시지 전송 중 오류: {str(e)}")
                # 연결이 끊어진 경우 재연결 필요
                self.is_connected = False
                raise

    async def _run_message_loop(self, symbols: List[str], tasks: List[asyncio.Task]) -> None:
        """
        메시지 처리 루프 실행 (부모 클래스의 메서드 오버라이드)
        
        Args:
            symbols: 구독한 심볼 목록
            tasks: 실행 중인 백그라운드 태스크 목록
        """
        # 메인 연결 및 메시지 처리 루프
        while not self.stop_event.is_set():
            try:
                # 연결 상태 확인 및 연결 시도
                if not self.is_connected:
                    if not await self._handle_connection_attempt():
                        continue
                
                # 연결 직후 last_message_time 초기화
                self.stats.last_message_time = time.time()

                # 심볼 구독 (부모 클래스의 start에서 이미 호출되었지만, 재연결 시 다시 호출 필요)
                try:
                    await self.subscribe(symbols)
                except Exception as sub_e:
                    self.log_error(f"구독 실패: {str(sub_e)}")
                    # 구독 실패 시 연결 재설정
                    self.is_connected = False
                    continue

                # 메시지 수신 루프
                last_message_time = time.time()
                while not self.stop_event.is_set() and self.is_connected:
                    try:
                        # 웹소켓 연결 상태 확인
                        if not self.ws:
                            self.log_error("웹소켓 객체가 None입니다. 재연결이 필요합니다.")
                            self.is_connected = False
                            break
                            
                        # 메시지 수신 및 처리
                        if not await self._handle_message_reception(last_message_time):
                            break
                        
                        # 마지막 메시지 시간 업데이트
                        last_message_time = time.time()

                    except asyncio.TimeoutError:
                        # 타임아웃 발생 시 연결 상태 확인
                        if not await self._handle_timeout(last_message_time):
                            break
                        continue
                    except websockets.exceptions.ConnectionClosed as e:
                        await self._handle_connection_closed(e)
                        break
                    except Exception as e:
                        self.log_error(f"메시지 수신 오류: {e}")
                        self.is_connected = False
                        break

                # 연결이 끊어진 경우 재연결 대기
                if not self.is_connected and not self.stop_event.is_set():
                    await self._prepare_reconnect()

            except Exception as e:
                self.log_error(f"연결 루프 오류: {str(e)}")
                await self._handle_loop_error()

    async def _handle_connection_attempt(self) -> bool:
        """
        연결 시도 및 실패 처리
        
        Returns:
            bool: 연결 성공 여부
        """
        connected = await self.connect()
        if not connected:
            # 연결 실패 시 지수 백오프 적용
            delay = self.reconnect_strategy.next_delay()
            self.log_error(f"연결 실패, {delay}초 후 재시도")
            await asyncio.sleep(delay)
            return False
        return True

    async def _handle_message_reception(self, last_message_time: float) -> bool:
        """
        메시지 수신 및 처리
        
        Args:
            last_message_time: 마지막 메시지 수신 시간
            
        Returns:
            bool: 계속 메시지를 수신해야 하는지 여부
        """
        # 메시지 수신
        message = await asyncio.wait_for(
            self.ws.recv(), 
            timeout=BITHUMB_CONFIG["message_timeout"]
        )
        
        # 메시지 지연 측정
        current_time = time.time()
        message_delay = current_time - last_message_time
        
        # 메시지 지연이 30초 이상이면 연결 재설정
        if message_delay > BITHUMB_CONFIG["message_timeout"]:
            self.log_error(f"높은 메시지 지연 감지: {message_delay:.1f}초 -> 연결 재설정")
            self.is_connected = False
            return False

        # 통계 업데이트
        self.stats.last_message_time = current_time
        self.stats.message_count += 1
        self.stats.total_messages += 1

        # 디버깅을 위해 메시지 내용 로깅
        if self.stats.message_count <= BITHUMB_CONFIG["log_sample_count"]:
            self.log_debug(f"수신된 메시지 샘플 (#{self.stats.message_count}): {message[:BITHUMB_CONFIG['log_message_preview_length']]}...")

        # 메시지 처리 (자식 클래스에서 구현)
        await self.process_message(message)
        return True

    async def _handle_timeout(self, last_message_time: float) -> bool:
        """
        타임아웃 처리
        
        Args:
            last_message_time: 마지막 메시지 수신 시간
            
        Returns:
            bool: 계속 메시지를 수신해야 하는지 여부
        """
        current_time = time.time()
        if current_time - last_message_time > BITHUMB_CONFIG["message_timeout"]:
            self.log_error(f"메시지 수신 타임아웃: {current_time - last_message_time:.1f}초 -> 연결 재설정")
            self.is_connected = False
            return False
        return True

    async def _handle_connection_closed(self, exception: Exception) -> None:
        """
        연결 종료 처리
        
        Args:
            exception: 연결 종료 예외
        """
        self.log_error(f"웹소켓 연결 끊김: {exception}")
        # 텔레그램 알림 전송
        await self._send_telegram_notification("error", f"빗썸 웹소켓 연결 끊김: {exception}")
        self.is_connected = False

    async def _prepare_reconnect(self) -> None:
        """재연결 준비"""
        # 재연결 카운트 증가 및 시간 기록
        self.reconnect_count += 1
        self.last_reconnect_time = time.time()
        
        # 재연결 지연 계산
        delay = self.reconnect_strategy.next_delay()
        self.log_info(f"{delay:.1f}초 후 재연결 시도 (#{self.reconnect_count})")
        
        # 연결 종료 콜백 호출
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "disconnect")
        
        # 웹소켓 객체 정리
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None
        
        # 재연결 대기
        await asyncio.sleep(delay)

    async def _handle_loop_error(self) -> None:
        """연결 루프 오류 처리"""
        # 재연결 지연
        delay = self.reconnect_strategy.next_delay()
        self.log_info(f"{delay:.1f}초 후 재연결 시도...")
        
        # 연결 상태 초기화
        self.is_connected = False
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None
        
        await asyncio.sleep(delay)

    async def process_message(self, message: str) -> None:
        """
        수신된 메시지 처리 (자식 클래스에서 구현)
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        # 이 메서드는 자식 클래스에서 구현해야 함
        pass

    async def stop(self) -> None:
        """
        웹소켓 연결 종료
        """
        self.log_info("웹소켓 연결 종료 중...")
        await super().stop()
        self.log_info("웹소켓 연결 종료 완료")

    async def subscribe_depth(self, symbol: str):
        """
        단일 심볼의 오더북 구독
        
        Args:
            symbol: 구독할 심볼
        """
        if not self.ws or not self.is_connected:
            self.log_error(f"{symbol} 웹소켓 연결이 없어 구독할 수 없음")
            return
            
        try:
            # 빗썸 형식으로 심볼 변환 (예: BTC -> BTC_KRW)
            symbol_str = f"{symbol.upper()}{SYMBOL_SUFFIX}"
            
            # 구독 메시지 구성
            sub_msg = {
                "type": BITHUMB_MESSAGE_TYPE,
                "symbols": [symbol_str],
                "tickTypes": BITHUMB_CONFIG["tick_types"]
            }
            
            # 웹소켓을 통해 구독 메시지 전송
            await self.ws.send(json.dumps(sub_msg))
            self.log_info(f"{symbol} 오더북 구독 메시지 전송 완료")
            
        except Exception as e:
            self.log_error(f"{symbol} 오더북 구독 메시지 전송 중 오류: {str(e)}")
            raise 