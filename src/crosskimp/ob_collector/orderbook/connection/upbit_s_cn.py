# file: orderbook/connection/upbit_s_cn.py

import asyncio
import time
import json
from websockets import connect
import websockets.exceptions
from typing import Dict, Optional, Tuple, Any

from crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector, ReconnectStrategy
from crosskimp.ob_collector.orderbook.metric.metrics_manager import WebsocketMetricsManager
from crosskimp.config.constants_v3 import Exchange, EXCHANGE_NAMES_KR

# ============================
# 업비트 웹소켓 연결 관련 상수
# ============================
# 거래소 정보 - Exchange 열거형 사용
# 거래소 코드 대문자로 통일 부분 제거

# 웹소켓 연결 설정
WS_URL = "wss://api.upbit.com/websocket/v1"  # 웹소켓 URL
PING_INTERVAL = 30  # 핑 전송 간격 (초)
PING_TIMEOUT = 10  # 핑 응답 타임아웃 (초)
MESSAGE_TIMEOUT = 60  # 메시지 타임아웃 (초)
HEALTH_CHECK_INTERVAL = 30  # 헬스 체크 간격 (초)

class UpbitWebSocketConnector(BaseWebsocketConnector):
    """
    업비트 웹소켓 연결 관리 클래스
    
    업비트 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    
    특징:
    - URL: wss://api.upbit.com/websocket/v1
    - 핑/퐁: 표준 WebSocket PING/PONG 프레임 사용
    
    책임:
    - 웹소켓 연결 관리 (연결, 재연결, 종료)
    - 연결 상태 모니터링
    """
    def __init__(self, settings: dict):
        """
        업비트 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, Exchange.UPBIT.value)
        self.ws_url = WS_URL
        
        # 업비트 전용 설정
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        self.message_timeout = MESSAGE_TIMEOUT
        self.health_check_interval = HEALTH_CHECK_INTERVAL
        
        # 재연결 전략 설정
        self.reconnect_strategy = ReconnectStrategy(
            initial_delay=1.0,
            max_delay=60.0,
            multiplier=2.0,
            max_attempts=0  # 무제한 재시도
        )
        
        # 메트릭 매니저 싱글톤 인스턴스 사용
        self.metrics = WebsocketMetricsManager.get_instance()
        
        # 연결 중 상태 관리 (is_connected와 별개)
        self.connecting = False
        
        # 헬스 체크 태스크
        self.health_check_task = None

    # 웹소켓 연결 관리
    # ==================================
    async def connect(self) -> bool:
        """
        웹소켓 연결 수행
        
        Returns:
            bool: 연결 성공 여부
        """
        # 이미 연결된 경우 바로 반환
        if self.is_connected and self.ws:
            return True
            
        # 연결 중인 경우 대기
        if self.connecting:
            self.log_debug("이미 연결 중")
            return True
            
        self.connecting = True
        
        try:
            # 웹소켓 연결 수립 - 표준 PING/PONG 프레임 사용
            self.ws = await connect(
                self.ws_url,
                ping_interval=self.ping_interval,  # 표준 PING 프레임 자동 전송
                ping_timeout=self.ping_timeout     # PONG 응답 타임아웃
            )
            
            # 연결 성공 처리 - 부모 클래스의 setter 사용
            self.is_connected = True
            self.stats.connection_start_time = time.time()
            self.reconnect_strategy.reset()
            
            # 연결 성공 알림
            connect_msg = "웹소켓 연결 성공"
            await self.send_telegram_notification("connect", connect_msg)
            
            # 헬스 체크 시작
            if not self.health_check_task or self.health_check_task.done():
                self.health_check_task = asyncio.create_task(self.health_check())
            
            self.log_info("웹소켓 연결 성공")
            return True
            
        except asyncio.TimeoutError as e:
            self.connecting = False
            self.log_error(f"연결 타임아웃: {str(e)}")
            
            # 연결 실패 처리
            self.is_connected = False
            return False
                
        except Exception as e:
            self.log_error(f"연결 오류: {str(e)}", exc_info=True)
            self.connecting = False
            
            # 연결 실패 처리
            self.is_connected = False
            return False
            
        finally:
            self.connecting = False

    async def disconnect(self) -> bool:
        """
        웹소켓 연결 종료
        
        Returns:
            bool: 종료 성공 여부
        """
        try:
            # 헬스 체크 태스크 취소
            if self.health_check_task and not self.health_check_task.done():
                self.health_check_task.cancel()
                try:
                    await self.health_check_task
                except asyncio.CancelledError:
                    pass
            
            if self.ws:
                await self.ws.close()
            
            # 연결 상태 업데이트
            self.is_connected = False
            return True
            
        except Exception as e:
            self.log_error(f"웹소켓 연결 종료 실패: {str(e)}")
            return False

    async def reconnect(self) -> bool:
        """
        웹소켓 재연결
        
        Returns:
            bool: 재연결 성공 여부
        """
        try:
            self.stats.reconnect_count += 1
            reconnect_msg = f"웹소켓 재연결 시도 중 (시도 횟수: {self.stats.reconnect_count})"
            self.log_info(reconnect_msg)
            await self.send_telegram_notification("reconnect", reconnect_msg)
            
            await self.disconnect()
            
            # 재연결 지연 시간 계산
            delay = self.reconnect_strategy.next_delay()
            self.log_info(f"재연결 대기: {delay:.1f}초")
            await asyncio.sleep(delay)
            
            success = await self.connect()
            
            if success:
                self.log_info("웹소켓 재연결 성공")
            else:
                self.log_error("웹소켓 재연결 실패")
            
            return success
            
        except Exception as e:
            self.log_error(f"웹소켓 재연결 실패: {str(e)}")
            return False

    # 상태 모니터링
    # ==================================
    async def health_check(self) -> None:
        """
        웹소켓 상태 체크 (백그라운드 태스크)
        """
        self.log_info(f"헬스 체크 시작 (간격: {self.health_check_interval}초)")
        
        while not self.stop_event.is_set() and self.is_connected:
            try:
                # 연결 상태 디버그 로깅은 1분에 한 번 정도만 출력 (타임스탬프 기준)
                current_time = time.time()
                if self.is_connected and (current_time % 60) < 1:
                    uptime = current_time - self.stats.connection_start_time
                    self.log_debug(
                        f"헬스 체크: 연결됨, 업타임={uptime:.1f}초, "
                        f"오류={self.stats.error_count}개"
                    )
                
                await asyncio.sleep(self.health_check_interval)
                
            except asyncio.CancelledError:
                self.log_info("헬스 체크 태스크 취소됨")
                break
                
            except Exception as e:
                self.log_error(f"웹소켓 상태 체크 중 오류 발생: {str(e)}")
                await asyncio.sleep(1)