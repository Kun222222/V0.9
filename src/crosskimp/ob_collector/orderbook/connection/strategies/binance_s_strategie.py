"""
바이낸스 현물 웹소켓 연결 전략 모듈

바이낸스 현물 거래소에 대한 웹소켓 연결 관리 전략을 제공합니다.
"""

import json
import time
import asyncio
import websockets
from typing import Dict, List, Any, Optional, Union
import logging

from crosskimp.common.config.app_config import get_config
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR

class BinanceSpotConnectionStrategy:
    """
    바이낸스 현물 웹소켓 연결 전략 클래스
    
    바이낸스 현물 거래소의 웹소켓 연결, 구독, 메시지 처리 등을 담당합니다.
    """
    
    # 상수 정의
    BASE_WS_URL = "wss://stream.binance.com/ws"
    BASE_REST_URL = "https://api.binance.com"
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    EXCHANGE_CODE = Exchange.BINANCE_SPOT.value
    
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        self.config = get_config()
        self.subscriptions = []
        self.id_counter = 1  # 요청 ID 카운터
        self.exchange_name = EXCHANGE_NAMES_KR.get(self.EXCHANGE_CODE, "[바이낸스 현물]")
        # self.logger.info(f"{self.exchange_name} 연결 전략 초기화")
        
    def get_ws_url(self) -> str:
        """웹소켓 URL 반환"""
        return self.BASE_WS_URL
        
    def get_rest_url(self) -> str:
        """REST API URL 반환"""
        return self.BASE_REST_URL
        
    def requires_ping(self) -> bool:
        """바이낸스는 클라이언트 측에서 ping이 필요하지 않음"""
        return False
        
    async def connect(self, timeout: int = 30) -> websockets.WebSocketClientProtocol:
        """
        웹소켓 연결 수립
        
        Args:
            timeout: 연결 타임아웃 (초)
            
        Returns:
            websockets.WebSocketClientProtocol: 웹소켓 연결 객체
        """
        # self.logger.info(f"{self.exchange_name} 웹소켓 연결 시도 중: {self.BASE_WS_URL}")
        
        try:
            # 웹소켓 연결 (websockets 버전에 따라 헤더 설정 방식이 다름)
            # 버전 호환성을 위해 더 단순한 방법 사용
            ws = await asyncio.wait_for(
                websockets.connect(
                    self.BASE_WS_URL,
                    ping_interval=30,  # 20초에서 180초(3분)로 변경
                    ping_timeout=20,    # 10초에서 60초로 변경
                    close_timeout=10    # 닫기 타임아웃 (초)
                ),
                timeout=timeout
            )
            
            self.logger.info(f"{self.exchange_name} 웹소켓 연결 성공")
            return ws
            
        except asyncio.TimeoutError:
            self.logger.error(f"{self.exchange_name} 웹소켓 연결 타임아웃 ({timeout}초)")
            raise
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name} 웹소켓 연결 실패: {str(e)}")
            raise
            
    async def disconnect(self, ws: websockets.WebSocketClientProtocol) -> None:
        """
        웹소켓 연결 종료
        
        Args:
            ws: 웹소켓 연결 객체
        """
        if ws:
            try:
                await ws.close()
                self.logger.info(f"{self.exchange_name} 웹소켓 연결 종료됨")
            except Exception as e:
                self.logger.error(f"{self.exchange_name} 웹소켓 연결 종료 중 오류: {str(e)}")
                
    async def on_connected(self, ws: websockets.WebSocketClientProtocol) -> None:
        """
        연결 후 초기화 작업 수행
        
        Args:
            ws: 웹소켓 연결 객체
        """
        # 기존 구독이 있으면 다시 구독
        if self.subscriptions:
            await self.subscribe(ws, self.subscriptions)
            
    async def subscribe(self, ws: websockets.WebSocketClientProtocol, symbols: List[str]) -> bool:
        """
        심볼 구독
        
        Args:
            ws: 웹소켓 연결 객체
            symbols: 구독할 심볼 목록
            
        Returns:
            bool: 구독 성공 여부
        """
        if not ws:
            self.logger.error(f"{self.exchange_name} 심볼 구독 실패: 웹소켓 연결 없음")
            return False
            
        try:
            # 심볼 형식 변환 (모두 소문자로)
            formatted_symbols = [s.lower() + 'usdt' if not s.lower().endswith('usdt') else s.lower() for s in symbols]
            
            # 심볼별로 depth 스트림 구독
            params = [f"{symbol}@depth@100ms" for symbol in formatted_symbols]
            
            # 구독 메시지 생성
            request_id = self._get_next_id()
            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": params,
                "id": request_id
            }
            
            # 구독 요청 전송
            self.logger.info(f"{self.exchange_name} 구독 요청: {len(params)}개 심볼")
            self.logger.debug(f"{self.exchange_name} 구독 요청 상세: {subscribe_msg}")
            
            await ws.send(json.dumps(subscribe_msg))
            
            # 구독 목록 저장
            self.subscriptions = symbols
            
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name} 구독 중 오류: {str(e)}")
            return False
            
    async def unsubscribe(self, ws: websockets.WebSocketClientProtocol, symbols: List[str]) -> bool:
        """
        심볼 구독 해제
        
        Args:
            ws: 웹소켓 연결 객체
            symbols: 구독 해제할 심볼 목록
            
        Returns:
            bool: 구독 해제 성공 여부
        """
        if not ws:
            self.logger.error(f"{self.exchange_name} 구독 해제 실패: 웹소켓 연결 없음")
            return False
            
        try:
            # 심볼 형식 변환 (모두 소문자로)
            formatted_symbols = [s.lower() + 'usdt' if not s.lower().endswith('usdt') else s.lower() for s in symbols]
            
            # 심볼별로 depth 스트림 구독 해제
            params = [f"{symbol}@depth@100ms" for symbol in formatted_symbols]
            
            # 구독 해제 메시지 생성
            request_id = self._get_next_id()
            unsubscribe_msg = {
                "method": "UNSUBSCRIBE",
                "params": params,
                "id": request_id
            }
            
            # 구독 해제 요청 전송
            self.logger.info(f"{self.exchange_name} 구독 해제 요청: {len(params)}개 심볼")
            self.logger.debug(f"{self.exchange_name} 구독 해제 요청 상세: {unsubscribe_msg}")
            
            await ws.send(json.dumps(unsubscribe_msg))
            
            # 구독 목록에서 제거
            self.subscriptions = [s for s in self.subscriptions if s not in symbols]
            
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name} 구독 해제 중 오류: {str(e)}")
            return False
    
    async def send_ping(self, ws: websockets.WebSocketClientProtocol) -> bool:
        """
        Ping 메시지 전송 (바이낸스 현물은 직접 ping이 필요 없음)
        
        Args:
            ws: 웹소켓 연결 객체
            
        Returns:
            bool: 전송 성공 여부
        """
        # 바이낸스는 클라이언트 측에서 ping을 보낼 필요가 없어 아무것도 하지 않음
        return True
    
    def preprocess_message(self, message: str) -> Dict[str, Any]:
        """
        수신된 메시지 전처리
        
        Args:
            message: 수신된 웹소켓 메시지
            
        Returns:
            Dict[str, Any]: 처리된 메시지
        """
        try:
            # JSON 문자열을 딕셔너리로 변환
            data = json.loads(message)
            
            # 구독 응답 메시지인 경우
            if isinstance(data, dict) and "result" in data:
                return {
                    "type": "subscription_response",
                    "data": data
                }
                
            # 일반 이벤트 메시지인 경우 (depth 업데이트)
            if isinstance(data, dict) and "e" in data and data["e"] == "depthUpdate":
                # 심볼 추출
                symbol = data.get("s", "").lower()
                
                # 데이터 형식 변환
                result = {
                    "type": "depth_update",
                    "exchange": Exchange.BINANCE_SPOT.value,
                    "symbol": symbol,
                    "event_time": data.get("E", 0),
                    "transaction_time": data.get("T", 0) if "T" in data else 0,
                    "first_update_id": data.get("U", 0),
                    "final_update_id": data.get("u", 0),
                    "bids": data.get("b", []),
                    "asks": data.get("a", [])
                }
                
                return result
                
            # 기타 메시지는 그대로 반환
            return {
                "type": "unknown",
                "data": data,
                "raw": message
            }
            
        except json.JSONDecodeError as e:
            self.logger.error(f"바이낸스 현물 메시지 JSON 파싱 오류: {str(e)}")
            return {
                "type": "error",
                "error": "json_decode_error",
                "message": str(e),
                "raw": message
            }
            
        except Exception as e:
            self.logger.error(f"바이낸스 현물 메시지 처리 중 오류: {str(e)}")
            return {
                "type": "error",
                "error": "preprocessing_error",
                "message": str(e),
                "raw": message
            }
    
    def _get_next_id(self) -> int:
        """
        다음 요청 ID 생성
        
        Returns:
            int: 요청 ID
        """
        current_id = self.id_counter
        self.id_counter += 1
        return current_id