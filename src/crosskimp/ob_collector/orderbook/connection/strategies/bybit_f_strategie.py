"""
바이빗 선물 웹소켓 연결 전략 모듈

바이빗 선물 거래소에 대한 웹소켓 연결 관리 전략을 제공합니다.
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

class BybitFutureConnectionStrategy:
    """
    바이빗 선물 웹소켓 연결 전략 클래스
    
    바이빗 선물 거래소의 웹소켓 연결, 구독, 메시지 처리 등을 담당합니다.
    """
    
    # 상수 정의
    BASE_WS_URL = "wss://stream.bybit.com/v5/public/linear"
    
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        self.config = get_config()
        self.exchange_code = Exchange.BYBIT_FUTURE.value
        self.exchange_name_kr = EXCHANGE_NAMES_KR[self.exchange_code]
        self.subscriptions = []
        self.id_counter = 1  # 요청 ID 카운터
        # self.logger.info(f"{self.exchange_name_kr} 연결 전략 초기화")
        
    def get_ws_url(self) -> str:
        """웹소켓 URL 반환"""
        return self.BASE_WS_URL
        
    def requires_ping(self) -> bool:
        """바이빗은 클라이언트 측에서 20초마다 ping이 필요함"""
        return True
        
    async def connect(self, timeout: int = 0.5) -> websockets.WebSocketClientProtocol:
        """
        웹소켓 연결 수립
        
        Args:
            timeout: 연결 타임아웃 (초)
            
        Returns:
            websockets.WebSocketClientProtocol: 웹소켓 연결 객체
        """
        # self.logger.info(f"{self.exchange_name_kr} 웹소켓 연결 시도 중: {self.BASE_WS_URL}")
        
        retry_count = 0
        while True:
            try:
                retry_count += 1
                # 웹소켓 연결 (짧은 타임아웃으로 설정)
                ws = await asyncio.wait_for(
                    websockets.connect(
                        self.BASE_WS_URL,
                        ping_interval=20,  # 내부 핑 간격 (초)
                        ping_timeout=10,   # 핑 타임아웃 (초)
                        close_timeout=10   # 닫기 타임아웃 (초)
                    ),
                    timeout=timeout
                )
                
                # self.logger.info(f"{self.exchange_name_kr} 웹소켓 연결 성공")
                return ws
                
            except asyncio.TimeoutError:
                self.logger.warning(f"{self.exchange_name_kr} 웹소켓 연결 타임아웃 ({retry_count}번째 시도), 재시도 중...")
                await asyncio.sleep(0.5)  # 0.5초 대기 후 재시도
                
            except Exception as e:
                self.logger.error(f"{self.exchange_name_kr} 웹소켓 연결 실패 ({retry_count}번째 시도): {str(e)}")
                await asyncio.sleep(0.5)  # 0.5초 대기 후 재시도
            
    async def disconnect(self, ws: websockets.WebSocketClientProtocol) -> None:
        """
        웹소켓 연결 종료
        
        Args:
            ws: 웹소켓 연결 객체
        """
        if ws:
            try:
                await ws.close()
                self.logger.info(f"{self.exchange_name_kr} 웹소켓 연결 종료됨")
            except Exception as e:
                self.logger.error(f"{self.exchange_name_kr} 웹소켓 연결 종료 중 오류: {str(e)}")
                
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
            self.logger.error(f"{self.exchange_name_kr} 심볼 구독 실패: 웹소켓 연결 없음")
            return False
            
        try:
            # 심볼 형식 변환 (모두 소문자로)
            formatted_symbols = [s.lower() + 'usdt' if not s.lower().endswith('usdt') else s.lower() for s in symbols]
            
            # 심볼별로 depth 스트림 구독
            args = [f"orderbook.50.{symbol.upper()}" for symbol in formatted_symbols]
            
            # 구독 메시지 생성
            request_id = self._get_next_id()
            subscribe_msg = {
                "op": "subscribe",
                "args": args,
                "req_id": str(request_id)
            }
            
            # 구독 요청 전송
            self.logger.info(f"{self.exchange_name_kr} 구독 요청: {len(args)}개 심볼")
            self.logger.debug(f"{self.exchange_name_kr} 구독 요청 상세: {subscribe_msg}")
            
            await ws.send(json.dumps(subscribe_msg))
            
            # 구독 목록 저장
            self.subscriptions = symbols
            
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 구독 중 오류: {str(e)}")
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
            self.logger.error(f"{self.exchange_name_kr} 구독 해제 실패: 웹소켓 연결 없음")
            return False
            
        try:
            # 심볼 형식 변환 (모두 소문자로)
            formatted_symbols = [s.lower() + 'usdt' if not s.lower().endswith('usdt') else s.lower() for s in symbols]
            
            # 심볼별로 depth 스트림 구독 해제
            args = [f"orderbook.50.{symbol.upper()}" for symbol in formatted_symbols]
            
            # 구독 해제 메시지 생성
            request_id = self._get_next_id()
            unsubscribe_msg = {
                "op": "unsubscribe",
                "args": args,
                "req_id": str(request_id)
            }
            
            # 구독 해제 요청 전송
            self.logger.info(f"{self.exchange_name_kr} 구독 해제 요청: {len(args)}개 심볼")
            self.logger.debug(f"{self.exchange_name_kr} 구독 해제 요청 상세: {unsubscribe_msg}")
            
            await ws.send(json.dumps(unsubscribe_msg))
            
            # 구독 목록에서 제거
            self.subscriptions = [s for s in self.subscriptions if s not in symbols]
            
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 구독 해제 중 오류: {str(e)}")
            return False
    
    async def send_ping(self, ws: websockets.WebSocketClientProtocol) -> bool:
        """
        Ping 메시지 전송
        
        Args:
            ws: 웹소켓 연결 객체
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            # 바이빗은 직접 ping 메시지 필요
            ping_msg = {
                "op": "ping",
                "req_id": str(self._get_next_id())
            }
            await ws.send(json.dumps(ping_msg))
            return True
        except Exception as e:
            self.logger.error(f"{self.exchange_name_kr} 핑 메시지 전송 실패: {str(e)}")
            return False
    
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
            
            # 핑퐁 응답 메시지인 경우
            if "op" in data and data["op"] == "pong":
                return {
                    "type": "pong",
                    "data": data
                }
                
            # 구독 응답 메시지인 경우
            if "success" in data and "op" in data and data["op"] == "subscribe":
                return {
                    "type": "subscription_response",
                    "data": data
                }

            # 오더북 메시지인 경우
            if "topic" in data and "data" in data and "topic" in data and "orderbook" in data["topic"]:
                # 심볼 추출 (예: orderbook.50.BTCUSDT)
                topic_parts = data["topic"].split(".")
                if len(topic_parts) >= 3:
                    symbol = topic_parts[-1].lower()
                    
                    # 원본 type 보존 (snapshot 또는 delta)
                    original_type = data.get("type", "delta")
                    
                    # 데이터 추출
                    orderbook_data = data.get("data", {})
                    
                    # 결과 구성 - depth_update로 타입 통일
                    result = {
                        "type": "depth_update",  # 바이빗 현물과 동일하게 depth_update로 타입 통일
                        "original_type": original_type,  # 원본 타입도 함께 저장
                        "exchange": Exchange.BYBIT_FUTURE.value,
                        "symbol": symbol,
                        "event_time": data.get("ts", 0),
                        "bids": orderbook_data.get("b", []),
                        "asks": orderbook_data.get("a", []),
                        "sequence": orderbook_data.get("seq", 0),
                        "update_id": orderbook_data.get("u", 0),
                        "cross_seq": orderbook_data.get("seq", 0),
                        "timestamp": orderbook_data.get("cts", 0)
                    }
                    
                    return result
                
            # 기타 메시지는 그대로 반환
            return {
                "type": "unknown",
                "data": data,
                "raw": message
            }
            
        except json.JSONDecodeError as e:
            self.logger.error(f"바이빗 선물 메시지 JSON 파싱 오류: {str(e)}")
            return {
                "type": "error",
                "error": "json_decode_error",
                "message": str(e),
                "raw": message
            }
            
        except Exception as e:
            self.logger.error(f"바이빗 선물 메시지 처리 중 오류: {str(e)}")
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