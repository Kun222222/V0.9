"""
빗썸 현물 웹소켓 연결 전략 모듈

빗썸 현물 거래소에 대한 웹소켓 연결 관리 전략을 제공합니다.
"""

import json
import time
import asyncio
import websockets
from typing import Dict, List, Any, Optional, Union

from crosskimp.common.config.app_config import get_config
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR

class BithumbSpotConnectionStrategy:
    """
    빗썸 현물 웹소켓 연결 전략 클래스
    
    빗썸 현물 거래소의 웹소켓 연결, 구독, 메시지 처리 등을 담당합니다.
    """
    
    # 상수 정의
    BASE_WS_URL = "wss://ws-api.bithumb.com/websocket/v1"
    EXCHANGE_CODE = Exchange.BITHUMB.value
    
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        self.config = get_config()
        self.subscriptions = []
        self.id_counter = 1  # 요청 ID 카운터
        self.exchange_name = EXCHANGE_NAMES_KR.get(self.EXCHANGE_CODE, "[빗썸]")
        
        # 빗썸은 오더북 깊이가 15로 고정
        self.orderbook_depth = 15
        
        self.logger.info(f"{self.exchange_name} 연결 전략 초기화 (깊이: {self.orderbook_depth}, 고정값)")
        
    def get_ws_url(self) -> str:
        """웹소켓 URL 반환"""
        return self.BASE_WS_URL
        
    def requires_ping(self) -> bool:
        """빗썸은 클라이언트 측 핑이 필요하지 않음"""
        return False
        
    async def connect(self, timeout: int = 3.0) -> websockets.WebSocketClientProtocol:
        """
        웹소켓 연결 수립
        
        Args:
            timeout: 연결 타임아웃 (초)
            
        Returns:
            websockets.WebSocketClientProtocol: 웹소켓 연결 객체
        """
        # self.logger.info(f"{self.exchange_name} 웹소켓 연결 시도 중: {self.BASE_WS_URL}")
        
        retry_count = 0
        while True:
            try:
                retry_count += 1
                # 웹소켓 연결 (짧은 타임아웃으로 설정)
                ws = await asyncio.wait_for(
                    websockets.connect(
                        self.BASE_WS_URL,
                        close_timeout=10   # 닫기 타임아웃 (초)
                    ),
                    timeout=timeout
                )
                
                self.logger.info(f"{self.exchange_name} 웹소켓 연결 성공")
                return ws
            
            except asyncio.TimeoutError:
                self.logger.warning(f"{self.exchange_name} 웹소켓 연결 타임아웃 ({retry_count}번째 시도), 재시도 중...")
                await asyncio.sleep(0.5)  # 0.5초 대기 후 재시도
                
            except Exception as e:
                self.logger.error(f"{self.exchange_name} 웹소켓 연결 실패 ({retry_count}번째 시도): {str(e)}")
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
            # 심볼 형식 변환 (KRW-BTC 형식으로 변경)
            formatted_symbols = [f"KRW-{s.upper()}" for s in symbols]
            
            success = True
            
            # 빗썸은 헤더-본문 구조로 구성
            # 형식: [헤더, 본문...]
            ticket = f"bithumb-ticket-{int(time.time() * 1000)}"
            
            # 구독 메시지 생성
            subscribe_msg = [
                {"ticket": ticket},
                {"type": "orderbook", "codes": formatted_symbols},
                {"format": "DEFAULT"}  # 필드 생략 방식 (스냅샷과 실시간 모두 수신)
            ]
            
            # 구독 요청 전송
            self.logger.info(f"{self.exchange_name} 구독 요청: {len(formatted_symbols)}개 심볼")
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
        심볼 구독 해제 - 빗썸은 별도의 구독 해제 메시지가 없으므로 재연결 시 필요한 것만 구독
        
        Args:
            ws: 웹소켓 연결 객체
            symbols: 구독 해제할 심볼 목록
            
        Returns:
            bool: 구독 해제 성공 여부
        """
        # 빗썸은 별도의 구독 해제 메시지가 없으므로 구독 목록에서만 제거
        self.subscriptions = [s for s in self.subscriptions if s not in symbols]
        self.logger.info(f"{self.exchange_name} {len(symbols)}개 심볼 구독 해제 (내부적으로만 처리됨)")
        return True
            
    async def send_ping(self, ws: websockets.WebSocketClientProtocol) -> bool:
        """
        Ping 메시지 전송 - 빗썸은 필요 없음
        
        Args:
            ws: 웹소켓 연결 객체
            
        Returns:
            bool: 전송 성공 여부
        """
        # 빗썸은 클라이언트 측 ping이 필요 없음
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
            
            # 메시지 타입 확인
            message_type = data.get("type")
            
            # 오더북 메시지인 경우
            if message_type == "orderbook":
                # 심볼 추출
                symbol = data.get("code", "").replace("KRW-", "").lower()
                stream_type = data.get("stream_type", "REALTIME")  # 스트림 타입 (SNAPSHOT 또는 REALTIME)
                
                # 결과 구성
                result = {
                    "type": "orderbook",
                    "subtype": stream_type.lower(),  # 스트림 타입 소문자로 변환
                    "exchange": self.EXCHANGE_CODE,
                    "symbol": symbol,
                    "event_time": data.get("timestamp", 0),
                    "bids": [],
                    "asks": [],
                    "timestamp": data.get("timestamp", 0)
                }
                
                # 오더북 데이터 변환 (price, size 형식으로)
                orderbook_units = data.get("orderbook_units", [])
                
                # 매수/매도 호가 처리
                for unit in orderbook_units:
                    bid_price = unit.get("bid_price")
                    bid_size = unit.get("bid_size")
                    ask_price = unit.get("ask_price")
                    ask_size = unit.get("ask_size")
                    
                    if bid_price is not None and bid_size is not None:
                        result["bids"].append([float(bid_price), float(bid_size)])
                    
                    if ask_price is not None and ask_size is not None:
                        result["asks"].append([float(ask_price), float(ask_size)])
                
                # 추가 정보
                result["total_bid_size"] = data.get("total_bid_size", 0)
                result["total_ask_size"] = data.get("total_ask_size", 0)
                
                return result
            
            # 기타 메시지는 그대로 반환
            return {
                "type": "unknown",
                "data": data,
                "raw": message
            }
            
        except json.JSONDecodeError as e:
            self.logger.error(f"{self.exchange_name} 메시지 JSON 파싱 오류: {str(e)}")
            return {
                "type": "error",
                "error": "json_decode_error",
                "message": str(e),
                "raw": message
            }
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name} 메시지 처리 중 오류: {str(e)}")
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