"""
업비트 현물 웹소켓 연결 전략 모듈

업비트 현물 거래소에 대한 웹소켓 연결 관리 전략을 제공합니다.
"""

import json
import time
import asyncio
import websockets
from typing import Dict, List, Any, Optional, Union

from crosskimp.common.config.app_config import get_config
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR

class UpbitSpotConnectionStrategy:
    """
    업비트 현물 웹소켓 연결 전략 클래스
    
    업비트 현물 거래소의 웹소켓 연결, 구독, 메시지 처리 등을 담당합니다.
    """
    
    # 상수 정의
    BASE_WS_URL = "wss://api.upbit.com/websocket/v1"
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    EXCHANGE_CODE = Exchange.UPBIT.value
    
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        self.config = get_config()
        self.subscriptions = []
        self.id_counter = 1  # 요청 ID 카운터
        self.exchange_name = EXCHANGE_NAMES_KR.get(self.EXCHANGE_CODE, "업비트")
        
        # 업비트는 오더북 깊이가 15로 고정
        self.orderbook_depth = 15
        
        # 호가 모아보기 레벨 설정 (설정 파일에서 가져옴)
        level_from_config = self.config.get(f"exchanges.{self.EXCHANGE_CODE}.orderbook_level", 0)
        # 명시적으로 정수로 변환
        self.orderbook_level = int(level_from_config)
        
        self.logger.info(f"{self.exchange_name} 연결 전략 초기화 (깊이: {self.orderbook_depth}, 고정값, 호가 모아보기 레벨: {self.orderbook_level})")
        
    def get_ws_url(self) -> str:
        """웹소켓 URL 반환"""
        return self.BASE_WS_URL
        
    def get_rest_url(self) -> str:
        """REST API URL 반환"""
        return "https://api.upbit.com/v1"
        
    def requires_ping(self) -> bool:
        """업비트는 클라이언트 측 핑이 필요 없음"""
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
                
                # self.logger.info(f"{self.exchange_name} 웹소켓 연결 성공")
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
            
    async def subscribe(self, ws: websockets.WebSocketClientProtocol, symbols: List[str], symbol_prices: Dict[str, float] = None) -> bool:
        """
        심볼 구독
        
        Args:
            ws: 웹소켓 연결 객체
            symbols: 구독할 심볼 목록
            symbol_prices: 심볼별 가격 정보 (제공되면 가격에 따라 레벨 자동 설정)
            
        Returns:
            bool: 구독 성공 여부
        """
        if not ws:
            self.logger.error(f"{self.exchange_name} 심볼 구독 실패: 웹소켓 연결 없음")
            return False
            
        try:
            # 구독할 심볼이 없는 경우
            if not symbols:
                self.logger.info(f"{self.exchange_name} 구독할 심볼이 없습니다")
                return True
                
            # 심볼 형식 변환 (KRW-BTC 형식으로 변경)
            formatted_symbols = [f"KRW-{s.upper()}" for s in symbols]
            
            # 업비트 구독 메시지 형식
            # [
            #   {"ticket": "티켓 이름"},
            #   {"type": "orderbook", "codes": ["KRW-BTC", "KRW-ETH", ...], "level": 레벨값},
            #   {"format": "SIMPLE"}
            # ]
            
            ticket = f"upbit-ticket-{int(time.time() * 1000)}"
            
            # 호가 모아보기 레벨 설정
            orderbook_level = self.orderbook_level  # 기본값 사용
            
            # 가격 정보가 제공된 경우 가격대별 레벨 설정
            if symbol_prices and symbols:
                self.logger.info(f"가격 정보에 기반한 호가 모아보기 레벨 계산 중 (심볼 수: {len(symbols)}개)")
                
                # 가격 기반 레벨 목록
                price_levels = []
                
                for sym in symbols:
                    price = symbol_prices.get(sym, 0)
                    if price <= 0:
                        continue
                    
                    # 가격대별 적절한 레벨 설정
                    # 업비트 API 호가 모아보기 레벨:
                    # 0: 호가를 그대로 표시
                    # 1: 1원 단위
                    # 2: 10원 단위
                    # 3: 100원 단위
                    # 4: 1,000원 단위
                    # 5: 10,000원 단위
                    if price < 100:  # 100원 미만
                        level = 1  # 호가를 그대로 표시
                    elif price < 1000:  # 1,000원 미만
                        level = 1  # 1원 단위
                    elif price < 10000:  # 10,000원 미만
                        level = 10  # 10원 단위
                    elif price < 100000:  # 100,000원 미만
                        level = 100  # 100원 단위
                    elif price < 1000000:  # 1,000,000원 미만
                        level = 1000  # 1,000원 단위
                    else:  # 1,000,000원 이상
                        level = 10000  # 10,000원 단위
                    
                    price_levels.append(level)
                
                # 가격 레벨이 있으면 가장 정밀한 레벨(가장 작은 값)으로 설정
                if price_levels:
                    orderbook_level = min(price_levels)
                    self.logger.info(f"가격 기반 호가 모아보기 레벨 설정: {orderbook_level}")
            
            # 구독 메시지 생성
            subscribe_msg = [
                {"ticket": ticket},
                {"type": "orderbook", "codes": formatted_symbols, "level": orderbook_level},
                {"format": "DEFAULT"}  # 필드 생략 방식
            ]
            
            # 구독 요청 전송
            self.logger.info(f"{self.exchange_name} 구독 요청: {len(formatted_symbols)}개 심볼, 호가 모아보기 레벨: {orderbook_level}")
            
            await ws.send(json.dumps(subscribe_msg))
            
            # 구독 목록 저장
            self.subscriptions = symbols
            
            return True
            
        except Exception as e:
            self.logger.error(f"{self.exchange_name} 구독 중 오류: {str(e)}")
            return False
            
    async def unsubscribe(self, ws: websockets.WebSocketClientProtocol, symbols: List[str]) -> bool:
        """
        심볼 구독 해제 - 업비트는 별도의 구독 해제 메시지가 없으므로 재연결 시 필요한 것만 구독
        
        Args:
            ws: 웹소켓 연결 객체
            symbols: 구독 해제할 심볼 목록
            
        Returns:
            bool: 구독 해제 성공 여부
        """
        # 업비트는 별도의 구독 해제 메시지가 없으므로 구독 목록에서만 제거
        self.subscriptions = [s for s in self.subscriptions if s not in symbols]
        self.logger.info(f"{self.exchange_name} {len(symbols)}개 심볼 구독 해제 (내부적으로만 처리됨)")
        return True
            
    async def send_ping(self, ws: websockets.WebSocketClientProtocol) -> bool:
        """
        Ping 메시지 전송 - 업비트는 필요 없음
        
        Args:
            ws: 웹소켓 연결 객체
            
        Returns:
            bool: 전송 성공 여부
        """
        # 업비트는 클라이언트 측 ping이 필요 없음
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
            # 메시지가 bytes인 경우 문자열로 변환
            if isinstance(message, bytes):
                message = message.decode('utf-8')
                
            # 메시지가 문자열인 경우 JSON으로 파싱
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
                
            # 타입 체크 (orderbook 타입만 처리)
            if data.get("type") != "orderbook":
                return {
                    "type": "unknown",
                    "data": data,
                    "raw": message
                }
                
            # 심볼 추출 (KRW-BTC -> BTC)
            code = data.get("code", "")
            if not code or not code.startswith("KRW-"):
                return {
                    "type": "error",
                    "error": "invalid_symbol",
                    "message": f"Invalid symbol format: {code}",
                    "raw": message
                }
                
            symbol = code.split("-")[1].lower()
            
            # 타임스탬프 추출 (업비트는 millisecond 단위 타임스탬프 제공)
            timestamp = data.get("timestamp", int(time.time() * 1000))
            
            # 오더북 데이터 추출
            orderbook_units = data.get("orderbook_units", [])
            
            # 매수/매도 호가 배열 생성 (price, quantity 형식으로 변환)
            bids = [[float(unit.get("bid_price", 0)), float(unit.get("bid_size", 0))] for unit in orderbook_units]
            asks = [[float(unit.get("ask_price", 0)), float(unit.get("ask_size", 0))] for unit in orderbook_units]
            
            # 결과 구성
            result = {
                "type": "orderbook",
                "subtype": "realtime",  # 업비트는 snapshot/realtime 구분 없이 항상 전체 데이터 전송
                "exchange": Exchange.UPBIT.value,
                "symbol": symbol,
                "timestamp": timestamp,
                "bids": bids,
                "asks": asks,
                "total_bid_size": sum(bid[1] for bid in bids),
                "total_ask_size": sum(ask[1] for ask in asks)
            }
            
            return result
            
        except json.JSONDecodeError as e:
            self.logger.error(f"업비트 현물 메시지 JSON 파싱 오류: {str(e)}")
            return {
                "type": "error",
                "error": "json_decode_error",
                "message": str(e),
                "raw": message
            }
            
        except Exception as e:
            self.logger.error(f"업비트 현물 메시지 처리 중 오류: {str(e)}")
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