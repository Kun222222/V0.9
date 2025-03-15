# file: orderbook/websocket/bithumb_spot_websocket.py

import asyncio
import json
import time
from typing import Dict, List, Optional, Any, Callable, Union

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG

from crosskimp.ob_collector.orderbook.connection.bithumb_s_cn import BithumbWebSocketConnector
from crosskimp.ob_collector.orderbook.orderbook.bithumb_s_ob import BithumbSpotOrderBookManager

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 빗썸 웹소켓 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BITHUMB.value  # 거래소 코드
BITHUMB_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 빗썸 설정

# 오더북 관련 설정
DEFAULT_DEPTH = BITHUMB_CONFIG["default_depth"]  # 기본 오더북 깊이
SYMBOL_SUFFIX = BITHUMB_CONFIG["symbol_suffix"]  # 심볼 접미사

# 빗썸 특화 설정
BITHUMB_MESSAGE_TYPE = BITHUMB_CONFIG["message_type_depth"]  # 오더북 메시지 타입

def parse_bithumb_depth_update(msg_data: dict) -> Optional[dict]:
    """
    빗썸 오더북 메시지 파싱
    
    Args:
        msg_data: 원본 메시지 데이터
        
    Returns:
        파싱된 오더북 데이터 또는 None
    """
    try:
        # 메시지 타입 확인
        content_type = msg_data.get("type")
        if content_type != BITHUMB_MESSAGE_TYPE:
            return None
            
        # 심볼 추출
        symbol_raw = msg_data.get("symbol", "")
        if not symbol_raw or not symbol_raw.endswith(SYMBOL_SUFFIX):
            return None
            
        # KRW 제거하여 심볼 정규화
        symbol = symbol_raw.replace(SYMBOL_SUFFIX, "").upper()
        
        # 데이터 추출
        data = msg_data.get("content", {})
        if not data:
            return None
            
        # 타임스탬프 추출 (밀리초)
        timestamp = int(data.get("datetime", 0))
        
        # 호가 데이터 추출
        bids_data = data.get("bids", [])
        asks_data = data.get("asks", [])
        
        # 호가 변환
        bids = []
        for bid in bids_data:
            price = float(bid.get("price", 0))
            quantity = float(bid.get("quantity", 0))
            if price > 0 and quantity > 0:
                bids.append([price, quantity])
                
        asks = []
        for ask in asks_data:
            price = float(ask.get("price", 0))
            quantity = float(ask.get("quantity", 0))
            if price > 0 and quantity > 0:
                asks.append([price, quantity])
        
        # 결과 반환
        return {
            "exchangename": EXCHANGE_CODE,
            "symbol": symbol,
            "bids": bids,
            "asks": asks,
            "timestamp": timestamp,
            "sequence": timestamp,  # 빗썸은 타임스탬프를 시퀀스로 사용
            "type": "snapshot"  # 빗썸은 항상 전체 스냅샷 방식
        }
        
    except Exception as e:
        logger.error(f"빗썸 오더북 메시지 파싱 오류: {str(e)}", exc_info=True)
        return None

class BithumbSpotWebsocket(BithumbWebSocketConnector):
    """
    빗썸 현물 웹소켓 클라이언트
    - 메시지 파싱 및 처리
    - 오더북 업데이트 관리
    
    연결 관리는 BithumbWebSocketConnector 클래스에서 처리합니다.
    """
    def __init__(self, settings: dict):
        super().__init__(settings)
        self.manager = BithumbSpotOrderBookManager(depth=self.depth)
        self.manager.set_websocket(self)  # 웹소켓 연결 설정

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        super().set_output_queue(queue)  # 부모 클래스의 메서드 호출 (기본 큐 설정 및 로깅)
        self.manager.set_output_queue(queue)  # 오더북 매니저 큐 설정

    async def subscribe(self, symbols: List[str]):
        """
        지정된 심볼 목록을 구독
        
        1. 각 심볼별로 REST API를 통해 스냅샷 요청
        2. 스냅샷을 오더북 매니저에 적용
        3. 웹소켓을 통해 실시간 업데이트 구독
        
        Args:
            symbols: 구독할 심볼 목록
        """
        if not symbols:
            self.log_error("구독할 심볼이 없음")
            return

        # 각 심볼별로 스냅샷 요청 및 오더북 초기화를 수행
        for sym in symbols:
            try:
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "snapshot_request")
                
                # 오더북 초기화 (스냅샷 요청 및 적용)
                await self.manager.subscribe(sym)
                
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "snapshot_received")
                    
            except Exception as e:
                self.log_error(f"{sym} 스냅샷 요청 중 오류: {str(e)}")

        # 부모 클래스의 subscribe 호출하여 웹소켓 구독 수행
        await super().subscribe(symbols)

    async def parse_message(self, message: str) -> Optional[dict]:
        """
        빗썸 특화 파싱 로직
        
        Args:
            message: 웹소켓에서 수신한 원본 메시지 문자열
            
        Returns:
            Optional[dict]: 파싱된 메시지 데이터 또는 None (파싱 실패 시)
        """
        try:
            # 문자열 메시지를 JSON으로 파싱
            data = json.loads(message)
            
            # 구독 응답 처리
            if "status" in data and "resmsg" in data:
                if data["status"] == "0000":  # 성공 상태 코드
                    if self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "subscribe_response")
                    self.log_info(f"구독 응답 성공: {data['resmsg']}")
                else:  # 실패 상태 코드
                    self.log_error(f"구독 실패: {data['resmsg']}")
                return None

            # Delta 메시지 처리 (오더북 업데이트)
            if data.get("type") == BITHUMB_MESSAGE_TYPE:
                # 로깅 추가
                symbol = self._extract_symbol(data)
                if symbol != "UNKNOWN":
                    self.log_raw_message("depthUpdate", json.dumps(data), symbol)
                return data

            return None
        except json.JSONDecodeError as e:
            self.log_error(f"JSON 파싱 오류: {e}, 메시지: {message[:100]}...")
            return None
        except Exception as e:
            self.log_error(f"메시지 파싱 중 예외 발생: {e}")
            return None

    def _extract_symbol(self, parsed: dict) -> str:
        """
        빗썸 특화 심볼 추출
        
        Args:
            parsed: 파싱된 메시지 데이터
            
        Returns:
            str: 추출된 심볼명 (KRW 접미사 제거 및 대문자 변환)
        """
        try:
            content = parsed.get("content", {})
            order_list = content.get("list", [])
            if order_list:
                raw_symbol = order_list[0].get("symbol", "")
                return raw_symbol.replace(SYMBOL_SUFFIX, "").upper()
        except Exception as e:
            self.log_error(f"심볼 추출 중 오류: {str(e)}")
        return "UNKNOWN"

    async def handle_parsed_message(self, parsed: dict) -> None:
        """
        파싱된 메시지 처리
        
        Args:
            parsed: 파싱된 메시지 데이터
        """
        try:
            # 메시지를 공통 포맷으로 변환
            evt = parse_bithumb_depth_update(parsed)
            if evt:
                symbol = evt["symbol"]
                # 오더북 매니저를 통해 업데이트 수행
                res = await self.manager.update(symbol, evt)
                if not res.is_valid:
                    self.log_error(f"{symbol} 업데이트 실패: {res.error_messages}")
                elif self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "message")
        except Exception as e:
            self.log_error(f"handle_parsed_message 예외: {e}")

    async def process_message(self, message: str) -> None:
        """
        수신된 메시지 처리 (BithumbWebSocketConnector 클래스의 추상 메서드 구현)
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        parsed = await self.parse_message(message)
        if parsed:
            await self.handle_parsed_message(parsed)