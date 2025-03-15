# file: orderbook/websocket/upbit_websocket.py

import asyncio
import json
import time
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG

from crosskimp.ob_collector.orderbook.connection.upbit_s_cn import UpbitWebSocketConnector, PONG_RESPONSE
from crosskimp.ob_collector.orderbook.orderbook.upbit_s_ob import UpbitOrderBookManager


# ============================
# 업비트 웹소켓 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.UPBIT.value  # 거래소 코드
UPBIT_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 업비트 설정

# 오더북 관련 설정
DEFAULT_DEPTH = UPBIT_CONFIG["default_depth"]  # 기본 오더북 깊이

@dataclass
class UpbitOrderbookUnit:
    """업비트 오더북 단위 데이터 구조체"""
    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float

class UpbitWebsocket(UpbitWebSocketConnector):
    """
    Upbit WebSocket 클라이언트
    
    업비트 거래소의 실시간 오더북 데이터를 수신하고 처리하는 웹소켓 클라이언트입니다.
    
    특징:
    - 메시지 형식: 전체 오더북 스냅샷 방식 (델타 업데이트 아님)
    """
    def __init__(self, settings: dict):
        """
        업비트 웹소켓 클라이언트 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings)
        
        # 오더북 설정
        depth = settings.get("websocket", {}).get("orderbook_depth", DEFAULT_DEPTH)
        self.orderbook_manager = UpbitOrderBookManager(depth=depth)
        
        # 초기화된 심볼 추적
        self.initialized_symbols = set()
        
        # 메시지 처리 통계 확장
        self.message_stats.update({
            "valid_parsed": 0,
            "invalid_parsed": 0,
            "processing_errors": 0
        })

    async def process_message(self, message: str) -> None:
        """
        수신된 메시지 처리 (UpbitWebSocketConnector 메서드 구현)
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        # PONG 메시지는 이미 처리됨
        if message == PONG_RESPONSE:
            return
            
        # 메시지 파싱 및 처리
        parsed = await self.parse_message(message)
        if parsed:
            await self.handle_parsed_message(parsed)
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "message")

    async def parse_message(self, message: str) -> Optional[dict]:
        """
        수신된 메시지 파싱
        
        Args:
            message: 수신된 웹소켓 메시지
            
        Returns:
            Optional[dict]: 파싱된 오더북 데이터 또는 None
        """
        try:
            # 바이트 메시지 디코딩
            if isinstance(message, bytes):
                message = message.decode("utf-8")
            
            # PING 응답 처리
            if message == PONG_RESPONSE:
                return None
            
            # JSON 파싱
            data = json.loads(message)
            
            # 오더북 메시지 타입 확인
            if data.get("type") != "orderbook":
                return None

            # 심볼 추출
            symbol = data.get("code", "").replace("KRW-","")
            if not symbol:
                self.log_error("심볼 정보 누락")
                return None

            # 타임스탬프 추출
            ts = data.get("timestamp", int(time.time()*1000))
            
            # 오더북 데이터 추출 및 변환
            bids, asks = self._extract_orderbook_data(data.get("orderbook_units", []), symbol)

            # 유효성 검사
            if not bids or not asks:
                self.message_stats["invalid_parsed"] += 1
                return None

            # 파싱 성공 카운트 증가
            self.message_stats["valid_parsed"] += 1
            
            # 로깅 추가
            parsed_data = {
                "exchangename": EXCHANGE_CODE,
                "symbol": symbol.upper(),
                "bids": bids,
                "asks": asks,
                "timestamp": ts,
                "sequence": ts,
                "type": "delta"  # 업비트는 항상 전체 오더북 제공
            }
            
            # 원본 메시지만 로깅 (parsedData 로깅 제거)
            self.log_raw_message("orderbook", message, symbol.upper())
            
            # 표준화된 오더북 데이터 반환
            return parsed_data
        except json.JSONDecodeError:
            self.log_error(f"JSON 파싱 실패: {message[:100]}...")
            return None
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}, raw={message[:200]}...")
            self.message_stats["processing_errors"] += 1
            return None

    def _extract_orderbook_data(self, units: List[Dict[str, Any]], symbol: str) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
        """
        오더북 유닛에서 매수/매도 데이터 추출
        
        Args:
            units: 오더북 유닛 목록
            symbol: 심볼 이름
            
        Returns:
            Tuple[List[Dict[str, float]], List[Dict[str, float]]]: 매수(bids)와 매도(asks) 데이터
        """
        bids = []
        asks = []
        
        for unit in units:
            # 가격과 수량 추출
            bid_price = float(unit.get("bid_price", 0))
            bid_size = float(unit.get("bid_size", 0))
            ask_price = float(unit.get("ask_price", 0))
            ask_size = float(unit.get("ask_size", 0))
            
            # 유효한 데이터만 추가
            if bid_price > 0:
                bids.append({'price': bid_price, 'size': bid_size})
            if ask_price > 0:
                asks.append({'price': ask_price, 'size': ask_size})

        # 빈 데이터 로깅
        if not bids:
            self.log_error(f"{symbol} bids가 비어 있습니다: {units}")
        if not asks:
            self.log_error(f"{symbol} asks가 비어 있습니다: {units}")
            
        return bids, asks

    async def handle_parsed_message(self, parsed: dict) -> None:
        """
        파싱된 메시지 처리
        
        Args:
            parsed: 파싱된 오더북 데이터
        """
        try:
            symbol = parsed["symbol"]
            
            # 업비트는 매 메시지가 전체 오더북이므로, 항상 스냅샷으로 처리
            result = await self.orderbook_manager.initialize_orderbook(symbol, parsed)
            
            if result.is_valid:
                # 시퀀스 업데이트
                self.orderbook_manager.update_sequence(symbol, parsed["timestamp"])
                
                # 오더북 데이터 가져오기
                ob_dict = self.orderbook_manager.get_orderbook(symbol).to_dict()
                
                # 출력 큐에 추가
                if self.output_queue:
                    await self.output_queue.put((self.exchangename, ob_dict))
            else:
                self.log_error(f"{symbol} 오더북 업데이트 실패: {result.error_messages}")

        except KeyError as e:
            self.log_error(f"필수 필드 누락: {e}")
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {e}")

    async def stop(self) -> None:
        """
        웹소켓 연결 종료
        """
        self.log_info("웹소켓 연결 종료 중...")
        await super().stop()
        
        # 오더북 데이터 정리
        self.orderbook_manager.clear_all()
        
        self.log_info("웹소켓 연결 종료 완료")        
        # 통계 로깅
        self.log_info(
            f"웹소켓 종료 | "
            f"총 메시지: {self.message_stats['total_received']}개, "
            f"유효 파싱: {self.message_stats['valid_parsed']}개, "
            f"핑퐁: {self.message_stats['ping_pong']}개, "
            f"오류: {self.message_stats['processing_errors']}개"
        )
