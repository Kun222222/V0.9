"""
바이빗 파서 클래스

이 모듈은 바이빗 현물 거래소의 웹소켓 메시지를 파싱하는 클래스를 제공합니다.
"""

import json
import time
from typing import Dict, List, Optional, Tuple, Any

from crosskimp.ob_collector.orderbook.parser.base_parser import BaseParser

# ============================
# 바이빗 파서 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = "BYBIT"  # 거래소 코드
DEFAULT_DEPTH = 50  # 기본 오더북 깊이


class BybitParser(BaseParser):
    """
    바이빗 현물 파서 클래스
    
    바이빗 현물 거래소의 웹소켓 메시지를 파싱하는 클래스입니다.
    
    특징:
    - 메시지 형식: 스냅샷 및 델타 업데이트 지원
    - 시퀀스 번호 기반 데이터 정합성 검증
    """
    def __init__(self):
        """
        바이빗 파서 초기화
        """
        super().__init__(EXCHANGE_CODE)
        self.depth_level = DEFAULT_DEPTH

    def parse_message(self, raw_message: str) -> Optional[Dict[str, Any]]:
        """
        수신된 메시지 파싱
        
        Args:
            raw_message: 수신된 웹소켓 메시지
            
        Returns:
            Optional[Dict[str, Any]]: 파싱된 오더북 데이터 또는 None
        """
        try:
            # 통계 업데이트
            self.stats["total_parsed"] += 1
            
            # JSON 파싱
            data = json.loads(raw_message)
            
            # 구독 응답 처리
            if data.get("op") == "subscribe":
                return None
                
            # PONG 응답 처리
            if self.is_pong_message(data):
                return None

            # 오더북 메시지 타입 확인
            if "topic" not in data or "data" not in data:
                return None
                
            topic = data.get("topic", "")
            if "orderbook" not in topic:
                return None
                
            # 심볼 추출
            parts = topic.split(".")
            if len(parts) < 3:
                self.log_error("토픽 형식 오류")
                self.stats["invalid_parsed"] += 1
                return None
                
            sym_str = parts[-1]  # 예: "BTCUSDT"
            symbol = sym_str.replace("USDT", "")  # 예: "BTC"
            
            # 메시지 타입 확인 (스냅샷 또는 델타)
            msg_type = data.get("type", "delta").lower()  # "snapshot" or "delta"
            
            # 오더북 데이터 추출
            ob_data = data.get("data", {})
            
            # 시퀀스 번호 및 타임스탬프 추출
            update_id = ob_data.get("u", 0)
            cross_seq = ob_data.get("seq", 0)
            ts = data.get("ts", int(time.time() * 1000))
            
            # 시퀀스 번호가 없으면 타임스탬프를 사용
            seq = cross_seq if cross_seq > 0 else update_id
            if seq == 0:
                seq = ts
            
            # 매수/매도 데이터 추출 및 필터링
            bids, asks = self._extract_orderbook_data(ob_data)
            
            # 유효성 검사
            if not bids and not asks:
                self.stats["invalid_parsed"] += 1
                return None
                
            # 파싱 성공 카운트 증가
            self.stats["valid_parsed"] += 1
            
            # 표준화된 오더북 데이터 반환
            return {
                "exchangename": EXCHANGE_CODE,
                "symbol": symbol.upper(),
                "bids": bids,
                "asks": asks,
                "timestamp": ts,
                "sequence": seq,
                "type": msg_type,
                "raw_data": {
                    "U": seq - 1,  # 시작 시퀀스
                    "u": seq       # 종료 시퀀스
                }
            }
            
        except json.JSONDecodeError:
            self.log_error(f"JSON 파싱 실패: {raw_message[:100]}...")
            self.stats["invalid_parsed"] += 1
            return None
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}, raw={raw_message[:200]}...")
            self.stats["processing_errors"] += 1
            return None

    def _extract_orderbook_data(self, ob_data: Dict[str, Any]) -> Tuple[List[List[float]], List[List[float]]]:
        """
        오더북 데이터에서 매수/매도 데이터 추출 및 변환
        
        Args:
            ob_data: 오더북 데이터
            
        Returns:
            Tuple[List[List[float]], List[List[float]]]: 매수(bids)와 매도(asks) 데이터
        """
        # 원본 데이터 형식: [price_str, size_str]
        raw_bids = ob_data.get("b", [])
        raw_asks = ob_data.get("a", [])
        
        # 변환된 데이터를 저장할 리스트
        bids = []
        asks = []
        
        # 매수 호가 변환
        for bid in raw_bids:
            if len(bid) >= 2:
                price = float(bid[0])
                size = float(bid[1])
                # 리스트 형식으로 저장 [price, size]
                bids.append([price, size])
        
        # 매도 호가 변환
        for ask in raw_asks:
            if len(ask) >= 2:
                price = float(ask[0])
                size = float(ask[1])
                # 리스트 형식으로 저장 [price, size]
                asks.append([price, size])
        
        # 0인 값 필터링 적용 (리스트 형식에 맞게 수정)
        filtered_bids = [bid for bid in bids if bid[1] > 0]
        filtered_asks = [ask for ask in asks if ask[1] > 0]
        
        return filtered_bids, filtered_asks

    def create_subscribe_message(self, symbols: List[str]) -> Dict[str, Any]:
        """
        구독 메시지 생성
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            Dict[str, Any]: 구독 메시지
        """
        # 심볼 형식 변환 ({symbol}USDT)
        args = []
        for sym in symbols:
            market = f"{sym}USDT"
            args.append(f"orderbook.{self.depth_level}.{market}")
        
        # 구독 메시지 생성
        return {
            "op": "subscribe",
            "args": args
        }

    def is_pong_message(self, data: Dict[str, Any]) -> bool:
        """
        PONG 메시지 여부 확인
        
        Args:
            data: 파싱된 메시지 데이터
            
        Returns:
            bool: PONG 메시지인 경우 True, 아니면 False
        """
        return (data.get("op") == "pong" or 
                (data.get("ret_msg") == "pong" and data.get("op") == "ping")) 