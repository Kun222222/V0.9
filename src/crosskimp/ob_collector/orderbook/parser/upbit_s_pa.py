"""
업비트 파서 클래스

이 모듈은 업비트 거래소의 웹소켓 메시지를 파싱하는 클래스를 제공합니다.
"""

import json
import time
from typing import Dict, List, Optional, Tuple, Any

from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG
from crosskimp.ob_collector.orderbook.parser.base_parser import BaseParser

# ============================
# 업비트 파서 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.UPBIT.value  # 거래소 코드
UPBIT_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 업비트 설정

class UpbitParser(BaseParser):
    """
    업비트 파서 클래스
    
    업비트 거래소의 웹소켓 메시지를 파싱하는 클래스입니다.
    
    특징:
    - 메시지 형식: 전체 오더북 스냅샷 방식 (델타 업데이트 아님)
    """
    def __init__(self):
        """
        업비트 파서 초기화
        """
        super().__init__(EXCHANGE_CODE)

    def parse_message(self, raw_message: str) -> Optional[Dict[str, Any]]:
        """
        수신된 메시지 파싱
        
        Args:
            raw_message: 수신된 웹소켓 메시지
            
        Returns:
            Optional[Dict[str, Any]]: 파싱된 오더북 데이터 또는 None
        """
        try:
            # 바이트 메시지 디코딩
            if isinstance(raw_message, bytes):
                raw_message = raw_message.decode("utf-8")
            
            # JSON 파싱
            data = json.loads(raw_message)
            
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
            bids_dict, asks_dict = self._extract_orderbook_data(data.get("orderbook_units", []), symbol)

            # 유효성 검사
            if not bids_dict or not asks_dict:
                return None
                
            # 딕셔너리 형식에서 리스트 형식으로 변환 ({'price': p, 'size': s} -> [p, s])
            bids = [[item['price'], item['size']] for item in bids_dict]
            asks = [[item['price'], item['size']] for item in asks_dict]
            
            # 표준화된 오더북 데이터 반환
            return {
                "exchangename": EXCHANGE_CODE,
                "symbol": symbol.upper(),
                "bids": bids,
                "asks": asks,
                "timestamp": ts,
                "sequence": ts,
                "type": "snapshot"  # 업비트는 항상 전체 오더북 제공
            }
        except json.JSONDecodeError:
            self.log_error(f"JSON 파싱 실패: {raw_message[:100]}...")
            return None
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}")
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
            
            # 데이터 추가 (0인 값도 포함하여 필터링은 나중에 수행)
            bids.append({'price': bid_price, 'size': bid_size})
            asks.append({'price': ask_price, 'size': ask_size})

        # 0인 값 필터링 적용
        filtered_bids, filtered_asks = self.filter_zero_values(bids, asks)
            
        return filtered_bids, filtered_asks 