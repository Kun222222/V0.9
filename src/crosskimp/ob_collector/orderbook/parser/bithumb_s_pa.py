"""
빗썸 파서 클래스

이 모듈은 빗썸 거래소의 웹소켓 메시지를 파싱하는 클래스를 제공합니다.
"""

import json
import time
from typing import Dict, List, Optional, Tuple, Any

from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG
from crosskimp.ob_collector.orderbook.parser.base_parser import BaseParser

# ============================
# 빗썸 파서 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BITHUMB.value  # 거래소 코드
BITHUMB_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 빗썸 설정

# 빗썸 특화 설정
BITHUMB_MESSAGE_TYPE = BITHUMB_CONFIG["message_type_depth"]  # 오더북 메시지 타입
SYMBOL_SUFFIX = BITHUMB_CONFIG["symbol_suffix"]  # 심볼 접미사 (예: "_KRW")

class BithumbParser(BaseParser):
    """
    빗썸 파서 클래스
    
    빗썸 거래소의 웹소켓 메시지를 파싱하는 클래스입니다.
    
    특징:
    - 메시지 형식: 항상 전체 스냅샷 방식
    - 타임스탬프 기반 시퀀스 관리
    - 심볼 형식: {symbol}_KRW
    """
    def __init__(self):
        """
        빗썸 파서 초기화
        """
        super().__init__(EXCHANGE_CODE)
        self.depth_level = BITHUMB_CONFIG.get("default_depth", 50)

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
            if "status" in data and "resmsg" in data:
                if data["status"] == "0000":  # 성공 상태 코드
                    self.log_info(f"구독 응답 성공: {data['resmsg']}")
                else:  # 실패 상태 코드
                    self.log_error(f"구독 실패: {data['resmsg']}")
                return None

            # 오더북 메시지 타입 확인
            if data.get("type") != BITHUMB_MESSAGE_TYPE:
                return None
                
            # 심볼 추출
            symbol_raw = data.get("symbol", "")
            if not symbol_raw or not symbol_raw.endswith(SYMBOL_SUFFIX):
                self.stats["invalid_parsed"] += 1
                return None
                
            # KRW 제거하여 심볼 정규화
            symbol = symbol_raw.replace(SYMBOL_SUFFIX, "").upper()
            
            # 데이터 추출
            content = data.get("content", {})
            if not content:
                self.stats["invalid_parsed"] += 1
                return None
                
            # 타임스탬프 추출 (밀리초)
            timestamp = int(content.get("datetime", 0))
            if timestamp == 0:
                timestamp = int(time.time() * 1000)
            
            # 호가 데이터 추출 및 변환
            bids, asks = self._extract_orderbook_data(content)
            
            # 유효성 검사
            if not bids and not asks:
                self.stats["invalid_parsed"] += 1
                return None
                
            # 파싱 성공 카운트 증가
            self.stats["valid_parsed"] += 1
            
            # 표준화된 오더북 데이터 반환
            return {
                "exchangename": EXCHANGE_CODE,
                "symbol": symbol,
                "bids": bids,
                "asks": asks,
                "timestamp": timestamp,
                "sequence": timestamp,  # 빗썸은 타임스탬프를 시퀀스로 사용
                "type": "snapshot"  # 빗썸은 항상 전체 스냅샷 방식
            }
            
        except json.JSONDecodeError:
            self.log_error(f"JSON 파싱 실패: {raw_message[:100]}...")
            self.stats["invalid_parsed"] += 1
            return None
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}, raw={raw_message[:200]}...")
            self.stats["processing_errors"] += 1
            return None

    def _extract_orderbook_data(self, content: Dict[str, Any]) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
        """
        오더북 데이터에서 매수/매도 데이터 추출 및 변환
        
        Args:
            content: 오더북 데이터
            
        Returns:
            Tuple[List[Dict[str, float]], List[Dict[str, float]]]: 매수(bids)와 매도(asks) 데이터
        """
        # 원본 데이터 추출
        bids_data = content.get("bids", [])
        asks_data = content.get("asks", [])
        
        # 변환된 데이터를 저장할 리스트
        bids = []
        asks = []
        
        # 매수 호가 변환
        for bid in bids_data:
            try:
                price = float(bid.get("price", 0))
                quantity = float(bid.get("quantity", 0))
                if price > 0 and quantity > 0:
                    bids.append({"price": price, "size": quantity})
            except (ValueError, TypeError):
                continue
        
        # 매도 호가 변환
        for ask in asks_data:
            try:
                price = float(ask.get("price", 0))
                quantity = float(ask.get("quantity", 0))
                if price > 0 and quantity > 0:
                    asks.append({"price": price, "size": quantity})
            except (ValueError, TypeError):
                continue
        
        # 0인 값 필터링 적용
        filtered_bids, filtered_asks = self.filter_zero_values(bids, asks)
        
        return filtered_bids, filtered_asks

    def create_subscribe_message(self, symbols: List[str]) -> Dict[str, Any]:
        """
        구독 메시지 생성
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            Dict[str, Any]: 구독 메시지
        """
        # 심볼 형식 변환 ({symbol}_KRW)
        symbol_list = [f"{s.upper()}{SYMBOL_SUFFIX}" for s in symbols]
        
        # 구독 메시지 생성
        return {
            "type": BITHUMB_MESSAGE_TYPE,
            "symbols": symbol_list,
            "tickTypes": BITHUMB_CONFIG["tick_types"]
        }

    def parse_snapshot_data(self, data: dict, symbol: str) -> dict:
        """
        REST API로 받은 스냅샷 데이터 파싱
        
        Args:
            data: 스냅샷 데이터
            symbol: 심볼 이름
            
        Returns:
            dict: 파싱된 스냅샷 데이터
        """
        try:
            # 데이터 추출
            bids_data = data.get("bids", [])
            asks_data = data.get("asks", [])
            
            # 변환된 데이터를 저장할 리스트
            bids = []
            asks = []
            
            # 매수 호가 변환
            for bid_item in bids_data:
                # 딕셔너리 형태인 경우 (API 응답)
                if isinstance(bid_item, dict):
                    price = bid_item.get("price")
                    quantity = bid_item.get("quantity")
                # 리스트/튜플 형태인 경우 (이전 코드 호환성)
                elif isinstance(bid_item, (list, tuple)) and len(bid_item) >= 2:
                    price, quantity = bid_item
                else:
                    continue
                    
                try:
                    price_float = float(price)
                    quantity_float = float(quantity)
                    if price_float > 0 and quantity_float > 0:
                        bids.append({"price": price_float, "size": quantity_float})
                except (ValueError, TypeError):
                    continue
            
            # 매도 호가 변환
            for ask_item in asks_data:
                # 딕셔너리 형태인 경우 (API 응답)
                if isinstance(ask_item, dict):
                    price = ask_item.get("price")
                    quantity = ask_item.get("quantity")
                # 리스트/튜플 형태인 경우 (이전 코드 호환성)
                elif isinstance(ask_item, (list, tuple)) and len(ask_item) >= 2:
                    price, quantity = ask_item
                else:
                    continue
                    
                try:
                    price_float = float(price)
                    quantity_float = float(quantity)
                    if price_float > 0 and quantity_float > 0:
                        asks.append({"price": price_float, "size": quantity_float})
                except (ValueError, TypeError):
                    continue
            
            # 타임스탬프 추출 (밀리초 단위)
            timestamp = int(data.get("timestamp", time.time() * 1000))
            
            # 0인 값 필터링 적용
            filtered_bids, filtered_asks = self.filter_zero_values(bids, asks)
            
            # 파싱 성공 카운트 증가
            self.stats["valid_parsed"] += 1
            
            # 결과 포맷팅
            return {
                "symbol": symbol.upper(),
                "exchangename": EXCHANGE_CODE,
                "bids": filtered_bids,
                "asks": filtered_asks,
                "timestamp": timestamp,
                "sequence": timestamp,  # 빗썸은 타임스탬프를 시퀀스로 사용
                "type": "snapshot"
            }
        except Exception as e:
            self.log_error(f"{symbol} 스냅샷 파싱 실패: {e}")
            self.stats["processing_errors"] += 1
            return {} 