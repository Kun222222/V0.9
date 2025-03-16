"""
바이낸스 현물 파서 클래스

이 모듈은 바이낸스 현물 거래소의 웹소켓 메시지를 파싱하는 클래스를 제공합니다.
"""

import json
import time
from typing import Dict, List, Optional, Tuple, Any

from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG
from crosskimp.ob_collector.orderbook.parser.base_parser import BaseParser

# ============================
# 바이낸스 현물 파서 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BINANCE.value  # 거래소 코드
BINANCE_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이낸스 설정

# 바이낸스 특화 설정
DEPTH_UPDATE_STREAM = BINANCE_CONFIG["depth_update_stream"]  # 깊이 업데이트 스트림 형식
SUBSCRIBE_CHUNK_SIZE = BINANCE_CONFIG["subscribe_chunk_size"]  # 한 번에 구독할 심볼 수
SNAPSHOT_URL = BINANCE_CONFIG["api_urls"]["depth"]  # 스냅샷 URL

class BinanceParser(BaseParser):
    """
    바이낸스 현물 파서 클래스
    
    바이낸스 현물 거래소의 웹소켓 메시지를 파싱하는 클래스입니다.
    
    특징:
    - 메시지 형식: 델타 업데이트 방식
    - 시퀀스 ID 기반 관리
    - 심볼 형식: {symbol}USDT
    """
    def __init__(self):
        """
        바이낸스 현물 파서 초기화
        """
        super().__init__(EXCHANGE_CODE)
        self.snapshot_url = SNAPSHOT_URL  # 스냅샷 URL 설정

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
            if "result" in data and "id" in data:
                self.log_info(f"구독 응답 수신: {data}")
                return None
                
            # 오더북 메시지 타입 확인
            if data.get("e") != "depthUpdate":
                self.stats["invalid_parsed"] += 1
                return None
                
            # 심볼 추출
            symbol_raw = data.get("s", "")
            symbol = symbol_raw.replace("USDT", "").upper()
            
            # 데이터 추출
            b_data = data.get("b", [])
            a_data = data.get("a", [])
            event_time = data.get("E", 0)
            first_id = data.get("U", 0)
            final_id = data.get("u", 0)
            
            # 호가 데이터 추출 및 변환
            bids, asks = self._extract_orderbook_data(b_data, a_data)
            
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
                "timestamp": event_time,
                "first_update_id": first_id,
                "final_update_id": final_id,
                "sequence": final_id,
                "type": "delta"
            }
            
        except json.JSONDecodeError:
            self.log_error(f"JSON 파싱 실패: {raw_message[:100]}...")
            self.stats["invalid_parsed"] += 1
            return None
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}, raw={raw_message[:200]}...")
            self.stats["processing_errors"] += 1
            return None

    def _extract_orderbook_data(self, b_data: List, a_data: List) -> Tuple[List[List[float]], List[List[float]]]:
        """
        오더북 데이터에서 매수/매도 데이터 추출 및 변환
        
        Args:
            b_data: 매수 호가 데이터
            a_data: 매도 호가 데이터
            
        Returns:
            Tuple[List[List[float]], List[List[float]]]: 매수(bids)와 매도(asks) 데이터
        """
        # 변환된 데이터를 저장할 리스트
        bids = []
        asks = []
        
        # 매수 호가 변환
        for b in b_data:
            try:
                px, qty = float(b[0]), float(b[1])
                if px > 0 and qty != 0:
                    bids.append([px, qty])
            except (ValueError, TypeError):
                continue
        
        # 매도 호가 변환
        for a in a_data:
            try:
                px, qty = float(a[0]), float(a[1])
                if px > 0 and qty != 0:
                    asks.append([px, qty])
            except (ValueError, TypeError):
                continue
        
        return bids, asks

    def create_subscribe_message(self, symbols: List[str]) -> Dict[str, Any]:
        """
        구독 메시지 생성
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            Dict[str, Any]: 구독 메시지
        """
        # 심볼 형식 변환 ({symbol}usdt@depth)
        sub_params = [f"{sym.lower()}usdt{DEPTH_UPDATE_STREAM}" for sym in symbols]
        
        # 구독 메시지 생성
        return {
            "method": "SUBSCRIBE",
            "params": sub_params,
            "id": int(time.time() * 1000)
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
            if "lastUpdateId" not in data:
                self.log_error(f"{symbol} 'lastUpdateId' 없음")
                return {}

            last_id = data["lastUpdateId"]
            
            # 매수/매도 호가 추출 및 변환
            bids = []
            asks = []
            
            for b in data.get("bids", []):
                try:
                    px, qty = float(b[0]), float(b[1])
                    if px > 0 and qty > 0:
                        bids.append([px, qty])
                except (ValueError, TypeError):
                    continue
                    
            for a in data.get("asks", []):
                try:
                    px, qty = float(a[0]), float(a[1])
                    if px > 0 and qty > 0:
                        asks.append([px, qty])
                except (ValueError, TypeError):
                    continue
            
            # 파싱 성공 카운트 증가
            self.stats["valid_parsed"] += 1
            
            # 결과 포맷팅
            return {
                "lastUpdateId": last_id,
                "bids": bids,
                "asks": asks
            }
        except Exception as e:
            self.log_error(f"{symbol} 스냅샷 파싱 실패: {e}")
            self.stats["processing_errors"] += 1
            return {} 