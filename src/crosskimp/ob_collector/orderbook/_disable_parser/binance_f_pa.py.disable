"""
바이낸스 선물 파서 클래스

이 모듈은 바이낸스 선물 거래소의 웹소켓 메시지를 파싱하는 클래스를 제공합니다.
"""

import json
import time
from typing import Dict, List, Optional, Tuple, Any

from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG
from crosskimp.ob_collector.orderbook.parser.base_parser import BaseParser

# ============================
# 바이낸스 선물 파서 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BINANCE_FUTURE.value  # 거래소 코드
BINANCE_FUTURE_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이낸스 선물 설정

# 바이낸스 선물 특화 설정
UPDATE_SPEED = BINANCE_FUTURE_CONFIG["update_speed"]  # 웹소켓 업데이트 속도 (100ms, 250ms, 500ms 중 선택)
SNAPSHOT_URL = BINANCE_FUTURE_CONFIG["api_urls"]["depth"]  # 스냅샷 URL
WS_BASE_URL = BINANCE_FUTURE_CONFIG["ws_base_url"]  # 웹소켓 기본 URL

class BinanceFutureParser(BaseParser):
    """
    바이낸스 선물 파서 클래스
    
    바이낸스 선물 거래소의 웹소켓 메시지를 파싱하는 클래스입니다.
    
    특징:
    - 메시지 형식: 델타 업데이트 방식
    - 시퀀스 ID 기반 관리
    - 심볼 형식: {symbol}USDT
    """
    def __init__(self):
        """
        바이낸스 선물 파서 초기화
        """
        super().__init__(EXCHANGE_CODE)
        self.snapshot_url = SNAPSHOT_URL  # 스냅샷 URL 설정
        self.ws_base_url = WS_BASE_URL  # 웹소켓 기본 URL 설정
        self.update_speed = UPDATE_SPEED  # 업데이트 속도 설정

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
            
            # 스트림 형식 처리
            if "stream" in data and "data" in data:
                data = data["data"]
            
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
            transaction_time = data.get("T", 0)  # 트랜잭션 시간 추가
            first_id = data.get("U", 0)
            final_id = data.get("u", 0)
            prev_final_id = data.get("pu", 0)  # 이전 final_update_id (바이낸스 선물 전용)
            
            # 호가 데이터 추출 및 변환
            bids, asks = self._extract_orderbook_data(b_data, a_data)
            
            # 유효성 검사 - 데이터가 완전히 없는 경우만 필터링
            if b_data is None or a_data is None:
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
                "transaction_time": transaction_time,  # 트랜잭션 시간 추가
                "first_update_id": first_id,
                "final_update_id": final_id,
                "prev_final_update_id": prev_final_id,  # 이전 final_update_id 추가
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
                # 수량이 0인 경우도 포함 (삭제 명령으로 처리)
                # 바이낸스 선물에서는 수량이 0인 경우 해당 가격의 주문을 삭제하라는 의미
                if px > 0:
                    bids.append([px, qty])
            except (ValueError, TypeError):
                self.log_warning(f"매수 호가 변환 실패: {b}")
                continue
        
        # 매도 호가 변환
        for a in a_data:
            try:
                px, qty = float(a[0]), float(a[1])
                # 수량이 0인 경우도 포함 (삭제 명령으로 처리)
                if px > 0:
                    asks.append([px, qty])
            except (ValueError, TypeError):
                self.log_warning(f"매도 호가 변환 실패: {a}")
                continue
                
        # 호가 데이터 로깅
        if not bids and not asks:
            self.log_warning("호가 데이터 없음 (bids와 asks 모두 비어있음)")
        elif not bids:
            self.log_debug(f"매수 호가 데이터 없음 (원본 데이터: {b_data[:5]}...)")
        elif not asks:
            self.log_debug(f"매도 호가 데이터 없음 (원본 데이터: {a_data[:5]}...)")
        else:
            self.log_debug(f"호가 데이터 추출 성공: bids={len(bids)}, asks={len(asks)}")
            
        return bids, asks

    def create_subscribe_message(self, symbols: List[str]) -> str:
        """
        구독 URL 생성
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            str: 구독 URL
        """
        # 스트림 목록 생성
        streams = []
        for sym in symbols:
            sym_lower = sym.lower()
            if not sym_lower.endswith("usdt"):
                sym_lower += "usdt"
            streams.append(f"{sym_lower}@depth@{self.update_speed}")
            
        # 스트림 URL 생성
        combined = "/".join(streams)
        return self.ws_base_url + combined

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
            raw_bids = []
            raw_asks = []
            
            for b in data.get("bids", []):
                try:
                    px, qty = float(b[0]), float(b[1])
                    if px > 0:  # 가격이 양수인 경우만 포함 (수량은 0이어도 포함)
                        raw_bids.append([px, qty])
                except (ValueError, TypeError):
                    continue
                    
            for a in data.get("asks", []):
                try:
                    px, qty = float(a[0]), float(a[1])
                    if px > 0:  # 가격이 양수인 경우만 포함 (수량은 0이어도 포함)
                        raw_asks.append([px, qty])
                except (ValueError, TypeError):
                    continue
            
            # 필터링 적용 (0인 수량도 포함하도록 직접 사용하지 않음)
            # bids, asks = self.filter_zero_values_array(raw_bids, raw_asks)
            bids, asks = raw_bids, raw_asks
            
            # 파싱 성공 카운트 증가
            self.stats["valid_parsed"] += 1
            
            # 결과 포맷팅
            return {
                "exchangename": EXCHANGE_CODE,
                "symbol": symbol,
                "bids": bids,
                "asks": asks,
                "timestamp": int(time.time() * 1000),
                "lastUpdateId": last_id,
                "type": "snapshot"
            }
        except Exception as e:
            self.log_error(f"{symbol} 스냅샷 파싱 실패: {e}")
            self.stats["processing_errors"] += 1
            return {}

    def parse_message_dict(self, data: dict) -> Optional[Dict[str, Any]]:
        """
        이미 파싱된 딕셔너리 메시지 처리
        
        Args:
            data: 파싱된 웹소켓 메시지 딕셔너리
            
        Returns:
            Optional[Dict[str, Any]]: 파싱된 오더북 데이터 또는 None
        """
        try:
            # 통계 업데이트
            self.stats["total_parsed"] += 1
            
            # 오더북 메시지 타입 확인
            if data.get("e") != "depthUpdate":
                self.stats["invalid_parsed"] += 1
                self.log_error(f"메시지 타입 오류: {data.get('e')}")
                return None
                
            # 심볼 추출
            symbol_raw = data.get("s", "")
            symbol = symbol_raw.replace("USDT", "").upper()
            
            # 데이터 추출
            b_data = data.get("b", [])
            a_data = data.get("a", [])
            event_time = data.get("E", 0)
            transaction_time = data.get("T", 0)  # 트랜잭션 시간 추가
            first_id = data.get("U", 0)
            final_id = data.get("u", 0)
            prev_final_id = data.get("pu", 0)  # 이전 final_update_id (바이낸스 선물 전용)
            
            # 호가 데이터 추출 및 변환
            bids, asks = self._extract_orderbook_data(b_data, a_data)
            
            # 유효성 검사 - 데이터가 완전히 없는 경우만 필터링
            if b_data is None or a_data is None:
                self.stats["invalid_parsed"] += 1
                self.log_error(f"{symbol} 호가 데이터 없음: bids={b_data}, asks={a_data}")
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
                "transaction_time": transaction_time,  # 트랜잭션 시간 추가
                "first_update_id": first_id,
                "final_update_id": final_id,
                "prev_final_update_id": prev_final_id,  # 이전 final_update_id 추가
                "sequence": final_id,
                "type": "delta",
                # 원본 필드도 유지
                "U": first_id,
                "u": final_id,
                "b": b_data,
                "a": a_data
            }
            
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}, data={str(data)[:200]}...")
            self.stats["processing_errors"] += 1
            return None 