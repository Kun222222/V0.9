"""
FlatBuffers 직렬화 모듈

이 모듈은 오더북 데이터를 FlatBuffers 형식으로 직렬화하는 기능을 제공합니다.
"""

import time
import os
import json
import base64
import struct
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

import flatbuffers
from crosskimp.ob_collector.cpp.flatbuffers.OrderBookData import OrderBook, PriceLevel
from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants_v3 import LOGS_DIR

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# 직렬화 데이터 저장 디렉토리
SERIALIZED_DATA_DIR = LOGS_DIR / 'orderbook_data'  # 기본 로그 폴더 아래 새 폴더 사용

# 디렉토리 생성
os.makedirs(SERIALIZED_DATA_DIR, exist_ok=True)

# FlatBuffers 파일 식별자 (4바이트)
FILE_IDENTIFIER = b'ORDB'

class OrderBookSerializer:
    """
    오더북 데이터 직렬화 클래스
    
    이 클래스는 오더북 데이터를 FlatBuffers 형식으로 직렬화하는 기능을 제공합니다.
    """
    
    def __init__(self, initial_size: int = 1024 * 1024):
        """
        직렬화 클래스 초기화
        
        Args:
            initial_size: 초기 버퍼 크기 (바이트)
        """
        self.builder = flatbuffers.Builder(initial_size)
        
        # 메트릭 초기화
        self.metrics = {
            "serialize_count": 0,
            "serialize_errors": 0,
            "total_bytes_serialized": 0,
            "last_serialize_time": 0,
            "max_data_size": 0,
            "avg_serialization_time_ms": 0,
        }
        self._total_serialization_time = 0
        
        # 거래소별 디렉토리 생성
        self.exchange_dirs = {}
    
    def _ensure_exchange_directory(self, exchange_name: str) -> str:
        """
        거래소별 디렉토리 생성 및 경로 반환
        
        Args:
            exchange_name: 거래소 이름
            
        Returns:
            str: 디렉토리 경로
        """
        if exchange_name not in self.exchange_dirs:
            exchange_dir = os.path.join(SERIALIZED_DATA_DIR, exchange_name)
            os.makedirs(exchange_dir, exist_ok=True)
            self.exchange_dirs[exchange_name] = exchange_dir
        
        return self.exchange_dirs[exchange_name]
    
    def _save_serialized_data(self, exchange_name: str, symbol: str, timestamp: int, serialized_data: bytes, original_data: Dict[str, Any]) -> None:
        """
        직렬화된 데이터를 파일로 저장
        
        Args:
            exchange_name: 거래소 이름
            symbol: 심볼
            timestamp: 타임스탬프
            serialized_data: 직렬화된 데이터
            original_data: 원본 데이터
        """
        try:
            # 거래소 디렉토리 확인
            exchange_dir = self._ensure_exchange_directory(exchange_name)
            
            # 현재 날짜로 디렉토리 생성
            date_str = datetime.now().strftime("%Y%m%d")
            date_dir = os.path.join(exchange_dir, date_str)
            os.makedirs(date_dir, exist_ok=True)
            
            # 파일명 생성 (거래소_심볼_타임스탬프.bin)
            file_name = f"{exchange_name}_{symbol}_{timestamp}.bin"
            file_path = os.path.join(date_dir, file_name)
            
            # 바이너리 파일로 저장
            with open(file_path, 'wb') as f:
                f.write(serialized_data)
            
            # 메타데이터 파일 생성 (JSON 형식)
            meta_file_name = f"{exchange_name}_{symbol}_{timestamp}.json"
            meta_file_path = os.path.join(date_dir, meta_file_name)
            
            # 원본 데이터에서 필요한 정보만 추출
            meta_data = {
                "exchange": exchange_name,
                "symbol": symbol,
                "timestamp": timestamp,
                "sequence": original_data.get("sequence", 0),
                "serialized_size": len(serialized_data),
                "bids_count": len(original_data.get("bids", [])),
                "asks_count": len(original_data.get("asks", [])),
                "bids": original_data.get("bids", [])[:5],  # 상위 5개만 저장
                "asks": original_data.get("asks", [])[:5],  # 상위 5개만 저장
                "serialized_data_b64": base64.b64encode(serialized_data[:100]).decode('utf-8') + "..." if len(serialized_data) > 100 else base64.b64encode(serialized_data).decode('utf-8')
            }
            
            with open(meta_file_path, 'w', encoding='utf-8') as f:
                json.dump(meta_data, f, indent=2)
                
            logger.info(f"직렬화 데이터 저장 완료: {file_path} (크기: {len(serialized_data)} 바이트)")
            
        except Exception as e:
            logger.error(f"직렬화 데이터 저장 실패: {str(e)}", exc_info=True)
    
    def serialize_orderbook(self, orderbook_data: Dict[str, Any]) -> Optional[bytes]:
        """
        오더북 데이터를 FlatBuffers 형식으로 직렬화
        
        Args:
            orderbook_data: 오더북 데이터 딕셔너리
            
        Returns:
            Optional[bytes]: 직렬화된 데이터 또는 None (실패 시)
        """
        try:
            start_time = time.time()
            
            # 빌더 초기화 - Clear() 대신 새 Builder 객체 생성
            self.builder = flatbuffers.Builder(1024 * 1024)
            
            # 거래소 이름과 심볼이 있는지 확인
            if "exchangename" not in orderbook_data or not orderbook_data["exchangename"]:
                logger.error("직렬화 오류: 거래소 이름이 없습니다")
                self.metrics["serialize_errors"] += 1
                return None
                
            if "symbol" not in orderbook_data or not orderbook_data["symbol"]:
                logger.error("직렬화 오류: 심볼이 없습니다")
                self.metrics["serialize_errors"] += 1
                return None
            
            # 필드 값 검증 및 로깅
            exchange_name_str = orderbook_data.get("exchangename", "")
            symbol_str = orderbook_data.get("symbol", "")
            timestamp = orderbook_data.get("timestamp", int(time.time() * 1000))
            sequence = orderbook_data.get("sequence", 0)
            
            logger.debug(f"직렬화 시작: {exchange_name_str} {symbol_str} (타임스탬프: {timestamp}, 시퀀스: {sequence})")
            
            # 문자열 데이터 생성
            exchange_name = self.builder.CreateString(exchange_name_str)
            symbol = self.builder.CreateString(symbol_str)
            
            # 매수/매도 호가 데이터 생성
            bids_data = orderbook_data.get("bids", [])
            asks_data = orderbook_data.get("asks", [])
            
            if not bids_data or not asks_data:
                logger.error(f"직렬화 오류: 매수 또는 매도 데이터가 없습니다. 매수: {len(bids_data)}, 매도: {len(asks_data)}")
                self.metrics["serialize_errors"] += 1
                return None
            
            logger.debug(f"직렬화 데이터 준비: 매수 {len(bids_data)}개, 매도 {len(asks_data)}개")
            
            # 매수 호가 벡터 생성
            bid_offsets = []
            for price, quantity in bids_data:
                PriceLevel.PriceLevelStart(self.builder)
                PriceLevel.PriceLevelAddPrice(self.builder, float(price))
                PriceLevel.PriceLevelAddQuantity(self.builder, float(quantity))
                bid_offsets.append(PriceLevel.PriceLevelEnd(self.builder))
            
            OrderBook.StartBidsVector(self.builder, len(bid_offsets))
            for offset in reversed(bid_offsets):
                self.builder.PrependUOffsetTRelative(offset)
            bids = self.builder.EndVector()
            
            # 매도 호가 벡터 생성
            ask_offsets = []
            for price, quantity in asks_data:
                PriceLevel.PriceLevelStart(self.builder)
                PriceLevel.PriceLevelAddPrice(self.builder, float(price))
                PriceLevel.PriceLevelAddQuantity(self.builder, float(quantity))
                ask_offsets.append(PriceLevel.PriceLevelEnd(self.builder))
            
            OrderBook.StartAsksVector(self.builder, len(ask_offsets))
            for offset in reversed(ask_offsets):
                self.builder.PrependUOffsetTRelative(offset)
            asks = self.builder.EndVector()
            
            # OrderBook 객체 생성
            OrderBook.OrderBookStart(self.builder)
            OrderBook.OrderBookAddExchangeName(self.builder, exchange_name)
            OrderBook.OrderBookAddSymbol(self.builder, symbol)
            OrderBook.OrderBookAddTimestamp(self.builder, timestamp)
            OrderBook.OrderBookAddSequence(self.builder, sequence)
            OrderBook.OrderBookAddBids(self.builder, bids)
            OrderBook.OrderBookAddAsks(self.builder, asks)
            orderbook = OrderBook.OrderBookEnd(self.builder)
            
            # 파일 식별자 추가
            self.builder.Finish(orderbook, file_identifier=FILE_IDENTIFIER)
            
            # 직렬화된 데이터 가져오기
            serialized_data = self.builder.Output()
            
            # 헤더 디버깅 로그
            header_hex = ' '.join([f'{b:02x}' for b in serialized_data[:16]])
            file_id_str = ''.join([chr(b) for b in serialized_data[4:8]])
            logger.debug(f"직렬화 완료: 크기={len(serialized_data)} 바이트, 헤더={header_hex}, 파일 ID={file_id_str}")
            
            # 파일 식별자 검증
            if serialized_data[4:8] != FILE_IDENTIFIER:
                logger.error(f"파일 식별자 오류: 예상={FILE_IDENTIFIER}, 실제={serialized_data[4:8]}")
                # 파일 식별자 강제 설정 (디버깅용)
                serialized_data_list = bytearray(serialized_data)
                serialized_data_list[4:8] = FILE_IDENTIFIER
                serialized_data = bytes(serialized_data_list)
                logger.debug(f"파일 식별자 수정 후: {' '.join([f'{b:02x}' for b in serialized_data[4:8]])}")
            
            # 직렬화된 데이터를 파일로 저장 (1분에 1개씩만 저장)
            current_minute = int(time.time() / 60)
            
            # 1분에 1개씩만 저장 (시퀀스 번호로 구분)
            save_key = f"{exchange_name_str}_{symbol_str}_{current_minute}"
            if not hasattr(self, '_last_saved') or save_key not in self._last_saved:
                if not hasattr(self, '_last_saved'):
                    self._last_saved = {}
                self._last_saved[save_key] = sequence
                
                # 직렬화된 데이터 저장
                self._save_serialized_data(
                    exchange_name=exchange_name_str,
                    symbol=symbol_str,
                    timestamp=timestamp,
                    serialized_data=serialized_data,
                    original_data=orderbook_data
                )
            
            # 메트릭 업데이트
            end_time = time.time()
            serialization_time_ms = (end_time - start_time) * 1000
            
            self.metrics["serialize_count"] += 1
            self.metrics["total_bytes_serialized"] += len(serialized_data)
            self.metrics["last_serialize_time"] = end_time
            self.metrics["max_data_size"] = max(self.metrics["max_data_size"], len(serialized_data))
            
            self._total_serialization_time += serialization_time_ms
            self.metrics["avg_serialization_time_ms"] = (
                self._total_serialization_time / self.metrics["serialize_count"]
                if self.metrics["serialize_count"] > 0 else 0
            )
            
            return serialized_data
            
        except Exception as e:
            logger.error(f"오더북 직렬화 실패: {str(e)}", exc_info=True)
            self.metrics["serialize_errors"] += 1
            return None
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        메트릭 정보 반환
        
        Returns:
            Dict[str, Any]: 메트릭 정보
        """
        return self.metrics


# 싱글톤 인스턴스
_orderbook_serializer = None

def get_orderbook_serializer() -> OrderBookSerializer:
    """
    오더북 직렬화 클래스 싱글톤 인스턴스 반환
    
    Returns:
        OrderBookSerializer: 오더북 직렬화 클래스 인스턴스
    """
    global _orderbook_serializer
    if _orderbook_serializer is None:
        _orderbook_serializer = OrderBookSerializer()
    return _orderbook_serializer 