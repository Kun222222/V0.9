"""
오더북 증분 업데이트 검증 도구

이 모듈은 로그 파일에서 오더북 데이터를 읽어 증분 업데이트가 올바르게 적용되는지 검증합니다.
"""

import os
import json
import re
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Set, Any
import argparse
import copy

# 로그 형식을 위한 정규식 패턴
RAW_LOG_PATTERN = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\.\d{3} - .*?DEBUG.*?\[(.*?)\] (.*)'
ORDERBOOK_DATA_PATTERN = r'매수 \((\d+)\) / 매도 \((\d+)\) (.*)'

class OrderbookDeltaValidator:
    """
    오더북 증분 업데이트 검증 도구
    
    로그 파일을 읽어서 스냅샷과 델타 업데이트를 구분하고,
    델타 업데이트가 올바르게 적용되는지 검증합니다.
    """
    
    def __init__(self, log_file_path: str):
        """
        초기화
        
        Args:
            log_file_path: 로그 파일 경로
        """
        self.log_file_path = log_file_path
        self.exchange_name = self._extract_exchange_name(log_file_path)
        
        # 심볼별 오더북 상태
        self.orderbooks = {}
        
        # 심볼별 마지막 시퀀스/타임스탬프
        self.last_sequences = {}
        self.last_timestamps = {}
        
        # 검증 결과
        self.validation_errors = []
        self.missing_sequences = {}
        self.price_inversions = {}
        
        # 통계 데이터
        self.stats = {
            "total_messages": 0,
            "snapshots": 0,
            "deltas": 0,
            "invalid_deltas": 0,
            "processed_symbols": set()
        }
        
    def _extract_exchange_name(self, file_path: str) -> str:
        """
        파일 경로에서 거래소 이름 추출
        
        Args:
            file_path: 로그 파일 경로
            
        Returns:
            거래소 이름
        """
        base_name = os.path.basename(file_path)
        parts = base_name.split('_')
        if len(parts) >= 4:
            return parts[3]  # {날짜}_{시간}_{거래소}_raw_logger.log
        return "unknown"
        
    def validate(self, max_lines: Optional[int] = None, symbols: Optional[List[str]] = None) -> Dict:
        """
        로그 파일 읽고 검증
        
        Args:
            max_lines: 최대 처리할 줄 수 (None이면 전체)
            symbols: 검증할 심볼 목록 (None이면 전체)
            
        Returns:
            검증 결과
        """
        print(f"[{self.exchange_name}] 로그 파일 검증 시작: {self.log_file_path}")
        
        line_count = 0
        processed_symbols = set()
        
        start_time = time.time()
        
        with open(self.log_file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line_count += 1
                
                if max_lines and line_count > max_lines:
                    break
                    
                # 진행 상황 출력 (10만 줄마다)
                if line_count % 100000 == 0:
                    elapsed = time.time() - start_time
                    print(f"처리 중... {line_count} 줄 ({elapsed:.2f}초)")
                
                # 오더북 데이터 추출
                message_data = self._parse_log_line(line)
                if not message_data:
                    continue
                    
                message_time, message_content = message_data
                
                # 오더북 데이터 추출
                orderbook_data = self._extract_orderbook_data(message_content)
                if not orderbook_data:
                    continue
                    
                # 심볼 필터링
                symbol = orderbook_data.get("symbol")
                if not symbol or (symbols and symbol not in symbols):
                    continue
                    
                # 처리된 심볼 추가
                processed_symbols.add(symbol)
                
                # 데이터 타입 확인 (스냅샷 vs 델타)
                is_snapshot = orderbook_data.get("type") == "snapshot"
                
                # 통계 업데이트
                self.stats["total_messages"] += 1
                if is_snapshot:
                    self.stats["snapshots"] += 1
                else:
                    self.stats["deltas"] += 1
                
                # 오더북 업데이트 및 검증
                self._update_and_validate_orderbook(symbol, orderbook_data, is_snapshot)
        
        # 검증 완료 후 통계 및 결과 정리
        self.stats["processed_symbols"] = processed_symbols
        
        elapsed_time = time.time() - start_time
        
        # 결과 반환
        return {
            "exchange": self.exchange_name,
            "file_path": self.log_file_path,
            "lines_processed": line_count,
            "elapsed_time": elapsed_time,
            "stats": {
                "total_messages": self.stats["total_messages"],
                "snapshots": self.stats["snapshots"],
                "deltas": self.stats["deltas"],
                "invalid_deltas": self.stats["invalid_deltas"],
                "symbols_count": len(processed_symbols),
                "symbols": sorted(list(processed_symbols))
            },
            "errors": {
                "validation_errors": len(self.validation_errors),
                "missing_sequences": self.missing_sequences,
                "price_inversions": self.price_inversions
            }
        }
        
    def _parse_log_line(self, line: str) -> Optional[Tuple[str, str]]:
        """
        로그 라인 파싱
        
        Args:
            line: 로그 라인
            
        Returns:
            (시간, 메시지 내용) 튜플 또는 None
        """
        # 로그 형식 매칭
        match = re.search(RAW_LOG_PATTERN, line)
        if not match:
            return None
            
        timestamp = match.group(1)  # 로그 시간
        content = match.group(2)    # 로그 내용
        
        return timestamp, content
        
    def _extract_orderbook_data(self, content: str) -> Optional[Dict]:
        """
        오더북 데이터 추출
        
        Args:
            content: 메시지 내용
            
        Returns:
            파싱된 오더북 데이터 또는 None
        """
        # 오더북 데이터 패턴 매칭
        match = re.search(ORDERBOOK_DATA_PATTERN, content)
        if not match:
            # 바이낸스 델타 메시지인지 확인 (매수/매도 표시 없이 직접 JSON 형태로 오는 경우)
            try:
                if "{" in content and "}" in content:
                    json_str = content[content.find("{"):content.rfind("}")+1]
                    data = json.loads(json_str)
                    
                    # depthUpdate 이벤트인지 확인
                    if "e" in data and data["e"] == "depthUpdate":
                        # 델타 데이터 변환
                        return self._transform_binance_delta(data)
                return None
            except (json.JSONDecodeError, ValueError, KeyError):
                return None
        
        try:
            # 매수/매도 수량
            bid_count = int(match.group(1))
            ask_count = int(match.group(2))
            
            # JSON 데이터 파싱
            json_str = match.group(3)
            data = json.loads(json_str)
            
            # 타입 판별 (스냅샷 vs 델타)
            # stream_type이 있으면 업비트 스냅샷
            if "stream_type" in json_str and "SNAPSHOT" in json_str:
                data["type"] = "snapshot"
            else:
                data["type"] = "delta"
                
            return data
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            print(f"오더북 데이터 파싱 오류: {e}")
            return None
    
    def _transform_binance_delta(self, data: Dict) -> Dict:
        """
        바이낸스 델타 메시지를 통합 형식으로 변환
        
        Args:
            data: 원본 바이낸스 델타 데이터
            
        Returns:
            변환된 델타 데이터
        """
        symbol = data.get("s", "").replace("USDT", "")
        
        return {
            "symbol": symbol,
            "ts": data.get("E"),
            "seq": data.get("u"),
            "first_seq": data.get("U"),
            "bids": data.get("b", []),
            "asks": data.get("a", []),
            "type": "delta"
        }
    
    def _update_and_validate_orderbook(self, symbol: str, data: Dict, is_snapshot: bool) -> None:
        """
        오더북 업데이트 및 검증
        
        Args:
            symbol: 심볼
            data: 오더북 데이터
            is_snapshot: 스냅샷 여부
        """
        timestamp = data.get("ts")
        sequence = data.get("seq")
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        
        # 스냅샷인 경우 기존 오더북 초기화
        if is_snapshot:
            self.orderbooks[symbol] = {
                "bids": {float(bid[0]): float(bid[1]) for bid in bids},
                "asks": {float(ask[0]): float(ask[1]) for ask in asks},
                "timestamp": timestamp,
                "sequence": sequence
            }
            self.last_sequences[symbol] = sequence
            self.last_timestamps[symbol] = timestamp
            return
            
        # 델타인 경우 시퀀스 검증
        if symbol in self.last_sequences:
            last_seq = self.last_sequences[symbol]
            
            # 바이낸스의 경우 first_seq로 연속성 검증
            if "first_seq" in data and data["first_seq"]:
                first_seq = data["first_seq"]
                if last_seq + 1 > first_seq:
                    # 중복된 메시지, 무시
                    return
                elif last_seq + 1 < first_seq:
                    # 시퀀스 누락됨
                    gap = first_seq - (last_seq + 1)
                    error = f"시퀀스 갭: {symbol} - 마지막 {last_seq}, 현재 첫 {first_seq}, 갭 {gap}"
                    self.validation_errors.append(error)
                    
                    if symbol not in self.missing_sequences:
                        self.missing_sequences[symbol] = []
                    self.missing_sequences[symbol].append((last_seq, first_seq, gap))
                    
                    # 시퀀스 갭이 있으면 스냅샷을 다시 받아야 하지만, 
                    # 여기서는 검증 목적으로 계속 진행
            
            elif sequence <= last_seq:
                # 이전 시퀀스 메시지, 무시
                return
        
        # 오더북이 없으면 델타를 적용할 수 없음
        if symbol not in self.orderbooks:
            return
            
        # 기존 오더북 복사 (검증용)
        old_orderbook = copy.deepcopy(self.orderbooks[symbol])
        
        # 오더북 업데이트
        for bid in bids:
            price = float(bid[0])
            size = float(bid[1])
            
            if size == 0:
                # 수량이 0이면 해당 가격 호가 삭제
                self.orderbooks[symbol]["bids"].pop(price, None)
            else:
                # 수량이 있으면 추가 또는 업데이트
                self.orderbooks[symbol]["bids"][price] = size
                
        for ask in asks:
            price = float(ask[0])
            size = float(ask[1])
            
            if size == 0:
                # 수량이 0이면 해당 가격 호가 삭제
                self.orderbooks[symbol]["asks"].pop(price, None)
            else:
                # 수량이 있으면 추가 또는 업데이트
                self.orderbooks[symbol]["asks"][price] = size
                
        # 시퀀스 및 타임스탬프 업데이트
        self.orderbooks[symbol]["sequence"] = sequence
        self.orderbooks[symbol]["timestamp"] = timestamp
        
        self.last_sequences[symbol] = sequence
        self.last_timestamps[symbol] = timestamp
        
        # 가격 역전 검증
        if self.orderbooks[symbol]["bids"] and self.orderbooks[symbol]["asks"]:
            max_bid = max(self.orderbooks[symbol]["bids"].keys())
            min_ask = min(self.orderbooks[symbol]["asks"].keys())
            
            if max_bid >= min_ask:
                error = f"가격 역전: {symbol} - 최고 매수가 {max_bid} >= 최저 매도가 {min_ask}"
                self.validation_errors.append(error)
                
                if symbol not in self.price_inversions:
                    self.price_inversions[symbol] = []
                self.price_inversions[symbol].append((sequence, max_bid, min_ask))
                self.stats["invalid_deltas"] += 1
                
    def get_report(self) -> str:
        """
        검증 보고서 생성
        
        Returns:
            보고서 문자열
        """
        report = []
        report.append(f"\n---- {self.exchange_name} 오더북 검증 보고서 ----\n")
        
        # 기본 통계
        report.append(f"총 메시지: {self.stats['total_messages']}")
        report.append(f"스냅샷: {self.stats['snapshots']}")
        report.append(f"델타: {self.stats['deltas']}")
        report.append(f"잘못된 델타: {self.stats['invalid_deltas']}")
        report.append(f"심볼 수: {len(self.stats['processed_symbols'])}")
        report.append(f"심볼 목록: {', '.join(sorted(list(self.stats['processed_symbols'])))}")
        
        # 오류 요약
        report.append(f"\n오류 요약:")
        report.append(f"검증 오류: {len(self.validation_errors)}")
        report.append(f"시퀀스 누락 심볼: {len(self.missing_sequences)}")
        report.append(f"가격 역전 심볼: {len(self.price_inversions)}")
        
        # 시퀀스 누락 상세 정보
        if self.missing_sequences:
            report.append("\n시퀀스 누락 상세:")
            for symbol, gaps in self.missing_sequences.items():
                total_gap = sum(gap for _, _, gap in gaps)
                report.append(f"  {symbol}: {len(gaps)}건, 누락된 총 메시지: {total_gap}")
                
                # 최대 5개만 출력
                if len(gaps) > 5:
                    sample_gaps = gaps[:5]
                    report.append(f"    처음 5개 예시: {sample_gaps}")
                else:
                    report.append(f"    모든 갭: {gaps}")
        
        # 가격 역전 상세 정보
        if self.price_inversions:
            report.append("\n가격 역전 상세:")
            for symbol, inversions in self.price_inversions.items():
                report.append(f"  {symbol}: {len(inversions)}건")
                
                # 최대 5개만 출력
                if len(inversions) > 5:
                    sample_inversions = inversions[:5]
                    report.append(f"    처음 5개 예시: {sample_inversions}")
                else:
                    report.append(f"    모든 역전: {inversions}")
                    
        return "\n".join(report)


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="오더북 증분 업데이트 검증 도구")
    parser.add_argument("log_file", help="검증할 로그 파일 경로")
    parser.add_argument("--max-lines", type=int, help="최대 처리할 줄 수", default=None)
    parser.add_argument("--symbols", help="검증할 심볼 목록 (쉼표로 구분)", default=None)
    
    args = parser.parse_args()
    
    # 심볼 목록 파싱
    symbols = None
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
    
    # 검증 실행
    validator = OrderbookDeltaValidator(args.log_file)
    result = validator.validate(max_lines=args.max_lines, symbols=symbols)
    
    # 결과 출력
    print(validator.get_report())
    

if __name__ == "__main__":
    main() 