#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
고급 오더북 분석 도구
- 가격 역전 현상 확인
- 시퀀스 역전 확인
- 오더북 뎁스 유지 여부 확인
- 증분 업데이트 정확성 검증
"""

import os
import json
import re
import time
import argparse
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
import copy
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.dates as mdates

from delta_validator import OrderbookDeltaValidator, RAW_LOG_PATTERN, ORDERBOOK_DATA_PATTERN

class AdvancedOrderbookAnalyzer:
    """
    고급 오더북 분석기
    
    로그 파일에서 오더북 데이터를 추출하여 다음 사항을 분석:
    1. 가격 역전 현상
    2. 시퀀스 역전
    3. 오더북 뎁스 유지 여부
    4. 증분 업데이트 정확성
    """
    
    def __init__(self, log_file_path: str, symbol: str, max_lines: Optional[int] = None):
        """
        초기화
        
        Args:
            log_file_path: 로그 파일 경로
            symbol: 분석할 심볼
            max_lines: 최대 처리할 줄 수
        """
        self.log_file_path = log_file_path
        self.symbol = symbol
        self.max_lines = max_lines
        self.exchange_name = self._extract_exchange_name(log_file_path)
        
        # 데이터 저장 변수
        self.timestamps = []
        self.sequences = []
        self.max_bids = []
        self.min_asks = []
        self.spreads = []
        self.bid_depths = []
        self.ask_depths = []
        
        # 분석 결과 저장
        self.price_inversions = []  # 가격 역전 발생 데이터 (timestamp, max_bid, min_ask)
        self.sequence_inversions = []  # 시퀀스 역전 발생 데이터 (timestamp, prev_seq, curr_seq)
        self.depth_violations = []  # 뎁스 위반 데이터 (timestamp, bid_depth, ask_depth)
        self.delta_errors = []  # 증분 업데이트 오류 (timestamp, error_type, details)
        
        # 오더북 상태 관리
        self.orderbook = None  # 현재 오더북 상태
        self.prev_orderbook = None  # 이전 오더북 상태
        self.last_sequence = None  # 마지막 시퀀스
        
    def _extract_exchange_name(self, file_path: str) -> str:
        """파일 경로에서 거래소 이름 추출"""
        base_name = os.path.basename(file_path)
        parts = base_name.split('_')
        if len(parts) >= 4:
            return parts[3]
        return "unknown"
        
    def analyze(self) -> bool:
        """
        로그 파일 분석 실행
        
        Returns:
            분석 성공 여부
        """
        print(f"[{self.exchange_name}] {self.symbol} 심볼의 오더북 데이터 분석 시작")
        
        # 오더북 검증 도구 초기화 (로직 재사용)
        validator = OrderbookDeltaValidator(self.log_file_path)
        
        line_count = 0
        found_data = False
        snapshot_count = 0
        delta_count = 0
        
        start_time = time.time()
        
        with open(self.log_file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line_count += 1
                
                if self.max_lines and line_count > self.max_lines:
                    break
                    
                # 진행 상황 출력 (10만 줄마다)
                if line_count % 100000 == 0:
                    elapsed = time.time() - start_time
                    print(f"처리 중... {line_count} 줄 ({elapsed:.2f}초)")
                
                # 오더북 데이터 추출
                message_data = validator._parse_log_line(line)
                if not message_data:
                    continue
                    
                message_time, message_content = message_data
                
                # 오더북 데이터 추출
                orderbook_data = validator._extract_orderbook_data(message_content)
                if not orderbook_data:
                    continue
                    
                # 심볼 필터링
                data_symbol = orderbook_data.get("symbol")
                if not data_symbol or data_symbol != self.symbol:
                    continue
                    
                found_data = True
                
                # 데이터 타입 확인 (스냅샷 vs 델타)
                is_snapshot = orderbook_data.get("type") == "snapshot"
                if is_snapshot:
                    snapshot_count += 1
                else:
                    delta_count += 1
                
                # 데이터 처리 및 분석
                self._process_orderbook_data(orderbook_data, is_snapshot)
        
        elapsed_time = time.time() - start_time
        print(f"\n데이터 분석 완료 ({elapsed_time:.2f}초):")
        print(f"  - 처리한 줄 수: {line_count}")
        print(f"  - 발견한 메시지 수: {len(self.timestamps)}")
        print(f"  - 스냅샷 수: {snapshot_count}")
        print(f"  - 델타 수: {delta_count}")
        print(f"  - 가격 역전 현상: {len(self.price_inversions)}건")
        print(f"  - 시퀀스 역전 현상: {len(self.sequence_inversions)}건")
        print(f"  - 뎁스 유지 위반: {len(self.depth_violations)}건")
        print(f"  - 증분 업데이트 오류: {len(self.delta_errors)}건")
        
        return found_data
        
    def _process_orderbook_data(self, data: Dict, is_snapshot: bool) -> None:
        """
        오더북 데이터 처리 및 분석
        
        Args:
            data: 오더북 데이터
            is_snapshot: 스냅샷 여부
        """
        timestamp = data.get("ts")
        sequence = data.get("seq")
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        
        if not bids or not asks:
            return
            
        # 타임스탬프, 시퀀스 저장
        self.timestamps.append(timestamp)
        self.sequences.append(sequence)
        
        # 최고 매수가, 최저 매도가 계산
        max_bid = max([float(bid[0]) for bid in bids])
        min_ask = min([float(ask[0]) for ask in asks])
        spread = min_ask - max_bid
        
        # 데이터 저장
        self.max_bids.append(max_bid)
        self.min_asks.append(min_ask)
        self.spreads.append(spread)
        self.bid_depths.append(len(bids))
        self.ask_depths.append(len(asks))
        
        # 1. 가격 역전 확인
        if max_bid >= min_ask:
            self.price_inversions.append((timestamp, max_bid, min_ask))
            
        # 2. 시퀀스 역전 확인
        if self.last_sequence is not None and sequence < self.last_sequence:
            self.sequence_inversions.append((timestamp, self.last_sequence, sequence))
        
        # 현재 시퀀스 저장
        self.last_sequence = sequence
        
        # 3. 오더북 뎁스 확인 (10 미만이면 위반)
        if len(bids) < 10 or len(asks) < 10:
            self.depth_violations.append((timestamp, len(bids), len(asks)))
            
        # 4. 증분 업데이트 검증
        if not is_snapshot and self.orderbook is not None:
            self._verify_delta_update(data, timestamp)
            
        # 현재 오더북 상태 업데이트
        self.prev_orderbook = self.orderbook
        
        # 오더북 업데이트
        if is_snapshot:
            # 스냅샷은 전체 교체
            self.orderbook = {
                "bids": {float(bid[0]): float(bid[1]) for bid in bids},
                "asks": {float(ask[0]): float(ask[1]) for ask in asks},
                "timestamp": timestamp,
                "sequence": sequence
            }
        elif self.orderbook is not None:
            # 델타는 증분 업데이트
            # 복사 후 업데이트 (검증용)
            self.orderbook = copy.deepcopy(self.orderbook)
            
            # 매수 호가 업데이트
            for bid in bids:
                price = float(bid[0])
                size = float(bid[1])
                
                if size == 0:
                    # 수량이 0이면 해당 가격 호가 삭제
                    self.orderbook["bids"].pop(price, None)
                else:
                    # 수량이 있으면 추가 또는 업데이트
                    self.orderbook["bids"][price] = size
                    
            # 매도 호가 업데이트
            for ask in asks:
                price = float(ask[0])
                size = float(ask[1])
                
                if size == 0:
                    # 수량이 0이면 해당 가격 호가 삭제
                    self.orderbook["asks"].pop(price, None)
                else:
                    # 수량이 있으면 추가 또는 업데이트
                    self.orderbook["asks"][price] = size
                    
            # 타임스탬프, 시퀀스 업데이트
            self.orderbook["timestamp"] = timestamp
            self.orderbook["sequence"] = sequence
    
    def _verify_delta_update(self, delta_data: Dict, timestamp: int) -> None:
        """
        증분 업데이트 검증
        
        Args:
            delta_data: 델타 데이터
            timestamp: 타임스탬프
        """
        # 기본 검증 (시퀀스 연속성)
        first_seq = delta_data.get("first_seq")
        if first_seq and self.orderbook["sequence"] + 1 < first_seq:
            # 시퀀스 갭 발견
            gap = first_seq - (self.orderbook["sequence"] + 1)
            self.delta_errors.append((timestamp, "sequence_gap", {
                "last_seq": self.orderbook["sequence"], 
                "first_seq": first_seq,
                "gap": gap
            }))
    
    def visualize(self, output_dir: Optional[str] = None) -> None:
        """
        분석 결과 시각화
        
        Args:
            output_dir: 결과물 저장 디렉토리
        """
        if not self.timestamps:
            print("시각화할 데이터가 없습니다.")
            return
            
        # 결과 저장 디렉토리 생성
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        # 날짜/시간 포맷 변환
        # None 값을 필터링하고 현재 시간으로 대체
        filtered_timestamps = []
        filtered_max_bids = []
        filtered_min_asks = []
        filtered_spreads = []
        filtered_sequences = []
        
        now = time.time()
        for i, ts in enumerate(self.timestamps):
            if ts is not None:
                filtered_timestamps.append(ts)
                if i < len(self.max_bids):
                    filtered_max_bids.append(self.max_bids[i])
                if i < len(self.min_asks):
                    filtered_min_asks.append(self.min_asks[i])
                if i < len(self.spreads):
                    filtered_spreads.append(self.spreads[i])
                if i < len(self.sequences):
                    filtered_sequences.append(self.sequences[i])
        
        if not filtered_timestamps:
            print("시각화할 유효한 타임스탬프 데이터가 없습니다.")
            return
            
        times = [datetime.fromtimestamp(ts/1000) if ts > 1000000000000 else datetime.fromtimestamp(ts) 
                 for ts in filtered_timestamps]
        
        # 날짜 포맷터 설정
        date_fmt = mdates.DateFormatter('%H:%M:%S')
        
        # 1. 가격 추이 및 가격 역전 표시
        plt.figure(figsize=(12, 6))
        plt.plot(times, filtered_max_bids, 'g-', label='최고 매수가', linewidth=1)
        plt.plot(times, filtered_min_asks, 'r-', label='최저 매도가', linewidth=1)
        
        # 가격 역전 지점 표시
        if self.price_inversions:
            inversion_times = [datetime.fromtimestamp(ts/1000) if ts > 1000000000000 else datetime.fromtimestamp(ts) 
                              for ts, _, _ in self.price_inversions]
            inversion_values = [bid for _, bid, _ in self.price_inversions]
            plt.scatter(inversion_times, inversion_values, c='purple', marker='x', s=100, label='가격 역전 발생')
        
        plt.title(f'{self.exchange_name} {self.symbol} 가격 변화 및 역전 현상')
        plt.xlabel('시간')
        plt.ylabel('가격')
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.gca().xaxis.set_major_formatter(date_fmt)
        plt.tight_layout()
        
        if output_dir:
            plt.savefig(os.path.join(output_dir, f'{self.exchange_name}_{self.symbol}_price_inversion.png'))
        else:
            plt.show()
            
        # 2. 시퀀스 변화 및 역전 표시
        plt.figure(figsize=(12, 6))
        plt.plot(times, filtered_sequences, 'b-', linewidth=1)
        
        # 시퀀스 역전 지점 표시
        if self.sequence_inversions:
            inversion_times = [datetime.fromtimestamp(ts/1000) if ts > 1000000000000 else datetime.fromtimestamp(ts) 
                              for ts, _, _ in self.sequence_inversions]
            inversion_values = [curr_seq for _, _, curr_seq in self.sequence_inversions]
            plt.scatter(inversion_times, inversion_values, c='red', marker='x', s=100, label='시퀀스 역전 발생')
        
        plt.title(f'{self.exchange_name} {self.symbol} 시퀀스 변화 및 역전 현상')
        plt.xlabel('시간')
        plt.ylabel('시퀀스 번호')
        plt.grid(True, alpha=0.3)
        if self.sequence_inversions:
            plt.legend()
        plt.gca().xaxis.set_major_formatter(date_fmt)
        plt.tight_layout()
        
        if output_dir:
            plt.savefig(os.path.join(output_dir, f'{self.exchange_name}_{self.symbol}_sequence_inversion.png'))
        else:
            plt.show()
            
        # 3. 오더북 뎁스 변화 및 위반 표시
        plt.figure(figsize=(12, 6))
        
        # 필터링된 뎁스 데이터 준비
        filtered_bid_depths = []
        filtered_ask_depths = []
        for i, ts in enumerate(self.timestamps):
            if ts is not None and i < len(self.bid_depths):
                filtered_bid_depths.append(self.bid_depths[i])
            if ts is not None and i < len(self.ask_depths):
                filtered_ask_depths.append(self.ask_depths[i])
        
        plt.plot(times, filtered_bid_depths, 'g-', label='매수 뎁스', linewidth=1)
        plt.plot(times, filtered_ask_depths, 'r-', label='매도 뎁스', linewidth=1)
        
        # 뎁스 위반 지점 표시
        if self.depth_violations:
            violation_times = [datetime.fromtimestamp(ts/1000) if ts > 1000000000000 else datetime.fromtimestamp(ts) 
                              for ts, _, _ in self.depth_violations]
            
            # 매수 뎁스 위반
            bid_violations = [(t, b) for (t, b, a) in self.depth_violations if b < 10]
            if bid_violations:
                bid_times = [datetime.fromtimestamp(ts/1000) if ts > 1000000000000 else datetime.fromtimestamp(ts) 
                            for ts, _ in bid_violations]
                bid_values = [b for _, b in bid_violations]
                plt.scatter(bid_times, bid_values, c='blue', marker='o', s=50, label='매수 뎁스 부족')
            
            # 매도 뎁스 위반
            ask_violations = [(t, a) for (t, b, a) in self.depth_violations if a < 10]
            if ask_violations:
                ask_times = [datetime.fromtimestamp(ts/1000) if ts > 1000000000000 else datetime.fromtimestamp(ts) 
                            for ts, _ in ask_violations]
                ask_values = [a for _, a in ask_violations]
                plt.scatter(ask_times, ask_values, c='orange', marker='o', s=50, label='매도 뎁스 부족')
        
        # 최소 뎁스 기준선 표시
        plt.axhline(y=10, color='gray', linestyle='--', alpha=0.7, label='최소 뎁스 기준')
        
        plt.title(f'{self.exchange_name} {self.symbol} 오더북 뎁스 및 위반 현상')
        plt.xlabel('시간')
        plt.ylabel('호가 수')
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.gca().xaxis.set_major_formatter(date_fmt)
        plt.tight_layout()
        
        if output_dir:
            plt.savefig(os.path.join(output_dir, f'{self.exchange_name}_{self.symbol}_depth_violations.png'))
        else:
            plt.show()
            
        # 4. 증분 업데이트 오류 및 스프레드 변화
        plt.figure(figsize=(12, 6))
        plt.plot(times, filtered_spreads, 'b-', linewidth=1)
        
        # 증분 업데이트 오류 지점 표시
        if self.delta_errors:
            # 유효한 타임스탬프만 처리
            valid_errors = []
            for ts, error_type, details in self.delta_errors:
                if ts is not None:
                    try:
                        idx = self.timestamps.index(ts)
                        if idx < len(filtered_spreads):  # 인덱스가 유효한지 확인
                            valid_errors.append((ts, filtered_spreads[idx]))
                    except ValueError:
                        # 타임스탬프가 리스트에 없는 경우 무시
                        pass
                        
            if valid_errors:
                error_times = [datetime.fromtimestamp(ts/1000) if ts > 1000000000000 else datetime.fromtimestamp(ts) 
                              for ts, _ in valid_errors]
                error_values = [spread for _, spread in valid_errors]
                plt.scatter(error_times, error_values, c='red', marker='x', s=100, label='증분 업데이트 오류')
        
        plt.title(f'{self.exchange_name} {self.symbol} 스프레드 변화 및 증분 업데이트 오류')
        plt.xlabel('시간')
        plt.ylabel('스프레드')
        plt.grid(True, alpha=0.3)
        if self.delta_errors:
            plt.legend()
        plt.gca().xaxis.set_major_formatter(date_fmt)
        plt.tight_layout()
        
        if output_dir:
            plt.savefig(os.path.join(output_dir, f'{self.exchange_name}_{self.symbol}_delta_errors.png'))
        else:
            plt.show()
            
        # 5. 종합 분석 결과 저장
        self._save_analysis_report(output_dir)
    
    def _save_analysis_report(self, output_dir: Optional[str]) -> None:
        """
        분석 보고서 저장
        
        Args:
            output_dir: 결과물 저장 디렉토리
        """
        if not output_dir:
            return
            
        report = {
            "exchange": self.exchange_name,
            "symbol": self.symbol,
            "analysis_time": datetime.now().isoformat(),
            "data_count": len(self.timestamps),
            "summary": {
                "price_inversions": len(self.price_inversions),
                "sequence_inversions": len(self.sequence_inversions),
                "depth_violations": len(self.depth_violations),
                "delta_errors": len(self.delta_errors)
            },
            "details": {
                "price_inversions": [
                    {
                        "timestamp": ts,
                        "datetime": datetime.fromtimestamp(ts/1000).isoformat() if ts > 1000000000000 else datetime.fromtimestamp(ts).isoformat(),
                        "max_bid": max_bid,
                        "min_ask": min_ask
                    } for ts, max_bid, min_ask in self.price_inversions[:100]  # 최대 100개만 저장
                ],
                "sequence_inversions": [
                    {
                        "timestamp": ts,
                        "datetime": datetime.fromtimestamp(ts/1000).isoformat() if ts > 1000000000000 else datetime.fromtimestamp(ts).isoformat(),
                        "prev_seq": prev_seq,
                        "curr_seq": curr_seq
                    } for ts, prev_seq, curr_seq in self.sequence_inversions[:100]
                ],
                "depth_violations": [
                    {
                        "timestamp": ts,
                        "datetime": datetime.fromtimestamp(ts/1000).isoformat() if ts > 1000000000000 else datetime.fromtimestamp(ts).isoformat(),
                        "bid_depth": bid_depth,
                        "ask_depth": ask_depth
                    } for ts, bid_depth, ask_depth in self.depth_violations[:100]
                ],
                "delta_errors": [
                    {
                        "timestamp": ts,
                        "datetime": datetime.fromtimestamp(ts/1000).isoformat() if ts > 1000000000000 else datetime.fromtimestamp(ts).isoformat(),
                        "error_type": error_type,
                        "details": details
                    } for ts, error_type, details in self.delta_errors[:100]
                ]
            }
        }
        
        # JSON 파일로 저장
        report_path = os.path.join(output_dir, f'{self.exchange_name}_{self.symbol}_analysis_report.json')
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
            
        print(f"분석 보고서가 {report_path}에 저장되었습니다.")
        
        # 간단한 요약 파일 생성 (텍스트)
        summary_path = os.path.join(output_dir, f'{self.exchange_name}_{self.symbol}_summary.txt')
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write(f"===== {self.exchange_name} {self.symbol} 오더북 분석 요약 =====\n\n")
            f.write(f"분석 시간: {datetime.now().isoformat()}\n")
            f.write(f"데이터 수: {len(self.timestamps)}\n\n")
            
            f.write("== 핵심 검증 결과 ==\n")
            f.write(f"1. 가격 역전 현상: {len(self.price_inversions)}건\n")
            f.write(f"2. 시퀀스 역전 현상: {len(self.sequence_inversions)}건\n")
            f.write(f"3. 뎁스 유지 위반: {len(self.depth_violations)}건\n")
            f.write(f"4. 증분 업데이트 오류: {len(self.delta_errors)}건\n\n")
            
            # 가격 역전 상세
            if self.price_inversions:
                f.write("== 가격 역전 상세 (최대 5건) ==\n")
                for i, (ts, max_bid, min_ask) in enumerate(self.price_inversions[:5]):
                    dt = datetime.fromtimestamp(ts/1000) if ts > 1000000000000 else datetime.fromtimestamp(ts)
                    f.write(f"{i+1}. {dt}: 최고 매수가 {max_bid} >= 최저 매도가 {min_ask}\n")
                if len(self.price_inversions) > 5:
                    f.write(f"... 외 {len(self.price_inversions)-5}건\n")
                f.write("\n")
            
            # 시퀀스 역전 상세
            if self.sequence_inversions:
                f.write("== 시퀀스 역전 상세 (최대 5건) ==\n")
                for i, (ts, prev_seq, curr_seq) in enumerate(self.sequence_inversions[:5]):
                    dt = datetime.fromtimestamp(ts/1000) if ts > 1000000000000 else datetime.fromtimestamp(ts)
                    f.write(f"{i+1}. {dt}: 이전 시퀀스 {prev_seq} > 현재 시퀀스 {curr_seq}\n")
                if len(self.sequence_inversions) > 5:
                    f.write(f"... 외 {len(self.sequence_inversions)-5}건\n")
                f.write("\n")
            
            # 뎁스 위반 상세
            if self.depth_violations:
                f.write("== 뎁스 위반 상세 (최대 5건) ==\n")
                for i, (ts, bid_depth, ask_depth) in enumerate(self.depth_violations[:5]):
                    dt = datetime.fromtimestamp(ts/1000) if ts > 1000000000000 else datetime.fromtimestamp(ts)
                    f.write(f"{i+1}. {dt}: 매수 뎁스 {bid_depth}, 매도 뎁스 {ask_depth}\n")
                if len(self.depth_violations) > 5:
                    f.write(f"... 외 {len(self.depth_violations)-5}건\n")
                f.write("\n")
            
            # 증분 업데이트 오류 상세
            if self.delta_errors:
                f.write("== 증분 업데이트 오류 상세 (최대 5건) ==\n")
                for i, (ts, error_type, details) in enumerate(self.delta_errors[:5]):
                    dt = datetime.fromtimestamp(ts/1000) if ts > 1000000000000 else datetime.fromtimestamp(ts)
                    f.write(f"{i+1}. {dt}: 오류 유형 {error_type}, 상세: {details}\n")
                if len(self.delta_errors) > 5:
                    f.write(f"... 외 {len(self.delta_errors)-5}건\n")
            
        print(f"요약 보고서가 {summary_path}에 저장되었습니다.")

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="고급 오더북 분석 도구")
    parser.add_argument("log_file", help="로그 파일 경로")
    parser.add_argument("symbol", help="분석할 심볼")
    parser.add_argument("--max-lines", type=int, help="최대 처리할 줄 수", default=None)
    parser.add_argument("--output-dir", help="결과물 저장 디렉토리", default=None)
    
    args = parser.parse_args()
    
    # 분석 실행
    analyzer = AdvancedOrderbookAnalyzer(args.log_file, args.symbol, args.max_lines)
    if analyzer.analyze():
        analyzer.visualize(args.output_dir)
    else:
        print(f"'{args.symbol}' 심볼의 데이터를 찾을 수 없습니다.")

if __name__ == "__main__":
    main()
