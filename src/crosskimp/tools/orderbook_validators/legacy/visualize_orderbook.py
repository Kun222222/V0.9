"""
오더북 데이터 시각화 도구

이 모듈은 로그 파일에서 특정 심볼의 오더북 데이터를 추출하여 시각적으로 보여줍니다.
시간에 따른 최고 매수가와 최저 매도가의 변화, 호가창 갭, 가격 역전 현상 등을 분석합니다.
"""

import os
import argparse
import re
import json
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
import copy
import matplotlib.pyplot as plt
import numpy as np

from delta_validator import OrderbookDeltaValidator, RAW_LOG_PATTERN, ORDERBOOK_DATA_PATTERN

class OrderbookVisualizer:
    """
    오더북 데이터 시각화 도구
    
    로그 파일에서 특정 심볼의 오더북 데이터를 추출하여 시각화합니다.
    """
    
    def __init__(self, log_file_path: str, symbol: str, max_lines: Optional[int] = None):
        """
        초기화
        
        Args:
            log_file_path: 로그 파일 경로
            symbol: 심볼
            max_lines: 최대 처리할 줄 수
        """
        self.log_file_path = log_file_path
        self.symbol = symbol
        self.max_lines = max_lines
        self.exchange_name = self._extract_exchange_name(log_file_path)
        
        # 데이터 저장
        self.timestamps = []
        self.max_bids = []
        self.min_asks = []
        self.spreads = []
        self.orderbook_snapshots = []
        self.price_inversions = []
        self.depth = []
        
    def _extract_exchange_name(self, file_path: str) -> str:
        """파일 경로에서 거래소 이름 추출"""
        base_name = os.path.basename(file_path)
        parts = base_name.split('_')
        if len(parts) >= 4:
            return parts[3]
        return "unknown"
        
    def extract_data(self) -> bool:
        """
        로그 파일에서 오더북 데이터 추출
        
        Returns:
            성공 여부
        """
        print(f"[{self.exchange_name}] {self.symbol} 심볼의 오더북 데이터 추출 시작")
        
        # 오더북 검증 도구 초기화
        validator = OrderbookDeltaValidator(self.log_file_path)
        
        line_count = 0
        found_data = False
        
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
                    
                # 데이터 타입 확인 (스냅샷 vs 델타)
                is_snapshot = orderbook_data.get("type") == "snapshot"
                
                # 데이터 처리
                timestamp = orderbook_data.get("ts")
                bids = orderbook_data.get("bids", [])
                asks = orderbook_data.get("asks", [])
                
                if not bids or not asks:
                    continue
                    
                found_data = True
                
                # 최고 매수가, 최저 매도가 계산
                max_bid = max([float(bid[0]) for bid in bids])
                min_ask = min([float(ask[0]) for ask in asks])
                spread = min_ask - max_bid
                
                # 데이터 저장
                self.timestamps.append(timestamp)
                self.max_bids.append(max_bid)
                self.min_asks.append(min_ask)
                self.spreads.append(spread)
                
                # 가격 역전 확인
                if max_bid >= min_ask:
                    self.price_inversions.append((timestamp, max_bid, min_ask))
                
                # 오더북 뎁스 (호가 수) 저장
                self.depth.append((len(bids), len(asks)))
                
                # 매 100번째 데이터만 스냅샷 저장 (메모리 절약)
                if len(self.timestamps) % 100 == 0:
                    self.orderbook_snapshots.append({
                        "timestamp": timestamp,
                        "bids": bids[:10],  # 상위 10개만 저장
                        "asks": asks[:10]
                    })
                
        elapsed_time = time.time() - start_time
        print(f"데이터 추출 완료: {len(self.timestamps)}개 데이터 ({elapsed_time:.2f}초)")
        
        return found_data
        
    def visualize(self, output_dir: Optional[str] = None) -> None:
        """
        추출한 데이터 시각화
        
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
        times = [datetime.fromtimestamp(ts/1000) if ts > 1000000000000 else datetime.fromtimestamp(ts) 
                 for ts in self.timestamps]
        
        # 1. 매수/매도 가격 변화 시각화
        plt.figure(figsize=(12, 6))
        plt.plot(times, self.max_bids, 'g-', label='최고 매수가', linewidth=1)
        plt.plot(times, self.min_asks, 'r-', label='최저 매도가', linewidth=1)
        plt.title(f'{self.exchange_name} {self.symbol} 매수/매도 가격 변화')
        plt.xlabel('시간')
        plt.ylabel('가격')
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        
        if output_dir:
            plt.savefig(os.path.join(output_dir, f'{self.exchange_name}_{self.symbol}_prices.png'))
        else:
            plt.show()
            
        # 2. 스프레드 시각화
        plt.figure(figsize=(12, 6))
        plt.plot(times, self.spreads, 'b-', linewidth=1)
        plt.title(f'{self.exchange_name} {self.symbol} 스프레드 변화')
        plt.xlabel('시간')
        plt.ylabel('스프레드')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        if output_dir:
            plt.savefig(os.path.join(output_dir, f'{self.exchange_name}_{self.symbol}_spread.png'))
        else:
            plt.show()
            
        # 3. 오더북 뎁스 시각화
        bid_depths = [d[0] for d in self.depth]
        ask_depths = [d[1] for d in self.depth]
        
        plt.figure(figsize=(12, 6))
        plt.plot(times, bid_depths, 'g-', label='매수 호가 수', linewidth=1)
        plt.plot(times, ask_depths, 'r-', label='매도 호가 수', linewidth=1)
        plt.title(f'{self.exchange_name} {self.symbol} 호가 뎁스 변화')
        plt.xlabel('시간')
        plt.ylabel('호가 수')
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        
        if output_dir:
            plt.savefig(os.path.join(output_dir, f'{self.exchange_name}_{self.symbol}_depth.png'))
        else:
            plt.show()
            
        # 4. 가격 역전 현상 보고
        if self.price_inversions:
            print(f"\n가격 역전 현상 발견: {len(self.price_inversions)}건")
            
            for i, (ts, max_bid, min_ask) in enumerate(self.price_inversions[:5]):
                dt = datetime.fromtimestamp(ts/1000) if ts > 1000000000000 else datetime.fromtimestamp(ts)
                print(f"  {i+1}. {dt}: 최고 매수가 {max_bid} >= 최저 매도가 {min_ask}")
                
            if len(self.price_inversions) > 5:
                print(f"  ... 외 {len(self.price_inversions)-5}건")
        else:
            print("\n가격 역전 현상 없음")
            
        # 5. 오더북 스냅샷 저장
        if output_dir and self.orderbook_snapshots:
            snapshot_file = os.path.join(output_dir, f'{self.exchange_name}_{self.symbol}_snapshots.json')
            with open(snapshot_file, 'w', encoding='utf-8') as f:
                json.dump(self.orderbook_snapshots, f, indent=2, ensure_ascii=False)
            print(f"\n오더북 스냅샷 {len(self.orderbook_snapshots)}개 저장: {snapshot_file}")
        
        print("\n시각화 완료")

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="오더북 데이터 시각화 도구")
    parser.add_argument("log_file", help="로그 파일 경로")
    parser.add_argument("symbol", help="분석할 심볼")
    parser.add_argument("--max-lines", type=int, help="최대 처리할 줄 수", default=None)
    parser.add_argument("--output-dir", help="결과물 저장 디렉토리", default=None)
    
    args = parser.parse_args()
    
    # 시각화 실행
    visualizer = OrderbookVisualizer(args.log_file, args.symbol, args.max_lines)
    if visualizer.extract_data():
        visualizer.visualize(args.output_dir)
    else:
        print(f"'{args.symbol}' 심볼의 데이터를 찾을 수 없습니다.")

if __name__ == "__main__":
    main() 