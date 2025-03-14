#!/usr/bin/env python
"""
FlatBuffers 바이너리 파일 읽기 도구

이 스크립트는 FlatBuffers 형식으로 직렬화된 바이너리 파일을 읽고 내용을 출력합니다.
"""

import os
import sys
import json
import argparse
from typing import Dict, Any

# FlatBuffers 모듈 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# FlatBuffers 모듈 임포트
from crosskimp.ob_collector.cpp.flatbuffers.OrderBookData import OrderBook

def read_flatbuffers_file(file_path: str) -> Dict[str, Any]:
    """
    FlatBuffers 바이너리 파일을 읽고 역직렬화
    
    Args:
        file_path: 바이너리 파일 경로
        
    Returns:
        Dict[str, Any]: 역직렬화된 데이터
    """
    # 파일 읽기
    with open(file_path, 'rb') as f:
        data = f.read()
    
    # 역직렬화
    orderbook = OrderBook.GetRootAsOrderBook(data, 0)
    
    # 데이터 추출
    exchange_name = orderbook.ExchangeName().decode('utf-8') if orderbook.ExchangeName() else ""
    symbol = orderbook.Symbol().decode('utf-8') if orderbook.Symbol() else ""
    timestamp = orderbook.Timestamp()
    sequence = orderbook.Sequence()
    
    # 매수 호가 추출
    bids = []
    for i in range(orderbook.BidsLength()):
        bid = orderbook.Bids(i)
        bids.append([bid.Price(), bid.Quantity()])
    
    # 매도 호가 추출
    asks = []
    for i in range(orderbook.AsksLength()):
        ask = orderbook.Asks(i)
        asks.append([ask.Price(), ask.Quantity()])
    
    # 결과 반환
    return {
        "exchange_name": exchange_name,
        "symbol": symbol,
        "timestamp": timestamp,
        "sequence": sequence,
        "bids": bids,
        "asks": asks
    }

def main():
    """메인 함수"""
    # 명령줄 인자 파싱
    parser = argparse.ArgumentParser(description='FlatBuffers 바이너리 파일 읽기 도구')
    parser.add_argument('file_path', help='바이너리 파일 경로')
    parser.add_argument('--output', '-o', help='출력 파일 경로 (기본값: 표준 출력)')
    parser.add_argument('--pretty', '-p', action='store_true', help='예쁘게 출력')
    args = parser.parse_args()
    
    try:
        # 파일 읽기
        data = read_flatbuffers_file(args.file_path)
        
        # 출력
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2 if args.pretty else None)
            print(f"결과가 {args.output}에 저장되었습니다.")
        else:
            print(json.dumps(data, indent=2 if args.pretty else None))
            
    except Exception as e:
        print(f"오류 발생: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main() 