#!/usr/bin/env python
"""
바이너리 파일 분석 도구

이 스크립트는 바이너리 파일을 읽고 헥사 덤프와 기본 구조를 분석합니다.
"""

import os
import sys
import json
import struct
import argparse
import binascii
from typing import Dict, Any, List, Tuple

def hex_dump(data: bytes, offset: int = 0, length: int = None, width: int = 16) -> str:
    """
    바이너리 데이터의 헥사 덤프 생성
    
    Args:
        data: 바이너리 데이터
        offset: 시작 오프셋
        length: 출력할 바이트 수 (None: 전체)
        width: 한 줄에 표시할 바이트 수
        
    Returns:
        str: 헥사 덤프 문자열
    """
    if length is None:
        length = len(data)
    
    result = []
    for i in range(offset, min(offset + length, len(data)), width):
        line_data = data[i:i+width]
        hex_part = ' '.join(f'{b:02x}' for b in line_data)
        ascii_part = ''.join(chr(b) if 32 <= b <= 126 else '.' for b in line_data)
        result.append(f'{i:08x}:  {hex_part:<{width*3}}  {ascii_part}')
    
    return '\n'.join(result)

def analyze_flatbuffers(data: bytes) -> Dict[str, Any]:
    """
    FlatBuffers 바이너리 데이터 분석
    
    Args:
        data: 바이너리 데이터
        
    Returns:
        Dict[str, Any]: 분석 결과
    """
    result = {
        "file_size": len(data),
        "header": {},
        "strings": [],
        "float_arrays": []
    }
    
    # 기본 헤더 분석 (FlatBuffers 형식)
    if len(data) >= 4:
        root_offset = struct.unpack("<I", data[-4:])[0]
        result["header"]["root_offset"] = root_offset
    
    # 문자열 추출 (ASCII 및 UTF-8)
    strings = []
    current_string = ""
    for i, byte in enumerate(data):
        if 32 <= byte <= 126:  # 출력 가능한 ASCII
            current_string += chr(byte)
        else:
            if len(current_string) >= 3:  # 최소 길이 3 이상인 문자열만 저장
                strings.append((i - len(current_string), current_string))
            current_string = ""
    
    if len(current_string) >= 3:
        strings.append((len(data) - len(current_string), current_string))
    
    result["strings"] = strings
    
    # 부동소수점 배열 추출 (가능한 오더북 데이터)
    float_arrays = []
    for i in range(0, len(data) - 8, 8):
        try:
            price = struct.unpack("<d", data[i:i+8])[0]
            if 0.0001 < price < 100000:  # 가격 범위 제한 (오더북 데이터 추정)
                if i + 8 < len(data):
                    quantity = struct.unpack("<d", data[i+8:i+16])[0]
                    if 0 < quantity < 1000000000:  # 수량 범위 제한
                        float_arrays.append((i, price, quantity))
        except:
            pass
    
    result["float_arrays"] = float_arrays
    
    return result

def extract_orderbook_data(data: bytes) -> Dict[str, Any]:
    """
    바이너리 데이터에서 오더북 데이터 추출 시도
    
    Args:
        data: 바이너리 데이터
        
    Returns:
        Dict[str, Any]: 추출된 오더북 데이터
    """
    result = {
        "exchange_name": "",
        "symbol": "",
        "timestamp": 0,
        "sequence": 0,
        "bids": [],
        "asks": []
    }
    
    # 문자열 추출 (거래소 이름, 심볼)
    strings = []
    current_string = ""
    for i, byte in enumerate(data):
        if 32 <= byte <= 126:  # 출력 가능한 ASCII
            current_string += chr(byte)
        else:
            if len(current_string) >= 3:  # 최소 길이 3 이상인 문자열만 저장
                strings.append((i - len(current_string), current_string))
            current_string = ""
    
    if len(current_string) >= 3:
        strings.append((len(data) - len(current_string), current_string))
    
    # 거래소 이름과 심볼 추정
    for _, string in strings:
        if "binance" in string.lower():
            result["exchange_name"] = string
        elif len(string) <= 10 and string.isupper():
            result["symbol"] = string
    
    # 타임스탬프와 시퀀스 번호 추정 (큰 정수값)
    for i in range(0, len(data) - 8, 4):
        try:
            value = struct.unpack("<Q", data[i:i+8])[0]
            if 1700000000000 < value < 1800000000000:  # 2023-2024년 타임스탬프 범위
                result["timestamp"] = value
            elif 1000000000 < value < 10000000000000:  # 시퀀스 번호 추정
                result["sequence"] = value
        except:
            pass
    
    # 부동소수점 배열 추출 (가능한 오더북 데이터)
    price_qty_pairs = []
    for i in range(0, len(data) - 8, 8):
        try:
            price = struct.unpack("<d", data[i:i+8])[0]
            if 0.0001 < price < 100000:  # 가격 범위 제한 (오더북 데이터 추정)
                if i + 8 < len(data):
                    quantity = struct.unpack("<d", data[i+8:i+16])[0]
                    if 0 < quantity < 1000000000:  # 수량 범위 제한
                        price_qty_pairs.append((i, price, quantity))
        except:
            pass
    
    # 매수/매도 호가 분리 (가격 오름차순으로 정렬 후 중간값 기준)
    if price_qty_pairs:
        sorted_pairs = sorted(price_qty_pairs, key=lambda x: x[1])
        mid_idx = len(sorted_pairs) // 2
        
        # 중간값보다 작은 가격은 매수, 큰 가격은 매도로 추정
        mid_price = sorted_pairs[mid_idx][1]
        
        bids = [(price, qty) for _, price, qty in sorted_pairs if price < mid_price]
        asks = [(price, qty) for _, price, qty in sorted_pairs if price >= mid_price]
        
        # 매수는 가격 내림차순, 매도는 가격 오름차순으로 정렬
        result["bids"] = sorted(bids, key=lambda x: x[0], reverse=True)
        result["asks"] = sorted(asks, key=lambda x: x[0])
    
    return result

def main():
    """메인 함수"""
    # 명령줄 인자 파싱
    parser = argparse.ArgumentParser(description='바이너리 파일 분석 도구')
    parser.add_argument('file_path', help='바이너리 파일 경로')
    parser.add_argument('--output', '-o', help='출력 파일 경로 (기본값: 표준 출력)')
    parser.add_argument('--hex', '-x', action='store_true', help='헥사 덤프 출력')
    parser.add_argument('--length', '-l', type=int, default=256, help='헥사 덤프 출력 길이 (기본값: 256)')
    parser.add_argument('--analyze', '-a', action='store_true', help='파일 구조 분석')
    parser.add_argument('--extract', '-e', action='store_true', help='오더북 데이터 추출 시도')
    args = parser.parse_args()
    
    try:
        # 파일 읽기
        with open(args.file_path, 'rb') as f:
            data = f.read()
        
        result = {
            "file_path": args.file_path,
            "file_size": len(data)
        }
        
        # 헥사 덤프
        if args.hex:
            result["hex_dump"] = hex_dump(data, length=args.length)
        
        # 파일 구조 분석
        if args.analyze:
            result["analysis"] = analyze_flatbuffers(data)
        
        # 오더북 데이터 추출
        if args.extract:
            result["orderbook"] = extract_orderbook_data(data)
        
        # 출력
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2)
            print(f"결과가 {args.output}에 저장되었습니다.")
        else:
            print(json.dumps(result, indent=2))
            
    except Exception as e:
        print(f"오류 발생: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main() 