#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import argparse
import subprocess
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="JSON 파일 기반 오더북 시각화 도구")
    parser.add_argument("json_file", help="검증 결과 JSON 파일 경로")
    parser.add_argument("--output-dir", help="결과 저장 디렉토리", default="visualization_results")
    parser.add_argument("--max-lines", type=int, help="각 파일당 최대 처리할 줄 수", default=100000)
    parser.add_argument("--max-symbols", type=int, help="각 거래소당 최대 처리할 심볼 수", default=5)
    args = parser.parse_args()
    
    # JSON 파일 읽기
    with open(args.json_file, 'r', encoding='utf-8') as f:
        validation_data = json.load(f)
    
    # 출력 디렉토리 생성
    os.makedirs(args.output_dir, exist_ok=True)
    
    # 각 거래소별 처리
    for exchange_code, exchange_data in validation_data.items():
        print(f"\n===== {exchange_code} 거래소 시각화 시작 =====")
        
        # 로그 파일 경로 추출
        log_file = exchange_data["file_path"]
        if not os.path.exists(log_file):
            print(f"로그 파일을 찾을 수 없습니다: {log_file}")
            continue
        
        # 심볼 목록 추출 (최대 max_symbols개)
        symbols = exchange_data["stats"]["symbols"][:args.max_symbols]
        print(f"처리할 심볼: {', '.join(symbols)}")
        
        # 각 심볼별 시각화
        for symbol in symbols:
            print(f"  시각화: {exchange_code} - {symbol}")
            
            # 결과 저장 경로
            output_path = os.path.join(args.output_dir, f"{exchange_code}_{symbol}")
            os.makedirs(output_path, exist_ok=True)
            
            # 시각화 스크립트 실행
            cmd = [
                "python", "visualize_orderbook.py",
                log_file,
                symbol,
                "--max-lines", str(args.max_lines),
                "--output-dir", output_path
            ]
            
            try:
                print(f"    명령어: {' '.join(cmd)}")
                process = subprocess.run(cmd, check=True, text=True, 
                                        capture_output=True)
                
                # 결과 요약
                if os.path.exists(os.path.join(output_path, f"{exchange_code}_{symbol}_prices.png")):
                    print(f"    ✓ 시각화 성공: {output_path}")
                else:
                    print(f"    ✗ 시각화 실패: 이미지 파일이 생성되지 않았습니다")
                    print(f"    오류: {process.stderr}")
            except subprocess.CalledProcessError as e:
                print(f"    ✗ 시각화 실패: {e}")
                print(f"    오류: {e.stderr}")
    
    print(f"\n모든 시각화 처리 완료. 결과는 {args.output_dir} 디렉토리에 저장되었습니다.")

if __name__ == "__main__":
    main()
