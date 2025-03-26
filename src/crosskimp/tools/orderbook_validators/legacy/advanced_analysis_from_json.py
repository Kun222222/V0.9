#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
JSON 파일 기반 고급 오더북 분석 도구
"""

import os
import json
import argparse
import subprocess
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="JSON 파일 기반 고급 오더북 분석 도구")
    parser.add_argument("json_file", help="검증 결과 JSON 파일 경로")
    parser.add_argument("--output-dir", help="결과 저장 디렉토리", default="advanced_analysis_results")
    parser.add_argument("--max-lines", type=int, help="각 파일당 최대 처리할 줄 수", default=100000)
    parser.add_argument("--max-symbols", type=int, help="각 거래소당 최대 처리할 심볼 수", default=5)
    args = parser.parse_args()
    
    # JSON 파일 읽기
    with open(args.json_file, 'r', encoding='utf-8') as f:
        validation_data = json.load(f)
    
    # 출력 디렉토리 생성
    os.makedirs(args.output_dir, exist_ok=True)
    
    # 전체 결과 저장
    all_results = {}
    
    # 각 거래소별 처리
    for exchange_code, exchange_data in validation_data.items():
        print(f"\n===== {exchange_code} 거래소 분석 시작 =====")
        
        exchange_results = {
            "exchange": exchange_code,
            "symbols_analyzed": [],
            "price_inversions_total": 0,
            "sequence_inversions_total": 0,
            "depth_violations_total": 0,
            "delta_errors_total": 0
        }
        
        # 로그 파일 경로 추출
        log_file = exchange_data["file_path"]
        if not os.path.exists(log_file):
            print(f"로그 파일을 찾을 수 없습니다: {log_file}")
            continue
        
        # 심볼 목록 추출 (최대 max_symbols개)
        symbols = exchange_data["stats"]["symbols"][:args.max_symbols]
        print(f"처리할 심볼: {', '.join(symbols)}")
        
        # 각 심볼별 분석
        for symbol in symbols:
            print(f"  분석: {exchange_code} - {symbol}")
            
            # 결과 저장 경로
            output_path = os.path.join(args.output_dir, f"{exchange_code}_{symbol}")
            os.makedirs(output_path, exist_ok=True)
            
            # 분석 스크립트 실행
            cmd = [
                "python", "advanced_orderbook_analyzer.py",
                log_file,
                symbol,
                "--max-lines", str(args.max_lines),
                "--output-dir", output_path
            ]
            
            try:
                print(f"    명령어: {' '.join(cmd)}")
                process = subprocess.run(cmd, check=True, text=True, 
                                        capture_output=True)
                
                # 분석 결과 파일 확인
                report_file = os.path.join(output_path, f"{exchange_code}_{symbol}_analysis_report.json")
                if os.path.exists(report_file):
                    print(f"    ✓ 분석 성공: {output_path}")
                    
                    # 분석 결과 읽기
                    with open(report_file, 'r', encoding='utf-8') as f:
                        symbol_report = json.load(f)
                        
                    # 결과 요약 추가
                    symbol_result = {
                        "symbol": symbol,
                        "price_inversions": symbol_report["summary"]["price_inversions"],
                        "sequence_inversions": symbol_report["summary"]["sequence_inversions"],
                        "depth_violations": symbol_report["summary"]["depth_violations"],
                        "delta_errors": symbol_report["summary"]["delta_errors"]
                    }
                    
                    # 전체 카운트 업데이트
                    exchange_results["price_inversions_total"] += symbol_result["price_inversions"]
                    exchange_results["sequence_inversions_total"] += symbol_result["sequence_inversions"]
                    exchange_results["depth_violations_total"] += symbol_result["depth_violations"]
                    exchange_results["delta_errors_total"] += symbol_result["delta_errors"]
                    
                    # 심볼 결과 추가
                    exchange_results["symbols_analyzed"].append(symbol_result)
                    
                else:
                    print(f"    ✗ 분석 실패: 보고서 파일이 생성되지 않았습니다")
                    print(f"    오류: {process.stderr}")
            except subprocess.CalledProcessError as e:
                print(f"    ✗ 분석 실패: {e}")
                print(f"    오류: {e.stderr}")
        
        # 거래소 결과 저장
        all_results[exchange_code] = exchange_results
    
    # 전체 결과 저장
    overall_report_path = os.path.join(args.output_dir, "overall_analysis_report.json")
    with open(overall_report_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    
    # 요약 보고서 생성
    summary_report_path = os.path.join(args.output_dir, "analysis_summary.txt")
    with open(summary_report_path, 'w', encoding='utf-8') as f:
        f.write("===== 오더북 데이터 고급 분석 요약 =====\n\n")
        
        for exchange_code, exchange_results in all_results.items():
            f.write(f"## {exchange_code} 거래소\n")
            f.write(f"분석한 심볼 수: {len(exchange_results['symbols_analyzed'])}\n")
            f.write(f"가격 역전 총 발생: {exchange_results['price_inversions_total']}건\n")
            f.write(f"시퀀스 역전 총 발생: {exchange_results['sequence_inversions_total']}건\n")
            f.write(f"뎁스 위반 총 발생: {exchange_results['depth_violations_total']}건\n")
            f.write(f"증분 업데이트 오류 총 발생: {exchange_results['delta_errors_total']}건\n\n")
            
            f.write("심볼별 결과:\n")
            for symbol_result in exchange_results["symbols_analyzed"]:
                f.write(f"  - {symbol_result['symbol']}\n")

if __name__ == "__main__":
    main()
