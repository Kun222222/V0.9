#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
오더북 분석 종합 보고서 생성기
- 모든 심볼의 분석 결과를 하나의 텍스트 표 형태로 정리
"""

import os
import json
import argparse
from datetime import datetime
from typing import Dict, List, Any

def read_analysis_results(results_dirs: List[str]) -> List[Dict[str, Any]]:
    """
    지정된 디렉토리들에서 모든 분석 결과 파일을 읽습니다.
    
    Args:
        results_dirs: 결과 파일이 있는 디렉토리 경로 목록
        
    Returns:
        분석 결과 목록
    """
    all_results = []
    
    for results_dir in results_dirs:
        if not os.path.isdir(results_dir):
            print(f"결과 디렉토리가 존재하지 않습니다: {results_dir}")
            continue
            
        # 디렉토리 내 모든 파일 검색
        for root, _, files in os.walk(results_dir):
            for file in files:
                # JSON 분석 보고서 파일만 선택
                if file.endswith('_analysis_report.json'):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            all_results.append(data)
                    except Exception as e:
                        print(f"파일 {file_path} 읽기 오류: {e}")
    
    return all_results

def generate_text_table(results: List[Dict[str, Any]], output_file: str) -> None:
    """
    분석 결과를 표 형태의 텍스트 파일로 생성합니다.
    
    Args:
        results: 분석 결과 목록
        output_file: 출력 파일 경로
    """
    # 결과가 없으면 종료
    if not results:
        print("분석 결과가 없습니다.")
        return
    
    # 결과를 거래소와 심볼별로 정렬
    results.sort(key=lambda x: (x.get('exchange', ''), x.get('symbol', '')))
    
    # 표 헤더 및 구분선 정의
    headers = [
        "거래소", "심볼", "데이터 수", 
        "가격 역전", "시퀀스 역전", "뎁스 위반", "증분 오류"
    ]
    
    col_widths = [10, 10, 10, 12, 12, 12, 12]
    
    header_line = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |"
    separator = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
    
    # 보고서 작성
    with open(output_file, 'w', encoding='utf-8') as f:
        # 제목 및 생성 시간
        f.write("===== 오더북 분석 종합 보고서 =====\n\n")
        f.write(f"생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"분석된 심볼 수: {len(results)}\n\n")
        
        # 표 헤더
        f.write(separator + "\n")
        f.write(header_line + "\n")
        f.write(separator + "\n")
        
        # 데이터 행
        for result in results:
            exchange = result.get('exchange', 'N/A')
            symbol = result.get('symbol', 'N/A')
            data_count = result.get('data_count', 0)
            
            summary = result.get('summary', {})
            price_inv = summary.get('price_inversions', 0)
            seq_inv = summary.get('sequence_inversions', 0)
            depth_vio = summary.get('depth_violations', 0)
            delta_err = summary.get('delta_errors', 0)
            
            row = [
                exchange.ljust(col_widths[0]),
                symbol.ljust(col_widths[1]),
                str(data_count).ljust(col_widths[2]),
                str(price_inv).ljust(col_widths[3]),
                str(seq_inv).ljust(col_widths[4]),
                str(depth_vio).ljust(col_widths[5]),
                str(delta_err).ljust(col_widths[6])
            ]
            
            f.write("| " + " | ".join(row) + " |\n")
        
        f.write(separator + "\n\n")
        
        # 요약 통계
        total_data = sum(r.get('data_count', 0) for r in results)
        total_price_inv = sum(r.get('summary', {}).get('price_inversions', 0) for r in results)
        total_seq_inv = sum(r.get('summary', {}).get('sequence_inversions', 0) for r in results)
        total_depth_vio = sum(r.get('summary', {}).get('depth_violations', 0) for r in results)
        total_delta_err = sum(r.get('summary', {}).get('delta_errors', 0) for r in results)
        
        f.write("== 전체 통계 요약 ==\n")
        f.write(f"총 데이터 수: {total_data}\n")
        f.write(f"총 가격 역전 현상: {total_price_inv}건\n")
        f.write(f"총 시퀀스 역전 현상: {total_seq_inv}건\n") 
        f.write(f"총 뎁스 위반: {total_depth_vio}건\n")
        f.write(f"총 증분 업데이트 오류: {total_delta_err}건\n\n")
        
        # 거래소별 통계
        f.write("== 거래소별 통계 ==\n")
        exchanges = {}
        for r in results:
            exchange = r.get('exchange', 'unknown')
            if exchange not in exchanges:
                exchanges[exchange] = {
                    'symbols': 0,
                    'data_count': 0,
                    'price_inv': 0,
                    'seq_inv': 0,
                    'depth_vio': 0,
                    'delta_err': 0
                }
            
            exchanges[exchange]['symbols'] += 1
            exchanges[exchange]['data_count'] += r.get('data_count', 0)
            exchanges[exchange]['price_inv'] += r.get('summary', {}).get('price_inversions', 0)
            exchanges[exchange]['seq_inv'] += r.get('summary', {}).get('sequence_inversions', 0)
            exchanges[exchange]['depth_vio'] += r.get('summary', {}).get('depth_violations', 0)
            exchanges[exchange]['delta_err'] += r.get('summary', {}).get('delta_errors', 0)
        
        # 거래소 통계 표 헤더
        ex_headers = ["거래소", "심볼 수", "데이터 수", "가격 역전", "시퀀스 역전", "뎁스 위반", "증분 오류"]
        ex_widths = [10, 10, 10, 12, 12, 12, 12]
        
        ex_header_line = "| " + " | ".join(h.ljust(w) for h, w in zip(ex_headers, ex_widths)) + " |"
        ex_separator = "+-" + "-+-".join("-" * w for w in ex_widths) + "-+"
        
        f.write(ex_separator + "\n")
        f.write(ex_header_line + "\n")
        f.write(ex_separator + "\n")
        
        for ex_name, stats in exchanges.items():
            row = [
                ex_name.ljust(ex_widths[0]),
                str(stats['symbols']).ljust(ex_widths[1]),
                str(stats['data_count']).ljust(ex_widths[2]),
                str(stats['price_inv']).ljust(ex_widths[3]),
                str(stats['seq_inv']).ljust(ex_widths[4]),
                str(stats['depth_vio']).ljust(ex_widths[5]),
                str(stats['delta_err']).ljust(ex_widths[6])
            ]
            
            f.write("| " + " | ".join(row) + " |\n")
        
        f.write(ex_separator + "\n\n")
        
        # 결론
        f.write("== 분석 결론 ==\n")
        f.write("1. 가격 역전 현상: 최고 매수가가 최저 매도가보다 크거나 같은 현상으로, 일시적인 시장 비효율성을 나타냅니다.\n")
        f.write("2. 시퀀스 역전 현상: 이전 시퀀스 번호가 현재 시퀀스 번호보다 큰 경우로, 메시지 처리 순서가 뒤바뀐 경우입니다.\n")
        f.write("3. 뎁스 위반: 오더북의 깊이가 최소 기준(10개)보다 적은 경우입니다.\n")
        f.write("4. 증분 업데이트 오류: 델타 메시지가 제대로 적용되지 않아 발생하는 오류입니다.\n\n")
        
        # 거래소별 특이사항
        f.write("== 거래소별 특이사항 ==\n")
        for ex_name, stats in exchanges.items():
            f.write(f"- {ex_name}: ")
            issues = []
            
            if stats['price_inv'] > 0:
                pct = (stats['price_inv'] / stats['data_count']) * 100 if stats['data_count'] > 0 else 0
                issues.append(f"가격 역전 {stats['price_inv']}건 ({pct:.2f}%)")
                
            if stats['seq_inv'] > 0:
                pct = (stats['seq_inv'] / stats['data_count']) * 100 if stats['data_count'] > 0 else 0
                issues.append(f"시퀀스 역전 {stats['seq_inv']}건 ({pct:.2f}%)")
                
            if stats['depth_vio'] > 0:
                pct = (stats['depth_vio'] / stats['data_count']) * 100 if stats['data_count'] > 0 else 0
                issues.append(f"뎁스 위반 {stats['depth_vio']}건 ({pct:.2f}%)")
                
            if stats['delta_err'] > 0:
                pct = (stats['delta_err'] / stats['data_count']) * 100 if stats['data_count'] > 0 else 0
                issues.append(f"증분 업데이트 오류 {stats['delta_err']}건 ({pct:.2f}%)")
                
            if issues:
                f.write(", ".join(issues) + "\n")
            else:
                f.write("특이사항 없음\n")
    
    print(f"종합 보고서가 {output_file}에 생성되었습니다.")

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="오더북 분석 종합 보고서 생성기")
    parser.add_argument("results_dirs", nargs='+', help="분석 결과가 있는 디렉토리 경로 (여러 개 지정 가능)")
    parser.add_argument("--output", "-o", default="orderbook_analysis_summary.txt", 
                        help="출력 파일 경로 (기본값: orderbook_analysis_summary.txt)")
    
    args = parser.parse_args()
    
    # 분석 결과 읽기
    results = read_analysis_results(args.results_dirs)
    if not results:
        print(f"지정된 디렉토리에서 분석 결과를 찾을 수 없습니다")
        return
    
    # 텍스트 표 생성
    generate_text_table(results, args.output)

if __name__ == "__main__":
    main() 