#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
C++로 전송된 오더북 데이터의 로그를 분석하는 스크립트
"""

import os
import sys
import re
import json
from datetime import datetime
from collections import defaultdict
import argparse

# 로그 패턴 정의
CPP_LOG_PATTERN = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}(?:\.\d{3})?) - (?:cpp_interface\.py|flatbuffers_serializer\.py|shared_memory_manager\.py)\s+:\d+ / (?:INFO|DEBUG|WARN|ERROR)\s+- (.*?)"
)

# 오더북 직렬화 성공 패턴
SERIALIZE_SUCCESS_PATTERN = re.compile(r".*오더북 직렬화 성공.*exchange=(.*?),.*symbol=(.*?),.*size=(\d+).*")

# 오더북 직렬화 실패 패턴
SERIALIZE_FAIL_PATTERN = re.compile(r".*오더북 직렬화 실패.*exchange=(.*?)(?:,|$)")

# 공유 메모리 쓰기 성공 패턴
MEMORY_WRITE_SUCCESS_PATTERN = re.compile(r".*공유 메모리 쓰기 성공.*exchange=(.*?),.*symbol=(.*?),.*size=(\d+).*")

# 공유 메모리 쓰기 실패 패턴
MEMORY_WRITE_FAIL_PATTERN = re.compile(r".*공유 메모리 쓰기 실패.*exchange=(.*?)(?:,|$)")

def find_log_files(log_dir=None):
    """로그 파일 찾기"""
    if log_dir is None:
        # 기본 로그 디렉토리
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src', 'logs')
    
    log_files = []
    
    # 로그 디렉토리 내 모든 파일 검색
    for root, _, files in os.walk(log_dir):
        for file in files:
            if file.endswith('.log') and ('cpp' in file.lower() or 'orderbook' in file.lower()):
                log_files.append(os.path.join(root, file))
    
    return log_files

def analyze_cpp_logs(log_files):
    """C++ 로그 분석"""
    stats = {
        "start_time": None,
        "end_time": None,
        "total_lines": 0,
        "total_serialization_attempts": 0,
        "successful_serializations": 0,
        "failed_serializations": 0,
        "total_memory_writes": 0,
        "successful_memory_writes": 0,
        "failed_memory_writes": 0,
        "exchanges": defaultdict(lambda: {
            "total_serialization_attempts": 0,
            "successful_serializations": 0,
            "failed_serializations": 0,
            "total_memory_writes": 0,
            "successful_memory_writes": 0,
            "failed_memory_writes": 0,
            "symbols": defaultdict(lambda: {
                "total_serialization_attempts": 0,
                "successful_serializations": 0,
                "failed_serializations": 0,
                "total_memory_writes": 0,
                "successful_memory_writes": 0,
                "failed_memory_writes": 0,
                "last_update_time": None,
                "data_sizes": []
            })
        })
    }
    
    for log_file in log_files:
        print(f"로그 파일 분석 중: {log_file}")
        
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line_num, line in enumerate(f, 1):
                stats["total_lines"] += 1
                
                # 로그 패턴 매칭
                match = CPP_LOG_PATTERN.match(line)
                if not match:
                    continue
                
                timestamp_str, message = match.groups()
                timestamp = datetime.strptime(timestamp_str.split(',')[0], '%Y-%m-%d %H:%M:%S')
                
                # 시작/종료 시간 업데이트
                if stats["start_time"] is None or timestamp < stats["start_time"]:
                    stats["start_time"] = timestamp
                if stats["end_time"] is None or timestamp > stats["end_time"]:
                    stats["end_time"] = timestamp
                
                # 직렬화 성공 로그 분석
                serialize_success = SERIALIZE_SUCCESS_PATTERN.match(message)
                if serialize_success:
                    exchange, symbol, size = serialize_success.groups()
                    stats["total_serialization_attempts"] += 1
                    stats["successful_serializations"] += 1
                    stats["exchanges"][exchange]["total_serialization_attempts"] += 1
                    stats["exchanges"][exchange]["successful_serializations"] += 1
                    stats["exchanges"][exchange]["symbols"][symbol]["total_serialization_attempts"] += 1
                    stats["exchanges"][exchange]["symbols"][symbol]["successful_serializations"] += 1
                    stats["exchanges"][exchange]["symbols"][symbol]["last_update_time"] = timestamp
                    stats["exchanges"][exchange]["symbols"][symbol]["data_sizes"].append(int(size))
                    continue
                
                # 직렬화 실패 로그 분석
                serialize_fail = SERIALIZE_FAIL_PATTERN.match(message)
                if serialize_fail:
                    exchange = serialize_fail.group(1)
                    stats["total_serialization_attempts"] += 1
                    stats["failed_serializations"] += 1
                    stats["exchanges"][exchange]["total_serialization_attempts"] += 1
                    stats["exchanges"][exchange]["failed_serializations"] += 1
                    continue
                
                # 공유 메모리 쓰기 성공 로그 분석
                memory_write_success = MEMORY_WRITE_SUCCESS_PATTERN.match(message)
                if memory_write_success:
                    exchange, symbol, size = memory_write_success.groups()
                    stats["total_memory_writes"] += 1
                    stats["successful_memory_writes"] += 1
                    stats["exchanges"][exchange]["total_memory_writes"] += 1
                    stats["exchanges"][exchange]["successful_memory_writes"] += 1
                    stats["exchanges"][exchange]["symbols"][symbol]["total_memory_writes"] += 1
                    stats["exchanges"][exchange]["symbols"][symbol]["successful_memory_writes"] += 1
                    continue
                
                # 공유 메모리 쓰기 실패 로그 분석
                memory_write_fail = MEMORY_WRITE_FAIL_PATTERN.match(message)
                if memory_write_fail:
                    exchange = memory_write_fail.group(1)
                    stats["total_memory_writes"] += 1
                    stats["failed_memory_writes"] += 1
                    stats["exchanges"][exchange]["total_memory_writes"] += 1
                    stats["exchanges"][exchange]["failed_memory_writes"] += 1
                    continue
    
    # 통계 계산
    for exchange, exchange_data in stats["exchanges"].items():
        # 거래소별 성공률 계산
        if exchange_data["total_serialization_attempts"] > 0:
            exchange_data["serialization_success_rate"] = (
                exchange_data["successful_serializations"] / exchange_data["total_serialization_attempts"] * 100
            )
        else:
            exchange_data["serialization_success_rate"] = 0
            
        if exchange_data["total_memory_writes"] > 0:
            exchange_data["memory_write_success_rate"] = (
                exchange_data["successful_memory_writes"] / exchange_data["total_memory_writes"] * 100
            )
        else:
            exchange_data["memory_write_success_rate"] = 0
        
        # 심볼별 통계 계산
        for symbol, symbol_data in exchange_data["symbols"].items():
            # 심볼별 성공률 계산
            if symbol_data["total_serialization_attempts"] > 0:
                symbol_data["serialization_success_rate"] = (
                    symbol_data["successful_serializations"] / symbol_data["total_serialization_attempts"] * 100
                )
            else:
                symbol_data["serialization_success_rate"] = 0
                
            if symbol_data["total_memory_writes"] > 0:
                symbol_data["memory_write_success_rate"] = (
                    symbol_data["successful_memory_writes"] / symbol_data["total_memory_writes"] * 100
                )
            else:
                symbol_data["memory_write_success_rate"] = 0
            
            # 데이터 크기 통계 계산
            if symbol_data["data_sizes"]:
                symbol_data["avg_data_size"] = sum(symbol_data["data_sizes"]) / len(symbol_data["data_sizes"])
                symbol_data["min_data_size"] = min(symbol_data["data_sizes"])
                symbol_data["max_data_size"] = max(symbol_data["data_sizes"])
                # 마지막 10개 데이터 크기만 유지
                symbol_data["data_sizes"] = symbol_data["data_sizes"][-10:]
            
            # datetime 객체를 문자열로 변환
            if symbol_data["last_update_time"]:
                symbol_data["last_update_time"] = symbol_data["last_update_time"].strftime('%Y-%m-%d %H:%M:%S')
    
    # 전체 성공률 계산
    if stats["total_serialization_attempts"] > 0:
        stats["serialization_success_rate"] = (
            stats["successful_serializations"] / stats["total_serialization_attempts"] * 100
        )
    else:
        stats["serialization_success_rate"] = 0
        
    if stats["total_memory_writes"] > 0:
        stats["memory_write_success_rate"] = (
            stats["successful_memory_writes"] / stats["total_memory_writes"] * 100
        )
    else:
        stats["memory_write_success_rate"] = 0
    
    # datetime 객체를 문자열로 변환
    if stats["start_time"]:
        stats["start_time"] = stats["start_time"].strftime('%Y-%m-%d %H:%M:%S')
    if stats["end_time"]:
        stats["end_time"] = stats["end_time"].strftime('%Y-%m-%d %H:%M:%S')
    
    return stats

def save_analysis_results(stats, output_file=None):
    """분석 결과 저장"""
    if output_file is None:
        # 결과 저장 디렉토리
        results_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src', 'logs', 'cpp_analysis_results')
        os.makedirs(results_dir, exist_ok=True)
        
        # 현재 시간으로 파일명 생성
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(results_dir, f"cpp_analysis_{timestamp}.json")
    
    # 결과 저장
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    print(f"분석 결과 저장 완료: {output_file}")
    
    return output_file

def print_analysis_summary(stats):
    """분석 결과 요약 출력"""
    print("\n" + "=" * 80)
    print("C++ 오더북 데이터 전송 분석 결과 요약")
    print("=" * 80)
    
    # 기본 정보
    print(f"분석 기간: {stats['start_time']} ~ {stats['end_time']}")
    print(f"총 분석 라인 수: {stats['total_lines']:,}개")
    print()
    
    # 직렬화 통계
    print(f"직렬화 시도: {stats['total_serialization_attempts']:,}회")
    print(f"직렬화 성공: {stats['successful_serializations']:,}회 ({stats['serialization_success_rate']:.2f}%)")
    print(f"직렬화 실패: {stats['failed_serializations']:,}회 ({100 - stats['serialization_success_rate']:.2f}%)")
    print()
    
    # 공유 메모리 쓰기 통계
    print(f"공유 메모리 쓰기 시도: {stats['total_memory_writes']:,}회")
    print(f"공유 메모리 쓰기 성공: {stats['successful_memory_writes']:,}회 ({stats['memory_write_success_rate']:.2f}%)")
    print(f"공유 메모리 쓰기 실패: {stats['failed_memory_writes']:,}회 ({100 - stats['memory_write_success_rate']:.2f}%)")
    print()
    
    # 거래소별 통계
    print("거래소별 통계:")
    print("-" * 80)
    print(f"{'거래소':15} {'직렬화 성공률':15} {'메모리 쓰기 성공률':20} {'심볼 수':10}")
    print("-" * 80)
    
    for exchange, exchange_data in sorted(stats["exchanges"].items()):
        print(f"{exchange:15} {exchange_data['serialization_success_rate']:13.2f}% {exchange_data['memory_write_success_rate']:18.2f}% {len(exchange_data['symbols']):10}")
    
    print("\n" + "=" * 80)

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='C++ 오더북 데이터 전송 로그 분석')
    parser.add_argument('--log-dir', help='로그 디렉토리 경로')
    parser.add_argument('--output', help='결과 파일 경로')
    args = parser.parse_args()
    
    # 로그 파일 찾기
    log_files = find_log_files(args.log_dir)
    if not log_files:
        print("분석할 로그 파일을 찾을 수 없습니다.")
        return
    
    print(f"총 {len(log_files)}개 로그 파일을 분석합니다.")
    
    # 로그 분석
    stats = analyze_cpp_logs(log_files)
    
    # 결과 저장
    output_file = save_analysis_results(stats, args.output)
    
    # 결과 요약 출력
    print_analysis_summary(stats)
    
    print(f"\n상세 분석 결과는 다음 파일에서 확인할 수 있습니다: {output_file}")

if __name__ == "__main__":
    main() 