#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
바이빗 로깅 테스트 스크립트

이 스크립트는 bybit_s_sub.py의 로깅 기능이 제대로 작동하는지 확인합니다.
로그 파일을 분석하여 원본 메시지를 확인하고, bybit_s_sub.py 파일의 수정 내용을 검증합니다.
"""

import json
import datetime
import os
from pathlib import Path

def analyze_log_file(log_file_path):
    """
    로그 파일을 분석하여 로깅 기능이 제대로 작동하는지 확인합니다.
    
    Args:
        log_file_path: 로그 파일 경로
    """
    print(f"로그 파일 분석: {log_file_path}")
    
    # 로그 파일 읽기
    try:
        with open(log_file_path, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"오류: 로그 파일을 찾을 수 없습니다: {log_file_path}")
        return
    except Exception as e:
        print(f"오류: 로그 파일 읽기 실패: {e}")
        return
    
    # 원본 메시지 분류
    snapshot_messages = []  # 스냅샷 메시지
    delta_messages = []     # 델타 메시지
    
    for line in lines:
        if 'type":"snapshot' in line:
            # 스냅샷 메시지
            snapshot_messages.append(line)
        elif 'type":"delta' in line:
            # 델타 메시지
            delta_messages.append(line)
    
    print(f"스냅샷 메시지 수: {len(snapshot_messages)}")
    print(f"델타 메시지 수: {len(delta_messages)}")
    
    # 원본 메시지 분석
    print("\n=== 스냅샷 메시지 분석 ===")
    analyze_original_messages(snapshot_messages[:5])  # 처음 5개 메시지만 분석
    
    print("\n=== 델타 메시지 분석 ===")
    analyze_original_messages(delta_messages[:5])  # 처음 5개 메시지만 분석
    
    # bybit_s_sub.py 파일 분석
    print("\n=== bybit_s_sub.py 파일 분석 ===")
    analyze_bybit_s_sub_file()

def analyze_original_messages(messages):
    """
    원본 메시지의 bids와 asks 길이를 분석합니다.
    
    Args:
        messages: 분석할 메시지 목록
    """
    if not messages:
        print("분석할 메시지가 없습니다.")
        return
    
    bid_lengths = []
    ask_lengths = []
    
    for i, msg in enumerate(messages):
        try:
            # 로그 메시지에서 JSON 부분 추출
            json_start = msg.find('{')
            if json_start != -1:
                json_data = json.loads(msg[json_start:])
                
                # 데이터 추출
                if 'data' in json_data:
                    data = json_data['data']
                    symbol = data.get('s', '알 수 없음')
                    bids = data.get('b', [])
                    asks = data.get('a', [])
                    
                    bid_lengths.append(len(bids))
                    ask_lengths.append(len(asks))
                    
                    msg_type = json_data.get("type", "알 수 없음")
                    
                    print(f"메시지 {i+1}: 심볼={symbol}, 타입={msg_type}, bids 길이={len(bids)}, asks 길이={len(asks)}")
                    
                    # 첫 번째 메시지의 경우 일부 bids와 asks 출력
                    if i == 0:
                        print(f"  첫 번째 bids 5개: {bids[:5]}")
                        print(f"  첫 번째 asks 5개: {asks[:5]}")
        except Exception as e:
            print(f"메시지 {i+1}: JSON 파싱 오류: {e}")
    
    # 통계 계산
    if bid_lengths:
        avg_bid_len = sum(bid_lengths) / len(bid_lengths)
        max_bid_len = max(bid_lengths)
        min_bid_len = min(bid_lengths)
        print(f"\nbids 길이 통계: 평균={avg_bid_len:.1f}, 최대={max_bid_len}, 최소={min_bid_len}")
    
    if ask_lengths:
        avg_ask_len = sum(ask_lengths) / len(ask_lengths)
        max_ask_len = max(ask_lengths)
        min_ask_len = min(ask_lengths)
        print(f"asks 길이 통계: 평균={avg_ask_len:.1f}, 최대={max_ask_len}, 최소={min_ask_len}")

def analyze_bybit_s_sub_file():
    """
    bybit_s_sub.py 파일을 분석하여 로깅 관련 코드를 확인합니다.
    """
    file_path = 'src/crosskimp/ob_collector/orderbook/subscription/bybit_s_sub.py'
    
    if not os.path.exists(file_path):
        print(f"오류: bybit_s_sub.py 파일을 찾을 수 없습니다: {file_path}")
        return
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # 로깅 관련 코드 확인
        logging_code_found = False
        depth_limit_code_found = False
        
        # 상위 10개 호가만 제한하는 코드 확인
        if '# 상위 10개 호가만 제한하여 로깅' in content:
            depth_limit_code_found = True
            print("상위 10개 호가만 제한하는 코드가 발견되었습니다.")
            
            # 해당 코드 주변 내용 출력
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if '# 상위 10개 호가만 제한하여 로깅' in line:
                    start_line = max(0, i - 1)
                    end_line = min(len(lines), i + 10)
                    print("\n상위 10개 호가 제한 코드:")
                    for j in range(start_line, end_line):
                        print(f"  {lines[j]}")
                    break
        
        # 로깅 관련 코드 확인
        if 'bids = parsed_data.get("bids", [])[:10]' in content and 'asks = parsed_data.get("asks", [])[:10]' in content:
            logging_code_found = True
            print("\n로깅 시 bids와 asks를 10개로 제한하는 코드가 발견되었습니다.")
        
        # 검증된 데이터 로깅 코드 확인
        validated_logging_code_found = False
        if '"bids": bids,' in content and '"asks": asks,' in content and '"type": "validated_' in content:
            validated_logging_code_found = True
            print("\n검증된 데이터 로깅 시 제한된 bids와 asks를 사용하는 코드가 발견되었습니다.")
        
        # 종합 결과
        if logging_code_found and depth_limit_code_found and validated_logging_code_found:
            print("\n결론: bybit_s_sub.py 파일이 성공적으로 수정되었습니다.")
            print("로그 파일에 상위 10개 호가만 로깅되도록 코드가 수정되었습니다.")
            print("이제 새로운 로그 파일이 생성될 때 상위 10개 호가만 로깅될 것입니다.")
        else:
            print("\n결론: bybit_s_sub.py 파일의 수정이 완전하지 않습니다.")
            if not logging_code_found:
                print("- bids와 asks를 10개로 제한하는 코드가 없습니다.")
            if not depth_limit_code_found:
                print("- 상위 10개 호가만 제한하는 주석이 없습니다.")
            if not validated_logging_code_found:
                print("- 검증된 데이터 로깅 시 제한된 bids와 asks를 사용하는 코드가 없습니다.")
    
    except Exception as e:
        print(f"오류: bybit_s_sub.py 파일 분석 실패: {e}")

def print_message_sample(message, max_length=500):
    """
    메시지 샘플을 출력합니다.
    
    Args:
        message: 출력할 메시지
        max_length: 최대 출력 길이
    """
    if len(message) > max_length:
        print(message[:max_length] + "...")
    else:
        print(message)

if __name__ == "__main__":
    # 로그 파일 경로
    log_file = 'logs/raw_data/bybit/250318_001251_bybit_raw_logger.log'
    
    # 로그 파일 분석
    analyze_log_file(log_file) 