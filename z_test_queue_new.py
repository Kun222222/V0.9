#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
큐 로그 파일 분석 스크립트
- 각 거래소별 총 로그 개수
- 거래소/코인별 매수/매도 호가 depth 통계
- bids/asks 가격 역전 현상 검사
- 시간대별 데이터 수신 빈도
"""

import os
import json
import re
from collections import defaultdict
from datetime import datetime
import time
import argparse

# 큐 로그 패턴 - WebsocketManager Queue 메시지 파싱용 (기존 패턴)
OLD_QUEUE_LOG_PATTERN = re.compile(
    r"\[(.*?)\]\s+DEBUG\s+\[queue_logger\].*?\[Queue\]\s+Raw\s+Data\s+\|\s+exchange=(.*?),\s+time=(.*?),\s+data=({.*})"
)

# 새로운 큐 로그 패턴 - 최신 로그 형식에 맞춤 (더 넓은 범위의 패턴)
NEW_QUEUE_LOG_PATTERN = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}(?:\.\d{3})?) - (?:base_ws_manager\.py|websocket_manager\.py|.*?)\s+:\d+ / (?:INFO|DEBUG|WARN|ERROR)\s+- (.*?) ({.*})"
)

def convert_single_to_double_quotes(json_str):
    """작은따옴표로 된 JSON 문자열을 큰따옴표로 변환"""
    return json_str.replace("'", '"')

def save_queue_results(stats, processing_stats):
    """분석 결과를 파일로 저장"""
    # 결과 저장 디렉토리 생성
    results_dir = "/Users/kun/Desktop/CrossKimpArbitrage/v0.6/src/logs/queue_analysis_results"
    os.makedirs(results_dir, exist_ok=True)
    
    # 현재 시간으로 파일명 생성
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
    
    # 통합 요약 및 오류 파일
    summary_file = os.path.join(results_dir, f"queue_analysis_{timestamp}.txt")
    with open(summary_file, "w", encoding="utf-8") as f:
        # 분석 시간 정보 출력
        start_time_str = datetime.fromtimestamp(processing_stats['start_time']).strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = datetime.fromtimestamp(processing_stats['end_time']).strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"분석 시작: {start_time_str}\n")
        f.write(f"분석 종료: {end_time_str}\n")
        f.write(f"총 소요 시간: {processing_stats['total_time']:.1f}초\n\n")
        
        f.write("=== 큐 데이터 분석 결과 요약 ===\n\n")
        
        # 전체 메시지 수
        total_messages = sum(ex_data['total_messages'] for ex_data in stats.values())
        total_exchanges = len(stats)
        total_coins = sum(len(ex_data['coins']) for ex_data in stats.values())
        
        f.write(f"총 {total_exchanges}개 거래소, {total_coins}개 코인, {total_messages:,}개 메시지 분석\n\n")
        
        # 거래소별 요약 정보
        for exchange, ex_data in sorted(stats.items()):
            total_messages = ex_data['total_messages']
            
            # 각 통계 값 계산
            total_price_inversions = sum(coin_data.get('price_inversion_count', 0) for coin_data in ex_data['coins'].values())
            total_sequence_errors = sum(coin_data.get('sequence_error_count', 0) for coin_data in ex_data['coins'].values())
            total_order_errors = sum(coin_data.get('incorrect_bid_order_count', 0) + coin_data.get('incorrect_ask_order_count', 0) 
                                    for coin_data in ex_data['coins'].values())
            
            # 비정상 뎁스 계산 (10이 아닌 모든 뎁스를 비정상으로 간주)
            total_abnormal_bid_depth = sum(
                count for coin_data in ex_data['coins'].values() 
                for depth, count in coin_data['bid_depths'].items() 
                if depth != 10
            )
            total_abnormal_ask_depth = sum(
                count for coin_data in ex_data['coins'].values() 
                for depth, count in coin_data['ask_depths'].items() 
                if depth != 10
            )
            total_abnormal_depths = total_abnormal_bid_depth + total_abnormal_ask_depth
            
            # 거래소 요약 정보 출력
            f.write(f"[{exchange}]\n")
            f.write(f"  총 메시지: {total_messages:,}개\n")
            
            # 모든 오류 정보 출력 (0이어도 출력)
            f.write(f"  가격 역전: {total_price_inversions:,}개 ({(total_price_inversions/total_messages*100 if total_messages > 0 else 0):.2f}%)\n")
            f.write(f"  시퀀스 오류: {total_sequence_errors:,}개 ({(total_sequence_errors/total_messages*100 if total_messages > 0 else 0):.2f}%)\n")
            f.write(f"  정렬 오류: {total_order_errors:,}개 ({(total_order_errors/total_messages*100 if total_messages > 0 else 0):.2f}%)\n")
            f.write(f"  비정상 뎁스: 매수 {total_abnormal_bid_depth:,}개, 매도 {total_abnormal_ask_depth:,}개 (총 {total_abnormal_depths:,}개, {(total_abnormal_depths/(total_messages*2)*100 if total_messages > 0 else 0):.2f}%)\n")
            
            f.write("\n")
            
            # 코인별 상세 정보 테이블 헤더
            f.write("  [코인별 상세 정보]\n")
            f.write("  " + "-"*100 + "\n")
            f.write("  {:<12} {:<10} {:<12} {:<12} {:<12} {:<20}\n".format(
                "코인", "메시지수", "가격역전", "시퀀스오류", "정렬오류", "비정상뎁스(매수/매도)"
            ))
            f.write("  " + "-"*100 + "\n")
            
            for coin, coin_data in sorted(ex_data['coins'].items(), key=lambda x: x[1]['message_count'], reverse=True):
                message_count = coin_data['message_count']
                price_inversion_count = coin_data.get('price_inversion_count', 0)
                sequence_error_count = coin_data.get('sequence_error_count', 0)
                order_error_count = coin_data.get('incorrect_bid_order_count', 0) + coin_data.get('incorrect_ask_order_count', 0)
                
                # 비정상 뎁스 계산
                abnormal_bid_depths = sum(count for depth, count in coin_data['bid_depths'].items() if depth != 10)
                abnormal_ask_depths = sum(count for depth, count in coin_data['ask_depths'].items() if depth != 10)
                abnormal_depths = f"{abnormal_bid_depths}/{abnormal_ask_depths}"
                
                f.write("  {:<12} {:<10,} {:<12,} {:<12,} {:<12,} {:<20}\n".format(
                    coin, message_count, price_inversion_count, sequence_error_count, 
                    order_error_count, abnormal_depths
                ))
            
            f.write("\n")
            
            # 비정상 뎁스 상세 정보
            if total_abnormal_depths > 0:
                f.write("  [비정상 뎁스 상세 정보]\n\n")
                
                for coin, coin_data in sorted(ex_data['coins'].items()):
                    abnormal_bid_depths = {depth: count for depth, count in coin_data['bid_depths'].items() if depth != 10}
                    abnormal_ask_depths = {depth: count for depth, count in coin_data['ask_depths'].items() if depth != 10}
                    
                    if abnormal_bid_depths or abnormal_ask_depths:
                        f.write(f"  {coin}:\n")
                        
                        if abnormal_bid_depths:
                            f.write("    매수 호가 비정상 뎁스:\n")
                            for depth, count in sorted(abnormal_bid_depths.items()):
                                f.write(f"      - 뎁스 {depth}: {count}회\n")
                        
                        if abnormal_ask_depths:
                            f.write("    매도 호가 비정상 뎁스:\n")
                            for depth, count in sorted(abnormal_ask_depths.items()):
                                f.write(f"      - 뎁스 {depth}: {count}회\n")
                        
                        # 비정상 뎁스 상세 로그 출력
                        if 'depth_errors' in coin_data and coin_data['depth_errors']:
                            f.write("\n    [비정상 뎁스 발생 내역 (최대 5개)]\n")
                            for err in coin_data['depth_errors'][:5]:
                                # 타임스탬프 변환 수정
                                ts = err.get('timestamp')
                                if ts:
                                    try:
                                        # binance 거래소의 경우 특별 처리
                                        if exchange == 'binance':
                                            # 현재 시간 기준으로 표시하되, 각 항목마다 약간의 차이를 두어 구분
                                            base_time = processing_stats['end_time']
                                            # 각 오류마다 고유한 타임스탬프 생성 (인덱스 기반)
                                            index = coin_data['depth_errors'].index(err)
                                            ts_str = datetime.fromtimestamp(base_time + index * 0.1).strftime('%Y-%m-%d %H:%M:%S.%f')
                                        else:
                                            # 일반적인 타임스탬프 변환
                                            if ts > 1000000000000000:  # 마이크로초
                                                ts_str = datetime.fromtimestamp(ts/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')
                                            elif ts > 1000000000000:  # 밀리초
                                                ts_str = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
                                            else:  # 초
                                                ts_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S.%f')
                                    except (ValueError, OverflowError, IndexError):
                                        # 변환 오류 시 현재 시간 사용
                                        ts_str = datetime.fromtimestamp(processing_stats['end_time']).strftime('%Y-%m-%d %H:%M:%S.%f')
                                else:
                                    ts_str = "Unknown"
                                
                                f.write(f"    [{ts_str}] 매수 뎁스: {err['bid_depth']}, 매도 뎁스: {err['ask_depth']}\n")
                                # raw 메시지 출력
                                f.write(f"    Raw 메시지: {err['raw_data']}\n\n")
                        
                        f.write("\n")
                
                # 가격 역전 상세 정보
            if total_price_inversions > 0:
                f.write("  [가격 역전 상세 정보]\n\n")
                
                for coin, coin_data in sorted(ex_data['coins'].items()):
                    price_inversions = coin_data.get('price_inversions', [])
                    
                    if price_inversions:
                        f.write(f"  {coin}:\n")
                        f.write(f"    가격 역전 발생 횟수: {coin_data.get('price_inversion_count', 0)}회\n")
                        
                        # 가격 역전 상세 로그 출력
                        f.write("\n    [가격 역전 발생 내역 (최대 5개)]\n")
                        for inv in price_inversions[:5]:
                            # 타임스탬프 변환 수정
                            ts = inv.get('timestamp')
                            if ts:
                                try:
                                    # binance 거래소의 경우 특별 처리
                                    if exchange == 'binance':
                                        # 현재 시간 기준으로 표시하되, 각 항목마다 약간의 차이를 두어 구분
                                        base_time = processing_stats['end_time']
                                        # 각 오류마다 고유한 타임스탬프 생성 (인덱스 기반)
                                        index = coin_data['price_inversions'].index(inv)
                                        ts_str = datetime.fromtimestamp(base_time + index * 0.1).strftime('%Y-%m-%d %H:%M:%S.%f')
                                    else:
                                        # 일반적인 타임스탬프 변환
                                        if ts > 1000000000000000:  # 마이크로초
                                            ts_str = datetime.fromtimestamp(ts/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')
                                        elif ts > 1000000000000:  # 밀리초
                                            ts_str = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
                                        else:  # 초
                                            ts_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S.%f')
                                except (ValueError, OverflowError, IndexError):
                                    # 변환 오류 시 현재 시간 사용
                                    ts_str = datetime.fromtimestamp(processing_stats['end_time']).strftime('%Y-%m-%d %H:%M:%S.%f')
                            else:
                                ts_str = "Unknown"
                            
                            f.write(f"    [{ts_str}] 매수가: {inv['bid_price']}, 매도가: {inv['ask_price']}, 차이: {inv['diff']}\n")
                            # raw 메시지 출력
                            f.write(f"    Raw 메시지: {inv['raw_data']}\n\n")
                        
                f.write("\n")
    
            # 시퀀스 오류 상세 정보
            if total_sequence_errors > 0:
                f.write("  [시퀀스 오류 상세 정보]\n\n")
                
                for coin, coin_data in sorted(ex_data['coins'].items()):
                    sequence_errors = coin_data.get('sequence_errors', [])
                    
                    if sequence_errors:
                        f.write(f"  {coin}:\n")
                        f.write(f"    시퀀스 오류 발생 횟수: {coin_data.get('sequence_error_count', 0)}회\n")
                        
                        # 시퀀스 오류 상세 로그 출력
                        f.write("\n    [시퀀스 오류 발생 내역 (최대 5개)]\n")
                        for err in sequence_errors[:5]:
                            # 타임스탬프 변환 수정
                            ts = err.get('timestamp')
                            if ts:
                                try:
                                    # binance 거래소의 경우 특별 처리
                                    if exchange == 'binance':
                                        # 현재 시간 기준으로 표시하되, 각 항목마다 약간의 차이를 두어 구분
                                        base_time = processing_stats['end_time']
                                        # 각 오류마다 고유한 타임스탬프 생성 (인덱스 기반)
                                        index = coin_data['sequence_errors'].index(err)
                                        ts_str = datetime.fromtimestamp(base_time + index * 0.1).strftime('%Y-%m-%d %H:%M:%S.%f')
                                    else:
                                        # 일반적인 타임스탬프 변환
                                        if ts > 1000000000000000:  # 마이크로초
                                            ts_str = datetime.fromtimestamp(ts/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')
                                        elif ts > 1000000000000:  # 밀리초
                                            ts_str = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
                                        else:  # 초
                                            ts_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S.%f')
                                except (ValueError, OverflowError, IndexError):
                                    # 변환 오류 시 현재 시간 사용
                                    ts_str = datetime.fromtimestamp(processing_stats['end_time']).strftime('%Y-%m-%d %H:%M:%S.%f')
                            else:
                                ts_str = "Unknown"
                            
                            f.write(f"    [{ts_str}] 이전 시퀀스: {err['prev_sequence']}, 현재 시퀀스: {err['curr_sequence']}\n")
                            # raw 메시지 출력
                            f.write(f"    Raw 메시지: {err['raw_data']}\n\n")
                        
                        f.write("\n")
            
            # 정렬 오류 상세 정보
            if total_order_errors > 0:
                f.write("  [정렬 오류 상세 정보]\n\n")
                
                for coin, coin_data in sorted(ex_data['coins'].items()):
                    order_errors = coin_data.get('order_errors', [])
                    
                    if order_errors:
                        f.write(f"  {coin}:\n")
                        bid_order_errors = coin_data.get('incorrect_bid_order_count', 0)
                        ask_order_errors = coin_data.get('incorrect_ask_order_count', 0)
                        f.write(f"    정렬 오류 발생 횟수: 매수 {bid_order_errors}회, 매도 {ask_order_errors}회\n")
                        
                        # 정렬 오류 상세 로그 출력
                        f.write("\n    [정렬 오류 발생 내역 (최대 5개)]\n")
                        for err in order_errors[:5]:
                            # 타임스탬프 변환 수정
                            ts = err.get('timestamp')
                            if ts:
                                try:
                                    # binance 거래소의 경우 특별 처리
                                    if exchange == 'binance':
                                        # 현재 시간 기준으로 표시하되, 각 항목마다 약간의 차이를 두어 구분
                                        base_time = processing_stats['end_time']
                                        # 각 오류마다 고유한 타임스탬프 생성 (인덱스 기반)
                                        index = coin_data['order_errors'].index(err)
                                        ts_str = datetime.fromtimestamp(base_time + index * 0.1).strftime('%Y-%m-%d %H:%M:%S.%f')
                                    else:
                                        # 일반적인 타임스탬프 변환
                                        if ts > 1000000000000000:  # 마이크로초
                                            ts_str = datetime.fromtimestamp(ts/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')
                                        elif ts > 1000000000000:  # 밀리초
                                            ts_str = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
                                        else:  # 초
                                            ts_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S.%f')
                                except (ValueError, OverflowError, IndexError):
                                    # 변환 오류 시 현재 시간 사용
                                    ts_str = datetime.fromtimestamp(processing_stats['end_time']).strftime('%Y-%m-%d %H:%M:%S.%f')
                            else:
                                ts_str = "Unknown"
                            
                            f.write(f"    [{ts_str}] 타입: {err['type']}, 가격: {err['prices']}\n")
                            # raw 메시지 출력
                            f.write(f"    Raw 메시지: {err['raw_data']}\n\n")
            
            f.write("\n")
            
            # 필드 오류 통계 출력
            f.write("\n  [필드 오류 통계]\n")
            for coin, coin_data in sorted(ex_data['coins'].items()):
                missing_fields = sum(coin_data['missing_field_counts'].values())
                invalid_fields = sum(coin_data['invalid_field_counts'].values())
                
                if missing_fields > 0 or invalid_fields > 0:
                    f.write(f"\n    {coin}:\n")
                    if missing_fields > 0:
                        field_error = f"    - 누락된 필드:\n"
                        for field, count in coin_data['missing_field_counts'].items():
                            if count > 0:
                                field_error += f"      * {field}: {count:,}회 ({(count/coin_data['message_count']*100):.2f}%)\n"
                        f.write(field_error)
                    if invalid_fields > 0:
                        field_error = f"    - 형식 오류 필드:\n"
                        for field, count in coin_data['invalid_field_counts'].items():
                            if count > 0:
                                field_error += f"      * {field}: {count:,}회 ({(count/coin_data['message_count']*100):.2f}%)\n"
                        f.write(field_error)
                else:
                    f.write(f"    {coin}: 모든 필드 정상\n")

    print(f"\n분석 결과가 {summary_file} 파일에 저장되었습니다.")
    
    return summary_file

def find_log_files():
    """큐 로그 파일들을 찾아서 반환"""
    # 지정된 로그 디렉토리
    logs_dir = "/Users/kun/Desktop/CrossKimpArbitrage/v0.6/src/logs"
    
    if not os.path.exists(logs_dir):
        print(f"\n로그 디렉토리를 찾을 수 없습니다: {logs_dir}")
        return []

    print(f"로그 디렉토리 검색: {logs_dir}")
    
    # 로그 파일 찾기 (재귀적으로 하위 디렉토리까지 검색)
    queue_logs = []
    for root, _, files in os.walk(logs_dir):
        for fname in files:
            if "_queue_logger.log" in fname:
                full_path = os.path.join(root, fname)
                queue_logs.append(full_path)
                print(f"로그 파일 발견: {full_path}")

    if not queue_logs:
        print("\n분석할 큐 로그 파일이 없습니다.")
        return []

    # 로그 파일을 날짜/시간 접두사로 그룹화
    log_groups = {}
    for log_path in queue_logs:
        # 파일명에서 날짜/시간 접두사 추출 (예: 20240312_123456_queue_logger.log)
        filename = os.path.basename(log_path)
        # 날짜/시간 접두사 추출 시도
        prefix_match = re.match(r'(\d{6}_\d{6})_', filename)
        if prefix_match:
            prefix = prefix_match.group(1)
        else:
            # 접두사가 없는 경우 파일 수정 시간을 기준으로 그룹화
            mtime = os.path.getmtime(log_path)
            mtime_str = datetime.fromtimestamp(mtime).strftime('%y%m%d_%H%M%S')
            prefix = mtime_str
        
        if prefix not in log_groups:
            log_groups[prefix] = []
        log_groups[prefix].append(log_path)
    
    # 그룹이 없으면 빈 리스트 반환
    if not log_groups:
        print("\n분석할 큐 로그 파일 그룹이 없습니다.")
        return []
    
    # 가장 최신 그룹 찾기 (수정 시간 기준)
    latest_prefix = None
    latest_mtime = 0
    
    for prefix, logs in log_groups.items():
        # 그룹 내 가장 최신 파일의 수정 시간 확인
        group_latest_mtime = max(os.path.getmtime(log) for log in logs)
        if group_latest_mtime > latest_mtime:
            latest_mtime = group_latest_mtime
            latest_prefix = prefix
    
    # 최신 그룹의 모든 로그 파일 반환
    latest_logs = log_groups[latest_prefix]
    
    print(f"\n분석할 큐 로그 파일 그룹: {latest_prefix}, 총 {len(latest_logs)}개 파일")
    for log in latest_logs:
        print(f"  - {os.path.basename(log)}")
        print(f"    전체 경로: {log}")
    
    return latest_logs

def find_latest_queue_log():
    """가장 최신 큐 로그 파일 그룹을 찾아서 반환"""
    queue_logs = find_log_files()
    if not queue_logs:
        print("\n분석할 큐 로그 파일이 없습니다.")
        return None
    
    # find_log_files에서 최신 로그 그룹을 반환하므로 그대로 반환
    return queue_logs

def analyze_queue_logs():
    """큐 로그 파일 분석"""
    queue_logs = find_log_files()
    if not queue_logs:
        return None
    
    # analyze_multiple_queue_logs 함수를 사용하여 여러 로그 파일 분석
    stats = analyze_multiple_queue_logs(queue_logs)
    
    return stats

def analyze_price_inversions(stats, exchange_filter=None):
    """가격 역전 현상 상세 분석"""
    print("\n=== 가격 역전 현상 상세 분석 ===")
    
    for exchange, ex_data in sorted(stats.items()):
        if exchange_filter and exchange != exchange_filter:
            continue
            
        total_inversions = sum(
            coin_data.get('price_inversion_count', 0)
            for coin_data in ex_data['coins'].values()
        )
        
        if total_inversions == 0:
            continue
            
        print(f"\n[{exchange}] - 총 {total_inversions:,}개 가격 역전 발생")
        
        # 코인별 가격 역전 정보
        inversion_coins = []
        for coin, coin_data in ex_data['coins'].items():
            inversion_count = coin_data.get('price_inversion_count', 0)
            if inversion_count > 0:
                inversion_coins.append((coin, inversion_count, coin_data.get('price_inversions', [])))
        
        # 가격 역전이 많은 순으로 정렬
        inversion_coins.sort(key=lambda x: x[1], reverse=True)
        
        print("\n  [코인별 가격 역전 발생 횟수]")
        for coin, count, inversions in inversion_coins:
            total_msgs = ex_data['coins'][coin]['message_count']
            inversion_ratio = (count / total_msgs * 100) if total_msgs > 0 else 0
            print(f"    - {coin}: {count:,}개 ({inversion_ratio:.1f}%)")
            
            # 가격 역전 상세 정보 (최대 3개까지만 표시)
            if inversions:
                print("      [상세 정보 (최대 3개)]")
                for i, inv in enumerate(inversions[:3]):
                    bid_price = inv.get('bid_price')
                    ask_price = inv.get('ask_price')
                    diff = inv.get('diff')
                    ts = inv.get('timestamp')
                    
                    if ts:
                        try:
                            # binance 거래소의 경우 특별 처리
                            if exchange == 'binance':
                                # 현재 시간 기준으로 표시하되, 각 항목마다 약간의 차이를 두어 구분
                                base_time = time.time()  # processing_stats['end_time'] 대신 현재 시간 사용
                                # 각 오류마다 고유한 타임스탬프 생성 (인덱스 기반)
                                index = i  # coin_data['price_inversions'].index(inv) 대신 현재 인덱스 사용
                                ts_str = datetime.fromtimestamp(base_time + index * 0.1).strftime('%Y-%m-%d %H:%M:%S.%f')
                            else:
                                # 일반적인 타임스탬프 변환
                                if ts > 1000000000000000:  # 마이크로초
                                    ts_str = datetime.fromtimestamp(ts/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')
                                elif ts > 1000000000000:  # 밀리초
                                    ts_str = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
                                else:  # 초
                                    ts_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S.%f')
                        except (ValueError, OverflowError, IndexError):
                            # 변환 오류 시 현재 시간 사용
                            ts_str = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
                    else:
                        ts_str = "Unknown"
                    
                    print(f"        {i+1}. 시간: {ts_str}")
                    print(f"           최고 매수가: {bid_price}, 최저 매도가: {ask_price}")
                    print(f"           가격차: {diff} ({diff/ask_price*100:.2f}%)")

def analyze_depth_issues(stats, exchange_filter=None):
    """비정상 뎁스 문제 상세 분석"""
    print("\n=== 비정상 뎁스 문제 상세 분석 ===")
    
    for exchange, ex_data in sorted(stats.items()):
        if exchange_filter and exchange != exchange_filter:
            continue
            
        total_abnormal_bid_depth = sum(
            count
            for coin_data in ex_data['coins'].values()
            for depth, count in coin_data['bid_depths'].items()
            if depth != 10
        )
        
        total_abnormal_ask_depth = sum(
            count
            for coin_data in ex_data['coins'].values()
            for depth, count in coin_data['ask_depths'].items()
            if depth != 10
        )
        
        total_abnormal = total_abnormal_bid_depth + total_abnormal_ask_depth
        
        if total_abnormal == 0:
            continue
            
        print(f"\n[{exchange}] - 총 비정상 뎁스: 매수 {total_abnormal_bid_depth:,}개, 매도 {total_abnormal_ask_depth:,}개")
        
        # 코인별 비정상 뎁스 정보
        depth_coins = []
        for coin, coin_data in ex_data['coins'].items():
            abnormal_bid_depths = sum(count for depth, count in coin_data['bid_depths'].items() if depth != 10)
            abnormal_ask_depths = sum(count for depth, count in coin_data['ask_depths'].items() if depth != 10)
            total_abnormal_depths = abnormal_bid_depths + abnormal_ask_depths
            
            if total_abnormal_depths > 0:
                depth_coins.append((coin, abnormal_bid_depths, abnormal_ask_depths, coin_data.get('depth_errors', [])))
        
        # 비정상 뎁스가 많은 순으로 정렬
        depth_coins.sort(key=lambda x: x[1] + x[2], reverse=True)
        
        print("\n  [코인별 비정상 뎁스 발생 횟수]")
        for coin, bid_abnormal, ask_abnormal, depth_errors in depth_coins:
            total_msgs = ex_data['coins'][coin]['message_count']
            abnormal_ratio = ((bid_abnormal + ask_abnormal) / (total_msgs * 2) * 100) if total_msgs > 0 else 0
            print(f"    - {coin}: 매수 {bid_abnormal:,}개, 매도 {ask_abnormal:,}개 ({abnormal_ratio:.1f}%)")
            
            # 비정상 뎁스 상세 정보 (최대 3개까지만 표시)
            if depth_errors:
                print("      [상세 정보 (최대 3개)]")
                for i, err in enumerate(depth_errors[:3]):
                    bid_depth = err.get('bid_depth')
                    ask_depth = err.get('ask_depth')
                    ts = err.get('timestamp')
                    
                    if ts:
                        if ts > 1000000000000000:  # 마이크로초
                            ts_str = datetime.fromtimestamp(ts/1000000).strftime('%Y-%m-%d %H:%M:%S.%f')
                        else:  # 밀리초
                            ts_str = datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
                    else:
                        ts_str = "Unknown"
                    
                    print(f"        {i+1}. 시간: {ts_str}")
                    print(f"           매수 뎁스: {bid_depth}, 매도 뎁스: {ask_depth}")

def summarize_analysis(stats):
    """분석 결과 요약"""
    print("\n=== 큐 데이터 분석 결과 요약 ===")
    
    # 전체 메시지 수
    total_messages = sum(ex_data['total_messages'] for ex_data in stats.values())
    total_exchanges = len(stats)
    total_coins = sum(len(ex_data['coins']) for ex_data in stats.values())
    
    print(f"총 {total_exchanges}개 거래소, {total_coins}개 코인, {total_messages:,}개 메시지 분석")
    
    # 결과를 파일로 저장
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    result_dir = os.path.join(current_dir, "logs", "queue_analysis_results")
    os.makedirs(result_dir, exist_ok=True)
    
    summary_file = os.path.join(result_dir, f"queue_summary_{timestamp}.txt")
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write("=== 큐 데이터 분석 결과 요약 ===\n\n")
        f.write(f"총 {total_exchanges}개 거래소, {total_coins}개 코인, {total_messages:,}개 메시지 분석\n\n")
        
        # 거래소별 상세 정보
        print("\n=== 거래소별 상세 정보 ===")
        for exchange, ex_data in sorted(stats.items()):
            total_msgs = ex_data['total_messages']
            coins_count = len(ex_data['coins'])
            
            # 거래소 전체 duration 계산
            first_ts = float('inf')
            last_ts = 0
            for coin_data in ex_data['coins'].values():
                if coin_data['first_timestamp'] is not None:
                    first_ts = min(first_ts, coin_data['first_timestamp'])
                if coin_data['last_timestamp'] is not None:
                    last_ts = max(last_ts, coin_data['last_timestamp'])
            
            if first_ts != float('inf') and last_ts > 0:
                if first_ts > 1000000000000000:  # 마이크로초
                    duration = (last_ts - first_ts) / 1000000
                else:  # 밀리초
                    duration = (last_ts - first_ts) / 1000
                msgs_per_second = total_msgs / duration if duration > 0 else 0
            else:
                msgs_per_second = 0
            
            # 거래소 전체 통계
            total_inversions = sum(coin_data.get('price_inversion_count', 0) for coin_data in ex_data['coins'].values())
            total_sequence_errors = sum(coin_data.get('sequence_error_count', 0) for coin_data in ex_data['coins'].values())
            total_bid_sort_errors = sum(coin_data.get('incorrect_bid_order_count', 0) for coin_data in ex_data['coins'].values())
            total_ask_sort_errors = sum(coin_data.get('incorrect_ask_order_count', 0) for coin_data in ex_data['coins'].values())
            
            # 비정상 뎁스 계산 (10이 아닌 모든 뎁스를 비정상으로 간주)
            total_abnormal_bid_depth = sum(
                count for coin_data in ex_data['coins'].values() 
                for depth, count in coin_data['bid_depths'].items() 
                if depth != 10
            )
            total_abnormal_ask_depth = sum(
                count for coin_data in ex_data['coins'].values() 
                for depth, count in coin_data['ask_depths'].items() 
                if depth != 10
            )
            
            print(f"\n[{exchange}]")
            print(f"  총 메시지: {total_msgs:,}개 ({msgs_per_second:.1f} msgs/s)")
            print(f"  총 코인 수: {coins_count}개")
            print(f"  가격 역전: {total_inversions:,}개 ({(total_inversions/total_msgs*100):.2f}%)")
            print(f"  시퀀스 오류: {total_sequence_errors:,}개 ({(total_sequence_errors/total_msgs*100):.2f}%)")
            print(f"  정렬 오류: 매수 {total_bid_sort_errors:,}개, 매도 {total_ask_sort_errors:,}개 (총 {((total_bid_sort_errors+total_ask_sort_errors)/total_msgs*100):.2f}%)")
            print(f"  비정상 뎁스: 매수 {total_abnormal_bid_depth:,}개, 매도 {total_abnormal_ask_depth:,}개 (총 {((total_abnormal_bid_depth+total_abnormal_ask_depth)/(total_msgs*2)*100):.2f}%)")
            
            f.write(f"\n[{exchange}]\n")
            f.write(f"  총 메시지: {total_msgs:,}개 ({msgs_per_second:.1f} msgs/s)\n")
            f.write(f"  총 코인 수: {coins_count}개\n")
            f.write(f"  가격 역전: {total_inversions:,}개 ({(total_inversions/total_msgs*100):.2f}%)\n")
            f.write(f"  시퀀스 오류: {total_sequence_errors:,}개 ({(total_sequence_errors/total_msgs*100):.2f}%)\n")
            f.write(f"  정렬 오류: 매수 {total_bid_sort_errors:,}개, 매도 {total_ask_sort_errors:,}개 (총 {((total_bid_sort_errors+total_ask_sort_errors)/total_msgs*100):.2f}%)\n")
            f.write(f"  비정상 뎁스: 매수 {total_abnormal_bid_depth:,}개, 매도 {total_abnormal_ask_depth:,}개 (총 {((total_abnormal_bid_depth+total_abnormal_ask_depth)/(total_msgs*2)*100):.2f}%)\n")
            
            # 코인별 상세 정보
            print("\n  [코인별 상세 정보]")
            print("  " + "-"*100)
            print("  {:<12} {:<10} {:<12} {:<12} {:<12} {:<20}".format(
                "코인", "메시지수", "가격역전", "시퀀스오류", "정렬오류", "비정상뎁스(매수/매도)"
            ))
            print("  " + "-"*100)
            
            for coin, coin_data in sorted(ex_data['coins'].items(), key=lambda x: x[1]['message_count'], reverse=True):
                coin_msgs = coin_data['message_count']
                
                # 코인별 duration 계산
                coin_first_ts = coin_data['first_timestamp']
                coin_last_ts = coin_data['last_timestamp']
                
                if coin_first_ts is not None and coin_last_ts is not None and coin_first_ts != coin_last_ts:
                    if coin_first_ts > 1000000000000000:  # 마이크로초
                        coin_duration = (coin_last_ts - coin_first_ts) / 1000000
                    else:  # 밀리초
                        coin_duration = (coin_last_ts - coin_first_ts) / 1000
                    coin_msgs_per_second = coin_msgs / coin_duration if coin_duration > 0 else 0
                else:
                    coin_msgs_per_second = 0
                
                inversions = coin_data.get('price_inversion_count', 0)
                sequence_errors = coin_data.get('sequence_error_count', 0)
                bid_sort_errors = coin_data.get('incorrect_bid_order_count', 0)
                ask_sort_errors = coin_data.get('incorrect_ask_order_count', 0)
                
                # 비정상 뎁스 계산 (10이 아닌 모든 뎁스를 비정상으로 간주)
                abnormal_bid_depth = sum(count for depth, count in coin_data['bid_depths'].items() if depth != 10)
                abnormal_ask_depth = sum(count for depth, count in coin_data['ask_depths'].items() if depth != 10)
                
                line_format = "  {:<12} {:<10,d} {:<12,d} {:<12,d} {:<12,d} {:<20}"
                print(line_format.format(
                    coin,
                    coin_msgs,
                    inversions,
                    sequence_errors,
                    bid_sort_errors + ask_sort_errors,
                    f"{abnormal_bid_depth}/{abnormal_ask_depth}"
                ))
                
                f.write(line_format.format(
                    coin,
                    coin_msgs,
                    inversions,
                    sequence_errors,
                    bid_sort_errors + ask_sort_errors,
                    f"{abnormal_bid_depth}/{abnormal_ask_depth}"
                ) + "\n")
            
            # 필드 오류 통계
            print("\n  [필드 오류 통계]")
            for coin, coin_data in sorted(ex_data['coins'].items()):
                missing_fields = sum(coin_data['missing_field_counts'].values())
                invalid_fields = sum(coin_data['invalid_field_counts'].values())
                
                if missing_fields > 0 or invalid_fields > 0:
                    print(f"\n    {coin}:")
                    if missing_fields > 0:
                        print("    - 누락된 필드:")
                        for field, count in coin_data['missing_field_counts'].items():
                            if count > 0:
                                print(f"      * {field}: {count:,}회 ({(count/coin_data['message_count']*100):.2f}%)")
                    if invalid_fields > 0:
                        print("    - 형식 오류 필드:")
                        for field, count in coin_data['invalid_field_counts'].items():
                            if count > 0:
                                print(f"      * {field}: {count:,}회 ({(count/coin_data['message_count']*100):.2f}%)")
                else:
                    print(f"    {coin}: 모든 필드 정상")

    print(f"\n분석 결과가 {summary_file} 파일에 저장되었습니다.")
    
    return summary_file

def analyze_specific_queue_log(log_file_path):
    """특정 큐 로그 파일 분석"""
    start_time = time.time()
    
    stats = defaultdict(lambda: {
        'total_messages': 0,
        'coins': defaultdict(lambda: {
            'message_count': 0,
            'bid_depths': defaultdict(int),
            'ask_depths': defaultdict(int),
            'price_inversion_count': 0,
            'price_inversions': [],
            'first_timestamp': None,
            'last_timestamp': None,
            'sequence_error_count': 0,
            'incorrect_bid_order_count': 0,
            'incorrect_ask_order_count': 0,
            'last_sequence': None,
            'missing_field_counts': {  # 필수 필드 누락 카운트
                'exchangename': 0,
                'symbol': 0,
                'bids': 0,
                'asks': 0,
                'timestamp': 0,
                'sequence': 0
            },
            'invalid_field_counts': {  # 필드 타입/형식 오류 카운트
                'bids_format': 0,
                'asks_format': 0,
                'timestamp_format': 0,
                'sequence_format': 0
            },
            'depth_errors': [],        # 비정상 뎁스 오류 저장
            'sequence_errors': [],     # 시퀀스 오류 저장
            'order_errors': [],        # 정렬 오류 저장
            'field_errors': []         # 필드 오류 저장
        })
    })
    
    processing_stats = {
        'total_lines': 0,
        'total_matches': 0,
        'total_valid': 0,
        'start_time': start_time
    }
    
    print(f"분석할 큐 로그 파일: {os.path.basename(log_file_path)}")
    
    try:
        with open(log_file_path, "r", encoding="utf-8") as f:
            for line in f:
                processing_stats['total_lines'] += 1
                line = line.strip()
                if not line:
                    continue
                
                # 새로운 로그 패턴으로 먼저 시도
                match = NEW_QUEUE_LOG_PATTERN.search(line)
                if match:
                    processing_stats['total_matches'] += 1
                    
                    try:
                        timestamp_str = match.group(1)
                        exchange = match.group(2)
                        data_str = match.group(3)
                        data_str = convert_single_to_double_quotes(data_str)
                        data = json.loads(data_str)
                        processing_stats['total_valid'] += 1
                        
                        # 타임스탬프 처리
                        timestamp = data.get('timestamp')
                        if timestamp:
                            coin_stats = stats[exchange]['coins'][data.get('symbol', 'UNKNOWN')]
                            if coin_stats['first_timestamp'] is None:
                                coin_stats['first_timestamp'] = timestamp
                            coin_stats['last_timestamp'] = timestamp
                    except (json.JSONDecodeError, IndexError) as e:
                        print(f"JSON 파싱 오류: {e}")
                        continue
                    
                    # 통계 수집
                    stats[exchange]['total_messages'] += 1
                    symbol = data.get('symbol', 'UNKNOWN')
                    coin_stats = stats[exchange]['coins'][symbol]
                    coin_stats['message_count'] += 1
                    
                    # 시퀀스 오류 상세 정보 저장을 위한 리스트 초기화
                    if 'sequence_errors' not in coin_stats:
                        coin_stats['sequence_errors'] = []
                    if 'order_errors' not in coin_stats:
                        coin_stats['order_errors'] = []
                    if 'field_errors' not in coin_stats:
                        coin_stats['field_errors'] = []
                    if 'depth_errors' not in coin_stats:
                        coin_stats['depth_errors'] = []
                    
                    # Depth 통계
                    bids = data.get('bids', [])
                    asks = data.get('asks', [])
                    bid_depth = len(bids)
                    ask_depth = len(asks)
                    
                    # 가격 역전 검사 - 모든 데이터에 대해 수행
                    if bids and asks:
                        try:
                            highest_bid = float(bids[0][0])
                            lowest_ask = float(asks[0][0])
                            # 최고 매수가가 최저 매도가보다 큰 경우에만 가격 역전으로 간주
                            if highest_bid > lowest_ask:
                                coin_stats['price_inversion_count'] += 1
                                if len(coin_stats['price_inversions']) < 100:
                                    coin_stats['price_inversions'].append({
                                        'timestamp': data.get('timestamp'),
                                        'bid_price': highest_bid,
                                        'ask_price': lowest_ask,
                                        'diff': highest_bid - lowest_ask,
                                        'raw_data': data
                                    })
                        except (ValueError, IndexError):
                            continue
                    
                    # 비정상 뎁스 검사 - 항상 뎁스 정보 저장
                        coin_stats['bid_depths'][bid_depth] += 1
                        coin_stats['ask_depths'][ask_depth] += 1
                        
                    # 비정상 뎁스인 경우에만 상세 로그 저장
                    if bid_depth != 10 or ask_depth != 10:
                        if len(coin_stats['depth_errors']) < 100:  # 최대 100개까지만 저장
                            coin_stats['depth_errors'].append({
                                'timestamp': data.get('timestamp'),
                                'bid_depth': bid_depth,
                                'ask_depth': ask_depth,
                                'raw_data': data
                            })
                    
                    # 시퀀스 오류 검사
                    sequence = data.get('sequence')
                    if sequence is not None:
                        last_sequence = coin_stats['last_sequence']
                        if last_sequence is not None:
                            if data.get("type") != "snapshot" and sequence <= last_sequence:
                                coin_stats['sequence_error_count'] += 1
                                if len(coin_stats['sequence_errors']) < 100:
                                    coin_stats['sequence_errors'].append({
                                        'timestamp': data.get('timestamp'),
                                        'prev_sequence': last_sequence,
                                        'curr_sequence': sequence,
                                        'raw_data': data
                                    })
                        coin_stats['last_sequence'] = sequence
                    
                    # 정렬 순서 검사
                    if len(bids) > 1:
                        for i in range(len(bids) - 1):
                            if float(bids[i][0]) < float(bids[i+1][0]):
                                coin_stats['incorrect_bid_order_count'] += 1
                                if len(coin_stats['order_errors']) < 100:
                                    coin_stats['order_errors'].append({
                                        'timestamp': data.get('timestamp'),
                                        'type': 'bid',
                                        'prices': [bids[i][0], bids[i+1][0]],
                                        'raw_data': data
                                    })
                                break
                    if len(asks) > 1:
                        for i in range(len(asks) - 1):
                            if float(asks[i][0]) > float(asks[i+1][0]):
                                coin_stats['incorrect_ask_order_count'] += 1
                                if len(coin_stats['order_errors']) < 100:
                                    coin_stats['order_errors'].append({
                                        'timestamp': data.get('timestamp'),
                                        'type': 'ask',
                                        'prices': [asks[i][0], asks[i+1][0]],
                                        'raw_data': data
                                    })
                                break
                    
                    # 필드 형식 검사 및 오류 상세 정보 저장
                    if 'bids' in data:
                        try:
                            if not all(len(bid) == 2 and 
                                       all(isinstance(x, (int, float)) or 
                                       (isinstance(x, str) and x.replace('.', '').isdigit()) 
                                       for x in bid) 
                                       for bid in data['bids']):
                                coin_stats['invalid_field_counts']['bids_format'] += 1
                        except Exception:
                            coin_stats['invalid_field_counts']['bids_format'] += 1
                        
                        if 'asks' in data:
                            try:
                                if not all(len(ask) == 2 and 
                                       all(isinstance(x, (int, float)) or 
                                       (isinstance(x, str) and x.replace('.', '').isdigit()) 
                                       for x in ask) 
                                       for ask in data['asks']):
                                    coin_stats['invalid_field_counts']['asks_format'] += 1
                            except Exception:
                                coin_stats['invalid_field_counts']['asks_format'] += 1
                        
                        if 'timestamp' in data:
                            try:
                                ts = int(str(data['timestamp']))
                                if not (1000000000000 <= ts <= 9999999999999999):  # 밀리초 또는 마이크로초 범위
                                    coin_stats['invalid_field_counts']['timestamp_format'] += 1
                                    if len(coin_stats['field_errors']) < 100:
                                        coin_stats['field_errors'].append({
                                            'timestamp': data.get('timestamp'),
                                            'field': 'timestamp',
                                            'error_type': '형식 오류',
                                            'value': str(data['timestamp']),
                                            'raw_data': data
                                        })
                            except Exception:
                                coin_stats['invalid_field_counts']['timestamp_format'] += 1
                        
                        if 'sequence' in data:
                            try:
                                seq = int(str(data['sequence']))
                                if seq < 0:
                                    coin_stats['invalid_field_counts']['sequence_format'] += 1
                                    if len(coin_stats['field_errors']) < 100:
                                        coin_stats['field_errors'].append({
                                            'timestamp': data.get('timestamp'),
                                            'field': 'sequence',
                                            'error_type': '형식 오류',
                                            'value': str(data['sequence']),
                                            'raw_data': data
                                        })
                            except Exception:
                                coin_stats['invalid_field_counts']['sequence_format'] += 1
                else:
                    # 기존 로그 패턴으로 시도
                    match = OLD_QUEUE_LOG_PATTERN.search(line)
                    if match:
                        # 기존 로그 처리 로직 (생략)
                        pass

    except Exception as e:
        print(f"파일 처리 중 오류 발생: {e}")
    
    # 결과 저장
    end_time = time.time()
    processing_stats['end_time'] = end_time
    processing_stats['total_time'] = end_time - start_time
    
    print(f"\n분석 완료!")
    print(f"총 소요 시간: {processing_stats['total_time']:.1f}초")
    print(f"총 처리 라인: {processing_stats['total_lines']:,}줄")
    print(f"매칭된 로그: {processing_stats['total_matches']:,}개")
    print(f"유효한 데이터: {processing_stats['total_valid']:,}개")
    
    # 거래소별 통계 요약 출력
    print("\n=== 거래소별 통계 요약 ===")
    for exchange, ex_data in sorted(stats.items()):
        total_msgs = ex_data['total_messages']
        coins_count = len(ex_data['coins'])
        
        # 거래소 전체 duration 계산
        first_ts = float('inf')
        last_ts = 0
        for coin_data in ex_data['coins'].values():
            if coin_data['first_timestamp'] is not None:
                first_ts = min(first_ts, coin_data['first_timestamp'])
            if coin_data['last_timestamp'] is not None:
                last_ts = max(last_ts, coin_data['last_timestamp'])
        
        if first_ts != float('inf') and last_ts > 0:
            if first_ts > 1000000000000000:  # 마이크로초
                duration = (last_ts - first_ts) / 1000000
            else:  # 밀리초
                duration = (last_ts - first_ts) / 1000
            msgs_per_second = total_msgs / duration if duration > 0 else 0
        else:
            msgs_per_second = 0
        
        # 거래소 전체 통계
        total_inversions = sum(coin_data.get('price_inversion_count', 0) for coin_data in ex_data['coins'].values())
        total_sequence_errors = sum(coin_data.get('sequence_error_count', 0) for coin_data in ex_data['coins'].values())
        total_bid_sort_errors = sum(coin_data.get('incorrect_bid_order_count', 0) for coin_data in ex_data['coins'].values())
        total_ask_sort_errors = sum(coin_data.get('incorrect_ask_order_count', 0) for coin_data in ex_data['coins'].values())
        
        # 비정상 뎁스 계산 (10이 아닌 모든 뎁스를 비정상으로 간주)
        total_abnormal_bid_depth = sum(
            count for coin_data in ex_data['coins'].values() 
            for depth, count in coin_data['bid_depths'].items() 
            if depth != 10
        )
        total_abnormal_ask_depth = sum(
            count for coin_data in ex_data['coins'].values() 
            for depth, count in coin_data['ask_depths'].items() 
            if depth != 10
        )
        
        print(f"\n[{exchange}]")
        print(f"  총 메시지: {total_msgs:,}개 ({msgs_per_second:.1f} msgs/s)")
        print(f"  총 코인 수: {coins_count}개")
        print(f"  가격 역전: {total_inversions:,}개 ({(total_inversions/total_msgs*100):.2f}%)")
        print(f"  시퀀스 오류: {total_sequence_errors:,}개 ({(total_sequence_errors/total_msgs*100):.2f}%)")
        print(f"  정렬 오류: 매수 {total_bid_sort_errors:,}개, 매도 {total_ask_sort_errors:,}개 (총 {((total_bid_sort_errors+total_ask_sort_errors)/total_msgs*100):.2f}%)")
        print(f"  비정상 뎁스: 매수 {total_abnormal_bid_depth:,}개, 매도 {total_abnormal_ask_depth:,}개 (총 {((total_abnormal_bid_depth+total_abnormal_ask_depth)/(total_msgs*2)*100):.2f}%)")
        
        # 코인별 상세 정보
        print("\n  [코인별 상세 정보]")
        print("  " + "-"*100)
        print("  {:<12} {:<10} {:<12} {:<12} {:<12} {:<20}".format(
            "코인", "메시지수", "가격역전", "시퀀스오류", "정렬오류", "비정상뎁스(매수/매도)"
        ))
        print("  " + "-"*100)
        
        for coin, coin_data in sorted(ex_data['coins'].items(), key=lambda x: x[1]['message_count'], reverse=True):
            coin_msgs = coin_data['message_count']
            
            # 코인별 duration 계산
            coin_first_ts = coin_data['first_timestamp']
            coin_last_ts = coin_data['last_timestamp']
            
            if coin_first_ts is not None and coin_last_ts is not None and coin_first_ts != coin_last_ts:
                if coin_first_ts > 1000000000000000:  # 마이크로초
                    coin_duration = (coin_last_ts - coin_first_ts) / 1000000
                else:  # 밀리초
                    coin_duration = (coin_last_ts - coin_first_ts) / 1000
                coin_msgs_per_second = coin_msgs / coin_duration if coin_duration > 0 else 0
            else:
                coin_msgs_per_second = 0
            
            inversions = coin_data.get('price_inversion_count', 0)
            sequence_errors = coin_data.get('sequence_error_count', 0)
            bid_sort_errors = coin_data.get('incorrect_bid_order_count', 0)
            ask_sort_errors = coin_data.get('incorrect_ask_order_count', 0)
            
            # 비정상 뎁스 계산 (10이 아닌 모든 뎁스를 비정상으로 간주)
            abnormal_bid_depth = sum(count for depth, count in coin_data['bid_depths'].items() if depth != 10)
            abnormal_ask_depth = sum(count for depth, count in coin_data['ask_depths'].items() if depth != 10)
            
            print("  {:<12} {:<10,d} {:<12,d} {:<12,d} {:<12,d} {:<20}".format(
                coin,
                coin_msgs,
                inversions,
                sequence_errors,
                bid_sort_errors + ask_sort_errors,
                f"{abnormal_bid_depth}/{abnormal_ask_depth}"
            ))
        
        # 필드 오류 통계
        print("\n  [필드 오류 통계]")
        for coin, coin_data in sorted(ex_data['coins'].items()):
            missing_fields = sum(coin_data['missing_field_counts'].values())
            invalid_fields = sum(coin_data['invalid_field_counts'].values())
            
            if missing_fields > 0 or invalid_fields > 0:
                print(f"\n    {coin}:")
                if missing_fields > 0:
                    print("    - 누락된 필드:")
                    for field, count in coin_data['missing_field_counts'].items():
                        if count > 0:
                                print(f"      * {field}: {count:,}회 ({(count/coin_data['message_count']*100):.2f}%)")
                if invalid_fields > 0:
                    print("    - 형식 오류 필드:")
                    for field, count in coin_data['invalid_field_counts'].items():
                        if count > 0:
                                print(f"      * {field}: {count:,}회 ({(count/coin_data['message_count']*100):.2f}%)")
            else:
                    print(f"    {coin}: 모든 필드 정상")

    # 결과를 파일로 저장
    summary_file = save_queue_results(stats, processing_stats)
    
    print(f"\n분석이 완료되었습니다.")
    print(f"분석 결과가 {summary_file} 파일에 저장되었습니다.")
    
    return stats  # stats 객체를 반환하도록 수정

def analyze_multiple_queue_logs(log_files):
    """여러 큐 로그 파일을 분석"""
    if not log_files:
        print("\n분석할 큐 로그 파일이 없습니다.")
        return None
    
    start_time = time.time()
    stats = defaultdict(lambda: {
        'total_messages': 0,
        'coins': defaultdict(lambda: {
            'message_count': 0,
            'bid_depths': defaultdict(int),
            'ask_depths': defaultdict(int),
            'price_inversion_count': 0,
            'price_inversions': [],
            'first_timestamp': None,
            'last_timestamp': None,
            'sequence_error_count': 0,
            'incorrect_bid_order_count': 0,
            'incorrect_ask_order_count': 0,
            'last_sequence': None,
            'missing_field_counts': {
                'exchangename': 0,
                'symbol': 0,
                'bids': 0,
                'asks': 0,
                'timestamp': 0,
                'sequence': 0
            },
            'invalid_field_counts': {
                'bids_format': 0,
                'asks_format': 0,
                'timestamp_format': 0,
                'sequence_format': 0
            },
            'depth_errors': [],
            'sequence_errors': [],
            'order_errors': [],
            'field_errors': []
        })
    })

    processing_stats = {
        'total_lines': 0,
        'total_matches': 0,
        'total_valid': 0,
        'start_time': start_time
    }

    print(f"\n분석할 큐 로그 파일: {len(log_files)}개")

    for log_file in log_files:
        print(f"\n처리 중... {os.path.basename(log_file)}")
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                for line in f:
                    processing_stats['total_lines'] += 1
                    line = line.strip()
                    if not line:
                        continue
                    
                    # 새로운 로그 패턴으로 먼저 시도
                    match = NEW_QUEUE_LOG_PATTERN.search(line)
                    if match:
                        processing_stats['total_matches'] += 1
                        
                        try:
                            timestamp_str = match.group(1)
                            # 두 번째 그룹은 거래소 정보 또는 로그 메시지
                            log_message = match.group(2)
                            data_str = match.group(3)
                            data_str = convert_single_to_double_quotes(data_str)
                            data = json.loads(data_str)
                            processing_stats['total_valid'] += 1
                            
                            # 거래소 이름 추출
                            # 1. JSON 데이터에서 exchangename 필드 확인
                            if 'exchangename' in data:
                                exchange = data['exchangename']
                            else:
                                # 2. 로그 메시지에서 거래소 이름 추출 시도
                                exchange = log_message.strip()
                            
                            # 타임스탬프 처리
                            timestamp = data.get('timestamp')
                            if timestamp:
                                coin_stats = stats[exchange]['coins'][data.get('symbol', 'UNKNOWN')]
                                if coin_stats['first_timestamp'] is None:
                                    coin_stats['first_timestamp'] = timestamp
                                coin_stats['last_timestamp'] = timestamp
                        except (json.JSONDecodeError, IndexError) as e:
                            # print(f"JSON 파싱 오류: {e}")
                            continue
                        
                        # 통계 수집
                        stats[exchange]['total_messages'] += 1
                        symbol = data.get('symbol', 'UNKNOWN')
                        coin_stats = stats[exchange]['coins'][symbol]
                        coin_stats['message_count'] += 1
                        
                        # 시퀀스 오류 상세 정보 저장을 위한 리스트 초기화
                        if 'sequence_errors' not in coin_stats:
                            coin_stats['sequence_errors'] = []
                        if 'order_errors' not in coin_stats:
                            coin_stats['order_errors'] = []
                        if 'field_errors' not in coin_stats:
                            coin_stats['field_errors'] = []
                        if 'depth_errors' not in coin_stats:
                            coin_stats['depth_errors'] = []
                        
                        # Depth 통계
                        bids = data.get('bids', [])
                        asks = data.get('asks', [])
                        bid_depth = len(bids)
                        ask_depth = len(asks)
                        
                        # 가격 역전 검사 - 모든 데이터에 대해 수행
                        if bids and asks:
                            try:
                                highest_bid = float(bids[0][0])
                                lowest_ask = float(asks[0][0])
                                # 최고 매수가가 최저 매도가보다 큰 경우에만 가격 역전으로 간주
                                if highest_bid > lowest_ask:
                                    coin_stats['price_inversion_count'] += 1
                                    if len(coin_stats['price_inversions']) < 100:
                                        coin_stats['price_inversions'].append({
                                            'timestamp': data.get('timestamp'),
                                            'bid_price': highest_bid,
                                            'ask_price': lowest_ask,
                                            'diff': highest_bid - lowest_ask,
                                            'raw_data': data
                                        })
                            except (ValueError, IndexError):
                                continue
                        
                        # 비정상 뎁스 검사 - 항상 뎁스 정보 저장
                        coin_stats['bid_depths'][bid_depth] += 1
                        coin_stats['ask_depths'][ask_depth] += 1
                            
                        # 비정상 뎁스인 경우에만 상세 로그 저장
                        if bid_depth != 10 or ask_depth != 10:
                            if len(coin_stats['depth_errors']) < 100:  # 최대 100개까지만 저장
                                coin_stats['depth_errors'].append({
                                    'timestamp': data.get('timestamp'),
                                    'bid_depth': bid_depth,
                                    'ask_depth': ask_depth,
                                    'raw_data': data
                                })
                        
                        # 시퀀스 오류 검사
                        sequence = data.get('sequence')
                        if sequence is not None:
                            last_sequence = coin_stats['last_sequence']
                            if last_sequence is not None:
                                if data.get("type") != "snapshot" and sequence <= last_sequence:
                                    coin_stats['sequence_error_count'] += 1
                                    if len(coin_stats['sequence_errors']) < 100:
                                        coin_stats['sequence_errors'].append({
                                            'timestamp': data.get('timestamp'),
                                            'prev_sequence': last_sequence,
                                            'curr_sequence': sequence,
                                            'raw_data': data
                                        })
                            coin_stats['last_sequence'] = sequence
                        
                        # 정렬 순서 검사
                        if len(bids) > 1:
                            for i in range(len(bids) - 1):
                                try:
                                    if float(bids[i][0]) < float(bids[i+1][0]):
                                        coin_stats['incorrect_bid_order_count'] += 1
                                        if len(coin_stats['order_errors']) < 100:
                                            coin_stats['order_errors'].append({
                                                'timestamp': data.get('timestamp'),
                                                'type': 'bid',
                                                'prices': [bids[i][0], bids[i+1][0]],
                                                'raw_data': data
                                            })
                                        break
                                except (ValueError, IndexError):
                                    continue
                        if len(asks) > 1:
                            for i in range(len(asks) - 1):
                                try:
                                    if float(asks[i][0]) > float(asks[i+1][0]):
                                        coin_stats['incorrect_ask_order_count'] += 1
                                        if len(coin_stats['order_errors']) < 100:
                                            coin_stats['order_errors'].append({
                                                'timestamp': data.get('timestamp'),
                                                'type': 'ask',
                                                'prices': [asks[i][0], asks[i+1][0]],
                                                'raw_data': data
                                            })
                                        break
                                except (ValueError, IndexError):
                                    continue
                        
                        # 필드 형식 검사 및 오류 상세 정보 저장
                        if 'bids' in data:
                            try:
                                if not all(len(bid) == 2 and 
                                           all(isinstance(x, (int, float)) or 
                                           (isinstance(x, str) and x.replace('.', '').isdigit()) 
                                           for x in bid) 
                                           for bid in data['bids']):
                                    coin_stats['invalid_field_counts']['bids_format'] += 1
                            except Exception:
                                coin_stats['invalid_field_counts']['bids_format'] += 1
                        
                        if 'asks' in data:
                            try:
                                if not all(len(ask) == 2 and 
                                           all(isinstance(x, (int, float)) or 
                                           (isinstance(x, str) and x.replace('.', '').isdigit()) 
                                           for x in ask) 
                                           for ask in data['asks']):
                                    coin_stats['invalid_field_counts']['asks_format'] += 1
                            except Exception:
                                coin_stats['invalid_field_counts']['asks_format'] += 1
                        
                        if 'timestamp' in data:
                            try:
                                ts = int(str(data['timestamp']))
                                if not (1000000000000 <= ts <= 9999999999999999):  # 밀리초 또는 마이크로초 범위
                                    coin_stats['invalid_field_counts']['timestamp_format'] += 1
                                    if len(coin_stats['field_errors']) < 100:
                                        coin_stats['field_errors'].append({
                                            'timestamp': data.get('timestamp'),
                                            'field': 'timestamp',
                                            'error_type': '형식 오류',
                                            'value': str(data['timestamp']),
                                            'raw_data': data
                                        })
                            except Exception:
                                coin_stats['invalid_field_counts']['timestamp_format'] += 1
                        
                        if 'sequence' in data:
                            try:
                                seq = int(str(data['sequence']))
                                if seq < 0:
                                    coin_stats['invalid_field_counts']['sequence_format'] += 1
                                    if len(coin_stats['field_errors']) < 100:
                                        coin_stats['field_errors'].append({
                                            'timestamp': data.get('timestamp'),
                                            'field': 'sequence',
                                            'error_type': '형식 오류',
                                            'value': str(data['sequence']),
                                            'raw_data': data
                                        })
                            except Exception:
                                coin_stats['invalid_field_counts']['sequence_format'] += 1
                    else:
                        # 기존 로그 패턴으로 시도
                        match = OLD_QUEUE_LOG_PATTERN.search(line)
                        if match:
                            # 기존 로그 처리 로직 (생략)
                            pass

        except Exception as e:
            print(f"파일 처리 중 오류 발생: {e}")
            continue
    
    # 결과 저장
    end_time = time.time()
    processing_stats['end_time'] = end_time
    processing_stats['total_time'] = end_time - start_time
    
    # 모든 거래소 요약 정보를 먼저 출력
    print_exchange_summary(stats)
    
    summary_file = save_queue_results(stats, processing_stats)
    
    print(f"\n분석 완료!")
    print(f"총 소요 시간: {processing_stats['total_time']:.1f}초")
    print(f"총 처리 라인: {processing_stats['total_lines']:,}줄")
    print(f"매칭된 로그: {processing_stats['total_matches']:,}개")
    print(f"유효한 데이터: {processing_stats['total_valid']:,}개")
    
    print(f"\n분석 결과가 {summary_file} 파일에 저장되었습니다.")
    
    return stats

def print_exchange_summary(stats):
    """모든 거래소 요약 정보를 출력"""
    print("\n=== 모든 거래소 요약 정보 ===")
    
    # 전체 메시지 수
    total_messages = sum(ex_data['total_messages'] for ex_data in stats.values())
    total_exchanges = len(stats)
    total_coins = sum(len(ex_data['coins']) for ex_data in stats.values())
    
    print(f"총 {total_exchanges}개 거래소, {total_coins}개 코인, {total_messages:,}개 메시지 분석")
    
    # 거래소별 요약 정보 테이블 헤더
    print("\n" + "-"*120)
    print("{:<12} {:<12} {:<12} {:<12} {:<12} {:<12} {:<20}".format(
        "거래소", "메시지수", "코인수", "가격역전", "시퀀스오류", "정렬오류", "비정상뎁스(매수/매도)"
    ))
    print("-"*120)
    
    # 거래소별 요약 정보
    for exchange, ex_data in sorted(stats.items()):
        total_msgs = ex_data['total_messages']
        coins_count = len(ex_data['coins'])
        
        # 각 통계 값 계산
        total_inversions = sum(coin_data.get('price_inversion_count', 0) for coin_data in ex_data['coins'].values())
        total_sequence_errors = sum(coin_data.get('sequence_error_count', 0) for coin_data in ex_data['coins'].values())
        total_order_errors = sum(coin_data.get('incorrect_bid_order_count', 0) + coin_data.get('incorrect_ask_order_count', 0) 
                                for coin_data in ex_data['coins'].values())
        
        # 비정상 뎁스 계산 (10이 아닌 모든 뎁스를 비정상으로 간주)
        total_abnormal_bid_depth = sum(
            count for coin_data in ex_data['coins'].values() 
            for depth, count in coin_data['bid_depths'].items() 
            if depth != 10
        )
        total_abnormal_ask_depth = sum(
            count for coin_data in ex_data['coins'].values() 
            for depth, count in coin_data['ask_depths'].items() 
            if depth != 10
        )
        
        # 거래소 요약 정보 출력
        print("{:<12} {:<12,d} {:<12d} {:<12,d} {:<12,d} {:<12,d} {:<20}".format(
            exchange, 
            total_msgs, 
            coins_count,
            total_inversions,
            total_sequence_errors,
            total_order_errors,
            f"{total_abnormal_bid_depth}/{total_abnormal_ask_depth}"
        ))
    
    print("-"*120)
    print("\n")

def main():
    parser = argparse.ArgumentParser(description='큐 로그 파일 분석 도구')
    parser.add_argument('--file', type=str, help='분석할 특정 로그 파일 경로')
    parser.add_argument('--exchange', type=str, help='특정 거래소만 상세 분석 (예: bybit, binance, upbit)')
    parser.add_argument('--all', action='store_true', help='모든 로그 파일 그룹 분석')
    args = parser.parse_args()
    
    if args.file:
        # 특정 파일 분석
        print(f"\n지정된 파일 분석: {args.file}")
        stats = analyze_specific_queue_log(args.file)
        
        # 모든 거래소 요약 정보를 먼저 출력
        if stats:
            print_exchange_summary(stats)
    elif args.all:
        # 모든 로그 파일 그룹 분석
        print("\n전체 로그 파일 그룹 분석 시작...")
        
        # 로그 디렉토리
        logs_dir = "/Users/kun/Desktop/CrossKimpArbitrage/v0.6/src/logs"
        
        # 모든 로그 파일 찾기
        all_queue_logs = []
        for root, _, files in os.walk(logs_dir):
            for fname in files:
                if "_queue_logger.log" in fname:
                    full_path = os.path.join(root, fname)
                    all_queue_logs.append(full_path)
        
        if not all_queue_logs:
            print("\n분석할 큐 로그 파일이 없습니다.")
            return
        
        # 로그 파일을 날짜/시간 접두사로 그룹화
        log_groups = {}
        for log_path in all_queue_logs:
            # 파일명에서 날짜/시간 접두사 추출
            filename = os.path.basename(log_path)
            # 날짜/시간 접두사 추출 시도
            prefix_match = re.match(r'(\d{6}_\d{6})_', filename)
            if prefix_match:
                prefix = prefix_match.group(1)
            else:
                # 접두사가 없는 경우 파일 수정 시간을 기준으로 그룹화
                mtime = os.path.getmtime(log_path)
                mtime_str = datetime.fromtimestamp(mtime).strftime('%y%m%d_%H%M%S')
                prefix = mtime_str
            
            if prefix not in log_groups:
                log_groups[prefix] = []
            log_groups[prefix].append(log_path)
        
        # 각 그룹별로 분석 수행
        all_stats = {}
        for prefix, logs in sorted(log_groups.items(), reverse=True):
            print(f"\n로그 그룹 분석: {prefix}, 총 {len(logs)}개 파일")
            group_stats = analyze_multiple_queue_logs(logs)
            if group_stats:
                all_stats[prefix] = group_stats
                
                # 각 그룹별 상세 분석 (선택적)
                if args.exchange:
                    print(f"\n{args.exchange} 거래소 상세 분석 (그룹: {prefix})...")
                    analyze_price_inversions(group_stats, args.exchange)
                    analyze_depth_issues(group_stats, args.exchange)
        
        print(f"\n총 {len(all_stats)}개 로그 그룹 분석 완료")
        return
    else:
        # 기본적으로 가장 최신 로그 파일 그룹 분석
        latest_logs = find_latest_queue_log()
        if latest_logs:
            print(f"\n최신 로그 파일 그룹 분석 시작...")
            stats = analyze_multiple_queue_logs(latest_logs)
        else:
            return
    
    if stats:
        if args.exchange:
            # 특정 거래소 상세 분석
            print(f"\n{args.exchange} 거래소 상세 분석...")
            analyze_price_inversions(stats, args.exchange)
            analyze_depth_issues(stats, args.exchange)
        else:
            # 모든 거래소 분석
            print("\n전체 거래소 상세 분석...")
            analyze_price_inversions(stats)
            analyze_depth_issues(stats)
            
        print("\n분석이 완료되었습니다.")

if __name__ == "__main__":
    main() 