#!/usr/bin/env python3
import os
import glob
import json
import re
from datetime import datetime

# 로그 디렉토리 설정
LOG_DIR = "/Users/kun/Desktop/CrossKimpArbitrage/v0.6/src/logs/raw_data"

# 거래소 목록
EXCHANGES = [
    "binance_future", "binance_spot", "bybit", 
    "bithumb", "bybit_future", "upbit"
]

def get_latest_log_for_exchange(exchange_name):
    """특정 거래소의 최신 로그 파일 찾기"""
    pattern = os.path.join(LOG_DIR, f"*_{exchange_name}_raw_logger.log")
    files = glob.glob(pattern)
    
    if not files:
        return None
    
    # 파일 이름에서 날짜/시간 추출 후 최신 파일 반환
    try:
        latest_file = max(files, key=lambda f: os.path.getmtime(f))
        return latest_file
    except Exception as e:
        print(f"Error finding latest log for {exchange_name}: {str(e)}")
        return None

def check_price_inversion(bids, asks):
    """
    가격역전 현상 검사
    
    Args:
        bids: 매수 호가 배열 [[가격, 수량], ...]
        asks: 매도 호가 배열 [[가격, 수량], ...]
        
    Returns:
        (bool, str, float): (역전 여부, 역전 내용 설명, 역전 정도(%))
    """
    # 빈 배열인 경우 체크
    if not bids or not asks:
        return False, "Empty orderbook", 0.0
    
    # 1. 매수 최고가가 매도 최저가보다 높은지 확인
    highest_bid = float(bids[0][0])
    lowest_ask = float(asks[0][0])
    
    if highest_bid > lowest_ask:  # 같은 경우(equal)는 역전이 아니라 스프레드가 0인 케이스로 간주
        # 역전 정도를 백분율로 계산 ((매수최고가 - 매도최저가) / 매도최저가 * 100)
        inversion_percent = (highest_bid - lowest_ask) / lowest_ask * 100
        return True, f"가격역전: 매수최고가({highest_bid}) > 매도최저가({lowest_ask})", inversion_percent
    
    # 2. 매수 호가 내부 정렬 확인 (내림차순)
    for i in range(len(bids)-1):
        if float(bids[i][0]) < float(bids[i+1][0]):
            diff_percent = (float(bids[i+1][0]) - float(bids[i][0])) / float(bids[i][0]) * 100
            return True, f"매수호가 정렬역전: {bids[i][0]} < {bids[i+1][0]}", diff_percent
    
    # 3. 매도 호가 내부 정렬 확인 (오름차순)
    for i in range(len(asks)-1):
        if float(asks[i][0]) > float(asks[i+1][0]):
            diff_percent = (float(asks[i][0]) - float(asks[i+1][0])) / float(asks[i+1][0]) * 100
            return True, f"매도호가 정렬역전: {asks[i][0]} > {asks[i+1][0]}", diff_percent
    
    return False, "", 0.0

def analyze_processed_logs(file_path):
    """
    "매수 (10) / 매도 (10)" 형식의 처리된 로그만 분석
    
    Args:
        file_path: 분석할 로그 파일 경로
    
    Returns:
        total_count: 총 로그 항목 수
        valid_count: 정상 항목 수 (bids/asks 배열이 실제로 10개인 것)
        invalid_details: 비정상 항목 상세 내역
        price_inversion_count: 가격역전 항목 수
        inversion_details: 가격역전 상세 내역
        symbol_stats: 심볼별 가격역전 통계
    """
    if not file_path or not os.path.exists(file_path):
        return 0, 0, [], 0, [], {}
    
    total_count = 0
    valid_count = 0
    invalid_details = []
    price_inversion_count = 0
    inversion_details = []
    symbol_stats = {}  # 심볼별 통계 저장
    
    pattern = r'매수 \(10\) / 매도 \(10\) (\{.*\})'
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line_num, line in enumerate(f, 1):
            # "매수 (10) / 매도 (10)" 형식의 로그만 처리
            match = re.search(pattern, line)
            if not match:
                continue
            
            try:
                # JSON 데이터 추출 및 파싱
                json_text = match.group(1)
                json_data = json.loads(json_text)
                
                # bids와 asks 배열 확인
                bids = json_data.get('bids', [])
                asks = json_data.get('asks', [])
                symbol = json_data.get('symbol', 'unknown')
                
                # 심볼별 통계 초기화
                if symbol not in symbol_stats:
                    symbol_stats[symbol] = {
                        'total': 0,
                        'inversion': 0,
                        'max_inversion_percent': 0.0
                    }
                
                symbol_stats[symbol]['total'] += 1
                total_count += 1
                
                # 배열 길이 확인 (실제로 10개인지)
                if len(bids) == 10 and len(asks) == 10:
                    valid_count += 1
                else:
                    # 비정상 항목 상세 정보 저장
                    exchange = json_data.get('exchange', 'unknown')
                    timestamp = json_data.get('ts', 0)
                    
                    time_str = datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S') if timestamp else 'unknown'
                    
                    invalid_details.append({
                        'line': line_num,
                        'exchange': exchange,
                        'symbol': symbol,
                        'time': time_str,
                        'bids_count': len(bids),
                        'asks_count': len(asks)
                    })
                
                # 가격역전 현상 확인
                is_inverted, inversion_reason, inversion_percent = check_price_inversion(bids, asks)
                if is_inverted:
                    price_inversion_count += 1
                    symbol_stats[symbol]['inversion'] += 1
                    
                    # 최대 역전 백분율 업데이트
                    if inversion_percent > symbol_stats[symbol]['max_inversion_percent']:
                        symbol_stats[symbol]['max_inversion_percent'] = inversion_percent
                    
                    exchange = json_data.get('exchange', 'unknown')
                    timestamp = json_data.get('ts', 0)
                    
                    time_str = datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S') if timestamp else 'unknown'
                    
                    inversion_details.append({
                        'line': line_num,
                        'exchange': exchange,
                        'symbol': symbol,
                        'time': time_str,
                        'reason': inversion_reason,
                        'highest_bid': float(bids[0][0]) if bids else 0,
                        'lowest_ask': float(asks[0][0]) if asks else 0,
                        'percent': inversion_percent
                    })
                    
            except Exception as e:
                print(f"Error parsing line {line_num}: {str(e)}")
                continue
    
    # 역전 정도가 큰 순서대로 정렬
    inversion_details.sort(key=lambda x: x['percent'], reverse=True)
    
    return total_count, valid_count, invalid_details, price_inversion_count, inversion_details, symbol_stats

def main():
    print("=== \"매수 (10) / 매도 (10)\" 로그만 검증 ===\n")
    
    all_total = 0
    all_valid = 0
    all_inversions = 0
    all_symbol_stats = {}
    
    for exchange in EXCHANGES:
        # 최신 로그 파일 찾기
        latest_log = get_latest_log_for_exchange(exchange)
        
        if not latest_log:
            print(f"[{exchange}] 로그 파일을 찾을 수 없습니다.")
            print()
            continue
            
        print(f"[{exchange}] 분석 파일: {os.path.basename(latest_log)}")
        
        # 로그 파일 분석
        total_count, valid_count, invalid_details, inversion_count, inversion_details, symbol_stats = analyze_processed_logs(latest_log)
        invalid_count = total_count - valid_count
        
        # 전체 통계 업데이트
        all_total += total_count
        all_valid += valid_count
        all_inversions += inversion_count
        
        # 전체 심볼 통계 업데이트
        for symbol, stats in symbol_stats.items():
            if symbol not in all_symbol_stats:
                all_symbol_stats[symbol] = {'total': 0, 'inversion': 0, 'max_inversion_percent': 0.0}
            all_symbol_stats[symbol]['total'] += stats['total']
            all_symbol_stats[symbol]['inversion'] += stats['inversion']
            all_symbol_stats[symbol]['max_inversion_percent'] = max(
                all_symbol_stats[symbol]['max_inversion_percent'], 
                stats['max_inversion_percent']
            )
        
        # 분석 결과 출력
        if total_count == 0:
            print(f"  - \"매수 (10) / 매도 (10)\" 형식의 로그가 없습니다.")
        else:
            # 분석 결과 출력
            print(f"  - 분석 결과:")
            print(f"    * \"매수 (10) / 매도 (10)\" 로그 항목 수: {total_count}개")
            print(f"    * 실제 정상 항목 수 (배열이 각각 10개): {valid_count}개 ({valid_count/total_count*100:.1f}%)")
            print(f"    * 불일치 항목 수: {invalid_count}개 ({invalid_count/total_count*100:.1f}%)")
            print(f"    * 가격역전 항목 수: {inversion_count}개 ({inversion_count/total_count*100:.2f}%)")
            
            # 비정상 항목 샘플 출력
            if invalid_details:
                sample_count = min(5, len(invalid_details))
                print(f"  - 불일치 항목 샘플 ({sample_count}개):")
                for i, item in enumerate(invalid_details[:sample_count], 1):
                    print(f"    * 예시 {i}: 라인 {item['line']}, 거래소={item['exchange']}, 심볼={item['symbol']}, 매수={item['bids_count']}개, 매도={item['asks_count']}개")
            else:
                print(f"  - 모든 \"매수 (10) / 매도 (10)\" 로그의 배열 길이가 정상입니다!")
                
            # 가격역전 항목 샘플 출력
            if inversion_details:
                sample_count = min(5, len(inversion_details))
                print(f"  - 가격역전 항목 샘플 (역전률 높은 순 {sample_count}개):")
                for i, item in enumerate(inversion_details[:sample_count], 1):
                    print(f"    * 예시 {i}: 라인 {item['line']}, 거래소={item['exchange']}, 심볼={item['symbol']}")
                    print(f"      - 사유: {item['reason']}")
                    print(f"      - 매수최고가: {item['highest_bid']}, 매도최저가: {item['lowest_ask']}")
                    print(f"      - 역전률: {item['percent']:.6f}%")
                
                # 심볼별 가격역전 통계 출력
                print(f"  - 심볼별 가격역전 현황:")
                for symbol, stats in sorted(symbol_stats.items(), key=lambda x: x[1]['max_inversion_percent'], reverse=True):
                    if stats['inversion'] > 0:
                        inversion_rate = stats['inversion'] / stats['total'] * 100
                        print(f"    * {symbol}: {stats['inversion']}개 / {stats['total']}개 ({inversion_rate:.2f}%), 최대역전률: {stats['max_inversion_percent']:.6f}%")
            else:
                print(f"  - 가격역전 현상이 발견되지 않았습니다!")
                
        print()
    
    # 전체 결과 요약
    if all_total > 0:
        print("=== 전체 분석 결과 요약 ===")
        print(f"총 \"매수 (10) / 매도 (10)\" 로그 항목: {all_total}개")
        print(f"정상 길이 항목 (배열이 각각 10개): {all_valid}개 ({all_valid/all_total*100:.1f}%)")
        print(f"가격역전 항목: {all_inversions}개 ({all_inversions/all_total*100:.3f}%)")
        
        # 전체 심볼별 가격역전 통계 출력
        if all_inversions > 0:
            print("\n=== 심볼별 가격역전 현황 (전체, 최대역전률 순) ===")
            for symbol, stats in sorted(all_symbol_stats.items(), key=lambda x: x[1]['max_inversion_percent'], reverse=True):
                if stats['inversion'] > 0:
                    inversion_rate = stats['inversion'] / stats['total'] * 100
                    print(f"* {symbol}: {stats['inversion']}개 / {stats['total']}개 ({inversion_rate:.2f}%), 최대역전률: {stats['max_inversion_percent']:.6f}%")
        
        if all_valid == all_total and all_inversions == 0:
            print("\n✅ 모든 로그가 정상적으로 처리되었습니다!")
        else:
            print("\n❌ 비정상 로그가 발견되었습니다!")

if __name__ == "__main__":
    main() 