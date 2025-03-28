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
    # 파일명 패턴 생성 (upbit는 "upbit_raw_logger.log"로 끝나는 파일)
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

def analyze_log_file(file_path, exchange_name):
    """
    로그 파일 분석
    
    Args:
        file_path: 분석할 로그 파일 경로
        exchange_name: 거래소 이름
    
    Returns:
        total_count: 총 항목 수
        normal_count: 정상 항목 수 (bids/asks 길이가 10인 것)
        abnormal_details: 비정상 항목 상세 내역
    """
    if not file_path or not os.path.exists(file_path):
        return 0, 0, []
    
    total_count = 0
    normal_count = 0
    abnormal_details = []
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line_num, line in enumerate(f, 1):
            try:
                # 오더북 데이터 라인인지 확인 ("매수" 및 "매도" 문자열 포함)
                if "매수" not in line and "매도" not in line:
                    # 업비트와 빗썸은 다른 형식으로 체크 (Python bytes 표현일 수 있음)
                    if exchange_name in ["upbit", "bithumb"]:
                        if not ('"orderbook_units"' in line or "'orderbook_units'" in line):
                            continue
                    else:
                        continue
                
                # JSON 데이터 찾기
                match = re.search(r'\{.*\}', line)
                if not match:
                    continue
                
                # JSON 텍스트 추출
                json_text = match.group(0)
                
                # bytes 리터럴인 경우 변환
                if json_text.startswith("b'") or json_text.startswith('b"'):
                    json_text = json_text[2:-1].replace('\\', '')
                
                # JSON 파싱
                try:
                    json_data = json.loads(json_text)
                except json.JSONDecodeError:
                    continue
                
                # 'bids'와 'asks' 필드 또는 'orderbook_units' 필드 확인
                bids = []
                asks = []
                
                if 'bids' in json_data and 'asks' in json_data:
                    # 바이낸스, 바이빗 스타일
                    bids = json_data.get('bids', [])
                    asks = json_data.get('asks', [])
                elif 'orderbook_units' in json_data:
                    # 업비트, 빗썸 스타일
                    units = json_data.get('orderbook_units', [])
                    bids = [[unit.get('bid_price', 0), unit.get('bid_size', 0)] for unit in units if 'bid_price' in unit and 'bid_size' in unit]
                    asks = [[unit.get('ask_price', 0), unit.get('ask_size', 0)] for unit in units if 'ask_price' in unit and 'ask_size' in unit]
                else:
                    continue  # 오더북 데이터가 아님
                
                # 유효한 오더북 데이터인지 확인
                if not bids and not asks:
                    continue
                
                total_count += 1
                
                # 정상 항목 카운트 (bids와 asks 길이가 모두 10인 경우)
                if len(bids) == 10 and len(asks) == 10:
                    normal_count += 1
                else:
                    # 비정상 항목 상세 정보 저장
                    symbol = json_data.get('symbol', 'unknown')
                    if not symbol or symbol == 'unknown':
                        # 업비트/빗썸의 경우 'code' 필드에서 심볼 정보 추출
                        code = json_data.get('code', '')
                        if code:
                            if '-' in code:  # 예: KRW-BTC
                                symbol = code.split('-')[1].lower()
                            elif '_' in code:  # 예: BTC_KRW
                                symbol = code.split('_')[0].lower()
                    
                    # 타임스탬프 추출
                    timestamp = json_data.get('ts', 0)
                    if not timestamp:
                        timestamp = json_data.get('timestamp', 0)
                    
                    time_str = datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S') if timestamp else 'unknown'
                    
                    abnormal_details.append({
                        'line': line_num,
                        'symbol': symbol,
                        'time': time_str,
                        'bids_count': len(bids),
                        'asks_count': len(asks)
                    })
                    
            except Exception as e:
                # 디버그용: 특정 오류 발생 시 로그 라인 출력
                #print(f"Error parsing line {line_num} in {os.path.basename(file_path)}: {str(e)}")
                continue
    
    return total_count, normal_count, abnormal_details

def main():
    print("=== 실제 로그 파일 분석 (오더북 스냅샷만) ===\n")
    
    all_total = 0
    all_normal = 0
    all_abnormal = 0
    
    for exchange in EXCHANGES:
        # 최신 로그 파일 찾기
        latest_log = get_latest_log_for_exchange(exchange)
        
        if not latest_log:
            print(f"[{exchange}] 로그 파일을 찾을 수 없습니다.")
            print()
            continue
            
        print(f"[{exchange}] 분석 파일: {os.path.basename(latest_log)}")
        
        # 로그 파일 분석
        total_count, normal_count, abnormal_details = analyze_log_file(latest_log, exchange)
        abnormal_count = total_count - normal_count
        
        # 전체 통계 업데이트
        all_total += total_count
        all_normal += normal_count
        all_abnormal += abnormal_count
        
        # 분석 결과 출력
        if total_count == 0:
            print(f"  - 분석할 오더북 데이터가 없습니다.")
        else:
            print(f"  - 분석 결과:")
            print(f"    * 총 오더북 항목 수: {total_count}개")
            print(f"    * 정상 항목 수 (bids/asks 길이가 10): {normal_count}개 ({normal_count/total_count*100:.1f}%)")
            print(f"    * 비정상 항목 수: {abnormal_count}개 ({abnormal_count/total_count*100:.1f}%)")
            
            # 비정상 항목 샘플 출력
            if abnormal_details:
                sample_count = min(5, len(abnormal_details))
                print(f"  - 비정상 항목 샘플 ({sample_count}개):")
                for i, item in enumerate(abnormal_details[:sample_count], 1):
                    print(f"    * 예시 {i}: 라인 {item['line']}, 심볼={item['symbol']}, 매수={item['bids_count']}개, 매도={item['asks_count']}개")
            else:
                print(f"  - 모든 항목이 정상입니다.")
                
        print()
    
    # 전체 결과 요약
    if all_total > 0:
        print("=== 전체 분석 결과 요약 ===")
        print(f"총 분석 항목: {all_total}개")
        print(f"정상 항목 (bids/asks 길이가 10): {all_normal}개 ({all_normal/all_total*100:.1f}%)")
        print(f"비정상 항목: {all_abnormal}개 ({all_abnormal/all_total*100:.1f}%)")

if __name__ == "__main__":
    main() 