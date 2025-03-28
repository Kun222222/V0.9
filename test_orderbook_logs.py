#!/usr/bin/env python3
import os
import json
import random
import re
from datetime import datetime

# 테스트 로그 디렉터리
TEST_LOG_DIR = "test_logs"

# 거래소 목록
EXCHANGES = [
    "binance_future", "binance_spot", "bybit", 
    "bithumb", "bybit_future", "upbit"
]

def create_test_log_file(exchange_name, entry_count=100):
    """
    테스트 로그 파일 생성
    
    Args:
        exchange_name: 거래소 이름
        entry_count: 생성할 로그 항목 수
    
    Returns:
        file_path: 생성된 파일 경로
        normal_count: 정상 항목 수 (bids/asks 길이가 10인 것)
        abnormal_count: 비정상 항목 수 (bids/asks 길이가 10이 아닌 것)
    """
    # 현재 날짜/시간 기반 파일명 생성
    now = datetime.now()
    date_str = now.strftime("%y%m%d_%H%M%S")
    file_name = f"{date_str}_{exchange_name}_raw_logger.log"
    file_path = os.path.join(TEST_LOG_DIR, file_name)
    
    # 비정상 항목 수 계산 (약 20%를 비정상으로 설정)
    abnormal_count = int(entry_count * 0.2)
    normal_count = entry_count - abnormal_count
    
    # 심볼 목록
    symbols = ["btc", "eth", "sol", "ada", "doge", "xrp", "dot", "avax"]
    
    # 로그 파일 생성
    with open(file_path, 'w', encoding='utf-8') as f:
        for i in range(entry_count):
            # 현재 항목이 정상인지 비정상인지 결정
            is_normal = i >= abnormal_count
            
            # 타임스탬프
            timestamp = int(now.timestamp() * 1000) + i * 100
            time_str = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            
            # 심볼 선택
            symbol = random.choice(symbols)
            
            # 호가 수 결정 (정상: 10, 비정상: 1~9 또는 11~15)
            bid_count = 10 if is_normal else random.choice([random.randint(1, 9), random.randint(11, 15)])
            ask_count = 10 if is_normal else random.choice([random.randint(1, 9), random.randint(11, 15)])
            
            # 호가 데이터 생성
            bids = [[random.uniform(10000, 70000), random.uniform(0.1, 2.0)] for _ in range(bid_count)]
            asks = [[random.uniform(10000, 70000), random.uniform(0.1, 2.0)] for _ in range(ask_count)]
            
            # JSON 데이터 생성
            data = {
                "exchange": exchange_name,
                "symbol": symbol,
                "ts": timestamp,
                "seq": i + 1000,
                "bids": bids,
                "asks": asks
            }
            
            # 로그 라인 작성
            log_line = f"[{time_str}] 매수 ({len(bids)}) / 매도 ({len(asks)}) {json.dumps(data)}\n"
            f.write(log_line)
    
    return file_path, normal_count, abnormal_count

def analyze_log_file(file_path):
    """
    로그 파일 분석
    
    Args:
        file_path: 분석할 로그 파일 경로
    
    Returns:
        total_count: 총 항목 수
        normal_count: 정상 항목 수 (bids/asks 길이가 10인 것)
        abnormal_details: 비정상 항목 상세 내역
    """
    if not os.path.exists(file_path):
        return 0, 0, []
    
    total_count = 0
    normal_count = 0
    abnormal_details = []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                total_count += 1
                
                # JSON 데이터 찾기
                match = re.search(r'\{.*\}', line)
                if not match:
                    continue
                
                # JSON 파싱
                json_data = json.loads(match.group(0))
                
                # bids와 asks 길이 확인
                bids = json_data.get('bids', [])
                asks = json_data.get('asks', [])
                
                # 정상 항목 카운트
                if len(bids) == 10 and len(asks) == 10:
                    normal_count += 1
                else:
                    # 비정상 항목 상세 정보 저장
                    symbol = json_data.get('symbol', 'unknown')
                    timestamp = json_data.get('ts', 0)
                    time_str = datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S') if timestamp else 'unknown'
                    
                    abnormal_details.append({
                        'line': line_num,
                        'symbol': symbol,
                        'time': time_str,
                        'bids_count': len(bids),
                        'asks_count': len(asks)
                    })
                    
            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"Error parsing line {line_num}: {str(e)}")
    
    return total_count, normal_count, abnormal_details

def main():
    # 테스트 디렉터리 생성
    os.makedirs(TEST_LOG_DIR, exist_ok=True)
    
    print("=== 테스트 로그 파일 생성 및 분석 ===\n")
    
    for exchange in EXCHANGES:
        # 테스트 로그 파일 생성
        print(f"[{exchange}] 테스트 로그 파일 생성 중...")
        file_path, expected_normal, expected_abnormal = create_test_log_file(exchange)
        
        print(f"  - 파일 생성 완료: {os.path.basename(file_path)}")
        print(f"  - 예상 정상 항목: {expected_normal}, 예상 비정상 항목: {expected_abnormal}")
        
        # 생성된 파일 분석
        print(f"  - 로그 파일 분석 중...")
        total_count, normal_count, abnormal_details = analyze_log_file(file_path)
        abnormal_count = total_count - normal_count
        
        # 분석 결과 출력
        print(f"  - 분석 결과:")
        print(f"    * 총 항목 수: {total_count}")
        print(f"    * 정상 항목 수 (bids/asks 길이가 10): {normal_count} ({normal_count/total_count*100:.1f}%)")
        print(f"    * 비정상 항목 수: {abnormal_count} ({abnormal_count/total_count*100:.1f}%)")
        
        # 예상 결과와 비교
        if expected_normal == normal_count and expected_abnormal == abnormal_count:
            print(f"  - 검증: 성공 ✅ (예상값과 일치)")
        else:
            print(f"  - 검증: 실패 ❌ (예상: 정상={expected_normal}, 비정상={expected_abnormal})")
        
        # 비정상 항목 샘플 출력
        if abnormal_details:
            sample_count = min(3, len(abnormal_details))
            print(f"  - 비정상 항목 샘플 ({sample_count}개):")
            for i, item in enumerate(abnormal_details[:sample_count], 1):
                print(f"    * 예시 {i}: 라인 {item['line']}, 심볼={item['symbol']}, 매수={item['bids_count']}, 매도={item['asks_count']}")
        
        print()

if __name__ == "__main__":
    main() 