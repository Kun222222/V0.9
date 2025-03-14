import os
import glob
import json
from collections import defaultdict
import re

def get_latest_log_file(log_dir="src/logs/queue"):
    # 로그 파일 패턴에 맞는 모든 파일 찾기
    log_files = glob.glob(os.path.join(log_dir, "*_queue_logger.log"))
    
    if not log_files:
        print("로그 파일을 찾을 수 없습니다.")
        return None
    
    # 파일 수정 시간을 기준으로 가장 최신 파일 찾기
    latest_file = max(log_files, key=os.path.getmtime)
    return latest_file

def analyze_exchange_data(log_file):
    if not log_file or not os.path.exists(log_file):
        print("유효한 로그 파일이 아닙니다.")
        return
    
    exchange_counts = defaultdict(int)
    processed_lines = 0
    error_lines = 0
    
    with open(log_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                # 기본 로그 패턴: 2025-03-10 06:48:37 - base_ws_manager.py  :220 / INFO    - bybitfuture {'exchangename': 'bybitfuture', ...}
                base_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} - .*? / INFO\s+- (\w+) ({.*})'
                match = re.search(base_pattern, line)
                
                if match:
                    exchange = match.group(1)     # exchange 이름
                    json_str = match.group(2)     # JSON 데이터
                    
                    # 작은따옴표를 큰따옴표로 변환
                    json_str = json_str.replace("'", '"')
                    
                    # 키를 큰따옴표로 감싸기
                    json_str = re.sub(r'([{,])\s*(\w+):', r'\1 "\2":', json_str)
                    
                    try:
                        json_data = json.loads(json_str)
                        
                        # 일반적인 거래소 데이터 형식 (bids와 asks가 직접 있는 경우)
                        if 'bids' in json_data and 'asks' in json_data:
                            exchange_counts[exchange] += 1
                            processed_lines += 1
                        # binance 특수 형식 (data 내부에 bids와 asks가 있는 경우)
                        elif 'data' in json_data and isinstance(json_data['data'], dict) and 'bids' in json_data['data'] and 'asks' in json_data['data']:
                            exchange_counts[exchange] += 1
                            processed_lines += 1
                    except json.JSONDecodeError:
                        # JSON 파싱 실패 시 다른 방법 시도
                        error_lines += 1
                        continue
                
                # bybit_s_ob.py 로그 형식 처리 (다른 형식)
                # 예: 2025-03-10 06:48:37 - bybit_s_ob.py       :217 / INFO    - bybit|ANIME|매수호가=0.01692|...
                elif "bybit_s_ob.py" in line and "|매수호가=" in line and "|매도호가=" in line:
                    bybit_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} - bybit_s_ob\.py.*? / INFO\s+- (\w+)\|'
                    match = re.search(bybit_pattern, line)
                    if match:
                        exchange = match.group(1)
                        exchange_counts[exchange] += 1
                        processed_lines += 1
                
                # 다른 거래소 로그 형식이 있다면 여기에 추가
                
            except Exception as e:
                error_lines += 1
                print(f"에러 발생 (라인 {line_num}): {e}")
                print(f"문제가 발생한 라인: {line[:100]}...")
                continue
    
    # 결과 출력
    print(f"\n분석 파일: {os.path.basename(log_file)}")
    print(f"총 처리된 라인 수: {processed_lines:,}개")
    print(f"에러 발생 라인 수: {error_lines:,}개")
    print("\n각 거래소별 orderbook 데이터 수:")
    print("-" * 40)
    
    # 거래소 이름 매핑 (필요한 경우)
    exchange_names = {
        "bybitfuture": "Bybit Future",
        "bybit": "Bybit",
        "upbit": "Upbit",
        "binance": "Binance",
        "binancefuture": "Binance Future",
        "bithumb": "Bithumb"
    }
    
    for exchange, count in sorted(exchange_counts.items(), key=lambda x: x[1], reverse=True):
        display_name = exchange_names.get(exchange, exchange)
        print(f"{display_name:<20} : {count:>8,}개")
    
    print("-" * 40)
    print(f"총 거래소 수         : {len(exchange_counts):>8,}개")
    print(f"총 orderbook 데이터 수: {sum(exchange_counts.values()):>8,}개")

def main():
    latest_log = get_latest_log_file()
    if latest_log:
        print(f"최신 로그 파일: {latest_log}")
        analyze_exchange_data(latest_log)
    else:
        print("로그 파일을 찾을 수 없습니다.")

if __name__ == "__main__":
    main() 