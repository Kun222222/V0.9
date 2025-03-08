import os
import glob
import json
from collections import defaultdict
import re

def get_latest_log_file(log_dir="src/logs"):
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
    
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                # 새로운 로그 형식에 맞는 정규식 패턴
                log_pattern = r'\[.*?\] \[.*?\] 큐 데이터 \[(\w+)\] - exchange: (\w+), data: ({.*})'
                match = re.search(log_pattern, line)
                
                if match:
                    msg_type = match.group(1)    # 메시지 타입 (orderbook_delta 등)
                    exchange = match.group(2)     # exchange 이름
                    json_str = match.group(3)     # JSON 데이터
                    
                    # 작은따옴표를 큰따옴표로 변환
                    json_str = json_str.replace("'", '"')
                    
                    # 키를 큰따옴표로 감싸기
                    json_str = re.sub(r'([{,])\s*(\w+):', r'\1 "\2":', json_str)
                    
                    json_data = json.loads(json_str)
                    
                    # 거래소 카운트 증가 (orderbook_delta 타입만 카운트)
                    if msg_type == "orderbook_delta":
                        exchange_counts[exchange] += 1
                
            except json.JSONDecodeError as e:
                print(f"JSON 파싱 에러: {e}")
                print(f"문제가 발생한 JSON 문자열: {json_str}")
                continue
            except Exception as e:
                print(f"에러 발생: {e}")
                continue
    
    # 결과 출력
    print(f"\n분석 파일: {os.path.basename(log_file)}")
    print("\n각 거래소별 orderbook_delta 데이터 수:")
    print("-" * 40)
    for exchange, count in sorted(exchange_counts.items()):
        print(f"{exchange:<20} : {count:>8,}개")
    print("-" * 40)
    print(f"총 거래소 수         : {len(exchange_counts):>8,}개")
    print(f"총 orderbook 데이터 수: {sum(exchange_counts.values()):>8,}개")

def main():
    latest_log = get_latest_log_file()
    if latest_log:
        analyze_exchange_data(latest_log)

if __name__ == "__main__":
    main() 