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
        content = f.read()
        # 로그 라인을 정규식으로 찾기
        log_pattern = r'큐 데이터 - ({.*?})'
        matches = re.finditer(log_pattern, content, re.DOTALL)
        
        for match in matches:
            try:
                json_str = match.group(1)
                # 줄바꿈과 공백 제거
                json_str = json_str.replace('\n', '').replace('\r', '').strip()
                # 작은따옴표를 큰따옴표로 변환
                json_str = json_str.replace("'", '"')
                json_data = json.loads(json_str)
                
                # 거래소 이름 추출 및 카운트 증가
                exchange = json_data.get('exchangename')
                if exchange:
                    exchange_counts[exchange] += 1
            except json.JSONDecodeError as e:
                print(f"JSON 파싱 에러: {e}")
                continue
            except Exception as e:
                print(f"에러 발생: {e}")
                continue
    
    # 결과 출력
    print(f"\n분석 파일: {os.path.basename(log_file)}")
    print("\n각 거래소별 데이터 수:")
    print("-" * 30)
    for exchange, count in sorted(exchange_counts.items()):
        print(f"{exchange:<15} : {count:>5}개")
    print("-" * 30)
    print(f"총 거래소 수    : {len(exchange_counts):>5}개")
    print(f"총 데이터 수    : {sum(exchange_counts.values()):>5}개")

def main():
    latest_log = get_latest_log_file()
    if latest_log:
        analyze_exchange_data(latest_log)

if __name__ == "__main__":
    main() 