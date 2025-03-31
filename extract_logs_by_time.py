#!/usr/bin/env python3
import os
import re
import glob
from datetime import datetime

# 원본 로그 폴더 경로
log_dir = "/Users/kun/Desktop/CrossKimpArbitrage/v0.6/src/logs/raw_data/"

# 결과를 저장할 폴더 경로
output_dir = "/Users/kun/Desktop/CrossKimpArbitrage/v0.6/src/logs/extracted_10am_30min/"

# 결과 폴더가 없으면 생성
os.makedirs(output_dir, exist_ok=True)

# 로그 파일 패턴 정의
log_pattern = os.path.join(log_dir, "*.log*")

# 시간 패턴 정의 (10시 전체)
time_pattern = re.compile(r'(\d{4}-\d{2}-\d{2} 10:(\d{2}):\d{2},\d{3})')

def process_log_file(log_file):
    # 출력 파일 이름 생성
    base_name = os.path.basename(log_file)
    output_file = os.path.join(output_dir, f"10am_30min_{base_name}")
    
    # 이미 존재하는 파일이면 건너뛰기
    if os.path.exists(output_file):
        print(f"파일이 이미 존재합니다: {output_file}")
        return
    
    print(f"처리 중: {log_file}")
    
    try:
        # 파일이 너무 크면 라인별로 읽기
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f_in:
            with open(output_file, 'w', encoding='utf-8') as f_out:
                for line in f_in:
                    match = time_pattern.search(line)
                    if match:
                        # 시간이 10:00~10:30 사이인지 확인
                        minute = int(match.group(2))
                        if 0 <= minute <= 30:
                            f_out.write(line)
        
        # 파일이 비어있으면 삭제
        if os.path.getsize(output_file) == 0:
            os.remove(output_file)
            print(f"빈 파일 삭제: {output_file}")
        else:
            print(f"저장 완료: {output_file}")
    
    except Exception as e:
        print(f"오류 발생: {log_file} - {str(e)}")

# 모든 로그 파일 처리
log_files = glob.glob(log_pattern)
print(f"총 {len(log_files)}개 파일 발견")

for log_file in log_files:
    process_log_file(log_file)

print("모든 파일 처리 완료") 