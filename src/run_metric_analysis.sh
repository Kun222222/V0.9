#!/bin/bash

# 메트릭 분석 프로그램 실행 스크립트

# 현재 디렉토리 경로 저장
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd $SCRIPT_DIR

# 필요한 패키지 설치 여부 확인
echo "필요한 패키지 설치 중..."
pip install -r requirements_metric_analyzer.txt

# 기본 분석 실행
echo "메트릭 데이터 분석 중..."
python metric_analyzer.py --output-dir ../reports

echo "분석이 완료되었습니다. 결과는 'reports' 디렉토리에서 확인할 수 있습니다."
echo "웹 브라우저에서 '../reports/index.html' 파일을 열어 결과를 확인하세요." 