#!/bin/bash

# 스크립트가 있는 디렉토리의 상위(프로젝트 루트)로 이동
cd "$(dirname "$0")/.." || exit

echo "=== Docker 이미지 빌드 시작 ==="

# 이미지 빌드
docker compose build

# 사용하지 않는 이미지 정리
echo -e "\n=== 사용하지 않는 이미지 정리 ==="
docker image prune -f

# 현재 이미지 상태 출력
echo -e "\n=== 현재 Docker 이미지 상태 ==="
docker images | grep v06-app

echo -e "\n=== 빌드 완료 ==="

# 컨테이너 bash 터미널 실행
echo -e "\n=== 컨테이너 bash 터미널 실행 ==="
docker compose run --rm app bash 