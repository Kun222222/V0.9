#!/bin/bash

# 스크립트가 있는 디렉토리의 상위(프로젝트 루트)로 이동
cd "$(dirname "$0")/.." || exit

echo "=== Docker 환경 정리 시작 ==="

# 실행 중인 컨테이너 중지
echo "=== 실행 중인 컨테이너 중지 ==="
docker compose down

# 모든 컨테이너 삭제
echo -e "\n=== 모든 컨테이너 삭제 ==="
docker rm -f $(docker ps -aq) 2>/dev/null || true

# 모든 이미지 삭제
echo -e "\n=== 모든 Docker 이미지 삭제 ==="
docker rmi -f $(docker images -q) 2>/dev/null || true

# Docker 시스템 정리
echo -e "\n=== Docker 시스템 정리 ==="
docker system prune -f

echo -e "\n=== 정리 완료 ===" 