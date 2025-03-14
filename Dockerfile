FROM python:3.11-slim

WORKDIR /app

# 시스템 패키지 설치 및 정리
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# 소스 코드 복사
COPY . .

# Python 패키지 설치 및 캐시 정리
RUN pip install --no-cache-dir . \
    && rm -rf ~/.cache/pip/*

# 디버깅: 현재 디렉토리 내용 확인
RUN ls -la && \
    echo "=== scripts directory ===" && \
    ls -la scripts/

# 스크립트 복사 및 권한 설정
RUN mkdir -p /usr/local/bin && \
    cp scripts/run_orderbook.sh /usr/local/bin/ && \
    chmod +x /usr/local/bin/run_orderbook.sh && \
    echo "=== copied script ===" && \
    ls -l /usr/local/bin/run_orderbook.sh

# 기본 명령어를 스크립트 실행으로 변경
CMD ["/usr/local/bin/run_orderbook.sh"] 