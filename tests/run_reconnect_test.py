#!/usr/bin/env python
"""
웹소켓 재연결 및 재구독 테스트 실행 스크립트
"""

import asyncio
import sys
import os

# 모듈 경로 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tests.test_websocket_reconnect import test_main

if __name__ == "__main__":
    print("웹소켓 연결 끊김 후 자동 재구독 테스트 시작...")
    asyncio.run(test_main())
    print("테스트 완료!") 