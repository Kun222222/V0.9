#!/usr/bin/env python
"""
오더북 수집기 실행 스크립트
"""

import asyncio
import signal
import sys
from crosskimp.ob_collector.obcollector import OrderbookCollectorManager

# 종료 이벤트 생성
stop_event = asyncio.Event()

def signal_handler(sig, frame):
    """시그널 핸들러 (Ctrl+C)"""
    print("종료 신호를 받았습니다. 프로그램을 종료합니다...")
    stop_event.set()

async def main():
    """메인 함수"""
    # 오더북 수집기 관리자 생성
    manager = OrderbookCollectorManager()
    
    # 초기화
    if not await manager.initialize():
        print("오더북 수집기 초기화 실패")
        return
    
    # 시작
    if not await manager.start():
        print("오더북 수집기 시작 실패")
        return
    
    print("오더북 수집기가 성공적으로 시작되었습니다. 종료하려면 Ctrl+C를 누르세요.")
    
    try:
        # 종료 이벤트 대기
        await stop_event.wait()
    finally:
        # 종료 처리
        print("오더북 수집기를 종료합니다...")
        await manager.stop()
        print("오더북 수집기 종료 완료")

if __name__ == "__main__":
    # 시그널 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 메인 함수 실행
    asyncio.run(main())