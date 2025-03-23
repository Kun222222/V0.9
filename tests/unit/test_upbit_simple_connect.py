#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
업비트 웹소켓 직접 연결 테스트 스크립트

이 스크립트는 업비트 웹소켓에 직접 연결하여 
기반 프레임워크나 이벤트 핸들러 없이 연결 가능성을 테스트합니다.
"""

import asyncio
import json
import logging
import websockets
import time
from typing import Dict, Any, List

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('upbit_simple_connect')

# 업비트 웹소켓 URL
UPBIT_WS_URL = "wss://api.upbit.com/websocket/v1"

async def simple_connect():
    """
    업비트 웹소켓에 직접 연결 테스트
    """
    logger.info("===== 업비트 웹소켓 직접 연결 테스트 =====")
    logger.info(f"연결 URL: {UPBIT_WS_URL}")
    
    try:
        # 웹소켓 연결
        logger.info("웹소켓 연결 시도...")
        async with websockets.connect(UPBIT_WS_URL, ping_interval=30) as websocket:
            logger.info("✅ 웹소켓 연결 성공!")
            
            # 구독 메시지 생성
            symbols = ["BTC", "ETH"]
            market_codes = [f"KRW-{s.upper()}" for s in symbols]
            
            ticket_name = f"simple_test_{int(time.time() * 1000)}"
            subscribe_message = [
                {"ticket": ticket_name},
                {
                    "type": "orderbook",
                    "codes": market_codes,
                    "is_only_realtime": False,
                },
                {"format": "DEFAULT"}
            ]
            
            # 구독 메시지 전송
            logger.info(f"구독 메시지 전송: {symbols}")
            await websocket.send(json.dumps(subscribe_message))
            
            # 응답 수신 (최대 3개까지만)
            logger.info("메시지 수신 대기 중...")
            for i in range(3):
                response = await websocket.recv()
                
                # 응답 처리
                if isinstance(response, bytes):
                    response = response.decode('utf-8')
                    
                try:
                    data = json.loads(response)
                    logger.info(f"메시지 #{i+1} 수신: {json.dumps(data, indent=2)[:200]}...")
                except:
                    logger.info(f"메시지 #{i+1} 수신: {response[:200]}...")
            
            logger.info("웹소켓 연결 종료")
            return True
    
    except Exception as e:
        logger.error(f"❌ 웹소켓 연결/통신 중 오류: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def main():
    """메인 함수"""
    success = await simple_connect()
    if success:
        logger.info("🎉 업비트 웹소켓 직접 연결 테스트 성공!")
        return 0
    else:
        logger.error("❌ 업비트 웹소켓 직접 연결 테스트 실패!")
        return 1

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    exit_code = loop.run_until_complete(main())
    loop.close() 