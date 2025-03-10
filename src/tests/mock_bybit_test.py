#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
바이빗 웹소켓 연결 테스트 스크립트 (모의 테스트)
"""

import asyncio
import json
import logging
import sys
import time
import websockets

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("bybit_test")

# 메시지 카운터
message_counter = 0
last_pong_time = 0

async def message_handler(message):
    """웹소켓 메시지 처리 함수"""
    global message_counter, last_pong_time
    
    try:
        # JSON 파싱 시도
        data = json.loads(message)
        message_counter += 1
        
        # 메시지 타입에 따라 다르게 처리
        if "op" in data:
            if data.get("op") == "pong" or (data.get("ret_msg") == "pong" and data.get("op") == "ping"):
                last_pong_time = time.time()
                logger.info(f"PONG 메시지 수신: {json.dumps(data, indent=2, ensure_ascii=False)}")
            elif data.get("op") == "subscribe":
                logger.info(f"구독 응답 수신: {json.dumps(data, indent=2, ensure_ascii=False)}")
            else:
                logger.info(f"기타 op 메시지 수신: {json.dumps(data, indent=2, ensure_ascii=False)}")
        elif "topic" in data and "data" in data:
            # 오더북 데이터는 간략하게 로깅
            topic = data.get("topic", "")
            logger.info(f"오더북 데이터 수신: {topic}")
            if message_counter % 10 == 0:  # 10개마다 한 번씩만 상세 출력
                logger.debug(f"오더북 상세 데이터: {json.dumps(data, indent=2, ensure_ascii=False)}")
        else:
            logger.info(f"기타 메시지 수신: {json.dumps(data, indent=2, ensure_ascii=False)}")
    except json.JSONDecodeError:
        logger.warning(f"JSON 파싱 실패: {message[:100]}...")

async def send_ping(websocket):
    """Ping 메시지 전송"""
    ping_message = {
        "req_id": str(int(time.time() * 1000)),
        "op": "ping"
    }
    await websocket.send(json.dumps(ping_message))
    logger.info(f"PING 전송: {json.dumps(ping_message, indent=2)}")
    return time.time()

async def test_bybit_connection():
    """바이빗 웹소켓 연결 테스트"""
    # 바이빗 스팟 웹소켓 URL
    ws_url = "wss://stream.bybit.com/v5/public/spot"
    
    logger.info(f"바이빗 웹소켓 연결 시도: {ws_url}")
    
    try:
        async with websockets.connect(ws_url) as websocket:
            logger.info("바이빗 웹소켓 연결 성공")
            
            # 구독할 심볼 목록
            symbols = ["BTCUSDT", "ETHUSDT"]
            
            # 구독 메시지 생성
            subscribe_message = {
                "op": "subscribe",
                "args": [f"orderbook.1.{symbol}" for symbol in symbols]
            }
            
            # 구독 메시지 전송
            logger.info(f"구독 메시지 전송: {json.dumps(subscribe_message, indent=2)}")
            await websocket.send(json.dumps(subscribe_message))
            
            # 3분(180초) 동안 실행
            start_time = time.time()
            last_ping_time = 0
            ping_interval = 20  # 20초마다 ping 전송
            
            logger.info("3분(180초) 동안 테스트 실행...")
            
            while time.time() - start_time < 180:
                try:
                    # 핑 전송 (20초마다)
                    current_time = time.time()
                    if current_time - last_ping_time > ping_interval:
                        last_ping_time = await send_ping(websocket)
                    
                    # 메시지 수신 (1초 타임아웃)
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=1)
                        await message_handler(message)
                    except asyncio.TimeoutError:
                        # 타임아웃은 정상적인 상황
                        pass
                    
                    # 10초마다 상태 출력
                    elapsed = int(current_time - start_time)
                    if elapsed % 10 == 0 and elapsed > 0 and int(current_time) % 10 == 0:
                        logger.info(f"테스트 진행 중... {elapsed}초 경과")
                        logger.info(f"총 수신 메시지 수: {message_counter}")
                        
                        # 마지막 PONG 시간 확인
                        if last_pong_time > 0:
                            pong_diff = current_time - last_pong_time
                            logger.info(f"마지막 PONG 수신 후 경과 시간: {pong_diff:.2f}초")
                    
                    # 잠시 대기
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"메시지 처리 중 오류 발생: {str(e)}")
            
            logger.info("테스트 완료: 최종 통계")
            logger.info(f"총 수신 메시지 수: {message_counter}")
            
    except Exception as e:
        logger.error(f"웹소켓 연결 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    try:
        # 테스트 실행
        asyncio.run(test_bybit_connection())
    except KeyboardInterrupt:
        logger.info("사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        logger.error(f"테스트 실행 중 오류 발생: {str(e)}") 