#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
바이빗 웹소켓 커넥터 테스트 스크립트
- 3분간 연결 유지 테스트
- 재연결 테스트 포함
"""

import asyncio
import json
import logging
import sys
import time
from typing import Dict, Any

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("bybit_test")

# cnt_bybit.py 파일 직접 import
try:
    from cnt_bybit import BybitConnector
    logger.info("BybitConnector 클래스를 성공적으로 import 했습니다.")
except ImportError as e:
    logger.error(f"BybitConnector 클래스를 import할 수 없습니다: {str(e)}")
    sys.exit(1)

# 메시지 카운터
message_counter = 0
last_pong_time = 0

# 메시지 핸들러 함수
async def message_handler(message: str) -> None:
    """
    웹소켓으로부터 수신한 메시지를 처리하는 함수
    
    Args:
        message: 수신한 메시지
    """
    global message_counter, last_pong_time
    
    try:
        # JSON 파싱 시도
        data = json.loads(message)
        message_counter += 1
        
        # 메시지 타입에 따라 다르게 처리
        if "op" in data:
            if data.get("op") == "pong":
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

# 연결 상태 콜백 함수
def connection_status_callback(exchange: str, status: str, details: Dict[str, Any] = None) -> None:
    """
    연결 상태 변경 시 호출되는 콜백 함수
    
    Args:
        exchange: 거래소 이름
        status: 상태 메시지
        details: 추가 상세 정보
    """
    logger.info(f"연결 상태 변경: {exchange} - {status}")
    if details:
        logger.info(f"상세 정보: {json.dumps(details, indent=2, ensure_ascii=False)}")

async def print_connection_status(connector):
    """현재 연결 상태 출력"""
    logger.info(f"현재 상태: {connector.status['connection_state']}")
    logger.info(f"메시지 수: {connector.metrics['message_count']}")
    logger.info(f"오류 수: {connector.metrics['error_count']}")
    logger.info(f"총 수신 메시지 수: {message_counter}")
    
    # 마지막 메시지 시간 확인
    last_msg_time = connector.metrics.get("last_message_time", 0)
    if last_msg_time > 0:
        time_diff = time.time() - last_msg_time
        logger.info(f"마지막 메시지 수신 후 경과 시간: {time_diff:.2f}초")
    
    # 마지막 PONG 시간 확인
    if last_pong_time > 0:
        pong_diff = time.time() - last_pong_time
        logger.info(f"마지막 PONG 수신 후 경과 시간: {pong_diff:.2f}초")

async def test_bybit_connection():
    """바이빗 웹소켓 연결 테스트 - 3분간 연결 유지 및 재연결 테스트"""
    # 커넥터 인스턴스 생성
    connector = BybitConnector()
    
    # 연결 상태 콜백 설정
    connector.set_connection_status_callback(connection_status_callback)
    
    # 연결 시도
    logger.info("바이빗 웹소켓 연결 시도...")
    connected = await connector.connect()
    
    if not connected:
        logger.error("바이빗 웹소켓 연결 실패")
        return
    
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
    await connector.ws.send(json.dumps(subscribe_message))
    
    # 메시지 리스닝 시작
    logger.info("메시지 리스닝 시작...")
    listen_task = asyncio.create_task(connector.listen(message_handler))
    
    # 3분(180초) 동안 실행 후 종료
    try:
        logger.info("3분(180초) 동안 테스트 실행...")
        
        # 처음 1분은 정상 연결 테스트
        for i in range(6):
            await asyncio.sleep(10)
            logger.info(f"테스트 진행 중... {(i+1)*10}초 경과")
            await print_connection_status(connector)
            
            # 30초 경과 시점에 수동으로 ping 전송
            if i == 2:
                logger.info("수동 PING 전송 테스트...")
                await connector.send_ping()
        
        # 1분 경과 후 강제 재연결 테스트
        logger.info("1분 경과: 강제 재연결 테스트 시작...")
        await connector.reconnect()
        
        # 재연결 후 5초 대기
        await asyncio.sleep(5)
        logger.info("재연결 후 상태 확인:")
        await print_connection_status(connector)
        
        # 재구독 메시지 전송
        logger.info("재구독 메시지 전송...")
        await connector.ws.send(json.dumps(subscribe_message))
        
        # 나머지 시간 동안 모니터링
        for i in range(11):
            await asyncio.sleep(10)
            logger.info(f"재연결 후 테스트 진행 중... {(i+1)*10}초 경과")
            await print_connection_status(connector)
            
            # 재연결 후 30초 경과 시점에 수동으로 ping 전송
            if i == 2:
                logger.info("재연결 후 수동 PING 전송 테스트...")
                await connector.send_ping()
    
    except asyncio.CancelledError:
        logger.info("테스트가 취소되었습니다.")
    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {str(e)}")
    finally:
        # 리스닝 태스크 취소
        if not listen_task.done():
            listen_task.cancel()
            try:
                await listen_task
            except asyncio.CancelledError:
                pass
        
        # 연결 종료
        logger.info("연결 종료 중...")
        await connector.disconnect()
        logger.info("연결 종료됨")
        
        # 최종 통계 출력
        logger.info("테스트 완료: 최종 통계")
        logger.info(f"총 수신 메시지 수: {message_counter}")
        logger.info(f"메트릭스 메시지 수: {connector.metrics['message_count']}")
        logger.info(f"오류 수: {connector.metrics['error_count']}")

if __name__ == "__main__":
    try:
        # 테스트 실행
        asyncio.run(test_bybit_connection())
    except KeyboardInterrupt:
        logger.info("사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        logger.error(f"테스트 실행 중 오류 발생: {str(e)}") 