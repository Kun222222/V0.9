#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
빗썸 웹소켓을 통한 오더북 스냅샷 수신 테스트

이 스크립트는 빗썸 웹소켓 API를 사용하여 오더북 스냅샷을 요청하고
응답을 출력하는 기본적인 테스트 코드입니다.
"""

import asyncio
import json
import logging
import sys
import websockets
from datetime import datetime

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("bithumb-test")

# 빗썸 웹소켓 URL
WS_URL = "wss://ws-api.bithumb.com/websocket/v1"

async def test_orderbook_snapshot():
    """
    빗썸 웹소켓을 통한 오더북 스냅샷 요청 및 수신 테스트
    """
    logger.info("빗썸 웹소켓 스냅샷 테스트 시작")
    
    try:
        # 웹소켓 연결
        logger.info(f"웹소켓 연결 시도: {WS_URL}")
        async with websockets.connect(WS_URL) as websocket:
            logger.info("웹소켓 연결 성공")
            
            # 테스트할 심볼 (BTC)
            symbol = "KRW-BTC"
            
            # 오더북 스냅샷 요청 메시지 생성
            request_message = [
                {
                    "ticket": "test-snapshot-1"
                },
                {
                    "type": "orderbook",
                    "codes": [symbol]
                },
                {
                    "format": "DEFAULT"
                }
            ]
            
            # 요청 메시지 송신
            logger.info(f"요청 메시지: {json.dumps(request_message)}")
            await websocket.send(json.dumps(request_message))
            
            # 응답 대기 및 처리 (여러 메시지를 수신할 수 있음)
            for _ in range(5):  # 최대 5개 메시지 수신
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    logger.info("응답 수신")
                    
                    # JSON 파싱
                    data = json.loads(response)
                    logger.info(f"전체 응답: {json.dumps(data, indent=2)}")
                    
                    # 메시지 타입 확인
                    msg_type = data.get("type")
                    
                    if msg_type == "orderbook":
                        # 오더북 데이터 출력
                        logger.info(f"오더북 스냅샷 수신 (symbol: {data.get('code', 'unknown')})")
                        logger.info(f"타임스탬프: {data.get('timestamp')}")
                        
                        # 매수/매도 호가 개수 출력
                        orderbook_units = data.get("orderbook_units", [])
                        logger.info(f"호가 데이터 개수: {len(orderbook_units)}개")
                        
                        # 최상위 호가 출력 (있는 경우)
                        if orderbook_units:
                            first_unit = orderbook_units[0]
                            logger.info(f"최고 매수호가: {first_unit.get('bid_price')} (수량: {first_unit.get('bid_size')})")
                            logger.info(f"최저 매도호가: {first_unit.get('ask_price')} (수량: {first_unit.get('ask_size')})")
                    else:
                        # 기타 메시지 출력
                        logger.info(f"기타 메시지 수신: {msg_type}")
                        logger.info(f"메시지 내용: {response[:200]}...")  # 일부만 출력
                
                except asyncio.TimeoutError:
                    logger.warning("응답 대기 타임아웃")
                    break
                    
                except Exception as e:
                    logger.error(f"메시지 처리 중 오류: {str(e)}")
                    break
    
    except Exception as e:
        logger.error(f"테스트 실행 중 오류 발생: {str(e)}")

async def test_orderbook_snapshot_v2():
    """
    빗썸 웹소켓을 통한 오더북 스냅샷 요청 및 수신 테스트 (API 문서 기반)
    """
    logger.info("빗썸 웹소켓 스냅샷 테스트 시작 (V2)")
    
    try:
        # 웹소켓 연결
        logger.info(f"웹소켓 연결 시도: {WS_URL}")
        async with websockets.connect(WS_URL) as websocket:
            logger.info("웹소켓 연결 성공")
            
            # 테스트할 심볼 (BTC)
            symbol = "KRW-BTC"
            
            # API 문서 기반 오더북 스냅샷 요청 메시지 생성
            request_message = [
                {
                    "ticket": "test-snapshot-2"
                },
                {
                    "type": "orderbook",
                    "codes": [symbol]
                },
                {
                    "format": "SIMPLE"
                }
            ]
            
            # 요청 메시지 송신
            logger.info(f"요청 메시지: {json.dumps(request_message)}")
            await websocket.send(json.dumps(request_message))
            
            # 응답 대기 및 처리 (여러 메시지를 수신할 수 있음)
            for _ in range(5):  # 최대 5개 메시지 수신
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    logger.info("응답 수신")
                    
                    # JSON 파싱
                    data = json.loads(response)
                    logger.info(f"전체 응답: {json.dumps(data, indent=2)}")
                    
                    # 메시지 타입 확인
                    msg_type = data.get("type")
                    
                    if msg_type == "orderbook":
                        # 오더북 데이터 출력
                        logger.info(f"오더북 스냅샷 수신 (symbol: {data.get('code', 'unknown')})")
                        logger.info(f"타임스탬프: {data.get('timestamp')}")
                        
                        # 매수/매도 호가 개수 출력
                        orderbook_units = data.get("orderbook_units", [])
                        logger.info(f"호가 데이터 개수: {len(orderbook_units)}개")
                        
                        # 최상위 호가 출력 (있는 경우)
                        if orderbook_units:
                            first_unit = orderbook_units[0]
                            logger.info(f"최고 매수호가: {first_unit.get('bid_price')} (수량: {first_unit.get('bid_size')})")
                            logger.info(f"최저 매도호가: {first_unit.get('ask_price')} (수량: {first_unit.get('ask_size')})")
                    else:
                        # 기타 메시지 출력
                        logger.info(f"기타 메시지 수신: {msg_type}")
                        logger.info(f"메시지 내용: {response[:200]}...")  # 일부만 출력
                
                except asyncio.TimeoutError:
                    logger.warning("응답 대기 타임아웃")
                    break
                    
                except Exception as e:
                    logger.error(f"메시지 처리 중 오류: {str(e)}")
                    break
    
    except Exception as e:
        logger.error(f"테스트 실행 중 오류 발생: {str(e)}")

async def main():
    """
    메인 함수
    """
    # 두 가지 방식으로 테스트 실행
    await test_orderbook_snapshot()
    logger.info("\n" + "-" * 50 + "\n")
    await test_orderbook_snapshot_v2()

if __name__ == "__main__":
    # 이벤트 루프 실행
    asyncio.run(main()) 