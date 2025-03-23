#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
업비트 웹소켓 커넥터 클래스 테스트

이 스크립트는 UpbitWebSocketConnector 클래스를 직접 테스트합니다.
프레임워크의 다른 부분에 의존하지 않도록 필요한 부분을 직접 구현합니다.
"""

import asyncio
import json
import logging
import os
import sys
import time
from typing import Dict, Any, List

# 프로젝트 루트 디렉토리를 파이썬 경로에 추가
project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_dir)

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('upbit_connector_test')

# 필요한 모듈 임포트
from src.crosskimp.ob_collector.orderbook.connection.upbit_s_cn import UpbitWebSocketConnector
from src.crosskimp.config.constants_v3 import Exchange

# 간단한 이벤트 핸들러 모킹
class MockEventHandler:
    def __init__(self):
        self.events = []
        self.logger = logging.getLogger('mock_event_handler')
    
    async def handle_data_event(self, exchange, event_type, data, **kwargs):
        self.events.append((exchange, event_type, data, kwargs))
        self.logger.info(f"이벤트 핸들링: {exchange} - {event_type}")
        return True
    
    async def handle_connection_status(self, status, exchange=None, message=None, timestamp=None, **kwargs):
        self.logger.info(f"연결 상태 변경: {exchange} - {status} - {message}")
        return True
    
    async def send_telegram_message(self, exchange, event_type, message, **kwargs):
        self.logger.info(f"텔레그램 메시지: {exchange} - {event_type} - {message}")
        return True

# 테스트 설정
test_settings = {
    "logging": {
        "level": "DEBUG",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    },
    "symbols": ["BTC", "ETH"],
    "exchange_settings": {
        "UPBIT": {
            "enabled": True,
            "use_rest_api": False
        }
    }
}

async def test_upbit_connector():
    """
    업비트 웹소켓 커넥터 테스트
    """
    logger.info("===== 업비트 웹소켓 커넥터 테스트 =====")
    
    # 이벤트 핸들러 모킹
    event_handler = MockEventHandler()
    
    try:
        # 커넥터 인스턴스 생성
        connector = UpbitWebSocketConnector(test_settings)
        
        # 이벤트 핸들러 직접 설정 (기존 BaseConnector 초기화 문제 우회)
        connector.event_handler = event_handler
        
        logger.info("웹소켓 연결 시도...")
        # 연결 시도
        connect_result = await connector.connect()
        
        if connect_result:
            logger.info("✅ 웹소켓 연결 성공!")
            
            # 연결 상태 체크
            logger.info(f"연결 상태: {connector.is_connected}")
            
            # 구독 메시지 생성 (실제 사용시에는 subscription 클래스가 처리)
            symbols = test_settings['symbols']
            market_codes = [f"KRW-{s.upper()}" for s in symbols]
            
            ticket_name = f"connector_test_{int(time.time() * 1000)}"
            subscribe_message = [
                {"ticket": ticket_name},
                {
                    "type": "orderbook",
                    "codes": market_codes,
                    "is_only_realtime": False,
                },
                {"format": "DEFAULT"}
            ]
            
            # 메시지 직접 전송
            logger.info(f"구독 메시지 직접 전송: {market_codes}")
            if connector.ws:
                await connector.ws.send(json.dumps(subscribe_message))
                
                # 몇 개의 메시지 수신
                try:
                    for i in range(3):
                        logger.info("메시지 수신 대기 중...")
                        response = await asyncio.wait_for(connector.ws.recv(), timeout=5.0)
                        
                        # 응답 처리
                        if isinstance(response, bytes):
                            response = response.decode('utf-8')
                            
                        try:
                            data = json.loads(response)
                            logger.info(f"메시지 #{i+1} 수신: {json.dumps(data, indent=2)[:200]}...")
                        except:
                            logger.info(f"메시지 #{i+1} 수신: {response[:200]}...")
                except asyncio.TimeoutError:
                    logger.warning("메시지 수신 타임아웃")
            
            # 연결 종료
            logger.info("웹소켓 연결 종료 요청")
            await connector.disconnect()
            logger.info(f"연결 종료 후 상태: {connector.is_connected}")
            
            return True
            
        else:
            logger.error("❌ 웹소켓 연결 실패")
            return False
    
    except Exception as e:
        logger.error(f"❌ 테스트 중 오류: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def main():
    """메인 함수"""
    success = await test_upbit_connector()
    if success:
        logger.info("🎉 업비트 웹소켓 커넥터 테스트 성공!")
        return 0
    else:
        logger.error("❌ 업비트 웹소켓 커넥터 테스트 실패!")
        return 1

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    exit_code = loop.run_until_complete(main())
    loop.close() 