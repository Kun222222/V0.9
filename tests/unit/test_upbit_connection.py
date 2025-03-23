#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
업비트 웹소켓 연결 테스트 스크립트

이 스크립트는 업비트 웹소켓 연결이 제대로 작동하는지 
단위 테스트하기 위한 파일입니다.
"""

import os
import sys
import asyncio
import time
import json
import logging
from typing import Dict, Any

# 상위 디렉토리를 sys.path에 추가하여 모듈 import가 가능하도록 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '../..'))
sys.path.insert(0, project_root)

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('upbit_test')

# 테스트 대상 모듈 import
from src.crosskimp.ob_collector.orderbook.connection.upbit_s_cn import UpbitWebSocketConnector
from src.crosskimp.ob_collector.orderbook.subscription.upbit_s_sub import UpbitSubscription

# 테스트 도우미 함수들
async def test_connection(settings: Dict[str, Any]) -> bool:
    """
    업비트 웹소켓 연결 테스트
    
    Args:
        settings: 연결 설정
        
    Returns:
        bool: 연결 성공 여부
    """
    logger.info("===== 업비트 웹소켓 연결 테스트 시작 =====")
    
    # 연결 객체 생성
    connector = UpbitWebSocketConnector(settings)
    
    # 연결 디버그 정보 출력
    logger.debug(f"웹소켓 URL: {connector.ws_url}")
    logger.debug(f"Exchange Code: {connector.exchange_code}")
    logger.debug(f"Exchange KR: {connector.exchange_kr}")
    logger.debug(f"Event Handler: {connector.event_handler}")
    
    # 연결 시도
    try:
        logger.info("웹소켓 연결 시도...")
        success = await connector.connect()
        
        if success:
            logger.info("✅ 웹소켓 연결 성공!")
            logger.debug(f"웹소켓 객체: {connector.ws}")
            logger.debug(f"연결 상태: {connector.is_connected}")
            
            # 연결 상태 확인
            assert connector.is_connected, "연결 성공했지만 is_connected가 False입니다"
            assert connector.ws is not None, "연결 성공했지만 웹소켓 객체가 None입니다"
            
            # 접속 끊기
            logger.info("웹소켓 연결 종료 중...")
            await connector.disconnect()
            logger.info("웹소켓 연결 종료됨")
            
            return True
        else:
            logger.error("❌ 웹소켓 연결 실패!")
            return False
            
    except Exception as e:
        logger.error(f"❌ 웹소켓 연결 테스트 중 예외 발생: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def test_subscription(settings: Dict[str, Any]) -> bool:
    """
    업비트 구독 테스트
    
    Args:
        settings: 연결 설정
        
    Returns:
        bool: 구독 성공 여부
    """
    logger.info("===== 업비트 구독 테스트 시작 =====")
    
    # 연결 객체 생성
    connector = UpbitWebSocketConnector(settings)
    
    try:
        # 구독 객체 생성
        subscription = UpbitSubscription(connector)
        
        # 구독 디버그 정보 출력
        logger.debug(f"Subscription Exchange Code: {subscription.exchange_code}")
        logger.debug(f"Connection: {subscription.connection}")
        logger.debug(f"Event Handler: {subscription.event_handler}")
        
        # 테스트 심볼 설정
        test_symbols = ["BTC", "ETH"]
        
        # 구독 메시지 생성 테스트
        logger.info(f"구독 메시지 생성 테스트: {test_symbols}")
        subscribe_msg = await subscription.create_subscribe_message(test_symbols)
        logger.debug(f"생성된 구독 메시지: {json.dumps(subscribe_msg, indent=2)}")
        
        # 심볼 구독 시도
        logger.info(f"심볼 구독 시도: {test_symbols}")
        success = await subscription.subscribe(test_symbols)
        
        if success:
            logger.info("✅ 구독 성공!")
            # 스트림 정보 출력
            logger.debug(f"구독된 심볼: {subscription.subscribed_symbols}")
            
            # 구독 취소
            logger.info("구독 취소 중...")
            await subscription.unsubscribe()
            logger.info("구독 취소됨")
            
            # 연결 종료
            await connector.disconnect()
            logger.info("연결 종료됨")
            
            return True
        else:
            logger.error("❌ 구독 실패!")
            
            # 실패 원인 분석
            logger.debug(f"연결 상태: {connector.is_connected}")
            logger.debug(f"웹소켓 객체: {connector.ws}")
            
            # 연결 종료
            await connector.disconnect()
            return False
            
    except Exception as e:
        logger.error(f"❌ 구독 테스트 중 예외 발생: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # 정리
        try:
            await connector.disconnect()
        except:
            pass
            
        return False

async def main():
    """
    메인 테스트 함수
    """
    # 테스트 설정
    settings = {
        "api_key": "",  # 실제 API 키는 필요하지 않음
        "api_secret": "",
        "name": "업비트",
        "log_level": "DEBUG"
    }
    
    # 웹소켓 연결 테스트
    connection_success = await test_connection(settings)
    
    if connection_success:
        logger.info("연결 테스트 성공! 이제 구독 테스트를 시작합니다.")
        # 구독 테스트
        subscription_success = await test_subscription(settings)
        
        if subscription_success:
            logger.info("🎉 모든 테스트 성공!")
            return 0
        else:
            logger.error("❌ 구독 테스트 실패!")
            return 1
    else:
        logger.error("❌ 연결 테스트 실패! 구독 테스트를 건너뜁니다.")
        return 1

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    exit_code = loop.run_until_complete(main())
    sys.exit(exit_code) 