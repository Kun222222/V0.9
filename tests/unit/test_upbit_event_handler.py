#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
업비트 이벤트 핸들러 진단 스크립트

이 스크립트는 업비트 이벤트 핸들러 초기화 문제를 진단하는 코드입니다.
"""

import os
import sys
import asyncio
import time
import logging
import importlib
import inspect
from typing import Dict, Any

# 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '../..'))
sys.path.insert(0, project_root)

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('upbit_event_handler_test')

# 모듈 임포트 (직접 클래스를 가져오지 않고 모듈을 가져옴)
from src.crosskimp.ob_collector.orderbook.util.event_handler import EventHandler, LoggingMixin
from src.crosskimp.ob_collector.orderbook.connection.base_connector import BaseWebsocketConnector

def inspect_event_handler():
    """
    이벤트 핸들러 클래스 검사
    """
    logger.info("===== 이벤트 핸들러 클래스 검사 =====")
    
    # EventHandler 클래스 검사
    logger.info("EventHandler 클래스 멤버 검사:")
    for name, member in inspect.getmembers(EventHandler):
        if not name.startswith('_'):  # 내부/매직 메서드 제외
            logger.debug(f"  - {name}: {type(member)}")
    
    # 특히 get_instance 메서드 확인
    if hasattr(EventHandler, 'get_instance'):
        logger.info("EventHandler.get_instance 메서드 시그니처 확인:")
        sig = inspect.signature(EventHandler.get_instance)
        logger.debug(f"  - 시그니처: {sig}")
        logger.debug(f"  - 파라미터: {sig.parameters}")
    else:
        logger.error("❌ EventHandler에 get_instance 메서드가 없습니다!")

def inspect_base_connector():
    """
    BaseWebsocketConnector 클래스 검사
    """
    logger.info("===== BaseWebsocketConnector 클래스 검사 =====")
    
    # 초기화 메서드 검사
    if hasattr(BaseWebsocketConnector, '__init__'):
        logger.info("BaseWebsocketConnector.__init__ 메서드 검사:")
        sig = inspect.signature(BaseWebsocketConnector.__init__)
        logger.debug(f"  - 시그니처: {sig}")
        logger.debug(f"  - 파라미터: {sig.parameters}")
        
        # 소스 코드 확인
        try:
            source_lines = inspect.getsourcelines(BaseWebsocketConnector.__init__)
            logger.info("BaseWebsocketConnector.__init__ 소스 코드:")
            for i, line in enumerate(source_lines[0]):
                logger.debug(f"  {source_lines[1] + i}: {line.rstrip()}")
        except Exception as e:
            logger.error(f"소스 코드 가져오기 실패: {e}")
    
    # 특히 이벤트 핸들러 초기화 부분 검사
    logger.info("BaseWebsocketConnector 클래스 소스 코드 검사:")
    try:
        source_lines = inspect.getsourcelines(BaseWebsocketConnector)
        
        # event_handler 초기화 부분 찾기
        for i, line in enumerate(source_lines[0]):
            if "event_handler" in line and "=" in line:
                start_line = max(0, i-2)
                end_line = min(len(source_lines[0]), i+5)
                
                logger.info("이벤트 핸들러 초기화 부분:")
                for j in range(start_line, end_line):
                    logger.debug(f"  {source_lines[1] + j}: {source_lines[0][j].rstrip()}")
    except Exception as e:
        logger.error(f"소스 코드 가져오기 실패: {e}")

def create_mock_event_handler():
    """
    EventHandler 대체 객체 테스트
    """
    logger.info("===== EventHandler 객체 생성 테스트 =====")
    
    # 직접 EventHandler 인스턴스 생성 시도
    try:
        # EventHandler 생성
        exchange_code = "upbit"
        settings = {"name": "업비트", "log_level": "DEBUG"}
        
        handler = EventHandler(exchange_code, settings)
        logger.info(f"✅ 직접 EventHandler 인스턴스 생성 성공: {handler}")
        
        # 주요 속성 확인
        logger.debug(f"  - Exchange Code: {handler.exchange_code}")
        logger.debug(f"  - Exchange Name KR: {handler.exchange_name_kr}")
        
        # get_instance 메서드 테스트
        singleton = EventHandler.get_instance(exchange_code, settings)
        logger.info(f"✅ EventHandler.get_instance 호출 성공: {singleton}")
        logger.debug(f"  - 동일 인스턴스 여부: {handler is singleton}")
        
    except Exception as e:
        logger.error(f"❌ EventHandler 객체 생성 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())

def main():
    """
    메인 테스트 함수
    """
    logger.info("===== 업비트 이벤트 핸들러 진단 시작 =====")
    
    # 이벤트 핸들러 클래스 검사
    inspect_event_handler()
    
    # BaseWebsocketConnector 클래스 검사 
    inspect_base_connector()
    
    # 이벤트 핸들러 객체 생성 테스트
    create_mock_event_handler()
    
    logger.info("===== 진단 완료 =====")
    
if __name__ == "__main__":
    main() 