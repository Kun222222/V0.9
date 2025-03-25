"""
오더북 프로세스 핸들러

ProcessComponent를 상속받아 오더북 프로세스의 시작/종료를 관리합니다.
"""

import asyncio
import logging
from typing import Dict, Any, Optional

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.domains.process_component import ProcessComponent
from crosskimp.common.config.common_constants import SystemComponent

# 오더북 수집기 임포트
from crosskimp.ob_collector.ob_collector import ObCollector

# 로거 설정
logger = get_unified_logger(component=SystemComponent.SYSTEM.value)

class OrderbookProcess(ProcessComponent):
    """
    오더북 프로세스 컴포넌트
    
    오더북 데이터 수집 프로세스의 시작/종료를 관리합니다.
    """
    
    def __init__(self, process_name: str = "ob_collector"):
        """
        오더북 프로세스 초기화
        
        Args:
            process_name: 프로세스 이름 (기본값: "ob_collector")
        """
        super().__init__(process_name)
        self.is_running = False
        self.collector = ObCollector()  # 오더북 수집기 인스턴스 생성
        
    async def start(self) -> bool:
        """
        오더북 프로세스 시작
        
        오더북 수집기의 start 메서드를 호출하여 오더북 수집을 시작합니다.
        
        Returns:
            bool: 시작 성공 여부
        """
        if self.is_running:
            self.logger.warning(f"오더북 프로세스가 이미 실행 중입니다.")
            return True
            
        try:
            self.logger.info("오더북 프로세스 시작 중...")
            self.logger.debug("OrderbookProcess.start() 메서드 호출됨 - ObCollector.start() 호출 전")
            
            # 오더북 수집기를 사용하여 시작
            success = await self.collector.start()
            
            self.logger.debug(f"ObCollector.start() 함수 호출 결과: {success}")
            
            if success:
                self.is_running = True
                self.logger.info("오더북 프로세스가 성공적으로 시작되었습니다.")
                return True
            else:
                self.logger.error("오더북 프로세스 시작 실패")
                return False
            
        except Exception as e:
            self.logger.error(f"오더북 프로세스 시작 중 오류 발생: {str(e)}")
            self.logger.error(f"오류 상세: {e.__class__.__name__}: {str(e)}")
            import traceback
            self.logger.error(f"스택 트레이스: {traceback.format_exc()}")
            self.is_running = False
            return False
    
    async def stop(self) -> bool:
        """
        오더북 프로세스 종료
        
        오더북 수집기의 stop 메서드를 호출하여 오더북 수집을 종료합니다.
        
        Returns:
            bool: 종료 성공 여부
        """
        if not self.is_running:
            self.logger.warning("오더북 프로세스가 이미 중지되었습니다.")
            return True
            
        try:
            self.logger.info("오더북 프로세스 종료 중...")
            
            # 오더북 수집기를 사용하여 종료
            success = await self.collector.stop()
            
            # 상태 업데이트
            self.is_running = False
            
            if success:
                self.logger.info("오더북 프로세스가 성공적으로 종료되었습니다.")
            else:
                self.logger.warning("오더북 프로세스 종료 중 일부 오류가 발생했습니다.")
                
            return True
            
        except Exception as e:
            self.logger.error(f"오더북 프로세스 종료 중 오류 발생: {str(e)}")
            self.is_running = False
            return False

# 싱글톤 인스턴스
_instance = None

def get_orderbook_process() -> OrderbookProcess:
    """
    오더북 프로세스 컴포넌트 인스턴스를 반환합니다.
    
    싱글톤 패턴을 사용하여 하나의 인스턴스만 생성합니다.
    
    Returns:
        OrderbookProcess: 오더북 프로세스 컴포넌트 인스턴스
    """
    global _instance
    if _instance is None:
        _instance = OrderbookProcess()
    return _instance

async def initialize_orderbook_process():
    """
    오더북 프로세스 컴포넌트를 초기화합니다.
    
    이 함수는 시스템 시작 시 호출되어야 합니다.
    """
    process = get_orderbook_process()
    await process.setup()
    await process.collector.setup()  # 오더북 수집기의 setup 메서드도 호출
    return process