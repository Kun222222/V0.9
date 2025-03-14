"""
C++ 인터페이스 모듈

이 모듈은 파이썬과 C++ 간의 인터페이스를 제공합니다.
"""

import time
import asyncio
from typing import Dict, Any, Optional, Callable

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.cpp.shared_memory_manager import get_shared_memory_manager
from crosskimp.ob_collector.cpp.flatbuffers_serializer import get_orderbook_serializer

# 로거 인스턴스 가져오기
logger = get_unified_logger()

class CppInterface:
    """
    C++ 인터페이스 클래스
    
    이 클래스는 파이썬과 C++ 간의 인터페이스를 제공합니다.
    - 오더북 데이터 직렬화 및 공유 메모리 저장
    - 메트릭 수집 및 모니터링
    """
    
    def __init__(self):
        """C++ 인터페이스 초기화"""
        self.shared_memory_manager = get_shared_memory_manager()
        self.orderbook_serializer = get_orderbook_serializer()
        
        # 메트릭 초기화
        self.metrics = {
            "process_count": 0,
            "process_errors": 0,
            "last_process_time": 0,
            "avg_process_time_ms": 0,
        }
        self._total_process_time = 0
        
        # 초기화
        self._initialize()
    
    def _initialize(self) -> bool:
        """
        인터페이스 초기화
        
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            # 공유 메모리 초기화
            if not self.shared_memory_manager.initialize():
                logger.error("공유 메모리 초기화 실패")
                return False
            
            logger.info("C++ 인터페이스 초기화 완료")
            return True
            
        except Exception as e:
            logger.error(f"C++ 인터페이스 초기화 실패: {str(e)}", exc_info=True)
            return False
    
    async def process_orderbook(self, exchange_name: str, orderbook_data: Dict[str, Any]) -> bool:
        """
        오더북 데이터 처리 및 C++로 전달
        
        Args:
            exchange_name: 거래소 이름
            orderbook_data: 오더북 데이터
            
        Returns:
            bool: 처리 성공 여부
        """
        try:
            start_time = time.time()
            symbol = orderbook_data.get("symbol", "UNKNOWN")
            
            # 오더북 데이터 직렬화
            logger.info(f"C++ 인터페이스: {exchange_name} {symbol} 데이터 직렬화 시작")
            serialized_data = self.orderbook_serializer.serialize_orderbook(orderbook_data)
            if serialized_data is None:
                logger.error(f"오더북 직렬화 실패: {exchange_name} {symbol}")
                self.metrics["process_errors"] += 1
                return False
                
            serialized_size = len(serialized_data)
            logger.info(f"C++ 인터페이스: {exchange_name} {symbol} 데이터 직렬화 완료 (크기: {serialized_size} 바이트)")
            
            # 공유 메모리에 데이터 저장
            logger.info(f"C++ 인터페이스: {exchange_name} {symbol} 공유 메모리 쓰기 시작")
            if not self.shared_memory_manager.write_data(serialized_data):
                logger.error(f"공유 메모리 쓰기 실패: {exchange_name} {symbol}")
                self.metrics["process_errors"] += 1
                return False
                
            logger.info(f"C++ 인터페이스: {exchange_name} {symbol} 공유 메모리 쓰기 완료")
            
            # 메트릭 업데이트
            end_time = time.time()
            process_time_ms = (end_time - start_time) * 1000
            
            self.metrics["process_count"] += 1
            self.metrics["last_process_time"] = end_time
            
            self._total_process_time += process_time_ms
            self.metrics["avg_process_time_ms"] = (
                self._total_process_time / self.metrics["process_count"]
                if self.metrics["process_count"] > 0 else 0
            )
            
            logger.info(f"C++ 인터페이스: {exchange_name} {symbol} 처리 완료 (소요시간: {process_time_ms:.2f}ms)")
            return True
            
        except Exception as e:
            logger.error(f"오더북 처리 실패: {str(e)}", exc_info=True)
            self.metrics["process_errors"] += 1
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        메트릭 정보 반환
        
        Returns:
            Dict[str, Any]: 메트릭 정보
        """
        # 자체 메트릭과 하위 컴포넌트 메트릭 통합
        combined_metrics = {
            "cpp_interface": self.metrics,
            "shared_memory": self.shared_memory_manager.get_metrics(),
            "serializer": self.orderbook_serializer.get_metrics(),
        }
        return combined_metrics


# 싱글톤 인스턴스
_cpp_interface = None

def get_cpp_interface() -> CppInterface:
    """
    C++ 인터페이스 싱글톤 인스턴스 반환
    
    Returns:
        CppInterface: C++ 인터페이스 인스턴스
    """
    global _cpp_interface
    if _cpp_interface is None:
        _cpp_interface = CppInterface()
    return _cpp_interface


async def send_orderbook_to_cpp(exchange_name: str, orderbook_data: Dict[str, Any]) -> bool:
    """
    오더북 데이터를 C++로 전송하는 유틸리티 함수
    
    Args:
        exchange_name: 거래소 이름
        orderbook_data: 오더북 데이터
        
    Returns:
        bool: 전송 성공 여부
    """
    cpp_interface = get_cpp_interface()
    return await cpp_interface.process_orderbook(exchange_name, orderbook_data) 