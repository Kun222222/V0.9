import asyncio
import json
import aiohttp
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Optional, List, Any, Callable, Union, Tuple

# 필요한 로거 임포트 - 실제 구현에 맞게 수정 필요
from crosskimp.logger.logger import get_unified_logger


class SnapshotMethod(Enum):
    """스냅샷 수신 방법"""
    WEBSOCKET = "websocket"  # 웹소켓을 통해 스냅샷 수신 (업비트, 바이빗)
    REST = "rest"            # REST API를 통해 스냅샷 수신 (빗섬, 바이낸스)
    NONE = "none"            # 스냅샷 필요 없음


class DeltaMethod(Enum):
    """델타 수신 방법"""
    WEBSOCKET = "websocket"  # 웹소켓을 통해 델타 수신
    NONE = "none"            # 델타 수신 안함 (업비트처럼 항상 전체 스냅샷)


class SubscriptionStatus(Enum):
    """구독 상태"""
    IDLE = "idle"                # 초기 상태
    CONNECTING = "connecting"    # 연결 중
    SUBSCRIBING = "subscribing"  # 구독 중
    SUBSCRIBED = "subscribed"    # 구독 완료
    SNAPSHOT_RECEIVED = "snapshot_received"  # 스냅샷 수신 완료
    DELTA_RECEIVING = "delta_receiving"      # 델타 수신 중
    ERROR = "error"              # 에러 상태
    CLOSED = "closed"            # 종료됨


class BaseSubscription(ABC):
    """
    오더북 구독 기본 클래스
    
    각 거래소별 구독 패턴을 처리할 수 있는 추상 클래스입니다.
    - 업비트: 웹소켓을 통해 스냅샷 형태로 계속 수신
    - 빗섬: REST 스냅샷 > 웹소켓 델타
    - 바이빗(현물, 선물): 웹소켓 스냅샷 > 웹소켓 델타
    - 바이낸스(현물, 선물): REST 스냅샷 > 웹소켓 델타
    """
    
    def __init__(self, connection, parser=None):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
            parser: 메시지 파싱 객체 (선택적)
        """
        self.connection = connection
        self.parser = parser
        self.logger = get_unified_logger()
        
        # 구독 상태 관리
        self.status = SubscriptionStatus.IDLE
        self.subscribed_symbols = {}  # symbol: status
        
        # 스냅샷과 델타 메소드 (거래소별로 달라짐)
        self.snapshot_method = self._get_snapshot_method()
        self.delta_method = self._get_delta_method()
    
    @abstractmethod
    def _get_snapshot_method(self) -> SnapshotMethod:
        """스냅샷 수신 방법 반환"""
        pass
    
    @abstractmethod
    def _get_delta_method(self) -> DeltaMethod:
        """델타 수신 방법 반환"""
        pass
    
    @abstractmethod
    async def create_subscribe_message(self, symbol: Union[str, List[str]]) -> Dict:
        """
        구독 메시지 생성
        
        Args:
            symbol: 심볼 또는 심볼 리스트
            
        Returns:
            Dict: 구독 메시지
        """
        pass
    
    @abstractmethod
    async def create_unsubscribe_message(self, symbol: str) -> Dict:
        """
        구독 취소 메시지 생성
        
        Args:
            symbol: 심볼
            
        Returns:
            Dict: 구독 취소 메시지
        """
        pass
    
    @abstractmethod
    def is_snapshot_message(self, message: str) -> bool:
        """
        메시지가 스냅샷인지 확인
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 스냅샷 메시지인 경우 True
        """
        pass
    
    @abstractmethod
    def is_delta_message(self, message: str) -> bool:
        """
        메시지가 델타인지 확인
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 델타 메시지인 경우 True
        """
        pass
    
    @abstractmethod
    async def get_rest_snapshot(self, symbol: str) -> Dict:
        """
        REST API를 통해 스냅샷 요청 (REST 방식인 경우)
        
        Args:
            symbol: 심볼
            
        Returns:
            Dict: 스냅샷 데이터
        """
        pass
    
    @abstractmethod
    async def subscribe(self, symbol, on_snapshot=None, on_delta=None, on_error=None):
        """
        심볼 구독
        
        단일 심볼 또는 심볼 리스트를 구독합니다.
        
        Args:
            symbol: 구독할 심볼 또는 심볼 리스트
            on_snapshot: 스냅샷 수신 시 호출할 콜백 함수
            on_delta: 델타 수신 시 호출할 콜백 함수
            on_error: 에러 발생 시 호출할 콜백 함수
            
        Returns:
            bool: 구독 성공 여부
        """
        pass
    
    @abstractmethod
    async def unsubscribe(self, symbol: str) -> bool:
        """
        구독 취소
        
        Args:
            symbol: 구독 취소할 심볼
            
        Returns:
            bool: 구독 취소 성공 여부
        """
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """모든 구독 취소 및 자원 정리"""
        pass

