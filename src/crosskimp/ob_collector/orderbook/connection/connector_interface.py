"""
거래소 커넥터 인터페이스 모듈

모든 거래소 커넥터가 구현해야 하는 표준 인터페이스를 정의합니다.
이를 통해 ConnectionManager와 OrderbookCollectorManager가 일관된 방식으로 
각 거래소 커넥터와 상호작용할 수 있습니다.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Callable
import websockets


class ExchangeConnectorInterface(ABC):
    """
    거래소 커넥터 인터페이스
    
    모든 거래소 커넥터 클래스가 구현해야 하는 표준 인터페이스입니다.
    이 인터페이스는 웹소켓 연결 관리, 메시지 처리, 구독 관리 등의 
    핵심 기능에 대한 일관된 API를 정의합니다.
    """

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """
        현재 연결 상태 반환
        
        Returns:
            bool: 연결 상태 (True: 연결됨, False: 연결 안됨)
        """
        pass
    
    @abstractmethod
    async def connect(self) -> bool:
        """
        거래소 웹소켓 서버에 연결
        
        Returns:
            bool: 연결 성공 여부
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> bool:
        """
        거래소 웹소켓 연결 종료
        
        Returns:
            bool: 연결 종료 성공 여부
        """
        pass
    
    @abstractmethod
    async def subscribe(self, symbols: List[str]) -> bool:
        """
        심볼 목록 구독
        
        Args:
            symbols: 구독할 심볼 목록
            
        Returns:
            bool: 구독 성공 여부
        """
        pass
    
    @abstractmethod
    async def unsubscribe(self, symbols: List[str]) -> bool:
        """
        심볼 목록 구독 해제
        
        Args:
            symbols: 구독 해제할 심볼 목록
            
        Returns:
            bool: 구독 해제 성공 여부
        """
        pass
    
    @abstractmethod
    def add_message_callback(self, callback: Callable) -> None:
        """
        웹소켓 메시지 콜백 함수 등록
        
        Args:
            callback: 메시지 수신 시 호출될 콜백 함수
        """
        pass
    
    @abstractmethod
    def remove_message_callback(self, callback: Callable) -> bool:
        """
        웹소켓 메시지 콜백 함수 제거
        
        Args:
            callback: 제거할 콜백 함수
            
        Returns:
            bool: 콜백 제거 성공 여부
        """
        pass
    
    @abstractmethod
    async def send_message(self, message: Union[str, Dict, List]) -> bool:
        """
        웹소켓을 통해 메시지 전송
        
        Args:
            message: 전송할 메시지 (문자열, 딕셔너리 또는 리스트)
            
        Returns:
            bool: 메시지 전송 성공 여부
        """
        pass
    
    @abstractmethod
    def add_orderbook_callback(self, callback: Callable) -> None:
        """
        오더북 업데이트 콜백 함수 등록
        
        Args:
            callback: 오더북 업데이트 수신 시 호출될 콜백 함수
        """
        pass
    
    @abstractmethod
    def remove_orderbook_callback(self, callback: Callable) -> bool:
        """
        오더북 업데이트 콜백 함수 제거
        
        Args:
            callback: 제거할 콜백 함수
            
        Returns:
            bool: 콜백 제거 성공 여부
        """
        pass
    
    @abstractmethod
    async def get_orderbook_snapshot(self, symbol: str) -> Dict[str, Any]:
        """
        특정 심볼의 오더북 스냅샷 조회
        
        Args:
            symbol: 조회할 심볼
            
        Returns:
            Dict[str, Any]: 오더북 스냅샷 데이터
        """
        pass
    
    @abstractmethod
    async def refresh_snapshots(self, symbols: List[str] = None) -> None:
        """
        오더북 스냅샷 갱신
        
        Args:
            symbols: 갱신할 심볼 목록 (None이면 모든 구독 중인 심볼)
        """
        pass
    
    @abstractmethod
    async def get_websocket(self) -> Optional[websockets.WebSocketClientProtocol]:
        """
        웹소켓 객체 반환 (ConnectionManager 호환용)
        
        Returns:
            Optional[websockets.WebSocketClientProtocol]: 웹소켓 객체
        """
        pass
        
    @abstractmethod
    async def reconnect(self) -> bool:
        """
        재연결 수행
        
        Returns:
            bool: 재연결 성공 여부
        """
        pass 