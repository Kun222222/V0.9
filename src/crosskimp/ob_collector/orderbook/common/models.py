"""
오더북 수집기에서 사용하는 공통 데이터 모델 정의
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


@dataclass
class ValidationResult:
    """
    데이터 검증 결과를 나타내는 클래스
    """
    is_valid: bool
    error_messages: List[str] = None

    def __post_init__(self):
        if self.error_messages is None:
            self.error_messages = []


@dataclass
class OrderBookUpdate:
    """
    오더북 업데이트 정보를 나타내는 클래스
    """
    is_valid: bool = True
    error_messages: List[str] = None
    symbol: str = ""
    data: Dict = None
    bids: List[List[float]] = None
    asks: List[List[float]] = None
    timestamp: int = 0
    sequence: int = 0
    first_update_id: int = 0
    final_update_id: int = 0
    prev_final_update_id: int = 0
    transaction_time: int = 0
    buffered: bool = False
    
    def __post_init__(self):
        if self.error_messages is None:
            self.error_messages = []
        if self.bids is None:
            self.bids = []
        if self.asks is None:
            self.asks = []
        if self.data is None:
            self.data = {}


@dataclass
class WebSocketStats:
    """
    웹소켓 통계 정보를 나타내는 클래스
    """
    connected: bool = False
    message_count: int = 0
    error_count: int = 0
    reconnect_count: int = 0
    last_message_time: float = 0.0
    last_error_time: float = 0.0
    last_error_message: str = ""
    last_ping_time: float = 0.0
    last_pong_time: float = 0.0
    latency_ms: float = 0.0
    connection_lost_count: int = 0
    total_uptime_sec: float = 0.0
    connection_start_time: float = 0.0
    total_messages: int = 0
    raw_logged_messages: int = 0


@dataclass
class ConnectionStatus:
    """
    웹소켓 연결 상태 정보를 나타내는 클래스
    """
    is_connected: bool
    last_message_time: float
    reconnect_count: int
    error_count: int
    last_error: Optional[str]
    uptime_sec: float
    latency_ms: float


@dataclass
class ExchangeConfig:
    """
    거래소 설정 정보를 나타내는 클래스
    """
    name: str
    code: str
    ws_url: str
    rest_url: str
    depth: int = 100
    ping_interval: int = 30
    ping_timeout: int = 10
    reconnect_interval: int = 5
    max_reconnect_attempts: int = 10
    subscribe_chunk_size: int = 10
    subscribe_delay: float = 1.0
    additional_settings: Dict[str, Any] = field(default_factory=dict) 