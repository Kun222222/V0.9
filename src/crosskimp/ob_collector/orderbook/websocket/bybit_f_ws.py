import asyncio
import json
import time
import aiohttp
from websockets import connect
from typing import Dict, List, Optional, Any

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG

from crosskimp.ob_collector.orderbook.connection.bybit_f_cn import BybitFutureWebSocketConnector
from crosskimp.ob_collector.orderbook.orderbook.bybit_f_ob import BybitFutureOrderBookManager
from crosskimp.ob_collector.orderbook.parser.bybit_f_pa import BybitFutureParser

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이빗 선물 웹소켓 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BYBIT_FUTURE.value  # 거래소 코드
BYBIT_FUTURE_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이빗 선물 설정

# 웹소켓 연결 설정
WS_URL = BYBIT_FUTURE_CONFIG["ws_url"]  # 웹소켓 URL
PING_INTERVAL = BYBIT_FUTURE_CONFIG["ping_interval"]  # 핑 전송 간격 (초)
PING_TIMEOUT = BYBIT_FUTURE_CONFIG["ping_timeout"]  # 핑 응답 타임아웃 (초)

# 오더북 관련 설정
DEFAULT_DEPTH = BYBIT_FUTURE_CONFIG["default_depth"]  # 기본 오더북 깊이
MAX_SYMBOLS_PER_BATCH = BYBIT_FUTURE_CONFIG["max_symbols_per_batch"]  # 배치당 최대 심볼 수

class BybitFutureWebsocket(BybitFutureWebSocketConnector):
    """
    Bybit 선물(Linear) WebSocket
    - wss://stream.bybit.com/v5/public/linear
    - 공식 문서 기반 구현
    - 핑퐁 메커니즘 (20초)
    - 스냅샷/델타 처리
    - 지수 백오프 재연결 전략
    - 공통 모듈 활용 강화
    
    메시지 처리 및 오더북 관리를 담당합니다.
    연결 관리는 BybitFutureWebSocketConnector 클래스에서 처리합니다.
    """
    def __init__(self, settings: dict):
        super().__init__(settings)
        
        # 오더북 설정
        self.depth_level = settings.get("depth", DEFAULT_DEPTH)
        self.orderbook_manager = BybitFutureOrderBookManager(self.depth_level)
        self.orderbook_manager.set_websocket(self)  # 웹소켓 연결 설정
        
        # 파서 초기화
        self.parser = BybitFutureParser()
        
        # 스냅샷 관리
        self.snapshot_received = set()  # 스냅샷을 받은 심볼 목록
        self.snapshot_pending = set()   # 스냅샷 요청 대기 중인 심볼 목록
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 메시지 처리 통계
        self._raw_log_count = 0

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """
        출력 큐 설정
        - 부모 클래스의 output_queue 설정
        - 오더북 매니저의 output_queue 설정
        """
        # 부모 클래스의 output_queue 설정
        super().set_output_queue(queue)
        
        # 오더북 매니저의 output_queue 설정
        self.orderbook_manager.set_output_queue(queue)
        
        # 로깅 추가
        self.log_info(f"웹소켓 출력 큐 설정 완료 (큐 ID: {id(queue)})")
        
        # 큐 설정 확인
        if not hasattr(self.orderbook_manager, '_output_queue') or self.orderbook_manager._output_queue is None:
            self.log_error("오더북 매니저 큐 설정 실패!")
        else:
            self.log_info(f"오더북 매니저 큐 설정 확인 (큐 ID: {id(self.orderbook_manager._output_queue)})")

    # 이 메서드는 더 이상 사용되지 않습니다.
    # async def parse_message(self, message: str) -> Optional[dict]:
    #     """
    #     수신된 메시지 파싱
        
    #     Args:
    #         message: 수신된 웹소켓 메시지
            
    #     Returns:
    #         Optional[dict]: 파싱된 메시지 또는 None
    #     """
    #     try:
    #         data = json.loads(message)
            
    #         # 구독 응답 처리
    #         if data.get("op") == "subscribe":
    #             if self.connection_status_callback:
    #                 self.connection_status_callback(self.exchangename, "subscribe_response")
    #             self.log_debug(f"구독 응답 수신: {data}")
    #             return None

    #         if data.get("op") == "pong" or (data.get("ret_msg") == "pong" and data.get("op") == "ping"):
    #             self._handle_pong(data)
    #             return None

    #         if "topic" in data and "data" in data:
    #             topic = data.get("topic", "")
    #             if "orderbook" in topic:
    #                 parts = topic.split(".")
    #                 if len(parts) >= 3:
    #                     symbol = parts[-1].replace("USDT", "")
    #                     msg_type = data.get("type", "delta")
                        
    #                     # 원본 메시지 로깅
    #                     self.log_raw_message(msg_type, message, symbol)
                        
    #                     # ELX 코인에 대해 추가 로깅
    #                     if symbol == "ELX":
    #                         ob_data = data.get("data", {})
    #                         bids_count = len(ob_data.get("b", []))
    #                         asks_count = len(ob_data.get("a", []))
    #                         self.log_info(
    #                             f"ELX 메시지 수신 | "
    #                             f"타입: {msg_type}, 매수: {bids_count}건, 매도: {asks_count}건, "
    #                             f"시퀀스: {ob_data.get('u', 'N/A')}"
    #                         )
                        
    #                     if msg_type == "snapshot":
    #                         self.log_info(f"스냅샷 메시지 수신: {symbol}")
    #                         self.snapshot_received.add(symbol)
    #                         self.snapshot_pending.discard(symbol)
    #                         if self.connection_status_callback:
    #                             self.connection_status_callback(self.exchangename, "snapshot_received")
    #                     return data
    #         return None
    #     except Exception as e:
    #         self.log_error(f"메시지 파싱 실패: {str(e)}")
    #         return None

    async def request_snapshot(self, market: str) -> Optional[dict]:
        """
        REST API를 통해 스냅샷 요청
        """
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            # 요청 뎁스를 200으로 증가 (최대값)
            request_depth = 200
            
            url = (f"https://api.bybit.com/v5/market/orderbook"
                   f"?category=linear&symbol={market}"
                   f"&limit={request_depth}")
            
            self.log_info(f"{market} 스냅샷 요청 | 요청 뎁스: {request_depth}")
            
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "snapshot_request")
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.log_raw_message("snapshot", json.dumps(data), market.replace("USDT", ""))
                    if data.get("retCode") == 0:
                        result = data.get("result", {})
                        
                        # 받은 데이터의 뎁스 로깅
                        bids_count = len(result.get("b", []))
                        asks_count = len(result.get("a", []))
                        self.log_info(
                            f"{market} 스냅샷 응답 | "
                            f"매수: {bids_count}건, 매도: {asks_count}건"
                        )
                        
                        if self.connection_status_callback:
                            self.connection_status_callback(self.exchangename, "snapshot_received")
                        
                        # 파서를 사용하여 스냅샷 데이터 파싱
                        return self.parser.parse_snapshot_data(result, market)
                    else:
                        self.log_error(f"{market} 스냅샷 응답 에러: {data}")
                else:
                    self.log_error(f"{market} 스냅샷 요청 실패: status={resp.status}")
        except Exception as e:
            self.log_error(f"{market} 스냅샷 요청 예외: {e}")
        return None

    async def process_message(self, message: str) -> None:
        """
        수신된 메시지 처리 (BybitFutureWebSocketConnector 클래스의 추상 메서드 구현)
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        # 메시지 수신 로깅
        self.log_debug(f"메시지 수신: {message[:100]}...")
        
        # 부모 클래스의 process_message 메서드 호출
        await super().process_message(message)

    async def _prepare_start(self, symbols: List[str]) -> None:
        """
        시작 전 초기화 및 설정 (BaseWebsocketConnector 템플릿 메서드 구현)
        """
        self.snapshot_received.clear()
        self.snapshot_pending.clear()
        self.orderbook_manager.clear_all()
        self.log_info(f"시작 준비 완료 | 심볼 수: {len(symbols)}개")