# file: orderbook/connection/binance_f_cn.py

import asyncio
import json
import time
import aiohttp
from websockets import connect
from typing import Dict, List, Optional

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG
from crosskimp.ob_collector.orderbook.connection.base_ws_connector import BaseWebsocketConnector

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이낸스 선물 웹소켓 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BINANCE_FUTURE.value  # 거래소 코드
BINANCE_FUTURE_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이낸스 선물 설정

# 웹소켓 연결 설정
WS_BASE_URL = BINANCE_FUTURE_CONFIG["ws_base_url"]  # 웹소켓 기본 URL
PING_INTERVAL = BINANCE_FUTURE_CONFIG["ping_interval"]  # 핑 전송 간격 (초)
PING_TIMEOUT = BINANCE_FUTURE_CONFIG["ping_timeout"]    # 핑 응답 타임아웃 (초)

# REST API 설정 (스냅샷 요청용)
REST_BASE_URL = BINANCE_FUTURE_CONFIG["api_urls"]["depth"]  # REST API 기본 URL

# 오더북 관련 설정
UPDATE_SPEED = BINANCE_FUTURE_CONFIG["update_speed"]  # 웹소켓 업데이트 속도 (100ms, 250ms, 500ms 중 선택)
DEFAULT_DEPTH = BINANCE_FUTURE_CONFIG["default_depth"]     # 기본 오더북 깊이
MAX_RETRY_COUNT = BINANCE_FUTURE_CONFIG["max_retry_count"]     # 최대 재시도 횟수

# 스냅샷 요청 설정
SNAPSHOT_RETRY_DELAY = BINANCE_FUTURE_CONFIG["snapshot_retry_delay"]  # 스냅샷 요청 재시도 초기 딜레이 (초)
SNAPSHOT_REQUEST_TIMEOUT = BINANCE_FUTURE_CONFIG["snapshot_request_timeout"]  # 스냅샷 요청 타임아웃 (초)

# DNS 캐시 설정
DNS_CACHE_TTL = BINANCE_FUTURE_CONFIG["dns_cache_ttl"]  # DNS 캐시 TTL (초)

class BinanceFutureWebSocketConnector(BaseWebsocketConnector):
    """
    바이낸스 선물 웹소켓 연결 관리 클래스
    
    바이낸스 선물 거래소의 웹소켓 연결을 관리하는 클래스입니다.
    
    특징:
    - 바이낸스 선물 전용 핑/퐁 메커니즘 사용
    - 재연결 전략 구현
    - 스트림 URL 동적 생성
    """
    def __init__(self, settings: dict):
        """
        바이낸스 선물 웹소켓 연결 관리자 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, EXCHANGE_CODE)
        
        # 웹소켓 URL 설정
        self.ws_base = WS_BASE_URL
        self.snapshot_base = REST_BASE_URL
        
        # 오더북 관련 설정
        self.update_speed = UPDATE_SPEED
        self.snapshot_depth = settings.get("depth", DEFAULT_DEPTH)
        
        # 연결 관련 설정
        self.subscribed_symbols: set = set()
        self.instance_key: Optional[str] = None
        
        # 실시간 url
        self.wsurl = ""
        
        # Ping/Pong 설정 추가
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        
        # 세션 관리
        self.session = None
        
        # 매니저 설정
        self.orderbook_manager = None

    async def connect(self):
        """
        바이낸스 선물 웹소켓 서버에 연결
        
        설정된 URL로 웹소켓 연결을 수립하고, 연결 상태를 업데이트합니다.
        연결 실패 시 예외를 발생시킵니다.
        """
        try:
            if not self.wsurl:
                raise ValueError("WebSocket URL is not set. Call subscribe() first.")
                
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect_attempt")
                
            self.session = aiohttp.ClientSession()
            self.ws = await connect(
                self.wsurl,
                ping_interval=self.ping_interval,  # 150초
                ping_timeout=self.ping_timeout,    # 10초
                compression=None
            )
            self.is_connected = True
            self.stats.connection_start_time = time.time()
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect")
            self.log_info("웹소켓 연결 성공")
            
            # 텔레그램 알림 전송
            await self._send_telegram_notification("connect", f"{self.exchange_korean_name} 웹소켓 연결 성공")
            
            return True
        except Exception as e:
            self.log_error(f"connect() 예외: {e}", exc_info=True)
            return False

    async def subscribe(self, symbols: List[str]):
        """
        지정된 심볼 목록을 구독
        
        Args:
            symbols: 구독할 심볼 목록
        """
        if not symbols:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "warning")
            return

        # wsurl은 이미 _prepare_start에서 설정됨
        if not self.wsurl:
            self.log_error("WebSocket URL이 설정되지 않았습니다.")
            return
            
        # 구독 상태 콜백
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "subscribe")

        # 심볼 추적
        for sym in symbols:
            self.subscribed_symbols.add(sym.upper())
            
        # 연결 시도
        await self.connect()

    async def request_snapshot(self, symbol: str) -> Optional[dict]:
        """
        REST API를 통해 스냅샷 요청
        
        Args:
            symbol: 스냅샷을 요청할 심볼
            
        Returns:
            Optional[dict]: 스냅샷 데이터 또는 None (요청 실패 시)
        """
        max_retries = MAX_RETRY_COUNT
        retry_delay = SNAPSHOT_RETRY_DELAY
        
        # 심볼 형식 처리
        symbol_upper = symbol.upper()
        if not symbol_upper.endswith("USDT"):
            symbol_upper += "USDT"
        
        for attempt in range(max_retries):
            try:
                url = f"{self.snapshot_base}?symbol={symbol_upper}&limit={self.snapshot_depth}"
                self.log_info(f"스냅샷 요청 URL: {url}")
                
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "snapshot_request")
                
                # TCP 커넥터에 DNS 캐시 설정 추가
                connector = aiohttp.TCPConnector(
                    ttl_dns_cache=DNS_CACHE_TTL,  # DNS 캐시 TTL
                    ssl=False  # SSL 검증 비활성화 (필요한 경우)
                )
                
                async with aiohttp.ClientSession(connector=connector) as sess:
                    async with sess.get(url, timeout=SNAPSHOT_REQUEST_TIMEOUT) as resp:
                        if resp.status == 200:
                            raw_data = await resp.json()
                            return self.parse_snapshot(raw_data, symbol)
                        else:
                            self.log_error(f"{symbol} 스냅샷 status={resp.status}")
                            
            except Exception as e:
                self.log_error(f"스냅샷 요청 실패: {e}", exc_info=True)
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                
        self.log_error(f"{symbol} ME 스냅샷 요청 최대 재시도 횟수 초과")
        return None

    def parse_snapshot(self, data: dict, symbol: str) -> Optional[dict]:
        """
        스냅샷 데이터 파싱
        
        Args:
            data: 원본 스냅샷 데이터
            symbol: 심볼
            
        Returns:
            Optional[dict]: 파싱된 스냅샷 데이터 또는 None (파싱 실패 시)
        """
        try:
            if "lastUpdateId" not in data:
                self.log_error(f"{symbol} 'lastUpdateId' 없음")
                return None

            last_id = data["lastUpdateId"]
            bids = [[float(b[0]), float(b[1])] for b in data.get("bids", []) if float(b[0])>0 and float(b[1])>0]
            asks = [[float(a[0]), float(a[1])] for a in data.get("asks", []) if float(a[0])>0 and float(a[1])>0]

            snapshot = {
                "exchangename": "binancefuture",
                "symbol": symbol,
                "bids": bids,
                "asks": asks,
                "timestamp": int(time.time()*1000),
                "lastUpdateId": last_id,
                "type": "snapshot"
            }
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "snapshot_parsed")
            return snapshot
        except Exception as e:
            self.log_error(f"parse_snapshot() 예외: {e}", exc_info=True)
            return None

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        """
        웹소켓 연결 시작 및 심볼 구독
        
        Args:
            symbols_by_exchange: 거래소별 구독할 심볼 목록
        """
        exchange_symbols = symbols_by_exchange.get(self.exchangename.lower(), [])
        if not exchange_symbols:
            self.log_error("구독할 심볼 없음.")
            return

        # 부모 클래스의 start 메서드 호출
        await super().start(symbols_by_exchange)

    async def _run_message_loop(self, symbols: List[str], tasks: List[asyncio.Task]) -> None:
        """
        메시지 처리 루프 실행 (BaseWebsocketConnector 템플릿 메서드 구현)
        
        Args:
            symbols: 구독한 심볼 목록
            tasks: 실행 중인 백그라운드 태스크 목록
        """
        try:
            while not self.stop_event.is_set() and self.is_connected:
                try:
                    message = await asyncio.wait_for(
                        self.ws.recv(),
                        timeout=self.health_check_interval
                    )
                    self.stats.last_message_time = time.time()
                    self.stats.message_count += 1
                    
                    # 메시지 처리 (자식 클래스에서 구현)
                    await self.process_message(message)

                except asyncio.TimeoutError:
                    # 헬스체크 timeout
                    continue
                except Exception as e:
                    self.log_error(f"메시지 수신 실패: {str(e)}")
                    break

        except Exception as conn_e:
            # 연결 실패 시 백오프
            delay = self.reconnect_strategy.next_delay()
            self.log_error(f"연결 실패: {conn_e}, 재연결 {delay}s 후 재시도")
            await asyncio.sleep(delay)
        finally:
            if self.ws:
                try:
                    await self.ws.close()
                except:
                    pass
            self.is_connected = False
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "disconnect")

    async def process_message(self, message: str) -> None:
        """
        수신된 메시지 처리 (자식 클래스에서 구현)
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        # 이 메서드는 자식 클래스에서 구현해야 함
        pass

    def set_manager(self, manager):
        """
        오더북 매니저 설정
        
        Args:
            manager: 오더북 매니저 객체
        """
        self.orderbook_manager = manager
        self.log_info("오더북 매니저 설정 완료")

    async def _prepare_start(self, symbols: List[str]) -> None:
        """
        시작 전 초기화 및 설정 (BaseWebsocketConnector 템플릿 메서드 구현)
        
        Args:
            symbols: 구독할 심볼 목록
        """
        # 필요한 초기화 작업 수행
        self.log_info(f"바이낸스 선물 웹소켓 초기화 시작 (심볼: {len(symbols)}개)")
        
        # 구독할 심볼 목록 초기화
        self.subscribed_symbols = set()
        
        # 웹소켓 URL 초기화
        streams = []
        for sym in symbols:
            sym_lower = sym.lower()
            if not sym_lower.endswith("usdt"):
                sym_lower += "usdt"
            streams.append(f"{sym_lower}@depth@{self.update_speed}")
            
        combined = "/".join(streams)
        self.wsurl = self.ws_base + combined
        self.log_info(f"웹소켓 URL 설정 완료: {self.wsurl[:50]}...")
        
        # 세션 초기화
        if self.session:
            try:
                await self.session.close()
            except:
                pass
            self.session = None 