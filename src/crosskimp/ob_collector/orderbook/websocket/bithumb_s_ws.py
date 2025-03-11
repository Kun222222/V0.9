# file: orderbook/websocket/bithumb_spot_websocket.py

import asyncio
import json
import time
from websockets import connect
from typing import Dict, List, Optional

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.orderbook.websocket.base_ws_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.orderbook.bithumb_s_ob import BithumbSpotOrderBookManager
from crosskimp.ob_collector.utils.config.constants import Exchange, WEBSOCKET_URLS, WEBSOCKET_CONFIG, EXCHANGE_NAMES_KR

# ============================
# 빗썸 웹소켓 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BITHUMB.value  # 거래소 코드
EXCHANGE_KR = EXCHANGE_NAMES_KR[EXCHANGE_CODE]  # 거래소 한글 이름

# 웹소켓 연결 설정
WS_URL = WEBSOCKET_URLS[EXCHANGE_CODE]  # 웹소켓 URL
PING_INTERVAL = 30  # 핑 전송 간격 (초)
PING_TIMEOUT = 10   # 핑 응답 타임아웃 (초)

# 오더북 관련 설정
DEFAULT_DEPTH = 15  # 기본 오더북 깊이
SYMBOL_SUFFIX = "_KRW"  # 심볼 접미사

# 빗썸 특화 설정
BITHUMB_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 빗썸 설정

# 로거 설정
logger = get_unified_logger()

def parse_bithumb_depth_update(msg_data: dict) -> Optional[dict]:
    """
    빗썸 웹소켓의 orderbookdepth 메시지를 공통 포맷으로 변환 (바이낸스 현물의 parse_binance_depth_update와 유사)
    
    Args:
        msg_data: 빗썸 웹소켓에서 수신한 원본 메시지
        
    Returns:
        dict: 공통 포맷으로 변환된 오더북 데이터 또는 None (변환 실패 시)
    """
    # 메시지 타입 확인
    if msg_data.get("type") != BITHUMB_CONFIG["message_type_depth"]:
        return None
    
    # 컨텐츠 및 주문 목록 추출
    content = msg_data.get("content", {})
    order_list = content.get("list", [])
    if not order_list:
        return None

    # 심볼 추출 및 변환
    raw_symbol = order_list[0].get("symbol", "")
    symbol = raw_symbol.replace(BITHUMB_CONFIG["symbol_suffix"], "").upper()

    # 타임스탬프 추출
    dt_str = content.get("datetime", str(int(time.time() * 1000)))
    try:
        dt_val = int(dt_str)
    except ValueError:
        dt_val = int(time.time() * 1000)

    # 호가 데이터 파싱: 숫자형 변환 및 0 필터링
    bids = []  # 매수 호가
    asks = []  # 매도 호가
    for order in order_list:
        otype = order.get("orderType", "").lower()
        try:
            price = float(order["price"])
            qty = float(order["quantity"])
            if price <= 0 or qty <= 0:  # 유효하지 않은 가격/수량 필터링
                continue
            if otype == "bid":
                bids.append([price, qty])
            elif otype == "ask":
                asks.append([price, qty])
        except Exception:
            continue

    # 호가 정렬
    bids.sort(key=lambda x: x[0], reverse=True)  # 매수 호가는 내림차순
    asks.sort(key=lambda x: x[0])                # 매도 호가는 오름차순

    # 공통 포맷으로 변환하여 반환
    return {
        "exchangename": EXCHANGE_CODE,
        "symbol": symbol,
        "bids": bids,
        "asks": asks,
        "timestamp": dt_val,
        "sequence": dt_val,  # 빗썸은 timestamp를 sequence로 사용
        "type": "delta"
    }

class BithumbSpotWebsocket(BaseWebsocketConnector):
    """
    빗썸 현물 웹소켓 클라이언트 (바이낸스 현물과 동일한 데이터 흐름)
    - 연결, 구독, 스냅샷 요청, 델타 메시지 수신 및 처리
    """
    def __init__(self, settings: dict):
        super().__init__(settings, EXCHANGE_CODE)
        self.ws_url = WS_URL  # 웹소켓 URL
        self.depth = settings.get("depth", DEFAULT_DEPTH)  # 내부 depth 설정
        self.manager = BithumbSpotOrderBookManager(depth=self.depth)
        self.subscribed_symbols: List[str] = []
        self.ws = None
        
        # 재연결 관련 속성 추가
        self.reconnect_count = 0
        self.last_reconnect_time = 0
        
        # raw 로거 초기화 제거
        # self.raw_logger = get_raw_logger(EXCHANGE_CODE)  # 중앙화된 로깅 사용을 위해 제거
        self.logger = logger

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        super().set_output_queue(queue)  # 부모 클래스의 메서드 호출 (기본 큐 설정 및 로깅)
        self.manager.output_queue = queue  # 오더북 매니저 큐 설정

    async def connect(self):
        """
        빗썸 웹소켓 서버에 연결
        
        설정된 URL로 웹소켓 연결을 수립하고, 연결 상태를 업데이트합니다.
        연결 실패 시 예외를 발생시킵니다.
        """
        try:
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect_attempt")
                
            # 웹소켓 연결 수립
            self.ws = await connect(
                self.ws_url,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
                close_timeout=BITHUMB_CONFIG["close_timeout"],
                max_size=BITHUMB_CONFIG["max_size"],
                compression=BITHUMB_CONFIG["compression"]
            )
            self.is_connected = True
            self.stats.connection_start_time = time.time()
            
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect")
                
            self.logger.info(f"{EXCHANGE_KR} 웹소켓 연결 성공")
            return True
                
        except Exception as e:
            self.log_error(f"{EXCHANGE_KR} 웹소켓 연결 중 예외 발생: {e}", exc_info=True)
            return False

    async def subscribe(self, symbols: List[str]):
        """
        지정된 심볼 목록을 구독
        
        1. 각 심볼별로 REST API를 통해 스냅샷 요청
        2. 스냅샷을 오더북 매니저에 적용
        3. 웹소켓을 통해 실시간 업데이트 구독
        
        Args:
            symbols: 구독할 심볼 목록
        """
        if not symbols:
            self.log_error(f"{EXCHANGE_KR} 구독할 심볼이 없음")
            return

        # 구독 심볼 목록 초기화
        self.subscribed_symbols = []

        # 각 심볼별로 스냅샷 요청 및 오더북 초기화를 수행
        for sym in symbols:
            try:
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "snapshot_request")
                # REST API를 통해 스냅샷 요청
                snap = await self.manager.fetch_snapshot(sym)
                if snap:
                    # 스냅샷을 오더북 매니저에 적용
                    init_res = await self.manager.initialize_orderbook(sym, snap)
                    if not init_res.is_valid:
                        self.log_error(f"{EXCHANGE_KR} {sym} 스냅샷 적용 실패: {init_res.error_messages}")
                    else:
                        if self.connection_status_callback:
                            self.connection_status_callback(self.exchangename, "snapshot_received")
                        self.subscribed_symbols.append(sym.upper())
                else:
                    self.log_error(f"{EXCHANGE_KR} {sym} 스냅샷 요청 실패")
            except Exception as e:
                self.log_error(f"{EXCHANGE_KR} {sym} 스냅샷 요청 중 오류: {str(e)}")

        # 구독 메시지 전송
        if self.subscribed_symbols and self.ws and self.is_connected:
            try:
                # 빗썸 형식으로 심볼 변환 (예: BTC -> BTC_KRW)
                symbol_list = [f"{s.upper()}{BITHUMB_CONFIG['symbol_suffix']}" for s in self.subscribed_symbols]
                # 구독 메시지 구성
                sub_msg = {
                    "type": BITHUMB_CONFIG["message_type_depth"],
                    "symbols": symbol_list,
                    "tickTypes": BITHUMB_CONFIG["tick_types"]
                }
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe")
                # 웹소켓을 통해 구독 메시지 전송
                await self.ws.send(json.dumps(sub_msg))
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe_complete")
                
                self.logger.info(f"{EXCHANGE_KR} 구독 완료: {len(self.subscribed_symbols)}개 심볼")
            except Exception as e:
                self.log_error(f"{EXCHANGE_KR} 구독 메시지 전송 중 오류: {str(e)}")
                # 연결이 끊어진 경우 재연결 필요
                self.is_connected = False
                raise

    async def parse_message(self, message: str) -> Optional[dict]:
        """
        빗썸 특화 파싱 로직
        
        Args:
            message: 웹소켓에서 수신한 원본 메시지 문자열
            
        Returns:
            Optional[dict]: 파싱된 메시지 데이터 또는 None (파싱 실패 시)
        """
        try:
            # 문자열 메시지를 JSON으로 파싱
            data = json.loads(message)
            
            # 구독 응답 처리
            if "status" in data and "resmsg" in data:
                if data["status"] == "0000":  # 성공 상태 코드
                    if self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "subscribe_response")
                    self.logger.info(f"{EXCHANGE_KR} 구독 응답 성공: {data['resmsg']}")
                else:  # 실패 상태 코드
                    self.log_error(f"{EXCHANGE_KR} 구독 실패: {data['resmsg']}")
                return None

            # Delta 메시지 처리 (오더북 업데이트)
            if data.get("type") == BITHUMB_CONFIG["message_type_depth"]:
                # 로깅 추가
                symbol = self._extract_symbol(data)
                if symbol != "UNKNOWN":
                    self.log_raw_message("depthUpdate", json.dumps(data), symbol)
                return data

            return None
        except json.JSONDecodeError as e:
            self.log_error(f"{EXCHANGE_KR} JSON 파싱 오류: {e}, 메시지: {message[:100]}...")
            return None
        except Exception as e:
            self.log_error(f"{EXCHANGE_KR} 메시지 파싱 중 예외 발생: {e}", exc_info=True)
            return None

    def _extract_symbol(self, parsed: dict) -> str:
        """
        빗썸 특화 심볼 추출
        
        Args:
            parsed: 파싱된 메시지 데이터
            
        Returns:
            str: 추출된 심볼명 (KRW 접미사 제거 및 대문자 변환)
        """
        try:
            content = parsed.get("content", {})
            order_list = content.get("list", [])
            if order_list:
                raw_symbol = order_list[0].get("symbol", "")
                return raw_symbol.replace(BITHUMB_CONFIG["symbol_suffix"], "").upper()
        except Exception as e:
            self.log_error(f"{EXCHANGE_KR} 심볼 추출 중 오류: {str(e)}")
        return "UNKNOWN"

    async def handle_parsed_message(self, parsed: dict) -> None:
        """
        파싱된 메시지 처리
        
        Args:
            parsed: 파싱된 메시지 데이터
        """
        try:
            # 메시지를 공통 포맷으로 변환
            evt = parse_bithumb_depth_update(parsed)
            if evt:
                symbol = evt["symbol"]
                # 오더북 매니저를 통해 업데이트 수행
                res = await self.manager.update(symbol, evt)
                if not res.is_valid:
                    self.log_error(f"{EXCHANGE_KR} {symbol} 업데이트 실패: {res.error_messages}")
                elif self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "message")
        except Exception as e:
            self.log_error(f"{EXCHANGE_KR} handle_parsed_message 예외: {e}", exc_info=True)

    async def _run_message_loop(self, symbols: List[str], tasks: List[asyncio.Task]) -> None:
        """
        메시지 처리 루프 실행 (부모 클래스의 메서드 오버라이드)
        
        Args:
            symbols: 구독한 심볼 목록
            tasks: 실행 중인 백그라운드 태스크 목록
        """
        # 메인 연결 및 메시지 처리 루프
        while not self.stop_event.is_set():
            try:
                # 연결 상태 확인 및 연결 시도
                if not self.is_connected:
                    connected = await self.connect()
                    if not connected:
                        # 연결 실패 시 지수 백오프 적용
                        delay = self.reconnect_strategy.next_delay()
                        self.log_error(f"{EXCHANGE_KR} 연결 실패, {delay}초 후 재시도")
                        await asyncio.sleep(delay)
                        continue
                
                # 연결 직후 last_message_time 초기화
                self.stats.last_message_time = time.time()

                # 심볼 구독 (부모 클래스의 start에서 이미 호출되었지만, 재연결 시 다시 호출 필요)
                try:
                    await self.subscribe(symbols)
                except Exception as sub_e:
                    self.log_error(f"{EXCHANGE_KR} 구독 실패: {str(sub_e)}")
                    # 구독 실패 시 연결 재설정
                    self.is_connected = False
                    continue

                # 메시지 수신 루프
                last_message_time = time.time()
                while not self.stop_event.is_set() and self.is_connected:
                    try:
                        # 웹소켓 연결 상태 확인
                        if not self.ws:
                            self.log_error(f"{EXCHANGE_KR} 웹소켓 객체가 None입니다. 재연결이 필요합니다.")
                            self.is_connected = False
                            break
                            
                        # 메시지 수신
                        message = await asyncio.wait_for(
                            self.ws.recv(), 
                            timeout=BITHUMB_CONFIG["message_timeout"]
                        )
                        
                        # 메시지 지연 측정
                        current_time = time.time()
                        message_delay = current_time - last_message_time
                        last_message_time = current_time
                        
                        # 메시지 지연이 30초 이상이면 연결 재설정
                        if message_delay > BITHUMB_CONFIG["message_timeout"]:
                            self.log_error(f"{EXCHANGE_KR} 높은 메시지 지연 감지: {message_delay:.1f}초 -> 연결 재설정")
                            self.is_connected = False
                            break

                        # 통계 업데이트
                        self.stats.last_message_time = current_time
                        self.stats.message_count += 1
                        self.stats.total_messages += 1

                        # 디버깅을 위해 메시지 내용 로깅
                        if self.stats.message_count <= BITHUMB_CONFIG["log_sample_count"]:
                            self.logger.debug(f"{EXCHANGE_KR} 수신된 메시지 샘플 (#{self.stats.message_count}): {message[:BITHUMB_CONFIG['log_message_preview_length']]}...")

                        # 메시지 파싱 및 처리
                        parsed = await self.parse_message(message)
                        if parsed:
                            await self.handle_parsed_message(parsed)

                    except asyncio.TimeoutError:
                        # 타임아웃 발생 시 연결 상태 확인
                        current_time = time.time()
                        if current_time - last_message_time > BITHUMB_CONFIG["message_timeout"]:
                            self.log_error(f"{EXCHANGE_KR} 메시지 수신 타임아웃: {current_time - last_message_time:.1f}초 -> 연결 재설정")
                            self.is_connected = False
                            break
                        continue
                    except Exception as e:
                        self.log_error(f"{EXCHANGE_KR} 메시지 수신 오류: {e}", exc_info=True)
                        self.is_connected = False
                        break

                # 연결이 끊어진 경우 재연결 대기
                if not self.is_connected and not self.stop_event.is_set():
                    # 재연결 카운트 증가 및 시간 기록
                    self.reconnect_count += 1
                    self.last_reconnect_time = time.time()
                    
                    # 재연결 지연 계산
                    delay = self.reconnect_strategy.next_delay()
                    self.logger.info(f"{EXCHANGE_KR} {delay:.1f}초 후 재연결 시도 (#{self.reconnect_count})")
                    
                    # 연결 종료 콜백 호출
                    if self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "disconnect")
                    
                    # 웹소켓 객체 정리
                    if self.ws:
                        try:
                            await self.ws.close()
                        except Exception:
                            pass
                        self.ws = None
                    
                    # 재연결 대기
                    await asyncio.sleep(delay)

            except Exception as e:
                self.log_error(f"{EXCHANGE_KR} 연결 루프 오류: {str(e)}", exc_info=True)
                
                # 재연결 지연
                delay = self.reconnect_strategy.next_delay()
                self.logger.info(f"{EXCHANGE_KR} {delay:.1f}초 후 재연결 시도...")
                
                # 연결 상태 초기화
                self.is_connected = False
                if self.ws:
                    try:
                        await self.ws.close()
                    except Exception:
                        pass
                    self.ws = None
                
                await asyncio.sleep(delay)

    async def stop(self) -> None:
        """
        웹소켓 연결 종료
        """
        self.logger.info(f"{EXCHANGE_KR} 웹소켓 연결 종료 중...")
        await super().stop()
        self.logger.info(f"{EXCHANGE_KR} 웹소켓 연결 종료 완료")