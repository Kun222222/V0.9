# file: orderbook/websocket/upbit_websocket.py

import asyncio
import json
import time
from websockets import connect
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG

from crosskimp.ob_collector.orderbook.websocket.base_ws_connector import BaseWebsocketConnector
from crosskimp.ob_collector.orderbook.orderbook.upbit_s_ob import UpbitOrderBookManager


# ============================
# 업비트 웹소켓 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.UPBIT.value  # 거래소 코드
UPBIT_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 업비트 설정

# 웹소켓 연결 설정
WS_URL = UPBIT_CONFIG["ws_url"]  # 웹소켓 URL
PING_INTERVAL = UPBIT_CONFIG["ping_interval"]  # 핑 전송 간격 (초)
PING_TIMEOUT = UPBIT_CONFIG["ping_timeout"]  # 핑 응답 타임아웃 (초)
PONG_RESPONSE = UPBIT_CONFIG["pong_response"]  # 업비트 PONG 응답 형식

# 오더북 관련 설정
DEFAULT_DEPTH = UPBIT_CONFIG["default_depth"]  # 기본 오더북 깊이

@dataclass
class UpbitOrderbookUnit:
    """업비트 오더북 단위 데이터 구조체"""
    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float

class UpbitWebsocket(BaseWebsocketConnector):
    """
    Upbit WebSocket 클라이언트
    
    업비트 거래소의 실시간 오더북 데이터를 수신하고 처리하는 웹소켓 클라이언트입니다.
    
    특징:
    - URL: wss://api.upbit.com/websocket/v1
    - 구독: orderbook (심볼별 "KRW-{symbol}")
    - 핑/퐁: 60초 간격으로 PING 전송, 10초 내 응답 필요
    - 메시지 형식: 전체 오더북 스냅샷 방식 (델타 업데이트 아님)
    """
    def __init__(self, settings: dict):
        """
        업비트 웹소켓 클라이언트 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings, EXCHANGE_CODE)
        self.ws_url = WS_URL
        
        # 오더북 설정
        depth = settings.get("websocket", {}).get("orderbook_depth", DEFAULT_DEPTH)
        self.orderbook_manager = UpbitOrderBookManager(depth=depth)
        
        # 연결 관련 설정
        self.is_connected = False
        self.initialized_symbols = set()  # 초기화된 심볼 추적
        
        # 핑/퐁 설정
        self.last_ping_time = 0
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        
        # 메시지 처리 통계
        self.message_stats = {
            "total_received": 0,
            "valid_parsed": 0,
            "invalid_parsed": 0,
            "ping_pong": 0,
            "processing_errors": 0
        }
        
        # 로깅 설정
        self.log_raw_data = settings.get("logging", {}).get("log_raw_data", True)

    async def connect(self) -> bool:
        """
        업비트 웹소켓 서버에 연결
        
        Returns:
            bool: 연결 성공 여부
        """
        try:
            # 연결 시도 상태 콜백
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect_attempt")
            
            # 웹소켓 연결 수립
            self.ws = await connect(
                self.ws_url,
                ping_interval=None,  # 자체 핑 메커니즘 사용
                compression=None     # 압축 비활성화
            )
            
            # 연결 상태 업데이트
            self.is_connected = True
            self.stats.connection_start_time = time.time()
            
            # 연결 성공 상태 콜백
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "connect")
                
            self.log_info("웹소켓 연결 성공")
            return True
            
        except asyncio.TimeoutError:
            self.log_error("웹소켓 연결 타임아웃")
            return False
        except ConnectionRefusedError:
            self.log_error("웹소켓 연결 거부됨")
            return False
        except Exception as e:
            self.log_error(f"웹소켓 연결 실패: {str(e)}")
            return False

    async def subscribe(self, symbols: List[str]) -> None:
        """
        지정된 심볼에 대한 오더북 구독
        
        Args:
            symbols: 구독할 심볼 목록
        
        Raises:
            ConnectionError: 웹소켓이 연결되지 않은 경우
            Exception: 구독 처리 중 오류 발생
        """
        try:
            # 연결 상태 확인
            if not self.ws or not self.is_connected:
                error_msg = "웹소켓이 연결되지 않음"
                self.log_error(error_msg)
                raise ConnectionError(error_msg)
            
            # 심볼 형식 변환 (KRW-{symbol})
            markets = [f"KRW-{s.upper()}" for s in symbols]
            
            # 구독 메시지 생성
            sub_message = [
                {"ticket": f"upbit_orderbook_{int(time.time())}"},
                {
                    "type": "orderbook",
                    "codes": markets,
                    "isOnlyRealtime": False  # 스냅샷 포함 요청
                }
            ]
            
            # 구독 시작 상태 콜백
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe")
                
            # 구독 메시지 전송
            await self.ws.send(json.dumps(sub_message))
            
            # 구독 완료 상태 콜백
            if self.connection_status_callback:
                self.connection_status_callback(self.exchangename, "subscribe_complete")
                
            self.log_info(f"오더북 구독 완료: {len(symbols)}개 심볼")
            
        except Exception as e:
            error_msg = f"구독 처리 중 오류 발생: {str(e)}"
            self.log_error(error_msg)
            raise

    async def parse_message(self, message: str) -> Optional[dict]:
        """
        수신된 메시지 파싱
        
        Args:
            message: 수신된 웹소켓 메시지
            
        Returns:
            Optional[dict]: 파싱된 오더북 데이터 또는 None
        """
        try:
            # 바이트 메시지 디코딩
            if isinstance(message, bytes):
                message = message.decode("utf-8")
            
            # PING 응답 처리
            if message == PONG_RESPONSE:
                self.message_stats["ping_pong"] += 1
                return None
            
            # JSON 파싱
            data = json.loads(message)
            
            # 오더북 메시지 타입 확인
            if data.get("type") != "orderbook":
                return None

            # 심볼 추출
            symbol = data.get("code", "").replace("KRW-","")
            if not symbol:
                self.log_error("심볼 정보 누락")
                return None

            # 타임스탬프 추출
            ts = data.get("timestamp", int(time.time()*1000))
            
            # 오더북 데이터 추출 및 변환
            bids, asks = self._extract_orderbook_data(data.get("orderbook_units", []), symbol)

            # 유효성 검사
            if not bids or not asks:
                self.message_stats["invalid_parsed"] += 1
                return None

            # 파싱 성공 카운트 증가
            self.message_stats["valid_parsed"] += 1
            
            # 로깅 추가
            parsed_data = {
                "exchangename": EXCHANGE_CODE,
                "symbol": symbol.upper(),
                "bids": bids,
                "asks": asks,
                "timestamp": ts,
                "sequence": ts,
                "type": "delta"  # 업비트는 항상 전체 오더북 제공
            }
            
            # 원본 메시지만 로깅 (parsedData 로깅 제거)
            self.log_raw_message("orderbook", message, symbol.upper())
            
            # 표준화된 오더북 데이터 반환
            return parsed_data
        except json.JSONDecodeError:
            self.log_error(f"JSON 파싱 실패: {message[:100]}...")
            return None
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}, raw={message[:200]}...")
            self.message_stats["processing_errors"] += 1
            return None

    def _extract_orderbook_data(self, units: List[Dict[str, Any]], symbol: str) -> Tuple[List[Dict[str, float]], List[Dict[str, float]]]:
        """
        오더북 유닛에서 매수/매도 데이터 추출
        
        Args:
            units: 오더북 유닛 목록
            symbol: 심볼 이름
            
        Returns:
            Tuple[List[Dict[str, float]], List[Dict[str, float]]]: 매수(bids)와 매도(asks) 데이터
        """
        bids = []
        asks = []
        
        for unit in units:
            # 가격과 수량 추출
            bid_price = float(unit.get("bid_price", 0))
            bid_size = float(unit.get("bid_size", 0))
            ask_price = float(unit.get("ask_price", 0))
            ask_size = float(unit.get("ask_size", 0))
            
            # 유효한 데이터만 추가
            if bid_price > 0:
                bids.append({'price': bid_price, 'size': bid_size})
            if ask_price > 0:
                asks.append({'price': ask_price, 'size': ask_size})

        # 빈 데이터 로깅
        if not bids:
            self.log_error(f"{symbol} bids가 비어 있습니다: {units}")
        if not asks:
            self.log_error(f"{symbol} asks가 비어 있습니다: {units}")
            
        return bids, asks

    def _log_parsed_data(self, symbol: str, parsed: dict) -> None:
        """
        파싱된 데이터 로깅 - 더 이상 사용하지 않음
        
        이 메서드는 더 이상 사용되지 않습니다.
        원본 데이터 로깅은 parse_message 메서드에서 직접 수행합니다.
        
        Args:
            symbol: 심볼명
            parsed: 파싱된 데이터
        """
        # 이 메서드는 더 이상 사용하지 않음
        pass

    async def handle_parsed_message(self, parsed: dict) -> None:
        """
        파싱된 메시지 처리
        
        Args:
            parsed: 파싱된 오더북 데이터
        """
        try:
            symbol = parsed["symbol"]
            
            # 업비트는 매 메시지가 전체 오더북이므로, 항상 스냅샷으로 처리
            result = await self.orderbook_manager.initialize_orderbook(symbol, parsed)
            
            if result.is_valid:
                # 시퀀스 업데이트
                self.orderbook_manager.update_sequence(symbol, parsed["timestamp"])
                
                # 오더북 데이터 가져오기
                ob_dict = self.orderbook_manager.get_orderbook(symbol).to_dict()
                
                # 출력 큐에 추가
                if self.output_queue:
                    await self.output_queue.put((self.exchangename, ob_dict))
            else:
                self.log_error(f"{symbol} 오더북 업데이트 실패: {result.error}")

        except KeyError as e:
            self.log_error(f"필수 필드 누락: {e}")
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {e}")

    async def _send_ping(self) -> None:
        """
        PING 메시지 전송
        
        업비트 서버에 PING 메시지를 전송하여 연결 상태를 유지합니다.
        """
        try:
            if self.ws and self.is_connected:
                # 일반 PING 메시지 전송 방식 사용
                await self.ws.send("PING")
                self.last_ping_time = time.time()
                self.stats.last_ping_time = self.last_ping_time
                self.log_debug("PING 전송")
        except Exception as e:
            self.log_error(f"PING 전송 실패: {str(e)}")

    async def _handle_pong(self, message: str) -> bool:
        """
        PONG 메시지 처리
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: PONG 메시지인 경우 True, 아니면 False
        """
        try:
            if message == PONG_RESPONSE:
                now = time.time()
                self.stats.last_pong_time = now
                
                # 레이턴시 계산 (PING-PONG 간격)
                if self.stats.last_ping_time > 0:
                    self.stats.latency_ms = (now - self.stats.last_ping_time) * 1000
                
                self.log_debug(f"PONG 수신 (레이턴시: {self.stats.latency_ms:.2f}ms)")
                return True
            return False
        except Exception as e:
            self.log_error(f"PONG 처리 실패: {str(e)}")
            return False

    async def _message_loop(self, symbols: List[str]) -> None:
        """
        메시지 수신 및 처리 루프
        
        Args:
            symbols: 구독 중인 심볼 목록
        """
        while not self.stop_event.is_set():
            try:
                # PING 전송 체크
                now = time.time()
                if now - self.last_ping_time >= self.ping_interval:
                    await self._send_ping()

                # 메시지 수신
                message = await asyncio.wait_for(
                    self.ws.recv(),
                    timeout=self.ping_timeout
                )
                
                # 메시지 수신 통계 업데이트
                self.message_stats["total_received"] += 1
                self.stats.last_message_time = time.time()
                self.stats.message_count += 1
                
                # PONG 메시지 체크
                if await self._handle_pong(message):
                    continue

                # 메시지 파싱 및 처리
                parsed = await self.parse_message(message)
                if parsed:
                    await self.handle_parsed_message(parsed)
                    if self.connection_status_callback:
                        self.connection_status_callback(self.exchangename, "message")

            except asyncio.TimeoutError:
                # PONG 응답 체크
                if time.time() - self.stats.last_pong_time > self.ping_timeout:
                    self.log_error("PONG 응답 타임아웃")
                    break
                continue
            except Exception as e:
                self.log_error(f"메시지 처리 실패: {str(e)}")
                break

    async def start(self, symbols_by_exchange: Dict[str, List[str]]) -> None:
        """
        웹소켓 연결 시작 및 메시지 처리
        
        Args:
            symbols_by_exchange: 거래소별 구독할 심볼 목록
        """
        # 중지 이벤트 초기화
        self.stop_event.clear()
        
        while not self.stop_event.is_set():
            try:
                # 심볼 목록 가져오기
                symbols = symbols_by_exchange.get(self.exchangename.lower(), [])
                if not symbols:
                    self.log_error("구독할 심볼 없음")
                    return

                # 공통 로깅을 위한 부모 클래스 start 호출
                await super().start(symbols_by_exchange)

                # 연결
                connected = await self.connect()
                if not connected:
                    delay = self.reconnect_strategy.next_delay()
                    self.log_error(f"연결 실패, {delay}초 후 재시도")
                    await asyncio.sleep(delay)
                    continue

                # 구독
                await self.subscribe(symbols)

                # 메시지 처리 루프
                await self._message_loop(symbols)

            except Exception as e:
                delay = self.reconnect_strategy.next_delay()
                self.log_error(f"연결 실패: {str(e)}, {delay}초 후 재연결")
                await asyncio.sleep(delay)
                continue

            finally:
                # 연결 종료 처리
                await self._cleanup_connection()

    async def _cleanup_connection(self) -> None:
        """연결 종료 및 정리"""
        if self.ws:
            try:
                await self.ws.close()
            except Exception as e:
                self.log_error(f"웹소켓 종료 실패: {e}")

        self.is_connected = False
        
        # 연결 종료 상태 콜백
        if self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "disconnect")

    async def stop(self) -> None:
        """
        웹소켓 연결 종료
        """
        self.log_info("웹소켓 연결 종료 중...")
        await super().stop()
        
        # 오더북 데이터 정리
        self.orderbook_manager.clear_all()
        
        self.log_info("웹소켓 연결 종료 완료")
        
        # 통계 로깅
        self.log_info(
            f"웹소켓 종료 | "
            f"총 메시지: {self.message_stats['total_received']}개, "
            f"유효 파싱: {self.message_stats['valid_parsed']}개, "
            f"핑퐁: {self.message_stats['ping_pong']}개, "
            f"오류: {self.message_stats['processing_errors']}개"
        )