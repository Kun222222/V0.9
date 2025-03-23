"""
바이낸스 현물 구독 클래스

이 모듈은 바이낸스 현물 거래소의 웹소켓 구독을 담당하는 클래스를 제공합니다.
바이낸스 공식 API 문서를 기반으로 구현되었으며, 오더북 데이터의 정합성을 유지하는 방법을 포함합니다.

참고 문서:
- https://developers.binance.com/docs/binance-spot-api-docs/web-socket-api
- https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
"""

import json
import asyncio
import time
import datetime
from typing import Dict, List, Optional, Any, Union
import aiohttp

from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.common.events.domains.orderbook import OrderbookEventTypes
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
from crosskimp.config.constants_v3 import Exchange
from crosskimp.system_manager.error_manager import ErrorSeverity, ErrorCategory

# 바이낸스 현물 웹소켓 및 REST API 설정
WS_URL = "wss://stream.binance.com:9443/ws"  # 웹소켓 URL (포트 9443 명시)
REST_URL = "https://api.binance.com/api/v3/depth"  # 스냅샷 REST API URL
DEFAULT_DEPTH = 100  # 기본 오더북 깊이
UPDATE_SPEED = "100ms"  # 업데이트 속도 (100ms, 250ms, 500ms 중 선택)

# 로깅 설정
ENABLE_RAW_LOGGING = True  # raw 데이터 로깅 활성화 여부
ENABLE_ORDERBOOK_LOGGING = True  # 오더북 데이터 로깅 활성화 여부

class BinanceSubscription(BaseSubscription):
    """
    바이낸스 현물 구독 클래스
    
    바이낸스 현물 거래소의 웹소켓 구독을 담당하는 클래스입니다.
    
    특징:
    - 증분 업데이트 구독 및 스냅샷 요청
    - REST API를 통한 초기 스냅샷 요청
    - 시퀀스 넘버 기반 데이터 정합성 검증
    - 바이낸스 공식 문서에 따른 오더북 관리 구현
    """
    
    # 1. 초기화 단계
    def __init__(self, connection):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
        """
        # 부모 클래스 초기화 (exchange_code 전달)
        super().__init__(connection, Exchange.BINANCE.value)
        
        # 거래소 이름 설정
        self.exchange_name = Exchange.BINANCE.value
        
        # 오더북 관련 설정
        self.depth_level = DEFAULT_DEPTH
        self.update_speed = UPDATE_SPEED
        
        # 로깅 설정
        self.raw_logging_enabled = ENABLE_RAW_LOGGING
        self.orderbook_logging_enabled = ENABLE_ORDERBOOK_LOGGING
        
        # 오더북 데이터 저장소
        self.orderbooks = {}  # symbol -> orderbook_data
        self.last_update_ids = {}  # symbol -> lastUpdateId
        
        # 스냅샷 요청 관련 설정
        self.snapshot_requests = {}  # symbol -> {"requested": bool, "time": timestamp}
        self.max_retry_count = 3  # 최대 재시도 횟수
        
        # 처리 대기 중인 델타 메시지 큐
        self.buffered_messages = {}  # symbol -> [messages]
        
        # ping-pong 관련 설정
        self.ping_id = None  # ping 요청 ID 초기화

    # 3. 구독 처리 단계
    async def create_subscribe_message(self, symbols: Union[str, List[str]]) -> Dict:
        """
        구독 메시지 생성
        
        Args:
            symbols: 구독할 심볼 또는 심볼 리스트
            
        Returns:
            Dict: 구독 메시지
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        
        # 구독 토픽 생성 (depth 스트림, 속도 지정)
        params = []
        for symbol in symbols:
            # 바이낸스 현물은 소문자 심볼 사용, 대문자 또는 소문자로 들어올 수 있음
            # 현물에서는 USDT 직접 추가
            formatted_symbol = symbol.lower()
            if not formatted_symbol.endswith('usdt'):
                formatted_symbol += 'usdt'
            
            # @depth@100ms 형식으로 스트림 이름 생성
            stream = f"{formatted_symbol}@depth@{self.update_speed}"
            params.append(stream)
            self.log_debug(f"구독 토픽 생성: {stream} (원본 심볼: {symbol})")
        
        # 구독 메시지 형식
        return {
            "method": "SUBSCRIBE",
            "params": params,
            "id": int(time.time() * 1000)  # 고유 요청 ID
        }

    async def _send_subscription_requests(self, symbols: List[str]) -> bool:
        """
        구독 요청 전송
        
        Args:
            symbols: 구독할 심볼 리스트
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            # 구독 메시지 생성
            msg = await self.create_subscribe_message(symbols)
            json_msg = json.dumps(msg)
            
            # 메시지 전송
            success = await self.send_message(json_msg)
            
            if not success:
                self.log_error(f"구독 메시지 전송 실패: {symbols[:3]}...")
                return False
            
            self.log_info(f"구독 메시지 전송 성공: {symbols}")
            
            # 스냅샷 요청 플래그 초기화
            for symbol in symbols:
                self.snapshot_requests[symbol] = {
                    "requested": False,
                    "time": None
                }
                # 메시지 버퍼 초기화
                self.buffered_messages[symbol] = []
            
            # 각 심볼별로 REST API를 통해 스냅샷 요청
            for symbol in symbols:
                asyncio.create_task(self.request_snapshot(symbol))
            
            return True
        except Exception as e:
            self.log_error(f"구독 메시지 전송 중 오류: {str(e)}")
            return False
    
    async def subscribe(self, symbol):
        """
        심볼 구독
        
        단일 심볼 또는 심볼 리스트를 구독합니다.
        
        Args:
            symbol: 구독할 심볼 또는 심볼 리스트
            
        Returns:
            bool: 구독 성공 여부
        """
        try:
            # 심볼 전처리
            symbols = await self._preprocess_symbols(symbol)
            if not symbols:
                return False
            
            # 구독 상태 업데이트
            for sym in symbols:
                self.subscribed_symbols[sym] = True
            
            # 웹소켓 연결 확보
            if not await self._ensure_websocket():
                self.log_error("웹소켓 연결이 없어 구독 실패")
                return False
            
            # 구독 요청 전송
            if not await self._send_subscription_requests(symbols):
                raise Exception("구독 메시지 전송 실패")
            
            # 메시지 수신 루프 시작
            if not self.message_loop_task or self.message_loop_task.done():
                self.log_info("메시지 수신 루프 시작")
                self.stop_event.clear()
                self.message_loop_task = asyncio.create_task(self.message_loop())
            
            # 구독 이벤트 발행
            try:
                await self.event_handler.handle_subscription_status(status="subscribed", symbols=symbols)
            except Exception as e:
                self.log_warning(f"구독 상태 이벤트 발행 실패: {e}")
            
            return True
            
        except Exception as e:
            self.log_error(f"구독 중 오류 발생: {str(e)}")
            self._update_metrics("error_count")
            return False

    # 4. 스냅샷 요청 및 처리
    async def request_snapshot(self, symbol: str) -> bool:
        """
        REST API를 통해 오더북 스냅샷 요청
        
        Args:
            symbol: 스냅샷을 요청할 심볼
            
        Returns:
            bool: 요청 성공 여부
        """
        try:
            # 스냅샷 요청 플래그 설정
            self.snapshot_requests[symbol] = {
                "requested": True,
                "time": time.time()
            }
            
            # 심볼 형식 변환 (BTC -> BTCUSDT)
            market = f"{symbol.upper()}USDT"
            
            # 스냅샷 URL 생성
            url = f"{REST_URL}?symbol={market}&limit={self.depth_level}"
            self.log_info(f"{symbol} 스냅샷 요청: {url}")
            
            # REST API 요청
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        # 응답 JSON 파싱
                        data = await response.json()
                        
                        # 스냅샷 처리
                        await self.process_snapshot(symbol, data)
                        return True
                    else:
                        error_text = await response.text()
                        self.log_error(f"{symbol} 스냅샷 요청 실패 ({response.status}): {error_text}")
                        return False
                        
        except Exception as e:
            self.log_error(f"{symbol} 스냅샷 요청 중 오류: {str(e)}")
            return False

    async def process_snapshot(self, symbol: str, data: Dict) -> None:
        """
        스냅샷 데이터 처리
        
        Args:
            symbol: 심볼 이름
            data: 스냅샷 데이터
        """
        try:
            # 타임스탬프 및 시퀀스 추출
            timestamp = data.get("lastUpdateId", int(time.time() * 1000))  # 타임스탬프 (현물은 E가 없음)
            last_update_id = data.get("lastUpdateId")  # 바이낸스 시퀀스 번호
            
            if not last_update_id:
                self.log_error(f"[{symbol}] 스냅샷 데이터에 시퀀스 번호 없음")
                return
                
            # 매수/매도 호가 추출
            bids_data = data.get("bids", [])
            asks_data = data.get("asks", [])
            
            # 오더북 객체 초기화
            orderbook = {
                "bids": {},
                "asks": {},
                "timestamp": timestamp,
                "sequence": last_update_id
            }
            
            # 매수 호가 처리
            for bid in bids_data:
                price = float(bid[0])
                size = float(bid[1])
                if size > 0:
                    orderbook["bids"][price] = size
            
            # 매도 호가 처리
            for ask in asks_data:
                price = float(ask[0])
                size = float(ask[1])
                if size > 0:
                    orderbook["asks"][price] = size
            
            # 오더북 저장
            self.orderbooks[symbol] = orderbook
            self.last_update_ids[symbol] = last_update_id
            
            # 정렬된 전체 오더북 구성
            sorted_bids = sorted(orderbook["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:self.depth_level]
            sorted_asks = sorted(orderbook["asks"].items(), key=lambda x: float(x[0]))[:self.depth_level]
            
            # 배열 형태로 변환 [price, size]
            bids_array = [[price, size] for price, size in sorted_bids]
            asks_array = [[price, size] for price, size in sorted_asks]
            
            # 완전한 오더북 데이터 구성
            snapshot_data = {
                "symbol": symbol,
                "bids": bids_array,
                "asks": asks_array,
                "timestamp": timestamp,
                "sequence": last_update_id
            }
            
            # 로깅
            self.log_info(f"[{symbol}] 스냅샷 초기화 완료 | lastUpdateId: {last_update_id}, 매수: {len(bids_data)}건, 매도: {len(asks_data)}건")
            
            # 오더북 데이터 로깅
            if self.orderbook_logging_enabled:
                self.log_orderbook_data(symbol, snapshot_data)
                
            # 이벤트 발행 (스냅샷)
            self.publish_event(symbol, snapshot_data, "snapshot")
            
            # 버퍼된 메시지 처리
            await self._process_buffered_messages(symbol)
            
        except Exception as e:
            self.log_error(f"[{symbol}] 스냅샷 처리 중 오류: {str(e)}")
            await self.event_handler.handle_error(
                error_type="snapshot_error",
                message=f"스냅샷 처리 실패: {str(e)}",
                symbol=symbol
            )
    
    async def _process_buffered_messages(self, symbol: str) -> None:
        """
        버퍼된 메시지 처리
        
        Args:
            symbol: 심볼 이름
        """
        if symbol not in self.buffered_messages or not self.buffered_messages[symbol]:
            return
            
        buffered = self.buffered_messages[symbol]
        self.log_info(f"[{symbol}] 버퍼된 메시지 처리 시작: {len(buffered)}개")
        
        # 버퍼 클리어
        self.buffered_messages[symbol] = []
        
        # 시퀀스로 정렬
        sorted_messages = sorted(buffered, key=lambda x: x.get("sequence", 0))
        
        # 버퍼된 메시지 순차 처리
        for message in sorted_messages:
            await self._process_delta(symbol, message)
            
        self.log_info(f"[{symbol}] 버퍼된 메시지 처리 완료")

    # 5. 메시지 수신 및 처리 단계
    def is_snapshot_message(self, message: str) -> bool:
        """
        메시지가 스냅샷인지 확인 (웹소켓에서는 스냅샷이 오지 않음)
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 스냅샷 메시지인 경우 True
        """
        # 바이낸스 현물은 웹소켓으로 스냅샷을 전송하지 않음
        return False
    
    def is_delta_message(self, message: str) -> bool:
        """
        메시지가 델타인지 확인
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 델타 메시지인 경우 True
        """
        try:
            data = self._parse_json_message(message)
            if not data:
                return False
                
            # 이벤트 타입이 depthUpdate인지 확인
            return data.get("e") == "depthUpdate"
            
        except Exception as e:
            self.log_error(f"델타 메시지 확인 중 오류: {str(e)}")
            return False

    def _parse_json_message(self, message: str):
        """JSON 메시지를 파싱합니다"""
        try:
            data = json.loads(message)
            
            # ping-pong 응답 처리
            if "result" in data and data.get("id") == self.ping_id:
                self.last_pong_time = time.time()
                return None
                
            return data
        except json.JSONDecodeError:
            self.log_error(f"JSON 디코딩 실패: {message[:100]}")
            return None
    
    def _parse_delta_message(self, symbol: str, data: dict) -> dict:
        """
        델타 메시지를 파싱합니다
        
        Args:
            symbol: 심볼 이름
            data: 원본 메시지 데이터
            
        Returns:
            파싱된 데이터
        """
        try:
            # 결과를 담을 딕셔너리
            result = {
                "symbol": symbol,
                "exchange": self.exchange_name,
                "bids": [],
                "asks": [],
                "sequence": data.get("u", 0),  # 마지막 업데이트 ID
                "U": data.get("U", 0),         # 첫 업데이트 ID - 정합성 검증에 필요
                "timestamp": data.get("E", int(time.time() * 1000))  # 이벤트 시간
            }
            
            # 호가 변경 추출
            if "b" in data and isinstance(data["b"], list):
                for bid in data["b"]:
                    if len(bid) >= 2:
                        price = float(bid[0])
                        quantity = float(bid[1])
                        result["bids"].append([price, quantity])
                        
            if "a" in data and isinstance(data["a"], list):
                for ask in data["a"]:
                    if len(ask) >= 2:
                        price = float(ask[0])
                        quantity = float(ask[1])
                        result["asks"].append([price, quantity])
            
            return result
        except Exception as e:
            self.log_error(f"델타 메시지 파싱 실패: {str(e)}, 데이터: {data}")
            return None
            
    async def _process_delta(self, symbol: str, data: dict) -> None:
        """
        오더북 델타 업데이트를 처리합니다
        
        Args:
            symbol: 심볼 이름
            data: 파싱된 델타 데이터
        """
        try:
            # 타임스탬프 및 시퀀스 추출
            timestamp = data.get("timestamp")
            sequence = data.get("sequence")  # u(마지막 업데이트 ID)
            first_update_id = data.get("U", 0)  # 첫 번째 업데이트 ID
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            
            # 오더북이 없으면 스냅샷 요청 필요
            if symbol not in self.orderbooks:
                self.log_debug(f"[{symbol}] 오더북 초기화 필요, 스냅샷 요청 대기")
                if symbol not in self.buffered_messages:
                    self.buffered_messages[symbol] = []
                # 버퍼에 메시지 저장하고 나중에 처리
                self.buffered_messages[symbol].append(data)
                # 스냅샷 요청
                await self.request_snapshot(symbol)
                return
            
            # 현물 시장에서 바이낸스의 데이터 정합성 유지 로직 (문서 기반)
            # 1. 이벤트의 U <= lastUpdateId+1 && u >= lastUpdateId+1 인 경우에만 처리
            # 2. U > lastUpdateId+1 이면 스냅샷을 다시 요청해야 함
            # 3. u < lastUpdateId 이면 이벤트 무시
            last_update_id = self.last_update_ids.get(symbol, 0)
            
            # 데이터 정합성 검증
            if first_update_id > last_update_id + 1:
                self.log_warning(f"[{symbol}] 업데이트 누락 감지: 첫 ID {first_update_id} > 마지막 ID+1 {last_update_id+1}")
                # 스냅샷 다시 요청
                await self.request_snapshot(symbol)
                return
                
            if sequence < last_update_id:
                self.log_debug(f"[{symbol}] 이전 시퀀스 메시지 무시: {sequence} < {last_update_id}")
                return
                
            # 기존 오더북 가져오기
            orderbook = self.orderbooks[symbol]
            
            # 매수 호가 업데이트
            for bid in bids:
                price, size = float(bid[0]), float(bid[1])
                if size == 0:
                    # 수량이 0이면 해당 가격 삭제
                    if price in orderbook["bids"]:
                        del orderbook["bids"][price]
                else:
                    # 수량이 있으면 추가/업데이트
                    orderbook["bids"][price] = size
            
            # 매도 호가 업데이트
            for ask in asks:
                price, size = float(ask[0]), float(ask[1])
                if size == 0:
                    # 수량이 0이면 해당 가격 삭제
                    if price in orderbook["asks"]:
                        del orderbook["asks"][price]
                else:
                    # 수량이 있으면 추가/업데이트
                    orderbook["asks"][price] = size
            
            # 시퀀스 및 타임스탬프 업데이트
            self.last_update_ids[symbol] = sequence
            orderbook["timestamp"] = timestamp
            orderbook["sequence"] = sequence
            
            # 정렬된 전체 오더북 구성
            sorted_bids = sorted(orderbook["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:self.depth_level]
            sorted_asks = sorted(orderbook["asks"].items(), key=lambda x: float(x[0]))[:self.depth_level]
            
            # 배열 형태로 변환 [price, size]
            bids_array = [[price, size] for price, size in sorted_bids]
            asks_array = [[price, size] for price, size in sorted_asks]
            
            # 완전한 오더북 데이터 구성
            updated_data = {
                "symbol": symbol,
                "bids": bids_array,
                "asks": asks_array,
                "timestamp": timestamp,
                "sequence": sequence
            }
            
            # 오더북 데이터 로깅
            if self.orderbook_logging_enabled:
                self.log_orderbook_data(symbol, updated_data)
            
            # 메트릭 업데이트
            await self.handle_metric_update(
                metric_name="message_processed",
                value=1,
                data={
                    "exchange": self.exchange_name,
                    "symbol": symbol,
                    "message_type": "delta"
                }
            )
            
            # 이벤트 발행 (델타)
            self.publish_event(symbol, updated_data, "delta")
            
        except Exception as e:
            self.log_error(f"[{symbol}] 델타 처리 중 오류: {str(e)}")
            await self.event_handler.handle_error(
                error_type="message_processing_error",
                message=f"델타 처리 실패: {str(e)}",
                symbol=symbol
            )

    async def _on_message(self, message: str) -> None:
        """
        메시지 수신 처리
        
        Args:
            message: 수신된 원본 메시지
        """
        try:
            # 원시 데이터 로깅
            self.log_raw_message(message)
            
            # 메시지가 JSON 형식이 아니면 종료
            data = self._parse_json_message(message)
            if not data:
                return
                
            # 심볼 추출
            symbol = self._extract_symbol_from_message(data)
            if not symbol:
                # 심볼 추출 실패 시 로그만 남기고 종료
                return
                
            # 델타 메시지 처리
            if data.get("e") == "depthUpdate":
                processed_data = self._parse_delta_message(symbol, data)
                if processed_data:
                    await self._process_delta(symbol, processed_data)
                    
        except Exception as e:
            # 메시지 처리 오류를 이벤트 핸들러에 보고
            await self.event_handler.handle_error(
                error_type="message_processing_error",
                message=f"메시지 처리 실패: {str(e)}",
                severity="error"
            )

    # 6. 구독 취소 단계
    async def create_unsubscribe_message(self, symbol: str) -> Dict:
        """
        구독 취소 메시지 생성
        
        Args:
            symbol: 구독 취소할 심볼
            
        Returns:
            Dict: 구독 취소 메시지
        """
        # 심볼 형식 변환 (BTC -> btcusdt)
        market = f"{symbol.lower()}usdt"
        
        # 구독 취소 메시지 형식
        return {
            "method": "UNSUBSCRIBE",
            "params": [f"{market}@depth@{self.update_speed}"],
            "id": int(time.time() * 1000)  # 고유 요청 ID
        }

    async def unsubscribe(self, symbol: Optional[str] = None) -> bool:
        """
        구독 취소
        
        Args:
            symbol: 구독 취소할 심볼 (None이면 모든 심볼)
            
        Returns:
            bool: 구독 취소 성공 여부
        """
        try:
            return await super().unsubscribe(symbol)
        except Exception as e:
            self.log_error(f"구독 취소 중 오류: {e}")
            return False

    def _extract_symbol_from_message(self, data: Dict) -> Optional[str]:
        """
        메시지에서 심볼 추출
        
        Args:
            data: 메시지 데이터
            
        Returns:
            str or None: 추출된 심볼 또는 None
        """
        try:
            # 바이낸스 현물은 's' 필드에 심볼 정보 포함 (예: "BTCUSDT")
            if 's' in data and isinstance(data['s'], str):
                original_symbol = data['s']
                
                # USDT 제거하고 소문자로 변환 (예: "BTCUSDT" -> "btc")
                if original_symbol.endswith('USDT'):
                    symbol = original_symbol[:-4].lower()
                    
                    # 구독 중인 심볼인지 확인 (대소문자 구분없이)
                    for subscribed in self.subscribed_symbols.keys():
                        if subscribed.lower() == symbol:
                            return subscribed
                
                self.log_warning(f"구독되지 않은 심볼: {original_symbol}")
                    
            return None
        except Exception as e:
            self.log_error(f"심볼 추출 중 오류: {str(e)}")
            return None 