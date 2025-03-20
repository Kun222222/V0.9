"""
빗썸 구독 클래스

이 모듈은 빗썸 거래소의 웹소켓 구독을 담당하는 클래스를 제공합니다.
"""

import json
import asyncio
import datetime
import time
from typing import Dict, List, Optional, Union, Any

from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription, EVENT_TYPES
from crosskimp.config.constants_v3 import Exchange
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator

# 빗썸 REST API 및 웹소켓 관련 설정
WS_URL = "wss://ws-api.bithumb.com/websocket/v1"  # 웹소켓 URL
DEFAULT_DEPTH = 15  # 기본 오더북 깊이

# 로깅 설정
ENABLE_RAW_LOGGING = True  # raw 데이터 로깅 활성화 여부
ENABLE_ORDERBOOK_LOGGING = True  # 오더북 데이터 로깅 활성화 여부

class BithumbSubscription(BaseSubscription):
    """
    빗썸 구독 클래스
    
    빗썸 거래소의 웹소켓 구독을 담당하는 클래스입니다.
    
    특징:
    - REST API를 통한 초기 스냅샷 요청
    - 웹소켓을 통한 델타 업데이트 처리
    - 완전한 오더북 상태 유지
    """
    
    # 1. 초기화 단계
    def __init__(self, connection):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
        """
        # 부모 클래스 초기화 (exchange_code 전달)
        super().__init__(connection, Exchange.BITHUMB.value)
        
        # 오더북 관련 설정
        self.depth_level = DEFAULT_DEPTH
        
        # 로깅 설정
        self.raw_logging_enabled = ENABLE_RAW_LOGGING
        self.orderbook_logging_enabled = ENABLE_ORDERBOOK_LOGGING
        
        # 오더북 데이터 저장소
        self.orderbooks = {}  # symbol -> orderbook_data
        
        # 타임스탬프 추적 (타임스탬프와 카운터를 함께 저장)
        self.last_timestamps = {}  # symbol -> {"timestamp": timestamp, "counter": count}

    # 3. 구독 처리 단계
    async def create_subscribe_message(self, symbols: List[str]) -> Dict:
        """
        구독 메시지 생성
        
        Args:
            symbols: 구독할 심볼 또는 심볼 목록
            
        Returns:
            Dict: 구독 메시지
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        
        codes = [f"KRW-{s.upper()}" for s in symbols]
        
        return [
            {"ticket": f"bithumb_orderbook_{int(datetime.datetime.now().timestamp() * 1000)}"},
            {"type": "orderbook", "codes": codes},
            {"format": "DEFAULT"}
        ]

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
            self._publish_subscription_status_event(symbols, "subscribed")
            
            return True
            
        except Exception as e:
            self.log_error(f"구독 중 오류 발생: {str(e)}")
            self.system_event_manager.record_metric(self.exchange_code, "error")
            return False

    # 4. 메시지 수신 및 처리 단계
    def _parse_json_message(self, message: str) -> Optional[Dict]:
        """
        JSON 메시지 파싱
        
        Args:
            message: JSON 메시지 문자열
            
        Returns:
            Dict 또는 None (파싱 실패시)
        """
        try:
            if isinstance(message, bytes):
                message = message.decode('utf-8')
                
            return json.loads(message)
        except json.JSONDecodeError:
            self.log_error(f"JSON 파싱 실패: {message[:100]}...")
            return None
        except Exception as e:
            self.log_error(f"메시지 파싱 중 오류: {str(e)}")
            return None

    def is_snapshot_message(self, message: str) -> bool:
        """
        메시지가 스냅샷인지 확인
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 스냅샷 메시지인 경우 True
        """
        try:
            data = json.loads(message)
            stream_type = data.get("st") or data.get("stream_type", "")
            return stream_type.upper() == "SNAPSHOT" if stream_type else False
        except:
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
            if isinstance(message, bytes):
                message = message.decode('utf-8')
                
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
                
            message_type = data.get("ty") or data.get("type")
            stream_type = data.get("st") or data.get("stream_type")
            
            return message_type == "orderbook" and stream_type and stream_type.upper() in ["REALTIME", "INCREMENTAL"]
            
        except Exception as e:
            self.log_error(f"델타 메시지 확인 중 오류: {e}")
            return False

    def _parse_message(self, message: str) -> Optional[Dict]:
        """
        메시지 파싱
        
        Args:
            message: 수신된 메시지
            
        Returns:
            Optional[Dict]: 파싱된 메시지 데이터 또는 None (파싱 실패시)
        """
        try:
            # 메시지 처리 시작 시간 측정
            start_time = time.time()
            
            data = self._parse_json_message(message)
            if not data:
                return None
                
            message_type = data.get("ty") or data.get("type")
            if message_type != "orderbook":
                return None
                
            code = data.get("cd") or data.get("code", "")
            if not code or not code.startswith("KRW-"):
                return None
                
            symbol = code.split("-")[1]
            
            timestamp = data.get("tms") or data.get("timestamp")
            if timestamp is None:
                timestamp = int(time.time() * 1000)
            
            # 동일 타임스탬프에 대한 카운터 적용
            if symbol not in self.last_timestamps:
                self.last_timestamps[symbol] = {"timestamp": 0, "counter": 0}
                
            if timestamp == self.last_timestamps[symbol]["timestamp"]:
                # 동일한 타임스탬프면 카운터 증가
                self.last_timestamps[symbol]["counter"] += 1
            else:
                # 새로운 타임스탬프면 카운터 초기화
                self.last_timestamps[symbol]["timestamp"] = timestamp
                self.last_timestamps[symbol]["counter"] = 1
                
            # 시퀀스 넘버 생성: 타임스탬프 * 100 + 카운터
            sequence = timestamp * 100 + self.last_timestamps[symbol]["counter"]
            
            stream_type = data.get("st") or data.get("stream_type", "")
            
            orderbook_units = data.get("obu") or data.get("orderbook_units", [])
            if not orderbook_units:
                return None
            
            bids = []
            asks = []
            
            for unit in orderbook_units:
                if "bp" in unit:
                    bid_price = unit.get("bp")
                    bid_size = unit.get("bs")
                    ask_price = unit.get("ap")
                    ask_size = unit.get("as")
                else:
                    bid_price = unit.get("bid_price")
                    bid_size = unit.get("bid_size")
                    ask_price = unit.get("ask_price")
                    ask_size = unit.get("ask_size")
                
                if bid_price is not None and bid_size is not None:
                    try:
                        bids.append([float(bid_price), float(bid_size)])
                    except (ValueError, TypeError):
                        pass
                    
                if ask_price is not None and ask_size is not None:
                    try:
                        asks.append([float(ask_price), float(ask_size)])
                    except (ValueError, TypeError):
                        pass
            
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])
            
            is_snapshot = stream_type and stream_type.upper() == "SNAPSHOT"
            
            result = {
                "symbol": symbol,
                "timestamp": timestamp,
                "sequence": sequence,  # 타임스탬프에 카운터를 추가한 고유 시퀀스
                "bids": bids,
                "asks": asks,
                "type": "snapshot" if is_snapshot else "delta"
            }
            
            # 메트릭 업데이트 - 오더북 메시지 처리 및 처리 시간
            processing_time_ms = (time.time() - start_time) * 1000
            self._update_metrics(self.exchange_code, start_time, "snapshot" if is_snapshot else "delta", result)
            
            return result
            
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}")
            return None

    async def _on_message(self, message: str) -> None:
        """메시지 수신 콜백"""
        try:
            # 메시지 처리 시작 시간 측정
            start_time = time.time()
            
            # 중앙 집중식 메시지 카운트 기록 (상위 클래스의 메서드 호출)
            await super()._on_message(message)
            
            parsed_data = self._parse_message(message)
            if not parsed_data:
                return
                
            symbol = parsed_data.get("symbol")
            if not symbol or symbol not in self.subscribed_symbols:
                return
            
            try:
                if self.validator:
                    is_snapshot = parsed_data.get("type") == "snapshot"
                    
                    # 검증 및 처리
                    if is_snapshot:
                        result = await self.validator.initialize_orderbook(symbol, parsed_data)
                        # 메트릭 업데이트
                        if result.is_valid:
                            self._update_metrics(self.exchange_code, start_time, "snapshot", parsed_data)
                    else:
                        result = await self.validator.update(symbol, parsed_data)
                        # 메트릭 업데이트
                        if result.is_valid:
                            self._update_metrics(self.exchange_code, start_time, "delta", parsed_data)
                        
                    if result.is_valid:
                        # 유효한 오더북 데이터 가져오기
                        orderbook_data = self.validator.get_orderbook(symbol)
                        
                        # 오더북 데이터 로깅 (플래그 확인)
                        if self.orderbook_logging_enabled:
                            self.log_orderbook_data(symbol, orderbook_data)
                        
                        # 이벤트 발행 (스냅샷 또는 델타)
                        event_type = "snapshot" if is_snapshot else "delta"
                        self.publish_event(symbol, orderbook_data, event_type)
                    else:
                        self.log_error(f"{symbol} 오더북 검증 실패: {result.errors}")
            except Exception as e:
                self.log_error(f"{symbol} 오더북 검증 중 오류: {str(e)}")
                asyncio.create_task(self.publish_system_event_sync(
                    EVENT_TYPES["ERROR_EVENT"],
                    message=f"{symbol} 오더북 검증 중 오류: {str(e)}"
                ))
                
                # 오류 이벤트 발행
                self.publish_event(symbol, str(e), "error")
                    
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {str(e)}")
            asyncio.create_task(self.publish_system_event_sync(
                EVENT_TYPES["ERROR_EVENT"],
                message=f"메시지 처리 중 오류: {str(e)}"
            ))

    # 6. 구독 취소 단계
    async def create_unsubscribe_message(self, symbol: str) -> Dict:
        """
        구독 취소 메시지 생성
        
        Args:
            symbol: 구독 취소할 심볼
            
        Returns:
            Dict: 구독 취소 메시지
        """
        # 빗썸은 구독 취소 메시지가 없으므로 빈 배열 반환
        return [
            {"ticket": f"bithumb_unsubscribe_{int(time.time())}"},
            {"type": "orderbook", "codes": []},
            {"format": "DEFAULT"}
        ]

    async def unsubscribe(self, symbol: Optional[str] = None) -> bool:
        """구독 취소"""
        try:
            return await super().unsubscribe(symbol)
        except Exception as e:
            self.log_error(f"구독 취소 중 오류: {e}")
            return False
