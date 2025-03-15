# file: orderbook/websocket/binance_future_websocket.py


import asyncio
import json
import time
import aiohttp
from typing import Dict, List, Optional

from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG

from crosskimp.ob_collector.orderbook.connection.binance_f_cn import BinanceFutureWebSocketConnector
from crosskimp.ob_collector.orderbook.orderbook.binance_f_ob import BinanceFutureOrderBookManagerV2, parse_binance_future_depth_update


# ============================
# 바이낸스 선물 웹소켓 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BINANCE_FUTURE.value  # 거래소 코드
BINANCE_FUTURE_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이낸스 선물 설정

# 오더북 관련 설정
UPDATE_SPEED = BINANCE_FUTURE_CONFIG["update_speed"]  # 웹소켓 업데이트 속도 (100ms, 250ms, 500ms 중 선택)
DEFAULT_DEPTH = BINANCE_FUTURE_CONFIG["default_depth"]     # 기본 오더북 깊이

class BinanceFutureWebsocket(BinanceFutureWebSocketConnector):
    """
    바이낸스 선물 웹소켓
    - 메시지 파싱 및 처리
    - 오더북 업데이트 관리
    
    연결 관리는 BinanceFutureWebSocketConnector 클래스에서 처리합니다.
    """

    def __init__(self, settings: dict):
        """
        바이낸스 선물 웹소켓 클라이언트 초기화
        
        Args:
            settings: 설정 딕셔너리
        """
        super().__init__(settings)
        self.orderbook_manager = BinanceFutureOrderBookManagerV2(self.snapshot_depth)
        self.set_manager(self.orderbook_manager)  # 부모 클래스에 매니저 설정

    def set_output_queue(self, queue: asyncio.Queue) -> None:
        """
        BaseWebsocket에도 저장 + orderbook_manager에도 동일하게 저장
        """
        super().set_output_queue(queue)
        if self.orderbook_manager:
            self.orderbook_manager.set_output_queue(queue)
            self.log_info(f"오더북 매니저에 출력 큐 설정 완료 (큐 ID: {id(queue)})")
            
            # 각 오더북 객체에 큐가 제대로 설정되었는지 확인
            for symbol in self.subscribed_symbols:
                orderbook = self.orderbook_manager.get_orderbook(symbol)
                if orderbook:
                    if orderbook.output_queue is None:
                        orderbook.output_queue = queue
                        self.log_info(f"{symbol} 오더북 객체에 출력 큐 직접 설정 완료")
                    else:
                        self.log_info(f"{symbol} 오더북 객체 출력 큐 확인: {id(orderbook.output_queue)}")

    async def subscribe(self, symbols: List[str]):
        """
        지정된 심볼 목록을 구독
        
        1. 웹소켓을 통해 실시간 업데이트 구독
        2. 각 심볼별로 REST API를 통해 스냅샷 요청
        3. 스냅샷을 오더북 매니저에 적용
        
        Args:
            symbols: 구독할 심볼 목록
        """
        # 부모 클래스의 subscribe 호출하여 웹소켓 구독 수행
        await super().subscribe(symbols)

        # 출력 큐가 설정되어 있는지 확인
        if self.output_queue and self.orderbook_manager:
            self.log_info(f"출력 큐 확인: {id(self.output_queue)}")
            self.orderbook_manager.set_output_queue(self.output_queue)

        # 스냅샷
        for sym in symbols:
            snapshot = await self.request_snapshot(sym)
            if snapshot:
                res = await self.orderbook_manager.initialize_orderbook(sym, snapshot)
                if not res.is_valid:
                    self.log_error(f"{sym} 스냅샷 적용 실패: {res.error_messages}")
                else:
                    self.log_info(f"{sym} 스냅샷 적용 성공")
                    
                    # 초기 오더북 데이터 큐에 전송 시도
                    orderbook = self.orderbook_manager.get_orderbook(sym)
                    if orderbook:
                        # 오더북 객체에 큐가 설정되어 있는지 확인
                        if orderbook.output_queue is None and self.output_queue:
                            orderbook.output_queue = self.output_queue
                            self.log_info(f"{sym} 오더북 객체에 출력 큐 설정 (subscribe 내)")
                        
                        # 큐로 데이터 전송
                        if self.output_queue:
                            try:
                                data = orderbook.to_dict()
                                await self.output_queue.put((self.exchangename, data))
                                self.log_info(f"{sym} 초기 오더북 데이터 큐 전송 성공")
                                
                                # 직접 send_to_queue 메서드 호출 시도
                                try:
                                    await orderbook.send_to_queue()
                                    self.log_info(f"{sym} send_to_queue 메서드 호출 성공")
                                except Exception as e:
                                    self.log_error(f"{sym} send_to_queue 메서드 호출 실패: {e}")
                            except Exception as e:
                                self.log_error(f"{sym} 초기 오더북 데이터 큐 전송 실패: {e}")
            else:
                self.log_error(f"{sym} 스냅샷 요청 실패")

    async def parse_message(self, message: str) -> Optional[dict]:
        try:
            data = json.loads(message)
            if "stream" in data and "data" in data:
                data = data["data"]

            # 구독 응답
            if "result" in data and "id" in data:
                if self.connection_status_callback:
                    self.connection_status_callback(self.exchangename, "subscribe_response")
                return None

            # depthUpdate
            if data.get("e") == "depthUpdate":
                symbol = data["s"].replace("USDT","").upper()
                # Raw 메시지 로깅
                self.log_raw_message("depthUpdate", message, symbol)
                return data

            return None
        except json.JSONDecodeError as je:
            self.log_error(f"JSON 파싱실패: {je}, message={message[:200]}")
        except Exception as e:
            self.log_error(f"parse_message() 예외: {e}")
        return None

    async def handle_parsed_message(self, parsed: dict) -> None:
        """파싱된 메시지 처리"""
        try:
            start_time = time.time()
            
            # depthUpdate 메시지 처리
            if parsed.get("e") == "depthUpdate":
                symbol = parsed.get("s", "").replace("USDT", "").upper()
                
                # 메시지 크기 메트릭 기록 - 안전하게 호출
                try:
                    # metrics_manager 속성이 있는지 확인
                    if hasattr(self, 'metrics_manager') and self.metrics_manager:
                        message_size = len(str(parsed))
                        await self.record_metric("bytes", size=message_size, message_type="orderbook", symbol=symbol)
                except Exception:
                    # 오류 로그를 출력하지 않음
                    pass
                
                # 오더북 업데이트
                if self.orderbook_manager:
                    try:
                        # 오더북 업데이트
                        result = await self.orderbook_manager.update(symbol, parsed)
                        
                        # 업데이트 성공 시 큐로 직접 전송 시도 (백업 메커니즘)
                        if result.is_valid and self.output_queue:
                            try:
                                orderbook = self.orderbook_manager.get_orderbook(symbol)
                                if orderbook:
                                    # 오더북 객체에 큐가 설정되어 있는지 확인
                                    if orderbook.output_queue is None:
                                        orderbook.output_queue = self.output_queue
                                        self.log_info(f"{symbol} 오더북 객체에 출력 큐 설정 (handle_parsed_message 내)")
                                    
                                    # 큐에 직접 전송
                                    data = orderbook.to_dict()
                                    await self.output_queue.put((self.exchangename, data))
                                    self.log_info(f"{symbol} 오더북 데이터 큐 직접 전송 성공")
                                    
                                    # 직접 send_to_queue 메서드 호출 시도
                                    try:
                                        await orderbook.send_to_queue()
                                        self.log_info(f"{symbol} send_to_queue 메서드 호출 성공")
                                    except Exception as e:
                                        self.log_error(f"{symbol} send_to_queue 메서드 호출 실패: {e}")
                            except Exception as e:
                                self.log_error(f"{symbol} 오더북 데이터 큐 직접 전송 실패: {e}")
                        
                        # 처리 시간 메트릭 기록 - 안전하게 호출
                        try:
                            # metrics_manager 속성이 있는지 확인
                            if hasattr(self, 'metrics_manager') and self.metrics_manager:
                                processing_time = (time.time() - start_time) * 1000  # ms 단위
                                await self.record_metric("processing_time", time_ms=processing_time, operation="orderbook_update", symbol=symbol)
                        except Exception:
                            # 오류 로그를 출력하지 않음
                            pass
                        
                        # 업데이트 결과에 따른 메트릭 기록 - 안전하게 호출
                        try:
                            # metrics_manager 속성이 있는지 확인
                            if hasattr(self, 'metrics_manager') and self.metrics_manager:
                                if result.is_valid:
                                    await self.record_metric("orderbook", symbol=symbol, operation="update")
                                else:
                                    await self.record_metric("error", error_type="update_failed", symbol=symbol, error=str(result.error_messages))
                        except Exception:
                            # 오류 로그를 출력하지 않음
                            pass
                    except Exception as e:
                        self.log_error(f"오더북 업데이트 실패: {e}")
            
            # 바이빗 스타일 메시지 처리 (이전 코드 유지)
            elif "topic" in parsed and "data" in parsed:
                topic = parsed["topic"]
                data = parsed["data"]
                
                # 오더북 메시지 처리
                if topic.startswith("orderbook."):
                    # 심볼 추출
                    symbol_with_suffix = topic.split(".")[-1]
                    symbol = symbol_with_suffix.replace("USDT", "")
                    
                    # 메시지 크기 메트릭 기록 - 안전하게 호출
                    try:
                        # metrics_manager 속성이 있는지 확인
                        if hasattr(self, 'metrics_manager') and self.metrics_manager:
                            message_size = len(str(parsed))
                            await self.record_metric("bytes", size=message_size, message_type="orderbook", symbol=symbol)
                    except Exception:
                        # 오류 로그를 출력하지 않음
                        pass
                    
                    # 오더북 업데이트
                    if self.orderbook_manager:
                        try:
                            # 데이터 변환
                            converted_data = self._convert_orderbook_data(data, symbol)
                            
                            # 오더북 업데이트
                            result = await self.orderbook_manager.update(symbol, converted_data)
                            
                            # 처리 시간 메트릭 기록 - 안전하게 호출
                            try:
                                # metrics_manager 속성이 있는지 확인
                                if hasattr(self, 'metrics_manager') and self.metrics_manager:
                                    processing_time = (time.time() - start_time) * 1000  # ms 단위
                                    await self.record_metric("processing_time", time_ms=processing_time, operation="orderbook_update", symbol=symbol)
                            except Exception:
                                # 오류 로그를 출력하지 않음
                                pass
                            
                            # 업데이트 결과에 따른 메트릭 기록 - 안전하게 호출
                            try:
                                # metrics_manager 속성이 있는지 확인
                                if hasattr(self, 'metrics_manager') and self.metrics_manager:
                                    if result.is_valid:
                                        await self.record_metric("orderbook", symbol=symbol, operation="update")
                                    else:
                                        await self.record_metric("error", error_type="update_failed", symbol=symbol, error=str(result.error_messages))
                            except Exception:
                                # 오류 로그를 출력하지 않음
                                pass
                        except Exception as e:
                            self.log_error(f"오더북 업데이트 실패: {e}")
                        
                # 핑/퐁 메시지 처리
                elif "op" in parsed and parsed["op"] == "pong":
                    # 퐁 수신 시간 기록
                    self.stats.last_pong_time = time.time()
                    
                    # 핑-퐁 지연시간 계산
                    latency = (self.stats.last_pong_time - self.stats.last_ping_time) * 1000  # ms 단위
                    self.stats.latency_ms = latency
                    
                    # 핑-퐁 메트릭 기록 - 안전하게 호출
                    try:
                        # metrics_manager 속성이 있는지 확인
                        if hasattr(self, 'metrics_manager') and self.metrics_manager:
                            await self.record_metric("pong", latency_ms=latency)
                    except Exception:
                        # 오류 로그를 출력하지 않음
                        pass
                    
                    self.log_debug(f"퐁 수신 | 지연시간: {latency:.2f}ms")
                
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {e}")
            
            # 오류 메트릭 기록 - 안전하게 호출
            try:
                # metrics_manager 속성이 있는지 확인
                if hasattr(self, 'metrics_manager') and self.metrics_manager:
                    await self.record_metric("error", error_type="message_handling", error=str(e))
            except Exception:
                # 오류 로그를 출력하지 않음
                pass

    async def process_message(self, message: str) -> None:
        """
        수신된 메시지 처리 (BinanceFutureWebSocketConnector 클래스의 추상 메서드 구현)
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        parsed = await self.parse_message(message)
        if parsed and self.connection_status_callback:
            self.connection_status_callback(self.exchangename, "message")
        if parsed:
            await self.handle_parsed_message(parsed)

    async def record_metric(self, metric_type: str, **kwargs):
        """
        메트릭 기록 메서드 (metrics_manager가 없을 때 호출되는 더미 메서드)
        
        이 메서드는 metrics_manager 속성이 없을 때 호출되어도 오류가 발생하지 않도록 합니다.
        실제 메트릭 기록은 수행하지 않습니다.
        
        Args:
            metric_type: 메트릭 유형
            **kwargs: 추가 매개변수
        """
        # metrics_manager가 없으면 아무 작업도 수행하지 않음
        pass