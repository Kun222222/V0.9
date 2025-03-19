"""
바이빗 선물 구독 클래스

이 모듈은 바이빗 선물 거래소의 웹소켓 연결을 통해 오더북 데이터를 구독하는 클래스를 제공합니다.
"""

import asyncio
import json
import time
import datetime
from typing import Dict, List, Optional, Union, Any

from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.logger.logger import create_raw_logger
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
from crosskimp.config.constants_v3 import Exchange, LOG_SUBDIRS

# 웹소켓 설정
WS_URL = "wss://stream.bybit.com/v5/public/linear"  # 웹소켓 URL 
MAX_SYMBOLS_PER_SUBSCRIPTION = 10  # 구독당 최대 심볼 수
DEFAULT_DEPTH = 50  # 기본 오더북 깊이

class BybitFutureSubscription(BaseSubscription):
    """
    바이빗 선물 구독 클래스
    
    바이빗 선물 거래소의 웹소켓 연결을 통해 오더북 데이터를 구독하는 클래스입니다.
    
    특징:
    - 메시지 형식: 스냅샷 및 델타 업데이트 지원
    - 웹소켓을 통한 스냅샷 및 델타 업데이트 수신
    """
    def __init__(self, connection):
        """
        바이빗 선물 구독 초기화
        
        Args:
            connection: 바이빗 선물 웹소켓 연결 객체
        """
        # 부모 클래스 초기화 (exchange_code 전달)
        super().__init__(connection, Exchange.BYBIT_FUTURE.value)
        
        # 구독 관련 설정
        self.max_symbols_per_subscription = MAX_SYMBOLS_PER_SUBSCRIPTION
        self.depth_level = DEFAULT_DEPTH
        
        # 로깅 설정
        self.log_raw_data = True  # 원시 데이터 로깅 활성화
        self.raw_logger = None
        self._setup_raw_logging()
        
        # 바이빗 오더북 검증기 초기화
        self.validator = BaseOrderBookValidator(self.exchange_code)
        
        # 각 심볼별 전체 오더북 상태 저장용
        self.orderbooks = {}  # symbol -> {"bids": {...}, "asks": {...}, "timestamp": ..., "sequence": ...}

    def _setup_raw_logging(self):
        """Raw 데이터 로깅 설정"""
        try:
            # 로그 디렉토리 설정
            raw_data_dir = LOG_SUBDIRS['raw_data']
            log_dir = raw_data_dir / self.exchange_code
            log_dir.mkdir(exist_ok=True, parents=True)
            
            # 로그 파일 경로 설정
            current_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_file_path = log_dir / f"{self.exchange_code}_raw_{current_datetime}.log"
            
            # 로거 설정
            self.raw_logger = create_raw_logger(self.exchange_code)
            self.log_info("raw 로거 초기화 완료")
        except Exception as e:
            self.log_error(f"Raw 로깅 설정 실패: {str(e)}", exc_info=True)
            self.log_raw_data = False

    async def create_subscribe_message(self, symbol: Union[str, List[str]]) -> Dict:
        """
        구독 메시지 생성
        
        Args:
            symbol: 구독할 심볼 또는 심볼 목록
            
        Returns:
            Dict: 구독 메시지
        """
        # 심볼 리스트로 변환
        symbols = [symbol] if isinstance(symbol, str) else symbol
        
        # 심볼 형식 변환 ({symbol}USDT)
        args = []
        for sym in symbols:
            market = f"{sym}USDT"
            args.append(f"orderbook.{self.depth_level}.{market}")
        
        symbols_str = ", ".join(symbols) if len(symbols) <= 5 else f"{len(symbols)}개 심볼"
        self.log_info(f"{symbols_str} 구독 메시지 생성")
        
        # 구독 메시지 생성
        return {
            "op": "subscribe",
            "args": args
        }

    async def create_unsubscribe_message(self, symbol: str) -> Dict:
        """
        구독 해제 메시지 생성
        
        Args:
            symbol: 구독 해제할 심볼
            
        Returns:
            Dict: 구독 해제 메시지
        """
        # 심볼 형식 변환 ({symbol}USDT)
        market = f"{symbol}USDT"
        
        # 구독 해제 메시지 생성
        return {
            "op": "unsubscribe",
            "args": [f"orderbook.{self.depth_level}.{market}"]
        }

    def log_raw_message(self, message: str) -> None:
        """
        원시 메시지 로깅
        
        Args:
            message: 원시 메시지
        """
        # 로그 출력 없이 메서드만 유지 (호환성을 위해)
        pass

    async def _on_message(self, message: str) -> None:
        """
        메시지 수신 및 처리
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        try:
            # 원시 메시지 로깅
            self.log_raw_message(message)
            
            # JSON 파싱
            data = json.loads(message)
            
            # 구독 응답 처리
            if data.get("op") == "subscribe":
                # self.log_debug(f"구독 응답 수신: {data}")
                return
                
            # 오더북 메시지 처리
            if "topic" in data and "data" in data:
                topic = data.get("topic", "")
                if "orderbook" in topic:
                    parts = topic.split(".")
                    if len(parts) >= 3:
                        symbol = parts[-1].replace("USDT", "")
                        msg_type = data.get("type", "delta").lower()  # "snapshot" or "delta"
                        
                        # 메시지 파싱
                        parsed_data = self._parse_message(message)
                        if not parsed_data:
                            return
                            
                        # 메시지 타입에 따른 처리
                        if msg_type == "snapshot":
                            # 현물과 같은 방식으로 오더북 초기화
                            bids = parsed_data.get("bids", [])
                            asks = parsed_data.get("asks", [])
                            timestamp = parsed_data.get("timestamp")
                            sequence = parsed_data.get("sequence")
                            
                            # 스냅샷인 경우 오더북 초기화
                            self.orderbooks[symbol] = {
                                "bids": {float(bid[0]): float(bid[1]) for bid in bids},
                                "asks": {float(ask[0]): float(ask[1]) for ask in asks},
                                "timestamp": timestamp,
                                "sequence": sequence
                            }
                            
                            # 오더북 초기화 후 콜백 호출
                            sorted_bids = sorted(self.orderbooks[symbol]["bids"].items(), key=lambda x: x[0], reverse=True)
                            sorted_asks = sorted(self.orderbooks[symbol]["asks"].items(), key=lambda x: x[0])
                            
                            # 배열 형태로 변환 [price, size]
                            bids_array = [[price, size] for price, size in sorted_bids]
                            asks_array = [[price, size] for price, size in sorted_asks]
                            
                            # 완전한 오더북 데이터 구성
                            full_orderbook = {
                                "bids": bids_array,
                                "asks": asks_array,
                                "timestamp": timestamp,
                                "sequence": sequence,
                                "type": "snapshot"
                            }
                            
                            # 오더북 데이터 로깅
                            if self.raw_logger:
                                # 로깅용 10개로 제한
                                limited_bids = bids_array[:10]
                                limited_asks = asks_array[:10]
                                
                                log_data = {
                                    "exchangename": self.exchange_code,
                                    "symbol": symbol,
                                    "bids": limited_bids,
                                    "asks": limited_asks,
                                    "timestamp": timestamp,
                                    "sequence": self.orderbooks[symbol]["sequence"]
                                }
                                
                                current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                                self.raw_logger.debug(f"[{current_time}] 매수 ({len(limited_bids)}) / 매도 ({len(limited_asks)}) {json.dumps(log_data)}")
                            
                            # 스냅샷 또는 델타 메시지 각각에 맞는 처리
                            if symbol in self.snapshot_callbacks:
                                await self._call_callback(symbol, full_orderbook, is_snapshot=True)
                                # 메트릭 업데이트
                                self.metrics_manager.update_message_stats(self.exchange_code, "snapshot")
                            elif symbol in self.delta_callbacks:
                                await self._call_callback(symbol, full_orderbook, is_snapshot=False)
                                # 메트릭 업데이트
                                self.metrics_manager.update_message_stats(self.exchange_code, "delta")
                        
                        elif msg_type == "delta":
                            # 델타 처리 - symbol이 orderbooks에 있을 때만 처리
                            if symbol in self.orderbooks:
                                # 델타 데이터 추출
                                bids = parsed_data.get("bids", [])
                                asks = parsed_data.get("asks", [])
                                timestamp = parsed_data.get("timestamp")
                                sequence = parsed_data.get("sequence")
                                
                                # 시퀀스 확인
                                if sequence <= self.orderbooks[symbol]["sequence"]:
                                    self.log_warning(f"{symbol} 이전 시퀀스의 델타 메시지 수신, 무시 ({sequence} <= {self.orderbooks[symbol]['sequence']})")
                                    return
                                
                                # 기존 오더북 가져오기
                                orderbook = self.orderbooks[symbol]
                                
                                # 매수 호가 업데이트
                                for bid in bids:
                                    price = float(bid[0])
                                    size = float(bid[1])
                                    if size == 0:
                                        # 수량이 0이면 해당 가격의 호가 삭제
                                        orderbook["bids"].pop(price, None)
                                    else:
                                        # 그렇지 않으면 추가 또는 업데이트
                                        orderbook["bids"][price] = size
                                
                                # 매도 호가 업데이트
                                for ask in asks:
                                    price = float(ask[0])
                                    size = float(ask[1])
                                    if size == 0:
                                        # 수량이 0이면 해당 가격의 호가 삭제
                                        orderbook["asks"].pop(price, None)
                                    else:
                                        # 그렇지 않으면 추가 또는 업데이트
                                        orderbook["asks"][price] = size
                                
                                # 타임스탬프와 시퀀스 업데이트
                                orderbook["timestamp"] = timestamp
                                orderbook["sequence"] = sequence
                                
                                # 정렬된 전체 오더북 구성
                                # 매수(높은 가격 -> 낮은 가격), 매도(낮은 가격 -> 높은 가격)
                                sorted_bids = sorted(orderbook["bids"].items(), key=lambda x: x[0], reverse=True)
                                sorted_asks = sorted(orderbook["asks"].items(), key=lambda x: x[0])
                                
                                # 배열 형태로 변환 [price, size]
                                bids_array = [[price, size] for price, size in sorted_bids]
                                asks_array = [[price, size] for price, size in sorted_asks]
                                
                                # 완전한 오더북 데이터 구성
                                full_orderbook = {
                                    "bids": bids_array,
                                    "asks": asks_array,
                                    "timestamp": timestamp,
                                    "sequence": sequence,
                                    "type": "snapshot"  # 델타도 스냅샷으로 전달
                                }
                                
                                # 오더북 데이터 로깅
                                if self.raw_logger:
                                    # 로깅용 10개로 제한
                                    limited_bids = bids_array[:10]
                                    limited_asks = asks_array[:10]
                                    
                                    log_data = {
                                        "exchangename": self.exchange_code,
                                        "symbol": symbol,
                                        "bids": limited_bids,
                                        "asks": limited_asks,
                                        "timestamp": timestamp,
                                        "sequence": orderbook["sequence"]
                                    }
                                    
                                    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                                    self.raw_logger.debug(f"[{current_time}] 매수 ({len(limited_bids)}) / 매도 ({len(limited_asks)}) {json.dumps(log_data)}")
                                
                                # 델타 콜백 호출
                                await self._call_callback(symbol, full_orderbook, is_snapshot=False)
                                # 메트릭 업데이트 추가
                                self.metrics_manager.update_message_stats(self.exchange_code, "delta")
                            else:
                                self.log_warning(f"{symbol} 스냅샷 없이 델타 수신, 무시")
        except json.JSONDecodeError:
            self.log_error(f"JSON 파싱 실패: {message[:100]}...")
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {str(e)}")

    def _parse_message(self, message: str) -> Optional[Dict]:
        """
        메시지 파싱 (JSON 문자열 -> 파이썬 딕셔너리)
        
        Args:
            message: 수신된 메시지
            
        Returns:
            Optional[Dict]: 파싱된 메시지 데이터 또는 None(파싱 실패시)
        """
        try:
            # JSON 문자열 파싱
            data = json.loads(message)
            result = {}
            
            # 메시지 형식 판별
            if "topic" in data and "data" in data:
                topic = data.get("topic", "")
                if "orderbook" in topic:
                    # 타입 확인
                    msg_type = data.get("type", "delta").lower()  # "snapshot" or "delta"
                    
                    # 심볼 추출 (topic 형식: orderbook.50.{symbol})
                    parts = topic.split(".")
                    if len(parts) >= 3:
                        symbol = parts[-1].replace("USDT", "")
                    else:
                        return None
                    
                    # 데이터 추출
                    inner_data = data.get("data", {})
                    
                    # 매수/매도 데이터
                    bids = inner_data.get("b", [])  # "b" for bids
                    asks = inner_data.get("a", [])  # "a" for asks
                    
                    # 시퀀스 및 타임스탬프
                    timestamp = inner_data.get("ts")  # 타임스탬프 (밀리초 단위)
                    if timestamp is None:
                        timestamp = int(time.time() * 1000)  # 현재 시간을 밀리초로 사용
                        
                    sequence = inner_data.get("seq") or inner_data.get("u")  # 시퀀스 번호
                    
                    # 결과 구성
                    result = {
                        "symbol": symbol,
                        "type": msg_type,
                        "bids": bids,
                        "asks": asks,
                        "timestamp": timestamp,  # 타임스탬프 추출
                        "sequence": sequence
                    }
                    
                    return result
            
            return None
            
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)} | 원본: {message[:100]}...")
            return None

    async def subscribe(self, symbol, on_snapshot=None, on_delta=None, on_error=None):
        """
        심볼 구독
        
        Args:
            symbol: 구독할 심볼 또는 심볼 리스트
            on_snapshot: 스냅샷 수신 시 호출할 콜백 함수
            on_delta: 델타 수신 시 호출할 콜백 함수
            on_error: 에러 발생 시 호출할 콜백 함수
            
        Returns:
            bool: 구독 성공 여부
        """
        try:
            # 심볼이 리스트인지 확인하고 처리
            if isinstance(symbol, list):
                symbols = symbol
            else:
                symbols = [symbol]
                
            if not symbols:
                self.log_warning("구독할 심볼이 없습니다.")
                return False
                
            # 이미 구독 중인 심볼 필터링
            new_symbols = [s for s in symbols if s not in self.orderbooks]
            if not new_symbols:
                self.log_info("모든 심볼이 이미 구독 중입니다.")
                return True
                
            # 콜백 함수 등록
            for sym in new_symbols:
                if on_snapshot is not None:
                    self.snapshot_callbacks[sym] = on_snapshot
                if on_delta is not None:
                    self.delta_callbacks[sym] = on_delta
                if on_error is not None:
                    self.error_callbacks[sym] = on_error
                
                # 구독 상태 초기화
                self.orderbooks[sym] = True
            
            # 연결 확인 및 연결
            if not self._is_connected():
                self.log_info("웹소켓 연결 시작")
                if not await self.connection.connect():
                    raise Exception("웹소켓 연결 실패")
                
                # 연결 상태 메트릭 업데이트
                self.metrics_manager.update_connection_state(self.exchange_code, "connected")
            
            # 배치 단위로 구독 처리
            total_batches = (len(new_symbols) + self.max_symbols_per_subscription - 1) // self.max_symbols_per_subscription
            self.log_info(f"구독 시작 | 총 {len(new_symbols)}개 심볼, {total_batches}개 배치로 나눔")
            
            for i in range(0, len(new_symbols), self.max_symbols_per_subscription):
                batch_symbols = new_symbols[i:i + self.max_symbols_per_subscription]
                batch_num = (i // self.max_symbols_per_subscription) + 1
                
                # 구독 메시지 생성 및 전송
                subscribe_message = await self.create_subscribe_message(batch_symbols)
                if not await self.connection.send_message(json.dumps(subscribe_message)):
                    raise Exception(f"배치 {batch_num}/{total_batches} 구독 메시지 전송 실패")
                
                self.log_info(f"구독 요청 전송 | 배치 {batch_num}/{total_batches}, {len(batch_symbols)}개 심볼")
                
                # 요청 간 짧은 딜레이 추가
                await asyncio.sleep(0.1)
            
            # 첫 번째 구독 시 메시지 수신 루프 시작
            if not self.message_loop_task or self.message_loop_task.done():
                self.log_info("첫 번째 구독으로 메시지 수신 루프 시작")
                self.stop_event.clear()
                self.message_loop_task = asyncio.create_task(self.message_loop())
            
            return True
            
        except Exception as e:
            self.log_error(f"구독 중 오류 발생: {str(e)}")
            # 에러 메트릭 업데이트
            self.metrics_manager.record_error(self.exchange_code)
            
            if on_error is not None:
                if isinstance(symbol, list):
                    for sym in symbol:
                        await on_error(sym, str(e))
                else:
                    await on_error(symbol, str(e))
            return False

    def _is_connected(self) -> bool:
        """연결 상태 확인"""
        return self.connection and self.connection.is_connected and self.connection.ws

    async def _cancel_tasks(self) -> None:
        """모든 태스크 취소"""
        for task in self.tasks.values():
            task.cancel()
        self.tasks.clear()
    
    async def _cancel_message_loop(self) -> None:
        """메시지 수신 루프 취소"""
        if self.message_loop_task and not self.message_loop_task.done():
            self.message_loop_task.cancel()
            try:
                await self.message_loop_task
            except asyncio.CancelledError:
                pass
    
    async def _unsubscribe_symbol(self, symbol: str) -> bool:
        """
        특정 심볼 구독 취소
        
        Args:
            symbol: 구독 취소할 심볼
            
        Returns:
            bool: 구독 취소 성공 여부
        """
        try:
            if symbol not in self.orderbooks:
                self.log_warning(f"{symbol} 구독 중이 아닙니다.")
                return True
                
            # 부모 클래스의 _cleanup_subscription 메서드 사용
            self._cleanup_subscription(symbol)
            
            # 구독 취소 메시지 생성 및 전송
            unsubscribe_message = await self.create_unsubscribe_message(symbol)
            if not await self.connection.send_message(json.dumps(unsubscribe_message)):
                raise Exception(f"{symbol} 구독 취소 메시지 전송 실패")
                
            self.log_info(f"{symbol} 구독 취소 완료")
            return True
            
        except Exception as e:
            self.log_error(f"{symbol} 구독 취소 중 오류 발생: {e}")
            return False
    
    async def unsubscribe(self, symbol: Optional[str] = None) -> bool:
        """
        구독 취소
        
        Args:
            symbol: 구독 취소할 심볼. None인 경우 모든 심볼 구독 취소 및 자원 정리
            
        Returns:
            bool: 구독 취소 성공 여부
        """
        try:
            # symbol이 None이면 모든 심볼 구독 취소 및 자원 정리
            if symbol is None:
                # 메시지 수신 루프 및 태스크 취소
                await self._cancel_message_loop()
                await self._cancel_tasks()
                
                # 모든 심볼 목록 저장
                symbols = list(self.subscribed_symbols.keys())
                
                # 각 심볼별로 구독 취소 메시지 전송
                for sym in symbols:
                    # 구독 취소 메시지 생성 및 전송
                    unsubscribe_message = await self.create_unsubscribe_message(sym)
                    try:
                        if self.connection and self.connection.is_connected:
                            await self.connection.send_message(json.dumps(unsubscribe_message))
                    except Exception as e:
                        self.log_warning(f"{sym} 구독 취소 메시지 전송 실패: {e}")
                    
                    # 내부적으로 구독 상태 정리
                    self._cleanup_subscription(sym)
                
                self.log_info("모든 심볼 구독 취소 완료")
                
                # 연결 종료 처리
                if hasattr(self.connection, 'disconnect'):
                    try:
                        await asyncio.wait_for(self.connection.disconnect(), timeout=2.0)
                    except (asyncio.TimeoutError, Exception) as e:
                        self.log_warning(f"연결 종료 중 오류: {str(e)}")
                
                self.log_info("모든 자원 정리 완료")
                return True
            
            # 특정 심볼 구독 취소
            return await self._unsubscribe_symbol(symbol)
            
        except Exception as e:
            self.log_error(f"구독 취소 중 오류 발생: {e}")
            # 에러 메트릭 업데이트
            self.metrics_manager.record_error(self.exchange_code)
            return False

    async def _call_callback(self, symbol: str, data: Dict, is_snapshot: bool = True) -> None:
        """
        콜백 메서드 (스냅샷 또는 델타)
        
        Args:
            symbol: 심볼
            data: 오더북 데이터
            is_snapshot: 스냅샷 여부 (True: 스냅샷 콜백, False: 델타 콜백)
        """
        try:
            callback = self.snapshot_callbacks.get(symbol) if is_snapshot else self.delta_callbacks.get(symbol)
            if callback:
                await callback(symbol, data)
        except Exception as e:
            callback_type = "스냅샷" if is_snapshot else "델타"
            self.log_error(f"{symbol} {callback_type} 콜백 호출 실패: {str(e)}")

    def is_snapshot_message(self, message: str) -> bool:
        """
        메시지가 스냅샷인지 확인
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 스냅샷 메시지인 경우 True
        """
        # _parse_message 결과를 활용하여 타입 판별
        parsed_data = self._parse_message(message)
        return parsed_data is not None and parsed_data.get("type") == "snapshot"
    
    def is_delta_message(self, message: str) -> bool:
        """
        메시지가 델타인지 확인
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 델타 메시지인 경우 True
        """
        # _parse_message 결과를 활용하여 타입 판별
        parsed_data = self._parse_message(message)
        return parsed_data is not None and parsed_data.get("type") == "delta" 