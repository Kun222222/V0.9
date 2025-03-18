"""
바이빗 현물 구독 클래스

이 모듈은 바이빗 현물 거래소의 웹소켓 구독을 담당하는 클래스를 제공합니다.
"""

import json
import asyncio
import datetime
from typing import Dict, List, Optional, Union

from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.logger.logger import create_raw_logger
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator
from crosskimp.config.paths import LOG_SUBDIRS

# ============================
# 바이빗 현물 구독 관련 상수
# ============================
# 거래소 코드
EXCHANGE_CODE = "BYBIT"  # 거래소 코드 (소문자로 통일)
EXCHANGE_NAME_KR = "[바이빗]"  # 한글 로깅용 이름

# 웹소켓 설정
WS_URL = "wss://stream.bybit.com/v5/public/spot"  # 웹소켓 URL
MAX_SYMBOLS_PER_SUBSCRIPTION = 10  # 구독당 최대 심볼 수
DEFAULT_DEPTH = 50  # 기본 오더북 깊이

class BybitSubscription(BaseSubscription):
    """
    바이빗 현물 구독 클래스
    
    바이빗 현물 거래소의 웹소켓 구독을 담당하는 클래스입니다.
    
    특징:
    - 웹소켓을 통한 스냅샷 수신 후 델타 업데이트 처리
    - 시퀀스 번호 기반 데이터 정합성 검증
    """
    
    def __init__(self, connection):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
        """
        # 부모 클래스 초기화 (exchange_code 전달)
        super().__init__(connection, EXCHANGE_CODE)
        
        # 구독 관련 설정
        self.max_symbols_per_subscription = MAX_SYMBOLS_PER_SUBSCRIPTION
        self.depth_level = DEFAULT_DEPTH
        
        # 로깅 설정
        self.log_raw_data = True  # 원시 데이터 로깅 활성화
        self.raw_logger = None
        self._setup_raw_logging()
        
        # 바이빗 오더북 검증기 초기화
        self.validator = BaseOrderBookValidator(EXCHANGE_CODE)
        
        # 각 심볼별 전체 오더북 상태 저장용
        self.orderbooks = {}  # symbol -> {"bids": {...}, "asks": {...}, "timestamp": ..., "sequence": ...}
    
    def _setup_raw_logging(self):
        """Raw 데이터 로깅 설정"""
        try:
            # 로그 디렉토리 설정
            raw_data_dir = LOG_SUBDIRS['raw_data']
            log_dir = raw_data_dir / EXCHANGE_CODE
            log_dir.mkdir(exist_ok=True, parents=True)
            
            # 로그 파일 경로 설정
            current_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_file_path = log_dir / f"{EXCHANGE_CODE}_raw_{current_datetime}.log"
            
            # 로거 설정
            self.raw_logger = create_raw_logger(EXCHANGE_CODE)
            self.logger.info(f"{EXCHANGE_NAME_KR} raw 로거 초기화 완료")
        except Exception as e:
            self.logger.error(f"Raw 로깅 설정 실패: {str(e)}", exc_info=True)
            self.log_raw_data = False
    
    async def create_subscribe_message(self, symbol: Union[str, List[str]]) -> Dict:
        """
        구독 메시지 생성
        
        Args:
            symbol: 심볼 또는 심볼 리스트
            
        Returns:
            Dict: 구독 메시지
        """
        # 단일 심볼을 리스트로 변환
        if isinstance(symbol, str):
            symbols = [symbol]
        else:
            symbols = symbol
            
        # 심볼 형식 변환 ({symbol}USDT)
        args = []
        for sym in symbols:
            market = f"{sym}USDT"
            args.append(f"orderbook.{DEFAULT_DEPTH}.{market}")
        
        # 구독 메시지 생성
        return {
            "op": "subscribe",
            "args": args
        }
    
    async def create_unsubscribe_message(self, symbol: str) -> Dict:
        """
        구독 취소 메시지 생성
        
        Args:
            symbol: 심볼
            
        Returns:
            Dict: 구독 취소 메시지
        """
        market = f"{symbol}USDT"
        return {
            "op": "unsubscribe",
            "args": [f"orderbook.{DEFAULT_DEPTH}.{market}"]
        }
    
    def _parse_json_message(self, message: str) -> Optional[Dict]:
        """
        메시지를 JSON으로 파싱하는 공통 헬퍼 메소드
        
        Args:
            message: 원시 메시지 (bytes 또는 str)
            
        Returns:
            Optional[Dict]: 파싱된 JSON 또는 None (파싱 실패시)
        """
        try:
            # 메시지가 bytes인 경우 문자열로 변환
            if isinstance(message, bytes):
                message = message.decode('utf-8')
                
            # 메시지가 문자열인 경우 JSON으로 파싱
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
                
            return data
        except Exception as e:
            self.logger.error(f"{EXCHANGE_NAME_KR} JSON 파싱 실패: {str(e)}")
            return None
    
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
    
    def log_raw_message(self, message: str) -> None:
        """
        원시 메시지 로깅
        
        Args:
            message: 원시 메시지
        """
        # 로그 출력 없이 메서드만 유지 (호환성을 위해)
        pass
    
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
            self.logger.error(f"{EXCHANGE_NAME_KR} {symbol} {callback_type} 콜백 호출 실패: {str(e)}")
    
    async def _on_message(self, message: str) -> None:
        """
        메시지 수신 처리
        
        Args:
            message: 수신된 원시 메시지
        """
        try:
            # 내부적으로 메시지 파싱
            parsed_data = self._parse_message(message)
                
            if not parsed_data:
                return
                
            symbol = parsed_data.get("symbol")
            if not symbol or symbol not in self.subscribed_symbols:
                return
            
            # 메시지 타입 확인
            is_snapshot = parsed_data.get("type") == "snapshot"
            
            # 오더북 데이터와 시퀀스 추출
            bids = parsed_data.get("bids", [])
            asks = parsed_data.get("asks", [])
            timestamp = parsed_data.get("timestamp")
            sequence = parsed_data.get("sequence")
            
            # 오더북 업데이트
            if is_snapshot:
                # 스냅샷인 경우 오더북 초기화
                self.orderbooks[symbol] = {
                    "bids": {float(bid[0]): float(bid[1]) for bid in bids},
                    "asks": {float(ask[0]): float(ask[1]) for ask in asks},
                    "timestamp": timestamp,
                    "sequence": sequence
                }
            else:
                # 델타인 경우 기존 오더북에 업데이트 적용
                if symbol not in self.orderbooks:
                    # 스냅샷 없이 델타가 먼저 도착한 경우 처리
                    self.logger.warning(f"{EXCHANGE_NAME_KR} {symbol} 스냅샷 없이 델타 메시지 수신, 무시")
                    return
                
                # 시퀀스 확인
                if sequence <= self.orderbooks[symbol]["sequence"]:
                    self.logger.warning(f"{EXCHANGE_NAME_KR} {symbol} 이전 시퀀스의 델타 메시지 수신, 무시 ({sequence} <= {self.orderbooks[symbol]['sequence']})")
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
            sorted_bids = sorted(self.orderbooks[symbol]["bids"].items(), key=lambda x: x[0], reverse=True)
            sorted_asks = sorted(self.orderbooks[symbol]["asks"].items(), key=lambda x: x[0])
            
            # 배열 형태로 변환 [price, size]
            bids_array = [[price, size] for price, size in sorted_bids]
            asks_array = [[price, size] for price, size in sorted_asks]
            
            # 오더북 데이터 로깅 - 원래 형식으로 변경
            if self.raw_logger:
                # 로깅용 10개로 제한
                limited_bids = bids_array[:10]
                limited_asks = asks_array[:10]
                
                log_data = {
                    "exchangename": self.exchange_code,
                    "symbol": symbol,
                    "bids": limited_bids,
                    "asks": limited_asks,
                    "timestamp": self.orderbooks[symbol]["timestamp"],
                    "sequence": self.orderbooks[symbol]["sequence"]
                }
                
                current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                self.raw_logger.debug(f"[{current_time}] 매수 ({len(limited_bids)}) / 매도 ({len(limited_asks)}) {json.dumps(log_data)}")
            
            # 메시지 처리 - 완전한 오더북 데이터로 콜백 호출
            full_orderbook = {
                "bids": bids_array,
                "asks": asks_array,
                "timestamp": self.orderbooks[symbol]["timestamp"],
                "sequence": self.orderbooks[symbol]["sequence"],
                "type": "snapshot"  # 항상 완전한 스냅샷 형태로 전달
            }
            
            # 스냅샷 또는 델타 메시지 각각에 맞는 처리
            if is_snapshot:
                # 스냅샷 콜백 호출
                if symbol in self.snapshot_callbacks:
                    self.logger.debug(f"{EXCHANGE_NAME_KR} {symbol} 스냅샷 수신 (시간: {timestamp}, 시퀀스: {sequence})")
                    await self._call_callback(symbol, full_orderbook, is_snapshot=True)
            else:
                # 델타 메시지지만 완전한 오더북으로 델타 콜백 호출
                if symbol in self.delta_callbacks:
                    self.logger.debug(f"{EXCHANGE_NAME_KR} {symbol} 델타 적용 후 오더북 업데이트 (시간: {timestamp}, 시퀀스: {sequence})")
                    await self._call_callback(symbol, full_orderbook, is_snapshot=False)
                    
        except Exception as e:
            self.logger.error(f"{EXCHANGE_NAME_KR} 메시지 처리 실패: {str(e)}")
            self.metrics_manager.record_error(self.exchange_code)
    
    def _parse_message(self, message: str) -> Optional[Dict]:
        """
        내부 메시지 파싱 메소드
        
        외부 파서 없이 직접 메시지를 파싱하는 기능
        
        Args:
            message: 수신된 원시 메시지
            
        Returns:
            Optional[Dict]: 파싱된 오더북 데이터 또는 None (파싱 실패시)
        """
        try:
            # 공통 JSON 파싱 메소드 사용
            data = self._parse_json_message(message)
            if data is None:
                return None
                
            # 바이빗 메시지 구조 체크
            if "topic" not in data or "data" not in data:
                return None
            
            # 토픽이 orderbook으로 시작하는지 확인
            topic = data.get("topic", "")
            if not topic.startswith("orderbook."):
                return None
            
            # 메시지 타입 확인 (snapshot 또는 delta)
            data_type = data.get("type", "").lower()
            if data_type not in ["snapshot", "delta"]:
                return None
            
            # 심볼 추출 (orderbook.50.BTCUSDT -> BTC)
            topic_parts = topic.split(".")
            if len(topic_parts) < 3:
                return None
            
            market = topic_parts[2]  # BTCUSDT
            if not market.endswith("USDT"):
                return None
            
            symbol = market[:-4]  # BTCUSDT -> BTC
            
            # 데이터 추출
            orderbook_data = data.get("data", {})
            
            # 타임스탬프 추출 (메시지 자체의 ts 필드 사용)
            timestamp = data.get("ts")
            
            # 시퀀스 추출 (u: updateId)
            sequence = orderbook_data.get("u", 0)
            
            # 매수 호가 추출
            bids_data = orderbook_data.get("b", [])
            bids = [[float(price), float(size)] for price, size in bids_data]
            
            # 매도 호가 추출
            asks_data = orderbook_data.get("a", [])
            asks = [[float(price), float(size)] for price, size in asks_data]
            
            # 파싱된 데이터 반환
            return {
                "symbol": symbol,
                "timestamp": timestamp,
                "sequence": sequence,
                "bids": bids,
                "asks": asks,
                "type": data_type  # snapshot 또는 delta
            }
            
        except Exception as e:
            self.logger.error(f"{EXCHANGE_NAME_KR} 메시지 파싱 실패: {str(e)}")
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
                self.logger.warning(f"{EXCHANGE_NAME_KR} 구독할 심볼이 없습니다.")
                return False
                
            # 이미 구독 중인 심볼 필터링
            new_symbols = [s for s in symbols if s not in self.subscribed_symbols]
            if not new_symbols:
                self.logger.info(f"{EXCHANGE_NAME_KR} 모든 심볼이 이미 구독 중입니다.")
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
                self.subscribed_symbols[sym] = True
            
            # 연결 확인 및 연결
            if not self._is_connected():
                self.logger.info(f"{EXCHANGE_NAME_KR} 웹소켓 연결 시작")
                if not await self.connection.connect():
                    raise Exception("웹소켓 연결 실패")
                
                # 연결 상태 메트릭 업데이트
                self.metrics_manager.update_connection_state(self.exchange_code, "connected")
            
            # 배치 단위로 구독 처리
            total_batches = (len(new_symbols) + self.max_symbols_per_subscription - 1) // self.max_symbols_per_subscription
            self.logger.info(f"{EXCHANGE_NAME_KR} 구독 시작 | 총 {len(new_symbols)}개 심볼, {total_batches}개 배치로 나눔")
            
            for i in range(0, len(new_symbols), self.max_symbols_per_subscription):
                batch_symbols = new_symbols[i:i + self.max_symbols_per_subscription]
                batch_num = (i // self.max_symbols_per_subscription) + 1
                
                # 구독 메시지 생성 및 전송
                subscribe_message = await self.create_subscribe_message(batch_symbols)
                if not await self.connection.send_message(json.dumps(subscribe_message)):
                    raise Exception(f"배치 {batch_num}/{total_batches} 구독 메시지 전송 실패")
                
                self.logger.info(f"{EXCHANGE_NAME_KR} 구독 요청 전송 | 배치 {batch_num}/{total_batches}, {len(batch_symbols)}개 심볼")
                
                # 요청 간 짧은 딜레이 추가
                await asyncio.sleep(0.1)
            
            # 첫 번째 구독 시 메시지 수신 루프 시작
            if not self.message_loop_task or self.message_loop_task.done():
                self.logger.info(f"{EXCHANGE_NAME_KR} 첫 번째 구독으로 메시지 수신 루프 시작")
                self.stop_event.clear()
                self.message_loop_task = asyncio.create_task(self.message_loop())
            
            return True
            
        except Exception as e:
            self.logger.error(f"{EXCHANGE_NAME_KR} 구독 중 오류 발생: {str(e)}")
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
            if symbol not in self.subscribed_symbols:
                self.logger.warning(f"{EXCHANGE_NAME_KR} {symbol} 구독 중이 아닙니다.")
                return True
                
            # 부모 클래스의 _cleanup_subscription 메서드 사용
            self._cleanup_subscription(symbol)
            
            # 구독 취소 메시지 생성 및 전송
            unsubscribe_message = await self.create_unsubscribe_message(symbol)
            if not await self.connection.send_message(json.dumps(unsubscribe_message)):
                raise Exception(f"{symbol} 구독 취소 메시지 전송 실패")
                
            self.logger.info(f"{EXCHANGE_NAME_KR} {symbol} 구독 취소 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"{EXCHANGE_NAME_KR} {symbol} 구독 취소 중 오류 발생: {e}")
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
                        self.logger.warning(f"{EXCHANGE_NAME_KR} {sym} 구독 취소 메시지 전송 실패: {e}")
                    
                    # 내부적으로 구독 상태 정리
                    self._cleanup_subscription(sym)
                
                self.logger.info(f"{EXCHANGE_NAME_KR} 모든 심볼 구독 취소 완료")
                
                # 연결 종료 처리
                if hasattr(self.connection, 'disconnect'):
                    try:
                        await asyncio.wait_for(self.connection.disconnect(), timeout=2.0)
                    except (asyncio.TimeoutError, Exception) as e:
                        self.logger.warning(f"{EXCHANGE_NAME_KR} 연결 종료 중 오류: {str(e)}")
                
                self.logger.info(f"{EXCHANGE_NAME_KR} 모든 자원 정리 완료")
                return True
            
            # 특정 심볼 구독 취소
            return await self._unsubscribe_symbol(symbol)
            
        except Exception as e:
            self.logger.error(f"{EXCHANGE_NAME_KR} 구독 취소 중 오류 발생: {e}")
            # 에러 메트릭 업데이트
            self.metrics_manager.record_error(self.exchange_code)
            return False 