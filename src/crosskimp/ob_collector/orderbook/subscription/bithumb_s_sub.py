"""
빗썸 구독 클래스

이 모듈은 빗썸 거래소의 웹소켓 구독을 담당하는 클래스를 제공합니다.
"""

import json
import asyncio
import datetime
import time
from typing import Dict, List, Optional, Union, Any

from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from crosskimp.logger.logger import create_raw_logger
from crosskimp.config.constants_v3 import Exchange, LOG_SUBDIRS
from crosskimp.ob_collector.orderbook.validator.validators import BaseOrderBookValidator

# 빗썸 REST API 및 웹소켓 관련 설정
WS_URL = "wss://ws-api.bithumb.com/websocket/v1"  # 웹소켓 URL
DEFAULT_DEPTH = 15  # 기본 오더북 깊이

class BithumbSubscription(BaseSubscription):
    """
    빗썸 구독 클래스
    
    빗썸 거래소의 웹소켓 구독을 담당하는 클래스입니다.
    
    특징:
    - REST API를 통한 초기 스냅샷 요청
    - 웹소켓을 통한 델타 업데이트 처리
    - 완전한 오더북 상태 유지
    """
    
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
        self.log_raw_data = True
        self.raw_logger = None
        self._setup_raw_logging()
        
        # 오더북 데이터 저장소
        self.orderbooks = {}  # symbol -> orderbook_data
        
        # 타임스탬프 추적 (타임스탬프와 카운터를 함께 저장)
        self.last_timestamps = {}  # symbol -> {"timestamp": timestamp, "counter": count}
        
        # 오더북 검증기 초기화
        self.validator = BaseOrderBookValidator(Exchange.BITHUMB.value)
        
        # 출력용 오더북 뎁스 설정
        self.output_depth = 10  # 로깅에 사용할 오더북 뎁스

    def _setup_raw_logging(self):
        """Raw 데이터 로깅 설정"""
        try:
            raw_data_dir = LOG_SUBDIRS['raw_data']
            log_dir = raw_data_dir / Exchange.BITHUMB.value
            log_dir.mkdir(exist_ok=True, parents=True)
            
            current_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            self.log_file_path = log_dir / f"{Exchange.BITHUMB.value}_raw_{current_datetime}.log"
            
            self.raw_logger = create_raw_logger(Exchange.BITHUMB.value)
            self.log_info("raw 로거 초기화 완료")
        except Exception as e:
            self.log_error(f"Raw 로깅 설정 실패: {str(e)}")
            self.log_raw_data = False
    
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
    
    async def create_unsubscribe_message(self, symbol: str) -> Dict:
        """
        구독 취소 메시지 생성
        
        Args:
            symbol: 구독 취소할 심볼
            
        Returns:
            Dict: 구독 취소 메시지
        """
        code = f"KRW-{symbol.upper()}"
        
        return [
            {"ticket": f"bithumb_unsubscribe_{int(datetime.datetime.now().timestamp() * 1000)}"},
            {"type": "orderbook", "codes": [code]},
            {"format": "DEFAULT"}
        ]
    
    async def send_subscribe_message(self, symbols: List[str]) -> bool:
        """구독 메시지 전송"""
        try:
            msg = await self.create_subscribe_message(symbols)
            json_msg = json.dumps(msg)
            success = await self.connection.send_message(json_msg)
            
            if not success:
                self.log_error(f"구독 메시지 전송 실패: {symbols[:3]}...")
                return False
            
            self.log_info(f"구독 메시지 전송 성공: {symbols}")
            return True
        except Exception as e:
            self.log_error(f"구독 메시지 전송 중 오류: {str(e)}")
            return False
    
    async def subscribe(self, symbol, on_snapshot=None, on_delta=None, on_error=None):
        """
        심볼 구독
        
        단일 심볼 또는 심볼 리스트를 구독합니다.
        
        Args:
            symbol: 구독할 심볼 또는 심볼 리스트
            on_snapshot: 스냅샷 수신 시 호출할 콜백 함수
            on_delta: 델타 수신 시 호출할 콜백 함수
            on_error: 에러 발생 시 호출할 콜백 함수
            
        Returns:
            bool: 구독 성공 여부
        """
        try:
            if isinstance(symbol, list):
                symbols = symbol
            else:
                symbols = [symbol]
                
            if not symbols:
                self.log_warning("구독할 심볼이 없습니다.")
                return False
                
            new_symbols = [s for s in symbols if s not in self.subscribed_symbols]
            if not new_symbols:
                self.log_info("모든 심볼이 이미 구독 중입니다.")
                return True
                
            for sym in new_symbols:
                if on_snapshot is not None:
                    self.snapshot_callbacks[sym] = on_snapshot
                if on_delta is not None:
                    self.delta_callbacks[sym] = on_delta
                elif on_snapshot is not None:
                    self.delta_callbacks[sym] = on_snapshot
                if on_error is not None:
                    self.error_callbacks[sym] = on_error
                
                self.subscribed_symbols[sym] = True
            
            if not self._is_connected():
                self.log_info("웹소켓 연결 시작")
                if not await self.connection.connect():
                    raise Exception("웹소켓 연결 실패")
                self.metrics_manager.update_connection_state(self.exchange_code, "connected")
            
            self.log_info(f"구독 시작 | 총 {len(new_symbols)}개 심볼")
            
            success = await self.send_subscribe_message(new_symbols)
            if not success:
                raise Exception("구독 메시지 전송 실패")
            
            if not self.message_loop_task or self.message_loop_task.done():
                self.log_info("메시지 수신 루프 시작")
                self.stop_event.clear()
                self.message_loop_task = asyncio.create_task(self.message_loop())
            
            return True
            
        except Exception as e:
            self.log_error(f"구독 중 오류 발생: {str(e)}")
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
        return self.connection and self.connection.is_connected
    
    async def _on_message(self, message: str) -> None:
        """메시지 수신 콜백"""
        try:
            # 메시지 처리 시작 시간 측정
            start_time = time.time()
            
            # 원시 메시지 로깅
            if self.log_raw_data and self.raw_logger:
                current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                # 디버그 로그에 원시 메시지 저장
                self.raw_logger.debug(f"[{current_time}] RAW: {message}")
                
                # 파일에도 원시 메시지 저장
                with open("logs/250319_193403_bithumb_raw_logger.log", "a", encoding="utf-8") as f:
                    f.write(f"[{current_time}] {message}\n")
                
            # 메시지 크기 측정 및 메트릭 업데이트
            message_size = len(message)
            self.metrics_manager.update_metric(self.exchange_code, "message")
            self.metrics_manager.update_metric(self.exchange_code, "bytes", size=message_size)
                
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
                        # 메트릭 업데이트 - 베이스 클래스의 메서드 사용
                        if result.is_valid:
                            self._update_metrics(self.exchange_code, start_time, "snapshot", parsed_data)
                    else:
                        result = await self.validator.update(symbol, parsed_data)
                        # 메트릭 업데이트 - 베이스 클래스의 메서드 사용
                        if result.is_valid:
                            self._update_metrics(self.exchange_code, start_time, "delta", parsed_data)
                        
                    if result.is_valid:
                        orderbook = self.validator.get_orderbook(symbol)
                        self._log_orderbook_data(symbol, orderbook)
                        
                        if is_snapshot and symbol in self.snapshot_callbacks:
                            await self.snapshot_callbacks[symbol](symbol, orderbook)
                        elif not is_snapshot and symbol in self.delta_callbacks:
                            await self.delta_callbacks[symbol](symbol, orderbook)
                    else:
                        self.log_error(f"{symbol} 오더북 검증 실패: {result.errors}")
            except Exception as e:
                self.log_error(f"{symbol} 오더북 검증 중 오류: {str(e)}")
                self.metrics_manager.record_error(self.exchange_code)
                
                if symbol in self.error_callbacks:
                    await self.error_callbacks[symbol](symbol, str(e))
                    
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {str(e)}")
            self.metrics_manager.record_error(self.exchange_code)
    
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
            
            # 메트릭 업데이트 - 오더북 메시지 처리
            self.metrics_manager.update_metric(self.exchange_code, "orderbook")
            
            # 처리 시간 측정 및 업데이트
            processing_time_ms = (time.time() - start_time) * 1000
            self.metrics_manager.update_metric(self.exchange_code, "processing_time", time_ms=processing_time_ms)
            
            return result
            
        except Exception as e:
            self.log_error(f"메시지 파싱 실패: {str(e)}")
            return None
    
    def _log_orderbook_data(self, symbol: str, orderbook: Dict) -> None:
        """처리된 오더북 데이터 로깅"""
        if self.raw_logger:
            try:
                current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                
                bids = orderbook.get('bids', [])[:self.output_depth]
                asks = orderbook.get('asks', [])[:self.output_depth]
                
                log_data = {
                    "exchangename": self.exchange_code,
                    "symbol": symbol,
                    "bids": bids,
                    "asks": asks,
                    "timestamp": orderbook.get('timestamp'),
                    "sequence": orderbook.get('sequence')
                }
                
                self.raw_logger.debug(f"[{current_time}] 매수 ({len(bids)}) / 매도 ({len(asks)}) {json.dumps(log_data)}")
            except Exception as e:
                self.log_error(f"오더북 데이터 로깅 실패: {str(e)}")
    
    def log_raw_message(self, message: str) -> None:
        """원시 메시지 로깅"""
        try:
            if self.log_raw_data and self.raw_logger:
                current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                self.raw_logger.debug(f"[{current_time}] RAW: {message}")
                
                # 파일에도 원시 메시지 저장
                with open("logs/250319_193403_bithumb_raw_logger.log", "a", encoding="utf-8") as f:
                    f.write(f"[{current_time}] {message}\n")
        except Exception as e:
            self.log_error(f"원시 메시지 로깅 실패: {str(e)}")
    
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
        """특정 심볼 구독 취소"""
        try:
            if symbol not in self.subscribed_symbols:
                return True
                
            self._cleanup_subscription(symbol)
            
            unsubscribe_message = await self.create_unsubscribe_message(symbol)
            if not await self.connection.send_message(json.dumps(unsubscribe_message)):
                raise Exception(f"{symbol} 구독 취소 메시지 전송 실패")
                
            self.log_info(f"{symbol} 구독 취소 완료")
            return True
            
        except Exception as e:
            self.log_error(f"{symbol} 구독 취소 중 오류 발생: {e}")
            return False
    
    async def unsubscribe(self, symbol: Optional[str] = None) -> bool:
        """구독 취소"""
        try:
            if symbol is None:
                self.stop_event.set()
                await self._cancel_message_loop()
                await self._cancel_tasks()
                
                symbols = list(self.subscribed_symbols.keys())
                
                for sym in symbols:
                    try:
                        unsubscribe_message = await self.create_unsubscribe_message(sym)
                        if self.connection and self.connection.is_connected:
                            await self.connection.send_message(json.dumps(unsubscribe_message))
                    except Exception:
                        pass
                    
                    self._cleanup_subscription(sym)
                
                if hasattr(self.connection, 'disconnect'):
                    try:
                        await asyncio.wait_for(self.connection.disconnect(), timeout=2.0)
                    except Exception:
                        pass
                
                return True
            
            return await self._unsubscribe_symbol(symbol)
            
        except Exception as e:
            self.log_error(f"구독 취소 중 오류 발생: {e}")
            self.metrics_manager.record_error(self.exchange_code)
            return False

    async def start_message_loop(self) -> None:
        """메시지 수신 루프 시작"""
        if self.message_loop_task and not self.message_loop_task.done():
            self.log_warning(f"메시지 수신 루프가 이미 실행 중입니다.")
            return
            
        # 이벤트 초기화
        self.stop_event.clear()
        
        # 부모 클래스의 message_loop 메서드 사용 (오버라이드 하지 않음)
        self.message_loop_task = asyncio.create_task(self.message_loop())
        self.log_info(f"메시지 수신 루프 시작됨")
