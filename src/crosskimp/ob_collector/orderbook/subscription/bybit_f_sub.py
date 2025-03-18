"""
바이빗 선물 구독 클래스

이 모듈은 바이빗 선물 거래소의 웹소켓 연결을 통해 오더북 데이터를 구독하는 클래스를 제공합니다.
"""

import asyncio
import json
import time
import aiohttp
from typing import Dict, List, Optional, Union, Any, Callable, Tuple
import logging

from crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription, SnapshotMethod, DeltaMethod
from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG
from crosskimp.ob_collector.orderbook.connection.bybit_f_cn import BybitFutureWebSocketConnector
from crosskimp.ob_collector.orderbook.parser.bybit_f_pa import BybitFutureParser

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 바이빗 선물 구독 관련 상수
# ============================
# 기본 설정
EXCHANGE_CODE = Exchange.BYBIT_FUTURE.value  # 거래소 코드
BYBIT_FUTURE_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이빗 선물 설정

# 오더북 관련 설정
DEFAULT_DEPTH = BYBIT_FUTURE_CONFIG.get("default_depth", 50)  # 기본 오더북 깊이

class BybitFutureSubscription(BaseSubscription):
    """
    바이빗 선물 구독 클래스
    
    바이빗 선물 거래소의 웹소켓 연결을 통해 오더북 데이터를 구독하는 클래스입니다.
    
    특징:
    - 메시지 형식: 스냅샷 및 델타 업데이트 지원
    - REST API를 통한 스냅샷 요청
    - 웹소켓을 통한 델타 업데이트 수신
    """
    def __init__(self, connection: BybitFutureWebSocketConnector, parser=None):
        """
        바이빗 선물 구독 초기화
        
        Args:
            connection: 바이빗 선물 웹소켓 연결 객체
            parser: 선택적 파서 객체 (기본값: None)
        """
        # 파서가 없는 경우 기본 파서 생성
        if parser is None:
            parser = BybitFutureParser()
            
        super().__init__(connection, EXCHANGE_CODE, parser)
        
        # 오더북 설정
        self.depth_level = DEFAULT_DEPTH
        
        # REST API 세션
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 구독 상태 관리
        self.snapshot_received = set()  # 스냅샷을 받은 심볼 목록
        self.snapshot_pending = set()   # 스냅샷 요청 대기 중인 심볼 목록
        
        # 메시지 처리 통계
        self.message_stats = {
            "total_received": 0,
            "ping_pong": 0,
            "snapshot": 0,
            "delta": 0,
            "processed": 0
        }
        
        # 로깅 설정
        self._setup_raw_logging()
        
        # 메시지 처리 태스크
        self.message_task = None
    
    def _setup_raw_logging(self):
        """
        원시 메시지 로깅 설정
        """
        self.log_raw_messages = False
        try:
            # 설정에서 로깅 활성화 여부 확인
            if hasattr(self.connection, 'settings'):
                self.log_raw_messages = self.connection.settings.get('logging', {}).get('raw_messages', False)
                
            if self.log_raw_messages:
                self.log_info(f"원시 메시지 로깅 활성화")
            else:
                self.log_debug(f"원시 메시지 로깅 비활성화")
        except Exception as e:
            self.log_error(f"로깅 설정 실패: {str(e)}")
            self.log_raw_messages = False

    def _get_snapshot_method(self) -> SnapshotMethod:
        """
        스냅샷 수신 방법 반환
        
        Returns:
            SnapshotMethod: 스냅샷 수신 방법 (REST)
        """
        return SnapshotMethod.REST

    def _get_delta_method(self) -> DeltaMethod:
        """
        델타 수신 방법 반환
        
        Returns:
            DeltaMethod: 델타 수신 방법 (WEBSOCKET)
        """
        return DeltaMethod.WEBSOCKET

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

    def is_snapshot_message(self, message: str) -> bool:
        """
        스냅샷 메시지 여부 확인
        
        Args:
            message: 수신된 웹소켓 메시지
            
        Returns:
            bool: 스냅샷 메시지인 경우 True, 아니면 False
        """
        try:
            data = json.loads(message)
            
            # 구독 응답 또는 핑/퐁 메시지는 스냅샷이 아님
            if data.get("op") == "subscribe" or self.parser.is_pong_message(data):
                return False
                
            # 토픽과 데이터가 있는지 확인
            if "topic" not in data or "data" not in data:
                return False
                
            # 오더북 토픽인지 확인
            topic = data.get("topic", "")
            if "orderbook" not in topic:
                return False
                
            # 메시지 타입이 스냅샷인지 확인
            return data.get("type", "").lower() == "snapshot"
            
        except Exception as e:
            self.log_error(f"스냅샷 메시지 확인 실패: {str(e)}")
            return False

    def is_delta_message(self, message: str) -> bool:
        """
        델타 메시지 여부 확인
        
        Args:
            message: 수신된 웹소켓 메시지
            
        Returns:
            bool: 델타 메시지인 경우 True, 아니면 False
        """
        try:
            data = json.loads(message)
            
            # 구독 응답 또는 핑/퐁 메시지는 델타가 아님
            if data.get("op") == "subscribe" or self.parser.is_pong_message(data):
                return False
                
            # 토픽과 데이터가 있는지 확인
            if "topic" not in data or "data" not in data:
                return False
                
            # 오더북 토픽인지 확인
            topic = data.get("topic", "")
            if "orderbook" not in topic:
                return False
                
            # 메시지 타입이 델타인지 확인 (기본값은 델타로 간주)
            return data.get("type", "delta").lower() == "delta"
            
        except Exception as e:
            self.log_error(f"델타 메시지 확인 실패: {str(e)}")
            return False

    def log_raw_message(self, message: str) -> None:
        """
        원시 메시지 로깅
        
        Args:
            message: 로깅할 원시 메시지
        """
        if not self.log_raw_messages:
            return
            
        try:
            # 메시지 카운터 증가
            if not hasattr(self, '_raw_log_count'):
                self._raw_log_count = 0
            self._raw_log_count += 1
            
            # 주기적으로만 로깅 (100개마다)
            if self._raw_log_count % 100 == 0:
                data = json.loads(message)
                if "topic" in data:
                    topic = data.get("topic", "")
                    parts = topic.split(".")
                    if len(parts) >= 3:
                        symbol = parts[-1].replace("USDT", "")
                        self.log_debug(f"{symbol} 원시 메시지 (#{self._raw_log_count}): {message[:150]}...")
        except Exception as e:
            # 로깅 중 오류 발생 시 무시 (로깅이 핵심 기능에 영향을 주지 않도록)
            pass

    async def get_rest_snapshot(self, symbol: str) -> Dict:
        """
        REST API를 통해 스냅샷 요청
        
        Args:
            symbol: 스냅샷을 요청할 심볼
            
        Returns:
            Dict: 스냅샷 데이터 또는 빈 딕셔너리
        """
        try:
            if not self.session:
                self.session = aiohttp.ClientSession()
            
            # 요청 뎁스를 200으로 증가 (최대값)
            request_depth = 200
            market = f"{symbol}USDT"
            
            url = (f"https://api.bybit.com/v5/market/orderbook"
                   f"?category=linear&symbol={market}"
                   f"&limit={request_depth}")
            
            self.log_info(f"{symbol} 스냅샷 요청 | 요청 뎁스: {request_depth}")
            
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    # 원시 메시지 로깅
                    if self.log_raw_messages:
                        self.log_debug(f"{symbol} 스냅샷 응답: {json.dumps(data)[:200]}...")
                    
                    if data.get("retCode") == 0:
                        result = data.get("result", {})
                        
                        # 받은 데이터의 뎁스 로깅
                        bids_count = len(result.get("b", []))
                        asks_count = len(result.get("a", []))
                        self.log_info(
                            f"{symbol} 스냅샷 응답 | "
                            f"매수: {bids_count}건, 매도: {asks_count}건"
                        )
                        
                        # 파서를 사용하여 스냅샷 데이터 파싱
                        parsed_data = self.parser.parse_snapshot_data(result, market)
                        if parsed_data:
                            self.snapshot_received.add(symbol)
                            self.snapshot_pending.discard(symbol)
                        return parsed_data
                    else:
                        self.log_error(f"{symbol} 스냅샷 응답 에러: {data}")
                else:
                    self.log_error(f"{symbol} 스냅샷 요청 실패: status={resp.status}")
        except Exception as e:
            self.log_error(f"{symbol} 스냅샷 요청 예외: {e}")
        return {}

    async def _on_message(self, message: str) -> None:
        """
        메시지 수신 및 처리
        
        Args:
            message: 수신된 웹소켓 메시지
        """
        try:
            # 원시 메시지 로깅
            self.log_raw_message(message)
            
            # 메시지 통계 업데이트
            self.message_stats["total_received"] += 1
            
            # JSON 파싱
            data = json.loads(message)
            
            # 핑/퐁 메시지 처리
            if data.get("op") == "pong" or (data.get("ret_msg") == "pong" and data.get("op") == "ping"):
                self.message_stats["ping_pong"] += 1
                return

            # 구독 응답 처리
            if data.get("op") == "subscribe":
                self.log_debug(f"구독 응답 수신: {data}")
                return
                
            # 오더북 메시지 처리
            if "topic" in data and "data" in data:
                topic = data.get("topic", "")
                if "orderbook" in topic:
                    parts = topic.split(".")
                    if len(parts) >= 3:
                        symbol = parts[-1].replace("USDT", "")
                        msg_type = data.get("type", "delta").lower()  # "snapshot" or "delta"
                        
                        # 스냅샷이나 델타 메시지인 경우 파싱 및 콜백 호출
                        if msg_type == "snapshot":
                            self.message_stats["snapshot"] += 1
                            self.snapshot_received.add(symbol)
                            self.snapshot_pending.discard(symbol)
                            
                            # 파서로 메시지 파싱
                            parsed_data = self.parser.parse_message(message)
                            if parsed_data:
                                await self._call_snapshot_callback(symbol, parsed_data)
                                self.message_stats["processed"] += 1
                            
                        elif msg_type == "delta":
                            self.message_stats["delta"] += 1
                            
                            # 파서로 메시지 파싱
                            parsed_data = self.parser.parse_message(message)
                            if parsed_data:
                                await self._call_delta_callback(symbol, parsed_data)
                                self.message_stats["processed"] += 1
                                
                        # 메시지 처리 통계 로깅 (1000개마다)
                        if self.message_stats["total_received"] % 1000 == 0:
                            self.log_info(
                                f"메시지 처리 통계 | 총 수신={self.message_stats['total_received']:,}개, "
                                f"스냅샷={self.message_stats['snapshot']:,}개, "
                                f"델타={self.message_stats['delta']:,}개, "
                                f"처리됨={self.message_stats['processed']:,}개"
                            )
        except json.JSONDecodeError:
            self.log_error(f"JSON 파싱 실패: {message[:100]}...")
        except Exception as e:
            self.log_error(f"메시지 처리 중 오류: {str(e)}")

    def _validate_timestamp(self, symbol: str, timestamp: int) -> bool:
        """
        타임스탬프 유효성 검사
        
        Args:
            symbol: 심볼
            timestamp: 검증할 타임스탬프
            
        Returns:
            bool: 유효한 타임스탬프인 경우 True, 아니면 False
        """
        # 현재 시간과 비교하여 미래 타임스탬프인지 확인
        current_time = int(time.time() * 1000)
        if timestamp > current_time + 5000:  # 5초 이상 미래의 타임스탬프는 의심스러움
            self.log_warning(f"{symbol} 타임스탬프 유효성 검사 실패: 미래 타임스탬프 ({timestamp} > {current_time})")
            return False
            
        # 과거 타임스탬프인지 확인 (30초 이상 과거면 의심스러움)
        if current_time - timestamp > 30000:
            self.log_warning(f"{symbol} 타임스탬프 유효성 검사 실패: 오래된 타임스탬프 ({current_time - timestamp}ms 지남)")
            return False
            
        return True

    async def subscribe(self, symbol, on_snapshot=None, on_delta=None, on_error=None):
        """
        오더북 구독 시작
        
        Args:
            symbol: 구독할 심볼
            on_snapshot: 스냅샷 콜백 함수
            on_delta: 델타 콜백 함수
            on_error: 오류 콜백 함수
            
        Returns:
            bool: 구독 성공 여부
        """
        try:
            # 콜백 등록
            self._register_callbacks(symbol, on_snapshot, on_delta, on_error)
            
            # 심볼 리스트로 변환
            if isinstance(symbol, list):
                symbols = symbol
            else:
                symbols = [symbol]
                
            # 심볼별 처리
            success_count = 0
            for sym in symbols:
                try:
                    # 구독 메시지 생성 및 전송 준비
                    subscribe_msg = await self.create_subscribe_message(sym)
                    
                    # 메시지 전송 (연결이 없거나 연결되지 않은 경우 연결 시도)
                    if not self.connection.is_connected:
                        self.log_info(f"{sym} 구독 전 연결 시도")
                        await self.connection.connect()
                        
                    # 연결 확인 후 구독 메시지 전송
                    if self.connection.is_connected:
                        await self.connection.send_message(json.dumps(subscribe_msg))
                        self.log_info(f"{sym} 구독 메시지 전송 완료")
                        
                        # 스냅샷 요청 예약
                        self.snapshot_pending.add(sym)
                        asyncio.create_task(self._request_snapshot_task(sym))
                        
                        success_count += 1
                    else:
                        self.log_error(f"{sym} 구독 실패: 연결되지 않음")
                        if on_error:
                            await on_error(sym, "웹소켓 연결 실패")
                    
                except Exception as e:
                    self.log_error(f"{sym} 구독 중 예외 발생: {str(e)}")
                    if on_error:
                        await on_error(sym, f"구독 오류: {str(e)}")
                
            # 메시지 처리 루프 시작 (이미 실행 중인 경우 생략)
            if success_count > 0 and (self.message_task is None or self.message_task.done()):
                self.message_task = asyncio.create_task(self.message_loop())
                
            return success_count > 0
            
        except Exception as e:
            self.log_error(f"구독 처리 중 예외 발생: {str(e)}")
            if on_error:
                await on_error(symbol if isinstance(symbol, str) else "multiple", f"구독 처리 오류: {str(e)}")
            return False

    async def _request_snapshot_task(self, symbol: str, retry_count: int = 3) -> None:
        """
        스냅샷 요청 태스크
        
        Args:
            symbol: 스냅샷을 요청할 심볼
            retry_count: 재시도 횟수
        """
        # 짧은 대기 후 시작 (구독 요청이 처리될 시간 부여)
        await asyncio.sleep(0.5)
        
        # 이미 스냅샷을 받은 경우 생략
        if symbol in self.snapshot_received:
            self.log_debug(f"{symbol} 이미 스냅샷 수신됨 (웹소켓)")
            return
            
        # 스냅샷 요청 중인 경우 대기
        attempt = 0
        while attempt < retry_count:
            try:
                # 스냅샷 요청
                self.log_info(f"{symbol} 스냅샷 요청 중 (시도 {attempt + 1}/{retry_count})")
                snapshot_data = await self.get_rest_snapshot(symbol)
                
                # 스냅샷 요청 성공
                if snapshot_data and 'symbol' in snapshot_data:
                    self.snapshot_received.add(symbol)
                    self.snapshot_pending.discard(symbol)
                    
                    # 스냅샷 콜백 호출
                    symbol_clean = snapshot_data['symbol']
                    await self._call_snapshot_callback(symbol_clean, snapshot_data)
                    
                    self.log_info(f"{symbol} 스냅샷 요청 성공")
                    return
                    
                # 스냅샷 요청 실패 시 재시도
                self.log_warning(f"{symbol} 스냅샷 요청 실패, 재시도 중")
                attempt += 1
                await asyncio.sleep(1)  # 1초 대기 후 재시도
                
            except Exception as e:
                self.log_error(f"{symbol} 스냅샷 요청 중 예외: {str(e)}")
                attempt += 1
                await asyncio.sleep(1)  # 1초 대기 후 재시도
                
        # 모든 시도 실패
        self.log_error(f"{symbol} 스냅샷 요청 최종 실패 ({retry_count}회 시도)")
        
        # 오류 콜백 호출
        callback = self._get_error_callback(symbol)
        if callback:
            try:
                await callback(symbol, f"스냅샷 요청 실패 ({retry_count}회 시도)")
            except Exception as e:
                self.log_error(f"{symbol} 오류 콜백 호출 실패: {str(e)}")

    def _is_connected(self) -> bool:
        """
        연결 상태 확인
        
        Returns:
            bool: 연결된 상태인 경우 True, 아니면 False
        """
        return self.connection.is_connected if self.connection else False

    async def _cancel_tasks(self) -> None:
        """백그라운드 태스크 취소"""
        if self.message_task and not self.message_task.done():
            self.message_task.cancel()
            try:
                await self.message_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.log_error(f"메시지 태스크 취소 중 오류: {str(e)}")

    async def _cancel_message_loop(self) -> None:
        """메시지 루프 취소"""
        await self._cancel_tasks()
        self.message_task = None

    async def _unsubscribe_symbol(self, symbol: str) -> bool:
        """
        개별 심볼 구독 해제
        
        Args:
            symbol: 구독 해제할 심볼
            
        Returns:
            bool: 구독 해제 성공 여부
        """
        if not self._is_connected():
            self.log_error(f"{symbol} 구독 해제 실패: 연결되지 않음")
            return False
            
        try:
            # 구독 해제 메시지 생성 및 전송
            unsubscribe_msg = await self.create_unsubscribe_message(symbol)
            await self.connection.send_message(json.dumps(unsubscribe_msg))
            
            # 구독 정리
            self._cleanup_subscription(symbol)
            
            # 스냅샷 관련 상태 정리
            self.snapshot_received.discard(symbol)
            self.snapshot_pending.discard(symbol)
            
            self.log_info(f"{symbol} 구독 해제 완료")
            return True
            
        except Exception as e:
            self.log_error(f"{symbol} 구독 해제 중 오류: {str(e)}")
            return False

    async def unsubscribe(self, symbol: Optional[str] = None) -> bool:
        """
        구독 해제
        
        Args:
            symbol: 구독 해제할 심볼 (None인 경우 모든 심볼 구독 해제)
            
        Returns:
            bool: 구독 해제 성공 여부
        """
        try:
            # 개별 심볼 구독 해제
            if symbol:
                return await self._unsubscribe_symbol(symbol)
                
            # 모든 심볼 구독 해제
            success = True
            symbols = list(self._get_all_subscribed_symbols())
            
            self.log_info(f"모든 심볼 구독 해제 ({len(symbols)}개)")
            
            # 각 심볼별 구독 해제
            for sym in symbols:
                result = await self._unsubscribe_symbol(sym)
                if not result:
                    success = False
                    
            # 메시지 루프 취소
            await self._cancel_message_loop()
            
            # REST API 세션 종료
            if self.session:
                await self.session.close()
                self.session = None
                
            return success
            
        except Exception as e:
            self.log_error(f"구독 해제 중 오류: {str(e)}")
            return False 