"""
업비트 구독 클래스

이 모듈은 업비트 거래소의 웹소켓 구독을 담당하는 클래스를 제공합니다.
"""

import json
import time
import asyncio
import os
import datetime
from typing import Dict, List, Any, Optional, Callable, Union
import websockets.exceptions
from enum import Enum

from crosskimp.ob_collector.orderbook.subscription.base_subscription import (
    BaseSubscription, 
    SnapshotMethod, 
    DeltaMethod,
    SubscriptionStatus
)
from crosskimp.logger.logger import create_raw_logger

# ============================
# 업비트 구독 관련 상수
# ============================
# 거래소 코드
EXCHANGE_CODE = "upbit"

# 웹소켓 설정
REST_URL = "https://api.upbit.com/v1/orderbook"  # https://docs.upbit.com/reference/호가-정보-조회
WS_URL = "wss://api.upbit.com/websocket/v1"     # https://docs.upbit.com/reference/websocket-orderbook

class UpbitSubscription(BaseSubscription):
    """
    업비트 구독 클래스
    
    업비트 거래소의 웹소켓 구독을 담당하는 클래스입니다.
    
    """
    
    def __init__(self, connection, parser=None):
        """
        초기화
        
        Args:
            connection: 웹소켓 연결 객체
            parser: 메시지 파싱 객체 (선택적)
        """
        super().__init__(connection, parser)
        
        # 메시지 처리 통계
        self.message_stats = {
            "total_received": 0,
            "orderbook": 0,
            "error": 0
        }
        
        # 메시지 수신 루프 태스크
        self.message_loop_task = None
        
        # 콜백 딕셔너리 초기화
        self.snapshot_callbacks = {}
        self.error_callbacks = {}
        
        # 구독 상태 관리 딕셔너리 초기화
        self.subscribed_symbols = {}
        
        # raw 로거 초기화
        self.raw_logger = create_raw_logger(EXCHANGE_CODE)
        self.logger.info(f"{EXCHANGE_CODE} raw 로거 초기화 완료")
    
    def _get_snapshot_method(self) -> SnapshotMethod:
        """
        스냅샷 수신 방법 반환
        
        업비트는 웹소켓을 통해 스냅샷을 수신합니다.
        
        Returns:
            SnapshotMethod: WEBSOCKET
        """
        return SnapshotMethod.WEBSOCKET
    
    def _get_delta_method(self) -> DeltaMethod:
        """
        델타 수신 방법 반환
        
        업비트는 델타를 사용하지 않고 항상 전체 스냅샷을 수신합니다.
        
        Returns:
            DeltaMethod: NONE
        """
        return DeltaMethod.NONE
    
    async def create_subscribe_message(self, symbol: Union[str, List[str]]) -> Dict:
        """
        구독 메시지 생성
        
        단일 심볼 또는 심볼 리스트에 대한 구독 메시지를 생성합니다.
        
        Args:
            symbol: 구독할 심볼 또는 심볼 리스트 (예: "BTC" 또는 ["BTC", "ETH"])
            
        Returns:
            Dict: 구독 메시지
        """
        # 심볼이 리스트인지 확인하고 처리
        if isinstance(symbol, list):
            symbols = symbol
        else:
            symbols = [symbol]
            
        # 심볼 형식 변환 (KRW-{symbol})
        market_codes = [f"KRW-{s.upper()}" for s in symbols]
        
        # 구독 메시지 생성
        message = [
            {"ticket": f"upbit_orderbook_{int(time.time())}"},
            {
                "type": "orderbook",
                "codes": market_codes,
                "is_only_realtime": False  # 스냅샷 포함 요청
            },
            {"format": "DEFAULT"}
        ]
        
        symbols_str = ", ".join(symbols) if len(symbols) <= 5 else f"{len(symbols)}개 심볼"
        self.logger.debug(f"[구독 메시지] {symbols_str} 구독 메시지 생성")
        return message
    
    async def create_unsubscribe_message(self, symbol: str) -> Dict:
        """
        구독 취소 메시지 생성
        
        업비트는 별도의 구독 취소 메시지가 없으므로, 
        빈 구독 메시지를 반환하여 기존 구독을 대체합니다.
        
        Args:
            symbol: 구독 취소할 심볼 (예: "BTC")
            
        Returns:
            Dict: 구독 취소 메시지 (빈 메시지)
        """
        # 업비트는 구독 취소 메시지가 없으므로 빈 배열 반환
        return [
            {"ticket": f"upbit_unsubscribe_{int(time.time())}"},
            {"type": "orderbook", "codes": []},
            {"format": "DEFAULT"}
        ]
    
    def is_snapshot_message(self, message: str) -> bool:
        """
        메시지가 스냅샷인지 확인
        
        업비트는 모든 오더북 메시지가 스냅샷 형태이므로,
        메시지 타입이 orderbook인지 확인합니다.
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 스냅샷 메시지인 경우 True
        """
        try:
            # JSON 파싱
            if isinstance(message, bytes):
                message = message.decode('utf-8')
                
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
                
            # orderbook 타입 확인
            is_orderbook = data.get("type") == "orderbook"
            
            if is_orderbook:
                self.logger.info(f"스냅샷 메시지 확인됨: 타임스탬프: {data.get('timestamp', 'N/A')}")
            
            return is_orderbook
            
        except Exception as e:
            self.logger.error(f"스냅샷 메시지 확인 중 오류: {e}")
            return False
    
    def is_delta_message(self, message: str) -> bool:
        """
        델타 메시지인지 확인
        
        업비트는 델타 메시지를 제공하지 않고 항상 스냅샷만 제공합니다.
        따라서 항상 False를 반환합니다.
        
        Args:
            message: 수신된 메시지
            
        Returns:
            bool: 항상 False (업비트는 델타 메시지 없음)
        """
        return False
    
    async def get_rest_snapshot(self, symbol: str) -> Dict:
        """
        REST API를 통해 스냅샷 요청
        
        업비트는 웹소켓으로 스냅샷을 수신하므로 이 메서드는 실제로 사용되지 않지만,
        베이스 클래스의 추상 메서드이므로 구현이 필요합니다.
        
        Args:
            symbol: 심볼 (예: "BTC")
            
        Returns:
            Dict: 빈 스냅샷 데이터
        """
        # 업비트는 웹소켓으로 스냅샷을 수신하므로 REST API는 사용하지 않음
        # 하지만 추상 메서드이므로 구현이 필요
        self.logger.warning(f"업비트는 REST API 스냅샷을 지원하지 않음: {symbol}")
        return {}
    
    async def _run_message_loop(self):
        """
        메시지 수신 루프 실행
        
        웹소켓 연결에서 메시지를 지속적으로 수신하고 처리합니다.
        """
        try:
            self.logger.info("메시지 수신 루프 시작")
            loop_count = 0
            last_log_time = time.time()
            
            while not asyncio.current_task().cancelled():
                # 연결 상태 확인
                # check_connection 메서드 사용
                try:
                    if hasattr(self.connection, 'check_connection'):
                        is_connected = await self.connection.check_connection()
                    elif hasattr(self.connection, 'is_connected'):
                        if callable(self.connection.is_connected):
                            is_connected = await self.connection.is_connected()
                        else:
                            is_connected = self.connection.is_connected
                    else:
                        is_connected = False
                except Exception as e:
                    self.logger.error(f"연결 상태 확인 중 오류: {e}")
                    is_connected = False
                
                if not is_connected:
                    self.logger.error("웹소켓 연결이 끊어짐")
                    await asyncio.sleep(1)  # 잠시 대기 후 재시도
                    continue
                
                try:
                    # 원시 메시지 수신
                    loop_count += 1
                    current_time = time.time()
                    
                    # 10초마다 또는 100번의 루프마다 상태 로깅
                    if loop_count % 100 == 0 or current_time - last_log_time > 10:
                        self.logger.info(f"메시지 수신 루프 실행 중 (루프 카운트: {loop_count}, 총 수신 메시지: {self.message_stats['total_received']}, 오더북 메시지: {self.message_stats['orderbook']})")
                        last_log_time = current_time
                    
                    message = await self.connection.receive_raw()
                    
                    if not message:
                        await asyncio.sleep(0.01)  # 짧은 대기로 CPU 사용량 감소
                        continue
                    
                    # 메시지 처리
                    await self._process_message(message)
                    
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger.error(f"메시지 수신 중 오류: {e}")
                    await asyncio.sleep(0.1)  # 오류 발생시 약간 더 긴 대기
            
        except asyncio.CancelledError:
            self.logger.info("메시지 수신 루프 취소됨")
        except Exception as e:
            self.logger.error(f"메시지 수신 루프 오류: {e}")
            
        finally:
            self.logger.info(f"메시지 수신 루프 종료 (총 수신 메시지: {self.message_stats['total_received']}, 오더북 메시지: {self.message_stats['orderbook']})")
    
    async def _process_message(self, message):
        """
        메시지 처리
        
        Args:
            message: 수신된 메시지
        """
        try:
            # 메시지 통계 업데이트
            self.message_stats["total_received"] += 1
            
            # 바이트 메시지 디코딩
            raw_message = message
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            
            # raw 로거를 사용하여 원본 메시지 로깅
            try:
                # 타임스탬프 추가
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                self.raw_logger.debug(f"[{timestamp}] {message}")
                self.logger.debug(f"원본 메시지 raw_data 디렉토리에 로깅 완료")
            except Exception as e:
                self.logger.error(f"원본 메시지 로깅 실패: {str(e)}")
                
            # 스냅샷 메시지 처리
            if self.is_snapshot_message(message):
                self.message_stats["orderbook"] += 1
                
                # 파서를 통한 메시지 파싱 (반드시 파싱 수행)
                self.logger.debug("파서를 통해 메시지 파싱 시작")
                parsed_data = self.parser.parse_message(message)
                
                # 파싱된 데이터 구조 상세 로깅
                symbol = parsed_data.get('symbol', '')
                self.logger.info(f"파서를 통해 메시지 파싱 완료: {symbol}")
                
                # 데이터 구조 로깅
                self.logger.info(f"파싱된 데이터 키: {list(parsed_data.keys())}")
                
                # bids/asks 구조 확인
                if 'bids' in parsed_data and parsed_data['bids']:
                    bid_sample = parsed_data['bids'][0]
                    self.logger.info(f"bids 샘플 구조: {bid_sample}, 타입: {type(bid_sample)}")
                
                if 'asks' in parsed_data and parsed_data['asks']:
                    ask_sample = parsed_data['asks'][0]
                    self.logger.info(f"asks 샘플 구조: {ask_sample}, 타입: {type(ask_sample)}")
                
                # 타임스탬프와 시퀀스 확인
                self.logger.info(f"timestamp: {parsed_data.get('timestamp')}, sequence: {parsed_data.get('sequence')}")
                
                # 구독 중인 심볼인지 확인
                if symbol in self.subscribed_symbols:
                    self.logger.info(f"[메시지 처리] {symbol} 구독 중인 심볼 확인됨")
                    
                    # 구독 상태 업데이트
                    prev_status = self.subscribed_symbols[symbol]
                    self.subscribed_symbols[symbol] = SubscriptionStatus.SNAPSHOT_RECEIVED
                    self.logger.info(f"[상태 업데이트] {symbol} 상태 변경: {prev_status} -> {SubscriptionStatus.SNAPSHOT_RECEIVED}")
                    
                    # 스냅샷 콜백 호출
                    if symbol in self.snapshot_callbacks:
                        try:
                            self.logger.info(f"[콜백 호출] {symbol} 스냅샷 콜백 호출 시작")
                            # 파싱된 데이터 전달
                            await self.snapshot_callbacks[symbol](symbol, parsed_data)
                            self.logger.info(f"[콜백 완료] {symbol} 스냅샷 콜백 완료")
                        except Exception as e:
                            self.logger.error(f"{symbol} 스냅샷 콜백 호출 실패: {str(e)}")
                            if symbol in self.error_callbacks:
                                self.error_callbacks[symbol](symbol, str(e))
                    else:
                        self.logger.warning(f"[콜백 없음] {symbol} 스냅샷 콜백이 등록되지 않음")
                else:
                    self.logger.warning(f"[구독 없음] {symbol} 구독되지 않은 심볼의 메시지 수신됨")
            else:
                # 오더북이 아닌 다른 메시지 처리
                try:
                    data = json.loads(message)
                    msg_type = data.get("type", "알 수 없음")
                    self.logger.info(f"[기타 메시지] 타입: {msg_type}, 내용: {message[:100]}...")
                except:
                    self.logger.info(f"[기타 메시지] 파싱 불가: {message[:100]}...")
            
        except Exception as e:
            self.logger.error(f"메시지 처리 중 오류 발생: {str(e)}")
            self.message_stats["error"] += 1
    
    async def subscribe(self, symbol, on_snapshot=None, on_delta=None, on_error=None):
        """
        심볼 구독
        
        단일 심볼 또는 심볼 리스트를 구독합니다.
        업비트는 한 번의 요청으로 여러 심볼을 구독할 수 있습니다.
        
        업비트는 항상 스냅샷만 제공하므로 델타 콜백은 무시됩니다.
        
        Args:
            symbol: 구독할 심볼 또는 심볼 리스트
            on_snapshot: 스냅샷 수신 시 호출할 콜백 함수
            on_delta: 델타 수신 시 호출할 콜백 함수 (업비트는 무시됨)
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
                self.logger.warning("구독할 심볼이 없습니다.")
                return False
                
            # 이미 구독 중인 심볼 필터링
            new_symbols = [s for s in symbols if s not in self.subscribed_symbols]
            if not new_symbols:
                self.logger.info("모든 심볼이 이미 구독 중입니다.")
                return True
                
            # 콜백 함수 등록
            for sym in new_symbols:
                if on_snapshot is not None:
                    self.snapshot_callbacks[sym] = on_snapshot
                # 업비트는 델타를 사용하지 않으므로 델타 콜백은 등록하지 않음
                if on_error is not None:
                    self.error_callbacks[sym] = on_error
            
            # 구독 메시지 생성 및 전송
            subscribe_message = await self.create_subscribe_message(new_symbols)
            if not await self.connection.send_message(json.dumps(subscribe_message)):
                raise Exception("구독 메시지 전송 실패")
            
            # 구독 상태 업데이트
            for sym in new_symbols:
                self.subscribed_symbols[sym] = SubscriptionStatus.SUBSCRIBED
            
            self.logger.info(f"구독 성공: {len(new_symbols)}개 심볼")
            
            # 첫 번째 구독 시 메시지 수신 루프 시작
            if not self.message_loop_task:
                self.logger.info("첫 번째 구독으로 메시지 수신 루프 시작")
                self.message_loop_task = asyncio.create_task(self._run_message_loop())
                self.logger.info(f"메시지 수신 루프 태스크 생성됨: {self.message_loop_task}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"구독 중 오류 발생: {str(e)}")
            if on_error is not None:
                if isinstance(symbol, list):
                    for sym in symbol:
                        on_error(sym, str(e))
                else:
                    on_error(symbol, str(e))
            return False
    
    async def close(self) -> None:
        """
        모든 구독 취소 및 자원 정리 (BaseSubscription 메서드 오버라이드)
        """
        try:
            # 모든 구독 취소
            symbols = list(self.subscribed_symbols.keys())
            for symbol in symbols:
                await self.unsubscribe(symbol)
            
            # 메시지 수신 루프 취소
            if self.message_loop_task and not self.message_loop_task.done():
                self.message_loop_task.cancel()
                try:
                    await self.message_loop_task
                except asyncio.CancelledError:
                    pass
            
            # 태스크 취소
            for task in self.tasks.values():
                task.cancel()
            self.tasks.clear()
            
            # REST 세션 종료 (사용 안함)
            if self.rest_session:
                await self.rest_session.close()
                self.rest_session = None
            
            self.status = SubscriptionStatus.CLOSED
            self.logger.info("모든 구독 취소 및 자원 정리 완료")
            
        except Exception as e:
            self.logger.error(f"자원 정리 중 오류 발생: {e}")
            self.status = SubscriptionStatus.ERROR 

    async def unsubscribe(self, symbol: str) -> bool:
        """
        구독 취소
        
        업비트는 구독 취소 메시지를 별도로 보내지 않고, 연결을 끊거나 새로운 구독 메시지를 보내는 방식으로 처리합니다.
        따라서 이 메서드는 내부적으로 구독 상태만 업데이트합니다.
        
        Args:
            symbol: 구독 취소할 심볼
            
        Returns:
            bool: 구독 취소 성공 여부
        """
        try:
            if symbol not in self.subscribed_symbols:
                self.logger.warning(f"{symbol} 구독 중이 아닙니다.")
                return True
            
            # 상태 업데이트
            del self.subscribed_symbols[symbol]
            self.logger.info(f"{symbol} 구독 취소 완료")
            
            # 모든 구독이 취소된 경우 상태 업데이트
            if not self.subscribed_symbols:
                self.status = SubscriptionStatus.IDLE
                
            return True
            
        except Exception as e:
            self.logger.error(f"{symbol} 구독 취소 중 오류 발생: {e}")
            return False 