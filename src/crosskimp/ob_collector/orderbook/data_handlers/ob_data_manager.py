"""
오더북 데이터 관리자 모듈

모든 거래소의 오더북 데이터를 중앙에서 관리하기 위한 클래스를 제공합니다.
현재는 로깅 집중화에 초점을 맞추고 있으며, 향후 C++ 연동 등으로 확장 가능합니다.

추가로 중앙화할 수 있는 기능들:
1. 심볼 관리: 모든 거래소의 심볼 매핑과 정규화를 중앙에서 처리
2. 오더북 검증: 오더북 데이터 검증 로직을 중앙에서 처리
3. 스냅샷 관리: REST API 호출과 스냅샷 요청 로직을 중앙에서 관리
4. 메모리 관리: 오더북 데이터 캐싱과 메모리 사용량 최적화
5. 데이터 변환: 다양한 형식으로의 데이터 변환 기능 (예: 웹소켓, gRPC, C++ 등)
"""

import json
import time
import datetime
import redis
import asyncio
from typing import Dict, List, Any, Optional, Callable, Set

from crosskimp.common.logger.logger import get_unified_logger, create_raw_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR
from crosskimp.common.redis import RedisConfig, RedisChannels, RedisKeys

# 로깅 설정 상수 - 필요에 따라 ON/OFF 가능
ENABLE_RAW_MESSAGE_LOGGING = False   # 원시 웹소켓 메시지 로깅 여부
ENABLE_ORDERBOOK_LOGGING = True    # 처리된 오더북 데이터 로깅 여부
ENABLE_CALLBACK_EXECUTION = False   # 콜백 함수 실행 여부
ENABLE_REDIS_PUBLISHING = True     # Redis 발행 활성화 여부

# 기존 하드코딩된 Redis 설정 제거 및 RedisConfig 임포트 사용

class OrderbookDataManager:
    """
    오더북 데이터 관리자 클래스
    
    모든 거래소의 오더북 데이터를 중앙에서 관리하고 로깅합니다.
    향후 C++ 연동 등을 위한 확장 지점이 될 수 있습니다.
    """
    
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        self.raw_loggers = {}  # 거래소별 raw_logger 저장
        self.callbacks = {}  # 콜백 함수 모음 - 향후 C++ 연동 등에 사용
        self.registered_exchanges = set()  # 등록된 거래소 목록
        self.message_counts = {}  # 거래소별 메시지 수 집계
        self.orderbook_message_counts = {}  # 거래소별 오더북 메시지 수 집계
        self.total_messages = 0  # 전체 메시지 수
        self.start_time = time.time()  # 시작 시간
        self.stats_interval = 20  # 통계 출력 주기 (초)
        
        # 이전 통계 값 저장용 변수 추가
        self._prev_counts = {}
        self._prev_time = time.time()
        
        # 오류 카운팅 추가
        self.error_counts = {}
        
        # 통계 타이머 관련 변수
        self.stats_timer_task = None  # 통계 출력용 타이머 태스크
        self.stats_running = False    # 통계 타이머 실행 상태
        
        # Redis 클라이언트 초기화
        if ENABLE_REDIS_PUBLISHING:
            try:
                self.redis_client = redis.Redis(
                    host=RedisConfig.HOST, 
                    port=RedisConfig.PORT, 
                    db=RedisConfig.DB,
                    password=RedisConfig.PASSWORD,
                    socket_timeout=RedisConfig.SOCKET_TIMEOUT,
                    socket_connect_timeout=RedisConfig.SOCKET_CONNECT_TIMEOUT,
                    socket_keepalive=RedisConfig.SOCKET_KEEPALIVE,
                    retry_on_timeout=RedisConfig.RETRY_ON_TIMEOUT
                )
                # Redis 연결 확인
                self.redis_client.ping()
                self.logger.info("Redis 클라이언트 초기화 완료")
            except Exception as e:
                self.redis_client = None
                self.logger.error(f"Redis 클라이언트 초기화 실패: {str(e)}")
        else:
            self.redis_client = None
        
        self.logger.info("오더북 데이터 관리자 초기화")
    
    def register_exchange(self, exchange_code: str) -> None:
        """
        거래소 등록
        
        Args:
            exchange_code: 거래소 코드 (예: "binance_future")
        """
        if exchange_code not in self.raw_loggers:
            self.raw_loggers[exchange_code] = create_raw_logger(exchange_code)
            self.registered_exchanges.add(exchange_code)
            self.message_counts[exchange_code] = 0
            self.orderbook_message_counts[exchange_code] = 0
            self.error_counts[exchange_code] = 0  # 오류 카운터 추가
            # 한글 거래소명 사용
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
            self.logger.info(f"{exchange_kr} 거래소 등록 완료")
    
    def register_callback(self, name: str, callback: Callable) -> None:
        """
        데이터 처리를 위한 콜백 함수 등록
        
        Args:
            name: 콜백 이름 (식별자)
            callback: 콜백 함수 (exchange_code, symbol, data, data_type을 인자로 받음)
        """
        self.callbacks[name] = callback
        self.logger.info(f"콜백 '{name}' 등록 완료")
    
    def unregister_callback(self, name: str) -> bool:
        """
        등록된 콜백 제거
        
        Args:
            name: 제거할 콜백 이름
            
        Returns:
            bool: 제거 성공 여부
        """
        if name in self.callbacks:
            del self.callbacks[name]
            self.logger.info(f"콜백 '{name}' 제거 완료")
            return True
        return False
    
    def log_raw_message(self, exchange_code: str, message: str) -> None:
        """
        원시 메시지 로깅
        
        Args:
            exchange_code: 거래소 코드
            message: 로깅할 원시 메시지
        """
        # 거래소가 등록되지 않은 경우 등록
        if exchange_code not in self.raw_loggers:
            self.register_exchange(exchange_code)
            
        try:
            # 메시지 통계 업데이트 - 로깅 활성화 여부와 관계없이 항상 수행
            self.message_counts[exchange_code] = self.message_counts.get(exchange_code, 0) + 1
            self.total_messages += 1
            
            # 로깅이 비활성화된 경우 로깅만 건너뜀
            if not ENABLE_RAW_MESSAGE_LOGGING:
                return
            
            # 로깅 수행
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            self.raw_loggers[exchange_code].debug(f"[{current_time}] {message}")
        except Exception as e:
            self.logger.error(f"원시 메시지 로깅 실패: {str(e)}")
            self.log_error(exchange_code)
    
    def log_orderbook_data(self, exchange_code: str, symbol: str, orderbook: Dict) -> None:
        """
        가공된 오더북 데이터 로깅
        
        Args:
            exchange_code: 거래소 코드
            symbol: 심볼 (예: "btc")
            orderbook: 오더북 데이터
        """
        if exchange_code not in self.raw_loggers:
            self.register_exchange(exchange_code)
            
        try:
            # 현재 시간 포맷팅
            current_time = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
            
            # 오더북 로깅이 활성화된 경우에만 로깅 수행
            if ENABLE_ORDERBOOK_LOGGING:
                # 상위 10개 호가만 로깅
                bids = orderbook.get("bids", [])[:10]
                asks = orderbook.get("asks", [])[:10]
                
                # 로그 데이터 구성
                log_data = {
                    "exchange": exchange_code,
                    "symbol": symbol,
                    "ts": orderbook.get("timestamp", int(time.time() * 1000)),
                    "seq": orderbook.get("sequence", 0),
                    "bids": bids,
                    "asks": asks
                }
                
                # raw_logger에 오더북 데이터 로깅
                self.raw_loggers[exchange_code].debug(
                    f"[{current_time}] 매수 ({len(bids)}) / 매도 ({len(asks)}) {json.dumps(log_data)}"
                )
            
            # Redis 발행이 활성화된 경우 Redis에 발행
            if ENABLE_REDIS_PUBLISHING and self.redis_client:
                try:
                    # Redis 채널 이름 생성 (명시적으로 심볼 포함하도록 수정)
                    channel = f"crosskimp:data:orderbook:{exchange_code}:{symbol}"
                    
                    # 디버깅을 위한 로깅 추가
                    if self.message_counts[exchange_code] % 10000 == 0:
                        self.logger.debug(f"오더북 발행 채널: {channel}, 심볼: {symbol}")
                    
                    # 메시지 직렬화
                    serialized_data = json.dumps(orderbook)
                    
                    # Redis에 발행 (PubSub) - 저장 없이 실시간 전송만
                    publish_result = self.redis_client.publish(channel, serialized_data)
                    
                    # 주기적 로깅 위치를 바꾸지 않기 위해 로그 레벨 제한
                    if self.message_counts[exchange_code] % 1000 == 0:
                        self.logger.debug(f"Redis 발행 결과: {publish_result} ({exchange_code}, {symbol})")
                except Exception as e:
                    self.logger.error(f"Redis 발행 중 오류: {str(e)}")
                    self.log_error(exchange_code)
            
            # 콜백 실행이 활성화된 경우에만 콜백 실행
            if ENABLE_CALLBACK_EXECUTION:
                # 콜백 실행 (향후 C++ 연동 등에 사용)
                for callback_name, callback_func in self.callbacks.items():
                    try:
                        callback_func(exchange_code, symbol, orderbook, "orderbook")
                    except Exception as e:
                        self.logger.error(f"콜백 '{callback_name}' 실행 중 오류: {str(e)}")
                        self.log_error(exchange_code)
            
            # 메시지 통계 업데이트
            self.orderbook_message_counts[exchange_code] = self.orderbook_message_counts.get(exchange_code, 0) + 1
        except Exception as e:
            self.logger.error(f"오더북 데이터 로깅 실패: {str(e)}")
            self.log_error(exchange_code)
    
    def log_error(self, exchange_code: str) -> None:
        """
        오류 카운트 증가
        
        Args:
            exchange_code: 거래소 코드
        """
        if exchange_code not in self.error_counts:
            self.error_counts[exchange_code] = 0
            
        self.error_counts[exchange_code] += 1
    
    def get_registered_exchanges(self) -> Set[str]:
        """
        등록된 거래소 목록 반환
        
        Returns:
            Set[str]: 등록된 거래소 코드 목록
        """
        return self.registered_exchanges
    
    def get_message_stats(self) -> Dict[str, Any]:
        """
        메시지 통계 반환 (기본 형식, 하위 호환성 유지)
        
        Returns:
            Dict[str, Any]: 메시지 통계 정보
        """
        elapsed = time.time() - self.start_time
        
        stats = {
            "total_messages": self.total_messages,
            "elapsed_seconds": elapsed,
            "messages_per_second": self.total_messages / elapsed if elapsed > 0 else 0,
            "exchanges": {}
        }
        
        for exchange in self.registered_exchanges:
            raw_count = self.message_counts.get(exchange, 0)
            ob_count = self.orderbook_message_counts.get(exchange, 0)
            
            stats["exchanges"][exchange] = {
                "raw_messages": raw_count,
                "orderbook_messages": ob_count,
                "messages_per_second": raw_count / elapsed if elapsed > 0 else 0
            }
        
        return stats
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        확장된 메시지 통계 반환 (구간별 속도 계산 추가)
        
        Returns:
            Dict[str, Any]: 확장된 메시지 통계 정보
        """
        elapsed = time.time() - self.start_time
        current_time = time.time()
        
        # 이전 수집 시간과의 간격 계산
        interval = current_time - self._prev_time
        
        stats = {
            "total_messages": self.total_messages,
            "elapsed_seconds": elapsed,
            "messages_per_second": self.total_messages / elapsed if elapsed > 0 else 0,
            "interval_seconds": interval,
            "exchanges": {}
        }
        
        for exchange in self.registered_exchanges:
            raw_count = self.message_counts.get(exchange, 0)
            ob_count = self.orderbook_message_counts.get(exchange, 0)
            
            # 구간별 속도 계산
            prev_count = self._prev_counts.get(exchange, 0)
            interval_count = raw_count - prev_count
            interval_rate = interval_count / interval if interval > 0 else 0
            
            stats["exchanges"][exchange] = {
                "raw_messages": raw_count,
                "orderbook_messages": ob_count,
                "total_rate": raw_count / elapsed if elapsed > 0 else 0,
                "interval_count": interval_count,
                "interval_rate": interval_rate,
                "errors": self.error_counts.get(exchange, 0)
            }
            
            # 현재 카운트를 이전 카운트로 저장
            self._prev_counts[exchange] = raw_count
        
        # 시간 업데이트
        self._prev_time = current_time
        
        return stats
    
    def print_stats(self) -> None:
        """메시지 통계 출력"""
        stats = self.get_statistics()
        
        self.logger.info(f"== 오더북 데이터 관리자 통계 ==")
        self.logger.info(f"총 메시지 수: {stats['total_messages']:,}개")
        self.logger.info(f"경과 시간: {stats['elapsed_seconds']:.2f}초")
        self.logger.info(f"초당 메시지: {stats['messages_per_second']:.2f}개/초")
        
        for exchange, ex_stats in stats["exchanges"].items():
            # 거래소 이름을 한글로 변환
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange, exchange)
            
            # 오류 정보 표시 추가
            error_text = f", 오류: {ex_stats['errors']}건" if ex_stats['errors'] > 0 else ""
            
            self.logger.info(f"{exchange_kr} 원시 메시지: {ex_stats['raw_messages']:,}개, "
                            f"오더북 메시지: {ex_stats['orderbook_messages']:,}개, "
                            f"초당 메시지: {ex_stats['interval_rate']:.2f}개/초{error_text}")
    
    async def _print_stats_periodically(self) -> None:
        """
        주기적으로 통계를 출력하는 비동기 메서드
        """
        self.stats_running = True
        self.logger.info(f"통계 출력 타이머 시작 (간격: {self.stats_interval}초)")
        
        try:
            while self.stats_running:
                # 통계 출력
                self.print_stats()
                
                # 설정된 간격만큼 대기
                await asyncio.sleep(self.stats_interval)
        except asyncio.CancelledError:
            self.logger.info("통계 출력 타이머 취소됨")
            self.stats_running = False
        except Exception as e:
            self.logger.error(f"통계 출력 중 오류 발생: {str(e)}")
            self.stats_running = False
    
    def start_stats_timer(self, interval: Optional[int] = None) -> None:
        """
        통계 출력 타이머 시작
        
        Args:
            interval: 통계 출력 간격 (초), None이면 기본값 사용
        """
        # 이미 실행 중이면 중지 후 재시작
        if self.stats_timer_task and not self.stats_timer_task.done():
            self.stop_stats_timer()
        
        # 간격 설정
        if interval is not None and interval > 0:
            self.stats_interval = interval
        
        # 새 이벤트 루프 가져오기 (없으면 생성)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 실행 중인 이벤트 루프가 없으면 새로 생성
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # 타이머 태스크 생성 및 시작
        self.stats_timer_task = asyncio.create_task(self._print_stats_periodically())
        self.logger.info(f"통계 출력 타이머 시작됨 (간격: {self.stats_interval}초)")
    
    def stop_stats_timer(self) -> None:
        """통계 출력 타이머 중지"""
        if self.stats_timer_task and not self.stats_timer_task.done():
            self.stats_running = False
            self.stats_timer_task.cancel()
            self.logger.info("통계 출력 타이머 중지됨")
    
    def prepare_for_cpp_export(self, orderbook: Dict) -> Dict:
        """
        C++ 내보내기용 데이터 준비
        
        Args:
            orderbook: 오더북 데이터
            
        Returns:
            Dict: C++ 내보내기용으로 변환된 데이터
        """
        # 향후 C++ 연동을 위한 데이터 변환 예시
        # 필요에 따라 확장 가능
        export_data = {
            "exchange": orderbook.get("exchange", ""),
            "symbol": orderbook.get("symbol", ""),
            "timestamp": orderbook.get("timestamp", 0),
            "sequence": orderbook.get("sequence", 0),
            "bids": orderbook.get("bids", []),  # 전체 매수 호가
            "asks": orderbook.get("asks", [])   # 전체 매도 호가
        }
        
        return export_data

    def process_orderbook_data(self, exchange_code: str, symbol: str, orderbook: Dict) -> None:
        """
        오더북 데이터 처리 및 로깅
        
        모든 거래소의 오더북 데이터를 여기서 중앙 처리합니다.
        각 거래소 핸들러는 이 메서드를 호출하여 데이터를 전달합니다.
        
        Args:
            exchange_code: 거래소 코드
            symbol: 심볼 (예: "btc")
            orderbook: 오더북 데이터
        """
        try:
            # 거래소가 등록되어 있지 않으면 등록
            if exchange_code not in self.raw_loggers:
                self.register_exchange(exchange_code)
                
            # 오더북 데이터 검증 (필요 시 추가 로직)
            # TODO: 여기에 공통 데이터 검증 로직 추가 가능
            
            # 로깅 처리
            self.log_orderbook_data(exchange_code, symbol, orderbook)
            
            # 추가 가공 또는 처리 (향후 확장)
            # TODO: 필요시 확장 (예: 실시간 분석, 통계 계산 등)
            
        except Exception as e:
            # 오류 처리
            exchange_kr = EXCHANGE_NAMES_KR.get(exchange_code, exchange_code)
            self.logger.error(f"{exchange_kr} {symbol} 오더북 데이터 처리 중 오류: {str(e)}")
            self.log_error(exchange_code)

# 싱글톤 인스턴스
_instance = None

def get_orderbook_data_manager() -> OrderbookDataManager:
    """
    오더북 데이터 관리자 싱글톤 인스턴스 반환
    
    Returns:
        OrderbookDataManager: 오더북 데이터 관리자 인스턴스
    """
    global _instance
    if _instance is None:
        _instance = OrderbookDataManager()
    return _instance
    
# C++ 인터페이스를 위한 예시 콜백 함수
def cpp_export_callback(exchange: str, symbol: str, data: Dict, data_type: str) -> None:
    """
    C++ 내보내기용 콜백 함수 예시
    
    Args:
        exchange: 거래소 코드
        symbol: 심볼
        data: 오더북 데이터
        data_type: 데이터 타입 ("orderbook", "trade" 등)
    """
    # 이 함수는 실제 C++ 바인딩이 구현될 때 사용될 예시입니다.
    # 향후 pybind11이나 다른 방식으로 C++로 데이터를 전달할 수 있습니다.
    if data_type == "orderbook":
        # 데이터 변환 및 C++ 함수 호출 예시
        # cpp_module.send_orderbook(exchange, symbol, json.dumps(data))
        pass

"""
향후 확장 가능한 아이디어:

1. 시간 동기화
   - 모든 거래소의 이벤트 타임스탬프를 통일된 시간 기준으로 조정
   - 시간차 보정 및 지연 측정

2. 데이터 지연 분석
   - 각 거래소별 데이터 수신 지연 분석 및 모니터링
   - 비정상적인 지연 감지 및 알림

3. 오더북 품질 지표
   - 오더북 데이터의 품질 지표 생성 (스프레드, 깊이, 유동성 등)
   - 품질 저하 감지 및 경고

4. 이상 탐지
   - 가격 급변, 유동성 급감 등 비정상 패턴 탐지
   - 잠재적 거래소 문제 사전 감지

5. 데이터 저장
   - 실시간 데이터를 지속 가능한 방식으로 저장
   - 분석용 데이터 다운샘플링 및 요약

6. API 제공
   - 내부 마이크로서비스를 위한 API 엔드포인트 제공
   - 실시간 및 히스토리 데이터 조회 기능

7. 펜딩 거래량 분석
   - 오더북 변화량 기반 거래량 예측
   - 유입/유출 패턴 분석

8. 멀티 프로세스 아키텍처
   - 수집과 처리를 별도 프로세스로 분리
   - IPC를 통한 효율적 데이터 전송

9. C++ 엔진 통합
   - C++ 기반 고성능 처리 엔진과 통합
   - 제로카피 또는 공유 메모리 기반 데이터 전송
""" 