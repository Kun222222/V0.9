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
from typing import Dict, List, Any, Optional, Callable, Set

from crosskimp.common.logger.logger import get_unified_logger, create_raw_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR
from crosskimp.common.config.app_config import get_config

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
        
        # 설정 로드
        self.config = get_config()
        
        # 오더북 설정 - 중앙에서 관리할 뎁스 값 (설정 파일에서 로드)
        self.orderbook_output_depth = self.config.get('trading.settings.orderbook_output_depth', 15)  # 출력 및 분석용 오더북 깊이
        self.orderbook_internal_depth = self.config.get('trading.settings.orderbook_internal_depth', 50)  # 내부 저장용 오더북 깊이
        
        # 이전 통계 값 저장용 변수 추가
        self._prev_counts = {}
        self._prev_time = time.time()
        
        # 오류 카운팅 추가
        self.error_counts = {}
        
        self.logger.info(f"오더북 데이터 관리자 초기화 (출력 깊이: {self.orderbook_output_depth}, 내부 깊이: {self.orderbook_internal_depth})")
    
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
        if exchange_code not in self.raw_loggers:
            self.register_exchange(exchange_code)
            
        try:
            # current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            # self.raw_loggers[exchange_code].debug(f"[{current_time}] {message}")
            
            # 메시지 통계 업데이트
            self.message_counts[exchange_code] = self.message_counts.get(exchange_code, 0) + 1
            self.total_messages += 1
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
            
            # 중앙 설정된 뎁스 값으로 호가 제한
            bids = orderbook.get("bids", [])[:self.orderbook_output_depth]
            asks = orderbook.get("asks", [])[:self.orderbook_output_depth]
            
            # 로그 데이터 구성
            log_data = {
                "exch": exchange_code,
                "sym": symbol,
                "ts": orderbook.get("timestamp", int(time.time() * 1000)),
                "seq": orderbook.get("sequence", 0),
                "bids": bids,
                "asks": asks
            }
            
            # raw_logger에 오더북 데이터 로깅
            self.raw_loggers[exchange_code].debug(
                f"매수 {len(bids)} / 매도 {len(asks)} {json.dumps(log_data)}"
            )
            
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
            
            self.logger.info(f"{exchange_kr} RAW: {ex_stats['raw_messages']:,}개, "
                            f"OB: {ex_stats['orderbook_messages']:,}개, "
                            f"M/S: {ex_stats['interval_rate']:.2f}개/초{error_text}")
    
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
    
    def get_orderbook_output_depth(self) -> int:
        """
        설정된 출력용 오더북 뎁스 값 반환
        
        Returns:
            int: 출력용 오더북 뎁스 값
        """
        return self.orderbook_output_depth
        
    def get_orderbook_internal_depth(self) -> int:
        """
        설정된 내부 저장용 오더북 뎁스 값 반환
        
        Returns:
            int: 내부 저장용 오더북 뎁스 값
        """
        return self.orderbook_internal_depth
        
    def set_orderbook_depths(self, output_depth: int = None, internal_depth: int = None) -> None:
        """
        오더북 뎁스 값 설정
        
        Args:
            output_depth: 설정할 출력용 오더북 뎁스
            internal_depth: 설정할 내부 저장용 오더북 뎁스
        """
        if output_depth is not None and output_depth > 0:
            self.orderbook_output_depth = output_depth
            self.logger.info(f"출력용 오더북 뎁스 설정: {output_depth}")
        
        if internal_depth is not None and internal_depth > 0:
            self.orderbook_internal_depth = internal_depth
            self.logger.info(f"내부 저장용 오더북 뎁스 설정: {internal_depth}")
            
    # 기존 메서드는 호환성을 위해 유지
    def get_orderbook_depth(self) -> int:
        """
        설정된 오더북 뎁스 값 반환 (호환성 유지용)
        
        Returns:
            int: 출력용 오더북 뎁스 값
        """
        return self.orderbook_output_depth
        
    def set_orderbook_depth(self, depth: int) -> None:
        """
        오더북 뎁스 값 설정 (호환성 유지용)
        
        Args:
            depth: 설정할 오더북 뎁스
        """
        self.set_orderbook_depths(output_depth=depth)

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