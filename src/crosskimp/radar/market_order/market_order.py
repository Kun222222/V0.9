"""
시장가 주문 시뮬레이션 모듈

오더북 데이터를 기반으로 시장가 주문 시뮬레이션을 수행하고 결과를 로깅합니다.
설정된 금액으로 매수/매도 평균 가격과 수량을 계산합니다.
"""

import os
import json
import time
import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging
from pathlib import Path

from crosskimp.common.logger.logger import get_unified_logger, create_raw_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.config.app_config import get_config

class MarketOrderSimulator:
    """
    시장가 주문 시뮬레이션 클래스
    
    오더북 데이터를 기반으로 시장가 주문 시뮬레이션을 수행하고
    평균 체결 가격과 수량을 계산합니다.
    """
    
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        
        # 설정 로드
        self.config = get_config()
        
        # 기본 주문 금액 설정 (KRW)
        self.base_order_amount_krw = self.config.get('trading.settings.base_order_amount_krw', 10000000)
        
        # 로그 디렉토리 설정
        log_dir = "src/logs/market_order_data"
        os.makedirs(log_dir, exist_ok=True)
        
        # 시장가 주문 시뮬레이션 로거 설정
        self.market_order_logger = self._setup_market_order_logger(log_dir)
        
        # 미소화 주문 로거 설정 (새로 추가)
        self.incomplete_order_logger = self._setup_incomplete_order_logger(log_dir)
        
        self.logger.info(f"시장가 주문 시뮬레이션 초기화 완료 (기본 주문 금액: {self.base_order_amount_krw:,} KRW)")
        
    def _setup_market_order_logger(self, log_dir: str) -> logging.Logger:
        """
        시장가 주문 시뮬레이션 로거 설정
        
        Args:
            log_dir: 로그 디렉토리
            
        Returns:
            logging.Logger: 설정된 로거
        """
        logger = logging.getLogger("market_order_simulator")
        logger.setLevel(logging.INFO)
        
        # 기존 핸들러 제거
        if logger.handlers:
            for handler in logger.handlers:
                logger.removeHandler(handler)
        
        # 현재 날짜와 시간으로 파일명 생성
        current_datetime = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"{current_datetime}_market_orderbook.log")
        
        # 파일 핸들러 추가
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # 매 로그 메시지마다 즉시 파일에 쓰도록 설정
        file_handler.setLevel(logging.INFO)
        
        return logger
    
    def _setup_incomplete_order_logger(self, log_dir: str) -> logging.Logger:
        """
        미소화 주문 로거 설정
        
        Args:
            log_dir: 로그 디렉토리
            
        Returns:
            logging.Logger: 설정된 로거
        """
        logger = logging.getLogger("incomplete_order_logger")
        logger.setLevel(logging.INFO)
        
        # 기존 핸들러 제거
        if logger.handlers:
            for handler in logger.handlers:
                logger.removeHandler(handler)
        
        # 현재 날짜와 시간으로 파일명 생성
        current_datetime = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"{current_datetime}_incomplete_orderbook.log")
        
        # 파일 핸들러 추가
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # 매 로그 메시지마다 즉시 파일에 쓰도록 설정
        file_handler.setLevel(logging.INFO)
        
        return logger
    
    def simulate_market_order(self, orderbook_data: Dict[str, Any]) -> None:
        """
        시장가 주문 시뮬레이션 수행
        
        Args:
            orderbook_data: 오더북 데이터
        """
        try:
            # 필요한 데이터 추출
            exchange = orderbook_data.get("exch", "")
            symbol = orderbook_data.get("sym", "")
            timestamp = orderbook_data.get("ts", 0)
            bids = orderbook_data.get("bids", [])
            asks = orderbook_data.get("asks", [])
            usdt_krw = orderbook_data.get("usdt_krw", {})
            
            # 필수 데이터 확인
            if not all([exchange, symbol, bids, asks]):
                self.logger.warning(f"필수 오더북 데이터 누락: exchange={exchange}, symbol={symbol}")
                return
            
            # 국내/해외 거래소 구분
            is_korean_exchange = exchange in ["upbit", "bithumb"]
            
            # USDT/KRW 환율 가져오기 (해외 거래소용)
            upbit_usdt_rate = usdt_krw.get("upbit", 0)
            if not is_korean_exchange and upbit_usdt_rate <= 0:
                self.logger.warning(f"유효한 USDT/KRW 환율이 없습니다: {upbit_usdt_rate}")
                return
            
            # 매수 시뮬레이션 (오더북의 매도 호가 사용)
            buy_avg_price, buy_amount, buy_consumed, is_buy_complete = self._simulate_buy_market_order(
                asks, 
                self.base_order_amount_krw, 
                is_korean_exchange,
                upbit_usdt_rate
            )
            
            # 매도 시뮬레이션 (오더북의 매수 호가 사용)
            sell_avg_price, sell_amount, sell_consumed, is_sell_complete = self._simulate_sell_market_order(
                bids, 
                self.base_order_amount_krw, 
                is_korean_exchange,
                upbit_usdt_rate
            )
            
            # 불완전 주문 감지 및 별도 로깅
            is_incomplete = not is_buy_complete or not is_sell_complete
            
            # 소비율 계산
            buy_consumption_rate = buy_consumed / self.base_order_amount_krw * 100
            sell_consumption_rate = sell_consumed / self.base_order_amount_krw * 100
            
            # 결과 로깅
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            log_data = {
                "timestamp": current_time,
                "exchange": exchange,
                "symbol": symbol,
                "base_amount_krw": self.base_order_amount_krw,
                "buy": {
                    "avg_price": buy_avg_price,
                    "amount": buy_amount,
                    "total_krw": self.base_order_amount_krw,  # 항상 기준 금액으로 설정
                    "consumed_krw": buy_consumed,
                    "consumption_rate": buy_consumption_rate
                },
                "sell": {
                    "avg_price": sell_avg_price,
                    "amount": sell_amount,
                    "total_krw": self.base_order_amount_krw,  # 항상 기준 금액으로 설정
                    "consumed_krw": sell_consumed,
                    "consumption_rate": sell_consumption_rate
                },
                "usdt_krw": upbit_usdt_rate if not is_korean_exchange else 0
            }
            
            # 모든 거래는 항상 시뮬레이션 로그에 기록
            self.market_order_logger.info(json.dumps(log_data))
            # 즉시 파일에 기록하도록 flush
            for handler in self.market_order_logger.handlers:
                handler.flush()
            
            # 불완전 주문만 별도 로깅
            if is_incomplete:
                incomplete_log_data = {
                    "timestamp": current_time,
                    "exchange": exchange,
                    "symbol": symbol,
                    "reason": "orderbook_depth_insufficient",
                    "buy_consumption_rate": buy_consumption_rate,
                    "sell_consumption_rate": sell_consumption_rate,
                    "ask_depth": len(asks),
                    "bid_depth": len(bids),
                    "base_amount_krw": self.base_order_amount_krw
                }
                self.incomplete_order_logger.info(json.dumps(incomplete_log_data))
                # 즉시 파일에 기록하도록 flush
                for handler in self.incomplete_order_logger.handlers:
                    handler.flush()
                
                # warning에서 debug로 로그 레벨 변경
                # self.logger.debug(f"불완전 주문 감지: {exchange} {symbol} - 매수 소비율: {buy_consumption_rate:.2f}%, 매도 소비율: {sell_consumption_rate:.2f}%")
            
        except Exception as e:
            self.logger.error(f"시장가 주문 시뮬레이션 중 오류 발생: {str(e)}")
    
    def _simulate_buy_market_order(
        self, 
        asks: List[List[float]], 
        order_amount_krw: float,
        is_korean_exchange: bool,
        usdt_krw_rate: float
    ) -> Tuple[float, float, float, bool]:
        """
        매수 시장가 주문 시뮬레이션
        
        Args:
            asks: 매도 호가 목록 [[가격, 수량], ...]
            order_amount_krw: 주문 금액 (KRW)
            is_korean_exchange: 국내 거래소 여부
            usdt_krw_rate: USDT/KRW 환율
            
        Returns:
            Tuple[float, float, float, bool]: (평균 가격, 총 수량, 소비된 금액, 완전 소진 여부)
        """
        remaining_amount_krw = order_amount_krw
        total_cost = 0.0
        total_quantity = 0.0
        
        # 호가별 처리
        for price, quantity in asks:
            # 남은 주문 금액이 없으면 종료
            if remaining_amount_krw <= 0:
                break
            
            # 해외 거래소는 KRW로 환산
            order_price = price
            if not is_korean_exchange:
                order_price = price * usdt_krw_rate
            
            # 현재 호가에서 구매 가능한 최대 수량 계산
            max_quantity_at_price = remaining_amount_krw / order_price
            
            # 실제 구매할 수량 (호가의 수량과 최대 구매 가능 수량 중 작은 값)
            buy_quantity = min(quantity, max_quantity_at_price)
            
            # 비용 및 수량 업데이트
            cost_at_price = buy_quantity * order_price
            total_cost += cost_at_price
            total_quantity += buy_quantity
            
            # 남은 주문 금액 업데이트
            remaining_amount_krw -= cost_at_price
        
        # 소비된 금액 계산
        consumed_amount = order_amount_krw - remaining_amount_krw
        
        # 완전 소진 여부 확인 (99.9% 이상 소비된 경우도 완전 소진으로 간주)
        is_complete = consumed_amount >= order_amount_krw * 0.999
        
        # 소비율 100%로 맞추기 위한 조정
        avg_price = 0.0
        if total_quantity > 0:
            # 국내 거래소는 KRW 기준, 해외 거래소는 USDT 기준
            if is_korean_exchange:
                avg_price = order_amount_krw / total_quantity  # 기준 금액으로 나누어 평균가 계산
            else:
                avg_price = (order_amount_krw / usdt_krw_rate) / total_quantity
        
        return avg_price, total_quantity, consumed_amount, is_complete
    
    def _simulate_sell_market_order(
        self, 
        bids: List[List[float]], 
        order_amount_krw: float,
        is_korean_exchange: bool,
        usdt_krw_rate: float
    ) -> Tuple[float, float, float, bool]:
        """
        매도 시장가 주문 시뮬레이션
        
        Args:
            bids: 매수 호가 목록 [[가격, 수량], ...]
            order_amount_krw: 주문 금액 (KRW)
            is_korean_exchange: 국내 거래소 여부
            usdt_krw_rate: USDT/KRW 환율
            
        Returns:
            Tuple[float, float, float, bool]: (평균 가격, 총 수량, 소비된 금액, 완전 소진 여부)
        """
        remaining_amount_krw = order_amount_krw
        total_revenue = 0.0
        total_quantity = 0.0
        
        # 호가별 처리
        for price, quantity in bids:
            # 남은 주문 금액이 없으면 종료
            if remaining_amount_krw <= 0:
                break
            
            # 해외 거래소는 KRW로 환산
            order_price = price
            if not is_korean_exchange:
                order_price = price * usdt_krw_rate
            
            # 현재 호가에서 판매 가능한 최대 수량 계산
            max_quantity_at_price = remaining_amount_krw / order_price
            
            # 실제 판매할 수량 (호가의 수량과 최대 판매 가능 수량 중 작은 값)
            sell_quantity = min(quantity, max_quantity_at_price)
            
            # 비용 및 수량 업데이트
            revenue_at_price = sell_quantity * order_price
            total_revenue += revenue_at_price
            total_quantity += sell_quantity
            
            # 남은 주문 금액 업데이트
            remaining_amount_krw -= revenue_at_price
        
        # 소비된 금액 계산
        consumed_amount = order_amount_krw - remaining_amount_krw
        
        # 완전 소진 여부 확인 (99.9% 이상 소비된 경우도 완전 소진으로 간주)
        is_complete = consumed_amount >= order_amount_krw * 0.999
        
        # 소비율 100%로 맞추기 위한 조정
        avg_price = 0.0
        if total_quantity > 0:
            # 국내 거래소는 KRW 기준, 해외 거래소는 USDT 기준
            if is_korean_exchange:
                avg_price = order_amount_krw / total_quantity  # 기준 금액으로 나누어 평균가 계산
            else:
                avg_price = (order_amount_krw / usdt_krw_rate) / total_quantity
        
        return avg_price, total_quantity, consumed_amount, is_complete

# 단일 인스턴스 제공 함수
_market_order_simulator_instance = None

def get_market_order_simulator() -> MarketOrderSimulator:
    """
    시장가 주문 시뮬레이터 인스턴스 반환
    
    Returns:
        MarketOrderSimulator: 시장가 주문 시뮬레이터 인스턴스
    """
    global _market_order_simulator_instance
    
    if _market_order_simulator_instance is None:
        _market_order_simulator_instance = MarketOrderSimulator()
        
    return _market_order_simulator_instance

# JSON 문자열에서 오더북 데이터 파싱 함수
def parse_orderbook_data(raw_data: str) -> Optional[Dict[str, Any]]:
    """
    로그 문자열에서 오더북 데이터 파싱
    
    Args:
        raw_data: 로그 문자열 (예: "매수 15 / 매도 15 {"exch": "bithumb", ...}")
        
    Returns:
        Optional[Dict[str, Any]]: 파싱된 오더북 데이터 또는 None
    """
    try:
        # 오더북 데이터 부분 추출 (JSON 부분)
        json_start = raw_data.find('{')
        if json_start == -1:
            return None
            
        json_data = raw_data[json_start:]
        
        # JSON 파싱
        return json.loads(json_data)
    except Exception as e:
        # 로거를 사용하여 오류 출력
        from crosskimp.common.logger.logger import get_unified_logger
        from crosskimp.common.config.common_constants import SystemComponent
        logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        logger.error(f"오더북 데이터 파싱 중 오류: {str(e)}")
        return None

# 메인 핸들러 함수
def handle_orderbook_data(raw_data: str) -> None:
    """
    오더북 데이터 처리 및 시장가 주문 시뮬레이션 수행
    
    Args:
        raw_data: 오더북 데이터 로그 문자열
    """
    try:
        # 오더북 데이터 파싱
        orderbook_data = parse_orderbook_data(raw_data)
        if not orderbook_data:
            return
        
        # 시장가 주문 시뮬레이션 수행
        simulator = get_market_order_simulator()
        simulator.simulate_market_order(orderbook_data)
    except Exception as e:
        # 로거가 없으면 표준 출력으로 오류 출력
        # 동일한 로거 사용
        from crosskimp.common.logger.logger import get_unified_logger
        from crosskimp.common.config.common_constants import SystemComponent
        logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)
        logger.error(f"시장가 주문 시뮬레이션 중 오류 발생: {str(e)}")
        # 디버그용 추가 정보
        import traceback
        logger.debug(f"상세 오류: {traceback.format_exc()}") 