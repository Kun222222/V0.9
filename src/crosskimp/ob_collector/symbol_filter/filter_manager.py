# file: core/aggregator.py

"""
심볼 집계 모듈

이 모듈은 여러 거래소에서 유효한 심볼 목록을 가져와 필터링하는 기능을 제공합니다.
"""

import asyncio
from datetime import datetime
import aiohttp
from typing import Dict, List, Set, Optional, Tuple, Any

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import Exchange, EXCHANGE_NAMES_KR, SystemComponent
from crosskimp.common.config.app_config import get_config

# 거래소 API 모듈 가져오기
from crosskimp.ob_collector.symbol_filter.exchanges.upbit_api import fetch_upbit_filtered_symbols
from crosskimp.ob_collector.symbol_filter.exchanges.bithumb_api import fetch_bithumb_filtered_symbols
from crosskimp.ob_collector.symbol_filter.exchanges.binance_api import fetch_binance_filtered_symbols
from crosskimp.ob_collector.symbol_filter.exchanges.bybit_api import fetch_bybit_filtered_symbols

# 로거 인스턴스 가져오기
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

# 설정 객체 초기화 (한 번만 호출)
config = get_config()
logger.debug(f"설정 객체 초기화 완료: config_dir={config.config_dir}")

# ============================
# 유틸리티 함수
# ============================
def get_current_time_str() -> str:
    """현재 시간 문자열 반환 (파일명용)"""
    return datetime.now().strftime("%y%m%d_%H%M%S")

def get_value_from_settings(path: str, default=None):
    """설정에서 값 가져오기"""
    try:
        logger.debug(f"설정 값 조회 시도: {path}, 기본값: {default}")
        
        if not hasattr(config, 'exchange_settings') or not config.exchange_settings:
            logger.error(f"거래소 설정이 로드되지 않았습니다. exchange_settings 객체: {config.exchange_settings}")
            return default
            
        if path.startswith("trading.") and 'trading' in config.exchange_settings:
            result = config.get_exchange(path, default)
            logger.debug(f"거래소 설정 반환: {path} = {result}")
            return result
        else:
            result = config.get_value_from_settings(path, default)
            logger.debug(f"시스템 설정 반환: {path} = {result}")
            return result
        
    except Exception as e:
        logger.error(f"설정 값 조회 실패: {path}, 예외: {str(e)}")
        return default

def get_symbol_filters():
    """심볼 필터 설정 가져오기"""
    try:
        return config.get_symbol_filters()
    except Exception as e:
        logger.error(f"심볼 필터 설정 획득 중 오류: {str(e)}")
        return {"excluded": [], "included": [], "excluded_patterns": []}

# ============================
# 거래소 API 호출 함수
# ============================
async def fetch_exchange_symbols(exchange: str, min_volume: float = None) -> Dict:
    """
    지정된 거래소의 심볼 데이터 조회
    
    Args:
        exchange: 거래소 코드
        min_volume: 최소 거래량
    
    Returns:
        Dict: 필터링된 심볼 데이터
    """
    try:
        exchg_name = EXCHANGE_NAMES_KR.get(exchange, exchange)
        
        if exchange == Exchange.UPBIT.value:
            result = await fetch_upbit_filtered_symbols(min_volume)
            return {
                "symbols": result.get("filtered_symbols", []),
                "prices": result.get("prices", {}),
                "exchange": exchange
            }
        elif exchange == Exchange.BITHUMB.value:
            result = await fetch_bithumb_filtered_symbols(min_volume)
            return {
                "symbols": result.get("filtered_symbols", []),
                "prices": {},
                "exchange": exchange
            }
        elif exchange in [Exchange.BINANCE_SPOT.value, Exchange.BINANCE_FUTURE.value]:
            result = await fetch_binance_filtered_symbols(min_volume)
            return {
                "symbols": result.get("filtered_symbols", []),
                "prices": {},
                "exchange": exchange
            }
        elif exchange in [Exchange.BYBIT_SPOT.value, Exchange.BYBIT_FUTURE.value]:
            result = await fetch_bybit_filtered_symbols(min_volume)
            return {
                "symbols": result.get("filtered_symbols", []),
                "prices": {},
                "exchange": exchange
            }
        else:
            logger.error(f"지원되지 않는 거래소: {exchange}")
            return {"symbols": [], "prices": {}, "exchange": exchange}
            
    except Exception as e:
        logger.error(f"{exchg_name} 심볼 조회에 실패했습니다: {str(e)}", exc_info=True)
        return {"symbols": [], "prices": {}, "exchange": exchange}

# ============================
# 심볼 페어링 함수
# ============================
async def get_paired_symbols(
    upbit_symbols: List[str],
    bithumb_symbols: List[str],
    binance_symbols: List[str],
    bybit_symbols: List[str]
) -> Dict[str, Set[str]]:
    """거래소 간 공통 심볼 페어링"""
    try:
        logger.info("거래소 간 동시 상장 분석을 시작합니다.")
        
        # 1. 국내 거래소 ↔ 해외 거래소 페어링
        pair_bithumb_binance = set(bithumb_symbols) & set(binance_symbols)
        pair_bithumb_bybit = set(bithumb_symbols) & set(bybit_symbols)
        pair_upbit_binance = set(upbit_symbols) & set(binance_symbols)
        pair_upbit_bybit = set(upbit_symbols) & set(bybit_symbols)
        
        # 2. 결과 로깅
        logger.info(f"=== 거래소 간 동시 상장 현황 ===")
        logger.info(f"{EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]}↔{EXCHANGE_NAMES_KR[Exchange.BINANCE_SPOT.value]}: {len(pair_bithumb_binance)}개 → {sorted(list(pair_bithumb_binance))}")
        logger.info(f"{EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]}↔{EXCHANGE_NAMES_KR[Exchange.BYBIT_SPOT.value]}: {len(pair_bithumb_bybit)}개 → {sorted(list(pair_bithumb_bybit))}")
        logger.info(f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]}↔{EXCHANGE_NAMES_KR[Exchange.BINANCE_SPOT.value]}: {len(pair_upbit_binance)}개 → {sorted(list(pair_upbit_binance))}")
        logger.info(f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]}↔{EXCHANGE_NAMES_KR[Exchange.BYBIT_SPOT.value]}: {len(pair_upbit_bybit)}개 → {sorted(list(pair_upbit_bybit))}")
        
        # 3. 최종 심볼 세트 구성
        final_symbols = {
            Exchange.BITHUMB.value: pair_bithumb_binance | pair_bithumb_bybit,
            Exchange.UPBIT.value: pair_upbit_binance | pair_upbit_bybit,
            Exchange.BINANCE_SPOT.value: pair_bithumb_binance | pair_upbit_binance,
            Exchange.BYBIT_SPOT.value: pair_bithumb_bybit | pair_upbit_bybit,
            Exchange.BINANCE_FUTURE.value: pair_bithumb_binance | pair_upbit_binance,
            Exchange.BYBIT_FUTURE.value: pair_bithumb_bybit | pair_upbit_bybit
        }
        
        # USDT 추가 (국내 거래소)
        for exchange in [Exchange.UPBIT.value, Exchange.BITHUMB.value]:
            final_symbols[exchange] = set(list(final_symbols[exchange]) + ["USDT"])
        
        return final_symbols

    except Exception as e:
        logger.error(f"심볼 페어링에 실패했습니다: {e}")
        return {}

# ============================
# Aggregator 클래스
# ============================
class Aggregator:
    """거래소 심볼 필터링 및 관리 클래스"""

    def __init__(self, settings: Dict):
        """초기화"""
        self.settings = settings
        self.exchanges = [
            Exchange.BINANCE_FUTURE.value,
            Exchange.BYBIT_FUTURE.value,
            Exchange.BINANCE_SPOT.value,
            Exchange.BYBIT_SPOT.value,
            Exchange.UPBIT.value,
            Exchange.BITHUMB.value
        ]
        
        # 최소 거래량 설정 가져오기
        min_volume = get_value_from_settings("trading.settings.min_daily_volume_krw")
        if min_volume is None:
            logger.error("최소 거래량 설정(trading.settings.min_daily_volume_krw)을 찾을 수 없습니다.")
            logger.error("exchange_settings.cfg 파일에 trading.settings.min_daily_volume_krw 설정이 필요합니다.")
            import sys
            sys.exit(1)
        
        self.min_volume = min_volume
        logger.info(f"최소 거래량 설정: {self.min_volume:,} KRW")

    async def run_filtering(self) -> Dict[str, Any]:
        """
        거래소별 심볼 필터링 실행
        
        Returns:
            Dict[str, Any]: {
                "symbols": Dict[str, List[str]] - 거래소별 필터링된 심볼 목록,
                "prices": Dict[str, Dict[str, float]] - 거래소별 심볼 가격 정보
            }
        """
        try:
            logger.info(f"=== 심볼 필터링을 시작합니다 ===")
            logger.info(f"최소 거래량: {self.min_volume:,} KRW")
            
            # 1. 거래소별 필터링
            filtered_data: Dict[str, List[str]] = {}
            # 가격 정보 저장 객체 추가
            price_data: Dict[str, Dict[str, float]] = {}
            
            # 거래소별 API 호출
            exchanges_to_call = [
                Exchange.UPBIT.value,
                Exchange.BITHUMB.value,
                Exchange.BINANCE_SPOT.value,
                Exchange.BYBIT_SPOT.value
            ]
            
            for exchange in exchanges_to_call:
                result = await fetch_exchange_symbols(exchange, self.min_volume)
                symbols = result.get("symbols", [])
                prices = result.get("prices", {})
                
                # 가격 정보 저장
                if prices:
                    price_data[exchange] = prices
                
                if symbols:
                    filtered_data[exchange] = symbols
                    if exchange == Exchange.BINANCE_SPOT.value:
                        filtered_data[Exchange.BINANCE_FUTURE.value] = symbols.copy()
                    elif exchange == Exchange.BYBIT_SPOT.value:
                        filtered_data[Exchange.BYBIT_FUTURE.value] = symbols.copy()
                    
                    # 심볼 샘플 출력 (최대 20개)
                    exchg_name = EXCHANGE_NAMES_KR.get(exchange, exchange)
                    sample = sorted(symbols)[:min(20, len(symbols))]
                    logger.info(f"{exchg_name} 필터링된 심볼: {len(symbols)}개")
                    logger.info(f"{exchg_name} 심볼 샘플: {sample}...")
            
            # 2. 거래소 간 페어링
            if len(filtered_data) >= 4:
                paired_data = await get_paired_symbols(
                    filtered_data.get(Exchange.UPBIT.value, []),
                    filtered_data.get(Exchange.BITHUMB.value, []),
                    filtered_data.get(Exchange.BINANCE_SPOT.value, []),
                    filtered_data.get(Exchange.BYBIT_SPOT.value, [])
                )
                filtered_data = {k: list(v) for k, v in paired_data.items()}
                
                # 가격 정보도 필터링된 심볼에 맞게 조정
                if Exchange.UPBIT.value in price_data and Exchange.UPBIT.value in filtered_data:
                    upbit_symbols = filtered_data[Exchange.UPBIT.value]
                    upbit_prices = price_data[Exchange.UPBIT.value]
                    price_data[Exchange.UPBIT.value] = {sym: upbit_prices[sym] for sym in upbit_symbols if sym in upbit_prices}
            
            # 3. 제외 심볼 필터링
            symbol_filters = get_symbol_filters()
            excluded_symbols = set(symbol_filters.get('excluded', []))
            
            logger.info(f"=== 제외할 심볼 목록 === {sorted(list(excluded_symbols))}")
            
            for exchange in filtered_data:
                before_symbols = filtered_data[exchange]
                filtered_data[exchange] = [
                    s for s in filtered_data[exchange]
                    if s not in excluded_symbols
                ]
                
                # 가격 정보도 함께 필터링
                if exchange in price_data:
                    price_data[exchange] = {sym: price for sym, price in price_data[exchange].items() 
                                          if sym in filtered_data[exchange]}
                
                removed = set(before_symbols) - set(filtered_data[exchange])
                if removed:
                    logger.info(
                        f"{EXCHANGE_NAMES_KR[exchange]} 제외된 심볼 목록: {sorted(list(removed))}"
                    )
            
            logger.info(f"=== 거래소별 최종 구독 심볼 ===")
            for exchange, symbols in filtered_data.items():
                name = EXCHANGE_NAMES_KR.get(exchange, exchange)
                logger.info(f"{name} 구독 심볼: {len(symbols)}개 → {sorted(symbols)}")
                
                # 가격 정보 로깅 (업비트의 경우)
                if exchange == Exchange.UPBIT.value and exchange in price_data:
                    logger.info(f"{name} 가격 정보: {len(price_data[exchange])}개 심볼에 대한 가격 수집됨")
            
            # 심볼 목록과 가격 정보를 포함하여 반환
            return {
                "symbols": filtered_data,
                "prices": price_data
            }

        except Exception as e:
            logger.error(f"심볼 필터링에 실패했습니다: {e}")
            return {"symbols": {}, "prices": {}}

