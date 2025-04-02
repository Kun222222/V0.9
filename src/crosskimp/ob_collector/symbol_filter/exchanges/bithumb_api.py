"""
빗썸 API 모듈

빗썸 거래소의 API를 호출하는 함수들을 제공합니다.
"""

import os
import aiohttp
from typing import Dict, Optional, Any

# 프로젝트 공통 로거 사용
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR
from crosskimp.common.config.env_loader import get_env_loader

from crosskimp.ob_collector.symbol_filter.exchanges.api_common import (
    get_cached_data,
    cache_data,
    make_async_request,
    make_sync_request
)

# 환경변수 로더 가져오기
env_loader = get_env_loader()

# API 키 정보 (환경변수 로더에서 가져오기)
BITHUMB_ACCESS_KEY = env_loader.get("bithumb.api_key", "")
BITHUMB_SECRET_KEY = env_loader.get("bithumb.api_secret", "")

logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

# 로그용 거래소 이름
EXCHANGE_NAME = EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]

# ============================
# 빗썸 API 함수
# ============================
async def fetch_tickers(session: Optional[aiohttp.ClientSession] = None, use_cache: bool = True) -> Dict:
    """
    빗썸 거래량 및 가격 정보 조회
    
    Args:
        session: aiohttp 세션 (없으면 생성)
        use_cache: 캐시 사용 여부
    
    Returns:
        Dict: 거래량 및 가격 정보
    """
    cache_key = "bithumb_tickers"
    
    # 캐시 확인
    if use_cache:
        cached_data = get_cached_data(cache_key)
        if cached_data:
            return cached_data
    
    result = {"symbols": [], "volumes": {}, "prices": {}, "raw_data": None}
    
    try:
        should_close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            should_close_session = True
        
        data = await make_async_request(session, "https://api.bithumb.com/public/ticker/ALL_KRW")
        
        if should_close_session:
            await session.close()
            
        if not data or data.get("status") != "0000":
            logger.error(f"{EXCHANGE_NAME} 티커 정보 조회 실패")
            return result
        
        market_data = data["data"]
        symbols = [sym for sym in market_data.keys() if sym != "date"]
        
        # 거래량 및 가격 정보 추출
        volumes = {}
        prices = {}
        
        for symbol in symbols:
            try:
                volume = float(market_data[symbol].get("acc_trade_value_24H", 0))
                price = float(market_data[symbol].get("closing_price", 0))
                volumes[symbol] = volume
                prices[symbol] = price
            except (KeyError, ValueError) as e:
                logger.warning(f"{EXCHANGE_NAME} {symbol} 데이터 파싱 오류: {str(e)}")
        
        result = {
            "symbols": symbols,
            "volumes": volumes,
            "prices": prices,
            "raw_data": market_data
        }
        
        # 캐시 저장
        if use_cache:
            cache_data(cache_key, result)
        
        return result
    
    except Exception as e:
        logger.error(f"{EXCHANGE_NAME} 티커 조회 중 오류: {str(e)}")
        if 'session' in locals() and should_close_session and session:
            try:
                await session.close()
            except:
                pass
        return result

async def fetch_deposit_withdraw_status(use_cache: bool = True) -> Dict:
    """
    빗썸 출금/입금 상태 조회
    
    Args:
        use_cache: 캐시 사용 여부
    
    Returns:
        Dict: 출금/입금 상태 정보
    """
    cache_key = "bithumb_deposit_withdraw"
    
    # 캐시 확인
    if use_cache:
        cached_data = get_cached_data(cache_key)
        if cached_data:
            return cached_data
    
    result = {
        "deposit_disabled": [],
        "withdraw_disabled": [],
        "raw_data": None
    }
    
    try:
        # API 호출 (동기식)
        response = make_sync_request("https://api.bithumb.com/public/assetsstatus/ALL")
        
        if not response or response.get("status") != "0000":
            logger.error(f"{EXCHANGE_NAME} 출금/입금 상태 조회 실패")
            return result
        
        # 결과 처리
        coin_status = response.get("data", {})
        deposit_disabled = []
        withdraw_disabled = []
        
        for currency, status in coin_status.items():
            # 입금 불가 확인
            if status.get("deposit_status") == 0:
                deposit_disabled.append({
                    "currency": currency,
                    "status_text": "입금 불가"
                })
            
            # 출금 불가 확인
            if status.get("withdrawal_status") == 0:
                withdraw_disabled.append({
                    "currency": currency,
                    "status_text": "출금 불가"
                })
        
        result = {
            "deposit_disabled": deposit_disabled,
            "withdraw_disabled": withdraw_disabled,
            "raw_data": coin_status
        }
        
        # 캐시 저장
        if use_cache:
            cache_data(cache_key, result)
        
        return result
    
    except Exception as e:
        logger.error(f"{EXCHANGE_NAME} 출금/입금 상태 조회 중 오류: {str(e)}")
        return result

async def fetch_all_data(use_cache: bool = True) -> Dict:
    """
    빗썸의 모든 데이터를 조회하는 통합 함수
    
    Args:
        use_cache: 캐시 사용 여부
    
    Returns:
        Dict: 빗썸 데이터
    """
    try:
        async with aiohttp.ClientSession() as session:
            # 티커 정보 조회
            tickers_data = await fetch_tickers(session, use_cache)
        
        # 출금/입금 상태 조회
        deposit_withdraw_data = await fetch_deposit_withdraw_status(use_cache)
        
        # 결과 통합
        return {
            "tickers": tickers_data,
            "deposit_withdraw": deposit_withdraw_data
        }
    
    except Exception as e:
        logger.error(f"{EXCHANGE_NAME} 데이터 조회 중 오류: {str(e)}")
        return {
            "tickers": {"symbols": [], "volumes": {}, "prices": {}, "raw_data": None},
            "deposit_withdraw": {"deposit_disabled": [], "withdraw_disabled": [], "raw_data": None}
        }

async def fetch_bithumb_filtered_symbols(min_volume: float = None, use_cache: bool = True) -> Dict:
    """
    빗썸 심볼 데이터를 가져와 최소 거래량과 입출금 상태로 필터링
    
    Args:
        min_volume: 최소 거래량 (KRW), None이면 설정 파일에서 가져옴
        use_cache: 캐시 사용 여부
    
    Returns:
        Dict: 필터링된 심볼 목록 및 관련 데이터
        {
            "filtered_symbols": [필터링된 심볼 목록],
            "disabled_symbols": [입출금 중지 심볼 목록],
            "total_count": 전체 심볼 수,
            "filtered_count": 필터링 후 남은 심볼 수,
            "filter_criteria": {필터링 기준 정보}
        }
    """
    from crosskimp.common.config.app_config import get_config
    
    result = {
        "filtered_symbols": [],
        "disabled_symbols": [],
        "total_count": 0,
        "filtered_count": 0,
        "filter_criteria": {}
    }
    
    try:
        # 1. 설정 파일에서 최소 거래량 가져오기
        if min_volume is None:
            config = get_config()
            min_volume = config.get_exchange("trading.settings.min_daily_volume_krw", None)
            if min_volume is None:
                logger.error("최소 거래량 설정(trading.settings.min_daily_volume_krw)을 찾을 수 없습니다.")
                logger.error("exchange_settings.cfg 파일에 trading.settings.min_daily_volume_krw 설정이 필요합니다.")
                return result
        
        result["filter_criteria"]["min_volume"] = min_volume
        logger.info(f"{EXCHANGE_NAME} 심볼 및 거래량 조회를 시작합니다. 최소거래량은 {min_volume:,.0f} KRW입니다.")
        
        # 2. 모든 데이터 가져오기
        all_data = await fetch_all_data(use_cache=False)
        
        # 3. 데이터 추출
        symbols = all_data.get("tickers", {}).get("symbols", [])
        volume_dict = all_data.get("tickers", {}).get("volumes", {})
        
        if not symbols or not volume_dict:
            logger.error("빗썸 심볼 또는 거래량 정보를 가져오지 못했습니다.")
            return result
        
        result["total_count"] = len(symbols)
        
        # 4. 거래량 필터링
        volume_filtered_symbols = [sym for sym, vol in volume_dict.items() if vol >= min_volume]
        
        logger.info(
            f"{EXCHANGE_NAME} 거래량 필터링 결과입니다. "
            f"전체 {len(symbols)}개 중 거래량 {min_volume:,.0f} KRW 이상은 {len(volume_filtered_symbols)}개입니다."
        )
        
        # 5. 입출금 상태 필터링
        deposit_withdraw_data = all_data.get("deposit_withdraw", {})
        deposit_disabled = [item.get('currency') for item in deposit_withdraw_data.get("deposit_disabled", [])]
        withdraw_disabled = [item.get('currency') for item in deposit_withdraw_data.get("withdraw_disabled", [])]
        disabled_symbols = set(deposit_disabled + withdraw_disabled)
        
        if disabled_symbols:
            logger.info(f"{EXCHANGE_NAME} 입출금 중지 심볼: {len(disabled_symbols)}개")
            sample = sorted(list(disabled_symbols))[:min(3, len(disabled_symbols))]
            logger.info(f"{EXCHANGE_NAME} 입출금 중지 샘플: {sample}")
            result["disabled_symbols"] = list(disabled_symbols)
        
        # 6. 최종 필터링 (거래량 + 입출금 상태)
        final_filtered_symbols = [sym for sym in volume_filtered_symbols if sym not in disabled_symbols]
        
        # 7. 결과 저장
        result["filtered_symbols"] = final_filtered_symbols
        result["filtered_count"] = len(final_filtered_symbols)
        
        if len(volume_filtered_symbols) > len(final_filtered_symbols):
            removed = set(volume_filtered_symbols) - set(final_filtered_symbols)
            logger.info(f"{EXCHANGE_NAME} 입출금 중지로 제외된 심볼: {len(removed)}개 {sorted(list(removed))}")
            logger.info(f"{EXCHANGE_NAME} 최종 필터링 결과: {len(final_filtered_symbols)}개")
        
        return result
        
    except Exception as e:
        logger.error(f"{EXCHANGE_NAME} 필터링된 심볼 조회 중 오류: {str(e)}", exc_info=True)
        return result 