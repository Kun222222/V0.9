"""
바이낸스 API 모듈

바이낸스 거래소의 API를 호출하는 함수들을 제공합니다.
"""

import asyncio
import aiohttp
from typing import Dict, Set, Optional, Any
from datetime import datetime, timezone

# 프로젝트 공통 로거 사용
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange, EXCHANGE_NAMES_KR
from crosskimp.common.config.env_loader import get_env_loader

from .api_common import (
    get_cached_data,
    cache_data,
    make_async_request
)

# 환경변수 로더 가져오기
env_loader = get_env_loader()

# API 키 정보 (환경변수 로더에서 가져오기)
BINANCE_API_KEY = env_loader.get("binance.api_key", "")
BINANCE_API_SECRET = env_loader.get("binance.api_secret", "")

logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

# 로그용 거래소 이름
EXCHANGE_NAME = EXCHANGE_NAMES_KR[Exchange.BINANCE_SPOT.value]
FUTURE_EXCHANGE_NAME = EXCHANGE_NAMES_KR[Exchange.BINANCE_FUTURE.value]

# ============================
# 바이낸스 API 함수
# ============================
async def fetch_exchange_info(session: Optional[aiohttp.ClientSession] = None, use_cache: bool = True) -> Dict:
    """
    바이낸스 거래소 정보 조회
    
    Args:
        session: aiohttp 세션 (없으면 생성)
        use_cache: 캐시 사용 여부
    
    Returns:
        Dict: 거래소 정보
    """
    cache_key = "binance_exchange_info"
    
    # 캐시 확인
    if use_cache:
        cached_data = get_cached_data(cache_key)
        if cached_data:
            return cached_data
    
    result = {
        "spot_symbols": set(),
        "margin_symbols": set(),
        "raw_data": None
    }
    
    try:
        should_close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            should_close_session = True
        
        data = await make_async_request(session, "https://api.binance.com/api/v3/exchangeInfo")
        
        if should_close_session:
            await session.close()
            
        if not data:
            logger.error(f"{EXCHANGE_NAME} 거래소 정보 조회 실패")
            return result
        
        # 스팟 및 마진 심볼 추출
        spot_symbols = set()
        margin_symbols = set()
        
        for symbol_data in data.get("symbols", []):
            if symbol_data["quoteAsset"] == "USDT" and symbol_data["status"] == "TRADING":
                base_asset = symbol_data["baseAsset"]
                spot_symbols.add(base_asset)
                
                if symbol_data.get("isMarginTradingAllowed", False):
                    margin_symbols.add(base_asset)
        
        result = {
            "spot_symbols": spot_symbols,
            "margin_symbols": margin_symbols,
            "raw_data": data
        }
        
        # 캐시 저장
        if use_cache:
            cache_data(cache_key, result)
        
        return result
    
    except Exception as e:
        logger.error(f"{EXCHANGE_NAME} 거래소 정보 조회 중 오류: {str(e)}")
        if 'session' in locals() and should_close_session and session:
            try:
                await session.close()
            except:
                pass
        return result

async def fetch_future_info(session: Optional[aiohttp.ClientSession] = None, use_cache: bool = True) -> Dict:
    """
    바이낸스 선물 거래소 정보 조회
    
    Args:
        session: aiohttp 세션 (없으면 생성)
        use_cache: 캐시 사용 여부
    
    Returns:
        Dict: 선물 거래소 정보
    """
    cache_key = "binance_future_info"
    
    # 캐시 확인
    if use_cache:
        cached_data = get_cached_data(cache_key)
        if cached_data:
            return cached_data
    
    result = {
        "future_symbols": set(),
        "listing_times": {},
        "raw_data": None
    }
    
    try:
        should_close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            should_close_session = True
        
        data = await make_async_request(session, "https://fapi.binance.com/fapi/v1/exchangeInfo")
        
        if should_close_session:
            await session.close()
            
        if not data:
            logger.error(f"{FUTURE_EXCHANGE_NAME} 거래소 정보 조회 실패")
            return result
        
        # 선물 심볼 및 상장 시간 추출
        future_symbols = set()
        listing_times = {}
        
        for symbol_data in data.get("symbols", []):
            if symbol_data["quoteAsset"] == "USDT" and symbol_data["status"] == "TRADING":
                base_asset = symbol_data["baseAsset"]
                future_symbols.add(base_asset)
                
                # 상장 시간이 있으면 저장
                if "onboardDate" in symbol_data:
                    onboard_date = datetime.fromtimestamp(
                        int(symbol_data["onboardDate"]) / 1000,
                        tz=timezone.utc
                    )
                    listing_times[base_asset] = onboard_date
        
        result = {
            "future_symbols": future_symbols,
            "listing_times": listing_times,
            "raw_data": data
        }
        
        # 캐시 저장
        if use_cache:
            cache_data(cache_key, result)
        
        return result
    
    except Exception as e:
        logger.error(f"{FUTURE_EXCHANGE_NAME} 거래소 정보 조회 중 오류: {str(e)}")
        if 'session' in locals() and should_close_session and session:
            try:
                await session.close()
            except:
                pass
        return result

async def fetch_all_data(use_cache: bool = True) -> Dict:
    """
    바이낸스의 모든 데이터를 조회하는 통합 함수
    
    Args:
        use_cache: 캐시 사용 여부
    
    Returns:
        Dict: 바이낸스 데이터
    """
    try:
        async with aiohttp.ClientSession() as session:
            # 거래소 정보 조회
            exchange_info_task = fetch_exchange_info(session, use_cache)
            future_info_task = fetch_future_info(session, use_cache)
            
            # 병렬 처리
            exchange_info, future_info = await asyncio.gather(
                exchange_info_task,
                future_info_task
            )
        
        # 결과 통합
        return {
            "exchange_info": exchange_info,
            "future_info": future_info
        }
    
    except Exception as e:
        logger.error(f"{EXCHANGE_NAME} 데이터 조회 중 오류: {str(e)}")
        return {
            "exchange_info": {"spot_symbols": set(), "margin_symbols": set(), "raw_data": None},
            "future_info": {"future_symbols": set(), "listing_times": {}, "raw_data": None}
        }

async def fetch_binance_filtered_symbols(min_volume: float = None, use_cache: bool = True, min_hours_since_listing: int = 10) -> Dict:
    """
    바이낸스 현물/마진/선물 공통 심볼 조회 및 필터링 함수
    
    Args:
        min_volume: 최소 거래량 (KRW), 바이낸스에서는 현재 사용하지 않음
        use_cache: 캐시 사용 여부
        min_hours_since_listing: 최소 상장 경과 시간 (시간)
    
    Returns:
        Dict: 필터링된 심볼 목록 및 관련 데이터
        {
            "filtered_symbols": [필터링된 심볼 목록],
            "total_count": 전체 심볼 수,
            "spot_count": 스팟 심볼 수, 
            "margin_count": 마진 심볼 수,
            "future_count": 선물 심볼 수,
            "filtered_count": 필터링 후 남은 심볼 수,
            "recently_listed_excluded": [최근 상장으로 제외된 심볼 목록]
        }
    """
    result = {
        "filtered_symbols": [],
        "total_count": 0,
        "spot_count": 0,
        "margin_count": 0,
        "future_count": 0,
        "filtered_count": 0,
        "recently_listed_excluded": []
    }
    
    try:
        logger.info(f"{EXCHANGE_NAME} 심볼 조회를 시작합니다.")
        
        # 모든 데이터 가져오기
        all_data = await fetch_all_data(use_cache=use_cache)
        
        # 데이터 추출
        exchange_info = all_data.get("exchange_info", {})
        future_info = all_data.get("future_info", {})
        
        spot_symbols = exchange_info.get("spot_symbols", set())
        margin_symbols = exchange_info.get("margin_symbols", set())
        future_symbols = future_info.get("future_symbols", set())
        listing_times = future_info.get("listing_times", {})
        
        # 결과 기록
        result["spot_count"] = len(spot_symbols)
        result["margin_count"] = len(margin_symbols)
        result["future_count"] = len(future_symbols)
        result["total_count"] = len(spot_symbols.union(margin_symbols, future_symbols))
        
        # 공통 심볼 계산 (현물, 마진, 선물 모두 상장된 심볼)
        common_symbols = spot_symbols & margin_symbols & future_symbols
        
        # 현재 시간 (UTC)
        now = datetime.now(timezone.utc)
        
        # 최근 상장된 심볼 필터링
        recently_listed = []
        filtered_symbols = []
        
        for symbol in common_symbols:
            # 상장 시간이 있는 경우만 확인
            if symbol in listing_times:
                listing_time = listing_times[symbol]
                time_diff = now - listing_time
                hours_since_listing = time_diff.days * 24 + time_diff.seconds // 3600
                
                if hours_since_listing < min_hours_since_listing:
                    # 최근 상장으로 제외
                    recently_listed.append(symbol)
                    logger.info(
                        f"{EXCHANGE_NAME} 최근 상장으로 제외: {symbol} "
                        f"(상장 후 {hours_since_listing}시간, "
                        f"기준: {min_hours_since_listing}시간)"
                    )
                else:
                    filtered_symbols.append(symbol)
            else:
                # 상장 시간 정보가 없는 경우 포함
                filtered_symbols.append(symbol)
        
        # 결과 정렬
        filtered_symbols = sorted(filtered_symbols)
        recently_listed = sorted(recently_listed)
        
        result["filtered_symbols"] = filtered_symbols
        result["recently_listed_excluded"] = recently_listed
        result["filtered_count"] = len(filtered_symbols)
        
        logger.info(
            f"{EXCHANGE_NAME} 심볼 통계입니다. "
            f"Spot({len(spot_symbols)}개), "
            f"Margin({len(margin_symbols)}개), "
            f"Future({len(future_symbols)}개), "
            f"공통 심볼은 {len(common_symbols)}개입니다."
        )
        
        if recently_listed:
            logger.info(f"{EXCHANGE_NAME} 최근 상장으로 제외된 심볼: {len(recently_listed)}개 {recently_listed}")
            
        if filtered_symbols:
            logger.info(f"{EXCHANGE_NAME} 최종 필터링된 심볼: {len(filtered_symbols)}개")
        
        return result
    
    except Exception as e:
        logger.error(f"{EXCHANGE_NAME} 필터링된 심볼 조회 중 오류: {str(e)}", exc_info=True)
        return result 