"""
공통 API 유틸리티 모듈

캐싱, HTTP 요청 처리 등 거래소 API 공통 기능을 제공합니다.
"""

import aiohttp
import requests
from typing import Dict, Optional, Any
from datetime import datetime, timedelta

# 프로젝트 공통 로거 사용
from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, COMPONENT_NAMES_KR

# 로그 설정
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

# 로그용 컴포넌트 이름
COMPONENT_NAME = COMPONENT_NAMES_KR[SystemComponent.OB_COLLECTOR.value]

# 캐싱 데이터 및 설정
_cache = {}
_cache_expiry = {}
CACHE_DURATIONS = {
    "upbit_markets": 3600,      # 1시간
    "upbit_tickers": 300,       # 5분
    "upbit_deposit_withdraw": 600,  # 10분
    "bithumb_tickers": 300,     # 5분
    "bithumb_deposit_withdraw": 600,  # 10분
    "binance_exchange_info": 3600,  # 1시간
    "binance_future_info": 3600,    # 1시간
    "bybit_spot_info": 3600,    # 1시간
    "bybit_future_info": 3600,  # 1시간
}

def set_cache_duration(cache_key: str, duration_seconds: int) -> None:
    """
    특정 캐시 키의 유효 기간 설정
    
    Args:
        cache_key: 캐시 키
        duration_seconds: 캐시 유효 기간 (초)
    """
    CACHE_DURATIONS[cache_key] = duration_seconds
    logger.debug(f"캐시 기간 설정: {cache_key} = {duration_seconds}초")

def get_cached_data(cache_key: str) -> Optional[Any]:
    """
    캐시에서 데이터 가져오기
    
    Args:
        cache_key: 캐시 키
        
    Returns:
        Optional[Any]: 캐시된 데이터 또는 None
    """
    if cache_key in _cache and cache_key in _cache_expiry:
        if _cache_expiry[cache_key] > datetime.now():
            logger.debug(f"캐시에서 데이터 로드: {cache_key}")
            return _cache[cache_key]
    return None

def cache_data(cache_key: str, data: Any) -> None:
    """
    데이터를 캐시에 저장
    
    Args:
        cache_key: 캐시 키
        data: 저장할 데이터
    """
    _cache[cache_key] = data
    duration = CACHE_DURATIONS.get(cache_key, 300)  # 기본 5분
    _cache_expiry[cache_key] = datetime.now() + timedelta(seconds=duration)
    logger.debug(f"데이터 캐싱 완료: {cache_key}, 만료 시간: {duration}초 후")

def clear_cache(cache_key: Optional[str] = None) -> None:
    """
    캐시 데이터 삭제
    
    Args:
        cache_key: 삭제할 캐시 키 (None이면 전체 삭제)
    """
    if cache_key is None:
        _cache.clear()
        _cache_expiry.clear()
        logger.debug("모든 캐시 데이터 삭제")
    elif cache_key in _cache:
        del _cache[cache_key]
        if cache_key in _cache_expiry:
            del _cache_expiry[cache_key]
        logger.debug(f"캐시 데이터 삭제: {cache_key}")

async def make_async_request(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[Dict] = None,
    headers: Optional[Dict] = None
) -> Optional[Dict]:
    """
    비동기 HTTP GET 요청 전송
    
    Args:
        session: aiohttp 세션
        url: 요청 URL
        params: URL 파라미터
        headers: HTTP 헤더
    
    Returns:
        Optional[Dict]: 응답 데이터 또는 None (실패 시)
    """
    try:
        async with session.get(url, params=params, headers=headers, timeout=10.0) as resp:
            if resp.status == 200:
                return await resp.json()
            logger.error(f"{COMPONENT_NAME} HTTP 에러 {resp.status}: {url}")
            return None
    except Exception as e:
        logger.error(f"{COMPONENT_NAME} 요청 실패 ({url}): {e}")
        return None

def make_sync_request(
    url: str,
    params: Optional[Dict] = None,
    headers: Optional[Dict] = None
) -> Optional[Dict]:
    """
    동기식 HTTP GET 요청 전송
    
    Args:
        url: 요청 URL
        params: URL 파라미터
        headers: HTTP 헤더
    
    Returns:
        Optional[Dict]: 응답 데이터 또는 None (실패 시)
    """
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10.0)
        if response.status_code == 200:
            return response.json()
        logger.error(f"HTTP 에러 {response.status_code}: {url}")
        return None
    except Exception as e:
        logger.error(f"요청 실패 ({url}): {e}")
        return None 