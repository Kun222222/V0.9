"""
업비트 API 모듈

업비트 거래소의 API를 호출하는 함수들을 제공합니다.
"""

import os
import uuid
import asyncio
import aiohttp
from typing import Dict, List, Optional, Any

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
UPBIT_ACCESS_KEY = env_loader.get("upbit.api_key", "")
UPBIT_SECRET_KEY = env_loader.get("upbit.api_secret", "")

logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

# 로그용 거래소 이름
EXCHANGE_NAME = EXCHANGE_NAMES_KR[Exchange.UPBIT.value]

# ============================
# 업비트 API 함수
# ============================
async def fetch_markets(session: Optional[aiohttp.ClientSession] = None, use_cache: bool = True) -> Dict:
    """
    업비트 마켓 정보 조회
    
    Args:
        session: aiohttp 세션 (없으면 생성)
        use_cache: 캐시 사용 여부
    
    Returns:
        Dict: 마켓 정보
    """
    cache_key = "upbit_markets"
    
    # 캐시 확인
    if use_cache:
        cached_data = get_cached_data(cache_key)
        if cached_data:
            return cached_data
    
    result = {"symbols": [], "raw_data": []}
    
    try:
        should_close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            should_close_session = True
        
        data = await make_async_request(session, "https://api.upbit.com/v1/market/all")
        
        if should_close_session:
            await session.close()
            
        if not data:
            logger.error(f"{EXCHANGE_NAME} 마켓 정보 조회 실패")
            return result
            
        # KRW 마켓만 필터링
        krw_markets = [
            item for item in data
            if item["market"].startswith("KRW-")
        ]
        
        # 심볼 목록 추출
        symbols = [item["market"].split('-')[1] for item in krw_markets]
        
        result = {
            "symbols": symbols,
            "raw_data": krw_markets
        }
        
        # 캐시 저장
        if use_cache:
            cache_data(cache_key, result)
        
        return result
    
    except Exception as e:
        logger.error(f"{EXCHANGE_NAME} 마켓 정보 조회 중 오류: {str(e)}")
        return result

async def fetch_tickers(symbols: List[str], session: Optional[aiohttp.ClientSession] = None, use_cache: bool = True) -> Dict:
    """
    업비트 거래량 및 가격 정보 조회
    
    Args:
        symbols: 조회할 심볼 목록
        session: aiohttp 세션 (없으면 생성)
        use_cache: 캐시 사용 여부
    
    Returns:
        Dict: 거래량 및 가격 정보
    """
    cache_key = "upbit_tickers"
    
    # 캐시 확인
    if use_cache:
        cached_data = get_cached_data(cache_key)
        if cached_data:
            return cached_data
    
    result = {"volumes": {}, "prices": {}, "raw_data": []}
    
    if not symbols:
        return result
    
    try:
        should_close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            should_close_session = True
        
        # 청크 단위로 나누어 요청 (업비트 API 제한)
        chunks = [symbols[i:i+99] for i in range(0, len(symbols), 99)]
        all_data = []
        
        for chunk in chunks:
            markets = ",".join(f"KRW-{symbol}" for symbol in chunk)
            data = await make_async_request(
                session, 
                "https://api.upbit.com/v1/ticker", 
                {"markets": markets}
            )
            
            if data:
                all_data.extend(data)
            
            # API 호출 간 약간의 딜레이
            await asyncio.sleep(0.2)
        
        if should_close_session:
            await session.close()
        
        # 결과 처리
        volumes = {}
        prices = {}
        
        for item in all_data:
            symbol = item['market'].split('-')[1]
            volume = float(item.get('acc_trade_price_24h', 0))
            price = float(item.get('trade_price', 0))
            volumes[symbol] = volume
            prices[symbol] = price
        
        result = {
            "volumes": volumes,
            "prices": prices,
            "raw_data": all_data
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
    업비트 출금/입금 상태 조회
    
    Args:
        use_cache: 캐시 사용 여부
    
    Returns:
        Dict: 출금/입금 상태 정보
    """
    cache_key = "upbit_deposit_withdraw"
    
    # 캐시 확인
    if use_cache:
        cached_data = get_cached_data(cache_key)
        if cached_data:
            return cached_data
    
    result = {
        "deposit_disabled": [],
        "withdraw_disabled": [],
        "raw_data": []
    }
    
    try:
        # 출금 상태 조회를 위한 API 호출 준비 (JWT 인증 필요)
        if not UPBIT_ACCESS_KEY or not UPBIT_SECRET_KEY:
            logger.warning(f"{EXCHANGE_NAME} API 키가 설정되지 않았습니다. 출금/입금 상태를 조회할 수 없습니다.")
            return result
        
        # JWT 토큰 생성
        import jwt
        
        payload = {
            'access_key': UPBIT_ACCESS_KEY,
            'nonce': str(uuid.uuid4()),
        }
        
        jwt_token = jwt.encode(payload, UPBIT_SECRET_KEY)
        headers = {"Authorization": f"Bearer {jwt_token}"}
        
        # API 호출 (동기식)
        response = make_sync_request(
            "https://api.upbit.com/v1/status/wallet",
            headers=headers
        )
        
        if not response:
            logger.error(f"{EXCHANGE_NAME} 출금/입금 상태 조회 실패")
            return result
        
        # 결과 처리
        deposit_disabled = []
        withdraw_disabled = []
        
        for status in response:
            currency = status.get('currency')
            
            if currency:
                # 입금 불가 확인
                if not status.get('deposit_status', True):
                    deposit_disabled.append({
                        'currency': currency,
                        'wallet_state': status.get('wallet_state', ''),
                        'block_reason': status.get('block_reason', '')
                    })
                
                # 출금 불가 확인
                if not status.get('withdraw_status', True):
                    withdraw_disabled.append({
                        'currency': currency,
                        'wallet_state': status.get('wallet_state', ''),
                        'block_reason': status.get('block_reason', '')
                    })
        
        result = {
            "deposit_disabled": deposit_disabled,
            "withdraw_disabled": withdraw_disabled,
            "raw_data": response
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
    업비트의 모든 데이터를 조회하는 통합 함수
    
    Args:
        use_cache: 캐시 사용 여부
    
    Returns:
        Dict: 업비트 데이터
    """
    try:
        async with aiohttp.ClientSession() as session:
            # 마켓 정보 조회
            markets_data = await fetch_markets(session, use_cache)
            
            # 티커 정보 조회
            symbols = markets_data.get("symbols", [])
            tickers_data = await fetch_tickers(symbols, session, use_cache)
        
        # 출금/입금 상태 조회
        deposit_withdraw_data = await fetch_deposit_withdraw_status(use_cache)
        
        # 결과 통합
        return {
            "markets": markets_data,
            "tickers": tickers_data,
            "deposit_withdraw": deposit_withdraw_data
        }
    
    except Exception as e:
        logger.error(f"{EXCHANGE_NAME} 데이터 조회 중 오류: {str(e)}")
        return {
            "markets": {"symbols": [], "raw_data": []},
            "tickers": {"volumes": {}, "prices": {}, "raw_data": []},
            "deposit_withdraw": {"deposit_disabled": [], "withdraw_disabled": [], "raw_data": []}
        }

async def fetch_upbit_filtered_symbols(min_volume: float = None, use_cache: bool = True) -> Dict:
    """
    업비트 심볼 데이터를 가져와 최소 거래량과 입출금 상태로 필터링
    
    Args:
        min_volume: 최소 거래량 (KRW), None이면 설정 파일에서 가져옴
        use_cache: 캐시 사용 여부
    
    Returns:
        Dict: 필터링된 심볼 목록 및 관련 데이터
        {
            "filtered_symbols": [필터링된 심볼 목록],
            "prices": {심볼별 가격 정보},
            "disabled_symbols": [입출금 중지 심볼 목록],
            "total_count": 전체 심볼 수,
            "filtered_count": 필터링 후 남은 심볼 수,
            "filter_criteria": {필터링 기준 정보}
        }
    """
    from crosskimp.common.config.app_config import get_config
    
    result = {
        "filtered_symbols": [],
        "prices": {},
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
        symbols = all_data.get("markets", {}).get("symbols", [])
        volume_dict = all_data.get("tickers", {}).get("volumes", {})
        price_dict = all_data.get("tickers", {}).get("prices", {})
        
        if not symbols or not volume_dict:
            logger.error("업비트 심볼 또는 거래량 정보를 가져오지 못했습니다.")
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
            result["disabled_symbols"] = list(disabled_symbols)
        
        # 6. 최종 필터링 (거래량 + 입출금 상태)
        final_filtered_symbols = [sym for sym in volume_filtered_symbols if sym not in disabled_symbols]
        
        # 7. 필터링된 심볼에 대한 가격 정보
        filtered_prices = {sym: price_dict[sym] for sym in final_filtered_symbols if sym in price_dict}
        
        # 8. 결과 저장
        result["filtered_symbols"] = final_filtered_symbols
        result["prices"] = filtered_prices
        result["filtered_count"] = len(final_filtered_symbols)
        
        return result
        
    except Exception as e:
        logger.error(f"{EXCHANGE_NAME} 필터링된 심볼 조회 중 오류: {str(e)}", exc_info=True)
        return result 