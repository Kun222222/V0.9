# file: core/aggregator.py

"""
거래소 심볼 필터링 모듈

이 모듈은 각 거래소의 거래 가능한 심볼을 필터링하고 관리하는 기능을 제공합니다.
주요 기능:
1. 거래소별 심볼 및 거래량 조회
2. 최소 거래량 기준 필터링
3. 거래소 간 공통 심볼 페어링
4. 필터링 결과 저장

"""

import asyncio
import json
import os
from datetime import datetime, timezone
import aiohttp
from typing import Dict, List, Set, Optional, Tuple

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants import EXCHANGE_NAMES_KR, LOG_SYSTEM

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# ============================
# 상수 정의
# ============================
# 최소 거래량 (KRW) - 이 값을 수정하여 필터링 기준 변경
MIN_VOLUME_KRW = 10_000_000_000  # 100억원

# API URL 정의
UPBIT_URLS = {
    "market": "https://api.upbit.com/v1/market/all",
    "ticker": "https://api.upbit.com/v1/ticker"
}

BITHUMB_URLS = {
    "ticker": "https://api.bithumb.com/public/ticker/ALL_KRW"
}

BINANCE_URLS = {
    "spot": "https://api.binance.com/api/v3/exchangeInfo",
    "future": "https://fapi.binance.com/fapi/v1/exchangeInfo"
}

BYBIT_URLS = {
    "spot": "https://api.bybit.com/v5/market/instruments-info?category=spot",
    "future": "https://api.bybit.com/v5/market/instruments-info?category=linear"
}

# API 요청 설정
REQUEST_TIMEOUT = 10.0  # 초
CHUNK_SIZE = 99        # Upbit API 제한
REQUEST_DELAY = 0.5    # 초

# ============================
# 유틸리티 함수
# ============================
# def create_log_directories() -> None:
#     """로그 디렉토리 생성"""
#     os.makedirs("logs/filtered_symbols", exist_ok=True)

def get_current_time_str() -> str:
    """현재 시간 문자열 반환 (파일명용)"""
    return datetime.now().strftime("%y%m%d_%H%M%S")

async def make_request(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[Dict] = None
) -> Optional[Dict]:
    """
    HTTP GET 요청 전송
    
    Args:
        session: aiohttp 세션
        url: 요청 URL
        params: URL 파라미터
    
    Returns:
        Optional[Dict]: 응답 데이터 또는 None (실패 시)
    """
    try:
        async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status == 200:
                return await resp.json()
            logger.error(f"[Request] HTTP {resp.status}: {url}")
            return None
    except Exception as e:
        logger.error(f"[Request] 요청 실패 ({url}): {e}")
        return None

# ============================
# Upbit 관련 함수
# ============================
async def fetch_upbit_symbols_and_volume(min_volume: float) -> List[str]:
    """
    Upbit 심볼 조회 및 거래량 필터링
    
    Args:
        min_volume: 최소 거래량 (KRW)
    
    Returns:
        List[str]: 필터링된 심볼 목록
    """
    try:
        logger.info(
            f"{EXCHANGE_NAMES_KR['upbit']} 심볼/거래량 조회 시작 | "
            f"최소거래량={min_volume:,.0f} KRW"
        )
        
        async with aiohttp.ClientSession() as session:
            # 1. KRW 마켓 심볼 조회
            data = await make_request(session, UPBIT_URLS["market"])
            if not data:
                logger.error(f"{EXCHANGE_NAMES_KR['upbit']} 마켓 정보 조회 실패")
                return []
                
            symbols = [
                item["market"].split('-')[1]
                for item in data
                if item["market"].startswith("KRW-")
            ]
            
            logger.info(f"{EXCHANGE_NAMES_KR['upbit']} KRW 마켓 심볼 조회 완료: {len(symbols)}개")
            
            # 2. 거래량 조회 (청크 단위로 분할)
            volume_dict: Dict[str, float] = {}
            chunks = [symbols[i:i+CHUNK_SIZE] for i in range(0, len(symbols), CHUNK_SIZE)]
            
            for i, chunk in enumerate(chunks, 1):
                markets = ",".join(f"KRW-{symbol}" for symbol in chunk)
                logger.info(
                    f"{EXCHANGE_NAMES_KR['upbit']} 거래량 조회 진행 중 | "
                    f"청크={i}/{len(chunks)}, 심볼={len(chunk)}개"
                )
                
                data = await make_request(
                    session,
                    UPBIT_URLS["ticker"],
                    {"markets": markets}
                )
                
                if not data:
                    logger.warning(
                        f"{EXCHANGE_NAMES_KR['upbit']} 거래량 조회 실패 | "
                        f"청크={i}/{len(chunks)}, markets={markets}"
                    )
                    continue
                    
                for item in data:
                    symbol = item['market'].split('-')[1]
                    volume = float(item.get('acc_trade_price_24h', 0))
                    volume_dict[symbol] = volume
                    # logger.info(
                    #     f"[Upbit] 거래량 데이터 | "
                    #     f"symbol={symbol}, volume={volume:,.0f} KRW"
                    # )
                
                await asyncio.sleep(REQUEST_DELAY)

            # 3. 거래량 필터링
            filtered_symbols = [
                sym for sym, vol in volume_dict.items()
                if vol >= min_volume
            ]
            
            logger.info(
                f"{EXCHANGE_NAMES_KR['upbit']} 필터링 결과 | "
                f"전체={len(symbols)}개, "
                f"거래량 {min_volume:,.0f} KRW 이상={len(filtered_symbols)}개"
            )
            logger.info(f"{EXCHANGE_NAMES_KR['upbit']} 필터링된 심볼: {sorted(filtered_symbols)}")
            
            return filtered_symbols

    except Exception as e:
        logger.error(
            f"{EXCHANGE_NAMES_KR['upbit']} 심볼/거래량 조회 실패: {str(e)}",
            exc_info=True
        )
        return []

# ============================
# Bithumb 관련 함수
# ============================
async def fetch_bithumb_symbols_and_volume(min_volume: float) -> List[str]:
    """
    Bithumb 심볼 조회 및 거래량 필터링
    
    Args:
        min_volume: 최소 거래량 (KRW)
    
    Returns:
        List[str]: 필터링된 심볼 목록
    """
    try:
        logger.info(
            f"{EXCHANGE_NAMES_KR['bithumb']} 심볼/거래량 조회 시작 | "
            f"최소거래량={min_volume:,.0f} KRW"
        )
        
        async with aiohttp.ClientSession() as session:
            data = await make_request(session, BITHUMB_URLS["ticker"])
            if not data or data.get("status") != "0000":
                logger.error(
                    f"{EXCHANGE_NAMES_KR['bithumb']} API 응답 오류 | "
                    f"status={data.get('status') if data else 'No data'}"
                )
                return []
            
            market_data = data["data"]
            symbols = [sym for sym in market_data.keys() if sym != "date"]
            
            # 거래량 필터링
            filtered_symbols = []
            for symbol in symbols:
                try:
                    volume = float(market_data[symbol].get("acc_trade_value_24H", 0))
                    if volume >= min_volume:
                        filtered_symbols.append(symbol)
                    # logger.info(
                    #     f"[Bithumb] 거래량 데이터 | "
                    #     f"symbol={symbol}, volume={volume:,.0f} KRW"
                    # )
                except (KeyError, ValueError) as e:
                    logger.warning(
                        f"{EXCHANGE_NAMES_KR['bithumb']} 거래량 파싱 실패 | "
                        f"symbol={symbol}, error={str(e)}"
                    )
            
            logger.info(
                f"{EXCHANGE_NAMES_KR['bithumb']} 필터링 결과 | "
                f"전체={len(symbols)}개, "
                f"거래량 {min_volume:,.0f} KRW 이상={len(filtered_symbols)}개"
            )
            logger.info(f"{EXCHANGE_NAMES_KR['bithumb']} 필터링된 심볼: {sorted(filtered_symbols)}")
            
            return filtered_symbols

    except Exception as e:
        logger.error(
            f"{EXCHANGE_NAMES_KR['bithumb']} 심볼/거래량 조회 실패: {str(e)}",
            exc_info=True
        )
        return []

# ============================
# Binance 관련 함수
# ============================
async def fetch_binance_symbols(min_volume: float) -> List[str]:
    """
    Binance Spot/Margin/Future 공통 심볼 조회
    
    Args:
        min_volume: 최소 거래량 (KRW, 현재는 미사용)
    
    Returns:
        List[str]: 필터링된 심볼 목록
    """
    try:
        logger.info(f"{EXCHANGE_NAMES_KR['binance']} 심볼 조회 시작")
        
        async with aiohttp.ClientSession() as session:
            # 1. Spot/Margin 심볼
            data = await make_request(session, BINANCE_URLS["spot"])
            if not data:
                logger.error(f"{EXCHANGE_NAMES_KR['binance']} Spot API 응답 실패")
                return []
                
            spot_symbols = {
                s["baseAsset"]
                for s in data.get("symbols", [])
                if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
            }
            
            margin_symbols = {
                s["baseAsset"]
                for s in data.get("symbols", [])
                if s["quoteAsset"] == "USDT" 
                and s.get("isMarginTradingAllowed", False)
                and s["status"] == "TRADING"
            }
            
            logger.info(
                f"{EXCHANGE_NAMES_KR['binance']} Spot/Margin 심볼 조회 완료 | "
                f"spot={len(spot_symbols)}개, margin={len(margin_symbols)}개"
            )
            
            # 2. Future 심볼
            data = await make_request(session, BINANCE_URLS["future"])
            if not data:
                logger.error(f"{EXCHANGE_NAMES_KR['binance']} Future API 응답 실패")
                return []
                
            future_symbols = {
                s["baseAsset"]
                for s in data.get("symbols", [])
                if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
            }
            
            logger.info(
                f"{EXCHANGE_NAMES_KR['binance']} Future 심볼 조회 완료 | "
                f"future={len(future_symbols)}개"
            )
            
            # 3. 교집합 계산
            common_symbols = spot_symbols & margin_symbols & future_symbols
            
            logger.info(
                f"{EXCHANGE_NAMES_KR['binance']} 심볼 통계 | "
                f"Spot({len(spot_symbols)}), "
                f"Margin({len(margin_symbols)}), "
                f"Future({len(future_symbols)}), "
                f"공통={len(common_symbols)}"
            )
            logger.info(f"{EXCHANGE_NAMES_KR['binance']} 공통 심볼: {sorted(list(common_symbols))}")
            
            return list(common_symbols)

    except Exception as e:
        logger.error(
            f"{EXCHANGE_NAMES_KR['binance']} 심볼 조회 실패: {str(e)}",
            exc_info=True
        )
        return []

# ============================
# Bybit 관련 함수
# ============================
async def fetch_bybit_symbols(min_volume: float) -> List[str]:
    """
    Bybit Spot/Future 공통 심볼 조회
    
    Args:
        min_volume: 최소 거래량 (KRW, 현재는 미사용)
    
    Returns:
        List[str]: 필터링된 심볼 목록
    """
    try:
        logger.info(f"{EXCHANGE_NAMES_KR['bybit']} 심볼 조회 시작")
        
        async with aiohttp.ClientSession() as session:
            # 1. Spot 심볼
            data = await make_request(session, BYBIT_URLS["spot"])
            if not data or data.get("retCode") != 0:
                logger.error(f"{EXCHANGE_NAMES_KR['bybit']} Spot API 응답 실패")
                return []
                
            spot_symbols = {
                item["baseCoin"]
                for item in data.get("result", {}).get("list", [])
                if item["quoteCoin"] == "USDT" and item["status"] == "Trading"
            }
            
            # 2. Future 심볼
            data = await make_request(session, BYBIT_URLS["future"])
            if not data or data.get("retCode") != 0:
                logger.error(f"{EXCHANGE_NAMES_KR['bybit']} Future API 응답 실패")
                return []
                
            future_symbols = {
                item["baseCoin"]
                for item in data.get("result", {}).get("list", [])
                if item["quoteCoin"] == "USDT" and item["status"] == "Trading"
            }
            
            # 3. 교집합 계산
            common_symbols = spot_symbols & future_symbols
            
            logger.info(
                f"{EXCHANGE_NAMES_KR['bybit']} 심볼 통계: "
                f"Spot({len(spot_symbols)}), Future/Linear({len(future_symbols)})"
            )
            logger.info(f"{EXCHANGE_NAMES_KR['bybit']} 공통 심볼: {sorted(list(common_symbols))}")
            
            return list(common_symbols)

    except Exception as e:
        logger.error(
            f"{EXCHANGE_NAMES_KR['bybit']} 심볼 조회 실패: {str(e)}",
            exc_info=True
        )
        return []

# ============================
# 상장 시간 조회 함수
# ============================
async def fetch_listing_times(symbols: List[str]) -> Dict[str, Dict[str, datetime]]:
    """
    바이낸스 선물과 바이빗 선물의 상장 시간을 조회합니다.
    
    Args:
        symbols: 조회할 심볼 목록
    
    Returns:
        Dict[str, Dict[str, datetime]]: 거래소별 심볼의 상장 시간 정보
        {
            "binancefuture": {"BTC": datetime(...), ...},
            "bybitfuture": {"BTC": datetime(...), ...}
        }
    """
    try:
        listing_times = {
            "binancefuture": {},
            "bybitfuture": {}
        }
        
        async with aiohttp.ClientSession() as session:
            # 바이낸스 선물 상장 시간 조회
            data = await make_request(session, BINANCE_URLS["future"])
            if data and "symbols" in data:
                for item in data["symbols"]:
                    symbol = item["baseAsset"]
                    if symbol in symbols and "onboardDate" in item:
                        onboard_date = datetime.fromtimestamp(
                            int(item["onboardDate"]) / 1000,
                            tz=timezone.utc
                        )
                        listing_times["binancefuture"][symbol] = onboard_date
            
            # 바이빗 선물 상장 시간 조회
            data = await make_request(session, BYBIT_URLS["future"])
            if data and data.get("retCode") == 0:
                for item in data.get("result", {}).get("list", []):
                    symbol = item["baseCoin"]
                    if symbol in symbols and "launchTime" in item:
                        launch_time = datetime.fromtimestamp(
                            int(item["launchTime"]) / 1000,
                            tz=timezone.utc
                        )
                        listing_times["bybitfuture"][symbol] = launch_time
            
            # 현재 시간 (UTC)
            now = datetime.now(timezone.utc)
            
            # 상장 시간 로깅
            logger.info(f"{LOG_SYSTEM} === 선물 거래소 상장 시간 ===")
            for exchange, times in listing_times.items():
                logger.info(f"\n[{EXCHANGE_NAMES_KR[exchange]}]")
                for symbol, listing_time in sorted(times.items()):
                    age = now - listing_time
                    days = age.days
                    hours = age.seconds // 3600
                    minutes = (age.seconds % 3600) // 60
                    
                    logger.debug(
                        f"- {symbol}: {listing_time.strftime('%Y-%m-%d %H:%M:%S')} UTC"
                        f" (상장 후 {days}일 {hours}시간 {minutes}분)"
                    )
        
        return listing_times
        
    except Exception as e:
        logger.error(f"선물 거래소 상장 시간 조회 실패: {str(e)}", exc_info=True)
        return {"binancefuture": {}, "bybitfuture": {}}

# ============================
# 최근 상장 심볼 필터링 함수
# ============================
def filter_recently_listed_symbols(
    filtered_data: Dict[str, List[str]],
    listing_times: Dict[str, Dict[str, datetime]],
    min_hours_since_listing: int = 10
) -> Dict[str, List[str]]:
    """
    상장 후 일정 시간이 지나지 않은 심볼을 모든 거래소에서 제외합니다.
    
    Args:
        filtered_data: 거래소별 필터링된 심볼 목록
        listing_times: 거래소별 심볼 상장 시간 정보
        min_hours_since_listing: 최소 상장 경과 시간 (시간)
    
    Returns:
        Dict[str, List[str]]: 최종 필터링된 심볼 목록
    """
    try:
        # 현재 시간 (UTC)
        now = datetime.now(timezone.utc)
        
        # 최근 상장된 심볼 목록
        recently_listed = set()
        
        # 각 거래소별 최근 상장 심볼 확인
        for exchange, times in listing_times.items():
            for symbol, listing_time in times.items():
                time_diff = now - listing_time
                hours_since_listing = time_diff.days * 24 + time_diff.seconds // 3600
                
                if hours_since_listing < min_hours_since_listing:
                    recently_listed.add(symbol)
                    logger.info(
                        f"{LOG_SYSTEM} 최근 상장 제외: {symbol} "
                        f"(상장 후 {hours_since_listing}시간, "
                        f"기준: {min_hours_since_listing}시간)"
                    )
        
        # 모든 거래소에서 최근 상장 심볼 제외
        if recently_listed:
            logger.info(f"{LOG_SYSTEM} === 최근 상장으로 제외된 심볼 === {sorted(list(recently_listed))}")
            
            for exchange in filtered_data:
                before_symbols = filtered_data[exchange]
                filtered_data[exchange] = [
                    s for s in filtered_data[exchange]
                    if s not in recently_listed
                ]
                
                removed = set(before_symbols) - set(filtered_data[exchange])
                if removed:
                    logger.info(
                        f"{EXCHANGE_NAMES_KR[exchange]} 최근 상장으로 제외된 심볼: {sorted(list(removed))}"
                    )
        
        return filtered_data
        
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 최근 상장 심볼 필터링 실패: {str(e)}", exc_info=True)
        return filtered_data

# ============================
# 심볼 페어링 함수
# ============================
async def get_paired_symbols(
    upbit_symbols: List[str],
    bithumb_symbols: List[str],
    binance_symbols: List[str],
    bybit_symbols: List[str]
) -> Dict[str, Set[str]]:
    """
    거래소 간 공통 심볼 페어링
    
    Args:
        upbit_symbols: Upbit 심볼 목록
        bithumb_symbols: Bithumb 심볼 목록
        binance_symbols: Binance 심볼 목록
        bybit_symbols: Bybit 심볼 목록
    
    Returns:
        Dict[str, Set[str]]: 거래소별 최종 심볼 세트
    """
    try:
        logger.info("[Pairing] 거래소 간 동시 상장 분석 시작")
        
        # 1. 국내 거래소 ↔ 해외 거래소 페어링
        pair_bithumb_binance = set(bithumb_symbols) & set(binance_symbols)
        pair_bithumb_bybit = set(bithumb_symbols) & set(bybit_symbols)
        pair_upbit_binance = set(upbit_symbols) & set(binance_symbols)
        pair_upbit_bybit = set(upbit_symbols) & set(bybit_symbols)
        
        # 2. 결과 로깅
        logger.info(f"{LOG_SYSTEM} === 거래소 간 동시 상장 현황 ===")
        logger.info(f"빗썸↔바이낸스: {len(pair_bithumb_binance)}개 → {sorted(list(pair_bithumb_binance))}")
        logger.info(f"빗썸↔바이빗: {len(pair_bithumb_bybit)}개 → {sorted(list(pair_bithumb_bybit))}")
        logger.info(f"업비트↔바이낸스: {len(pair_upbit_binance)}개 → {sorted(list(pair_upbit_binance))}")
        logger.info(f"업비트↔바이빗: {len(pair_upbit_bybit)}개 → {sorted(list(pair_upbit_bybit))}")
        
        # 3. 최종 심볼 세트 구성
        final_symbols = {
            "bithumb": pair_bithumb_binance | pair_bithumb_bybit,
            "upbit": pair_upbit_binance | pair_upbit_bybit,
            "binance": pair_bithumb_binance | pair_upbit_binance,
            "bybit": pair_bithumb_bybit | pair_upbit_bybit,
            "binancefuture": pair_bithumb_binance | pair_upbit_binance,
            "bybitfuture": pair_bithumb_bybit | pair_upbit_bybit
        }
        
        # USDT 추가 (국내 거래소)
        for exchange in ["upbit", "bithumb"]:
            final_symbols[exchange] = set(list(final_symbols[exchange]) + ["USDT"])
        
        return final_symbols

    except Exception as e:
        logger.error(f"[Pairing] 심볼 페어링 실패: {e}")
        return {}

# ============================
# Aggregator 클래스
# ============================
class Aggregator:
    """거래소 심볼 필터링 및 관리 클래스"""
    
    # 거래소 이름 매핑
    EXCHANGE_NAMES = {
        "upbit": "업비트",
        "bithumb": "빗썸",
        "binance": "바이낸스",
        "bybit": "바이빗",
        "binancefuture": "바이낸스선물",
        "bybitfuture": "바이빗선물"
    }

    def __init__(self, settings: Dict):
        """
        Args:
            settings: 설정 데이터
        """
        self.settings = settings
        self.exchanges = [
            "binancefuture",
            "bybitfuture",
            "binance",
            "bybit",
            "upbit",
            "bithumb"
        ]
        
        # 최소 거래량 - 상수 사용
        self.min_volume = MIN_VOLUME_KRW
        
        # 심볼 캐시
        self._binance_symbols_cache: Optional[List[str]] = None
        self._bybit_symbols_cache: Optional[List[str]] = None
        
        # create_log_directories()

    async def run_filtering(self) -> Dict[str, List[str]]:
        """
        거래소별 심볼 필터링 실행
        
        Returns:
            Dict[str, List[str]]: 거래소별 필터링된 심볼 목록
        """
        try:
            logger.info(f"{LOG_SYSTEM} === 심볼 필터링 시작 ===")
            logger.info(f"{LOG_SYSTEM} 최소 거래량: {self.min_volume:,} KRW")
            
            # 1. 거래소별 필터링
            filtered_data: Dict[str, List[str]] = {}
            
            # Upbit
            upbit_symbols = await fetch_upbit_symbols_and_volume(self.min_volume)
            if upbit_symbols:
                filtered_data["upbit"] = upbit_symbols
            
            # Bithumb
            bithumb_symbols = await fetch_bithumb_symbols_and_volume(self.min_volume)
            if bithumb_symbols:
                filtered_data["bithumb"] = bithumb_symbols
            
            # Binance
            binance_symbols = await fetch_binance_symbols(self.min_volume)
            if binance_symbols:
                filtered_data["binance"] = binance_symbols
                filtered_data["binancefuture"] = binance_symbols
            
            # Bybit
            bybit_symbols = await fetch_bybit_symbols(self.min_volume)
            if bybit_symbols:
                filtered_data["bybit"] = bybit_symbols
                filtered_data["bybitfuture"] = bybit_symbols
            
            # 2. 거래소 간 페어링
            if len(filtered_data) >= 4:
                paired_data = await get_paired_symbols(
                    filtered_data.get("upbit", []),
                    filtered_data.get("bithumb", []),
                    filtered_data.get("binance", []),
                    filtered_data.get("bybit", [])
                )
                filtered_data = {k: list(v) for k, v in paired_data.items()}
            
            # 3. 제외 심볼 필터링
            excluded_symbols = set(self.settings.get("trading", {})
                                 .get("excluded_symbols", {})
                                 .get("list", []))
            
            logger.info(f"{LOG_SYSTEM} === 제외할 심볼 === {sorted(list(excluded_symbols))}")
            
            for exchange in filtered_data:
                before_symbols = filtered_data[exchange]
                filtered_data[exchange] = [
                    s for s in filtered_data[exchange]
                    if s not in excluded_symbols
                ]
                
                removed = set(before_symbols) - set(filtered_data[exchange])
                if removed:
                    logger.info(
                        f"{EXCHANGE_NAMES_KR[exchange]} 제외된 심볼: {sorted(list(removed))}"
                    )
            
            # 4. 선물 거래소 상장 시간 조회
            all_symbols = set()
            for symbols in filtered_data.values():
                all_symbols.update(symbols)
            listing_times = await fetch_listing_times(list(all_symbols))
            
            # 5. 최근 상장 심볼 필터링
            filtered_data = filter_recently_listed_symbols(
                filtered_data,
                listing_times
            )
            
            logger.info(f"{LOG_SYSTEM} === 거래소별 최종 구독 심볼 ===")
            for exchange, symbols in filtered_data.items():
                name = EXCHANGE_NAMES_KR.get(exchange, exchange)
                logger.info(f"{name} 구독 심볼: {len(symbols)}개 → {sorted(symbols)}")
            
            return filtered_data

        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 심볼 필터링 실패: {e}")
            return {}

