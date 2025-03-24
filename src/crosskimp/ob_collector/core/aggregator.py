# file: core/aggregator.py

"""
심볼 집계 모듈

이 모듈은 여러 거래소에서 유효한 심볼 목록을 가져와 필터링하는 기능을 제공합니다.
"""

import asyncio
from datetime import datetime, timezone
import aiohttp
import time
import json
from typing import Dict, List, Set, Optional, Any, Tuple

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import Exchange, EXCHANGE_NAMES_KR, SystemComponent
from crosskimp.common.config.app_config import get_config

# 로거 인스턴스 가져오기
logger = get_unified_logger(component=SystemComponent.ORDERBOOK.value)

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
    """
    설정에서 값 가져오기 - 애플리케이션 설정 연동
    
    Args:
        path: 설정 경로
        default: 기본값
        
    Returns:
        설정값 또는 기본값
    """
    try:
        logger.debug(f"설정 값 조회 시도: {path}, 기본값: {default}")
        
        # 거래소 설정이 제대로 로드되었는지 확인
        if not hasattr(config, 'exchange_settings') or not config.exchange_settings:
            logger.error(f"거래소 설정이 로드되지 않았습니다. exchange_settings 객체: {config.exchange_settings}")
            return default
            
        # 설정 값 가져오기
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
    """
    심볼 필터 설정 가져오기 - 애플리케이션 설정 연동
    
    Returns:
        Dict[str, list]: 심볼 필터 설정
    """
    try:
        return config.get_symbol_filters()
    except Exception as e:
        logger.error(f"심볼 필터 설정 획득 중 오류: {str(e)}")
        # 기본 빈 필터 반환
        return {"excluded": [], "included": [], "excluded_patterns": []}

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
        async with session.get(url, params=params, timeout=10.0) as resp:
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
            f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]} 심볼 및 거래량 조회를 시작합니다. "
            f"최소거래량은 {min_volume:,.0f} KRW입니다."
        )
        
        async with aiohttp.ClientSession() as session:
            # 1. KRW 마켓 심볼 조회
            data = await make_request(session, "https://api.upbit.com/v1/market/all")
            if not data:
                logger.error(f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]} 마켓 정보 조회에 실패했습니다.")
                return []
                
            symbols = [
                item["market"].split('-')[1]
                for item in data
                if item["market"].startswith("KRW-")
            ]
            
            logger.info(f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]} KRW 마켓 심볼 조회가 완료되었습니다: 총 {len(symbols)}개")
            
            # 2. 거래량 조회 (청크 단위로 분할)
            volume_dict: Dict[str, float] = {}
            chunks = [symbols[i:i+99] for i in range(0, len(symbols), 99)]
            
            for i, chunk in enumerate(chunks, 1):
                markets = ",".join(f"KRW-{symbol}" for symbol in chunk)
                logger.info(
                    f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]} 거래량 조회가 진행 중입니다. "
                    f"청크 {i}/{len(chunks)}, 심볼 {len(chunk)}개"
                )
                
                data = await make_request(session, "https://api.upbit.com/v1/ticker", {"markets": markets})
                
                if not data:
                    logger.warning(
                        f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]} 거래량 조회에 실패했습니다. "
                        f"청크 {i}/{len(chunks)}, markets={markets}"
                    )
                    continue
                    
                for item in data:
                    symbol = item['market'].split('-')[1]
                    volume = float(item.get('acc_trade_price_24h', 0))
                    volume_dict[symbol] = volume
                
                await asyncio.sleep(0.5)

            # 3. 거래량 필터링
            filtered_symbols = [
                sym for sym, vol in volume_dict.items()
                if vol >= min_volume
            ]
            
            logger.info(
                f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]} 필터링 결과입니다. "
                f"전체 {len(symbols)}개 중 거래량 {min_volume:,.0f} KRW 이상은 {len(filtered_symbols)}개입니다."
            )
            logger.info(f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]} 필터링된 심볼: {sorted(filtered_symbols)}")
            
            return filtered_symbols

    except Exception as e:
        logger.error(
            f"{EXCHANGE_NAMES_KR[Exchange.UPBIT.value]} 심볼 및 거래량 조회에 실패했습니다: {str(e)}",
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
            f"{EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]} 심볼 및 거래량 조회를 시작합니다. "
            f"최소거래량은 {min_volume:,.0f} KRW입니다."
        )
        
        async with aiohttp.ClientSession() as session:
            data = await make_request(session, "https://api.bithumb.com/public/ticker/ALL_KRW")
            if not data or data.get("status") != "0000":
                logger.error(
                    f"{EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]} API 응답에 오류가 발생했습니다. "
                    f"상태코드: {data.get('status') if data else 'No data'}"
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
                except (KeyError, ValueError) as e:
                    logger.warning(
                        f"{EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]} 거래량 파싱에 실패했습니다. "
                        f"심볼: {symbol}, 오류: {str(e)}"
                    )
            
            logger.info(
                f"{EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]} 필터링 결과입니다. "
                f"전체 {len(symbols)}개 중 거래량 {min_volume:,.0f} KRW 이상은 {len(filtered_symbols)}개입니다."
            )
            logger.info(f"{EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]} 필터링된 심볼: {sorted(filtered_symbols)}")
            
            return filtered_symbols

    except Exception as e:
        logger.error(
            f"{EXCHANGE_NAMES_KR[Exchange.BITHUMB.value]} 심볼 및 거래량 조회에 실패했습니다: {str(e)}",
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
        logger.info(f"{EXCHANGE_NAMES_KR[Exchange.BINANCE.value]} 심볼 조회를 시작합니다.")
        
        async with aiohttp.ClientSession() as session:
            # 1. Spot/Margin 심볼
            data = await make_request(session, "https://api.binance.com/api/v3/exchangeInfo")
            if not data:
                logger.error(f"{EXCHANGE_NAMES_KR[Exchange.BINANCE.value]} Spot API 응답에 실패했습니다.")
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
                f"{EXCHANGE_NAMES_KR[Exchange.BINANCE.value]} Spot/Margin 심볼 조회가 완료되었습니다. "
                f"Spot {len(spot_symbols)}개, Margin {len(margin_symbols)}개"
            )
            
            # 2. Future 심볼
            data = await make_request(session, "https://fapi.binance.com/fapi/v1/exchangeInfo")
            if not data:
                logger.error(f"{EXCHANGE_NAMES_KR[Exchange.BINANCE.value]} Future API 응답에 실패했습니다.")
                return []
                
            future_symbols = {
                s["baseAsset"]
                for s in data.get("symbols", [])
                if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
            }
            
            logger.info(
                f"{EXCHANGE_NAMES_KR[Exchange.BINANCE.value]} Future 심볼 조회가 완료되었습니다. "
                f"Future {len(future_symbols)}개"
            )
            
            # 3. 교집합 계산
            common_symbols = spot_symbols & margin_symbols & future_symbols
            
            logger.info(
                f"{EXCHANGE_NAMES_KR[Exchange.BINANCE.value]} 심볼 통계입니다. "
                f"Spot({len(spot_symbols)}개), "
                f"Margin({len(margin_symbols)}개), "
                f"Future({len(future_symbols)}개), "
                f"공통 심볼은 {len(common_symbols)}개입니다."
            )
            logger.info(f"{EXCHANGE_NAMES_KR[Exchange.BINANCE.value]} 공통 심볼: {sorted(list(common_symbols))}")
            
            return list(common_symbols)

    except Exception as e:
        logger.error(
            f"{EXCHANGE_NAMES_KR[Exchange.BINANCE.value]} 심볼 조회에 실패했습니다: {str(e)}",
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
        logger.info(f"{EXCHANGE_NAMES_KR[Exchange.BYBIT.value]} 심볼 조회를 시작합니다.")
        
        async with aiohttp.ClientSession() as session:
            # 1. Spot 심볼
            data = await make_request(session, "https://api.bybit.com/v5/market/instruments-info?category=spot")
            if not data or data.get("retCode") != 0:
                logger.error(f"{EXCHANGE_NAMES_KR[Exchange.BYBIT.value]} Spot API 응답에 실패했습니다.")
                return []
                
            spot_symbols = {
                item["baseCoin"]
                for item in data.get("result", {}).get("list", [])
                if item["quoteCoin"] == "USDT" and item["status"] == "Trading"
            }
            
            # 2. Future 심볼
            data = await make_request(session, "https://api.bybit.com/v5/market/instruments-info?category=linear")
            if not data or data.get("retCode") != 0:
                logger.error(f"{EXCHANGE_NAMES_KR[Exchange.BYBIT.value]} Future API 응답에 실패했습니다.")
                return []
                
            future_symbols = {
                item["baseCoin"]
                for item in data.get("result", {}).get("list", [])
                if item["quoteCoin"] == "USDT" and item["status"] == "Trading"
            }
            
            # 3. 교집합 계산
            common_symbols = spot_symbols & future_symbols
            
            logger.info(
                f"{EXCHANGE_NAMES_KR[Exchange.BYBIT.value]} 심볼 통계입니다: "
                f"Spot({len(spot_symbols)}개), Future/Linear({len(future_symbols)}개)"
            )
            logger.info(f"{EXCHANGE_NAMES_KR[Exchange.BYBIT.value]} 공통 심볼: {sorted(list(common_symbols))}")
            
            return list(common_symbols)

    except Exception as e:
        logger.error(
            f"{EXCHANGE_NAMES_KR[Exchange.BYBIT.value]} 심볼 조회에 실패했습니다: {str(e)}",
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
            "binance_future": {"BTC": datetime(...), ...},
            "bybit_future": {"BTC": datetime(...), ...}
        }
    """
    try:
        listing_times = {
            Exchange.BINANCE_FUTURE.value: {},
            Exchange.BYBIT_FUTURE.value: {}
        }
        
        async with aiohttp.ClientSession() as session:
            # 바이낸스 선물 상장 시간 조회
            data = await make_request(session, "https://fapi.binance.com/fapi/v1/exchangeInfo")
            if data and "symbols" in data:
                for item in data["symbols"]:
                    symbol = item["baseAsset"]
                    if symbol in symbols and "onboardDate" in item:
                        onboard_date = datetime.fromtimestamp(
                            int(item["onboardDate"]) / 1000,
                            tz=timezone.utc
                        )
                        listing_times[Exchange.BINANCE_FUTURE.value][symbol] = onboard_date
            
            # 바이빗 선물 상장 시간 조회
            data = await make_request(session, "https://api.bybit.com/v5/market/instruments-info?category=linear")
            if data and data.get("retCode") == 0:
                for item in data.get("result", {}).get("list", []):
                    symbol = item["baseCoin"]
                    if symbol in symbols and "launchTime" in item:
                        launch_time = datetime.fromtimestamp(
                            int(item["launchTime"]) / 1000,
                            tz=timezone.utc
                        )
                        listing_times[Exchange.BYBIT_FUTURE.value][symbol] = launch_time
            
            # 현재 시간 (UTC)
            now = datetime.now(timezone.utc)
            
            # 상장 시간 로깅
            logger.info(f"=== 선물 거래소 상장 시간 ===")
            for exchange, times in listing_times.items():
                logger.info(f"{EXCHANGE_NAMES_KR[exchange]}")
                for symbol, listing_time in sorted(times.items()):
                    age = now - listing_time
                    days = age.days
                    hours = age.seconds // 3600
                    minutes = (age.seconds % 3600) // 60
                    
                    logger.debug(
                        f"{symbol}: {listing_time.strftime('%Y-%m-%d %H:%M:%S')} UTC"
                        f" (상장 후 {days}일 {hours}시간 {minutes}분)"
                    )
        
        return listing_times
        
    except Exception as e:
        logger.error(f"선물 거래소 상장 시간 조회에 실패했습니다: {str(e)}", exc_info=True)
        return {Exchange.BINANCE_FUTURE.value: {}, Exchange.BYBIT_FUTURE.value: {}}

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
                        f"최근 상장으로 제외합니다: {symbol} "
                        f"(상장 후 {hours_since_listing}시간, "
                        f"기준: {min_hours_since_listing}시간)"
                    )
        
        # 모든 거래소에서 최근 상장 심볼 제외
        if recently_listed:
            logger.info(f"=== 최근 상장으로 제외된 심볼 === {sorted(list(recently_listed))}")
            
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
        logger.error(f"최근 상장 심볼 필터링에 실패했습니다: {str(e)}", exc_info=True)
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
        logger.info("거래소 간 동시 상장 분석을 시작합니다.")
        
        # 1. 국내 거래소 ↔ 해외 거래소 페어링
        pair_bithumb_binance = set(bithumb_symbols) & set(binance_symbols)
        pair_bithumb_bybit = set(bithumb_symbols) & set(bybit_symbols)
        pair_upbit_binance = set(upbit_symbols) & set(binance_symbols)
        pair_upbit_bybit = set(upbit_symbols) & set(bybit_symbols)
        
        # 2. 결과 로깅
        logger.info(f"=== 거래소 간 동시 상장 현황 ===")
        logger.info(f"빗썸↔바이낸스: {len(pair_bithumb_binance)}개 → {sorted(list(pair_bithumb_binance))}")
        logger.info(f"빗썸↔바이빗: {len(pair_bithumb_bybit)}개 → {sorted(list(pair_bithumb_bybit))}")
        logger.info(f"업비트↔바이낸스: {len(pair_upbit_binance)}개 → {sorted(list(pair_upbit_binance))}")
        logger.info(f"업비트↔바이빗: {len(pair_upbit_bybit)}개 → {sorted(list(pair_upbit_bybit))}")
        
        # 3. 최종 심볼 세트 구성
        final_symbols = {
            Exchange.BITHUMB.value: pair_bithumb_binance | pair_bithumb_bybit,
            Exchange.UPBIT.value: pair_upbit_binance | pair_upbit_bybit,
            Exchange.BINANCE.value: pair_bithumb_binance | pair_upbit_binance,
            Exchange.BYBIT.value: pair_bithumb_bybit | pair_upbit_bybit,
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
        """
        Args:
            settings: 설정 데이터
        """
        self.settings = settings
        self.exchanges = [
            Exchange.BINANCE_FUTURE.value,
            Exchange.BYBIT_FUTURE.value,
            Exchange.BINANCE.value,
            Exchange.BYBIT.value,
            Exchange.UPBIT.value,
            Exchange.BITHUMB.value
        ]
        
        # 최소 거래량 설정 가져오기
        min_volume = get_value_from_settings("trading.settings.min_volume_krw")
        if min_volume is None:
            # min_volume_krw가 없으면 min_daily_volume_krw 설정을 대신 사용
            min_volume = get_value_from_settings("trading.settings.min_daily_volume_krw")
            if min_volume is not None:
                logger.info(f"min_daily_volume_krw 설정을 사용합니다: {min_volume:,} KRW")
            else:
                logger.error(f"필수 설정 누락: trading.settings.min_volume_krw 또는 trading.settings.min_daily_volume_krw")
                logger.error(f"exchange_settings.cfg 파일에서 최소 거래량 설정이 필요합니다.")
                import sys
                sys.exit(1)  # 에러 코드와 함께 프로그램 종료
            
        self.min_volume = min_volume
        
        # 심볼 캐시
        self._binance_symbols_cache: Optional[List[str]] = None
        self._bybit_symbols_cache: Optional[List[str]] = None

    async def run_filtering(self) -> Dict[str, List[str]]:
        """
        거래소별 심볼 필터링 실행
        
        Returns:
            Dict[str, List[str]]: 거래소별 필터링된 심볼 목록
        """
        try:
            logger.info(f"=== 심볼 필터링을 시작합니다 ===")
            logger.info(f"최소 거래량: {self.min_volume:,} KRW")
            
            # 1. 거래소별 필터링
            filtered_data: Dict[str, List[str]] = {}
            
            # Upbit
            upbit_symbols = await fetch_upbit_symbols_and_volume(self.min_volume)
            if upbit_symbols:
                filtered_data[Exchange.UPBIT.value] = upbit_symbols
            
            # Bithumb
            bithumb_symbols = await fetch_bithumb_symbols_and_volume(self.min_volume)
            if bithumb_symbols:
                filtered_data[Exchange.BITHUMB.value] = bithumb_symbols
            
            # Binance
            binance_symbols = await fetch_binance_symbols(self.min_volume)
            if binance_symbols:
                filtered_data[Exchange.BINANCE.value] = binance_symbols
                filtered_data[Exchange.BINANCE_FUTURE.value] = binance_symbols
            
            # Bybit
            bybit_symbols = await fetch_bybit_symbols(self.min_volume)
            if bybit_symbols:
                filtered_data[Exchange.BYBIT.value] = bybit_symbols
                filtered_data[Exchange.BYBIT_FUTURE.value] = bybit_symbols
            
            # 2. 거래소 간 페어링
            if len(filtered_data) >= 4:
                paired_data = await get_paired_symbols(
                    filtered_data.get(Exchange.UPBIT.value, []),
                    filtered_data.get(Exchange.BITHUMB.value, []),
                    filtered_data.get(Exchange.BINANCE.value, []),
                    filtered_data.get(Exchange.BYBIT.value, [])
                )
                filtered_data = {k: list(v) for k, v in paired_data.items()}
            
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
                
                removed = set(before_symbols) - set(filtered_data[exchange])
                if removed:
                    logger.info(
                        f"{EXCHANGE_NAMES_KR[exchange]} 제외된 심볼 목록: {sorted(list(removed))}"
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
            
            logger.info(f"=== 거래소별 최종 구독 심볼 ===")
            for exchange, symbols in filtered_data.items():
                name = EXCHANGE_NAMES_KR.get(exchange, exchange)
                logger.info(f"{name} 구독 심볼: {len(symbols)}개 → {sorted(symbols)}")
            
            return filtered_data

        except Exception as e:
            logger.error(f"심볼 필터링에 실패했습니다: {e}")
            return {}

