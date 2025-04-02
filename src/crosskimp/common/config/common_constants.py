"""
크로스 김프 아비트리지 - 공통 상수 모듈

이 모듈은 시스템 전체에서 공통으로 사용되는 상수값들을 정의합니다.
모든 상수는 이 파일에서 중앙 관리됩니다.
"""

from enum import Enum, auto
from typing import Dict, List, Any, Optional, Union

#############################################################
#                시스템 관련 상수 정의                       #
#############################################################

class SystemComponent(Enum):
    """
    시스템 컴포넌트 식별자
    
    각 컴포넌트는 시스템의 특정 부분을 담당합니다.
    로깅, 설정, 이벤트 등에서 컴포넌트 구분에 사용됩니다.
    """
    OB_COLLECTOR = "ob_collector"    # 오더북 수집기
    RADAR = "radar"            # 레이더 (시장 감시기)
    TRADER = "trader"          # 트레이더 (자동 거래 시스템)
    TELEGRAM = "telegram"      # 텔레그램 서비스
    SYSTEM = "system"          # 시스템 (통합 프로그램)
    WEB_SERVER = "web_server"  # 웹 서버

# 컴포넌트 이름 한글 매핑
COMPONENT_NAMES_KR = {
    SystemComponent.OB_COLLECTOR.value: "[오더북수집기]",
    SystemComponent.RADAR.value: "[레이더]",
    SystemComponent.TRADER.value: "[트레이더]",
    SystemComponent.TELEGRAM.value: "[텔레그램]",
    SystemComponent.SYSTEM.value: "[시스템]",
    SystemComponent.WEB_SERVER.value: "[웹서버]",
}

# 이전 버전과의 호환성을 위한 별칭
ORDERBOOK = SystemComponent.OB_COLLECTOR.value

#############################################################
#                프로세스 관련 상수 정의                      #
#############################################################

# 프로세스 관련 상수는 system_types.py로 이동되었습니다.
# 아래 import를 사용하여 접근하세요:
# from crosskimp.common.events.system_types import ProcessStatus, ProcessEvent, ProcessEventData

#############################################################
#                HTTP 상태 코드 정의                         #
#############################################################

class StatusCode(Enum):
    """
    시스템 상태 코드
    
    HTTP 상태 코드와 유사한 시스템 응답 코드를 정의합니다.
    """
    # 성공 응답 (2xx)
    OK = 200                  # 요청 성공
    CREATED = 201             # 생성됨
    ACCEPTED = 202            # 허용됨
    NO_CONTENT = 204          # 내용 없음
    
    # 클라이언트 오류 응답 (4xx)
    BAD_REQUEST = 400         # 잘못된 요청
    UNAUTHORIZED = 401        # 인증 필요
    FORBIDDEN = 403           # 접근 금지
    NOT_FOUND = 404           # 찾을 수 없음
    TIMEOUT = 408             # 시간 초과
    CONFLICT = 409            # 충돌
    
    # 서버 오류 응답 (5xx)
    INTERNAL_ERROR = 500      # 내부 서버 오류
    SERVICE_UNAVAILABLE = 503 # 서비스 이용 불가

#############################################################
#                거래소 관련 상수 정의                       #
#############################################################

class Exchange(Enum):
    """
    거래소 식별자
    
    지원되는 모든 거래소의 식별자를 정의합니다.
    코드 전체에서 일관된 거래소 식별을 위해 사용됩니다.
    """
    # 현물 거래소
    BINANCE_SPOT = "binance_spot"            # 바이낸스 현물
    BYBIT_SPOT = "bybit_spot"                # 바이빗 현물
    UPBIT = "upbit"                # 업비트 현물
    BITHUMB = "bithumb"            # 빗썸 현물
    
    # 선물 거래소
    BINANCE_FUTURE = "binance_future"  # 바이낸스 선물
    BYBIT_FUTURE = "bybit_future"      # 바이빗 선물

# 거래소 그룹화
EXCHANGE_GROUPS = {
    "korean": [Exchange.UPBIT.value, Exchange.BITHUMB.value],
    "global": [Exchange.BINANCE_SPOT.value, Exchange.BYBIT_SPOT.value],
    "spot": [Exchange.BINANCE_SPOT.value, Exchange.BYBIT_SPOT.value, Exchange.UPBIT.value, Exchange.BITHUMB.value],
    "futures": [Exchange.BINANCE_FUTURE.value, Exchange.BYBIT_FUTURE.value]
}

# 거래소 한글 이름
EXCHANGE_NAMES_KR = {
    Exchange.BINANCE_SPOT.value: "[바이낸스 현물]",
    Exchange.BYBIT_SPOT.value: "[바이빗 현물]",
    Exchange.UPBIT.value: "[업비트]",
    Exchange.BITHUMB.value: "[빗썸]",
    Exchange.BINANCE_FUTURE.value: "[바이낸스 선물]",
    Exchange.BYBIT_FUTURE.value: "[바이빗 선물]",
}

#############################################################
#                거래소 관련 유틸리티 함수                   #
#############################################################

def normalize_exchange_code(code: str) -> str:
    """
    거래소 코드를 정규화하여 반환 (소문자로 변환)
    
    Args:
        code: 정규화할 거래소 코드
        
    Returns:
        정규화된 거래소 코드 (소문자)
    """
    return code.lower() if code else ""

def get_exchange_enum(code: str) -> Exchange:
    """
    문자열 코드에서 Exchange 열거형 값 반환
    
    Args:
        code: 거래소 코드 문자열
        
    Returns:
        Exchange 열거형 값
        
    Raises:
        ValueError: 알 수 없는 거래소 코드인 경우
    """
    code = normalize_exchange_code(code)
    for exchange in Exchange:
        if exchange.value == code:
            return exchange
    raise ValueError(f"알 수 없는 거래소 코드: {code}")

def is_korean_exchange(exchange_code: str) -> bool:
    """
    거래소가 한국 거래소인지 확인
    
    Args:
        exchange_code: 확인할 거래소 코드
        
    Returns:
        한국 거래소 여부
    """
    return normalize_exchange_code(exchange_code) in EXCHANGE_GROUPS["korean"]

def is_global_exchange(exchange_code: str) -> bool:
    """
    거래소가 글로벌 거래소인지 확인
    
    Args:
        exchange_code: 확인할 거래소 코드
        
    Returns:
        글로벌 거래소 여부
    """
    return normalize_exchange_code(exchange_code) in EXCHANGE_GROUPS["global"]

def is_futures_exchange(exchange_code: str) -> bool:
    """
    거래소가 선물 거래소인지 확인
    
    Args:
        exchange_code: 확인할 거래소 코드
        
    Returns:
        선물 거래소 여부
    """
    return normalize_exchange_code(exchange_code) in EXCHANGE_GROUPS["futures"] 