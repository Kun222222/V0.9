# file: utils/logger.py

import logging
import os
import time
from datetime import datetime

# ============================
# 상수 정의
# ============================
LOG_FORMAT = '[%(asctime)s.%(msecs)03d] - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s'
DEBUG_LOG_FORMAT = '[%(asctime)s.%(msecs)03d] - [%(filename)s:%(lineno)d] - %(message)s'
LOG_ENCODING = 'utf-8'
LOG_MODE = 'a'  # append 모드

# 콘솔/파일 로그 레벨 설정
DEFAULT_CONSOLE_LEVEL = logging.ERROR
DEFAULT_FILE_LEVEL = logging.DEBUG

# 로그 디렉토리 설정
BASE_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))), "logs")
LOG_DIRS = {
    'base': BASE_LOG_DIR,  # 일반 로그가 저장됨
    'raw': os.path.join(BASE_LOG_DIR, 'raw')  # raw 데이터 로그가 저장됨
}

# 로거 이름 설정
LOGGER_NAMES = {
    'unified': 'unified_logger',
    'bybit': 'bybit_logger',
    'upbit': 'upbit_logger',
    'bybit_future': 'bybitfuture_logger',
    'bithumb': 'bithumb_logger',
    'binance': 'binance_logger',
    'binancefuture': 'binancefuture_logger',
    'market_price': 'market_price_logger'
}

# ============================
# 유틸리티 함수
# ============================
def get_current_time_str() -> str:
    """현재 시간을 yymmdd_hhmmss 형식으로 반환"""
    return datetime.now().strftime("%y%m%d_%H%M%S")

def ensure_log_directories():
    """로그 디렉토리 존재 여부 확인 및 생성, 오래된 로그 파일 정리"""
    for dir_name, dir_path in LOG_DIRS.items():
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            print(f"[Logger] 디렉토리 생성: {dir_path}")
        cleanup_old_logs(dir_path)

def cleanup_old_logs(directory: str, max_days: int = 7):
    """max_days일보다 오래된 로그 파일 삭제"""
    try:
        current_time = time.time()
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                file_time = os.path.getmtime(filepath)
                if current_time - file_time > max_days * 86400:  # 86400초 = 1일
                    os.remove(filepath)
                    print(f"[Logger] 오래된 로그 파일 삭제: {filepath}")
    except Exception as e:
        print(f"[Logger] 로그 파일 정리 중 오류: {e}")

# ============================
# 필터 클래스 정의
# ============================
class InfoFilter(logging.Filter):
    def filter(self, record):
        # DEBUG 레벨 메시지는 제외하고 INFO 이상만 허용
        return record.levelno >= logging.INFO

class DebugFilter(logging.Filter):
    def filter(self, record):
        # 오직 DEBUG 레벨 메시지만 허용
        return record.levelno == logging.DEBUG

# ============================
# 로거 생성 함수
# ============================
def create_logger(
    name: str,
    exchange_dir: str,
    level: int = logging.DEBUG,
    console_level: int = DEFAULT_CONSOLE_LEVEL
) -> logging.Logger:
    """
    거래소별 로거 생성 및 설정
    - 각 거래소별로 일반 로그와 raw 로그 파일 분리
    - raw 데이터는 logs/raw 디렉토리에 저장
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # 기존 핸들러 제거
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # 부모 로거로 로그가 전파되지 않도록 설정
    logger.propagate = False
    
    current_time = get_current_time_str()
    
    # 일반 로그 핸들러
    main_log_file = f"{LOG_DIRS['base']}/{current_time}_{name}.log"
    main_handler = logging.FileHandler(filename=main_log_file, encoding=LOG_ENCODING, mode=LOG_MODE)
    main_handler.setLevel(logging.DEBUG)
    main_handler.addFilter(lambda record: (
        'raw:' not in record.getMessage() and
        'raw delta:' not in record.getMessage() and
        'depthUpdate raw:' not in record.getMessage() and
        'depth raw:' not in record.getMessage()
    ))
    
    # raw 로그 핸들러 (raw 디렉토리에 저장)
    raw_log_file = f"{LOG_DIRS['raw']}/{current_time}_{name}_raw.log"
    raw_handler = logging.FileHandler(filename=raw_log_file, encoding=LOG_ENCODING, mode=LOG_MODE)
    raw_handler.setLevel(logging.DEBUG)
    raw_handler.addFilter(lambda record: (
        'raw:' in record.getMessage() or
        'raw delta:' in record.getMessage() or
        'depthUpdate raw:' in record.getMessage() or
        'depth raw:' in record.getMessage()
    ))
    
    # 포맷터 설정
    main_formatter = logging.Formatter(LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    raw_formatter = logging.Formatter(DEBUG_LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    
    main_handler.setFormatter(main_formatter)
    raw_handler.setFormatter(raw_formatter)
    
    logger.addHandler(main_handler)
    logger.addHandler(raw_handler)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(console_handler)
    
    logger.info(f"[Logger] {name} 로거 초기화 완료 (일반/RAW 로그 파일 분리)")
    return logger

def create_queue_logger(name: str, exchange_dir: str) -> logging.Logger:
    """
    큐 데이터 전용 로거 생성
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # 기존 핸들러 제거
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # 부모 로거로 로그가 전파되지 않도록 설정
    logger.propagate = False
    
    current_time = get_current_time_str()
    
    # 로그 파일 생성 (_debug 접미사 제거)
    log_file = f"{exchange_dir}/{current_time}_{name}.log"
    handler = logging.FileHandler(filename=log_file, encoding=LOG_ENCODING, mode=LOG_MODE)
    handler.setLevel(logging.DEBUG)
    
    # 포맷터 설정
    formatter = logging.Formatter(DEBUG_LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # 콘솔 핸들러 (에러만 출력)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(console_handler)
    
    logger.debug(f"[Logger] {name} 큐 로거 초기화 완료")
    return logger

def create_unified_logger(name: str, base_dir: str, level: int = logging.INFO) -> logging.Logger:
    """
    통합 로거 생성 - 단일 파일로 모든 로그 저장
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 기존 핸들러 제거
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # 부모 로거로 로그가 전파되지 않도록 설정
    logger.propagate = False
    
    current_time = get_current_time_str()
    
    # 단일 파일 핸들러
    log_file = f"{base_dir}/{current_time}_{name}.log"
    file_handler = logging.FileHandler(filename=log_file, encoding=LOG_ENCODING, mode=LOG_MODE)
    file_handler.setLevel(level)
    
    # 포맷터 설정
    formatter = logging.Formatter(LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    logger.info(f"[Logger] {name} 통합 로거 초기화 완료")
    return logger

# ============================
# 로거 초기화
# ============================
current_time = get_current_time_str()
ensure_log_directories()

# 거래소별 로거 생성
binance_logger = create_logger(
    name=LOGGER_NAMES['binance'],
    exchange_dir=LOG_DIRS['base'],
    level=logging.DEBUG,
    console_level=logging.ERROR
)

binance_future_logger = create_logger(
    name=LOGGER_NAMES['binancefuture'],
    exchange_dir=LOG_DIRS['base'],
    level=logging.DEBUG,
    console_level=logging.ERROR
)

bybit_logger = create_logger(
    name=LOGGER_NAMES['bybit'],
    exchange_dir=LOG_DIRS['base'],
    level=logging.DEBUG,
    console_level=logging.ERROR
)

upbit_logger = create_logger(
    name=LOGGER_NAMES['upbit'],
    exchange_dir=LOG_DIRS['base'],
    level=logging.DEBUG,
    console_level=logging.ERROR
)

bybit_future_logger = create_logger(
    name=LOGGER_NAMES['bybit_future'],
    exchange_dir=LOG_DIRS['base'],
    level=logging.DEBUG,
    console_level=logging.ERROR
)

bithumb_logger = create_logger(
    name=LOGGER_NAMES['bithumb'],
    exchange_dir=LOG_DIRS['base'],
    level=logging.DEBUG,
    console_level=logging.ERROR
)

# 통합 로거는 단일 파일로 생성
unified_logger = create_unified_logger(
    name=LOGGER_NAMES['unified'],
    base_dir=LOG_DIRS['base'],
    level=logging.INFO
)

# 시장가격 로거 생성
market_price_logger = create_unified_logger(
    name=LOGGER_NAMES['market_price'],
    base_dir=LOG_DIRS['base'],
    level=logging.INFO
)

# 큐 데이터 전용 로거 생성
queue_logger = create_queue_logger(
    name='queue_logger',
    exchange_dir=LOG_DIRS['base']
)

# 거래소별 로거 매핑 추가
EXCHANGE_LOGGER_MAP = {
    "binance": binance_logger,
    "binancefuture": binance_future_logger,
    "bybit": bybit_logger,
    "bybitfuture": bybit_future_logger,
    "upbit": upbit_logger,
    "bithumb": bithumb_logger
}

__all__ = [
    'unified_logger',
    'queue_logger',
    'binance_logger',
    'bybit_logger',
    'upbit_logger',
    'bybit_future_logger',
    'bithumb_logger',
    'binance_future_logger',
    'market_price_logger',
    'EXCHANGE_LOGGER_MAP'
]

print("[Logger] logger.py 모듈 초기화 완료")