# file: utils/logger.py

import logging
import os
import time
from datetime import datetime
from crosskimp.ob_collector.config.constants import LOG_SYSTEM

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
            print(f"{LOG_SYSTEM} 디렉토리 생성: {dir_path}")
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
                    print(f"{LOG_SYSTEM} 오래된 로그 파일 삭제: {filepath}")
    except Exception as e:
        print(f"{LOG_SYSTEM} 로그 파일 정리 중 오류: {e}")

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
    
    # 일반 로그 핸들러
    log_file = f"{base_dir}/{current_time}_{name}.log"
    file_handler = logging.FileHandler(filename=log_file, encoding=LOG_ENCODING, mode=LOG_MODE)
    file_handler.setLevel(level)
    
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
    formatter = logging.Formatter(LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    raw_formatter = logging.Formatter(DEBUG_LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    
    file_handler.setFormatter(formatter)
    raw_handler.setFormatter(raw_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(raw_handler)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    logger.info(f"{LOG_SYSTEM} 통합 로거 초기화 완료")
    return logger

# ============================
# 큐 로거 생성
# ============================
def create_queue_logger(name: str, base_dir: str) -> logging.Logger:
    """큐 데이터 전용 로거 생성"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    if logger.hasHandlers():
        logger.handlers.clear()
    
    logger.propagate = False
    
    current_time = get_current_time_str()
    
    log_file = f"{base_dir}/{current_time}_{name}.log"
    handler = logging.FileHandler(filename=log_file, encoding=LOG_ENCODING, mode=LOG_MODE)
    handler.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(DEBUG_LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(console_handler)
    
    logger.debug(f"{LOG_SYSTEM} {name} 큐 로거 초기화 완료")
    return logger

# ============================
# 로거 초기화
# ============================
_loggers = {}  # 싱글톤 패턴을 위한 로거 캐시
_unified_logger = None  # 통합 로거 인스턴스

def get_unified_logger():
    """통합 로거 싱글톤 인스턴스 반환"""
    global _unified_logger
    if _unified_logger is None:
        ensure_log_directories()
        _unified_logger = create_unified_logger(
            name='unified_logger',
            base_dir=LOG_DIRS['base'],
            level=logging.INFO
        )
    return _unified_logger

def get_queue_logger():
    """큐 로거 싱글톤 인스턴스 반환"""
    if 'queue_logger' not in _loggers:
        ensure_log_directories()
        _loggers['queue_logger'] = create_queue_logger(
            name='queue_logger',
            base_dir=LOG_DIRS['base']
        )
    return _loggers['queue_logger']

# 거래소별 로거 매핑 - unified_logger로 통일
EXCHANGE_LOGGER_MAP = {
    "binance": get_unified_logger(),
    "binancefuture": get_unified_logger(),
    "bybit": get_unified_logger(),
    "bybitfuture": get_unified_logger(),
    "upbit": get_unified_logger(),
    "bithumb": get_unified_logger()
}

__all__ = [
    'get_unified_logger',
    'get_queue_logger',
    'EXCHANGE_LOGGER_MAP'
]

# 모듈 초기화 시 한 번만 로깅
if not _unified_logger:
    print(f"{LOG_SYSTEM} logger.py 모듈 초기화 완료")