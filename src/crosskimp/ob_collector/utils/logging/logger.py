# file: utils/logger.py

import logging
import os
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler

from crosskimp.ob_collector.utils.config.constants import LOG_SYSTEM

# ============================
# 상수 정의
# ============================
LOG_FORMAT = '[%(asctime)s.%(msecs)03d] [%(filename)-20s:%(lineno)3d] %(levelname)-7s %(message)s'
DEBUG_LOG_FORMAT = '[%(asctime)s.%(msecs)03d] [%(filename)s:%(lineno)d] %(message)s'
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
    - 파일 크기가 100MB를 초과하면 자동으로 새 파일 생성
    - 최대 100개의 백업 파일 유지
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 기존 핸들러 제거
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # 부모 로거로 로그가 전파되지 않도록 설정
    logger.propagate = False
    
    current_time = get_current_time_str()
    
    # 일반 로그 핸들러 (RotatingFileHandler 사용)
    log_file = f"{base_dir}/{current_time}_{name}.log"
    file_handler = SafeRotatingFileHandler(
        filename=log_file,
        maxBytes=100*1024*1024,  # 100MB
        backupCount=100,         # 최대 100개 파일
        encoding=LOG_ENCODING,
        mode=LOG_MODE
    )
    file_handler.setLevel(level)
    
    # raw 로그 핸들러 (raw 디렉토리에 저장)
    raw_log_file = f"{LOG_DIRS['raw']}/{current_time}_{name}_raw.log"
    raw_handler = SafeRotatingFileHandler(
        filename=raw_log_file,
        maxBytes=100*1024*1024,  # 100MB
        backupCount=100,         # 최대 100개 파일
        encoding=LOG_ENCODING,
        mode=LOG_MODE
    )
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
    """
    큐 데이터 전용 로거 생성
    - 파일 크기가 100MB를 초과하면 자동으로 새 파일 생성
    - 최대 100개의 백업 파일 유지
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    if logger.hasHandlers():
        logger.handlers.clear()
    
    logger.propagate = False
    
    current_time = get_current_time_str()
    
    log_file = f"{base_dir}/{current_time}_{name}.log"
    handler = SafeRotatingFileHandler(
        filename=log_file,
        maxBytes=100*1024*1024,  # 100MB
        backupCount=100,         # 최대 100개 파일
        encoding=LOG_ENCODING,
        mode=LOG_MODE
    )
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

class SafeRotatingFileHandler(RotatingFileHandler):
    """
    안전한 로그 파일 로테이션 핸들러
    - 파일 크기 제한 및 백업 파일 관리
    - 주기적인 핸들러 리프레시로 파일 디스크립터 누수 방지
    - 에러 발생 시 자동 복구 시도
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_refresh = time.time()
        self.refresh_interval = 300  # 5분마다 리프레시
        import threading
        self.lock = threading.RLock()  # 스레드 안전한 재진입 락 사용

    def _open(self):
        """파일 열기"""
        try:
            return super()._open()
        except Exception as e:
            print(f"{LOG_SYSTEM} 로그 파일 열기 실패: {e}")
            # 파일 열기 실패 시 임시 파일로 대체
            import tempfile
            return tempfile.TemporaryFile(mode='a+', encoding=LOG_ENCODING)

    def close(self):
        """파일 닫기"""
        try:
            super().close()
        except Exception as e:
            print(f"{LOG_SYSTEM} 로그 파일 닫기 실패: {e}")

    def doRollover(self):
        """로그 파일 로테이션 수행"""
        try:
            with self.lock:  # 락 사용
                super().doRollover()
        except Exception as e:
            print(f"{LOG_SYSTEM} 로그 로테이션 중 오류 발생: {e}")
            # 에러 발생 시 새로운 파일 생성 시도
            try:
                self.close()
                self.stream = self._open()
            except Exception as inner_e:
                print(f"{LOG_SYSTEM} 로그 스트림 재생성 실패: {inner_e}")

    def emit(self, record):
        """로그 레코드 출력"""
        try:
            # 주기적으로 핸들러 리프레시
            current_time = time.time()
            if current_time - self.last_refresh > self.refresh_interval:
                with self.lock:  # 락 사용
                    self.close()
                    self.stream = self._open()
                    self.last_refresh = current_time
            
            # 파일 크기 체크 및 로테이션
            if self.shouldRollover(record):
                self.doRollover()
            
            with self.lock:  # 락 사용
                super().emit(record)
                self.flush()  # 즉시 디스크에 기록
        except Exception as e:
            self.handleError(record)
            print(f"{LOG_SYSTEM} 로그 기록 중 오류 발생: {e}")
            # 에러 발생시 핸들러 리프레시 시도
            try:
                with self.lock:  # 락 사용
                    self.close()
                    self.stream = self._open()
            except Exception as inner_e:
                print(f"{LOG_SYSTEM} 로그 스트림 재생성 실패: {inner_e}")

    def acquire(self):
        """락 획득 - 오버라이드"""
        self.lock.acquire()
        
    def release(self):
        """락 해제 - 오버라이드"""
        try:
            self.lock.release()
        except RuntimeError:
            # 이미 해제된 락에 대한 예외 무시
            pass

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
    try:
        if 'queue_logger' not in _loggers or not _loggers['queue_logger'].handlers:
            ensure_log_directories()
            _loggers['queue_logger'] = create_queue_logger(
                name='queue_logger',
                base_dir=LOG_DIRS['base']
            )
        
        # 핸들러 상태 확인 및 복구
        for handler in _loggers['queue_logger'].handlers[:]:
            if isinstance(handler, SafeRotatingFileHandler):
                try:
                    # 핸들러 상태 테스트
                    if handler.stream is None or handler.stream.closed:
                        _loggers['queue_logger'].removeHandler(handler)
                        new_handler = SafeRotatingFileHandler(
                            filename=handler.baseFilename,
                            maxBytes=handler.maxBytes,
                            backupCount=handler.backupCount,
                            encoding=handler.encoding,
                            mode=handler.mode
                        )
                        new_handler.setFormatter(handler.formatter)
                        new_handler.setLevel(handler.level)
                        _loggers['queue_logger'].addHandler(new_handler)
                except Exception as e:
                    print(f"{LOG_SYSTEM} 큐 로거 핸들러 복구 실패: {e}")
                    # 완전히 새로운 로거 생성
                    _loggers['queue_logger'] = create_queue_logger(
                        name='queue_logger',
                        base_dir=LOG_DIRS['base']
                    )
                    break
        
        return _loggers['queue_logger']
    except Exception as e:
        print(f"{LOG_SYSTEM} 큐 로거 가져오기 실패: {e}")
        # 비상용 로거 반환
        fallback_logger = logging.getLogger('fallback_queue_logger')
        if not fallback_logger.handlers:
            fallback_logger.setLevel(logging.INFO)
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
            fallback_logger.addHandler(console_handler)
        return fallback_logger

# 거래소별 로거 매핑 - unified_logger로 통일
EXCHANGE_LOGGER_MAP = {
    "binance": get_unified_logger(),
    "binancefuture": get_unified_logger(),
    "bybit": get_unified_logger(),
    "bybitfuture": get_unified_logger(),
    "upbit": get_unified_logger(),
    "bithumb": get_unified_logger()
}

def get_raw_logger(exchange_name: str) -> logging.Logger:
    """
    거래소별 raw 데이터 로깅을 위한 로거를 반환
    
    Args:
        exchange_name (str): 거래소 이름 (예: upbit, binance, bybit 등)
        
    Returns:
        logging.Logger: raw 데이터 전용 로거
    """
    logger = logging.getLogger(f"raw_{exchange_name}")
    
    if not logger.handlers:  # 핸들러가 없을 때만 추가
        logger.setLevel(logging.INFO)
        
        # 파일명 설정 (예: raw_upbit_20240307_153000.log)
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(LOG_DIRS['raw'], f"raw_{exchange_name}_{current_time}.log")
        
        # 파일 핸들러 설정 (100MB 단위로 로테이션, 최대 100개 파일 유지)
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=100*1024*1024,  # 100MB
            backupCount=100,         # 최대 100개 파일
            encoding='utf-8'
        )
        
        # 포맷터 설정 (timestamp와 raw 데이터만 기록)
        formatter = logging.Formatter('%(asctime)s|%(message)s')
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.propagate = False  # 상위 로거로 전파하지 않음
        
    return logger

__all__ = [
    'get_unified_logger',
    'get_queue_logger',
    'EXCHANGE_LOGGER_MAP'
]

# 모듈 초기화 시 한 번만 로깅
if not _unified_logger:
    print(f"{LOG_SYSTEM} logger.py 모듈 초기화 완료")