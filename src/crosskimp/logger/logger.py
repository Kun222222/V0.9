# file: utils/logger.py

"""
로깅 시스템 모듈

이 모듈은 애플리케이션 전체에서 사용되는 로깅 시스템을 제공합니다.
주요 기능:
1. 통합 로거 (일반 로그)
2. Raw 로거 (거래소별 원시 데이터)
3. 로그 파일 관리 (로테이션, 정리)
4. 로그 레벨 관리
5. 예외 처리 및 복구
"""

import logging
import os
import time
import threading
import re
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Dict, Optional, List, Union, Any

# paths.py 대신 constants_v3.py에서 모든 경로 관련 상수와 함수 임포트
from crosskimp.config.constants_v3 import (
    LOG_SYSTEM, LOG_FORMAT, DEBUG_LOG_FORMAT, LOG_ENCODING, LOG_MODE,
    DEFAULT_CONSOLE_LEVEL, DEFAULT_FILE_LEVEL, LOG_MAX_BYTES, LOG_BACKUP_COUNT,
    LOG_CLEANUP_DAYS, PROJECT_ROOT, LOGS_DIR, LOG_SUBDIRS
)

# ============================
# 로깅 상태 및 캐시
# ============================
_loggers = {}  # 싱글톤 패턴을 위한 로거 캐시
_unified_logger = None  # 통합 로거 인스턴스
_initialized = False  # 로깅 시스템 초기화 여부
_lock = threading.RLock()  # 스레드 안전한 재진입 락
_internal_logger = None  # 내부 로깅용 로거

# ============================
# 유틸리티 함수
# ============================
def get_current_time_str() -> str:
    """현재 시간을 yymmdd_hhmmss 형식으로 반환"""
    return datetime.now().strftime("%y%m%d_%H%M%S")

def _get_internal_logger():
    """내부 로깅용 로거 반환 (콘솔 출력만 사용)"""
    global _internal_logger
    if _internal_logger is None:
        _internal_logger = logging.getLogger('internal_logger')
        _internal_logger.setLevel(logging.INFO)
        
        # 기존 핸들러 제거
        if _internal_logger.hasHandlers():
            _internal_logger.handlers.clear()
        
        # 부모 로거로 로그가 전파되지 않도록 설정
        _internal_logger.propagate = False
        
        # 콘솔 핸들러 추가
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(LOG_FORMAT)
        console_handler.setFormatter(formatter)
        _internal_logger.addHandler(console_handler)
    
    return _internal_logger

def cleanup_old_logs(max_days: int = LOG_CLEANUP_DAYS) -> None:
    """오래된 로그 파일 정리"""
    internal_logger = _get_internal_logger()
    
    try:
        current_time = time.time()
        
        # 삭제 대상 파일 목록 미리 수집
        delete_files = []
        
        # 메인 로그 디렉토리 정리
        if os.path.exists(LOGS_DIR):
            for filename in os.listdir(LOGS_DIR):
                filepath = os.path.join(LOGS_DIR, filename)
                
                # 디렉토리는 건너뜀
                if not os.path.isfile(filepath) or filepath.endswith('.gz'):
                    continue
                    
                # 파일 수정 시간 확인
                file_time = os.path.getmtime(filepath)
                file_age_days = (current_time - file_time) / 86400  # 일 단위로 변환
                
                # 삭제 대상: max_days일 이상 된 파일
                if file_age_days >= max_days:
                    delete_files.append(filepath)
        
        # 모든 로그 하위 디렉토리 순회
        for dir_name, dir_path in LOG_SUBDIRS.items():
            if not os.path.exists(dir_path):
                continue
                
            # 디렉토리 내 모든 파일 확인
            for filename in os.listdir(dir_path):
                filepath = os.path.join(dir_path, filename)
                
                # 디렉토리는 건너뜀
                if not os.path.isfile(filepath) or filepath.endswith('.gz'):
                    continue
                    
                # 파일 수정 시간 확인
                file_time = os.path.getmtime(filepath)
                file_age_days = (current_time - file_time) / 86400  # 일 단위로 변환
                
                # 삭제 대상: max_days일 이상 된 파일
                if file_age_days >= max_days:
                    delete_files.append(filepath)
        
        # 삭제 작업 수행
        for filepath in delete_files:
            try:
                os.remove(filepath)
                internal_logger.info(f"{LOG_SYSTEM} 오래된 로그 파일 삭제: {filepath}")
            except Exception as e:
                internal_logger.error(f"{LOG_SYSTEM} 로그 파일 삭제 실패: {filepath} | {str(e)}")
                
    except Exception as e:
        internal_logger.error(f"{LOG_SYSTEM} 로그 파일 정리 중 오류: {str(e)}")

# ============================
# 필터 클래스 정의
# ============================
class LevelFilter(logging.Filter):
    """특정 레벨 범위의 로그만 허용하는 필터"""
    def __init__(self, min_level: int, max_level: Optional[int] = None):
        super().__init__()
        self.min_level = min_level
        self.max_level = max_level if max_level is not None else logging.CRITICAL
        
    def filter(self, record):
        return self.min_level <= record.levelno <= self.max_level

class ErrorFilter(logging.Filter):
    """에러 레벨 이상의 로그만 허용하는 필터"""
    def filter(self, record):
        return record.levelno >= logging.ERROR

class RawDataFilter(logging.Filter):
    """Raw 데이터 로그만 허용하는 필터 (정규식 사용)"""
    def __init__(self):
        super().__init__()
        self.pattern = re.compile(r'(raw:|raw delta:|depthUpdate raw:|depth raw:)')
        
    def filter(self, record):
        message = record.getMessage()
        return bool(self.pattern.search(message))

# ============================
# 안전한 로그 핸들러
# ============================
class SafeRotatingFileHandler(RotatingFileHandler):
    """
    안전한 로그 파일 로테이션 핸들러
    - 파일 크기 제한 및 백업 파일 관리
    - 주기적인 핸들러 리프레시로 파일 디스크립터 누수 방지
    - 에러 발생 시 자동 복구 시도
    - 스레드 안전한 락 사용
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_refresh = time.time()
        self.refresh_interval = 300  # 5분마다 리프레시
        self.lock = threading.RLock()  # 스레드 안전한 재진입 락 사용
        self.error_count = 0  # 연속 에러 카운트
        self.max_errors = 5  # 최대 연속 에러 허용 횟수
        self.last_time_check = 0  # 마지막 시간 체크 타임스탬프
        self.time_check_interval = 5  # 시간 체크 간격 (초)
        self.internal_logger = _get_internal_logger()

    def _open(self):
        """파일 열기 (에러 처리 포함)"""
        try:
            return super()._open()
        except Exception as e:
            self.error_count += 1
            self.internal_logger.error(f"{LOG_SYSTEM} 로그 파일 열기 실패 ({self.error_count}/{self.max_errors}): {e}")
            
            # 임시 파일로 대체
            import tempfile
            return tempfile.TemporaryFile(mode='a+', encoding=LOG_ENCODING)

    def close(self):
        """파일 닫기 (에러 처리 포함)"""
        try:
            if self.stream:
                super().close()
        except Exception as e:
            self.internal_logger.error(f"{LOG_SYSTEM} 로그 파일 닫기 실패: {e}")

    def doRollover(self):
        """로그 파일 로테이션 수행 (에러 처리 포함)"""
        try:
            with self.lock:  # 락 사용
                super().doRollover()
                self.error_count = 0  # 성공 시 에러 카운트 초기화
        except Exception as e:
            self.error_count += 1
            self.internal_logger.error(f"{LOG_SYSTEM} 로그 로테이션 중 오류 발생 ({self.error_count}/{self.max_errors}): {e}")
            
            # 에러 발생 시 새로운 파일 생성 시도
            if self.error_count < self.max_errors:
                try:
                    self.close()
                    self.stream = self._open()
                except Exception as inner_e:
                    self.internal_logger.error(f"{LOG_SYSTEM} 로그 스트림 재생성 실패: {inner_e}")

    def emit(self, record):
        """로그 레코드 출력 (에러 처리 및 자동 복구 포함, 시간 체크 최적화)"""
        if self.error_count >= self.max_errors:
            # 너무 많은 에러 발생 시 콘솔로만 출력
            self.internal_logger.error(f"{LOG_SYSTEM} 로그 핸들러 비활성화됨 (너무 많은 에러): {record.getMessage()}")
            return
            
        try:
            # 주기적으로만 시간 체크 (매 로그마다 체크하지 않음)
            current_time = time.time()
            if current_time - self.last_time_check > self.time_check_interval:
                self.last_time_check = current_time
                
                # 리프레시 간격 체크
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
                # 모든 로그마다 flush하는 것은 비효율적이므로 제거
                self.error_count = 0  # 성공 시 에러 카운트 초기화
        except Exception as e:
            self.error_count += 1
            self.internal_logger.error(f"{LOG_SYSTEM} 로그 출력 중 오류 발생 ({self.error_count}/{self.max_errors}): {e}")
            
            # 에러 발생 시 복구 시도
            if self.error_count < self.max_errors:
                try:
                    with self.lock:
                        self.close()
                        self.stream = self._open()
                        super().emit(record)
                except Exception as inner_e:
                    self.internal_logger.error(f"{LOG_SYSTEM} 로그 출력 복구 실패: {inner_e}")

    def acquire(self):
        """락 획득"""
        self.lock.acquire()

    def release(self):
        """락 해제"""
        self.lock.release()

# ============================
# 로거 생성 함수
# ============================
def create_logger(
    name: str,
    log_dir: str,
    level: int = DEFAULT_FILE_LEVEL,
    console_level: int = DEFAULT_CONSOLE_LEVEL,
    format_str: str = LOG_FORMAT,
    add_console: bool = False,
    add_error_file: bool = True
) -> logging.Logger:
    """
    로거 생성 함수
    
    Args:
        name: 로거 이름
        log_dir: 로그 파일 저장 디렉토리
        level: 파일 로그 레벨
        console_level: 콘솔 로그 레벨
        format_str: 로그 포맷 문자열
        add_console: 콘솔 핸들러 추가 여부 (기본값: False)
        add_error_file: 에러 전용 로그 파일 추가 여부
        
    Returns:
        logging.Logger: 생성된 로거 인스턴스
    """
    internal_logger = _get_internal_logger()
    
    logger = logging.getLogger(name)
    logger.setLevel(min(level, console_level if add_console else level))  # 가장 낮은 레벨로 설정
    
    # 기존 핸들러 제거
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # 부모 로거로 로그가 전파되지 않도록 설정
    logger.propagate = False
    
    current_time = get_current_time_str()
    
    # 로그 디렉토리 존재 확인
    os.makedirs(log_dir, exist_ok=True)
    
    # 일반 로그 핸들러
    log_file = f"{log_dir}/{current_time}_{name}.log"
    try:
        file_handler = SafeRotatingFileHandler(
            filename=log_file,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding=LOG_ENCODING,
            mode=LOG_MODE
        )
        # 파일 권한 설정
        os.chmod(log_file, 0o644)
        
        file_handler.setLevel(level)
        formatter = logging.Formatter(format_str)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        internal_logger.info(f"{LOG_SYSTEM} 로그 파일 생성: {log_file}")
    except Exception as e:
        internal_logger.error(f"{LOG_SYSTEM} 로그 파일 생성 실패: {log_file} | {str(e)}")
    
    # 에러 전용 로그 파일 추가
    if add_error_file:
        error_log_file = f"{log_dir}/{current_time}_{name}_error.log"
        try:
            error_handler = SafeRotatingFileHandler(
                filename=error_log_file,
                maxBytes=LOG_MAX_BYTES,
                backupCount=LOG_BACKUP_COUNT,
                encoding=LOG_ENCODING,
                mode=LOG_MODE
            )
            # 파일 권한 설정
            os.chmod(error_log_file, 0o644)
            
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(formatter)
            error_handler.addFilter(ErrorFilter())
            logger.addHandler(error_handler)
            internal_logger.info(f"{LOG_SYSTEM} 에러 로그 파일 생성: {error_log_file}")
        except Exception as e:
            internal_logger.error(f"{LOG_SYSTEM} 에러 로그 파일 생성 실패: {error_log_file} | {str(e)}")
    
    # 콘솔 핸들러 추가
    if add_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger

def create_raw_logger(exchange_name: str) -> logging.Logger:
    """
    거래소별 raw 데이터 로거 생성
    
    Args:
        exchange_name: 거래소 이름
        
    Returns:
        logging.Logger: 생성된 로거 인스턴스
    """
    logger_name = f"{exchange_name}_raw_logger"
    
    # 한글 거래소 이름 사용 
    from crosskimp.config.constants_v3 import EXCHANGE_NAMES_KR
    exchange_name_kr = EXCHANGE_NAMES_KR.get(exchange_name, f"[{exchange_name}]")
    
    # 하위 폴더 생성하지 않고 raw_data 디렉토리에 직접 로깅
    raw_data_dir = LOG_SUBDIRS['raw_data']
    os.makedirs(raw_data_dir, exist_ok=True)
    
    logger = create_logger(
        name=logger_name,
        log_dir=raw_data_dir,
        level=logging.DEBUG,
        console_level=logging.ERROR,
        format_str=DEBUG_LOG_FORMAT,
        add_console=False,
        add_error_file=False
    )
    
    logger.debug(f"{exchange_name_kr} raw 로거 초기화 완료")
    return logger

# ============================
# 로거 인스턴스 관리 함수
# ============================
def initialize_logging() -> None:
    """로깅 시스템 초기화"""
    global _initialized
    internal_logger = _get_internal_logger()
    
    with _lock:
        if _initialized:
            return
            
        try:
            # 로그 디렉토리 생성 - error 폴더 제외
            os.makedirs(LOGS_DIR, exist_ok=True)
            for dir_name, dir_path in LOG_SUBDIRS.items():
                os.makedirs(dir_path, exist_ok=True)
            
            # 오래된 로그 파일 정리
            cleanup_old_logs()
            
            # 기본 로거 생성
            get_unified_logger()
            
            # 루트 로거 설정 - 콘솔 출력 방지
            root_logger = logging.getLogger()
            root_logger.handlers.clear()  # 기존 핸들러 제거
            
            _initialized = True
            internal_logger.info(f"{LOG_SYSTEM} 로깅 시스템 초기화 완료")
        except Exception as e:
            internal_logger.error(f"{LOG_SYSTEM} 로깅 시스템 초기화 실패: {str(e)}")
            raise

def get_unified_logger() -> logging.Logger:
    """통합 로거 싱글톤 인스턴스 반환"""
    global _unified_logger
    
    with _lock:
        if _unified_logger is None:
            # 로그 디렉토리 생성 - error 폴더 제외
            os.makedirs(LOGS_DIR, exist_ok=True)
            for dir_name, dir_path in LOG_SUBDIRS.items():
                os.makedirs(dir_path, exist_ok=True)
                
            _unified_logger = create_logger(
                name='unified_logger',
                log_dir=LOGS_DIR,
                level=DEFAULT_FILE_LEVEL,
                console_level=DEFAULT_FILE_LEVEL,
                format_str=LOG_FORMAT,
                add_console=True,
                add_error_file=True
            )
            _unified_logger.info(f"{LOG_SYSTEM} 통합 로거 초기화 완료")
            
        return _unified_logger

def get_logger(name: str, log_dir: Optional[str] = None, add_console: bool = False) -> logging.Logger:
    """
    이름으로 로거 인스턴스 가져오기
    - 이미 생성된 로거가 있으면 반환, 없으면 새로 생성
    - 특별한 로거가 아니면 통합 로거를 반환 (불필요한 로그 파일 생성 방지)
    
    Args:
        name: 로거 이름
        log_dir: 로그 파일 저장 디렉토리 (기본값: LOGS_DIR)
        add_console: 콘솔 출력 활성화 여부 (기본값: False)
        
    Returns:
        logging.Logger: 로거 인스턴스
    """
    # 특별한 로거가 아니면 통합 로거 반환 (불필요한 로그 파일 생성 방지)
    special_loggers = ['unified_logger', 'metrics_logger']
    if name not in special_loggers:
        return get_unified_logger()
        
    with _lock:
        if name not in _loggers or not _loggers[name].handlers:
            # 로그 디렉토리 생성
            os.makedirs(LOGS_DIR, exist_ok=True)
            for dir_name, dir_path in LOG_SUBDIRS.items():
                os.makedirs(dir_path, exist_ok=True)
            
            # 로그 디렉토리 설정
            if log_dir is None:
                if name == 'metrics_logger':
                    log_dir = LOGS_DIR
                else:
                    log_dir = LOGS_DIR
            
            _loggers[name] = create_logger(
                name=name,
                log_dir=log_dir,
                level=DEFAULT_FILE_LEVEL,
                console_level=DEFAULT_CONSOLE_LEVEL,
                format_str=LOG_FORMAT,
                add_console=add_console,
                add_error_file=True
            )
            
        return _loggers[name]

def shutdown_logging() -> None:
    """로깅 시스템 종료"""
    internal_logger = _get_internal_logger()
    
    with _lock:
        for name, logger in _loggers.items():
            for handler in logger.handlers[:]:
                try:
                    handler.close()
                    logger.removeHandler(handler)
                except Exception as e:
                    internal_logger.error(f"{LOG_SYSTEM} 로거 '{name}' 핸들러 종료 실패: {str(e)}")
        
        # 통합 로거 종료
        if _unified_logger:
            for handler in _unified_logger.handlers[:]:
                try:
                    handler.close()
                    _unified_logger.removeHandler(handler)
                except Exception as e:
                    internal_logger.error(f"{LOG_SYSTEM} 통합 로거 핸들러 종료 실패: {str(e)}")
        
        internal_logger.info(f"{LOG_SYSTEM} 로깅 시스템 종료 완료")
        
        # 내부 로거 종료
        if _internal_logger:
            for handler in _internal_logger.handlers[:]:
                try:
                    handler.close()
                    _internal_logger.removeHandler(handler)
                except Exception:
                    pass

# 모듈 초기화 시 내부 로거 초기화 및 루트 로거 설정
# 루트 로거 설정 - 콘솔 출력 방지
root_logger = logging.getLogger()
root_logger.handlers.clear()  # 기존 핸들러 제거

# 내부 로거 초기화
_get_internal_logger().info(f"{LOG_SYSTEM} logger.py 모듈 초기화 완료")

# 모듈 내보내기
__all__ = [
    'initialize_logging',
    'get_unified_logger',
    'get_logger',
    'shutdown_logging',
    'get_current_time_str',
    'create_raw_logger'
]