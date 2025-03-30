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

# 새로운 config 모듈에서 로깅 관련 상수 가져오기
from crosskimp.common.config.common_constants import (
    SystemComponent, COMPONENT_NAMES_KR
)

# 로그 디렉토리 및 파일 경로 설정 - 기본값으로 설정 (실제 경로는 나중에 초기화됨)
LOGS_DIR = "src/logs"
LOG_SUBDIRS = {
    'raw_data': "src/logs/raw_data",
    'orderbook_data': "src/logs/orderbook_data"
}

# 설정 가져오기 초기화 여부 플래그
_config_initialized = False

# 로깅 설정 값 - 기본값 사용 (후에 시스템 설정으로 덮어씌워짐)
DEFAULT_CONSOLE_LEVEL = logging.INFO  # 20
DEFAULT_FILE_LEVEL = logging.DEBUG    # 10

# ============================
# 로깅 상태 및 캐시
# ============================
_loggers = {}  # 싱글톤 패턴을 위한 로거 캐시
_unified_logger = None  # 통합 로거 인스턴스
_initialized = False  # 로깅 시스템 초기화 여부
_lock = threading.RLock()  # 스레드 안전한 재진입 락
_internal_logger = None  # 내부 로깅용 로거

# 컴포넌트별 로거 인스턴스를 저장할 딕셔너리
_component_loggers = {}  # 컴포넌트별 로거 캐시

# ============================
# 필터 클래스 정의 (내부 로거보다 먼저 정의)
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

# 시스템 컴포넌트를 로그에 추가하기 위한 필터
class ComponentFilter(logging.Filter):
    """로그 레코드에 컴포넌트 정보를 추가하는 필터"""
    def __init__(self, component=SystemComponent.SYSTEM.value):
        super().__init__()
        self.component = component
        self.component_name = COMPONENT_NAMES_KR.get(component, f"[{component}]")
    
    def filter(self, record):
        # 레코드에 컴포넌트 정보 추가
        record.component = self.component_name
        return True

# ============================
# 유틸리티 함수
# ============================
def _get_logging_setting(key, default_value):
    """시스템 설정에서 로깅 관련 설정 가져오기"""
    try:
        # 늦은 임포트 (lazy import) 사용 - 순환 참조 방지
        from crosskimp.common.config.app_config import get_config
        config = get_config()
        return config.get_system(f"logging.{key}", default_value)
    except:
        return default_value

def _initialize_config_settings():
    """설정 값 로드 및 초기화"""
    global _config_initialized, DEFAULT_CONSOLE_LEVEL, DEFAULT_FILE_LEVEL
    
    if _config_initialized:
        return
    
    # 콘솔 및 파일 로그 레벨 가져오기
    console_level_str = _get_logging_setting("console_level", "INFO")
    file_level_str = _get_logging_setting("file_level", "DEBUG")
    
    # 레벨 문자열을 로깅 레벨로 변환
    level_map = {
        "DEBUG": logging.DEBUG,      # 10
        "INFO": logging.INFO,        # 20
        "WARNING": logging.WARNING,  # 30
        "ERROR": logging.ERROR,      # 40
        "CRITICAL": logging.CRITICAL # 50
    }
    
    DEFAULT_CONSOLE_LEVEL = level_map.get(console_level_str.upper(), logging.INFO)
    DEFAULT_FILE_LEVEL = level_map.get(file_level_str.upper(), logging.DEBUG)
    
    _config_initialized = True

def get_current_time_str() -> str:
    """현재 시간을 yymmdd_hhmmss 형식으로 반환"""
    return datetime.now().strftime("%y%m%d_%H%M%S")

def _get_log_format():
    """로그 포맷 가져오기"""
    return _get_logging_setting("format", "%(asctime)s.%(msecs)03d - %(filename)-20s:%(lineno)-3d / %(levelname)-7s - %(component)s %(message)s")

def _get_debug_log_format():
    """디버그 로그 포맷 가져오기"""
    return _get_logging_setting("debug_format", "%(asctime)s.%(msecs)03d - %(filename)-20s:%(lineno)-3d / %(levelname)-7s - %(component)s %(message)s")

def _get_log_encoding():
    """로그 인코딩 가져오기"""
    return _get_logging_setting("encoding", "utf-8")

def _get_log_mode():
    """로그 파일 모드 가져오기"""
    return _get_logging_setting("mode", "a")

def _get_log_max_bytes():
    """로그 파일 최대 크기 가져오기"""
    return _get_logging_setting("max_bytes", 100 * 1024 * 1024)  # 100MB

def _get_log_backup_count():
    """로그 백업 파일 수 가져오기"""
    return _get_logging_setting("backup_count", 100)

def _get_log_cleanup_days():
    """로그 파일 정리 기간 가져오기"""
    return _get_logging_setting("cleanup_days", 30)

def _get_flush_interval():
    """로그 플러시 간격 가져오기"""
    return _get_logging_setting("flush_interval", 1.0)

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
        
        # 컴포넌트 필터 추가
        _internal_logger.addFilter(ComponentFilter())
        
        # 콘솔 핸들러 추가
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(_get_log_format())
        console_handler.setFormatter(formatter)
        _internal_logger.addHandler(console_handler)
    
    return _internal_logger

def cleanup_old_logs(max_days: int = None) -> None:
    """오래된 로그 파일 정리"""
    if max_days is None:
        max_days = _get_log_cleanup_days()
        
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
                internal_logger.info(f"오래된 로그 파일 삭제: {filepath}")
            except Exception as e:
                internal_logger.error(f"로그 파일 삭제 실패: {filepath} | {str(e)}")
                
    except Exception as e:
        internal_logger.error(f"로그 파일 정리 중 오류: {str(e)}")

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
        self.last_flush = time.time()
        self.flush_interval = _get_flush_interval()

    def _open(self):
        """파일 열기 (에러 처리 포함)"""
        try:
            return super()._open()
        except Exception as e:
            self.error_count += 1
            self.internal_logger.error(f"로그 파일 열기 실패 ({self.error_count}/{self.max_errors}): {e}")
            
            # 임시 파일로 대체
            import tempfile
            return tempfile.TemporaryFile(mode='a+', encoding=_get_log_encoding())

    def close(self):
        """파일 닫기 (에러 처리 포함)"""
        try:
            if self.stream:
                super().close()
        except Exception as e:
            self.internal_logger.error(f"로그 파일 닫기 실패: {e}")

    def doRollover(self):
        """로그 파일 로테이션 수행 (에러 처리 포함)"""
        try:
            with self.lock:  # 락 사용
                super().doRollover()
                self.error_count = 0  # 성공 시 에러 카운트 초기화
        except Exception as e:
            self.error_count += 1
            self.internal_logger.error(f"로그 로테이션 중 오류 발생 ({self.error_count}/{self.max_errors}): {e}")
            
            # 에러 발생 시 새로운 파일 생성 시도
            if self.error_count < self.max_errors:
                try:
                    self.close()
                    self.stream = self._open()
                except Exception as inner_e:
                    self.internal_logger.error(f"로그 스트림 재생성 실패: {inner_e}")

    def emit(self, record):
        """로그 레코드 출력 (에러 처리 및 자동 복구 포함, 시간 체크 최적화)"""
        if self.error_count >= self.max_errors:
            # 너무 많은 에러 발생 시 콘솔로만 출력
            self.internal_logger.error(f"로그 핸들러 비활성화됨 (너무 많은 에러): {record.getMessage()}")
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
                # 로그가 즉시 파일에 기록되도록 flush 호출
                # 매 로그마다 flush하는 대신 주기적으로 수행
                if current_time - self.last_flush > self.flush_interval:
                    self.flush()
                    self.last_flush = current_time
                self.error_count = 0  # 성공 시 에러 카운트 초기화
        except Exception as e:
            self.error_count += 1
            self.internal_logger.error(f"로그 출력 중 오류 발생 ({self.error_count}/{self.max_errors}): {e}")
            
            # 에러 발생 시 복구 시도
            if self.error_count < self.max_errors:
                try:
                    with self.lock:
                        self.close()
                        self.stream = self._open()
                        super().emit(record)
                        # 에러 복구 시에도 flush 수행
                        self.flush()
                        self.last_flush = time.time()
                except Exception as inner_e:
                    self.internal_logger.error(f"로그 출력 복구 실패: {inner_e}")

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
    level: int = None,
    console_level: int = None,
    format_str: str = None,
    add_console: bool = False,
    add_error_file: bool = True,
    component: str = SystemComponent.SYSTEM.value
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
        component: 시스템 컴포넌트 식별자
        
    Returns:
        logging.Logger: 생성된 로거 인스턴스
    """
    # 설정 초기화 확인
    _initialize_config_settings()
    
    # 기본값 설정
    if level is None:
        level = DEFAULT_FILE_LEVEL
    if console_level is None:
        console_level = DEFAULT_CONSOLE_LEVEL
    if format_str is None:
        format_str = _get_log_format()
        
    internal_logger = _get_internal_logger()
    
    logger = logging.getLogger(name)
    logger.setLevel(min(level, console_level if add_console else level))  # 가장 낮은 레벨로 설정
    
    # 기존 핸들러 제거
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # 부모 로거로 로그가 전파되지 않도록 설정
    logger.propagate = False
    
    # 컴포넌트 필터 추가
    logger.addFilter(ComponentFilter(component))
    
    current_time = get_current_time_str()
    
    # 로그 디렉토리 존재 확인
    os.makedirs(log_dir, exist_ok=True)
    
    # 일반 로그 핸들러
    log_file = f"{log_dir}/{current_time}_{name}.log"
    try:
        file_handler = SafeRotatingFileHandler(
            filename=log_file,
            maxBytes=_get_log_max_bytes(),
            backupCount=_get_log_backup_count(),
            encoding=_get_log_encoding(),
            mode=_get_log_mode()
        )
        # 파일 권한 설정
        os.chmod(log_file, 0o644)
        
        file_handler.setLevel(level)
        formatter = logging.Formatter(format_str)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        internal_logger.info(f"로그 파일 생성: {log_file}")
    except Exception as e:
        internal_logger.error(f"로그 파일 생성 실패: {log_file} | {str(e)}")
    
    # 에러 전용 로그 파일 추가
    if add_error_file:
        error_log_file = f"{log_dir}/{current_time}_{name}_error.log"
        try:
            error_handler = SafeRotatingFileHandler(
                filename=error_log_file,
                maxBytes=_get_log_max_bytes(),
                backupCount=_get_log_backup_count(),
                encoding=_get_log_encoding(),
                mode=_get_log_mode()
            )
            # 파일 권한 설정
            os.chmod(error_log_file, 0o644)
            
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(formatter)
            error_handler.addFilter(ErrorFilter())
            logger.addHandler(error_handler)
            internal_logger.info(f"에러 로그 파일 생성: {error_log_file}")
        except Exception as e:
            internal_logger.error(f"에러 로그 파일 생성 실패: {error_log_file} | {str(e)}")
    
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
    logger_name = f"{exchange_name}_logger"
    
    # 하위 폴더 생성하지 않고 raw_data 디렉토리에 직접 로깅
    raw_data_dir = LOG_SUBDIRS['raw_data']
    os.makedirs(raw_data_dir, exist_ok=True)
    
    logger = create_logger(
        name=logger_name,
        log_dir=raw_data_dir,
        level=logging.DEBUG,
        console_level=logging.ERROR,
        format_str=_get_debug_log_format(),
        add_console=False,
        add_error_file=False,
        component=SystemComponent.OB_COLLECTOR.value  # 오더북 컴포넌트 지정
    )
    
    logger.debug(f"raw 로거 초기화 완료")
    return logger

# ============================
# 로거 인스턴스 관리 함수
# ============================
def initialize_logging() -> None:
    """로깅 시스템 초기화"""
    # 내부 로거 가져오기
    internal_logger = _get_internal_logger()
    
    global _initialized
    
    with _lock:
        if _initialized:
            return
            
        try:
            # AppConfig 에서 경로 정보 가져오기
            try:
                # 늦은 임포트 (lazy import) 사용 - 순환 참조 방지
                from crosskimp.common.config.app_config import get_config
                config = get_config()
                global LOGS_DIR, LOG_SUBDIRS
                LOGS_DIR = config.get_path("logs_dir")
                LOG_SUBDIRS['raw_data'] = config.get_path("raw_data_dir")
                LOG_SUBDIRS['orderbook_data'] = config.get_path("orderbook_data_dir")
                internal_logger.info(f"설정에서 로그 경로 로드: {LOGS_DIR}")
            except Exception as e:
                internal_logger.error(f"로그 경로 로드 실패, 기본값 사용: {str(e)}")
                
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
            internal_logger.info(f"로깅 시스템 초기화 완료")
        except Exception as e:
            internal_logger.error(f"로깅 시스템 초기화 실패: {str(e)}")
            raise

def get_unified_logger(component: str = SystemComponent.SYSTEM.value) -> logging.Logger:
    """
    컴포넌트별 통합 로거 인스턴스 반환
    각 컴포넌트마다 별도의 로거 인스턴스를 생성하고 캐싱합니다.
    
    Args:
        component: 시스템 컴포넌트 식별자 (기본값: SYSTEM)
        
    Returns:
        logging.Logger: 컴포넌트 필터가 적용된 통합 로거 인스턴스
    """
    global _unified_logger, _component_loggers
    
    # 설정 초기화 먼저 수행
    _initialize_config_settings()
    
    with _lock:
        # 기본 통합 로거가 없으면 생성
        if _unified_logger is None:
            # 로그 디렉토리 생성 - 필요한 모든 디렉토리 확인
            os.makedirs(LOGS_DIR, exist_ok=True)
            for dir_name, dir_path in LOG_SUBDIRS.items():
                os.makedirs(dir_path, exist_ok=True)
                
            _unified_logger = create_logger(
                name='unified_logger',
                log_dir=LOGS_DIR,
                level=DEFAULT_FILE_LEVEL,
                console_level=DEFAULT_CONSOLE_LEVEL,
                format_str=_get_log_format(),
                add_console=True,
                add_error_file=True,
                component=SystemComponent.SYSTEM.value
            )
            _unified_logger.info(f"통합 로거 초기화 완료")
        
        # 컴포넌트별 로거가 없으면 생성
        if component not in _component_loggers:
            # 기본 로거 복제
            component_logger = logging.getLogger(f'unified_logger_{component}')
            component_logger.handlers = _unified_logger.handlers
            component_logger.level = _unified_logger.level
            component_logger.propagate = False
            
            # 기존 컴포넌트 필터 제거
            for f in component_logger.filters:
                if isinstance(f, ComponentFilter):
                    component_logger.filters.remove(f)
            
            # 새 컴포넌트 필터 추가
            component_logger.addFilter(ComponentFilter(component))
            
            # 캐시에 저장
            _component_loggers[component] = component_logger
        
        return _component_loggers[component]

def shutdown_logging() -> None:
    """로깅 시스템 종료"""
    internal_logger = _get_internal_logger()
    
    with _lock:
        for name, logger in _loggers.items():
            for handler in logger.handlers[:]:
                try:
                    # 핸들러가 flush 메서드를 지원하면 호출
                    if hasattr(handler, 'flush'):
                        handler.flush()
                    handler.close()
                    logger.removeHandler(handler)
                except Exception as e:
                    internal_logger.error(f"로거 '{name}' 핸들러 종료 실패: {str(e)}")
        
        # 통합 로거 종료
        if _unified_logger:
            for handler in _unified_logger.handlers[:]:
                try:
                    # 핸들러가 flush 메서드를 지원하면 호출
                    if hasattr(handler, 'flush'):
                        handler.flush()
                    handler.close()
                    _unified_logger.removeHandler(handler)
                except Exception as e:
                    internal_logger.error(f"통합 로거 핸들러 종료 실패: {str(e)}")
                    
        internal_logger.info("로깅 시스템이 종료되었습니다.")

# 모듈 초기화 시 내부 로거 초기화 및 루트 로거 설정
# 루트 로거 설정 - 콘솔 출력 방지
root_logger = logging.getLogger()
root_logger.handlers.clear()  # 기존 핸들러 제거

# 내부 로거 초기화
_get_internal_logger().info(f"logger.py 모듈 초기화 완료")

# 모듈 내보내기
__all__ = [
    'initialize_logging',
    'get_unified_logger',
    'shutdown_logging',
    'get_current_time_str',
    'create_raw_logger'
]