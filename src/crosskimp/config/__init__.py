"""
크로스킴프 설정 패키지

이 패키지는 프로젝트 전체에서 사용되는 설정 및 경로 관련 모듈을 포함합니다.
"""

# 경로 관련 상수 가져오기
from crosskimp.config.paths import (
    PROJECT_ROOT, SRC_DIR, LOGS_DIR, DATA_DIR, 
    LOG_SUBDIRS, ensure_directories
)

__all__ = [
    'PROJECT_ROOT',
    'SRC_DIR',
    'LOGS_DIR',
    'DATA_DIR',
    'LOG_SUBDIRS',
    'ensure_directories'
] 