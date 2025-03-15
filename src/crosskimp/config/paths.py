import os
import sys
from pathlib import Path

def get_project_root():
    """
    프로젝트 루트 디렉토리를 찾는 함수
    
    여러 방법을 시도하여 프로젝트 루트를 찾습니다:
    1. 환경 변수 확인
    2. 패키지 기반 검색
    3. 프로젝트 마커 파일 검색
    """
    # 1. 환경 변수에서 확인 (Docker나 배포 환경에서 유용)
    if 'PROJECT_ROOT' in os.environ:
        return Path(os.environ['PROJECT_ROOT'])
    
    # 2. Docker 환경 확인
    if os.path.exists('/.dockerenv') or os.environ.get('PYTHONPATH') == '/app':
        return Path('/app')
    
    # 3. 패키지 기반 검색
    try:
        # 프로젝트의 최상위 패키지 경로 찾기
        import crosskimp
        package_path = Path(crosskimp.__file__).parent
        # src/crosskimp에서 src의 상위 디렉토리가 프로젝트 루트
        return package_path.parent.parent
    except (ImportError, AttributeError):
        pass
    
    # 4. 프로젝트 마커 파일 검색 (pyproject.toml, setup.py 등)
    current_path = Path(__file__).resolve()
    for parent in current_path.parents:
        if (parent / 'pyproject.toml').exists() or (parent / 'setup.py').exists():
            return parent
    
    # 5. 현재 작업 디렉토리 사용 (마지막 수단)
    return Path.cwd()

# 프로젝트 루트 경로 (모듈 로드 시 한 번만 계산)
PROJECT_ROOT = get_project_root()

# 주요 디렉토리 경로 정의
SRC_DIR = PROJECT_ROOT / 'src'
LOGS_DIR = PROJECT_ROOT / 'logs'
DATA_DIR = PROJECT_ROOT / 'data'
CONFIG_DIR = Path(__file__).parent

# 로그 하위 디렉토리 경로 설정
LOG_SUBDIRS = {
    'queue': LOGS_DIR / 'queue',
    'error': LOGS_DIR / 'error',
    'telegram': LOGS_DIR / 'telegram',
    'metrics': LOGS_DIR / 'metrics',
    'archive': LOGS_DIR / 'archive',
    'raw_data': LOGS_DIR / 'raw_data',
    'serialized_data': LOGS_DIR / 'serialized_data'
}

# 필요한 디렉토리 생성 함수
def ensure_directories():
    """필요한 모든 디렉토리가 존재하는지 확인하고, 없으면 생성합니다."""
    # 기본 디렉토리 생성
    for dir_path in [SRC_DIR, LOGS_DIR, DATA_DIR]:
        dir_path.mkdir(exist_ok=True, parents=True)
    
    # 로그 하위 디렉토리 생성
    for subdir_path in LOG_SUBDIRS.values():
        subdir_path.mkdir(exist_ok=True, parents=True)

# 모듈 로드 시 디렉토리 생성 실행
ensure_directories()

# 모듈 내보내기
__all__ = [
    'PROJECT_ROOT',
    'SRC_DIR',
    'LOGS_DIR',
    'DATA_DIR',
    'CONFIG_DIR',
    'LOG_SUBDIRS',
    'ensure_directories'
] 