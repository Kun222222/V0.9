"""
애플리케이션 설정 관리 모듈

시스템 설정, 거래소 설정, 환경변수를 통합 관리하는 단일 설정 진입점을 제공합니다.
"""

import os
import logging
import configparser
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
import json
from datetime import datetime

from crosskimp.common.config.env_loader import EnvLoader
from crosskimp.common.config.common_constants import SystemComponent

# 기본 로거 설정 - 순환 참조 방지를 위해 기본 Python logging 사용
logger = logging.getLogger("app_config")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[시스템] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

class AppConfig:
    """애플리케이션 통합 설정 관리 클래스"""
    
    # 싱글톤 인스턴스
    _instance = None
    
    # 기본 설정파일 이름 (이것만 하드코딩)
    SYSTEM_SETTINGS_FILE = "system_settings.cfg"
    
    @classmethod
    def get_instance(cls) -> 'AppConfig':
        """싱글톤 인스턴스 반환"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    @classmethod
    def get_project_root(cls) -> str:
        """
        프로젝트 루트 디렉토리 찾기 (공개 클래스 메서드)
        
        main.py에서 설정한 PROJECT_ROOT 환경변수를 사용합니다.
        환경변수가 없으면 현재 작업 디렉토리를 사용합니다.
        
        Returns:
            str: 프로젝트 루트 경로
        """
        # 환경 변수에서 확인 (최우선)
        if 'PROJECT_ROOT' in os.environ:
            project_root = os.environ['PROJECT_ROOT']
            if os.path.exists(project_root):
                return project_root
            
        # 환경변수가 없거나 경로가 존재하지 않으면 현재 작업 디렉토리 사용
        logger.warning("PROJECT_ROOT 환경변수가 설정되지 않았거나 경로가 존재하지 않습니다. 현재 작업 디렉토리를 사용합니다.")
        return str(Path.cwd())
    
    def __init__(self):
        """초기화 - 환경변수 및 설정 로드"""
        # 1. 먼저 프로젝트 루트 설정 (다른 모든 경로의 기준점)
        self.project_root = self.get_project_root()
        
        # 2. 필수 디렉토리 경로 먼저 설정 (다른 설정보다 우선)
        self._setup_essential_paths()
        
        # 3. 기본 딕셔너리 초기화 (이후 로드 실패해도 빈 객체는 있음)
        self.system_settings = {}
        self.exchange_settings = {}
        self.constants = {}
        
        # 4. 시스템 설정 로드 (경로 관리에 필요)
        self._load_system_settings()
        
        # 5. 전체 경로 초기화 (시스템 설정 이후)
        self._initialize_all_paths()
        
        # 6. 환경변수 로더 초기화 (경로 설정 이후)
        self.env_loader = EnvLoader.get_instance()
        # .env 파일은 항상 프로젝트 루트에 있음
        self.env_file_path = os.path.join(self.project_root, ".env")
        self.env_loader.load(self.env_file_path)
        
        # 7. 거래소 설정 로드
        self._load_exchange_settings()
        
        # 8. 설정 유효성 검증
        self._validate_settings()
        
        logger.info("애플리케이션 설정 로드 완료")
    
    def _setup_essential_paths(self):
        """가장 기본적인 필수 경로만 설정 (다른 설정 로드 전에 필요)"""
        # 현재 파일의 위치 (app_config.py)를 기준으로 config 디렉토리 찾기
        current_file_path = os.path.abspath(__file__)
        current_dir = os.path.dirname(current_file_path)
        
        # config 디렉토리는 현재 파일이 있는 디렉토리 (common/config)
        self.config_dir = current_dir
        logger.info(f"설정 디렉토리 경로: {self.config_dir}")
        
        # 설정 파일 이름 설정
        self.exchange_settings_file = "exchange_settings.cfg"
    
    def _initialize_all_paths(self):
        """모든 경로 설정 초기화 - 시스템 설정 로드 후 호출"""
        # paths 섹션 가져오기
        self.paths = self.system_settings.get('paths', {})
        
        # 설정 파일 관련 경로
        self.exchange_settings_file = self.get_system("paths.exchange_settings_file", "exchange_settings.cfg")
        
        # 기본 디렉토리
        self.logs_dir = self._get_full_path("logs_dir", "logs")
        self.data_dir = self._get_full_path("data_dir", "data")
        self.temp_dir = self._get_full_path("temp_dir", "temp")
        
        # 설정 파일 관련 경로
        self.backup_dir = self._get_full_path("backup_dir", "config/backups")
        
        # 로그 관련 경로
        self.raw_data_dir = self._get_full_path("raw_data", "logs/raw_data")
        
        # 데이터베이스 관련 경로
        self.db_path = self._get_full_path("db_path", "data/trading.db")
        
        # 디렉토리 생성
        self._ensure_directories_exist()
    
    def _load_system_settings(self) -> None:
        """시스템 설정 파일 로드"""
        system_settings_path = os.path.join(self.config_dir, self.SYSTEM_SETTINGS_FILE)
        
        if not os.path.exists(system_settings_path):
            raise FileNotFoundError(f"시스템 설정 파일을 찾을 수 없습니다: {system_settings_path}")
        
        config = configparser.ConfigParser(interpolation=None)
        config.read(system_settings_path, encoding='utf-8')
        
        # ConfigParser 객체를 딕셔너리로 변환
        self.system_settings = self._config_to_dict(config)
        logger.debug(f"시스템 설정 로드 완료: {len(self.system_settings)} 섹션")
    
    def _set_default_settings(self) -> None:
        """설정 파일을 읽지 못한 경우 사용할 기본 설정"""
        # 시스템 설정 기본값
        self.system_settings = {
            "paths": {
                "logs_dir": "logs",
                "data_dir": "data",
                "config_dir": "config",
                "temp_dir": "temp",
                "backup_dir": "config/backups",
                "env_file": ".env",
                "raw_data": "logs/raw_data",
                "db_path": "data/trading.db"
            },
            "logging": {
                "console_level": "INFO",
                "file_level": "DEBUG",
                "format": "%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s",
                "max_bytes": 104857600,
                "backup_count": 10
            }
        }
        
        logger.warning("기본 시스템 설정을 사용합니다.")
    
    def _load_exchange_settings(self) -> None:
        """거래소 설정 파일 로드"""
        exchange_settings_path = os.path.join(self.config_dir, self.exchange_settings_file)
        
        if not os.path.exists(exchange_settings_path):
            raise FileNotFoundError(f"거래소 설정 파일을 찾을 수 없습니다: {exchange_settings_path}")
        
        config = configparser.ConfigParser(interpolation=None)
        config.read(exchange_settings_path, encoding='utf-8')
        
        # ConfigParser 객체를 딕셔너리로 변환
        self.exchange_settings = self._config_to_dict(config)
        logger.debug(f"거래소 설정 로드 완료: {len(self.exchange_settings)} 섹션")
    
    def _set_default_exchange_settings(self) -> None:
        """거래소 설정 파일을 읽지 못한 경우 사용할 기본 설정"""
        # 거래소 설정 기본값
        self.exchange_settings = {
            "exchanges": {
                "binance": {"maker_fee": 0.1, "taker_fee": 0.1},
                "bybit": {"maker_fee": 0.1, "taker_fee": 0.1},
                "upbit": {"maker_fee": 0.05, "taker_fee": 0.05},
                "bithumb": {"maker_fee": 0.037, "taker_fee": 0.037}
            },
            "trading": {
                "symbols": {
                    "excluded": [],
                    "included": [],
                    "excluded_patterns": []
                },
                "settings": {
                    "min_volume_krw": 10000000000
                }
            }
        }
        
        logger.warning("기본 거래소 설정을 사용합니다.")
    
    def _config_to_dict(self, config: configparser.ConfigParser) -> Dict[str, Any]:
        """ConfigParser 객체를 딕셔너리로 변환"""
        result = {}
        logger.debug(f"ConfigParser를 딕셔너리로 변환 시작. 섹션 목록: {config.sections()}")
        
        for section in config.sections():
            logger.debug(f"섹션 처리: {section}")
            if '.' in section:
                # 계층 구조 처리 (예: exchanges.binance)
                main_section, sub_section = section.split('.', 1)
                logger.debug(f"계층 구조 처리: main_section={main_section}, sub_section={sub_section}")
                
                if main_section not in result:
                    result[main_section] = {}
                
                if sub_section not in result[main_section]:
                    result[main_section][sub_section] = {}
                
                # 설명 처리
                if 'description' in config[section]:
                    result[main_section][sub_section]['description'] = config[section]['description']
                
                # 나머지 키-값 처리
                for key, value in config[section].items():
                    if key != 'description':
                        parsed_value = self._parse_config_value(value)
                        result[main_section][sub_section][key] = parsed_value
                        logger.debug(f"키-값 처리: {main_section}.{sub_section}.{key} = {parsed_value}")
            else:
                # 일반 섹션 처리
                result[section] = {}
                logger.debug(f"일반 섹션 처리: {section}")
                
                # 설명 처리
                if 'description' in config[section]:
                    result[section]['description'] = config[section]['description']
                
                # 나머지 키-값 처리
                for key, value in config[section].items():
                    if key != 'description':
                        parsed_value = self._parse_config_value(value)
                        result[section][key] = parsed_value
                        logger.debug(f"키-값 처리: {section}.{key} = {parsed_value}")
        
        logger.debug(f"ConfigParser를 딕셔너리로 변환 완료. 결과: {list(result.keys())}")
        return result
    
    def _parse_config_value(self, value: str) -> Any:
        """설정 값을 적절한 타입으로 변환"""
        # 주석 제거 (# 이후 부분)
        if '#' in value:
            value = value.split('#')[0].strip()
            
        # 불리언 처리
        if value.lower() in ('true', 'yes', '1'):
            return True
        elif value.lower() in ('false', 'no', '0'):
            return False
        
        # 숫자 처리
        try:
            # 정수 처리
            if value.isdigit():
                return int(value)
            # 실수 처리
            return float(value)
        except (ValueError, TypeError):
            pass
        
        # 리스트 처리 (쉼표로 구분된 값)
        if ',' in value:
            return [item.strip() for item in value.split(',')]
        
        # 기본적으로 문자열 반환
        return value
    
    def _validate_settings(self) -> None:
        """로드된 설정의 유효성 검증"""
        # 필수 섹션 확인
        required_system_sections = ["logging", "paths"]
        for section in required_system_sections:
            if section not in self.system_settings:
                logger.warning(f"필수 시스템 설정 섹션이 없습니다: {section}")
        
        required_exchange_sections = ["exchanges", "trading"]
        for section in required_exchange_sections:
            if section not in self.exchange_settings:
                logger.warning(f"필수 거래소 설정 섹션이 없습니다: {section}")
    
    def get_system(self, path: str, default: Any = None) -> Any:
        """
        시스템 설정 값 가져오기
        
        Args:
            path: 설정 경로 (예: "logging.file_level")
            default: 기본값
            
        Returns:
            설정 값 또는 기본값
        """
        return self._get_from_dict(self.system_settings, path, default)
    
    def get_exchange(self, path: str, default: Any = None) -> Any:
        """
        거래소 설정 값 가져오기
        
        Args:
            path: 설정 경로 (예: "exchanges.binance.maker_fee")
            default: 기본값
            
        Returns:
            설정 값 또는 기본값
        """
        return self._get_from_dict(self.exchange_settings, path, default)
    
    def get_env(self, path: str, default: Any = None) -> Any:
        """
        환경변수 값 가져오기
        
        Args:
            path: 환경변수 경로 (예: "binance.api_key")
            default: 기본값
            
        Returns:
            환경변수 값 또는 기본값
        """
        return self.env_loader.get(path, default)
    
    def get(self, path: str, default: Any = None) -> Any:
        """
        통합 설정 값 가져오기 (우선순위: 환경변수 > 설정파일 > 상수 > 기본값)
        
        Args:
            path: 설정 경로 (예: "exchanges.binance.maker_fee")
            default: 기본값
            
        Returns:
            설정 값 또는 기본값
        """
        # 1. 환경변수에서 확인 (최우선)
        env_value = self.get_env(path)
        if env_value is not None:
            return env_value
        
        # 2. 시스템 설정에서 확인
        sys_value = self.get_system(path)
        if sys_value is not None:
            return sys_value
        
        # 3. 거래소 설정에서 확인
        exchange_value = self.get_exchange(path)
        if exchange_value is not None:
            return exchange_value
        
        # 4. 기본값 반환
        return default
    
    def _get_from_dict(self, data: Dict[str, Any], path: str, default: Any = None) -> Any:
        """
        중첩 딕셔너리에서 경로를 통해 값 가져오기
        
        Args:
            data: 대상 딕셔너리
            path: 키 경로 (예: "logging.file_level")
            default: 기본값
            
        Returns:
            값 또는 기본값
        """
        parts = path.split('.')
        current = data
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return default
        
        return current
    
    def backup_settings(self) -> Dict[str, str]:
        """
        현재 설정 파일 백업
        
        Returns:
            Dict[str, str]: 백업 파일 경로
        """
        # 백업 디렉토리 확인 및 생성
        os.makedirs(self.backup_dir, exist_ok=True)
        
        # 타임스탬프 생성
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        backup_files = {}
        
        # 시스템 설정 파일 백업
        system_settings_path = os.path.join(self.config_dir, self.SYSTEM_SETTINGS_FILE)
        if os.path.exists(system_settings_path):
            backup_system_path = os.path.join(self.backup_dir, f"system_settings_{timestamp}.cfg")
            try:
                with open(system_settings_path, 'r', encoding='utf-8') as src:
                    with open(backup_system_path, 'w', encoding='utf-8') as dst:
                        dst.write(src.read())
                backup_files['system'] = backup_system_path
                logger.info(f"시스템 설정 파일 백업 완료: {backup_system_path}")
            except Exception as e:
                logger.error(f"시스템 설정 파일 백업 실패: {str(e)}")
        
        # 거래소 설정 파일 백업
        exchange_settings_path = os.path.join(self.config_dir, self.exchange_settings_file)
        if os.path.exists(exchange_settings_path):
            backup_exchange_path = os.path.join(self.backup_dir, f"exchange_settings_{timestamp}.cfg")
            try:
                with open(exchange_settings_path, 'r', encoding='utf-8') as src:
                    with open(backup_exchange_path, 'w', encoding='utf-8') as dst:
                        dst.write(src.read())
                backup_files['exchange'] = backup_exchange_path
                logger.info(f"거래소 설정 파일 백업 완료: {backup_exchange_path}")
            except Exception as e:
                logger.error(f"거래소 설정 파일 백업 실패: {str(e)}")
                
        return backup_files
    
    def save_system_settings(self, settings: Dict[str, Any]) -> bool:
        """
        시스템 설정 저장
        
        Args:
            settings: 저장할 설정 딕셔너리
            
        Returns:
            bool: 저장 성공 여부
        """
        return self._save_settings(settings, self.SYSTEM_SETTINGS_FILE)
    
    def save_exchange_settings(self, settings: Dict[str, Any]) -> bool:
        """
        거래소 설정 저장
        
        Args:
            settings: 저장할 설정 딕셔너리
            
        Returns:
            bool: 저장 성공 여부
        """
        return self._save_settings(settings, self.exchange_settings_file)
    
    def _save_settings(self, settings: Dict[str, Any], filename: str) -> bool:
        """
        설정 저장 공통 로직
        
        Args:
            settings: 저장할 설정 딕셔너리
            filename: 저장할 파일 이름
            
        Returns:
            bool: 저장 성공 여부
        """
        # 백업 먼저 수행
        self.backup_settings()
        
        config = configparser.ConfigParser(interpolation=None)
        
        # 딕셔너리를 ConfigParser 형식으로 변환
        for section, section_data in settings.items():
            if isinstance(section_data, dict):
                for subsection, subsection_data in section_data.items():
                    if isinstance(subsection_data, dict):
                        # 중첩 섹션 (예: exchanges.binance)
                        section_name = f"{section}.{subsection}"
                        config[section_name] = {}
                        
                        for key, value in subsection_data.items():
                            config[section_name][key] = self._format_config_value(value)
                    else:
                        # 일반 키-값
                        if section not in config:
                            config[section] = {}
                        config[section][subsection] = self._format_config_value(section_data)
            else:
                # 직접 값이 있는 경우
                if 'DEFAULT' not in config:
                    config['DEFAULT'] = {}
                config['DEFAULT'][section] = self._format_config_value(section_data)
        
        # 설정 파일 저장
        file_path = os.path.join(self.config_dir, filename)
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                config.write(f)
            
            logger.info(f"설정 파일 저장 완료: {file_path}")
            
            # 설정 다시 로드
            if filename == self.SYSTEM_SETTINGS_FILE:
                self._load_system_settings()
                
            return True
            
        except Exception as e:
            logger.error(f"설정 파일 저장 실패: {str(e)}")
            return False
    
    def _format_config_value(self, value: Any) -> str:
        """
        ConfigParser 저장을 위한 값 형식 변환
        
        Args:
            value: 변환할 값
            
        Returns:
            str: 변환된 문자열
        """
        if isinstance(value, bool):
            return "true" if value else "false"
        elif isinstance(value, (list, tuple)):
            return ", ".join(str(item) for item in value)
        elif isinstance(value, dict):
            return json.dumps(value)
        else:
            return str(value)
            
    def dump_settings(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """
        모든 설정을 딕셔너리로 덤프 (디버깅용)
        
        Args:
            include_sensitive: 민감한 정보 포함 여부
            
        Returns:
            Dict: 전체 설정
        """
        result = {
            "system": self.system_settings,
            "exchange": self.exchange_settings,
            "env": self.env_loader.dump_settings(include_sensitive)
        }
        
        return result 
    
    def get_value_from_settings(self, path: str, default: Any = None) -> Any:
        """
        설정에서 값 가져오기 - 더 간단한 인터페이스를 제공하는 편의 메서드
        
        Args:
            path: 설정 경로 (예: "exchanges.binance.maker_fee" 또는 "logging.file_level")
            default: 기본값
            
        Returns:
            Any: 설정 값 또는 기본값
        """
        logger.debug(f"get_value_from_settings 호출됨: path={path}, default={default}")
        
        if path.startswith("exchanges."):
            logger.debug(f"거래소 설정에서 값 조회: {path}")
            result = self.get_exchange(path, default)
            logger.debug(f"거래소 설정 결과: {result}")
            return result
        elif path.startswith("trading."):
            # 'trading.settings.min_volume_krw' 형식의 경로 처리
            # exchange_settings의 첫 번째 수준은 'trading'
            logger.debug(f"거래소 설정(trading)에서 값 조회: {path}")
            # 디버깅을 위해 trading 섹션 출력
            if 'trading' in self.exchange_settings:
                logger.debug(f"trading 섹션 확인: {self.exchange_settings['trading']}")
            result = self.get_exchange(path, default)
            logger.debug(f"trading 설정 결과: {result}")
            return result
        else:
            logger.debug(f"시스템 설정에서 값 조회: {path}")
            result = self.get_system(path, default)
            logger.debug(f"시스템 설정 결과: {result}")
            return result
    
    def get_symbol_filters(self) -> Dict[str, list]:
        """
        심볼 필터 설정 가져오기
        
        Returns:
            Dict[str, list]: 심볼 필터 설정 (excluded, included, excluded_patterns)
        """
        excluded = self.get_exchange("trading.symbols.excluded", [])
        included = self.get_exchange("trading.symbols.included", [])
        patterns = self.get_exchange("trading.symbols.excluded_patterns", [])
        
        return {
            "excluded": excluded,
            "included": included,
            "excluded_patterns": patterns
        }

    def _ensure_directories_exist(self) -> None:
        """모든 필요한 디렉토리가 존재하는지 확인하고 없으면 생성"""
        directories = [
            self.logs_dir,
            self.data_dir,
            self.backup_dir,
            self.temp_dir,
            self.raw_data_dir,
            os.path.dirname(self.db_path)
        ]
        
        for directory in directories:
            if directory and not os.path.exists(directory):
                try:
                    os.makedirs(directory, exist_ok=True)
                    logger.debug(f"디렉토리 생성: {directory}")
                except Exception as e:
                    logger.warning(f"디렉토리 생성 실패: {directory}, 오류: {str(e)}")

    def _get_full_path(self, path_key: str, default_path: str = None) -> str:
        """
        경로 설정값을 가져와서 절대 경로로 변환
        
        Args:
            path_key: 경로 키 (예: "logs_dir")
            default_path: 기본 경로
        
        Returns:
            str: 절대 경로
        """
        # paths 섹션에서 경로 가져오기
        path = self.get_system(f"paths.{path_key}", default_path)
        
        if not path:
            return None
        
        # 이미 절대 경로인 경우
        if os.path.isabs(path):
            return path
        
        # 상대 경로인 경우 프로젝트 루트에 결합
        return os.path.join(self.project_root, path)

    def get_path(self, path_key: str) -> str:
        """
        지정된 경로 키에 대한 전체 경로 반환 (공개 메서드)
        
        Args:
            path_key: 경로 키 이름 (예: "logs_dir", "data_dir", "backup_dir" 등)
            
        Returns:
            str: 해당 경로의 절대 경로
        """
        # 이미 초기화된 경로 속성 사용
        if hasattr(self, path_key):
            return getattr(self, path_key)
        
        # 없는 경우 시스템 설정에서 찾아서 절대 경로로 변환
        return self._get_full_path(path_key)

# 모듈 레벨 편의 함수
def get_config() -> 'AppConfig':
    """
    설정 관리자 싱글톤 인스턴스 가져오기
    
    Returns:
        AppConfig: 설정 관리자 인스턴스
    """
    try:
        return AppConfig.get_instance()
    except Exception as e:
        logger.error(f"설정 관리자 인스턴스 가져오기 실패: {str(e)}")
        raise