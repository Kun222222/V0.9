"""
환경변수 로더 모듈

.env 파일에서 환경변수를 로드하고 애플리케이션에 필요한 값을 제공합니다.
API 키와 같은 민감한 정보를 안전하게 관리합니다.
"""

import os
import logging
from typing import Dict, Optional, Any, List
from dotenv import load_dotenv

# 기본 로거 설정
logger = logging.getLogger(__name__)

class EnvLoader:
    """환경변수 로더 클래스"""
    
    # 싱글톤 인스턴스
    _instance = None
    
    # API 키 변수 정의
    API_ENV_VARS = {
        "binance": {
            "api_key": "BINANCE_API_KEY",
            "api_secret": "BINANCE_API_SECRET"
        },
        "bybit": {
            "api_key": "BYBIT_API_KEY",
            "api_secret": "BYBIT_API_SECRET"
        },
        "bithumb": {
            "api_key": "BITHUMB_API_KEY",
            "api_secret": "BITHUMB_API_SECRET"
        },
        "upbit": {
            "api_key": "UPBIT_API_KEY",
            "api_secret": "UPBIT_API_SECRET"
        },
        "security": {
            "access_token_secret": "ACCESS_TOKEN_SECRET"
        },
        "system": {
            "project_root": "PROJECT_ROOT"
        },
        "user": {
            "first_superuser": "FIRST_SUPERUSER",
            "first_superuser_password": "FIRST_SUPERUSER_PASSWORD"
        },
        "telegram": {
            "bot_token": "TELEGRAM_BOT_TOKEN",
            "chat_id": "TELEGRAM_CHAT_ID"
        }
    }
    
    @classmethod
    def get_instance(cls) -> 'EnvLoader':
        """싱글톤 인스턴스 반환"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        """초기화 - 환경변수 로드"""
        self.env_vars = {}
        self.loaded = False
        self.env_file_path = None
        
    def load(self, env_file_path: Optional[str] = None) -> bool:
        """
        환경변수 로드
        
        Args:
            env_file_path: .env 파일 경로 (기본값: 프로젝트 루트의 .env)
            
        Returns:
            bool: 로드 성공 여부
        """
        if self.loaded:
            logger.debug("환경변수가 이미 로드되었습니다.")
            return True
            
        # 환경변수 파일 경로 설정
        if env_file_path:
            self.env_file_path = env_file_path
        else:
            # 환경변수 경로가 지정되지 않은 경우 현재 디렉토리의 .env 사용
            self.env_file_path = os.path.join(os.getcwd(), ".env")
            
        # 환경변수 파일 존재 확인
        if not os.path.exists(self.env_file_path):
            logger.warning(f"환경변수 파일을 찾을 수 없습니다: {self.env_file_path}")
            # 파일은 없지만 시스템 환경변수는 계속 사용
        else:
            # dotenv로 환경변수 파일 로드
            load_dotenv(self.env_file_path)
            logger.info(f"환경변수 파일 로드 완료: {self.env_file_path}")
            
        # 모든 필요한 환경변수 캐싱
        self._cache_env_vars()
        
        # 필수 환경변수 검증
        missing_vars = self._validate_required_vars()
        if missing_vars:
            logger.warning(f"필수 환경변수가 설정되지 않았습니다: {', '.join(missing_vars)}")
            
        self.loaded = True
        return True
        
    def _cache_env_vars(self):
        """모든 필요한 환경변수를 캐싱"""
        # API_ENV_VARS에 정의된 모든 변수 가져오기
        for exchange, vars_dict in self.API_ENV_VARS.items():
            self.env_vars[exchange] = {}
            for var_name, env_key in vars_dict.items():
                self.env_vars[exchange][var_name] = os.environ.get(env_key)
    
    def _validate_required_vars(self) -> List[str]:
        """필수 환경변수 검증"""
        missing_vars = []
        
        # 텔레그램 관련 변수는 필수
        for var in ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]:
            if not os.environ.get(var):
                missing_vars.append(var)
                
        # ACCESS_TOKEN_SECRET은 필수
        if not os.environ.get("ACCESS_TOKEN_SECRET"):
            missing_vars.append("ACCESS_TOKEN_SECRET")
            
        return missing_vars
    
    def get(self, key_path: str, default: Optional[Any] = None) -> Any:
        """
        환경변수 값 가져오기
        
        Args:
            key_path: 키 경로 (예: "binance.api_key")
            default: 기본값
            
        Returns:
            Any: 환경변수 값 또는 기본값
        """
        if not self.loaded:
            self.load()
            
        # 키 경로 분리
        parts = key_path.split('.')
        
        if len(parts) < 2:
            # 직접 환경변수 이름으로 시도
            return os.environ.get(key_path, default)
            
        # 내부 캐시에서 값 검색
        current = self.env_vars
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return default
                
        return current if current is not None else default
    
    def get_masked(self, key_path: str) -> str:
        """
        민감한 정보를 마스킹하여 반환 (로깅용)
        
        Args:
            key_path: 키 경로 (예: "binance.api_key")
            
        Returns:
            str: 마스킹된 값 (예: "ABCD...XYZ")
        """
        value = self.get(key_path)
        if not value:
            return "Not set"
            
        # 민감한 정보 마스킹
        if len(value) <= 8:
            return "****"
        else:
            return f"{value[:4]}...{value[-4:]}"
    
    def is_api_key_set(self, exchange: str) -> bool:
        """
        특정 거래소의 API 키가 설정되어 있는지 확인
        
        Args:
            exchange: 거래소 코드 (예: "binance")
            
        Returns:
            bool: API 키 설정 여부
        """
        api_key = self.get(f"{exchange}.api_key")
        api_secret = self.get(f"{exchange}.api_secret")
        
        return bool(api_key and api_secret)
    
    def dump_settings(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """
        설정된 모든 환경변수를 딕셔너리로 반환 (디버깅용)
        
        Args:
            include_sensitive: 민감한 정보 포함 여부
            
        Returns:
            Dict: 환경변수 딕셔너리
        """
        result = {}
        
        for exchange, vars_dict in self.API_ENV_VARS.items():
            result[exchange] = {}
            for var_name, _ in vars_dict.items():
                value = self.get(f"{exchange}.{var_name}")
                
                if value and not include_sensitive:
                    # 민감한 정보 마스킹
                    if "key" in var_name.lower() or "secret" in var_name.lower() or "token" in var_name.lower() or "password" in var_name.lower():
                        value = self.get_masked(f"{exchange}.{var_name}")
                
                result[exchange][var_name] = value
                
        return result 

# 모듈 레벨 편의 함수
def get_env_loader() -> 'EnvLoader':
    """환경변수 로더 인스턴스 반환 (편의 함수)"""
    return EnvLoader.get_instance() 