# file: config/config_loader.py

"""
설정 파일 로더 모듈

이 모듈은 프로그램의 설정 파일들을 로드하고 관리하는 기능을 제공합니다.
주요 기능:
1. settings.json 파일에서 프로그램 설정 로드
2. api_keys.json 파일에서 API 키 정보 로드
3. 설정값 유효성 검증
4. 기본값 제공
5. 에러 처리 및 로깅
6. 실시간 설정 업데이트
7. 웹 인터페이스 연동

작성자: CrossKimp Arbitrage Bot 개발팀
최종수정: 2024.02
"""

import json
import os
import time
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
import aiofiles
import aiofiles.os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from utils.logging.logger import unified_logger
from dotenv import load_dotenv

# ============================
# 로깅 메시지 상수
# ============================
LOG_SYSTEM = "[시스템]"  # 기존 [Config] 대체

# ============================
# 상수 정의
# ============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = BASE_DIR
SETTINGS_FILE = "settings.json"
BACKUP_DIR = os.path.join(BASE_DIR, "backups")

# 설정 파일 전체 경로
SETTINGS_PATH = os.path.join(CONFIG_DIR, SETTINGS_FILE)

# API 키 환경 변수 이름
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
    "telegram": {
        "bot_token": "TELEGRAM_BOT_TOKEN",
        "command_bot_token": "TELEGRAM_COMMAND_BOT_TOKEN",
        "chat_id": "TELEGRAM_CHAT_ID"
    }
}

# 설정 파일 전체 경로
SETTINGS_PATH = os.path.join(CONFIG_DIR, SETTINGS_FILE)
API_KEYS_FILE = "api_keys.json"
API_KEYS_PATH = os.path.join(CONFIG_DIR, API_KEYS_FILE)

# 타임아웃 설정
LOAD_TIMEOUT = 10  # 초
SAVE_TIMEOUT = 5   # 초
RETRY_DELAY = 1    # 초
MAX_RETRIES = 3    # 최대 재시도 횟수

# 필수 설정 스키마 정의
SETTINGS_SCHEMA = {
    "version": {
        "required_fields": [],
        "types": {
            "": str
        }
    },
    "last_updated": {
        "required_fields": [],
        "types": {
            "": str
        }
    },
    "exchanges": {
        "required_fields": ["spot", "future"],
        "types": {
            "spot": dict,
            "future": dict,
            "spot.binance": dict,
            "spot.bybit": dict,
            "spot.upbit": dict,
            "spot.bithumb": dict,
            "future.binance": dict,
            "future.bybit": dict
        }
    },
    "trading": {
        "required_fields": [
            "general.base_order_amount_krw",
            "general.min_volume_krw",
            "general.interval_sec"
        ],
        "types": {
            "general": dict,
            "general.base_order_amount_krw": (int, float),
            "general.min_volume_krw": (int, float),
            "general.interval_sec": (int, float),
            "risk": dict,
            "excluded_symbols": dict
        }
    },
    "connection": {
        "required_fields": [
            "websocket.delay_threshold_ms",
            "websocket.alert_cooldown_sec",
            "websocket.max_retries",
            "websocket.retry_delay_sec",
            "websocket.ping_interval_sec"
        ],
        "types": {
            "websocket": dict,
            "websocket.delay_threshold_ms": (int, float),
            "websocket.alert_cooldown_sec": (int, float),
            "websocket.max_retries": int,
            "websocket.retry_delay_sec": (int, float),
            "websocket.ping_interval_sec": (int, float),
            "rest_api": dict
        }
    },
    "monitoring": {
        "required_fields": [
            "port",
            "host",
            "update_interval_ms",
            "metrics"
        ],
        "types": {
            "port": int,
            "host": str,
            "update_interval_ms": int,
            "metrics": dict
        }
    },
    "notifications": {
        "required_fields": [
            "telegram.enabled",
            "telegram.token",
            "telegram.chat_id"
        ],
        "types": {
            "telegram": dict,
            "telegram.enabled": bool,
            "telegram.token": str,
            "telegram.chat_id": str,
            "discord": dict
        }
    },
    "database": {
        "required_fields": ["enabled"],
        "types": {
            "enabled": bool,
            "type": str,
            "path": str,
            "tables": dict
        }
    }
}

# ============================
# 예외 클래스 정의
# ============================
class ConfigError(Exception):
    """설정 관련 기본 예외"""
    pass

class ValidationError(ConfigError):
    """설정 유효성 검증 실패"""
    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.details = details or {}

class LoadError(ConfigError):
    """설정 파일 로드 실패"""
    pass

class SaveError(ConfigError):
    """설정 파일 저장 실패"""
    pass

# ============================
# 설정 관리 클래스
# ============================
class ConfigManager:
    """설정 파일 관리 클래스"""
    
    def __init__(self):
        self._settings: Dict = {}
        self._api_keys: Dict = {}
        self._observers: List = []  # 설정 변경 옵저버
        self._lock = asyncio.Lock()
        self._file_observer = None
        self._last_update = 0
        
        # 디렉토리 생성
        os.makedirs(CONFIG_DIR, exist_ok=True)
        os.makedirs(BACKUP_DIR, exist_ok=True)
        
        # .env 파일 로드
        load_dotenv()
        
    async def initialize(self) -> None:
        """초기 설정 로드 및 파일 감시 시작"""
        try:
            unified_logger.info(f"{LOG_SYSTEM} 설정 시스템 초기화 시작")
            
            # 설정 파일 로드
            settings = await self.load_settings()
            api_keys = await self.load_api_keys()
            
            unified_logger.info(
                f"{LOG_SYSTEM} 설정 로드 완료 | "
                f"settings_size={len(settings)}, "
                f"api_keys_size={len(api_keys)}"
            )
            
            # 파일 변경 감시 시작
            self._start_file_watching()
            
            unified_logger.info(f"{LOG_SYSTEM} 설정 시스템 초기화 완료")
            
        except Exception as e:
            unified_logger.error(
                f"{LOG_SYSTEM} 초기화 실패 | error={str(e)}",
                exc_info=True
            )
            raise

    def _start_file_watching(self) -> None:
        """설정 파일 변경 감시 시작"""
        event_handler = ConfigFileHandler(self)
        self._file_observer = Observer()
        self._file_observer.schedule(event_handler, CONFIG_DIR, recursive=False)
        self._file_observer.start()
        unified_logger.debug(f"{LOG_SYSTEM} 설정 파일 감시 시작")

    async def load_settings(self, retries: int = MAX_RETRIES) -> Dict:
        """settings.json 파일 로드"""
        for attempt in range(retries):
            try:
                if attempt > 0:
                    unified_logger.warning(
                        f"{LOG_SYSTEM} 설정 파일 재시도 | "
                        f"attempt={attempt + 1}/{retries}"
                    )
                    
                async with self._lock:
                    async with aiofiles.open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
                        content = await f.read()
                        settings = json.loads(content)
                        
                    # 유효성 검증
                    self._validate_settings(settings)
                    
                    # 기본값 적용
                    settings = self._apply_defaults(settings)
                    
                    self._settings = settings
                    self._last_update = time.time()
                    
                    unified_logger.info(
                        f"{LOG_SYSTEM} 설정 파일 로드 완료 | "
                        f"sections={list(settings.keys())}"
                    )
                    return settings
                    
            except json.JSONDecodeError as e:
                unified_logger.error(
                    f"{LOG_SYSTEM} 설정 파일 JSON 파싱 실패 | "
                    f"error={str(e)}, line={e.lineno}, col={e.colno}",
                    exc_info=True
                )
                if attempt == retries - 1:
                    raise LoadError(f"설정 파일 JSON 파싱 실패: {e}")
            except Exception as e:
                if attempt == retries - 1:
                    unified_logger.error(
                        f"{LOG_SYSTEM} 설정 파일 로드 실패 | error={str(e)}",
                        exc_info=True
                    )
                    raise LoadError(f"설정 파일 로드 실패: {e}")
                await asyncio.sleep(RETRY_DELAY)

    async def load_api_keys(self) -> Dict:
        """환경 변수에서 API 키 로드"""
        api_keys = {}
        errors = []

        # 거래소 API 키 로드
        for exchange, env_vars in API_ENV_VARS.items():
            api_keys[exchange] = {}
            
            if exchange == "telegram":
                # 텔레그램은 특별 처리 (토큰 2개)
                bot_token = os.getenv(env_vars["bot_token"])
                command_bot_token = os.getenv(env_vars["command_bot_token"])
                chat_id = os.getenv(env_vars["chat_id"])
                
                if not all([bot_token, command_bot_token, chat_id]):
                    errors.append(f"Telegram API 설정 누락")
                else:
                    api_keys[exchange] = {
                        "bot_token": bot_token,
                        "command_bot_token": command_bot_token,
                        "chat_id": chat_id
                    }
            else:
                # 일반 거래소 API 키 로드
                api_key = os.getenv(env_vars["api_key"])
                api_secret = os.getenv(env_vars["api_secret"])
                
                if not all([api_key, api_secret]):
                    errors.append(f"{exchange.title()} API 키 설정 누락")
                else:
                    api_keys[exchange] = {
                        "api_key": api_key,
                        "api_secret": api_secret
                    }

        if errors:
            error_msg = ", ".join(errors)
            unified_logger.error(f"{LOG_SYSTEM} API 키 로드 실패 | errors={error_msg}")
            raise LoadError(f"API 키 로드 실패: {error_msg}")

        self._api_keys = api_keys
        unified_logger.info(f"{LOG_SYSTEM} API 키 로드 완료")
        return api_keys

    def _validate_settings(self, settings: Dict) -> None:
        """설정값 유효성 검증"""
        errors = []
        unified_logger.debug(f"{LOG_SYSTEM} 설정 파일 검증 시작")
        
        def get_nested_value(data: dict, path: str):
            """중첩된 딕셔너리에서 값 가져오기"""
            try:
                current = data
                for key in path.split('.'):
                    current = current[key]
                return current
            except (KeyError, TypeError):
                unified_logger.warning(f"{LOG_SYSTEM} 필수 필드 누락: {path}")
                return None

        # 1. 필수 섹션 및 필드 검사
        for section, schema in SETTINGS_SCHEMA.items():
            if section not in settings:
                error_msg = f"필수 섹션 '{section}' 누락"
                errors.append(error_msg)
                unified_logger.error(f"{LOG_SYSTEM} {error_msg}")
                continue
                
            section_data = settings[section]
            
            # 필수 필드 검사
            for field in schema["required_fields"]:
                value = get_nested_value(section_data, field)
                if value is None:
                    error_msg = f"필수 필드 '{section}.{field}' 누락"
                    errors.append(error_msg)
                    unified_logger.error(f"{LOG_SYSTEM} {error_msg}")
                    continue
                
                # 타입 검사
                field_type = schema["types"].get(field)
                if field_type:
                    if isinstance(field_type, tuple):
                        if not isinstance(value, field_type):
                            error_msg = (
                                f"필드 '{section}.{field}'의 타입이 잘못됨 "
                                f"(예상: {field_type}, 실제: {type(value)})"
                            )
                            errors.append(error_msg)
                            unified_logger.error(f"{LOG_SYSTEM} {error_msg}")
                    else:
                        if not isinstance(value, field_type):
                            error_msg = (
                                f"필드 '{section}.{field}'의 타입이 잘못됨 "
                                f"(예상: {field_type}, 실제: {type(value)})"
                            )
                            errors.append(error_msg)
                            unified_logger.error(f"{LOG_SYSTEM} {error_msg}")
            
        if errors:
            unified_logger.error(
                f"{LOG_SYSTEM} 설정 파일 검증 실패 | "
                f"errors={json.dumps(errors, ensure_ascii=False)}"
            )
            raise ValidationError("설정 파일 검증 실패", {"errors": errors})
            
        unified_logger.debug(f"{LOG_SYSTEM} 설정 파일 검증 완료")

    def _validate_api_keys(self, api_keys: Dict) -> None:
        """API 키 유효성 검증"""
        errors = []
        
        # 1. 거래소 API 키 검증
        for exchange in ["binance", "bybit", "upbit", "bithumb"]:
            if exchange not in api_keys:
                errors.append(f"필수 거래소 '{exchange}' API 키 누락")
                continue
            
            if not all(api_keys[exchange].get(k) for k in ["api_key", "api_secret"]):
                errors.append(f"'{exchange}' API 키 또는 시크릿 누락")
        
        # 2. 텔레그램 설정 검증
        if "telegram" not in api_keys:
            errors.append("Telegram 설정 누락")
        else:
            telegram_keys = api_keys["telegram"]
            required_fields = ["bot_token", "command_bot_token", "chat_id"]
            if not all(telegram_keys.get(k) for k in required_fields):
                errors.append("Telegram 토큰 또는 채팅 ID 누락")
                    
        if errors:
            raise ValidationError("API 키 검증 실패", {"errors": errors})

    def _apply_defaults(self, settings: Dict) -> Dict:
        """기본값 적용"""
        defaults = {
            "trading": {
                "general": {
                    "base_order_amount_krw": 10_000_000,
                    "min_volume_krw": 100_000_000_000,
                    "interval_sec": 1.0
                }
            },
            "connection": {
                "websocket": {
                    "max_retries": 5,
                    "retry_delay_sec": 3,
                    "ping_interval_sec": 30
                }
            }
        }
        
        def deep_update(source: Dict, updates: Dict) -> Dict:
            for key, value in updates.items():
                if key not in source:
                    source[key] = value
                elif isinstance(source[key], dict) and isinstance(value, dict):
                    deep_update(source[key], value)
            return source
            
        return deep_update(settings, defaults)

    async def save_settings(self, new_settings: Dict, backup: bool = True) -> None:
        """
        설정 파일 저장
        
        Args:
            new_settings: 새로운 설정 데이터
            backup: 백업 파일 생성 여부
        """
        try:
            # 유효성 검증
            self._validate_settings(new_settings)
            
            async with self._lock:
                # 백업 생성
                if backup:
                    await self._create_backup(SETTINGS_PATH)
                
                # 새 설정 저장
                async with aiofiles.open(SETTINGS_PATH, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(new_settings, indent=2, ensure_ascii=False))
                
                self._settings = new_settings
                self._last_update = time.time()
                
                # 옵저버 알림
                await self._notify_observers()
                
                unified_logger.info(f"{LOG_SYSTEM} 설정 파일 저장 완료")
                
        except Exception as e:
            raise SaveError(f"설정 파일 저장 실패: {e}")

    async def _create_backup(self, filepath: str) -> None:
        """설정 파일 백업 생성"""
        try:
            filename = os.path.basename(filepath)
            backup_name = f"{filename}.{datetime.now().strftime('%y%m%d_%H%M%S')}.bak"
            backup_path = os.path.join(BACKUP_DIR, backup_name)
            
            async with aiofiles.open(filepath, 'r', encoding='utf-8') as src:
                content = await src.read()
                async with aiofiles.open(backup_path, 'w', encoding='utf-8') as dst:
                    await dst.write(content)
                    
            unified_logger.info(f"{LOG_SYSTEM} 백업 파일 생성: {backup_path}")
            
        except Exception as e:
            unified_logger.error(f"{LOG_SYSTEM} 백업 파일 생성 실패: {e}")

    def add_observer(self, callback) -> None:
        """설정 변경 옵저버 등록"""
        if callback not in self._observers:
            self._observers.append(callback)

    def remove_observer(self, callback) -> None:
        """설정 변경 옵저버 제거"""
        if callback in self._observers:
            self._observers.remove(callback)

    async def _notify_observers(self) -> None:
        """설정 변경 옵저버 알림"""
        for observer in self._observers:
            try:
                if asyncio.iscoroutinefunction(observer):
                    await observer(self._settings)
                else:
                    observer(self._settings)
            except Exception as e:
                unified_logger.error(f"{LOG_SYSTEM} 옵저버 알림 실패: {e}")

    def get_settings(self) -> Dict:
        """현재 설정값 조회"""
        return self._settings.copy()

    def get_api_keys(self) -> Dict:
        """현재 API 키 조회"""
        return self._api_keys.copy()

    async def update_section(self, section: str, data: Dict) -> None:
        """
        설정 섹션 업데이트
        
        Args:
            section: 섹션 이름 (예: "trading", "connection" 등)
            data: 업데이트할 데이터
        """
        try:
            settings = self.get_settings()
            if section not in settings:
                raise ValueError(f"존재하지 않는 섹션: {section}")
                
            # 섹션 데이터 업데이트
            settings[section] = data
            
            # 저장
            await self.save_settings(settings)
            
        except Exception as e:
            raise SaveError(f"섹션 업데이트 실패: {e}")

    def shutdown(self) -> None:
        """설정 관리자 종료"""
        if self._file_observer:
            self._file_observer.stop()
            self._file_observer.join()
        unified_logger.info(f"{LOG_SYSTEM} 설정 관리자 종료")

# ============================
# 파일 변경 감시 핸들러
# ============================
class ConfigFileHandler(FileSystemEventHandler):
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.loop = asyncio.get_event_loop()
        self._last_modified = 0
        self._cooldown = 1  # 1초 쿨다운

    def on_modified(self, event):
        if not event.is_directory:
            current_time = time.time()
            if current_time - self._last_modified < self._cooldown:
                return
            self._last_modified = current_time

            if event.src_path.endswith('api_keys.json'):
                try:
                    # 메인 스레드의 이벤트 루프에 태스크 추가
                    if self.loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            self._handle_api_keys_change(),
                            self.loop
                        )
                except Exception as e:
                    unified_logger.error(f"{LOG_SYSTEM} 설정 파일 변경 처리 중 오류: {e}")

            elif event.src_path.endswith('settings.json'):
                try:
                    if self.loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            self._handle_settings_change(),
                            self.loop
                        )
                except Exception as e:
                    unified_logger.error(f"{LOG_SYSTEM} 설정 파일 변경 처리 중 오류: {e}")

    async def _handle_api_keys_change(self):
        """API 키 파일 변경 처리"""
        try:
            await self.config_manager.reload_api_keys()
            unified_logger.info(f"{LOG_SYSTEM} API 키 설정이 다시 로드되었습니다.")
        except Exception as e:
            unified_logger.error(f"{LOG_SYSTEM} API 키 설정 리로드 중 오류: {e}")

    async def _handle_settings_change(self):
        """설정 파일 변경 처리"""
        try:
            await self.config_manager.reload_settings()
            unified_logger.info(f"{LOG_SYSTEM} 일반 설정이 다시 로드되었습니다.")
        except Exception as e:
            unified_logger.error(f"{LOG_SYSTEM} 설정 리로드 중 오류: {e}")

# ============================
# 전역 인스턴스
# ============================
config_manager = ConfigManager()

# ============================
# 편의 함수
# ============================
async def initialize_config() -> None:
    """설정 시스템 초기화"""
    await config_manager.initialize()

def get_settings() -> Dict:
    """현재 설정값 조회"""
    return config_manager.get_settings()

def get_api_keys() -> Dict:
    """현재 API 키 조회"""
    return config_manager.get_api_keys()

async def update_settings_section(section: str, data: Dict) -> None:
    """설정 섹션 업데이트"""
    await config_manager.update_section(section, data)

def add_config_observer(callback) -> None:
    """설정 변경 옵저버 등록"""
    config_manager.add_observer(callback)

def remove_config_observer(callback) -> None:
    """설정 변경 옵저버 제거"""
    config_manager.remove_observer(callback)

def shutdown_config() -> None:
    """설정 시스템 종료"""
    config_manager.shutdown()

# ============================
# Export
# ============================
__all__ = [
    'initialize_config',
    'get_settings',
    'get_api_keys',
    'update_settings_section',
    'add_config_observer',
    'remove_config_observer',
    'shutdown_config'
]