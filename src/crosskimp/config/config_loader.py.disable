"""
설정 파일 로더 모듈

이 모듈은 프로그램의 설정 파일들을 로드하고 관리하는 기능을 제공합니다.
주요 기능:
1. settings.json 파일에서 프로그램 설정 로드
2. 환경 변수(.env)에서 API 키 정보 로드
3. 설정값 유효성 검증
4. 기본값 제공
5. 에러 처리 및 로깅
6. 실시간 설정 업데이트
7. 웹 인터페이스 연동

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
from dotenv import load_dotenv
import threading

# 경로 관련 상수는 paths.py에서 직접 임포트
from crosskimp.config.paths import PROJECT_ROOT, CONFIG_DIR

from crosskimp.logger.logger import get_unified_logger
from crosskimp.config.constants import LOG_SYSTEM, SETTINGS_FILE, BACKUP_DIR, SETTINGS_PATH, LOAD_TIMEOUT, SAVE_TIMEOUT, RETRY_DELAY, MAX_RETRIES, ENV_FILE_PATHS, API_ENV_VARS


# ============================
# 로깅 설정
# ============================
logger = get_unified_logger()  # 로거 인스턴스 초기화

# ============================
# 상수 정의
# ============================
# 상수들은 constants.py로 이동했습니다.

# 환경 변수 로드
for env_path in ENV_FILE_PATHS:
    if os.path.exists(env_path):
        logger.info(f"{LOG_SYSTEM} .env 파일 로드: {env_path}")
        load_dotenv(env_path)
        break
else:
    logger.warning(f"{LOG_SYSTEM} .env 파일을 찾을 수 없습니다. 환경 변수를 사용합니다.")
    load_dotenv()

# ============================
# 클래스 정의
# ============================
class ConfigError(Exception):
    """설정 관련 기본 예외 클래스"""
    pass

class ValidationError(ConfigError):
    """설정 유효성 검증 실패 예외"""
    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.details = details or {}

class LoadError(ConfigError):
    """설정 로드 실패 예외"""
    pass

class SaveError(ConfigError):
    """설정 저장 실패 예외"""
    pass

class ConfigManager:
    """설정 파일 관리 클래스"""

    def __init__(self):
        self._settings = {}
        self._api_keys = {}
        self._observers = []
        self._observer = None
        self._event_handler = None
        self._initialized = False
        self._file_watching_started = False
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """설정 초기화"""
        if self._initialized:
            return

        try:
            # 설정 파일 로드
            logger.debug(f"{LOG_SYSTEM} 설정 파일 로드 시작")
            self._settings = await self.load_settings()
            logger.debug(f"{LOG_SYSTEM} 설정 파일 로드 완료")
            
            # API 키 로드
            logger.debug(f"{LOG_SYSTEM} API 키 로드 시작")
            self._api_keys = await self.load_api_keys()
            logger.debug(f"{LOG_SYSTEM} API 키 로드 완료")
            
            # 초기화 완료 표시
            self._initialized = True
            logger.info(f"{LOG_SYSTEM} 설정 초기화 완료")
            
            # 파일 감시 시작 (비동기적으로)
            asyncio.create_task(self._start_file_watching_async())
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 설정 초기화 실패: {str(e)}", exc_info=True)
            raise

    async def _start_file_watching_async(self) -> None:
        """파일 변경 감지를 비동기적으로 시작"""
        try:
            # 최소한의 지연만 사용
            await asyncio.sleep(0.5)
            self._start_file_watching()
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 파일 감시 시작 실패: {str(e)}", exc_info=True)

    def _start_file_watching(self) -> None:
        """설정 파일 변경 감지 시작"""
        if self._file_watching_started:
            return
            
        try:
            # 이벤트 핸들러 및 Observer 설정
            self._event_handler = ConfigFileHandler(self)
            self._observer = Observer()
            self._observer.daemon = True
            
            # 설정 파일만 감시
            settings_path = os.path.join(CONFIG_DIR, SETTINGS_FILE)
            self._observer.schedule(self._event_handler, path=os.path.dirname(settings_path), recursive=False)
            
            # Observer 시작
            self._observer.start()
            self._file_watching_started = True
            logger.info(f"{LOG_SYSTEM} 설정 파일 감시 시작")
            
        except Exception as e:
            self._file_watching_started = False
            logger.error(f"{LOG_SYSTEM} 설정 파일 감시 시작 실패: {str(e)}", exc_info=True)

    async def load_settings(self, retries: int = MAX_RETRIES) -> Dict:
        """설정 파일 로드"""
        for attempt in range(retries + 1):
            try:
                async with self._lock:
                    if not os.path.exists(SETTINGS_PATH):
                        logger.error(f"{LOG_SYSTEM} 설정 파일 없음: {SETTINGS_PATH}")
                        raise LoadError(f"설정 파일 없음: {SETTINGS_PATH}")
                    
                    # 파일 읽기 최적화 - 비동기 파일 읽기 사용
                    async with aiofiles.open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
                        content = await f.read()
                    
                    settings = json.loads(content)
                    
                    # 설정 유효성 검증 - 필수 검증만 수행
                    self._validate_settings_minimal(settings, True)
                    
                    # 기본값 적용
                    settings = self._apply_defaults(settings)
                    
                    logger.info(f"{LOG_SYSTEM} 설정 파일 로드 완료")
                    return settings
                    
            except json.JSONDecodeError as e:
                if attempt < retries:
                    logger.warning(
                        f"{LOG_SYSTEM} 설정 파일 JSON 파싱 실패 (재시도 {attempt+1}/{retries}): {e}"
                    )
                else:
                    logger.error(
                        f"{LOG_SYSTEM} 설정 파일 JSON 파싱 실패: {e}",
                        exc_info=True
                    )
                    raise LoadError(f"설정 파일 JSON 파싱 실패: {e}")
                    
            except Exception as e:
                if attempt < retries:
                    logger.warning(
                        f"{LOG_SYSTEM} 설정 파일 로드 실패 (재시도 {attempt+1}/{retries}): {e}"
                    )
                else:
                    logger.error(
                        f"{LOG_SYSTEM} 설정 파일 로드 실패: {e}",
                        exc_info=True
                    )
                    raise LoadError(f"설정 파일 로드 실패: {e}")
                await asyncio.sleep(RETRY_DELAY)

    async def load_api_keys(self) -> Dict:
        """
        환경 변수에서 API 키 로드
        
        Returns:
            Dict: API 키 정보
        """
        api_keys = {}
        errors = []

        # 텔레그램 설정은 bot_constants.py에서 직접 관리하므로 여기서는 처리하지 않음
        
        # 보안 설정 (선택)
        exchange = "security"
        env_vars = API_ENV_VARS[exchange]
        access_token_secret = os.getenv(env_vars["access_token_secret"])
        if access_token_secret:
            api_keys[exchange] = {
                "access_token_secret": access_token_secret
            }
        
        # 사용자 설정 (선택)
        exchange = "user"
        env_vars = API_ENV_VARS[exchange]
        first_superuser = os.getenv(env_vars["first_superuser"])
        first_superuser_password = os.getenv(env_vars["first_superuser_password"])
        if all([first_superuser, first_superuser_password]):
            api_keys[exchange] = {
                "first_superuser": first_superuser,
                "first_superuser_password": first_superuser_password
            }
        
        # 나머지 거래소 API 키는 백그라운드에서 비동기적으로 로드
        asyncio.create_task(self._load_exchange_api_keys(api_keys))

        if errors:
            error_msg = ", ".join(errors)
            logger.error(f"{LOG_SYSTEM} 필수 API 키 로드 실패 | errors={error_msg}")
            raise LoadError(f"API 키 로드 실패: {error_msg}")
            
        return api_keys

    async def _load_exchange_api_keys(self, api_keys: Dict) -> None:
        """거래소 API 키 비동기 로드 (백그라운드)"""
        try:
            # 일반 거래소 API 키 로드 (선택적)
            # 한 번에 모든 환경 변수를 가져와서 처리 (최적화)
            for exchange, env_vars in API_ENV_VARS.items():
                # 이미 처리된 특수 키는 건너뜀
                if exchange in ["security", "user"]:
                    continue
                    
                api_key = os.getenv(env_vars["api_key"])
                api_secret = os.getenv(env_vars["api_secret"])
                
                if all([api_key, api_secret]):
                    api_keys[exchange] = {
                        "api_key": api_key,
                        "api_secret": api_secret
                    }
                else:
                    # 로그 레벨을 warning에서 debug로 변경하여 불필요한 로깅 줄임
                    logger.debug(f"{LOG_SYSTEM} {exchange.title()} API 키 설정 누락")
                    
            logger.debug(f"{LOG_SYSTEM} 모든 거래소 API 키 로드 완료")
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 거래소 API 키 로드 중 오류: {str(e)}", exc_info=True)

    def _validate_settings_minimal(self, settings: Dict, detailed: bool = False) -> None:
        """
        설정값 유효성 검증
        
        Args:
            settings: 검증할 설정 데이터
            detailed: 상세 검증 여부
        """
        errors = []
        
        # 필수 섹션 확인
        required_sections = ["exchanges", "trading", "monitoring", "notifications"]
        for section in required_sections:
            if section not in settings:
                error_msg = f"필수 섹션 '{section}' 누락"
                errors.append(error_msg)
                logger.error(f"{LOG_SYSTEM} {error_msg}")
        
        # 상세 검증이 필요한 경우
        if detailed and not errors:
            # 거래소 설정 확인
            if "exchanges" in settings:
                exchanges = settings["exchanges"]
                if "spot" not in exchanges:
                    errors.append("'exchanges.spot' 섹션 누락")
                if "future" not in exchanges:
                    errors.append("'exchanges.future' 섹션 누락")
            
            # 트레이딩 설정 확인
            if "trading" in settings and "general" in settings["trading"]:
                general = settings["trading"]["general"]
                for field in ["base_order_amount_krw", "min_volume_krw", "interval_sec"]:
                    if field not in general:
                        errors.append(f"'trading.general.{field}' 필드 누락")
            
            # 알림 설정 확인
            if "notifications" in settings:
                notifications = settings["notifications"]
                if "telegram" in notifications:
                    telegram = notifications["telegram"]
                    if "enabled" not in telegram:
                        errors.append("'notifications.telegram.enabled' 필드 누락")
                    if "alert_types" not in telegram:
                        errors.append("'notifications.telegram.alert_types' 필드 누락")
        
        # 오류가 있으면 예외 발생
        if errors:
            error_msg = ", ".join(errors)
            logger.error(f"{LOG_SYSTEM} 설정 파일 유효성 검증 실패: {error_msg}")
            raise ValidationError("설정 파일 유효성 검증 실패", {"errors": errors})
        
        logger.debug(f"{LOG_SYSTEM} 설정 파일 검증 완료")
        return

    def _validate_settings(self, settings: Dict) -> None:
        """설정값 유효성 검증"""
        # 통합된 _validate_settings_minimal 메서드를 사용
        self._validate_settings_minimal(settings, True)

    def _validate_api_keys(self, api_keys: Dict) -> None:
        """API 키 유효성 검증"""
        errors = []
        
        # 거래소 API 키 검증 (선택적)
        for exchange in ["binance", "bybit", "upbit", "bithumb"]:
            if exchange not in api_keys:
                logger.debug(f"{LOG_SYSTEM} 거래소 '{exchange}' API 키 설정 없음")
                continue
            
            if not all(api_keys[exchange].get(k) for k in ["api_key", "api_secret"]):
                logger.warning(f"{LOG_SYSTEM} '{exchange}' API 키 또는 시크릿 누락")
        
        # 오류가 있으면 예외 발생
        if errors:
            error_msg = ", ".join(errors)
            logger.error(f"{LOG_SYSTEM} API 키 유효성 검증 실패: {error_msg}")
            raise ValidationError("API 키 유효성 검증 실패", {"errors": errors})
        
        logger.debug(f"{LOG_SYSTEM} API 키 검증 완료")
        return

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
            },
            "notifications": {
                "telegram": {
                    "enabled": True,
                    "alert_types": ["error", "trade", "profit"]
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
            self._validate_settings_minimal(new_settings)
            
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
                
                logger.info(f"{LOG_SYSTEM} 설정 파일 저장 완료")
                
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
                    
            logger.info(f"{LOG_SYSTEM} 백업 파일 생성: {backup_path}")
            
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 백업 파일 생성 실패: {e}")

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
                logger.error(f"{LOG_SYSTEM} 옵저버 알림 실패: {e}")

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
        """설정 시스템 종료"""
        try:
            logger.debug(f"{LOG_SYSTEM} 설정 시스템 종료 시작")
            
            # 파일 감시 중지
            if self._file_watching_started and self._observer:
                self._observer.stop()
                self._observer.join(timeout=1.0)  # 최대 1초 대기
                self._observer = None
                self._file_watching_started = False
            
            logger.info(f"{LOG_SYSTEM} 설정 시스템 종료됨")
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 설정 시스템 종료 중 오류: {str(e)}", exc_info=True)

    async def reload_settings(self) -> None:
        """설정 파일 다시 로드"""
        try:
            new_settings = await self.load_settings()
            async with self._lock:
                self._settings = new_settings
            await self._notify_observers()
            logger.info(f"{LOG_SYSTEM} 설정 파일이 다시 로드되었습니다.")
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 설정 파일 리로드 중 오류: {e}")
            raise

# ============================
# 파일 변경 감시 핸들러
# ============================
class ConfigFileHandler(FileSystemEventHandler):
    def __init__(self, config_manager):
        """초기화"""
        logger.debug(f"{LOG_SYSTEM} 설정 파일 핸들러 초기화")
        self.config_manager = config_manager
        self.loop = asyncio.get_event_loop()
        self._last_modified = 0
        self._cooldown = 1.0  # 1초 쿨다운

    def on_modified(self, event):
        """파일 변경 감지 이벤트 핸들러"""
        try:
            # 디렉토리 변경은 무시
            if event.is_directory:
                return
                
            # 설정 파일이 아닌 경우 무시
            if not event.src_path.endswith(SETTINGS_FILE):
                return
                
            logger.debug(f"{LOG_SYSTEM} 설정 파일 변경 감지: {event.src_path}")
                
            # 쿨다운 체크
            current_time = time.time()
            if current_time - self._last_modified < self._cooldown:
                logger.debug(f"{LOG_SYSTEM} 쿨다운 기간 내 변경 무시")
                return
                
            self._last_modified = current_time
            
            # 비동기 처리를 위한 태스크 생성
            if self.loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    self._handle_settings_change(),
                    self.loop
                )
            else:
                asyncio.create_task(self._handle_settings_change())
                
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 파일 변경 이벤트 처리 중 오류: {str(e)}", exc_info=True)

    async def _handle_settings_change(self):
        """설정 파일 변경 처리"""
        try:
            logger.debug(f"{LOG_SYSTEM} 설정 파일 변경 처리 시작")
            
            # 설정 다시 로드
            await self.config_manager.reload_settings()
            
            logger.info(f"{LOG_SYSTEM} 설정 파일이 다시 로드되었습니다.")
        except Exception as e:
            logger.error(f"{LOG_SYSTEM} 설정 리로드 중 오류: {e}")

# ============================
# 전역 인스턴스
# ============================
config_manager = ConfigManager()

# ============================
# 편의 함수
# ============================
async def initialize_config() -> None:
    """설정 시스템 초기화"""
    # 이미 초기화되었는지 확인
    if config_manager._initialized:
        logger.debug(f"{LOG_SYSTEM} 설정 시스템이 이미 초기화되었습니다.")
        return
        
    try:
        logger.debug(f"{LOG_SYSTEM} 설정 시스템 초기화 시작")
        
        # 설정 초기화 수행
        await config_manager.initialize()
        
        logger.debug(f"{LOG_SYSTEM} 설정 시스템 초기화 완료")
        
        # 설정 반환 (비동기 작업은 백그라운드에서 계속 진행)
        return config_manager.get_settings()
    except Exception as e:
        logger.error(f"{LOG_SYSTEM} 설정 초기화 실패: {str(e)}", exc_info=True)
        raise

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