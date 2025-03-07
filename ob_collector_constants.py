#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
OB-Collector 상수 정의
모든 경로 및 상수를 중앙에서 관리합니다.
"""

import os
from pathlib import Path
from enum import Enum, auto
from typing import Dict, Optional
from datetime import datetime

# 기본 디렉토리 설정
OB_COLLECTOR_ROOT = Path(__file__).parent
PROJECT_ROOT = OB_COLLECTOR_ROOT.parent.parent.parent

class DirectoryManager:
    """디렉토리 및 파일 경로 관리 클래스"""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.ob_collector_root = OB_COLLECTOR_ROOT
        
        # 설정 관련 경로
        self.config_dir = self.ob_collector_root / "core" / "config"
        self.config_file = self.config_dir / "config_ob_collector.json"
        self.schema_dir = self.ob_collector_root / "core" / "schemas"
        self.config_schema_file = self.schema_dir / "config_schema.json"
        self.config_backup_dir = self.config_dir / "backups"
        
        # 로그 관련 경로
        self.log_dir = self.project_root / "src" / "logs"
        self.log_backup_dir = self.log_dir / "backups"
        # raw 로그 디렉토리 추가
        self.raw_log_dir = self.log_dir / "raw"
        
        # 구독 정보 관련 경로
        self.subscribe_dir = self.ob_collector_root / "subscribe"
        self.filtered_symbols_file = self.subscribe_dir / "filtered_symbols.json"
        
        # 환경 변수 파일 경로
        self.env_file = self.project_root / ".env"
        
        # 기타 설정 파일 경로
        self.schema_files = {
            'config': self.schema_dir / "config_schema.json",
            'log': self.schema_dir / "log_format.json",
            'exchange': self.schema_dir / "exchange_schema.json"
        }
        
    def get_log_path(self, filename: Optional[str] = None) -> Path:
        """로그 파일 경로 생성"""
        if filename is None:
            filename = get_log_filename()
        return self.log_dir / filename
    
    def ensure_directories(self) -> None:
        """필요한 모든 디렉토리 생성"""
        directories = [
            self.config_dir,
            self.config_backup_dir,
            self.log_dir,
            self.log_backup_dir,
            self.raw_log_dir,  # raw 로그 디렉토리 추가
            self.subscribe_dir,
            self.schema_dir
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            
        # 디렉토리 권한 설정
        try:
            self.log_dir.chmod(0o755)  # rwxr-xr-x
            self.log_backup_dir.chmod(0o755)  # rwxr-xr-x
            self.raw_log_dir.chmod(0o755)  # rwxr-xr-x
        except Exception as e:
            print(f"Warning: Could not set directory permissions: {e}")
    
    def get_backup_path(self, original_path: Path, timestamp: Optional[str] = None) -> Path:
        """백업 파일 경로 생성"""
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if str(original_path).startswith(str(self.config_dir)):
            backup_dir = self.config_backup_dir
        elif str(original_path).startswith(str(self.log_dir)):
            backup_dir = self.log_backup_dir
        else:
            backup_dir = original_path.parent / "backups"
            backup_dir.mkdir(parents=True, exist_ok=True)
            
        return backup_dir / f"{original_path.name}.{timestamp}.bak"

# 디렉토리 매니저 인스턴스 생성
DIRS = DirectoryManager(PROJECT_ROOT)

# 로그 파일명 형식 설정
def get_log_filename(prefix: str = "ob_collector") -> str:
    """타임스탬프가 포함된 로그 파일명 생성"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{prefix}_{timestamp}.log"

# 로그 관련 상수
MAX_LOG_SIZE = 50 * 1024 * 1024  # 50MB
LOG_BACKUP_COUNT = 5
DEFAULT_LOG_FORMAT = "[%(asctime)s] - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s"

# 거래소 코드
class Exchange(Enum):
    """거래소 코드"""
    UPBIT = "upbit"
    BITHUMB = "bithumb"
    BINANCE = "binance"
    BINANCE_FUTURES = "binance_futures"
    BYBIT = "bybit"
    BYBIT_FUTURES = "bybit_futures"

# 거래소 한글 이름
EXCHANGE_NAMES_KR: Dict[str, str] = {
    Exchange.UPBIT.value: "업비트",
    Exchange.BITHUMB.value: "빗썸",
    Exchange.BINANCE.value: "바이낸스",
    Exchange.BINANCE_FUTURES.value: "바이낸스 선물",
    Exchange.BYBIT.value: "바이빗",
    Exchange.BYBIT_FUTURES.value: "바이빗 선물"
}

# 거래소 이모지
EXCHANGE_EMOJIS: Dict[str, str] = {
    Exchange.UPBIT.value: "🇰🇷",
    Exchange.BITHUMB.value: "🇰🇷",
    Exchange.BINANCE.value: "🌏",
    Exchange.BINANCE_FUTURES.value: "🌏",
    Exchange.BYBIT.value: "🌍",
    Exchange.BYBIT_FUTURES.value: "🌍"
}

# 웹소켓 URL
WEBSOCKET_URLS: Dict[str, str] = {
    Exchange.UPBIT.value: "wss://api.upbit.com/websocket/v1",
    Exchange.BITHUMB.value: "wss://pubwss.bithumb.com/pub/ws",
    Exchange.BINANCE.value: "wss://stream.binance.com:9443/ws",
    Exchange.BINANCE_FUTURES.value: "wss://fstream.binance.com/ws",
    Exchange.BYBIT.value: "wss://stream.bybit.com/v5/public/spot",
    Exchange.BYBIT_FUTURES.value: "wss://stream.bybit.com/v5/public/linear"
}

# 테스트넷 URL (필요한 경우)
WEBSOCKET_TESTNET_URLS = {
    Exchange.BYBIT_FUTURES.value: "wss://stream-testnet.bybit.com/v5/public/linear"
}

# REST API URL
API_URLS: Dict[str, Dict[str, str]] = {
    Exchange.UPBIT.value: {
        "market": "https://api.upbit.com/v1/market/all",
        "ticker": "https://api.upbit.com/v1/ticker"
    },
    Exchange.BITHUMB.value: {
        "ticker": "https://api.bithumb.com/public/ticker/ALL_KRW"
    },
    Exchange.BINANCE.value: {
        "spot": "https://api.binance.com/api/v3/exchangeInfo",
        "depth": "https://api.binance.com/api/v3/depth"
    },
    Exchange.BINANCE_FUTURES.value: {
        "future": "https://fapi.binance.com/fapi/v1/exchangeInfo",
        "depth": "https://fapi.binance.com/fapi/v1/depth"
    },
    Exchange.BYBIT.value: {
        "spot": "https://api.bybit.com/v5/market/instruments-info?category=spot"
    },
    Exchange.BYBIT_FUTURES.value: {
        "future": "https://api.bybit.com/v5/market/instruments-info?category=linear"
    }
}

# 상태 이모지
STATUS_EMOJIS: Dict[str, str] = {
    "CONNECTED": "🟢",
    "DISCONNECTED": "🔴",
    "CONNECTING": "🟡",
    "RECONNECTING": "🟠",
    "ERROR": "⛔"
}

# 재시도 설정
MAX_RETRY_COUNT = 30
BASE_RETRY_DELAY = 4.0
MAX_RETRY_DELAY = 300.0  # 5분

# API 요청 설정
REQUEST_TIMEOUT = 10.0  # 초
CHUNK_SIZE = 99        # Upbit API 제한
REQUEST_DELAY = 0.5    # 초

# 메트릭스 전송 설정
METRICS_TO_SEND = [
    "error_count",
    "uptime_sec",
    "reconnect_count"
]

# 웹소켓 설정
WEBSOCKET_CONFIG = {
    Exchange.UPBIT.value: {
        "update_speed": 100,  # ms
        "depth_levels": 15,   # 호가 단계
        "ping_interval": 60   # 초
    },
    Exchange.BITHUMB.value: {
        "update_speed": 100,
        "depth_levels": 15,
        "ping_interval": 30
    },
    Exchange.BINANCE.value: {
        "update_speed": 100,
        "depth_levels": 20,
        "ping_interval": 20
    },
    Exchange.BINANCE_FUTURES.value: {
        "update_speed": 100,
        "depth_levels": 20,
        "ping_interval": 20
    },
    Exchange.BYBIT.value: {
        "update_speed": 100,
        "depth_levels": 25,
        "ping_interval": 20
    },
    Exchange.BYBIT_FUTURES.value: {
        "update_speed": 100,
        "depth_levels": 25,
        "ping_interval": 20
    }
}

# 초기화: 필요한 디렉토리 생성
DIRS.ensure_directories() 