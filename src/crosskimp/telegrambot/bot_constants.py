"""
텔레그램 봇 관련 상수 정의

이 모듈은 텔레그램 봇 관련 상수들을 정의합니다.
- 봇 설정 관련 상수
- 명령어 관련 상수
- 메시지 타입 및 포맷 관련 상수
- 알림 관련 상수

작성자: CrossKimp Arbitrage Bot 개발팀
최종수정: 2024.03
"""

import os
from enum import Enum
from typing import Dict, List
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Bot settings
COMMAND_BOT_TOKEN = os.getenv("TELEGRAM_COMMAND_BOT_TOKEN")  # 명령 봇용 토큰
NOTIFICATION_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  # 알림 봇용 토큰
ADMIN_USER_IDS = [int(os.getenv("TELEGRAM_CHAT_ID", "0"))]
NOTIFICATION_CHAT_IDS = [int(os.getenv("TELEGRAM_CHAT_ID", "0"))]

# ============================
# 텔레그램 메시지 타입 및 아이콘
# ============================
class MessageType:
    """메시지 타입 정의"""
    ERROR = "error"
    INFO = "info"
    TRADE = "trade"
    PROFIT = "profit"
    STARTUP = "startup"
    SHUTDOWN = "shutdown"
    WARNING = "warning"
    MARKET = "market"
    SYSTEM = "system"

class MessageIcon:
    """메시지 아이콘 정의"""
    ERROR = "🚨"
    INFO = "ℹ️"
    TRADE = "💰"
    PROFIT = "💵"
    STARTUP = "🚀"
    SHUTDOWN = "🔴"
    WARNING = "⚠️"
    MARKET = "📊"
    SYSTEM = "⚙️"
    CONNECTION = {
        True: "🟢",   # 연결됨
        False: "🔴"   # 연결 끊김
    }

# 텔레그램 메시지 템플릿
MESSAGE_TEMPLATES = {
    MessageType.ERROR: {
        "icon": MessageIcon.ERROR,
        "format": """[에러 발생]
- 컴포넌트: {component}
- 메시지: {message}
- 시간: {time}"""
    },
    MessageType.INFO: {
        "icon": MessageIcon.INFO,
        "format": """[알림]
{message}
- 시간: {time}"""
    },
    MessageType.TRADE: {
        "icon": MessageIcon.TRADE,
        "format": """[거래 실행]
- 거래소: {exchange_from} ➜ {exchange_to}
- 심볼: {symbol}
- 수량: {amount}
- 가격: {price:,.0f} KRW
- 김프: {kimp:.2f}%
- 시간: {time}"""
    },
    MessageType.PROFIT: {
        "icon": MessageIcon.PROFIT,
        "format": """[수익 발생]
- 금액: {amount:,.0f} KRW
- 수익률: {percentage:.2f}%
- 상세: {details}
- 시간: {time}"""
    },
    MessageType.STARTUP: {
        "icon": MessageIcon.STARTUP,
        "format": """[시스템 시작]
- 컴포넌트: {component}
- 상태: {status}
- 시간: {time}"""
    },
    MessageType.SHUTDOWN: {
        "icon": MessageIcon.SHUTDOWN,
        "format": """[시스템 종료]
- 컴포넌트: {component}
- 사유: {reason}
- 시간: {time}"""
    },
    MessageType.WARNING: {
        "icon": MessageIcon.WARNING,
        "format": """[경고]
- 컴포넌트: {component}
- 메시지: {message}
- 시간: {time}"""
    },
    MessageType.MARKET: {
        "icon": MessageIcon.MARKET,
        "format": """[시장 상태]
- USDT/KRW: {usdt_price:,.2f} KRW
- 업비트: {upbit_status}
- 빗썸: {bithumb_status}
- 시간: {time}"""
    },
    MessageType.SYSTEM: {
        "icon": MessageIcon.SYSTEM,
        "format": """[시스템 상태]
- CPU: {cpu_usage:.1f}%
- 메모리: {memory_usage:.1f}%
- 업타임: {uptime}
- 시간: {time}"""
    }
}

# Command settings
class BotCommands(str, Enum):
    START = "start"
    HELP = "help"
    START_OB = "start_ob"
    STOP_OB = "stop_ob"
    STATUS = "status"

# Command descriptions
COMMAND_DESCRIPTIONS: Dict[str, str] = {
    BotCommands.START: "봇 시작 및 소개",
    BotCommands.HELP: "사용 가능한 명령어 목록",
    BotCommands.START_OB: "오더북 수집 프로그램 시작",
    BotCommands.STOP_OB: "오더북 수집 프로그램 중지",
    BotCommands.STATUS: "현재 실행 중인 프로그램 상태 확인",
}

# Messages
WELCOME_MESSAGE = """
🤖 크로스김프 오더북 수집 봇에 오신 것을 환영합니다!
사용 가능한 명령어를 보려면 /help를 입력하세요.
"""

HELP_MESSAGE = """
📋 사용 가능한 명령어 목록:

/start - 봇 시작 및 소개
/help - 이 도움말 메시지 표시
/start_ob - 오더북 수집 프로그램 시작
/stop_ob - 오더북 수집 프로그램 중지
/status - 현재 실행 중인 프로그램 상태 확인
"""

# Status messages
PROGRAM_STARTED = "✅ 오더북 수집 프로그램이 시작되었습니다."
PROGRAM_STOPPED = "🛑 오더북 수집 프로그램이 중지되었습니다."
PROGRAM_ALREADY_RUNNING = "⚠️ 오더북 수집 프로그램이 이미 실행 중입니다."
PROGRAM_NOT_RUNNING = "⚠️ 오더북 수집 프로그램이 실행 중이지 않습니다."
UNAUTHORIZED_USER = "🚫 권한이 없는 사용자입니다."

# 시작/종료 메시지 상수 정의
TELEGRAM_START_MESSAGE = {
    "component": "OrderBook Collector",
    "status": "시스템 초기화 시작"
}

TELEGRAM_STOP_MESSAGE = {
    "component": "OrderBook Collector",
    "reason": "사용자 요청으로 인한 종료"
}

# 텔레그램 설정
TELEGRAM_MAX_MESSAGE_LENGTH = 4096  # 텔레그램 메시지 최대 길이
TELEGRAM_MAX_RETRIES = 3            # 최대 재시도 횟수
TELEGRAM_RETRY_DELAY = 1.0          # 재시도 간격 (초)

# Export all constants
__all__ = [
    # 봇 설정
    'COMMAND_BOT_TOKEN', 'NOTIFICATION_BOT_TOKEN',
    'ADMIN_USER_IDS', 'NOTIFICATION_CHAT_IDS',
    
    # 메시지 타입 및 아이콘
    'MessageType', 'MessageIcon', 'MESSAGE_TEMPLATES',
    
    # 명령어 관련
    'BotCommands', 'COMMAND_DESCRIPTIONS',
    'WELCOME_MESSAGE', 'HELP_MESSAGE',
    'PROGRAM_STARTED', 'PROGRAM_STOPPED',
    
    # 시작/종료 메시지
    'TELEGRAM_START_MESSAGE', 'TELEGRAM_STOP_MESSAGE',
    
    # 텔레그램 설정
    'TELEGRAM_MAX_MESSAGE_LENGTH', 'TELEGRAM_MAX_RETRIES', 'TELEGRAM_RETRY_DELAY'
] 