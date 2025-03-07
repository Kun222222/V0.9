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