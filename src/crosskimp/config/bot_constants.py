"""
텔레그램 봇 관련 상수 및 유틸리티 정의

이 모듈은 텔레그램 봇 관련 상수들과 유틸리티 함수들을 정의합니다.
- 봇 설정 관련 상수
- 명령어 관련 상수
- 메시지 타입 및 포맷 관련 상수
- 알림 관련 상수
- 텔레그램 API 관련 유틸리티 함수

"""

import os
import asyncio
import aiohttp
import logging
import time
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ============================
# 봇 설정 관련 상수
# ============================
COMMAND_BOT_TOKEN = os.getenv("TELEGRAM_COMMAND_BOT_TOKEN")  # 명령 봇용 토큰
NOTIFICATION_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  # 알림 봇용 토큰
ADMIN_USER_IDS = [int(os.getenv("TELEGRAM_CHAT_ID", "0"))]
NOTIFICATION_CHAT_IDS = [int(os.getenv("TELEGRAM_CHAT_ID", "0"))]

# ============================
# 텔레그램 메시지 타입 및 아이콘
# ============================
class MessageType(Enum):
    INFO = "info"
    ERROR = "error"
    WARNING = "warning"
    SUCCESS = "success"
    STARTUP = "startup"
    SHUTDOWN = "shutdown"
    CONNECTION = "connection"  # 웹소켓 연결 상태 메시지
    RECONNECT = "reconnect"    # 재연결 메시지
    DISCONNECT = "disconnect"  # 연결 종료 메시지
    TRADE = "trade"            # 거래 실행 메시지
    PROFIT = "profit"          # 수익 발생 메시지
    MARKET = "market"          # 시장 상태 메시지
    SYSTEM = "system"          # 시스템 상태 메시지

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
    MessageType.ERROR: f"{MessageIcon.ERROR} <b>오류 발생</b>\n\n<b>컴포넌트:</b> {{component}}\n<b>메시지:</b> {{message}}",
    MessageType.INFO: f"{MessageIcon.INFO} <b>알림</b>\n\n{{message}}",
    MessageType.TRADE: f"{MessageIcon.TRADE} <b>거래 실행</b>\n\n<b>거래소:</b> {{exchange_from}} ➜ {{exchange_to}}\n<b>심볼:</b> {{symbol}}\n<b>수량:</b> {{amount}}\n<b>가격:</b> {{price:,.0f}} KRW\n<b>김프:</b> {{kimp:.2f}}%",
    MessageType.PROFIT: f"{MessageIcon.PROFIT} <b>수익 발생</b>\n\n<b>금액:</b> {{amount:,.0f}} KRW\n<b>수익률:</b> {{percentage:.2f}}%\n<b>상세:</b> {{details}}",
    MessageType.STARTUP: f"{MessageIcon.STARTUP} <b>시스템 시작</b>\n\n<b>컴포넌트:</b> {{component}}\n<b>상태:</b> {{status}}",
    MessageType.SHUTDOWN: f"{MessageIcon.SHUTDOWN} <b>시스템 종료</b>\n\n<b>컴포넌트:</b> {{component}}\n<b>사유:</b> {{reason}}",
    MessageType.WARNING: f"{MessageIcon.WARNING} <b>경고</b>\n\n<b>컴포넌트:</b> {{component}}\n<b>메시지:</b> {{message}}",
    MessageType.MARKET: f"{MessageIcon.MARKET} <b>시장 상태</b>\n\n<b>USDT/KRW:</b> {{usdt_price:,.2f}} KRW\n<b>업비트:</b> {{upbit_status}}\n<b>빗썸:</b> {{bithumb_status}}",
    MessageType.SYSTEM: f"{MessageIcon.SYSTEM} <b>시스템 상태</b>\n\n<b>CPU:</b> {{cpu_usage:.1f}}%\n<b>메모리:</b> {{memory_usage:.1f}}%\n<b>업타임:</b> {{uptime}}",
    MessageType.CONNECTION: f"{MessageIcon.CONNECTION[True]} <b>웹소켓 연결</b>\n\n<b>메시지:</b> {{message}}",
    MessageType.RECONNECT: f"{MessageIcon.CONNECTION[False]} <b>웹소켓 재연결</b>\n\n<b>메시지:</b> {{message}}",
    MessageType.DISCONNECT: f"{MessageIcon.CONNECTION[False]} <b>웹소켓 연결 종료</b>\n\n<b>메시지:</b> {{message}}"
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

# ============================
# 텔레그램 API 관련 유틸리티
# ============================
# 로거 설정
logger = logging.getLogger('telegram_bot')

def setup_logger():
    """텔레그램 봇 로거 설정"""
    global logger
    
    # 로그 디렉토리 경로 설정
    from crosskimp.config.paths import LOGS_DIR
    TELEGRAM_LOG_DIR = os.path.join(LOGS_DIR, "telegram")
    os.makedirs(TELEGRAM_LOG_DIR, exist_ok=True)
    
    # 로거 레벨 설정
    logger.setLevel(logging.DEBUG)
    
    # 파일 핸들러 설정
    log_file = os.path.join(TELEGRAM_LOG_DIR, f"telegram_{datetime.now().strftime('%y%m%d_%H%M%S')}.log")
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    
    # 콘솔 핸들러 설정
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 포맷터 설정
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03d] - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 핸들러 추가 (기존 핸들러 제거 후)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info("[텔레그램] 로거 초기화 완료 (경로: %s)", TELEGRAM_LOG_DIR)
    return logger

def format_message(
    message_type: Union[str, MessageType],
    data: Dict[str, Union[str, int, float]],
    add_time: bool = True
) -> str:
    """
    메시지 포맷팅
    
    Args:
        message_type: 메시지 타입 (MessageType 클래스 참조 또는 문자열)
        data: 템플릿에 들어갈 데이터
        add_time: 시간 추가 여부
        
    Returns:
        str: 포맷팅된 메시지
    """
    logger.debug("[텔레그램] 메시지 포맷팅 시작 (타입: %s)", message_type)
    
    # MessageType 객체를 받은 경우 처리
    if isinstance(message_type, MessageType):
        message_type_key = message_type
    else:
        # 문자열을 받은 경우 MessageType으로 변환 시도
        try:
            message_type_key = MessageType[message_type.upper()]
        except (KeyError, AttributeError):
            logger.error("[텔레그램] 알 수 없는 메시지 타입: %s", message_type)
            message_type_key = MessageType.INFO
    
    # 템플릿 가져오기
    template = MESSAGE_TEMPLATES.get(message_type_key)
    if not template:
        logger.error("[텔레그램] 알 수 없는 메시지 타입: %s", message_type)
        return f"알 수 없는 메시지 타입: {message_type}"
    
    # 시간 추가
    if add_time:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        template = f"<b>[{current_time}]</b>\n\n{template}"
    
    try:
        # 템플릿에 데이터 적용
        formatted_message = template.format(**data)
        logger.debug("[텔레그램] 메시지 포맷팅 완료 (길이: %d자)", len(formatted_message))
        
        # 메시지 길이 제한
        if len(formatted_message) > TELEGRAM_MAX_MESSAGE_LENGTH:
            logger.warning("[텔레그램] 메시지 길이 초과 (%d자, 최대: %d자)", 
                          len(formatted_message), TELEGRAM_MAX_MESSAGE_LENGTH)
            formatted_message = formatted_message[:TELEGRAM_MAX_MESSAGE_LENGTH - 100] + "...(잘림)"
        
        return formatted_message
    except KeyError as e:
        logger.error("[텔레그램] 메시지 포맷팅 중 키 오류: %s", str(e))
        return f"메시지 포맷팅 오류: {str(e)}"
    except Exception as e:
        logger.error("[텔레그램] 메시지 포맷팅 중 오류: %s", str(e))
        return f"메시지 포맷팅 오류: {str(e)}"

def validate_telegram_config(settings: Dict) -> bool:
    """
    텔레그램 설정 유효성 검증
    
    Args:
        settings: 설정 데이터
        
    Returns:
        bool: 유효성 여부
    """
    logger.debug("[텔레그램] 설정 유효성 검증 시작")
    
    # 설정에서 텔레그램 활성화 여부 확인
    if 'notifications' not in settings:
        logger.error("[텔레그램] 설정에 'notifications' 섹션 누락")
        return False
        
    if 'telegram' not in settings['notifications']:
        logger.error("[텔레그램] 설정에 'notifications.telegram' 섹션 누락")
        return False
        
    if 'enabled' not in settings['notifications']['telegram']:
        logger.error("[텔레그램] 설정에 'notifications.telegram.enabled' 필드 누락")
        return False
        
    # 텔레그램 비활성화 상태면 유효성 검증 통과 (사용하지 않으므로)
    if not settings['notifications']['telegram']['enabled']:
        logger.debug("[텔레그램] 텔레그램 알림 비활성화 상태")
        return True
    
    # 환경 변수에서 토큰과 채팅 ID 확인
    token = NOTIFICATION_BOT_TOKEN
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if not token:
        logger.error("[텔레그램] 봇 토큰 누락 (환경 변수 TELEGRAM_BOT_TOKEN)")
        return False
        
    if not chat_id:
        logger.error("[텔레그램] chat_id 누락 (환경 변수 TELEGRAM_CHAT_ID)")
        return False
    
    logger.debug("[텔레그램] 설정 유효성 검증 완료 (유효함)")
    return True

async def send_telegram_message(
    settings: Dict,
    message_type: Union[str, MessageType],
    data: Union[Dict[str, Union[str, int, float]], str],
    retry_count: int = 0,
    timeout: float = 10.0  # 타임아웃 설정 추가 (기본 10초)
) -> bool:
    """
    텔레그램 메시지 전송
    
    Args:
        settings: 설정 데이터
        message_type: 메시지 타입 (MessageType 클래스 참조 또는 문자열)
        data: 템플릿에 들어갈 데이터 또는 메시지 문자열
        retry_count: 현재 재시도 횟수
        timeout: API 요청 타임아웃 (초)
    
    Returns:
        bool: 전송 성공 여부
    """
    # 로깅 추가: 함수 호출 시작
    logger.debug("[텔레그램] send_telegram_message 함수 호출 (타입: %s, 재시도: %d, 타임아웃: %.1f초)", 
                message_type, retry_count, timeout)
    task_id = f"{message_type}_{int(time.time() * 1000)}"
    
    # 문자열을 받은 경우 딕셔너리로 변환
    if isinstance(data, str):
        data = {"message": data}
    
    # 비동기 태스크로 실행하여 초기화 과정을 블로킹하지 않도록 함
    async def _send_message():
        try:
            # 로깅 추가: 비동기 태스크 시작
            start_time = time.time()
            logger.debug("[텔레그램] 메시지 전송 태스크 시작 (ID: %s)", task_id)
            
            # 설정 유효성 검증
            if not validate_telegram_config(settings):
                logger.error("[텔레그램] 설정 유효성 검증 실패 (ID: %s)", task_id)
                return False
                
            # 환경 변수에서 직접 토큰과 채팅 ID 가져오기
            token = NOTIFICATION_BOT_TOKEN
            chat_id = os.getenv('TELEGRAM_CHAT_ID')
            logger.debug("[텔레그램] 토큰 및 채팅 ID 확인 완료 (ID: %s)", task_id)
            
            # 메시지 포맷팅
            logger.debug("[텔레그램] 메시지 포맷팅 시작 (ID: %s)", task_id)
            formatted_message = format_message(message_type, data)
            logger.debug("[텔레그램] 메시지 포맷팅 완료 (ID: %s, 길이: %d자)", task_id, len(formatted_message))
            
            # API 요청 준비
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": formatted_message,
                "parse_mode": "HTML"
            }
            
            logger.debug("[텔레그램] API 요청 준비 완료 (ID: %s, URL: %s)", task_id, url)
            logger.info("[텔레그램] 메시지 전송 시도 (ID: %s, 타입: %s)", task_id, message_type)
            
            # API 요청 전송
            request_start_time = time.time()
            async with aiohttp.ClientSession() as session:
                logger.debug("[텔레그램] ClientSession 생성 완료 (ID: %s)", task_id)
                
                try:
                    # 타임아웃 설정
                    async with session.post(url, json=payload, timeout=timeout) as response:
                        request_elapsed = time.time() - request_start_time
                        logger.debug("[텔레그램] API 응답 수신 (ID: %s, 소요 시간: %.3f초)", task_id, request_elapsed)
                        
                        if response.status == 200:
                            response_json = await response.json()
                            logger.debug("[텔레그램] API 응답 내용: %s", response_json)
                            logger.info("[텔레그램] 메시지 전송 성공 (ID: %s, 타입: %s, 소요 시간: %.3f초)", 
                                       task_id, message_type, request_elapsed)
                            return True
                        else:
                            error_text = await response.text()
                            logger.error("[텔레그램] 메시지 전송 실패 (ID: %s, 상태 코드: %d): %s", 
                                        task_id, response.status, error_text)
                            
                            # 재시도
                            if retry_count < TELEGRAM_MAX_RETRIES:
                                logger.info("[텔레그램] 메시지 전송 재시도 (ID: %s, %d/%d)", 
                                           task_id, retry_count + 1, TELEGRAM_MAX_RETRIES)
                                await asyncio.sleep(TELEGRAM_RETRY_DELAY)
                                return await send_telegram_message(settings, message_type, data, retry_count + 1, timeout)
                            else:
                                logger.error("[텔레그램] 최대 재시도 횟수 초과 (ID: %s, %d)", task_id, TELEGRAM_MAX_RETRIES)
                                return False
                except asyncio.TimeoutError:
                    logger.error("[텔레그램] API 요청 타임아웃 (ID: %s, 제한 시간: %.1f초)", task_id, timeout)
                    
                    # 재시도
                    if retry_count < TELEGRAM_MAX_RETRIES:
                        logger.info("[텔레그램] 메시지 전송 재시도 (ID: %s, %d/%d, 타임아웃)", 
                                   task_id, retry_count + 1, TELEGRAM_MAX_RETRIES)
                        await asyncio.sleep(TELEGRAM_RETRY_DELAY)
                        return await send_telegram_message(settings, message_type, data, retry_count + 1, timeout)
                    else:
                        logger.error("[텔레그램] 최대 재시도 횟수 초과 (ID: %s, %d)", task_id, TELEGRAM_MAX_RETRIES)
                        return False
        except Exception as e:
            logger.error("[텔레그램] 메시지 전송 중 오류 발생 (ID: %s): %s", task_id, str(e), exc_info=True)
            
            # 재시도
            if retry_count < TELEGRAM_MAX_RETRIES:
                logger.info("[텔레그램] 메시지 전송 재시도 (ID: %s, %d/%d, 오류)", 
                           task_id, retry_count + 1, TELEGRAM_MAX_RETRIES)
                await asyncio.sleep(TELEGRAM_RETRY_DELAY)
                return await send_telegram_message(settings, message_type, data, retry_count + 1, timeout)
            else:
                logger.error("[텔레그램] 최대 재시도 횟수 초과 (ID: %s, %d)", task_id, TELEGRAM_MAX_RETRIES)
                return False
        finally:
            # 로깅 추가: 태스크 종료
            total_elapsed = time.time() - start_time
            logger.debug("[텔레그램] 메시지 전송 태스크 종료 (ID: %s, 총 소요 시간: %.3f초)", task_id, total_elapsed)
    
    # 비동기 태스크 생성 (백그라운드에서 실행)
    task = asyncio.create_task(_send_message())
    logger.debug("[텔레그램] 메시지 전송 태스크 생성 완료 (ID: %s)", task_id)
    
    # 즉시 True 반환 (비동기 처리)
    return True

# ============================
# 편의 함수
# ============================
async def send_error(settings: Dict, component: str, message: str) -> bool:
    """오류 메시지 전송"""
    data = {
        "component": component,
        "message": message
    }
    # 비동기 태스크로 실행
    asyncio.create_task(send_telegram_message(settings, MessageType.ERROR, data))
    return True

async def send_trade(settings: Dict, exchange_from: str, exchange_to: str,
                    symbol: str, amount: float, price: float, kimp: float) -> bool:
    """거래 메시지 전송"""
    data = {
        "exchange_from": exchange_from,
        "exchange_to": exchange_to,
        "symbol": symbol,
        "amount": amount,
        "price": price,
        "kimp": kimp
    }
    # 비동기 태스크로 실행
    asyncio.create_task(send_telegram_message(settings, MessageType.TRADE, data))
    return True

async def send_profit(settings: Dict, amount: float, percentage: float, details: str) -> bool:
    """수익 메시지 전송"""
    data = {
        "amount": amount,
        "percentage": percentage,
        "details": details
    }
    # 비동기 태스크로 실행
    asyncio.create_task(send_telegram_message(settings, MessageType.PROFIT, data))
    return True

async def send_market_status(settings: Dict, usdt_price: float,
                           upbit_status: bool, bithumb_status: bool) -> bool:
    """시장 상태 메시지 전송"""
    data = {
        "usdt_price": usdt_price,
        "upbit_status": MessageIcon.CONNECTION[upbit_status],
        "bithumb_status": MessageIcon.CONNECTION[bithumb_status]
    }
    # 비동기 태스크로 실행
    asyncio.create_task(send_telegram_message(settings, MessageType.MARKET, data))
    return True

async def send_system_status(settings: Dict, cpu_usage: float,
                           memory_usage: float, uptime: str) -> bool:
    """시스템 상태 메시지 전송"""
    data = {
        "cpu_usage": cpu_usage,
        "memory_usage": memory_usage,
        "uptime": uptime
    }
    # 비동기 태스크로 실행
    asyncio.create_task(send_telegram_message(settings, MessageType.SYSTEM, data))
    return True

# Export all constants and functions
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
    'PROGRAM_ALREADY_RUNNING', 'PROGRAM_NOT_RUNNING',
    'UNAUTHORIZED_USER',
    
    # 시작/종료 메시지
    'TELEGRAM_START_MESSAGE', 'TELEGRAM_STOP_MESSAGE',
    
    # 텔레그램 설정
    'TELEGRAM_MAX_MESSAGE_LENGTH', 'TELEGRAM_MAX_RETRIES', 'TELEGRAM_RETRY_DELAY',
    
    # 텔레그램 API 유틸리티
    'setup_logger', 'format_message', 'validate_telegram_config', 'send_telegram_message',
    
    # 편의 함수
    'send_error', 'send_trade', 'send_profit', 'send_market_status', 'send_system_status'
] 