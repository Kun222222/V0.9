# file: telegrambot/notification/telegram_bot.py

"""
텔레그램 봇 모듈

이 모듈은 텔레그램 봇을 통한 알림 기능을 제공합니다.
주요 기능:
1. 텔레그램 메시지 전송
2. 에러 처리 및 재시도
3. 메시지 포맷팅
4. 비동기 처리

최종수정: 2024.03
"""

import asyncio
import aiohttp
from datetime import datetime
from crosskimp.ob_collector.utils.config.config_loader import get_settings
from typing import Dict, Optional, List, Union, Any
import logging
import os
import json
import telegram
from telegram.ext import Updater
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# 상수 가져오기
from crosskimp.ob_collector.utils.config.constants import LOG_SYSTEM
from crosskimp.telegrambot.bot_constants import (
    MessageType, MessageIcon, MESSAGE_TEMPLATES,
    TELEGRAM_MAX_MESSAGE_LENGTH, TELEGRAM_MAX_RETRIES, TELEGRAM_RETRY_DELAY
)

# ============================
# 로깅 설정
# ============================
TELEGRAM_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), "logs", "telegram")
os.makedirs(TELEGRAM_LOG_DIR, exist_ok=True)

# 로거 생성
logger = logging.getLogger('telegram_bot')
logger.setLevel(logging.INFO)

# 파일 핸들러 설정
log_file = os.path.join(TELEGRAM_LOG_DIR, f"telegram_{datetime.now().strftime('%y%m%d_%H%M%S')}.log")
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.INFO)

# 콘솔 핸들러 설정
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)

# 포맷터 설정
formatter = logging.Formatter('[%(asctime)s.%(msecs)03d] - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# 핸들러 추가
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ============================
# 텔레그램 봇 설정
# ============================
TELEGRAM_CONFIG = {
    'bot_token': os.getenv('TELEGRAM_BOT_TOKEN'),
    'chat_id': os.getenv('TELEGRAM_CHAT_ID')
}

# 봇 인스턴스 생성
bot: Optional[telegram.Bot] = None
try:
    bot = telegram.Bot(token=TELEGRAM_CONFIG['bot_token'])
    logger.info("[텔레그램] 봇 초기화 완료")
except Exception as e:
    logger.error(f"[텔레그램] 봇 초기화 실패: {e}")

# ============================
# 유틸리티 함수
# ============================
def format_message(
    message_type: str,
    data: Dict[str, Union[str, int, float]],
    add_time: bool = True
) -> str:
    """
    메시지 포맷팅
    
    Args:
        message_type: 메시지 타입 (MessageType 클래스 참조)
        data: 템플릿에 들어갈 데이터
        add_time: 시간 자동 추가 여부
    
    Returns:
        str: 포맷팅된 메시지
    """
    template = MESSAGE_TEMPLATES.get(message_type, MESSAGE_TEMPLATES[MessageType.INFO])
    
    # 시간 자동 추가
    if add_time and "time" not in data:
        data["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        formatted = f"{template['icon']} {template['format'].format(**data)}"
    except KeyError as e:
        logger.error(f"메시지 포맷팅 오류: 누락된 키 {e}")
        formatted = f"{MessageIcon.ERROR} [포맷팅 오류] 메시지를 표시할 수 없습니다."
    except Exception as e:
        logger.error(f"메시지 포맷팅 오류: {e}")
        formatted = f"{MessageIcon.ERROR} [포맷팅 오류] {str(e)}"
    
    # 최대 길이 제한
    if len(formatted) > TELEGRAM_MAX_MESSAGE_LENGTH:
        formatted = formatted[:TELEGRAM_MAX_MESSAGE_LENGTH-3] + "..."
    
    return formatted

def validate_telegram_config(settings: Dict) -> bool:
    """설정 유효성 검증"""
    # 환경 변수에서 직접 토큰과 채팅 ID 확인
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    # 텔레그램 알림 활성화 여부 확인
    telegram_config = settings.get("notifications", {}).get("telegram", {})
    if not telegram_config.get("enabled", False):
        logger.info("텔레그램 알림 비활성화 상태")
        return False
    
    # 토큰과 채팅 ID 확인
    if not token:
        logger.error("텔레그램 토큰 누락 (환경 변수 TELEGRAM_BOT_TOKEN)")
        return False
        
    if not chat_id:
        logger.error("텔레그램 chat_id 누락 (환경 변수 TELEGRAM_CHAT_ID)")
        return False
        
    return True

# ============================
# 메인 함수
# ============================
async def send_telegram_message(
    settings: Dict,
    message_type: str,
    data: Dict[str, Union[str, int, float]],
    retry_count: int = 0
) -> bool:
    """
    텔레그램 메시지 전송
    
    Args:
        settings: 설정 데이터
        message_type: 메시지 타입 (MessageType 클래스 참조)
        data: 템플릿에 들어갈 데이터
        retry_count: 현재 재시도 횟수
    
    Returns:
        bool: 전송 성공 여부
    """
    try:
        # 설정 유효성 검증
        if not validate_telegram_config(settings):
            return False
            
        # 환경 변수에서 직접 토큰과 채팅 ID 가져오기
        token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('TELEGRAM_CHAT_ID')
        
        # 메시지 포맷팅
        formatted_message = format_message(message_type, data)
        
        # API 요청 준비
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": formatted_message,
            "parse_mode": "HTML"
        }
        
        logger.debug(f"메시지 전송 시도 (타입: {message_type})")
        
        # API 요청 전송
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    logger.info(f"메시지 전송 성공 (타입: {message_type})")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"메시지 전송 실패 (상태 코드: {response.status}): {error_text}")
                    
                    # 재시도
                    if retry_count < TELEGRAM_MAX_RETRIES:
                        logger.info(f"메시지 전송 재시도 ({retry_count + 1}/{TELEGRAM_MAX_RETRIES})")
                        await asyncio.sleep(TELEGRAM_RETRY_DELAY)
                        return await send_telegram_message(settings, message_type, data, retry_count + 1)
                    else:
                        logger.error(f"최대 재시도 횟수 초과 ({TELEGRAM_MAX_RETRIES})")
                        return False
                        
    except Exception as e:
        logger.error(f"메시지 전송 중 예외 발생: {str(e)}", exc_info=True)
        
        # 재시도
        if retry_count < TELEGRAM_MAX_RETRIES:
            logger.info(f"메시지 전송 재시도 ({retry_count + 1}/{TELEGRAM_MAX_RETRIES})")
            await asyncio.sleep(TELEGRAM_RETRY_DELAY)
            return await send_telegram_message(settings, message_type, data, retry_count + 1)
        else:
            logger.error(f"최대 재시도 횟수 초과 ({TELEGRAM_MAX_RETRIES})")
            return False

# ============================
# 편의 함수
# ============================
async def send_error(settings: Dict, component: str, message: str) -> bool:
    """에러 메시지 전송"""
    return await send_telegram_message(settings, MessageType.ERROR, {
        "component": component,
        "message": message
    })

async def send_trade(settings: Dict, exchange_from: str, exchange_to: str,
                    symbol: str, amount: float, price: float, kimp: float) -> bool:
    """거래 메시지 전송"""
    return await send_telegram_message(settings, MessageType.TRADE, {
        "exchange_from": exchange_from,
        "exchange_to": exchange_to,
        "symbol": symbol,
        "amount": amount,
        "price": price,
        "kimp": kimp
    })

async def send_profit(settings: Dict, amount: float, percentage: float, details: str) -> bool:
    """수익 메시지 전송"""
    return await send_telegram_message(settings, MessageType.PROFIT, {
        "amount": amount,
        "percentage": percentage,
        "details": details
    })

async def send_market_status(settings: Dict, usdt_price: float,
                           upbit_status: bool, bithumb_status: bool) -> bool:
    """시장 상태 메시지 전송"""
    return await send_telegram_message(settings, MessageType.MARKET, {
        "usdt_price": usdt_price,
        "upbit_status": MessageIcon.CONNECTION[upbit_status],
        "bithumb_status": MessageIcon.CONNECTION[bithumb_status]
    })

async def send_system_status(settings: Dict, cpu_usage: float,
                           memory_usage: float, uptime: str) -> bool:
    """시스템 상태 메시지 전송"""
    return await send_telegram_message(settings, MessageType.SYSTEM, {
        "cpu_usage": cpu_usage,
        "memory_usage": memory_usage,
        "uptime": uptime
    })

# 테스트 코드
if __name__ == "__main__":
    async def test_telegram():
        settings = {
            "notifications": {
                "telegram": {
                    "enabled": True,
                    "token": os.getenv('TELEGRAM_BOT_TOKEN'),
                    "chat_id": os.getenv('TELEGRAM_CHAT_ID')
                }
            }
        }
        
        # 테스트 메시지 전송
        await send_error(settings, "텔레그램 봇", "테스트 에러 메시지")
        await send_system_status(settings, 25.5, 40.2, "1일 2시간 30분")
        
    asyncio.run(test_telegram())