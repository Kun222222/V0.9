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
import time

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
# 로그 디렉토리 경로 수정
TELEGRAM_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "logs", "telegram")
os.makedirs(TELEGRAM_LOG_DIR, exist_ok=True)

# 로거 생성
logger = logging.getLogger('telegram_bot')
logger.setLevel(logging.DEBUG)  # DEBUG 레벨로 변경하여 더 많은 로그 기록

# 파일 핸들러 설정
log_file = os.path.join(TELEGRAM_LOG_DIR, f"telegram_{datetime.now().strftime('%y%m%d_%H%M%S')}.log")
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)  # DEBUG 레벨로 설정

# 콘솔 핸들러 설정
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# 포맷터 설정
formatter = logging.Formatter('[%(asctime)s.%(msecs)03d] - [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# 핸들러 추가
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 로거 초기화 로그
logger.info("[텔레그램] 로거 초기화 완료 (경로: %s)", TELEGRAM_LOG_DIR)

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
        add_time: 시간 추가 여부
        
    Returns:
        str: 포맷팅된 메시지
    """
    logger.debug("[텔레그램] 메시지 포맷팅 시작 (타입: %s)", message_type)
    
    # 템플릿 가져오기
    template = MESSAGE_TEMPLATES.get(message_type)
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
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = os.getenv('TELEGRAM_CHAT_ID')
    
    if not token:
        logger.error("[텔레그램] 봇 토큰 누락 (환경 변수 TELEGRAM_BOT_TOKEN)")
        return False
        
    if not chat_id:
        logger.error("[텔레그램] chat_id 누락 (환경 변수 TELEGRAM_CHAT_ID)")
        return False
    
    logger.debug("[텔레그램] 설정 유효성 검증 완료 (유효함)")
    return True

# ============================
# 메인 함수
# ============================
async def send_telegram_message(
    settings: Dict,
    message_type: str,
    data: Dict[str, Union[str, int, float]],
    retry_count: int = 0,
    timeout: float = 10.0  # 타임아웃 설정 추가 (기본 10초)
) -> bool:
    """
    텔레그램 메시지 전송
    
    Args:
        settings: 설정 데이터
        message_type: 메시지 타입 (MessageType 클래스 참조)
        data: 템플릿에 들어갈 데이터
        retry_count: 현재 재시도 횟수
        timeout: API 요청 타임아웃 (초)
    
    Returns:
        bool: 전송 성공 여부
    """
    # 로깅 추가: 함수 호출 시작
    logger.debug("[텔레그램] send_telegram_message 함수 호출 (타입: %s, 재시도: %d, 타임아웃: %.1f초)", 
                message_type, retry_count, timeout)
    task_id = f"{message_type}_{int(time.time() * 1000)}"
    
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
            token = os.getenv('TELEGRAM_BOT_TOKEN')
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