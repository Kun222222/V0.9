"""
텔레그램 봇 알림 모듈

이 모듈은 텔레그램 봇을 통한 기본적인 알림 기능을 제공합니다.
단순히 메시지를 텔레그램으로 전송하는 기능만 구현합니다.

최종수정: 2024.07
"""

import os
import asyncio
import aiohttp
import logging
import time
from datetime import datetime
from enum import Enum
from typing import Union, Dict, Any
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# ============================
# 텔레그램 설정 값
# ============================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_MAX_RETRIES = 3         # 최대 재시도 횟수
TELEGRAM_RETRY_DELAY = 1.0       # 재시도 간격 (초)
TELEGRAM_MAX_MESSAGE_LENGTH = 4096  # 텔레그램 메시지 최대 길이

# ============================
# 메시지 타입 정의 (단순화)
# ============================
class MessageType(Enum):
    """텔레그램 메시지 타입 정의"""
    INFO = "info"
    ERROR = "error"
    WARNING = "warning"
    SUCCESS = "success"
    CONNECTION = "connection"
    RECONNECT = "reconnect"
    DISCONNECT = "disconnect"
    SYSTEM = "system"

# 로거 설정
logger = logging.getLogger('telegram_simple')

def setup_logger():
    """텔레그램 봇 로거 설정"""
    global logger
    
    # 로그 디렉토리 경로 설정
    log_dir = os.path.join(os.getcwd(), "src", "logs", "telegram")
    os.makedirs(log_dir, exist_ok=True)
    
    # 로거 레벨 설정
    logger.setLevel(logging.DEBUG)
    
    # 핸들러가 이미 있는지 확인
    if not logger.handlers:
        # 파일 핸들러 설정
        log_file = os.path.join(log_dir, f"telegram_{datetime.now().strftime('%y%m%d_%H%M%S')}.log")
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        
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
    
    logger.info("[텔레그램] 로거 초기화 완료 (경로: %s)", log_dir)
    return logger

# 로거 초기화
logger = setup_logger()

async def send_telegram_message(
    message: str,
    retry_count: int = 0,
    timeout: float = 10.0
) -> bool:
    """
    텔레그램 메시지 전송 (단순화된 버전)
    
    Args:
        message: 전송할 메시지 문자열
        retry_count: 현재 재시도 횟수
        timeout: API 요청 타임아웃 (초)
    
    Returns:
        bool: 전송 성공 여부
    """
    # 로깅 추가: 함수 호출 시작
    task_id = f"msg_{int(time.time() * 1000)}"
    logger.debug("[텔레그램] 메시지 전송 시작 (재시도: %d)", retry_count)
    
    # 토큰과 채팅 ID 확인
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("[텔레그램] 봇 토큰 또는 채팅 ID가 설정되지 않았습니다.")
        return False
    
    try:
        # 메시지 길이 제한
        if len(message) > TELEGRAM_MAX_MESSAGE_LENGTH:
            message = message[:TELEGRAM_MAX_MESSAGE_LENGTH - 100] + "...(잘림)"
        
        # API 요청 준비
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        
        # 비동기 컨텍스트 매니저로 API 요청 전송
        async with aiohttp.ClientSession() as session:
            request_start_time = time.time()
            
            # 타임아웃 설정
            async with session.post(url, json=payload, timeout=timeout) as response:
                request_elapsed = time.time() - request_start_time
                response_text = await response.text()
                
                if response.status == 200:
                    logger.info("[텔레그램] 메시지 전송 성공 (ID: %s, 소요 시간: %.3f초)", 
                               task_id, request_elapsed)
                    return True
                else:
                    logger.error("[텔레그램] 메시지 전송 실패 (ID: %s, 상태 코드: %d): %s", 
                                task_id, response.status, response_text)
                    
                    # 재시도
                    if retry_count < TELEGRAM_MAX_RETRIES:
                        await asyncio.sleep(TELEGRAM_RETRY_DELAY)
                        return await send_telegram_message(message, retry_count + 1, timeout)
                    else:
                        logger.error("[텔레그램] 최대 재시도 횟수 초과 (ID: %s)", task_id)
                        return False
    except asyncio.TimeoutError:
        logger.error("[텔레그램] API 요청 타임아웃 (ID: %s, 제한 시간: %.1f초)", task_id, timeout)
        
        # 재시도
        if retry_count < TELEGRAM_MAX_RETRIES:
            await asyncio.sleep(TELEGRAM_RETRY_DELAY)
            return await send_telegram_message(message, retry_count + 1, timeout)
        else:
            logger.error("[텔레그램] 최대 재시도 횟수 초과 (ID: %s)", task_id)
            return False
    except Exception as e:
        logger.error("[텔레그램] 메시지 전송 중 오류 발생 (ID: %s): %s", task_id, str(e), exc_info=True)
        
        # 재시도
        if retry_count < TELEGRAM_MAX_RETRIES:
            await asyncio.sleep(TELEGRAM_RETRY_DELAY)
            return await send_telegram_message(message, retry_count + 1, timeout)
        else:
            logger.error("[텔레그램] 최대 재시도 횟수 초과 (ID: %s)", task_id)
            return False

# 테스트 코드
if __name__ == "__main__":
    async def test_telegram():
        # 텔레그램 토큰 및 채팅 ID 확인
        logger.info(f"텔레그램 봇 토큰: {TELEGRAM_BOT_TOKEN[:5]}...{TELEGRAM_BOT_TOKEN[-5:] if TELEGRAM_BOT_TOKEN else None}")
        logger.info(f"텔레그램 채팅 ID: {TELEGRAM_CHAT_ID}")
        
        # 테스트 메시지 전송
        message = f"🚨 [테스트] 텔레그램 메시지 전송 테스트 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        result = await send_telegram_message(message)
        logger.info(f"테스트 결과: {'성공' if result else '실패'}")
        
    import asyncio
    asyncio.run(test_telegram())