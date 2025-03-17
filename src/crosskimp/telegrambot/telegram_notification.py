# file: telegrambot/notification/telegram_bot.py

"""
텔레그램 봇 알림 모듈

이 모듈은 텔레그램 봇을 통한 알림 기능을 제공합니다.
bot_constants.py에 정의된 함수들을 사용하여 텔레그램 메시지를 전송합니다.

최종수정: 2024.03
"""

from typing import Dict, Union, Any, Optional
from crosskimp.telegrambot.bot_constants import (
    # 메시지 타입 및 아이콘
    MessageType, MessageIcon,
    
    # 텔레그램 API 유틸리티
    setup_logger, send_telegram_message as bot_send_telegram_message,
    
    # 편의 함수
    send_error, send_trade, send_profit, send_market_status, send_system_status,
    
    # 텔레그램 토큰 및 채팅 ID
    NOTIFICATION_BOT_TOKEN, NOTIFICATION_CHAT_IDS
)
from crosskimp.config.ob_constants import LogMessageType

# 로거 초기화
logger = setup_logger()

def convert_log_message_type(log_type: LogMessageType) -> MessageType:
    """
    LogMessageType을 MessageType으로 변환
    
    Args:
        log_type: 로그 메시지 타입
        
    Returns:
        MessageType: 변환된 텔레그램 메시지 타입
    """
    mapping = {
        LogMessageType.INFO: MessageType.INFO,
        LogMessageType.ERROR: MessageType.ERROR,
        LogMessageType.WARNING: MessageType.WARNING,
        LogMessageType.DEBUG: MessageType.INFO,
        LogMessageType.CRITICAL: MessageType.ERROR,
        LogMessageType.CONNECTION: MessageType.CONNECTION,
        LogMessageType.RECONNECT: MessageType.RECONNECT,
        LogMessageType.DISCONNECT: MessageType.DISCONNECT,
        LogMessageType.TRADE: MessageType.TRADE,
        LogMessageType.MARKET: MessageType.MARKET,
        LogMessageType.SYSTEM: MessageType.SYSTEM
    }
    return mapping.get(log_type, MessageType.INFO)

async def send_telegram_message(
    settings: Dict = None,  # settings 파라미터를 선택적으로 변경
    message_type: Union[str, LogMessageType, MessageType] = MessageType.INFO,
    data: Union[Dict[str, Union[str, int, float]], str] = None,
    retry_count: int = 0,
    timeout: float = 10.0
) -> bool:
    """
    텔레그램 메시지 전송
    
    Args:
        settings: 설정 데이터 (선택적, 사용하지 않음)
        message_type: 메시지 타입 (LogMessageType, MessageType 또는 문자열)
        data: 템플릿에 들어갈 데이터 또는 메시지 문자열
        retry_count: 현재 재시도 횟수
        timeout: API 요청 타임아웃 (초)
    
    Returns:
        bool: 전송 성공 여부
    """
    # LogMessageType을 MessageType으로 변환
    if isinstance(message_type, LogMessageType):
        message_type = convert_log_message_type(message_type)
    
    # 기존 텔레그램 전송 함수 호출 (settings 파라미터 없이)
    return await bot_send_telegram_message(None, message_type, data, retry_count, timeout)

# 편의 함수들 - settings 파라미터 없이 호출
async def send_error_notification(component: str = "", message: str = "") -> bool:
    """오류 메시지 전송"""
    return await send_error(None, component, message)

async def send_trade_notification(exchange_from: str = "", exchange_to: str = "",
                                symbol: str = "", amount: float = 0.0, price: float = 0.0, kimp: float = 0.0) -> bool:
    """거래 메시지 전송"""
    return await send_trade(None, exchange_from, exchange_to, symbol, amount, price, kimp)

async def send_profit_notification(amount: float = 0.0, percentage: float = 0.0, details: str = "") -> bool:
    """수익 메시지 전송"""
    return await send_profit(None, amount, percentage, details)

async def send_market_status_notification(usdt_price: float = 0.0, upbit_status: bool = False, bithumb_status: bool = False) -> bool:
    """시장 상태 메시지 전송"""
    return await send_market_status(None, usdt_price, upbit_status, bithumb_status)

async def send_system_status_notification(cpu_usage: float = 0.0, memory_usage: float = 0.0, uptime: str = "") -> bool:
    """시스템 상태 메시지 전송"""
    return await send_system_status(None, cpu_usage, memory_usage, uptime)

# 테스트 코드
if __name__ == "__main__":
    async def test_telegram():
        # 텔레그램 토큰 및 채팅 ID 확인
        logger.info(f"텔레그램 봇 토큰: {NOTIFICATION_BOT_TOKEN[:5]}...{NOTIFICATION_BOT_TOKEN[-5:] if NOTIFICATION_BOT_TOKEN else None}")
        logger.info(f"텔레그램 채팅 ID: {NOTIFICATION_CHAT_IDS}")
        
        # 테스트 메시지 전송
        await send_error_notification("텔레그램 봇", "테스트 에러 메시지")
        await send_system_status_notification(25.5, 40.2, "1일 2시간 30분")
        
    import asyncio
    asyncio.run(test_telegram())