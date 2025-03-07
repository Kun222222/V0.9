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
from crosskimp.ob_collector.config.config_loader import get_settings
from typing import Dict, Optional, List, Union, Any
import logging
import os
import json
import telegram
from telegram.ext import Updater

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
# 상수 정의
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

# 메시지 템플릿
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

# 설정
MAX_MESSAGE_LENGTH = 4096  # 텔레그램 메시지 최대 길이
MAX_RETRIES = 3           # 최대 재시도 횟수
RETRY_DELAY = 1.0         # 재시도 간격 (초)

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
    if len(formatted) > MAX_MESSAGE_LENGTH:
        formatted = formatted[:MAX_MESSAGE_LENGTH-3] + "..."
    
    return formatted

def validate_telegram_config(settings: Dict) -> bool:
    """설정 유효성 검증"""
    telegram_config = settings.get("notifications", {}).get("telegram", {})
    
    if not telegram_config.get("enabled"):
        logger.info("텔레그램 알림 비활성화 상태")
        return False
        
    if not telegram_config.get("token"):
        logger.error("텔레그램 토큰 누락")
        return False
        
    if not telegram_config.get("chat_id"):
        logger.error("텔레그램 chat_id 누락")
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
            
        # 설정 추출
        telegram_config = settings["notifications"]["telegram"]
        token = telegram_config["token"]
        chat_id = telegram_config["chat_id"]
        
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
        
        # API 요청
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    logger.debug("메시지 전송 성공")
                    return True
                    
                # 에러 응답
                error_data = await response.text()
                logger.error(
                    f"메시지 전송 실패 (HTTP {response.status}): {error_data}"
                )
                
                # 재시도
                if retry_count < MAX_RETRIES:
                    logger.info(
                        f"{retry_count + 1}번째 재시도 ({RETRY_DELAY}초 후)"
                    )
                    await asyncio.sleep(RETRY_DELAY)
                    return await send_telegram_message(
                        settings, message_type, data, retry_count + 1
                    )
                    
                return False
                
    except aiohttp.ClientError as e:
        logger.error(f"네트워크 오류: {e}")
        if retry_count < MAX_RETRIES:
            await asyncio.sleep(RETRY_DELAY)
            return await send_telegram_message(
                settings, message_type, data, retry_count + 1
            )
        return False
        
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}")
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
    """거래 알림 메시지 전송"""
    return await send_telegram_message(settings, MessageType.TRADE, {
        "exchange_from": exchange_from,
        "exchange_to": exchange_to,
        "symbol": symbol,
        "amount": amount,
        "price": price,
        "kimp": kimp
    })

async def send_profit(settings: Dict, amount: float, percentage: float, details: str) -> bool:
    """수익 발생 메시지 전송"""
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

# ============================
# 테스트 코드
# ============================
if __name__ == "__main__":
    async def test_telegram():
        """텔레그램 봇 테스트"""
        try:
            logger.info("테스트 시작")
            settings = get_settings()
            
            # 각 메시지 타입 테스트
            test_data = {
                MessageType.ERROR: {
                    "component": "테스트",
                    "message": "테스트 에러 메시지"
                },
                MessageType.TRADE: {
                    "exchange_from": "업비트",
                    "exchange_to": "바이낸스",
                    "symbol": "BTC",
                    "amount": 0.1,
                    "price": 50000000,
                    "kimp": 2.5
                },
                MessageType.PROFIT: {
                    "amount": 100000,
                    "percentage": 1.5,
                    "details": "BTC 거래 수익"
                },
                MessageType.MARKET: {
                    "usdt_price": 1320.50,
                    "upbit_status": True,
                    "bithumb_status": True
                },
                MessageType.SYSTEM: {
                    "cpu_usage": 45.2,
                    "memory_usage": 60.8,
                    "uptime": "1일 2시간 30분"
                }
            }
            
            for msg_type, data in test_data.items():
                logger.info(f"{msg_type} 메시지 테스트")
                success = await send_telegram_message(settings, msg_type, data)
                if success:
                    logger.info(f"{msg_type} 메시지 전송 성공")
                else:
                    logger.error(f"{msg_type} 메시지 전송 실패")
                await asyncio.sleep(1)  # API 레이트 리밋 고려
            
            logger.info("테스트 완료")
            
        except Exception as e:
            logger.error(f"테스트 중 오류 발생: {e}")
    
    # 테스트 실행
    asyncio.run(test_telegram())