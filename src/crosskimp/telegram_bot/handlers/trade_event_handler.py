"""
거래 이벤트 핸들러 모듈

거래 관련 이벤트를 처리합니다.
"""

from typing import Dict, Any
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from crosskimp.common.events.system_types import EventPaths

class TradeEventHandler:
    """거래 이벤트 처리 클래스"""
    
    def __init__(self, notifier):
        """
        거래 이벤트 핸들러 초기화
        
        Args:
            notifier: 알림 전송을 위한 TelegramNotifier 인스턴스
        """
        self.notifier = notifier
        self.logger = notifier.logger
        self.event_type = EventPaths.TELEGRAM_NOTIFY_TRADE
    
    async def handle(self, data: Dict[str, Any]):
        """
        거래 이벤트 처리
        
        Args:
            data: 거래 이벤트 데이터
        """
        try:
            trade_type = data.get("trade_type", "unknown")
            exchange = data.get("exchange", "unknown")
            symbol = data.get("symbol", "unknown")
            amount = data.get("amount", 0)
            price = data.get("price", 0)
            
            # 거래 정보 메시지 생성
            message = (
                f"💸 거래 실행 ({trade_type}):\n"
                f"- 거래소: {exchange}\n"
                f"- 심볼: {symbol}\n"
                f"- 수량: {amount}\n"
                f"- 가격: {price:,}원"
            )
            
            # 확인 버튼이 있는 알림 전송
            keyboard = [[InlineKeyboardButton("✅ 확인", callback_data="trade_ack")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.notifier.send_notification(message, level=self.notifier.NotificationLevel.INFO, reply_markup=reply_markup)
                
        except Exception as e:
            self.logger.error(f"거래 이벤트 처리 중 오류: {str(e)}") 