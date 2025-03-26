"""
정보 요청 이벤트 핸들러 모듈

텔레그램을 통한 정보 요청 이벤트를 처리합니다.
"""

from typing import Dict, Any
from crosskimp.common.events.system_types import EventPaths

class InfoRequestHandler:
    """정보 요청 이벤트 처리 클래스"""
    
    def __init__(self, notifier):
        """
        정보 요청 이벤트 핸들러 초기화
        
        Args:
            notifier: 알림 전송을 위한 TelegramNotifier 인스턴스
        """
        self.notifier = notifier
        self.logger = notifier.logger
        self.event_type = EventPaths.TELEGRAM_COMMANDER_EXECUTE
    
    async def handle(self, data: Dict[str, Any]):
        """
        텔레그램에서의 정보 요청 처리
        
        Args:
            data: 정보 요청 이벤트 데이터
        """
        try:
            request_type = data.get("request_type", "")
            chat_id = data.get("chat_id")
            
            if not chat_id or not request_type:
                return
                
            # 요청 유형별 처리
            if request_type.startswith("system_"):
                await self._forward_system_info_request(request_type, chat_id, data)
            elif request_type.startswith("account_") or request_type.startswith("exchange_") or request_type.startswith("asset_") or request_type.startswith("api_"):
                await self._forward_account_info_request(request_type, chat_id, data)
            elif request_type.startswith("trade_"):
                await self._forward_trade_info_request(request_type, chat_id, data)
            else:
                await self.notifier.send_direct_message(
                    chat_id, 
                    f"⚠️ 알 수 없는 정보 요청 유형: {request_type}"
                )
                
        except Exception as e:
            self.logger.error(f"정보 요청 처리 중 오류: {str(e)}")
    
    async def _forward_system_info_request(self, request_type: str, chat_id: int, data: Dict[str, Any]):
        """시스템 정보 요청 전달"""
        # 기본 응답
        await self.notifier.send_direct_message(
            chat_id,
            "⏳ 시스템 정보 요청을 처리 중입니다...\n\n" +
            "상세 시스템 정보는 확장 모듈을 로드해야 이용할 수 있습니다. " +
            "자세한 내용은 시스템 관리자에게 문의하세요."
        )
    
    async def _forward_account_info_request(self, request_type: str, chat_id: int, data: Dict[str, Any]):
        """계좌 정보 요청 전달"""
        # 기본 응답
        await self.notifier.send_direct_message(
            chat_id,
            "⏳ 계좌 정보 요청을 처리 중입니다...\n\n" +
            "상세 계좌 정보는 확장 모듈을 로드해야 이용할 수 있습니다. " +
            "자세한 내용은 시스템 관리자에게 문의하세요."
        )
    
    async def _forward_trade_info_request(self, request_type: str, chat_id: int, data: Dict[str, Any]):
        """거래 정보 요청 전달"""
        # 기본 응답
        await self.notifier.send_direct_message(
            chat_id,
            "⏳ 거래 정보 요청을 처리 중입니다...\n\n" +
            "상세 거래 정보는 확장 모듈을 로드해야 이용할 수 있습니다. " +
            "자세한 내용은 시스템 관리자에게 문의하세요."
        ) 