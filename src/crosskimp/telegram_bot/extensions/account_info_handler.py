"""
텔레그램 봇 계좌 및 거래 정보 처리 모듈

사용자가 요청한 계좌 정보 및 거래 내역을 수집하고 표시합니다.
"""

import asyncio
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime, timedelta
import traceback
import random

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.events.system_eventbus import get_event_bus

# 로거 설정
logger = get_unified_logger(component=SystemComponent.TELEGRAM.value)

class AccountInfoHandler:
    """
    계좌 및 거래 정보 처리 클래스
    """
    
    def __init__(self, bot: Bot):
        """
        계좌 정보 핸들러 초기화
        
        Args:
            bot: 텔레그램 봇 인스턴스
        """
        self.bot = bot
        self.logger = logger
        
        # 이벤트 버스
        self.event_bus = get_event_bus()
        
        self.logger.info("계좌 정보 핸들러가 초기화되었습니다.")
    
    async def handle_account_request(self, chat_id: int, request_type: str, **kwargs) -> bool:
        """
        계좌 정보 요청 처리
        
        Args:
            chat_id: 요청한 채팅 ID
            request_type: 요청 유형 (account_balance, exchange_balance, asset_distribution, api_key_status)
            kwargs: 추가 매개변수
            
        Returns:
            bool: 처리 성공 여부
        """
        try:
            self.logger.debug(f"계좌 정보 요청 처리: {request_type}")
            
            # 요청 유형에 따라 적절한 처리
            if request_type == "account_balance":
                return await self._handle_account_balance(chat_id, **kwargs)
                
            elif request_type == "exchange_balance":
                exchange = kwargs.get("exchange")
                return await self._handle_exchange_balance(chat_id, exchange)
                
            elif request_type == "asset_distribution":
                return await self._handle_asset_distribution(chat_id, **kwargs)
                
            elif request_type == "api_key_status":
                return await self._handle_api_key_status(chat_id, **kwargs)
                
            else:
                # 지원하지 않는 요청 유형
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=f"⚠️ 지원하지 않는 계좌 정보 요청 유형: {request_type}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"계좌 정보 요청 처리 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 메시지 전송
            try:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=f"⚠️ 계좌 정보 요청 처리 중 오류가 발생했습니다: {str(e)}"
                )
            except Exception:
                self.logger.error("오류 메시지 전송도 실패했습니다.")
                
            return False
    
    async def handle_trade_request(self, chat_id: int, request_type: str, **kwargs) -> bool:
        """
        거래 정보 요청 처리
        
        Args:
            chat_id: 요청한 채팅 ID
            request_type: 요청 유형 (trade_daily, trade_monthly, trade_stats, trade_recent)
            kwargs: 추가 매개변수
            
        Returns:
            bool: 처리 성공 여부
        """
        try:
            self.logger.debug(f"거래 정보 요청 처리: {request_type}")
            
            # 요청 유형에 따라 적절한 처리
            if request_type == "trade_daily":
                date = kwargs.get("date")
                return await self._handle_trade_daily(chat_id, date)
                
            elif request_type == "trade_monthly":
                month = kwargs.get("month")
                return await self._handle_trade_monthly(chat_id, month)
                
            elif request_type == "trade_stats":
                period = kwargs.get("period", "all")
                return await self._handle_trade_stats(chat_id, period)
                
            elif request_type == "trade_recent":
                limit = kwargs.get("limit", 20)
                symbol = kwargs.get("symbol")
                exchange = kwargs.get("exchange")
                return await self._handle_trade_recent(chat_id, limit, symbol, exchange)
                
            else:
                # 지원하지 않는 요청 유형
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=f"⚠️ 지원하지 않는 거래 정보 요청 유형: {request_type}"
                )
                return False
                
        except Exception as e:
            self.logger.error(f"거래 정보 요청 처리 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 메시지 전송
            try:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=f"⚠️ 거래 정보 요청 처리 중 오류가 발생했습니다: {str(e)}"
                )
            except Exception:
                self.logger.error("오류 메시지 전송도 실패했습니다.")
                
            return False
    
    # 계좌 정보 처리 메서드들
    
    async def _handle_account_balance(self, chat_id: int, **kwargs) -> bool:
        """
        계좌 잔고 정보 처리
        
        Args:
            chat_id: 요청한 채팅 ID
            kwargs: 추가 매개변수
            
        Returns:
            bool: 처리 성공 여부
        """
        # 실제로는 거래소 API나 DB에서 데이터를 가져옴
        # 여기서는 샘플 데이터 사용
        
        # 진행 중 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text="⏳ 계좌 잔고 정보를 불러오는 중입니다..."
        )
        
        # 데이터 수집 시뮬레이션
        await asyncio.sleep(1)
        
        # 보유자산 현황
        message = (
            "💰 보유자산 현황:\n\n"
            "- 총 자산 가치: ₩12,560,000\n"
            "- 현금: ₩5,230,000\n"
            "- 암호화폐: ₩7,330,000\n\n"
            "- BTC: 0.025 (₩3,750,000)\n"
            "- ETH: 0.58 (₩2,900,000)\n"
            "- XRP: 1000 (₩680,000)"
        )
        
        # 액션 버튼
        keyboard = [
            [
                InlineKeyboardButton("📊 자산분포", callback_data="account_request:asset_distribution"),
                InlineKeyboardButton("🏦 거래소별", callback_data="account_request:exchange_balance")
            ],
            [
                InlineKeyboardButton("📜 거래내역", callback_data="trade_request:trade_recent"),
                InlineKeyboardButton("🔑 API 상태", callback_data="account_request:api_key_status")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text=message,
            reply_markup=reply_markup
        )
        
        return True
    
    async def _handle_exchange_balance(self, chat_id: int, exchange: Optional[str] = None) -> bool:
        """
        거래소별 잔고 정보 처리
        
        Args:
            chat_id: 요청한 채팅 ID
            exchange: 특정 거래소 (None이면 모든 거래소)
            
        Returns:
            bool: 처리 성공 여부
        """
        # 실제로는 거래소 API에서 데이터를 가져옴
        # 여기서는 샘플 데이터 사용
        
        # 진행 중 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text="⏳ 거래소별 잔고 정보를 불러오는 중입니다..."
        )
        
        # 데이터 수집 시뮬레이션
        await asyncio.sleep(1.5)
        
        # 특정 거래소 요청인 경우
        if exchange:
            if exchange.lower() == "upbit":
                message = (
                    "💱 업비트 잔고:\n\n"
                    "- KRW: ₩2,500,000\n"
                    "- BTC: 0.015 (₩2,250,000)\n"
                    "- ETH: 0.28 (₩1,400,000)\n\n"
                    "총 자산가치: ₩6,150,000"
                )
            elif exchange.lower() == "bithumb":
                message = (
                    "💱 빗썸 잔고:\n\n"
                    "- KRW: ₩2,730,000\n"
                    "- BTC: 0.01 (₩1,500,000)\n"
                    "- ETH: 0.3 (₩1,500,000)\n\n"
                    "총 자산가치: ₩5,730,000"
                )
            elif exchange.lower() == "binance":
                message = (
                    "💱 바이낸스 잔고:\n\n"
                    "- USDT: $500 (₩680,000)\n"
                    "- XRP: 1000 (₩680,000)\n\n"
                    "총 자산가치: ₩1,360,000"
                )
            else:
                message = f"⚠️ 지원하지 않는 거래소입니다: {exchange}"
        else:
            # 모든 거래소 정보
            message = (
                "💱 거래소별 잔고:\n\n"
                "- 업비트:\n"
                "  KRW: ₩2,500,000\n"
                "  BTC: 0.015 (₩2,250,000)\n"
                "  ETH: 0.28 (₩1,400,000)\n\n"
                "- 빗썸:\n"
                "  KRW: ₩2,730,000\n"
                "  BTC: 0.01 (₩1,500,000)\n"
                "  ETH: 0.3 (₩1,500,000)\n\n"
                "- 바이낸스:\n"
                "  USDT: $500 (₩680,000)\n"
                "  XRP: 1000 (₩680,000)"
            )
        
        # 거래소 선택 버튼
        keyboard = [
            [
                InlineKeyboardButton("업비트", callback_data="account_request:exchange_balance:upbit"),
                InlineKeyboardButton("빗썸", callback_data="account_request:exchange_balance:bithumb"),
                InlineKeyboardButton("바이낸스", callback_data="account_request:exchange_balance:binance")
            ],
            [
                InlineKeyboardButton("전체보기", callback_data="account_request:exchange_balance"),
                InlineKeyboardButton("뒤로", callback_data="account_request:account_balance")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text=message,
            reply_markup=reply_markup
        )
        
        return True
    
    async def _handle_asset_distribution(self, chat_id: int, **kwargs) -> bool:
        """
        자산 분포 정보 처리
        
        Args:
            chat_id: 요청한 채팅 ID
            kwargs: 추가 매개변수
            
        Returns:
            bool: 처리 성공 여부
        """
        # 실제로는 거래소 API에서 데이터를 가져옴
        # 여기서는 샘플 데이터 사용
        
        # 진행 중 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text="⏳ 자산 분포 정보를 불러오는 중입니다..."
        )
        
        # 데이터 수집 시뮬레이션
        await asyncio.sleep(1)
        
        # 자산 분포 메시지
        message = (
            "📊 자산 분포:\n\n"
            "- 현금 (KRW): 41.6% (₩5,230,000)\n"
            "- BTC: 29.9% (₩3,750,000)\n"
            "- ETH: 23.1% (₩2,900,000)\n"
            "- 기타: 5.4% (₩680,000)"
        )
        
        # 액션 버튼
        keyboard = [
            [
                InlineKeyboardButton("📊 파이차트 보기", callback_data="chart_request:asset_distribution"),
                InlineKeyboardButton("🔄 새로고침", callback_data="account_request:asset_distribution:refresh")
            ],
            [
                InlineKeyboardButton("뒤로", callback_data="account_request:account_balance")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text=message,
            reply_markup=reply_markup
        )
        
        # 차트 요청 이벤트 발송
        # 여기서는 바로 파이차트 전송 (실제로는 ChartVisualizer에 요청)
        try:
            # 파이 차트 데이터
            chart_data = {
                'labels': ['현금 (KRW)', 'BTC', 'ETH', '기타'],
                'values': [41.6, 29.9, 23.1, 5.4],
                'colors': ['#3498db', '#f39c12', '#2ecc71', '#95a5a6']
            }
            
            # 차트 요청 이벤트 발행
            self.event_bus.publish({
                'event_type': 'chart_request',
                'chart_type': 'pie',
                'chart_data': chart_data,
                'title': '자산 분포',
                'chat_id': chat_id
            })
            
        except Exception as e:
            self.logger.error(f"자산 분포 차트 요청 중 오류: {str(e)}")
        
        return True
    
    async def _handle_api_key_status(self, chat_id: int, **kwargs) -> bool:
        """
        API 키 상태 정보 처리
        
        Args:
            chat_id: 요청한 채팅 ID
            kwargs: 추가 매개변수
            
        Returns:
            bool: 처리 성공 여부
        """
        # 실제로는 거래소 API에서 데이터를 가져옴
        # 여기서는 샘플 데이터 사용
        
        # 진행 중 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text="⏳ API 키 상태 정보를 확인하는 중입니다..."
        )
        
        # 데이터 수집 시뮬레이션
        await asyncio.sleep(2)
        
        # API 키 상태 메시지
        message = (
            "📝 API 키 상태:\n\n"
            "- 업비트: ✅ 정상 (읽기/쓰기 권한)\n"
            "- 빗썸: ✅ 정상 (읽기/쓰기 권한)\n"
            "- 바이낸스: ✅ 정상 (읽기/쓰기 권한)\n"
            "- 바이비트: ❌ 키 만료 (갱신 필요)"
        )
        
        # 확인 버튼이 있는 알림 전송
        keyboard = [
            [
                InlineKeyboardButton("🔄 API 키 갱신", callback_data="api_refresh"),
                InlineKeyboardButton("🔍 상세 정보", callback_data="api_details")
            ],
            [
                InlineKeyboardButton("뒤로", callback_data="account_request:account_balance")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text=message,
            reply_markup=reply_markup
        )
        
        return True
    
    # 거래 정보 처리 메서드들
    
    async def _handle_trade_daily(self, chat_id: int, date: Optional[str] = None) -> bool:
        """
        일간 거래 정보 처리
        
        Args:
            chat_id: 요청한 채팅 ID
            date: 날짜 (YYYY-MM-DD 형식, None이면 오늘)
            
        Returns:
            bool: 처리 성공 여부
        """
        # 실제로는 DB에서 데이터를 가져옴
        # 여기서는 샘플 데이터 사용
        
        # 날짜 설정
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
            
        # 진행 중 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text=f"⏳ {date} 거래 정보를 불러오는 중입니다..."
        )
        
        # 데이터 수집 시뮬레이션
        await asyncio.sleep(1.5)
        
        # 일간 거래 요약 메시지
        message = (
            f"📅 일간 수익현황 ({date}):\n\n"
            "- 총 거래 수: 32건\n"
            "- 총 거래액: ₩8,750,000\n"
            "- 총 수익: +₩34,500 (0.39%)\n"
            "- 최대 김프: 3.8%\n"
            "- 평균 김프: 2.1%\n\n"
            "- BTC: 20건 (+₩21,500)\n"
            "- ETH: 12건 (+₩13,000)"
        )
        
        # 날짜 이동 및 액션 버튼
        current_date = datetime.strptime(date, '%Y-%m-%d')
        prev_date = (current_date - timedelta(days=1)).strftime('%Y-%m-%d')
        next_date = (current_date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        keyboard = [
            [
                InlineKeyboardButton("◀️ 이전일", callback_data=f"trade_request:trade_daily:{prev_date}"),
                InlineKeyboardButton("다음일 ▶️", callback_data=f"trade_request:trade_daily:{next_date}")
            ],
            [
                InlineKeyboardButton("📊 차트보기", callback_data=f"chart_request:trade_daily:{date}"),
                InlineKeyboardButton("📜 상세내역", callback_data=f"trade_request:trade_recent:{date}")
            ],
            [
                InlineKeyboardButton("뒤로", callback_data="menu:trades")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text=message,
            reply_markup=reply_markup
        )
        
        # 차트 요청 이벤트 발행
        try:
            # 차트 데이터 - 시간별 수익
            hours = list(range(0, 24))
            profits = [
                1200, 2500, 3100, 4000, 3800, 2900, 2100, 1800,
                2200, 2600, 1900, 1400, 1100, 1000, 900, 1300,
                1500, 1700, 2000, 1800, 1600, 1400, 1200, 1000
            ]
            
            chart_data = {
                'x': hours,
                'y': profits,
                'title': f"{date} 시간별 거래 수익",
                'xlabel': '시간',
                'ylabel': '수익 (원)'
            }
            
            # 차트 요청 이벤트 발행
            self.event_bus.publish({
                'event_type': 'chart_request',
                'chart_type': 'line',
                'chart_data': chart_data,
                'title': f"{date} 시간별 거래 수익",
                'chat_id': chat_id
            })
            
        except Exception as e:
            self.logger.error(f"일간 거래 차트 요청 중 오류: {str(e)}")
        
        return True
    
    async def _handle_trade_monthly(self, chat_id: int, month: Optional[str] = None) -> bool:
        """
        월간 거래 정보 처리
        
        Args:
            chat_id: 요청한 채팅 ID
            month: 월 (YYYY-MM 형식, None이면 이번 달)
            
        Returns:
            bool: 처리 성공 여부
        """
        # 실제로는 DB에서 데이터를 가져옴
        # 여기서는 샘플 데이터 사용
        
        # 월 설정
        if month is None:
            month = datetime.now().strftime('%Y-%m')
            
        # 인간 친화적 표시
        display_month = datetime.strptime(month, '%Y-%m').strftime('%Y년 %m월')
            
        # 진행 중 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text=f"⏳ {display_month} 거래 정보를 불러오는 중입니다..."
        )
        
        # 데이터 수집 시뮬레이션
        await asyncio.sleep(2)
        
        # 월간 거래 요약 메시지
        message = (
            f"📆 월간 수익현황 ({display_month}):\n\n"
            "- 총 거래 수: 283건\n"
            "- 총 거래액: ₩87,520,000\n"
            "- 총 수익: +₩1,230,500 (1.41%)\n"
            "- 일평균 수익: ₩41,017\n"
            "- 최고 수익일: 8일 (₩132,500)\n\n"
            "- BTC: 180건 (+₩820,500)\n"
            "- ETH: 103건 (+₩410,000)"
        )
        
        # 월 이동 및 액션 버튼
        current_month = datetime.strptime(month, '%Y-%m')
        prev_month = (current_month.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
        next_month = (current_month.replace(day=28) + timedelta(days=5)).strftime('%Y-%m')
        
        keyboard = [
            [
                InlineKeyboardButton("◀️ 이전월", callback_data=f"trade_request:trade_monthly:{prev_month}"),
                InlineKeyboardButton("다음월 ▶️", callback_data=f"trade_request:trade_monthly:{next_month}")
            ],
            [
                InlineKeyboardButton("📊 차트보기", callback_data=f"chart_request:trade_monthly:{month}"),
                InlineKeyboardButton("📅 일별보기", callback_data=f"trade_request:trade_daily")
            ],
            [
                InlineKeyboardButton("뒤로", callback_data="menu:trades")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text=message,
            reply_markup=reply_markup
        )
        
        # 차트 요청 이벤트 발행
        try:
            # 월 내의 일별 수익 차트 데이터
            days = list(range(1, 31))  # 1일부터 30일까지
            daily_profits = [
                4200, 3800, 3500, 4100, 5200, 6800, 8500, 13250, 9800, 8700,
                7600, 6500, 5400, 6200, 7100, 8000, 7500, 6900, 7200, 8100,
                7400, 6800, 5900, 6300, 7000, 8200, 7800, 6700, 5600, 4500
            ]
            
            chart_data = {
                'x': days,
                'y': daily_profits,
                'title': f"{display_month} 일별 거래 수익",
                'xlabel': '일',
                'ylabel': '수익 (원)'
            }
            
            # 차트 요청 이벤트 발행
            self.event_bus.publish({
                'event_type': 'chart_request',
                'chart_type': 'line',
                'chart_data': chart_data,
                'title': f"{display_month} 일별 거래 수익",
                'chat_id': chat_id
            })
            
        except Exception as e:
            self.logger.error(f"월간 거래 차트 요청 중 오류: {str(e)}")
        
        return True
    
    async def _handle_trade_stats(self, chat_id: int, period: str = "all") -> bool:
        """
        거래 통계 정보 처리
        
        Args:
            chat_id: 요청한 채팅 ID
            period: 기간 (all, month, week, day)
            
        Returns:
            bool: 처리 성공 여부
        """
        # 실제로는 DB에서 데이터를 가져옴
        # 여기서는 샘플 데이터 사용
        
        # 기간에 따른 라벨 설정
        period_label = {
            "all": "전체",
            "month": "월간",
            "week": "주간",
            "day": "일간"
        }.get(period, "전체")
            
        # 진행 중 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text=f"⏳ {period_label} 거래 통계 정보를 분석 중입니다..."
        )
        
        # 데이터 수집 시뮬레이션
        await asyncio.sleep(1.8)
        
        # 거래 통계 메시지
        message = (
            f"📊 {period_label} 거래통계:\n\n"
            "- 총 실행 거래: 283건\n"
            "- 성공 거래: 247건 (87.3%)\n"
            "- 실패 거래: 36건 (12.7%)\n"
            "- 평균 김프율: 2.23%\n"
            "- 평균 거래 수수료: 0.28%\n"
            "- 평균 거래 소요시간: 2.3초"
        )
        
        # 기간 선택 및 액션 버튼
        keyboard = [
            [
                InlineKeyboardButton("일간", callback_data="trade_request:trade_stats:day"),
                InlineKeyboardButton("주간", callback_data="trade_request:trade_stats:week"),
                InlineKeyboardButton("월간", callback_data="trade_request:trade_stats:month"),
                InlineKeyboardButton("전체", callback_data="trade_request:trade_stats:all")
            ],
            [
                InlineKeyboardButton("📊 차트보기", callback_data=f"chart_request:trade_stats:{period}"),
                InlineKeyboardButton("📜 상세내역", callback_data="trade_request:trade_recent")
            ],
            [
                InlineKeyboardButton("뒤로", callback_data="menu:trades")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text=message,
            reply_markup=reply_markup
        )
        
        # 차트 요청 이벤트 발행 (파이차트 - 성공률)
        try:
            # 성공/실패 파이차트 데이터
            chart_data = {
                'labels': ['성공 거래', '실패 거래'],
                'values': [87.3, 12.7],
                'colors': ['#2ecc71', '#e74c3c']
            }
            
            # 차트 요청 이벤트 발행
            self.event_bus.publish({
                'event_type': 'chart_request',
                'chart_type': 'pie',
                'chart_data': chart_data,
                'title': f"{period_label} 거래 성공률",
                'chat_id': chat_id
            })
            
        except Exception as e:
            self.logger.error(f"거래 통계 차트 요청 중 오류: {str(e)}")
        
        return True
    
    async def _handle_trade_recent(self, chat_id: int, limit: int = 20, symbol: Optional[str] = None, exchange: Optional[str] = None) -> bool:
        """
        최근 거래 내역 처리
        
        Args:
            chat_id: 요청한 채팅 ID
            limit: 조회할 거래 수
            symbol: 심볼 필터 (예: BTC)
            exchange: 거래소 필터 (예: upbit)
            
        Returns:
            bool: 처리 성공 여부
        """
        # 실제로는 DB에서 데이터를 가져옴
        # 여기서는 샘플 데이터 사용
        
        # 필터링 라벨 구성
        filter_label = ""
        if symbol:
            filter_label += f" - {symbol}"
        if exchange:
            filter_label += f" - {exchange}"
            
        # 진행 중 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text=f"⏳ 최근 거래 내역을 불러오는 중입니다{filter_label}..."
        )
        
        # 데이터 수집 시뮬레이션
        await asyncio.sleep(1.5)
        
        # 최근 거래 내역 메시지
        message = (
            f"📜 최근 주문내역 ({limit}개){filter_label}:\n\n"
            "1. BTC 매수 - 업비트 - 0.01 BTC - ₩15,230,000 - 11/15 14:32\n"
            "2. BTC 매도 - 빗썸 - 0.01 BTC - ₩15,380,000 - 11/15 14:33\n"
            "3. ETH 매수 - 업비트 - 0.5 ETH - ₩1,230,000 - 11/15 13:22\n"
            "4. ETH 매도 - 빗썸 - 0.5 ETH - ₩1,243,000 - 11/15 13:24\n"
            "5. XRP 매수 - 바이낸스 - 1000 XRP - ₩678,000 - 11/15 12:05\n"
            "6. BTC 매수 - 업비트 - 0.02 BTC - ₩15,250,000 - 11/15 11:35\n"
            "7. BTC 매도 - 빗썸 - 0.02 BTC - ₩15,390,000 - 11/15 11:36\n"
            "8. ETH 매수 - 업비트 - 0.3 ETH - ₩1,235,000 - 11/15 10:47\n"
            "9. ETH 매도 - 빗썸 - 0.3 ETH - ₩1,250,000 - 11/15 10:48\n"
            "10. BTC 매수 - 업비트 - 0.015 BTC - ₩15,220,000 - 11/15 09:12\n"
            "...\n"
            "더 많은 거래 내역은 웹 대시보드에서 확인 가능합니다."
        )
        
        # 필터링 및 페이징 버튼
        keyboard = [
            [
                InlineKeyboardButton("BTC 필터", callback_data="trade_request:trade_recent:20:BTC"),
                InlineKeyboardButton("ETH 필터", callback_data="trade_request:trade_recent:20:ETH"),
                InlineKeyboardButton("필터 해제", callback_data="trade_request:trade_recent")
            ],
            [
                InlineKeyboardButton("◀️ 이전", callback_data="trade_request:trade_recent:prev"),
                InlineKeyboardButton("다음 ▶️", callback_data="trade_request:trade_recent:next")
            ],
            [
                InlineKeyboardButton("뒤로", callback_data="menu:trades")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # 메시지 전송
        await self.bot.send_message(
            chat_id=chat_id,
            text=message,
            reply_markup=reply_markup
        )
        
        return True 