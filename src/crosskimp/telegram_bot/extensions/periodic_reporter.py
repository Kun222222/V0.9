"""
텔레그램 봇 주기적 보고서 모듈

시스템 상태, 거래 내역 등의 정기 보고서를 생성하고 전송하는 기능을 담당합니다.
"""

import asyncio
import time
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime, timedelta
import traceback
import random

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.events.system_eventbus import get_event_bus
from crosskimp.common.events.system_types import (
    SystemEventType, PerformanceEventType
)

# 로거 설정
logger = get_unified_logger(component=SystemComponent.TELEGRAM.value)

class PeriodicReporter:
    """
    주기적 보고서 생성 및 전송 클래스
    """
    
    def __init__(self, bot: Bot, chat_ids: List[int]):
        """
        주기적 보고서 모듈 초기화
        
        Args:
            bot: 텔레그램 봇 인스턴스
            chat_ids: 보고서를 전송할 채팅 ID 목록
        """
        self.bot = bot
        self.chat_ids = chat_ids
        self.logger = logger
        
        # 이벤트 버스
        self.event_bus = get_event_bus()
        
        # 실행 중인 보고서 작업 목록
        self.running_tasks = {}
        
        # 보고서 스케줄 설정
        self.report_schedule = {
            'system_metrics': {
                'interval': 60,  # 1분마다 (테스트용)
                'last_run': 0,
                'enabled': True,
                'description': '시스템 메트릭 보고서'
            },
            'daily_trade_summary': {
                'interval': 86400,  # 24시간마다
                'last_run': 0,
                'enabled': True,
                'description': '일간 거래 요약'
            },
            'error_summary': {
                'interval': 43200,  # 12시간마다
                'last_run': 0,
                'enabled': True,
                'description': '오류 요약 보고서'
            },
            'weekly_performance': {
                'interval': 604800,  # 1주일마다
                'last_run': 0,
                'enabled': True,
                'description': '주간 성능 보고서'
            }
        }
        
        self.logger.info("주기적 보고서 모듈이 초기화되었습니다.")
    
    async def start(self) -> bool:
        """
        주기적 보고서 작업 시작
        
        Returns:
            bool: 시작 성공 여부
        """
        try:
            self.logger.info("주기적 보고서 작업을 시작합니다...")
            
            # 보고서 작업 시작
            self.running_tasks['main'] = asyncio.create_task(self._run_periodic_reports())
            
            self.logger.info("주기적 보고서 작업이 시작되었습니다.")
            return True
            
        except Exception as e:
            self.logger.error(f"주기적 보고서 시작 중 오류 발생: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
    
    async def stop(self) -> bool:
        """
        주기적 보고서 작업 중지
        
        Returns:
            bool: 중지 성공 여부
        """
        try:
            self.logger.info("주기적 보고서 작업을 중지합니다...")
            
            # 실행 중인 모든 작업 취소
            for task_name, task in self.running_tasks.items():
                if not task.done():
                    task.cancel()
            
            self.running_tasks = {}
            
            self.logger.info("주기적 보고서 작업이 중지되었습니다.")
            return True
            
        except Exception as e:
            self.logger.error(f"주기적 보고서 중지 중 오류 발생: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
    
    async def _run_periodic_reports(self):
        """
        설정된 스케줄에 따라 주기적으로 보고서 생성 및 전송
        """
        try:
            self.logger.debug("주기적 보고서 루프 시작")
            
            while True:
                current_time = time.time()
                
                # 각 보고서 유형 확인
                for report_type, config in self.report_schedule.items():
                    if not config['enabled']:
                        continue
                        
                    # 마지막 실행 후 간격 검사
                    if current_time - config['last_run'] >= config['interval']:
                        # 보고서 생성 및 전송
                        self.logger.debug(f"{report_type} 보고서 생성 시작")
                        await self._generate_and_send_report(report_type)
                        
                        # 마지막 실행 시간 업데이트
                        self.report_schedule[report_type]['last_run'] = current_time
                        
                # 10초마다 스케줄 체크 (테스트용)
                await asyncio.sleep(10)
                
        except asyncio.CancelledError:
            self.logger.info("주기적 보고서 루프가 취소되었습니다.")
            raise
            
        except Exception as e:
            self.logger.error(f"주기적 보고서 루프 실행 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
    
    async def _generate_and_send_report(self, report_type: str):
        """
        지정된 유형의 보고서 생성 및 전송
        
        Args:
            report_type: 보고서 유형
        """
        try:
            # 보고서 유형에 따라 적절한 데이터 수집 및 생성
            if report_type == 'system_metrics':
                data = self._collect_system_metrics()
                await self._send_system_metrics_report(data)
                
            elif report_type == 'daily_trade_summary':
                data = self._collect_daily_trade_data()
                await self._send_daily_trade_report(data)
                
            elif report_type == 'error_summary':
                data = self._collect_error_data()
                await self._send_error_summary_report(data)
                
            elif report_type == 'weekly_performance':
                data = self._collect_weekly_performance_data()
                await self._send_weekly_performance_report(data)
                
            else:
                self.logger.warning(f"알 수 없는 보고서 유형: {report_type}")
                
        except Exception as e:
            self.logger.error(f"{report_type} 보고서 생성 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
    
    # 데이터 수집 메서드들
    
    def _collect_system_metrics(self) -> Dict[str, Any]:
        """
        시스템 메트릭 데이터 수집
        
        Returns:
            Dict: 시스템 메트릭 데이터
        """
        # 실제로는 시스템 모니터링 모듈에서 데이터를 가져옴
        # 여기서는 샘플 데이터 반환
        
        return {
            'processes': {
                'running': ['orderbook_collector', 'telegram_bot'],
                'stopped': ['radar', 'trader'],
                'errors': []
            },
            'resources': {
                'cpu_usage': 23,
                'memory_usage': 1.2,
                'memory_total': 8,
                'disk_usage': 12,
                'disk_total': 500
            },
            'network': {
                'upbit_ws': {'status': 'connected', 'latency': 49},
                'bithumb_ws': {'status': 'connected', 'latency': 67},
                'binance_ws': {'status': 'connected', 'latency': 210},
                'bybit_ws': {'status': 'connected', 'latency': 180}
            },
            'errors': {
                'total_count': 0,
                'last_error_time': None,
                'last_error_msg': None
            }
        }
    
    def _collect_daily_trade_data(self) -> Dict[str, Any]:
        """
        일간 거래 데이터 수집
        
        Returns:
            Dict: 일간 거래 데이터
        """
        # 실제로는 거래 기록 DB에서 데이터를 가져옴
        # 여기서는 샘플 데이터 반환
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        return {
            'date': today,
            'trade_count': 32,
            'total_volume': 8750000,
            'profit': 34500,
            'profit_percentage': 0.39,
            'max_kimchi_premium': 3.8,
            'avg_kimchi_premium': 2.1,
            'hourly_profits': [
                1200, 2500, 3100, 4000, 3800, 2900, 2100, 1800,
                2200, 2600, 1900, 1400, 1100, 1000, 900, 1300,
                1500, 1700, 2000, 1800, 1600, 1400, 1200, 1000
            ],
            'trade_pairs': {
                'BTC/KRW': {'count': 20, 'profit': 21500},
                'ETH/KRW': {'count': 12, 'profit': 13000}
            }
        }
    
    def _collect_error_data(self) -> Dict[str, Any]:
        """
        오류 데이터 수집
        
        Returns:
            Dict: 오류 데이터
        """
        # 실제로는 로그나 오류 기록에서 데이터를 가져옴
        # 여기서는 샘플 데이터 반환
        
        return {
            'total_errors': 5,
            'error_by_component': {
                'orderbook_collector': 2,
                'radar': 0,
                'trader': 3,
                'telegram_bot': 0
            },
            'error_by_type': {
                'connection': 3,
                'timeout': 1,
                'api_error': 1
            },
            'recent_errors': [
                {
                    'time': '2023-11-15 14:30:22',
                    'component': 'trader',
                    'message': '업비트 API 호출 중 타임아웃 발생'
                },
                {
                    'time': '2023-11-15 12:45:18',
                    'component': 'orderbook_collector',
                    'message': '빗썸 웹소켓 연결 끊김'
                },
                {
                    'time': '2023-11-15 10:12:05',
                    'component': 'trader',
                    'message': '바이낸스 주문 API 오류'
                }
            ]
        }
    
    def _collect_weekly_performance_data(self) -> Dict[str, Any]:
        """
        주간 성능 데이터 수집
        
        Returns:
            Dict: 주간 성능 데이터
        """
        # 실제로는 성능 기록과 거래 기록에서 데이터를 가져옴
        # 여기서는 샘플 데이터 반환
        
        today = datetime.now()
        start_of_week = (today - timedelta(days=today.weekday())).strftime('%Y-%m-%d')
        end_of_week = (today - timedelta(days=today.weekday()) + timedelta(days=6)).strftime('%Y-%m-%d')
        
        # 1주일 동안의 일별 수익률 (%)
        daily_profits = [0.3, 0.4, 0.5, 0.1, -0.2, 0.3, 0.4]
        
        # 일별 누적 수익률 계산
        cumulative_profits = []
        cum_profit = 0
        for profit in daily_profits:
            cum_profit += profit
            cumulative_profits.append(cum_profit)
        
        return {
            'period': f"{start_of_week} ~ {end_of_week}",
            'total_trades': 283,
            'total_volume': 87520000,
            'total_profit': 1230500,
            'profit_percentage': 1.41,
            'daily_profits': daily_profits,
            'cumulative_profits': cumulative_profits,
            'best_day': {
                'day': '수요일',
                'profit': 0.5,
                'profit_amount': 132500
            },
            'worst_day': {
                'day': '금요일',
                'profit': -0.2,
                'profit_amount': -50000
            },
            'avg_trade_count_per_day': 40.4,
            'performance_metrics': {
                'avg_execution_time': 2.3,
                'avg_kimchi_premium': 2.23,
                'success_rate': 87.3
            }
        }
    
    # 보고서 전송 메서드들
    
    async def _send_system_metrics_report(self, data: Dict[str, Any]):
        """
        시스템 메트릭 보고서 전송
        
        Args:
            data: 시스템 메트릭 데이터
        """
        try:
            # 메시지 구성
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            message = (
                f"🖥️ 시스템 상태 보고서 ({current_time})\n\n"
                f"- 실행 중인 프로세스:\n"
            )
            
            # 프로세스 상태 정보
            for proc in data['processes']['running']:
                message += f"  ✅ {proc}\n"
            for proc in data['processes']['stopped']:
                message += f"  ❌ {proc} (중지됨)\n"
            for proc in data['processes']['errors']:
                message += f"  ⚠️ {proc} (오류)\n"
                
            message += f"\n- 시스템 리소스:\n"
            message += f"  CPU: {data['resources']['cpu_usage']}%\n"
            message += f"  메모리: {data['resources']['memory_usage']}GB / {data['resources']['memory_total']}GB\n"
            message += f"  디스크: {data['resources']['disk_usage']}GB / {data['resources']['disk_total']}GB\n"
            
            message += f"\n- 네트워크 상태:\n"
            for name, ws in data['network'].items():
                status_emoji = "✅" if ws['status'] == 'connected' else "❌"
                message += f"  {status_emoji} {name}: {ws['latency']}ms\n"
                
            # 오류 정보
            if data['errors']['total_count'] > 0:
                message += f"\n- 마지막 오류: {data['errors']['last_error_time']} - {data['errors']['last_error_msg']}"
            else:
                message += f"\n- 마지막 오류: 없음"
            
            # 액션 버튼
            keyboard = [
                [
                    InlineKeyboardButton("🔄 새로고침", callback_data="refresh_metrics"),
                    InlineKeyboardButton("📊 성능 차트", callback_data="show_performance_chart")
                ],
                [
                    InlineKeyboardButton("▶️ 시작", callback_data="start_all"),
                    InlineKeyboardButton("⏹️ 중지", callback_data="stop_all")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # 모든 허용된 채팅 ID에 전송
            for chat_id in self.chat_ids:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    reply_markup=reply_markup
                )
                
            self.logger.debug("시스템 메트릭 보고서 전송 완료")
            
        except Exception as e:
            self.logger.error(f"시스템 메트릭 보고서 전송 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
    
    async def _send_daily_trade_report(self, data: Dict[str, Any]):
        """
        일간 거래 보고서 전송
        
        Args:
            data: 일간 거래 데이터
        """
        try:
            # 메시지 구성
            message = (
                f"📅 일간 거래 요약 ({data['date']})\n\n"
                f"- 총 거래 수: {data['trade_count']}건\n"
                f"- 총 거래액: ₩{data['total_volume']:,}\n"
                f"- 총 수익: {'+' if data['profit'] >= 0 else ''}₩{data['profit']:,} ({data['profit_percentage']}%)\n"
                f"- 최대 김프: {data['max_kimchi_premium']}%\n"
                f"- 평균 김프: {data['avg_kimchi_premium']}%\n\n"
            )
            
            # 거래 쌍별 정보
            message += "- 거래 쌍별 실적:\n"
            for pair, stats in data['trade_pairs'].items():
                message += f"  {pair}: {stats['count']}건, {'+' if stats['profit'] >= 0 else ''}₩{stats['profit']:,}\n"
            
            # 액션 버튼
            keyboard = [
                [
                    InlineKeyboardButton("📊 일간 차트", callback_data="show_daily_chart"),
                    InlineKeyboardButton("📜 상세 내역", callback_data="show_daily_details")
                ],
                [
                    InlineKeyboardButton("📆 월간 요약", callback_data="show_monthly_summary")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # 채팅 ID 목록에 전송
            for chat_id in self.chat_ids:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    reply_markup=reply_markup
                )
                
                # 차트 전송을 위한 데이터 생성
                # 실제로는 ChartVisualizer 통해 차트 전송
                
            self.logger.debug("일간 거래 보고서 전송 완료")
            
        except Exception as e:
            self.logger.error(f"일간 거래 보고서 전송 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
    
    async def _send_error_summary_report(self, data: Dict[str, Any]):
        """
        오류 요약 보고서 전송
        
        Args:
            data: 오류 데이터
        """
        try:
            # 오류가 없으면 보고서 생략
            if data['total_errors'] == 0:
                self.logger.debug("오류 없음 - 오류 요약 보고서 생략")
                return
                
            # 메시지 구성
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            message = (
                f"⚠️ 오류 요약 보고서 ({current_time})\n\n"
                f"- 총 오류 수: {data['total_errors']}개\n\n"
            )
            
            # 컴포넌트별 오류
            message += "- 컴포넌트별 오류:\n"
            for component, count in data['error_by_component'].items():
                if count > 0:
                    message += f"  {component}: {count}개\n"
                    
            # 유형별 오류
            message += "\n- 유형별 오류:\n"
            for error_type, count in data['error_by_type'].items():
                if count > 0:
                    message += f"  {error_type}: {count}개\n"
                    
            # 최근 오류 목록
            message += "\n- 최근 오류 로그:\n"
            for i, error in enumerate(data['recent_errors']):
                message += f"  {i+1}. [{error['time']}] {error['component']}: {error['message']}\n"
            
            # 액션 버튼
            keyboard = [
                [
                    InlineKeyboardButton("🔄 새로고침", callback_data="refresh_errors"),
                    InlineKeyboardButton("📜 상세 로그", callback_data="show_error_logs")
                ],
                [
                    InlineKeyboardButton("🔧 문제 해결", callback_data="fix_common_issues")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # 채팅 ID 목록에 전송
            for chat_id in self.chat_ids:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    reply_markup=reply_markup
                )
                
            self.logger.debug("오류 요약 보고서 전송 완료")
            
        except Exception as e:
            self.logger.error(f"오류 요약 보고서 전송 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
    
    async def _send_weekly_performance_report(self, data: Dict[str, Any]):
        """
        주간 성능 보고서 전송
        
        Args:
            data: 주간 성능 데이터
        """
        try:
            # 메시지 구성
            message = (
                f"📊 주간 성능 보고서 ({data['period']})\n\n"
                f"- 총 거래 수: {data['total_trades']}건\n"
                f"- 총 거래액: ₩{data['total_volume']:,}\n"
                f"- 총 수익: {'+' if data['total_profit'] >= 0 else ''}₩{data['total_profit']:,} ({data['profit_percentage']}%)\n"
                f"- 일평균 거래: {data['avg_trade_count_per_day']}건\n\n"
            )
            
            # 최고/최저 수익일
            message += f"- 최고 수익일: {data['best_day']['day']} ({data['best_day']['profit']}%, ₩{data['best_day']['profit_amount']:,})\n"
            message += f"- 최저 수익일: {data['worst_day']['day']} ({data['worst_day']['profit']}%, ₩{data['worst_day']['profit_amount']:,})\n\n"
            
            # 성능 지표
            message += "- 성능 지표:\n"
            message += f"  평균 실행 시간: {data['performance_metrics']['avg_execution_time']}초\n"
            message += f"  평균 김프: {data['performance_metrics']['avg_kimchi_premium']}%\n"
            message += f"  성공률: {data['performance_metrics']['success_rate']}%\n"
            
            # 액션 버튼
            keyboard = [
                [
                    InlineKeyboardButton("📈 주간 차트", callback_data="show_weekly_chart"),
                    InlineKeyboardButton("📋 상세 보고서", callback_data="show_weekly_details")
                ],
                [
                    InlineKeyboardButton("📊 월간 비교", callback_data="show_monthly_comparison")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # 채팅 ID 목록에 전송
            for chat_id in self.chat_ids:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    reply_markup=reply_markup
                )
                
                # 차트 전송을 위한 데이터 생성
                # 실제로는 ChartVisualizer 통해 차트 전송
                
            self.logger.debug("주간 성능 보고서 전송 완료")
            
        except Exception as e:
            self.logger.error(f"주간 성능 보고서 전송 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
    
    # 스케줄 관리 메서드
    
    def update_schedule(self, report_type: str, interval: int = None, enabled: bool = None) -> bool:
        """
        보고서 스케줄 업데이트
        
        Args:
            report_type: 보고서 유형
            interval: 보고서 간격(초)
            enabled: 활성화 여부
            
        Returns:
            bool: 업데이트 성공 여부
        """
        try:
            if report_type not in self.report_schedule:
                self.logger.warning(f"알 수 없는 보고서 유형: {report_type}")
                return False
                
            if interval is not None:
                self.report_schedule[report_type]['interval'] = interval
                
            if enabled is not None:
                self.report_schedule[report_type]['enabled'] = enabled
                
            self.logger.info(f"{report_type} 보고서 스케줄이 업데이트되었습니다. 간격: {interval}, 활성화: {enabled}")
            return True
            
        except Exception as e:
            self.logger.error(f"보고서 스케줄 업데이트 중 오류: {str(e)}")
            return False
    
    def add_report_type(self, report_type: str, interval: int, enabled: bool = True, description: str = "") -> bool:
        """
        새 보고서 유형 추가
        
        Args:
            report_type: 보고서 유형
            interval: 보고서 간격(초)
            enabled: 활성화 여부
            description: 보고서 설명
            
        Returns:
            bool: 추가 성공 여부
        """
        try:
            if report_type in self.report_schedule:
                self.logger.warning(f"이미 존재하는 보고서 유형: {report_type}")
                return False
                
            self.report_schedule[report_type] = {
                'interval': interval,
                'last_run': 0,
                'enabled': enabled,
                'description': description
            }
            
            self.logger.info(f"새 보고서 유형이 추가되었습니다: {report_type}")
            return True
            
        except Exception as e:
            self.logger.error(f"보고서 유형 추가 중 오류: {str(e)}")
            return False
    
    async def send_immediate_report(self, report_type: str, chat_id: int = None) -> bool:
        """
        즉시 보고서 생성 및 전송
        
        Args:
            report_type: 보고서 유형
            chat_id: 특정 채팅 ID (None이면 모든 허용된 채팅 ID에 전송)
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            if report_type not in self.report_schedule:
                self.logger.warning(f"알 수 없는 보고서 유형: {report_type}")
                return False
                
            # 보고서 생성
            self.logger.info(f"즉시 {report_type} 보고서 생성 시작")
            
            # 채팅 ID 설정
            target_chat_ids = [chat_id] if chat_id else self.chat_ids
            
            # 데이터 수집 및 보고서 전송
            if report_type == 'system_metrics':
                data = self._collect_system_metrics()
                
                for id in target_chat_ids:
                    await self._send_custom_message(
                        id,
                        f"⏳ 시스템 메트릭 보고서를 생성 중입니다..."
                    )
                    
                await self._send_system_metrics_report(data)
                
            elif report_type == 'daily_trade_summary':
                data = self._collect_daily_trade_data()
                
                for id in target_chat_ids:
                    await self._send_custom_message(
                        id,
                        f"⏳ 일간 거래 요약 보고서를 생성 중입니다..."
                    )
                    
                await self._send_daily_trade_report(data)
                
            elif report_type == 'error_summary':
                data = self._collect_error_data()
                
                for id in target_chat_ids:
                    await self._send_custom_message(
                        id,
                        f"⏳ 오류 요약 보고서를 생성 중입니다..."
                    )
                    
                await self._send_error_summary_report(data)
                
            elif report_type == 'weekly_performance':
                data = self._collect_weekly_performance_data()
                
                for id in target_chat_ids:
                    await self._send_custom_message(
                        id,
                        f"⏳ 주간 성능 보고서를 생성 중입니다..."
                    )
                    
                await self._send_weekly_performance_report(data)
                
            return True
            
        except Exception as e:
            self.logger.error(f"즉시 보고서 전송 중 오류: {str(e)}")
            self.logger.error(traceback.format_exc())
            return False
    
    async def _send_custom_message(self, chat_id: int, message: str, reply_markup: InlineKeyboardMarkup = None) -> bool:
        """
        특정 채팅 ID에 메시지 전송
        
        Args:
            chat_id: 전송할 채팅 ID
            message: 전송할 메시지
            reply_markup: 인라인 키보드 (옵션)
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            await self.bot.send_message(
                chat_id=chat_id,
                text=message,
                reply_markup=reply_markup
            )
            return True
        except Exception as e:
            self.logger.error(f"메시지 전송 중 오류: {str(e)}")
            return False 