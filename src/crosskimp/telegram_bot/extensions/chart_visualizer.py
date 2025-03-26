"""
텔레그램 봇 차트 시각화 모듈

다양한 데이터 시각화를 생성하고 텔레그램으로 전송하는 기능을 담당합니다.
"""

import io
import asyncio
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime, timedelta
import traceback
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # GUI 없이 사용하기 위한 설정
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.figure import Figure
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.ticker as ticker

from telegram import Bot, InlineKeyboardMarkup

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent

# 로거 설정
logger = get_unified_logger(component=SystemComponent.TELEGRAM.value)

class ChartVisualizer:
    """
    차트 생성 및 텔레그램 전송 클래스
    """
    
    def __init__(self, bot: Bot):
        """
        차트 시각화 모듈 초기화
        
        Args:
            bot: 텔레그램 봇 인스턴스
        """
        self.bot = bot
        self.logger = logger
        
        # 차트 스타일 설정
        plt.style.use('dark_background')
        self.default_figsize = (10, 6)
        self.default_dpi = 100
        
        # 차트 폰트 설정
        font_properties = {
            'family': 'DejaVu Sans',
            'weight': 'bold',
            'size': 12
        }
        plt.rc('font', **font_properties)
        plt.rc('axes', titlesize=14, labelsize=12)
        plt.rc('xtick', labelsize=10)
        plt.rc('ytick', labelsize=10)
        plt.rc('legend', fontsize=10)
        
        self.logger.info("차트 시각화 모듈이 초기화되었습니다.")
    
    async def send_chart(self, 
                         title: str, 
                         chat_id: int, 
                         chart_type: str = 'line', 
                         data: Optional[Dict[str, Any]] = None, 
                         caption: Optional[str] = None,
                         reply_markup: Optional[InlineKeyboardMarkup] = None) -> bool:
        """
        차트를 생성하고 텔레그램으로 전송
        
        Args:
            title: 차트 제목
            chat_id: 전송할 채팅 ID
            chart_type: 차트 유형 ('line', 'bar', 'pie', 'candle', 'scatter')
            data: 차트 데이터
            caption: 차트 캡션 (옵션)
            reply_markup: 인라인 키보드 (옵션)
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            self.logger.debug(f"'{chart_type}' 유형의 차트 생성 시작: {title}")
            
            # 데이터가 없으면 샘플 데이터 생성
            if data is None:
                data = self._get_sample_data(chart_type)
            
            # 차트 유형에 따라 적절한 그래프 생성
            plt.figure(figsize=self.default_figsize, dpi=self.default_dpi)
            
            if chart_type == 'pie':
                self._create_pie_chart(data, title)
            elif chart_type == 'bar':
                self._create_bar_chart(data, title)
            elif chart_type == 'line':
                self._create_line_chart(data, title)
            elif chart_type == 'candle':
                self._create_candle_chart(data, title)
            elif chart_type == 'scatter':
                self._create_scatter_chart(data, title)
            else:
                self.logger.warning(f"지원하지 않는 차트 유형: {chart_type}")
                return False
            
            # 차트를 바이트 스트림으로 변환
            img_buffer = io.BytesIO()
            plt.savefig(img_buffer, format='png', bbox_inches='tight')
            img_buffer.seek(0)
            
            # 차트 전송
            self.logger.debug(f"생성된 차트를 채팅 ID {chat_id}로 전송 중...")
            await self.bot.send_photo(
                chat_id=chat_id,
                photo=img_buffer,
                caption=caption or f"{title} 차트",
                reply_markup=reply_markup
            )
            
            # 메모리 정리
            plt.close()
            
            self.logger.debug(f"차트 전송 완료: {title}")
            return True
            
        except Exception as e:
            self.logger.error(f"차트 생성/전송 중 오류 발생: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 발생 시 텍스트 메시지로 대체하여 전송
            try:
                error_message = f"⚠️ 차트 생성 중 오류가 발생했습니다: {str(e)}"
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=error_message,
                    reply_markup=reply_markup
                )
            except Exception:
                self.logger.error("오류 메시지 전송도 실패했습니다.")
                
            return False
    
    def _get_sample_data(self, chart_type: str) -> Dict[str, Any]:
        """
        샘플 차트 데이터 생성
        
        Args:
            chart_type: 차트 유형
            
        Returns:
            Dict: 샘플 데이터
        """
        if chart_type == 'pie':
            return {
                'labels': ['업비트', '빗썸', '바이낸스', '바이비트', '기타'],
                'values': [40, 25, 20, 10, 5],
                'colors': ['#ff9500', '#3498db', '#f1c40f', '#2ecc71', '#95a5a6']
            }
            
        elif chart_type == 'candle':
            # 캔들스틱 차트용 OHLC 데이터
            today = datetime.now()
            dates = [today - timedelta(days=i) for i in range(14, -1, -1)]
            
            np.random.seed(42)  # 재현 가능한 결과를 위한 시드 설정
            
            # 최근 15일간의 BTC 가격 시뮬레이션
            closes = np.random.normal(35000, 1000, 15)  # 종가
            highs = closes + np.random.normal(500, 200, 15)  # 고가
            lows = closes - np.random.normal(500, 200, 15)  # 저가
            opens = closes + np.random.normal(0, 300, 15)  # 시가
            
            # 거래량
            volumes = np.random.normal(100, 30, 15)
            
            # 데이터프레임 생성
            ohlc_data = []
            for i, date in enumerate(dates):
                # mplfinance 형식에 맞게 데이터 변환
                date_value = mdates.date2num(date)
                ohlc_data.append([date_value, opens[i], highs[i], lows[i], closes[i], volumes[i]])
            
            return {
                'data': ohlc_data,
                'ticker': 'BTC/KRW'
            }
            
        elif chart_type == 'line':
            # 시계열 라인 차트 데이터
            today = datetime.now()
            dates = [today - timedelta(days=i) for i in range(29, -1, -1)]
            
            np.random.seed(42)
            
            # 김프 변동 시뮬레이션
            base_value = 3.0  # 기본 김프 %
            kimchi_premium = [base_value + np.random.normal(0, 0.5) for _ in range(30)]
            
            return {
                'x': dates,
                'y': kimchi_premium,
                'title': '비트코인 김프 변동 (최근 30일)',
                'xlabel': '날짜',
                'ylabel': '김프율 (%)'
            }
            
        elif chart_type == 'bar':
            # 막대 그래프 데이터
            return {
                'labels': ['1일', '7일', '30일', '90일', '1년'],
                'values': [0.5, 2.3, 8.7, 21.2, 65.8],
                'colors': ['#3498db', '#2ecc71', '#f1c40f', '#e67e22', '#e74c3c'],
                'title': '기간별 누적 수익률 (%)',
                'ylabel': '수익률 (%)'
            }
            
        elif chart_type == 'scatter':
            # 산점도 데이터
            np.random.seed(42)
            
            # 빗썸 vs 업비트 가격 산점도
            upbit_prices = np.random.normal(35000000, 300000, 50)
            bithumb_multiplier = np.random.normal(1.02, 0.01, 50)  # 평균 2% 김프
            bithumb_prices = upbit_prices * bithumb_multiplier
            
            return {
                'x': upbit_prices,
                'y': bithumb_prices,
                'title': '업비트 vs 빗썸 BTC 가격 비교',
                'xlabel': '업비트 가격 (원)',
                'ylabel': '빗썸 가격 (원)'
            }
            
        else:
            # 기본 라인 차트
            x = list(range(1, 31))
            y = [i + np.random.normal(0, 3) for i in x]
            
            return {
                'x': x,
                'y': y,
                'title': '샘플 데이터',
                'xlabel': 'X축',
                'ylabel': 'Y축'
            }
    
    def _create_pie_chart(self, data: Dict[str, Any], title: str):
        """
        파이 차트 생성
        
        Args:
            data: 차트 데이터
            title: 차트 제목
        """
        # 파이 차트
        plt.pie(
            data['values'],
            labels=data['labels'],
            autopct='%1.1f%%',
            startangle=90,
            colors=data.get('colors'),
            shadow=True,
            wedgeprops={'edgecolor': 'white', 'linewidth': 1}
        )
        plt.title(title, pad=20)
        plt.axis('equal')  # 원형 비율 유지
        
        # 범례 추가
        plt.legend(data['labels'], loc='upper right', bbox_to_anchor=(1.0, 0.9))
    
    def _create_bar_chart(self, data: Dict[str, Any], title: str):
        """
        막대 차트 생성
        
        Args:
            data: 차트 데이터
            title: 차트 제목
        """
        # 막대 차트
        bars = plt.bar(
            data['labels'],
            data['values'],
            color=data.get('colors'),
            width=0.6,
            edgecolor='white',
            linewidth=1
        )
        
        # 막대 위에 값 표시
        for bar in bars:
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width()/2.,
                height + 0.3,
                f'{height}%',
                ha='center',
                va='bottom',
                fontweight='bold'
            )
        
        plt.title(title)
        plt.ylabel(data.get('ylabel', '값'))
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # y축 범위 설정 (여유 공간 추가)
        max_value = max(data['values'])
        plt.ylim(0, max_value * 1.2)
    
    def _create_line_chart(self, data: Dict[str, Any], title: str):
        """
        라인 차트 생성
        
        Args:
            data: 차트 데이터
            title: 차트 제목
        """
        # 라인 차트
        plt.plot(
            data['x'],
            data['y'],
            marker='o',
            linestyle='-',
            linewidth=2,
            markersize=5,
            color='#3498db'
        )
        
        plt.title(title)
        plt.xlabel(data.get('xlabel', 'X축'))
        plt.ylabel(data.get('ylabel', 'Y축'))
        plt.grid(True, linestyle='--', alpha=0.7)
        
        # 날짜 형식인 경우 x축 포맷 지정
        if isinstance(data['x'][0], datetime):
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=5))
            plt.gcf().autofmt_xdate()
    
    def _create_candle_chart(self, data: Dict[str, Any], title: str):
        """
        캔들스틱 차트 생성
        
        Args:
            data: 차트 데이터
            title: 차트 제목
        """
        ohlc_data = data['data']
        ticker = data.get('ticker', 'BTC/KRW')
        
        # 메인 축 생성
        ax1 = plt.subplot(1, 1, 1)
        
        # 캔들스틱 차트 그리기
        candlestick_ohlc(
            ax1,
            ohlc_data,
            width=0.6,
            colorup='#2ecc71',
            colordown='#e74c3c'
        )
        
        # 축 설정
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        ax1.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        ax1.set_xlabel('날짜')
        ax1.set_ylabel('가격 (KRW)')
        
        # 그리드 추가
        ax1.grid(True, linestyle='--', alpha=0.5)
        
        # 제목 설정
        plt.title(f"{title} - {ticker}")
        
        plt.gcf().autofmt_xdate()
    
    def _create_scatter_chart(self, data: Dict[str, Any], title: str):
        """
        산점도 차트 생성
        
        Args:
            data: 차트 데이터
            title: 차트 제목
        """
        plt.scatter(
            data['x'],
            data['y'],
            alpha=0.7,
            s=50,
            c='#3498db',
            edgecolor='white',
            linewidth=0.5
        )
        
        # 추세선 추가 (선형 회귀)
        z = np.polyfit(data['x'], data['y'], 1)
        p = np.poly1d(z)
        plt.plot(
            data['x'],
            p(data['x']),
            "r--",
            linewidth=2,
            alpha=0.8
        )
        
        # 동일 가격 기준선 추가
        min_val = min(min(data['x']), min(data['y']))
        max_val = max(max(data['x']), max(data['y']))
        plt.plot(
            [min_val, max_val],
            [min_val, max_val],
            'k-',
            linewidth=1,
            alpha=0.5
        )
        
        plt.title(title)
        plt.xlabel(data.get('xlabel', 'X축'))
        plt.ylabel(data.get('ylabel', 'Y축'))
        plt.grid(True, linestyle='--', alpha=0.5)
        
        # 축 단위를 천 단위로 표시
        plt.gca().xaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
        plt.gca().yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
    
    async def send_multi_chart(self,
                              title: str,
                              chat_id: int,
                              datasets: List[Dict[str, Any]],
                              chart_type: str = 'line',
                              caption: Optional[str] = None,
                              reply_markup: Optional[InlineKeyboardMarkup] = None) -> bool:
        """
        여러 데이터셋이 포함된 차트 전송
        
        Args:
            title: 차트 제목
            chat_id: 전송할 채팅 ID
            datasets: 여러 데이터셋 목록
            chart_type: 차트 유형 ('line', 'bar')
            caption: 차트 캡션 (옵션)
            reply_markup: 인라인 키보드 (옵션)
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            self.logger.debug(f"다중 데이터셋 차트 생성 시작: {title}")
            
            # 차트 생성
            plt.figure(figsize=self.default_figsize, dpi=self.default_dpi)
            
            # 차트 유형에 따라 다중 데이터셋 플로팅
            if chart_type == 'line':
                # 여러 개의 라인 차트
                for i, dataset in enumerate(datasets):
                    label = dataset.get('label', f'시리즈 {i+1}')
                    color = dataset.get('color', f'C{i}')  # 자동 색상 순환
                    
                    plt.plot(
                        dataset['x'],
                        dataset['y'],
                        marker=dataset.get('marker', 'o'),
                        linestyle=dataset.get('linestyle', '-'),
                        linewidth=dataset.get('linewidth', 2),
                        markersize=dataset.get('markersize', 5),
                        color=color,
                        label=label
                    )
                
            elif chart_type == 'bar':
                # 그룹화된 막대 차트
                n_datasets = len(datasets)
                bar_width = 0.8 / n_datasets
                
                for i, dataset in enumerate(datasets):
                    x = range(len(dataset['labels']))
                    offset = i * bar_width - (n_datasets - 1) * bar_width / 2
                    
                    bars = plt.bar(
                        [xi + offset for xi in x],
                        dataset['values'],
                        width=bar_width,
                        color=dataset.get('color', f'C{i}'),
                        edgecolor='white',
                        linewidth=1,
                        label=dataset.get('label', f'시리즈 {i+1}')
                    )
                
                plt.xticks(range(len(datasets[0]['labels'])), datasets[0]['labels'])
                
            else:
                self.logger.warning(f"다중 데이터셋에 지원되지 않는 차트 유형: {chart_type}")
                return False
            
            # 공통 설정
            plt.title(title)
            plt.xlabel(datasets[0].get('xlabel', 'X축'))
            plt.ylabel(datasets[0].get('ylabel', 'Y축'))
            plt.grid(True, linestyle='--', alpha=0.5)
            plt.legend()
            
            # 날짜 형식인 경우 x축 포맷 지정
            if chart_type == 'line' and isinstance(datasets[0]['x'][0], datetime):
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
                plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=5))
                plt.gcf().autofmt_xdate()
            
            # 차트를 바이트 스트림으로 변환
            img_buffer = io.BytesIO()
            plt.savefig(img_buffer, format='png', bbox_inches='tight')
            img_buffer.seek(0)
            
            # 차트 전송
            await self.bot.send_photo(
                chat_id=chat_id,
                photo=img_buffer,
                caption=caption or f"{title} 차트",
                reply_markup=reply_markup
            )
            
            # 메모리 정리
            plt.close()
            
            self.logger.debug(f"다중 데이터셋 차트 전송 완료: {title}")
            return True
            
        except Exception as e:
            self.logger.error(f"다중 데이터셋 차트 생성/전송 중 오류 발생: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 발생 시 텍스트 메시지로 대체하여 전송
            try:
                error_message = f"⚠️ 다중 데이터셋 차트 생성 중 오류가 발생했습니다: {str(e)}"
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=error_message,
                    reply_markup=reply_markup
                )
            except Exception:
                self.logger.error("오류 메시지 전송도 실패했습니다.")
                
            return False
            
    async def send_kimchi_premium_chart(self,
                                       chat_id: int,
                                       data: Optional[Dict[str, Any]] = None,
                                       caption: Optional[str] = None,
                                       reply_markup: Optional[InlineKeyboardMarkup] = None) -> bool:
        """
        김프 차트 생성 및 전송
        
        Args:
            chat_id: 전송할 채팅 ID
            data: 차트 데이터 (없으면 샘플 데이터 사용)
            caption: 차트 캡션 (옵션)
            reply_markup: 인라인 키보드 (옵션)
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            self.logger.debug("김프 차트 생성 시작")
            
            # 데이터가 없으면 샘플 데이터 생성
            if data is None:
                # 샘플 김프 데이터 생성
                today = datetime.now()
                dates = [today - timedelta(days=i) for i in range(29, -1, -1)]
                
                np.random.seed(42)
                
                # BTC, ETH 김프 변동 시뮬레이션
                base_btc = 3.0  # 기본 BTC 김프 %
                base_eth = 2.5  # 기본 ETH 김프 %
                
                btc_premium = [base_btc + np.random.normal(0, 0.5) for _ in range(30)]
                eth_premium = [base_eth + np.random.normal(0, 0.6) for _ in range(30)]
                
                data = {
                    'dates': dates,
                    'btc_premium': btc_premium,
                    'eth_premium': eth_premium
                }
            
            # 차트 생성
            plt.figure(figsize=self.default_figsize, dpi=self.default_dpi)
            
            # BTC 김프 라인
            plt.plot(
                data['dates'],
                data['btc_premium'],
                marker='o',
                linestyle='-',
                linewidth=2,
                markersize=5,
                color='#f39c12',
                label='BTC 김프'
            )
            
            # ETH 김프 라인
            plt.plot(
                data['dates'],
                data['eth_premium'],
                marker='s',
                linestyle='-',
                linewidth=2,
                markersize=5,
                color='#3498db',
                label='ETH 김프'
            )
            
            # 0% 기준선
            plt.axhline(y=0, color='red', linestyle='--', alpha=0.5)
            
            # 차트 설정
            plt.title('김프 변동 추이 (최근 30일)')
            plt.xlabel('날짜')
            plt.ylabel('김프율 (%)')
            plt.grid(True, linestyle='--', alpha=0.5)
            plt.legend()
            
            # x축 날짜 포맷
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
            plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=5))
            plt.gcf().autofmt_xdate()
            
            # y축 음수 값도 표시
            plt.ylim(min(min(data['btc_premium']), min(data['eth_premium'])) - 1,
                     max(max(data['btc_premium']), max(data['eth_premium'])) + 1)
            
            # 차트를 바이트 스트림으로 변환
            img_buffer = io.BytesIO()
            plt.savefig(img_buffer, format='png', bbox_inches='tight')
            img_buffer.seek(0)
            
            # 차트 전송
            await self.bot.send_photo(
                chat_id=chat_id,
                photo=img_buffer,
                caption=caption or "BTC/ETH 김프 변동 추이 (최근 30일)",
                reply_markup=reply_markup
            )
            
            # 메모리 정리
            plt.close()
            
            self.logger.debug("김프 차트 전송 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"김프 차트 생성/전송 중 오류 발생: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            # 오류 발생 시 텍스트 메시지로 대체하여 전송
            try:
                error_message = f"⚠️ 김프 차트 생성 중 오류가 발생했습니다: {str(e)}"
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=error_message,
                    reply_markup=reply_markup
                )
            except Exception:
                self.logger.error("오류 메시지 전송도 실패했습니다.")
                
            return False