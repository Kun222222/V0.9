#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
거래소 및 시스템 메트릭 데이터 분석 프로그램
"""

import json
import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime, timedelta
import argparse
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


class MetricAnalyzer:
    """메트릭 데이터 분석 클래스"""
    
    def __init__(self, log_dir="src/logs/metrics"):
        """
        초기화 함수
        
        Args:
            log_dir (str): 메트릭 로그 파일이 저장된 디렉토리 경로
        """
        self.log_dir = log_dir
        self.data = None
        self.system_df = None
        self.exchange_dfs = {}
        
    def load_data(self, date_str=None):
        """
        특정 날짜 또는 모든 메트릭 데이터 파일 로드
        
        Args:
            date_str (str, optional): 'YYYYMMDD' 형식의 날짜 문자열
                                     None인 경우 모든 파일 로드
        
        Returns:
            bool: 데이터 로드 성공 여부
        """
        if date_str:
            file_pattern = f"metrics_{date_str}.json"
        else:
            file_pattern = "metrics_*.json"
            
        file_paths = glob.glob(os.path.join(self.log_dir, file_pattern))
        
        if not file_paths:
            print(f"경고: '{file_pattern}'에 해당하는 파일을 찾을 수 없습니다.")
            return False
        
        print(f"다음 파일들을 로드합니다: {file_paths}")    
        data_list = []
        
        for file_path in file_paths:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    file_data = json.load(f)
                    print(f"파일 '{file_path}' 로드 완료. 데이터 구조: {list(file_data.keys()) if isinstance(file_data, dict) else '리스트'}")
                    # JSON 구조에 맞게 수정: "metrics" 키에 있는 데이터 배열을 가져옴
                    if "metrics" in file_data and isinstance(file_data["metrics"], list):
                        data_list.extend(file_data["metrics"])
                        print(f"'metrics' 키에서 {len(file_data['metrics'])}개의 데이터 항목을 로드했습니다.")
                    elif isinstance(file_data, list):
                        data_list.extend(file_data)
                        print(f"리스트에서 {len(file_data)}개의 데이터 항목을 로드했습니다.")
                    else:
                        data_list.append(file_data)
                        print("단일 데이터 항목을 로드했습니다.")
            except Exception as e:
                print(f"파일 '{file_path}' 로드 중 오류 발생: {e}")
                
        if not data_list:
            print("로드된 데이터가 없습니다.")
            return False
        
        print(f"총 {len(data_list)}개의 데이터 항목을 로드했습니다.")    
        self.data = data_list
        self._preprocess_data()
        return True
    
    def _preprocess_data(self):
        """데이터 전처리 및 데이터프레임 생성"""
        if not self.data:
            return
            
        # 시스템 메트릭 데이터프레임 생성
        system_data = []
        for entry in self.data:
            if "timestamp" in entry and "system" in entry:
                system_row = {
                    "timestamp": datetime.fromisoformat(entry["timestamp"]),
                    "uptime": entry.get("uptime", 0)
                }
                system_row.update(entry["system"])
                system_data.append(system_row)
                
        if system_data:
            self.system_df = pd.DataFrame(system_data)
            self.system_df.sort_values("timestamp", inplace=True)
            
        # 거래소별 메트릭 데이터프레임 생성
        exchange_data = {}
        for entry in self.data:
            if "timestamp" in entry and "exchanges" in entry:
                timestamp = datetime.fromisoformat(entry["timestamp"])
                
                for exchange_name, exchange_metrics in entry["exchanges"].items():
                    if exchange_name not in exchange_data:
                        exchange_data[exchange_name] = []
                        
                    exchange_row = {"timestamp": timestamp}
                    exchange_row.update(exchange_metrics)
                    exchange_data[exchange_name].append(exchange_row)
        
        # 각 거래소별 데이터프레임 생성 및 정렬
        for exchange_name, data in exchange_data.items():
            if data:
                df = pd.DataFrame(data)
                df.sort_values("timestamp", inplace=True)
                self.exchange_dfs[exchange_name] = df
    
    def analyze_system_metrics(self, save_path=None):
        """
        시스템 메트릭 분석 및 시각화
        
        Args:
            save_path (str, optional): 그래프 저장 경로
        """
        if self.system_df is None or self.system_df.empty:
            print("시스템 메트릭 데이터가 없습니다.")
            return
            
        # 리소스 사용량 시각화 (CPU, 메모리, 디스크)
        fig = make_subplots(
            rows=3, cols=1,
            subplot_titles=("CPU 사용률 (%)", "메모리 사용률 (%)", "디스크 사용률 (%)"),
            shared_xaxes=True,
            vertical_spacing=0.1
        )
        
        # CPU 사용률
        fig.add_trace(
            go.Scatter(x=self.system_df["timestamp"], y=self.system_df["cpu_percent"],
                      mode="lines", name="CPU 사용률"),
            row=1, col=1
        )
        
        # 메모리 사용률
        fig.add_trace(
            go.Scatter(x=self.system_df["timestamp"], y=self.system_df["memory_percent"],
                      mode="lines", name="메모리 사용률"),
            row=2, col=1
        )
        
        # 디스크 사용률
        fig.add_trace(
            go.Scatter(x=self.system_df["timestamp"], y=self.system_df["disk_percent"],
                      mode="lines", name="디스크 사용률"),
            row=3, col=1
        )
        
        # 레이아웃 설정
        fig.update_layout(
            height=900,
            title_text="시스템 리소스 사용률 추이",
            template="plotly_white"
        )
        
        # 네트워크 트래픽 시각화
        network_fig = make_subplots(
            rows=1, cols=1,
            subplot_titles=("네트워크 트래픽 (GB)"),
        )
        
        # 바이트 단위를 GB로 변환
        network_fig.add_trace(
            go.Scatter(
                x=self.system_df["timestamp"], 
                y=self.system_df["network_bytes_sent"] / (1024**3),
                mode="lines", 
                name="송신 데이터"
            )
        )
        
        network_fig.add_trace(
            go.Scatter(
                x=self.system_df["timestamp"], 
                y=self.system_df["network_bytes_recv"] / (1024**3),
                mode="lines", 
                name="수신 데이터"
            )
        )
        
        network_fig.update_layout(
            height=500,
            title_text="네트워크 트래픽 추이",
            template="plotly_white"
        )
        
        # 결과 표시 또는 저장
        if save_path:
            fig.write_html(os.path.join(save_path, "system_resource_usage.html"))
            network_fig.write_html(os.path.join(save_path, "network_traffic.html"))
            
            # 통계 요약 보고서 생성
            self._generate_system_stats_report(save_path)
        else:
            fig.show()
            network_fig.show()
    
    def analyze_exchange_metrics(self, exchange_name=None, save_path=None):
        """
        특정 거래소 또는 모든 거래소의 메트릭 분석 및 시각화
        
        Args:
            exchange_name (str, optional): 분석할 거래소 이름
                                          None인 경우 모든 거래소 분석
            save_path (str, optional): 그래프 저장 경로
        """
        if not self.exchange_dfs:
            print("거래소 메트릭 데이터가 없습니다.")
            return
            
        exchange_names = [exchange_name] if exchange_name else list(self.exchange_dfs.keys())
        
        for name in exchange_names:
            if name not in self.exchange_dfs:
                print(f"'{name}' 거래소 데이터가 없습니다.")
                continue
                
            df = self.exchange_dfs[name]
            
            # 메시지 처리율 시각화
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=(f"{name} 메시지 처리율 (초당)", f"{name} 평균 처리 시간 (ms)"),
                shared_xaxes=True,
                vertical_spacing=0.1
            )
            
            # 메시지 처리율
            fig.add_trace(
                go.Scatter(x=df["timestamp"], y=df["message_rate"],
                          mode="lines", name="메시지 처리율"),
                row=1, col=1
            )
            
            # 평균 처리 시간
            fig.add_trace(
                go.Scatter(x=df["timestamp"], y=df["avg_processing_time"],
                          mode="lines", name="평균 처리 시간"),
                row=2, col=1
            )
            
            # 레이아웃 설정
            fig.update_layout(
                height=700,
                title_text=f"{name} 거래소 성능 지표",
                template="plotly_white"
            )
            
            # 연결 상태 및 오류 시각화
            error_fig = make_subplots(
                rows=1, cols=1,
                subplot_titles=(f"{name} 오류 및 재연결 횟수"),
            )
            
            error_fig.add_trace(
                go.Scatter(
                    x=df["timestamp"], 
                    y=df["error_count"].cumsum(),
                    mode="lines", 
                    name="누적 오류 횟수"
                )
            )
            
            error_fig.add_trace(
                go.Scatter(
                    x=df["timestamp"], 
                    y=df["reconnect_count"].cumsum(),
                    mode="lines", 
                    name="누적 재연결 횟수"
                )
            )
            
            error_fig.update_layout(
                height=500,
                title_text=f"{name} 연결 안정성",
                template="plotly_white"
            )
            
            # 결과 표시 또는 저장
            if save_path:
                exchange_dir = os.path.join(save_path, "exchanges", name)
                os.makedirs(exchange_dir, exist_ok=True)
                fig.write_html(os.path.join(exchange_dir, "performance_metrics.html"))
                error_fig.write_html(os.path.join(exchange_dir, "error_metrics.html"))
                
                # 통계 요약 보고서 생성
                self._generate_exchange_stats_report(name, exchange_dir)
            else:
                fig.show()
                error_fig.show()
    
    def compare_exchanges(self, metric_name, save_path=None):
        """
        모든 거래소 간 특정 메트릭 비교 분석
        
        Args:
            metric_name (str): 비교할 메트릭 이름 (예: 'message_rate', 'avg_processing_time' 등)
            save_path (str, optional): 그래프 저장 경로
        """
        if not self.exchange_dfs:
            print("거래소 메트릭 데이터가 없습니다.")
            return
            
        # 모든 거래소의 해당 메트릭을 하나의 그래프에 표시
        fig = go.Figure()
        
        for exchange_name, df in self.exchange_dfs.items():
            if metric_name in df.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df["timestamp"],
                        y=df[metric_name],
                        mode="lines",
                        name=exchange_name
                    )
                )
        
        # 한글 메트릭 이름 맵핑
        metric_name_kr = {
            "message_rate": "메시지 처리율 (초당)",
            "orderbook_rate": "호가창 업데이트율 (초당)",
            "avg_processing_time": "평균 처리 시간 (ms)",
            "latency_ms": "지연 시간 (ms)"
        }.get(metric_name, metric_name)
        
        # 레이아웃 설정
        fig.update_layout(
            height=600,
            title_text=f"거래소 간 {metric_name_kr} 비교",
            xaxis_title="시간",
            yaxis_title=metric_name_kr,
            template="plotly_white"
        )
        
        # 결과 표시 또는 저장
        if save_path:
            fig.write_html(os.path.join(save_path, f"exchange_comparison_{metric_name}.html"))
        else:
            fig.show()
    
    def _generate_system_stats_report(self, save_path):
        """시스템 메트릭 통계 요약 보고서 생성"""
        if self.system_df is None or self.system_df.empty:
            return
            
        report = {
            "기간": {
                "시작": self.system_df["timestamp"].min().strftime("%Y-%m-%d %H:%M:%S"),
                "종료": self.system_df["timestamp"].max().strftime("%Y-%m-%d %H:%M:%S"),
                "데이터 포인트 수": len(self.system_df)
            },
            "CPU 사용률 (%)": {
                "평균": round(self.system_df["cpu_percent"].mean(), 2),
                "최대": round(self.system_df["cpu_percent"].max(), 2),
                "최소": round(self.system_df["cpu_percent"].min(), 2),
                "표준편차": round(self.system_df["cpu_percent"].std(), 2)
            },
            "메모리 사용률 (%)": {
                "평균": round(self.system_df["memory_percent"].mean(), 2),
                "최대": round(self.system_df["memory_percent"].max(), 2),
                "최소": round(self.system_df["memory_percent"].min(), 2),
                "표준편차": round(self.system_df["memory_percent"].std(), 2)
            },
            "디스크 사용률 (%)": {
                "평균": round(self.system_df["disk_percent"].mean(), 2),
                "최대": round(self.system_df["disk_percent"].max(), 2),
                "최소": round(self.system_df["disk_percent"].min(), 2)
            },
            "네트워크 트래픽 (GB)": {
                "총 송신 데이터": round(self.system_df["network_bytes_sent"].max() / (1024**3), 2),
                "총 수신 데이터": round(self.system_df["network_bytes_recv"].max() / (1024**3), 2)
            }
        }
        
        # JSON 형식으로 저장
        with open(os.path.join(save_path, "system_stats_report.json"), 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
    
    def _generate_exchange_stats_report(self, exchange_name, save_path):
        """거래소 메트릭 통계 요약 보고서 생성"""
        if exchange_name not in self.exchange_dfs:
            return
            
        df = self.exchange_dfs[exchange_name]
        
        # 메시지 처리율과 평균 처리 시간에 대한 일간 통계 계산
        df['date'] = df['timestamp'].dt.date
        daily_stats = df.groupby('date').agg({
            'message_rate': ['mean', 'max', 'min', 'std'],
            'avg_processing_time': ['mean', 'max', 'min', 'std'],
            'error_count': 'sum',
            'reconnect_count': 'sum'
        }).reset_index()
        
        # 전체 데이터 요약
        total_stats = {
            "기간": {
                "시작": df["timestamp"].min().strftime("%Y-%m-%d %H:%M:%S"),
                "종료": df["timestamp"].max().strftime("%Y-%m-%d %H:%M:%S"),
                "데이터 포인트 수": len(df)
            },
            "메시지 처리율 (초당)": {
                "평균": round(df["message_rate"].mean(), 2),
                "최대": int(df["message_rate"].max()),
                "최소": int(df["message_rate"].min()),
                "표준편차": round(df["message_rate"].std(), 2)
            },
            "평균 처리 시간 (ms)": {
                "평균": round(df["avg_processing_time"].mean(), 3),
                "최대": round(df["avg_processing_time"].max(), 3),
                "최소": round(df["avg_processing_time"].min(), 3),
                "표준편차": round(df["avg_processing_time"].std(), 3)
            },
            "연결 안정성": {
                "총 오류 횟수": int(df["error_count"].sum()),
                "총 재연결 횟수": int(df["reconnect_count"].sum())
            },
            "데이터 전송량 (MB)": {
                "총 수신 데이터": round(df["bytes_received"].max() / (1024**2), 2),
                "총 송신 데이터": round(df["bytes_sent"].max() / (1024**2), 2) if "bytes_sent" in df.columns else 0
            }
        }
        
        # 일간 통계를 DataFrame에서 딕셔너리로 변환
        daily_stats_dict = {}
        for _, row in daily_stats.iterrows():
            date_str = str(row['date'])
            daily_stats_dict[date_str] = {
                "메시지 처리율 (초당)": {
                    "평균": round(row[('message_rate', 'mean')], 2),
                    "최대": int(row[('message_rate', 'max')]),
                    "최소": int(row[('message_rate', 'min')]),
                    "표준편차": round(row[('message_rate', 'std')], 2) if not np.isnan(row[('message_rate', 'std')]) else 0
                },
                "평균 처리 시간 (ms)": {
                    "평균": round(row[('avg_processing_time', 'mean')], 3),
                    "최대": round(row[('avg_processing_time', 'max')], 3),
                    "최소": round(row[('avg_processing_time', 'min')], 3),
                    "표준편차": round(row[('avg_processing_time', 'std')], 3) if not np.isnan(row[('avg_processing_time', 'std')]) else 0
                },
                "오류 및 재연결": {
                    "오류 횟수": int(row[('error_count', 'sum')]),
                    "재연결 횟수": int(row[('reconnect_count', 'sum')])
                }
            }
        
        # 종합 보고서 생성
        report = {
            "거래소_이름": exchange_name,
            "전체_통계": total_stats,
            "일간_통계": daily_stats_dict
        }
        
        # JSON 형식으로 저장
        with open(os.path.join(save_path, "exchange_stats_report.json"), 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
    
    def generate_performance_report(self, output_dir="reports"):
        """
        시스템 및 거래소 종합 성능 보고서 생성
        
        Args:
            output_dir (str): 보고서 저장 디렉토리
        """
        # 출력 디렉토리 생성
        os.makedirs(output_dir, exist_ok=True)
        
        # 시스템 메트릭 분석
        self.analyze_system_metrics(save_path=output_dir)
        
        # 모든 거래소 메트릭 분석
        for exchange_name in self.exchange_dfs.keys():
            self.analyze_exchange_metrics(exchange_name=exchange_name, save_path=output_dir)
        
        # 거래소 간 비교 분석
        key_metrics = ["message_rate", "orderbook_rate", "avg_processing_time", "latency_ms"]
        for metric in key_metrics:
            self.compare_exchanges(metric, save_path=output_dir)
        
        # 종합 보고서 인덱스 HTML 생성
        self._generate_report_index(output_dir)
        
        print(f"성능 보고서가 '{output_dir}' 디렉토리에 생성되었습니다.")
    
    def _generate_report_index(self, output_dir):
        """보고서 인덱스 HTML 파일 생성"""
        exchanges = list(self.exchange_dfs.keys())
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>거래소 및 시스템 성능 분석 보고서</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1, h2, h3 {{ color: #333; }}
                .section {{ margin-bottom: 20px; }}
                ul {{ list-style-type: disc; padding-left: 20px; }}
                a {{ color: #0066cc; text-decoration: none; }}
                a:hover {{ text-decoration: underline; }}
            </style>
        </head>
        <body>
            <h1>거래소 및 시스템 성능 분석 보고서</h1>
            <p>생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div class="section">
                <h2>시스템 메트릭</h2>
                <ul>
                    <li><a href="system_resource_usage.html">시스템 리소스 사용률</a></li>
                    <li><a href="network_traffic.html">네트워크 트래픽</a></li>
                    <li><a href="system_stats_report.json">시스템 통계 요약 (JSON)</a></li>
                </ul>
            </div>
            
            <div class="section">
                <h2>거래소 메트릭</h2>
        """
        
        for exchange in exchanges:
            html_content += f"""
                <h3>{exchange}</h3>
                <ul>
                    <li><a href="exchanges/{exchange}/performance_metrics.html">성능 지표</a></li>
                    <li><a href="exchanges/{exchange}/error_metrics.html">오류 및 재연결</a></li>
                    <li><a href="exchanges/{exchange}/exchange_stats_report.json">통계 요약 (JSON)</a></li>
                </ul>
            """
        
        html_content += """
            </div>
            
            <div class="section">
                <h2>거래소 간 비교</h2>
                <ul>
                    <li><a href="exchange_comparison_message_rate.html">메시지 처리율 비교</a></li>
                    <li><a href="exchange_comparison_orderbook_rate.html">호가창 업데이트율 비교</a></li>
                    <li><a href="exchange_comparison_avg_processing_time.html">평균 처리 시간 비교</a></li>
                    <li><a href="exchange_comparison_latency_ms.html">지연 시간 비교</a></li>
                </ul>
            </div>
        </body>
        </html>
        """
        
        with open(os.path.join(output_dir, "index.html"), 'w', encoding='utf-8') as f:
            f.write(html_content)


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="거래소 및 시스템 메트릭 데이터 분석 프로그램")
    parser.add_argument("--log-dir", type=str, default="src/logs/metrics",
                        help="메트릭 로그 파일이 저장된 디렉토리 경로")
    parser.add_argument("--date", type=str, help="분석할 날짜 (YYYYMMDD 형식)")
    parser.add_argument("--output-dir", type=str, default="reports",
                        help="보고서 저장 디렉토리")
    parser.add_argument("--exchange", type=str, help="분석할 특정 거래소 이름")
    
    args = parser.parse_args()
    
    print(f"메트릭 로그 디렉토리: {args.log_dir}")
    print(f"출력 디렉토리: {args.output_dir}")
    
    analyzer = MetricAnalyzer(log_dir=args.log_dir)
    success = analyzer.load_data(date_str=args.date)
    
    if not success:
        print("데이터 로드에 실패했습니다. 프로그램을 종료합니다.")
        return
    
    print("데이터 로드에 성공했습니다. 분석을 시작합니다.")
    
    if args.exchange:
        analyzer.analyze_exchange_metrics(exchange_name=args.exchange, save_path=args.output_dir)
    else:
        analyzer.generate_performance_report(output_dir=args.output_dir)


if __name__ == "__main__":
    main() 