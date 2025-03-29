"""
메트릭 수집기

메시지 수집과 오류 추적을 위한 카운터 클래스들을 정의합니다.
"""

import time
import random
from typing import Dict, Any

# 효율적인 메시지 카운팅을 위한 클래스
class HybridMessageCounter:
    """
    하이브리드 메시지 카운터 클래스
    
    이 클래스는 두 가지 측정 방식을 결합합니다:
    1. 절대적 카운팅 - 모든 메시지 수를 카운트
    2. 샘플링 기반 속도 계산 - 단위 시간당 메시지 수 계산
    """
    
    def __init__(self, window_size=30):
        """초기화"""
        # 절대적 카운팅용 변수
        self.total_count = 0
        
        # 샘플링 기반 속도 계산용 변수
        self.window_size = window_size  # 초 단위 윈도우 크기
        self.samples = []  # (timestamp, count) 튜플 저장
        self.last_sample_time = time.time()
        self.sample_count = 0
        
    def update(self, count=1):
        """
        카운터 업데이트
        
        Args:
            count: 증가시킬 카운트 (기본값: 1)
        """
        # 절대적 카운팅 업데이트
        self.total_count += count
        
        # 샘플링 카운팅 업데이트
        self.sample_count += count
        
        # 샘플링 시간 확인
        current_time = time.time()
        elapsed = current_time - self.last_sample_time
        
        # 샘플링 간격 (기본 1초) 이상 지났으면 샘플 추가
        if elapsed >= 1.0:
            self.samples.append((current_time, self.sample_count / elapsed))
            self.sample_count = 0
            self.last_sample_time = current_time
            
            # 윈도우 크기 유지
            while len(self.samples) > 0 and current_time - self.samples[0][0] > self.window_size:
                self.samples.pop(0)
                
    def reset_rate_counter(self):
        """
        속도 계산을 위한 샘플링 데이터 초기화
        재연결 후 메시지 속도를 새로 측정하기 위해 사용
        총 메시지 카운트는 유지됨
        """
        # 샘플 데이터 초기화
        self.samples = []
        self.last_sample_time = time.time()
        self.sample_count = 0

    def get_metrics(self):
        """
        메트릭 데이터 조회
        
        Returns:
            Dict: 메트릭 데이터
        """
        current_time = time.time()
        
        # 현재 속도 계산
        if len(self.samples) > 0:
            # 최근 샘플들의 평균 속도
            rate = sum(rate for _, rate in self.samples) / len(self.samples)
        else:
            # 샘플이 없는 경우
            rate = 0
            
        # 결과 반환
        return {
            "total_estimated": self.total_count,  # 추정 총 메시지 수
            "rate": rate,  # 현재 초당 메시지 속도
            "sample_count": len(self.samples)  # 샘플 개수
        }

# 오류 카운터 클래스
class ErrorCounter:
    """오류 추적 카운터"""
    
    def __init__(self):
        """오류 카운터 초기화"""
        self.error_types = {
            "connection_errors": 0,
            "parsing_errors": 0, 
            "sequence_errors": 0,
            "timeout_errors": 0,
            "other_errors": 0
        }
        self.last_error_time = 0  # 마지막 오류 시간
        self.total_errors = 0  # 총 오류 수
        
    def update(self, error_type, count=1):
        """오류 발생 기록"""
        if error_type in self.error_types:
            self.error_types[error_type] += count
        else:
            self.error_types["other_errors"] += count
            
        self.total_errors += count
        self.last_error_time = time.time()
        
    def get_metrics(self):
        """오류 메트릭 조회"""
        return {
            "total": self.total_errors,
            "types": self.error_types,
            "last_error_time": self.last_error_time
        } 