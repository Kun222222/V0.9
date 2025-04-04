"""
데이터 수신 클래스

오더북 수집기로부터 오더북 데이터를 수신합니다.
"""

import asyncio
import time
import threading
import json
import redis
from typing import Callable, List, Dict, Any, Optional
import logging

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.config.app_config import get_config
from crosskimp.radar.caching.orderbook_cache import OrderbookCache
from crosskimp.ob_collector.obcollector import OrderbookCollectorManager

# Redis 설정
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_CHANNEL_PATTERN = 'orderbook:*'  # 구독할 채널 패턴

class DataReceiver:
    """
    데이터 수신 클래스
    
    오더북 수집기(ob_collector)의 콜백 시스템을 활용하여
    오더북 데이터를 수신하고 처리합니다.
    Redis 구독을 통해서도 데이터를 수신할 수 있습니다.
    """
    
    def __init__(self):
        """초기화"""
        self.logger = get_unified_logger(SystemComponent.RADAR.value)
        self.logger.info("데이터 수신부 초기화 중...")
        
        # 설정 로드
        self.config = get_config()
        
        # 오더북 캐시 인스턴스
        self.orderbook_cache = OrderbookCache()
        
        # 콜백 함수 목록
        self.callbacks = []
        
        # Redis 설정
        self.use_redis = True  # Redis 사용 여부
        self.redis_client = None
        self.redis_pubsub = None
        self.redis_thread = None
        
        # 상태 변수
        self.is_running = False
        self.stats = {
            'received': 0,
            'processed': 0,
            'errors': 0,
            'start_time': 0,
            'last_report_time': 0,
            'redis_received': 0  # Redis를 통해 수신한 메시지 수
        }
        
        self.logger.info("데이터 수신부 초기화 완료")
    
    async def start(self) -> bool:
        """
        데이터 수신부 시작
        
        Returns:
            bool: 시작 성공 여부
        """
        try:
            self.logger.info("데이터 수신부 시작 중...")
            
            if self.is_running:
                self.logger.warning("데이터 수신부가 이미 실행 중입니다.")
                return True
            
            # 상태 초기화
            self.is_running = True
            self.stats['start_time'] = time.time()
            self.stats['last_report_time'] = time.time()
            
            # Redis 연결 및 구독 설정
            if self.use_redis:
                try:
                    self.redis_client = redis.Redis(
                        host=REDIS_HOST, 
                        port=REDIS_PORT, 
                        db=REDIS_DB
                    )
                    # Redis 연결 테스트
                    self.redis_client.ping()
                    
                    # PubSub 객체 생성 및 채널 구독
                    self.redis_pubsub = self.redis_client.pubsub()
                    self.redis_pubsub.psubscribe(REDIS_CHANNEL_PATTERN)
                    
                    # 리스너 스레드 시작
                    self.redis_thread = threading.Thread(
                        target=self._redis_listener_thread,
                        daemon=True
                    )
                    self.redis_thread.start()
                    
                    self.logger.info("Redis 구독 시작 완료")
                except Exception as e:
                    self.logger.error(f"Redis 연결 실패: {str(e)}")
                    self.logger.warning("Redis 없이 콜백 방식으로만 실행합니다.")
                    self.use_redis = False
            
            # 오더북 수집기 싱글톤 인스턴스 가져오기 (Redis 방식과 병행)
            try:
                # 이미 실행 중인 인스턴스를 참조하기 위해 여러 방법 시도
                # 1. 먼저 직접 인스턴스 생성 (이미 실행 중인 경우 동일 인스턴스 반환)
                ob_collector = OrderbookCollectorManager()
                
                # 2. 실행 여부 확인
                if not hasattr(ob_collector, 'is_running') or not ob_collector.is_running:
                    self.logger.warning("오더북 수집기가 실행 중이지 않습니다. 콜백 등록만 진행합니다.")
                
                # 콜백 등록
                ob_collector.add_orderbook_callback(self.process_orderbook)
                self.logger.info("오더북 수집기에 콜백 등록 완료")
                
            except Exception as e:
                if not self.use_redis:
                    self.logger.error(f"오더북 수집기 접근 중 오류: {str(e)}. 콜백 등록에 실패했습니다.")
                    return False
                else:
                    self.logger.warning(f"오더북 수집기 접근 중 오류: {str(e)}. Redis 구독으로 계속합니다.")
            
            # 통계 보고 태스크 시작
            self.stats_task = asyncio.create_task(self._report_stats())
            
            self.logger.info("데이터 수신부 시작 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"데이터 수신부 시작 중 오류: {str(e)}", exc_info=True)
            self.is_running = False
            return False
    
    def _redis_listener_thread(self):
        """Redis 메시지 구독 및 처리 스레드"""
        self.logger.info("Redis 리스너 스레드 시작")
        
        try:
            # pubsub.listen()은 메시지를 기다리는 제너레이터를 반환
            for message in self.redis_pubsub.listen():
                # 종료 체크
                if not self.is_running:
                    break
                
                try:
                    # pmessage 타입의 메시지만 처리 (패턴 매칭 구독)
                    if message['type'] == 'pmessage':
                        # 메시지 복호화
                        data = json.loads(message['data'])
                        channel = message['channel'].decode('utf-8')
                        
                        # 채널에서 거래소 코드 추출 (예: 'orderbook:binance_spot' -> 'binance_spot')
                        exchange = channel.split(':', 1)[1] if ':' in channel else 'unknown'
                        
                        # 거래소 정보가 없으면 데이터에서 찾아봄
                        if exchange == 'unknown' and 'exchange' in data:
                            exchange = data['exchange']
                        
                        # 로깅 (1000개 메시지마다)
                        if self.stats['redis_received'] % 1000 == 0:
                            symbol = data.get('symbol', 'unknown')
                            self.logger.debug(f"Redis 메시지 수신: {exchange}/{symbol}")
                        
                        # 메시지 처리 (기존 처리 메서드 재사용)
                        self.process_orderbook(data)
                        
                        # Redis 수신 통계 업데이트
                        self.stats['redis_received'] += 1
                        
                except json.JSONDecodeError as e:
                    self.logger.error(f"Redis 메시지 디코딩 오류: {str(e)}")
                except Exception as e:
                    self.logger.error(f"Redis 메시지 처리 중 오류: {str(e)}")
        
        except Exception as e:
            if self.is_running:  # 종료 과정에서 발생한 오류는 무시
                self.logger.error(f"Redis 리스너 스레드 오류: {str(e)}")
        finally:
            if self.redis_pubsub:
                try:
                    self.redis_pubsub.punsubscribe()
                except:
                    pass
            self.logger.info("Redis 리스너 스레드 종료")
    
    async def stop(self) -> None:
        """데이터 수신부 종료"""
        if not self.is_running:
            return
            
        self.logger.info("데이터 수신부 종료 중...")
        
        # 상태 변경
        self.is_running = False
        
        # Redis 정리
        if self.redis_pubsub:
            try:
                self.redis_pubsub.punsubscribe()
            except Exception as e:
                self.logger.error(f"Redis 구독 해제 중 오류: {str(e)}")
        
        # Redis 스레드 종료 대기
        if self.redis_thread and self.redis_thread.is_alive():
            try:
                self.redis_thread.join(timeout=3)
                if self.redis_thread.is_alive():
                    self.logger.warning("Redis 스레드가 3초 안에 종료되지 않았습니다.")
            except Exception as e:
                self.logger.error(f"Redis 스레드 종료 대기 중 오류: {str(e)}")
        
        # Redis 클라이언트 연결 종료
        if self.redis_client:
            try:
                self.redis_client.close()
            except Exception as e:
                self.logger.error(f"Redis 연결 종료 중 오류: {str(e)}")
        
        # 태스크 종료 대기
        if hasattr(self, 'stats_task'):
            try:
                await asyncio.wait_for(self.stats_task, timeout=5)
            except asyncio.TimeoutError:
                self.logger.warning("통계 태스크 종료 대기 시간 초과")
        
        self.logger.info("데이터 수신부 종료 완료")
    
    def add_callback(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        콜백 함수 등록
        
        Args:
            callback: 오더북 데이터를 처리할 콜백 함수
        """
        if callback not in self.callbacks:
            self.callbacks.append(callback)
            self.logger.debug(f"콜백 함수 등록됨 (총 {len(self.callbacks)}개)")
    
    def process_orderbook(self, data: Dict[str, Any]) -> None:
        """
        오더북 데이터 처리
        
        이 메서드는 ob_collector 콜백 또는 Redis 구독을 통해 호출됩니다.
        
        Args:
            data: 오더북 데이터
        """
        if not self.is_running:
            return
            
        try:
            # 통계 업데이트
            self.stats['received'] += 1
            
            # 캐시 업데이트
            self.orderbook_cache.update(data)
            
            # 등록된 모든 콜백 함수 호출
            for callback in self.callbacks:
                try:
                    callback(data)
                except Exception as e:
                    self.logger.error(f"콜백 실행 중 오류: {str(e)}")
            
            # 통계 업데이트
            self.stats['processed'] += 1
            
        except Exception as e:
            self.logger.error(f"오더북 데이터 처리 중 오류: {str(e)}")
            self.stats['errors'] += 1
    
    async def _report_stats(self) -> None:
        """통계 보고 루프"""
        self.logger.info("통계 보고 루프 시작")
        
        while self.is_running:
            try:
                current_time = time.time()
                elapsed = current_time - self.stats['last_report_time']
                
                if elapsed >= 10:  # 10초마다 통계 보고
                    total_elapsed = current_time - self.stats['start_time']
                    rate = self.stats['received'] / total_elapsed if total_elapsed > 0 else 0
                    
                    # Redis 수신 정보 추가
                    redis_info = ""
                    if self.use_redis:
                        redis_rate = self.stats['redis_received'] / total_elapsed if total_elapsed > 0 else 0
                        redis_percentage = (self.stats['redis_received'] / max(1, self.stats['received'])) * 100
                        redis_info = f", Redis 수신 {self.stats['redis_received']}건 ({redis_rate:.1f}건/초, {redis_percentage:.1f}%)"
                    
                    self.logger.info(
                        f"수신 통계: 수신 {self.stats['received']}건 "
                        f"(평균 {rate:.1f}건/초), "
                        f"처리 {self.stats['processed']}건, "
                        f"오류 {self.stats['errors']}건{redis_info}"
                    )
                    
                    # 캐시 통계도 함께 출력
                    self.logger.info(
                        f"캐시 통계: 업데이트 {self.orderbook_cache.stats['updates']}건, "
                        f"히트 {self.orderbook_cache.stats['hits']}건, "
                        f"미스 {self.orderbook_cache.stats['misses']}건"
                    )
                    
                    self.stats['last_report_time'] = current_time
                
                await asyncio.sleep(1)  # 1초 대기
                
            except Exception as e:
                self.logger.error(f"통계 보고 중 오류: {str(e)}")
                await asyncio.sleep(5)  # 오류 발생 시 5초 대기
        
        self.logger.info("통계 보고 루프 종료")


# 싱글톤 인스턴스
_data_receiver_instance = None

def get_data_receiver() -> DataReceiver:
    """
    데이터 수신부 싱글톤 인스턴스 반환
    
    Returns:
        DataReceiver: 데이터 수신부 인스턴스
    """
    global _data_receiver_instance
    
    if _data_receiver_instance is None:
        _data_receiver_instance = DataReceiver()
    
    return _data_receiver_instance 