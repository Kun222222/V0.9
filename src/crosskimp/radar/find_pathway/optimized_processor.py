import json
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import time
import os
import configparser

class OptimizedKimpProcessor:
    """최적화된 김프 차이 계산 프로세서"""
    
    def __init__(self, apply_fee_discount=True):
        """최적화된 김프 처리기 초기화"""
        self.domestic_exchanges = ['upbit', 'bithumb']
        self.foreign_exchanges = ['binance_spot', 'bybit_spot']  # bybit_spot 추가
        
        self.n_dom = len(self.domestic_exchanges)
        self.n_for = len(self.foreign_exchanges)
        
        self.idx_to_coin = {}
        self.coin_to_idx = {}
        self.n_coins = 0
        
        # 김프율 저장소
        self.premiums = None
        self.valid_pairs = None
        
        # 오더북 데이터 저장소
        self.orderbooks = {exchange: {} for exchange in self.domestic_exchanges + self.foreign_exchanges}
        
        # 최적 기회 저장
        self.best_opportunity = None
        
        # 기본 수수료 설정 (설정 파일 로드 실패 시 사용)
        self.exchange_fees = {}
        for exchange in self.domestic_exchanges:
            self.exchange_fees[exchange] = 0.0005  # 0.05%
        for exchange in self.foreign_exchanges:
            self.exchange_fees[exchange] = 0.001  # 해외 거래소는 0.1%로 설정
        
        # 기본 전송 수수료
        self.transfer_fee = 0.0002  # 0.02%로 변경
        
        # 수수료 설정 로드 시도 - 로드 후에 해외 거래소와 전송 수수료 재설정
        self._load_fee_settings()
        
        # 해외 거래소 및 전송 수수료 강제 설정
        for exchange in self.foreign_exchanges:
            self.exchange_fees[exchange] = 0.001  # 해외 거래소는 무조건 0.1%로 설정
        self.transfer_fee = 0.0002  # 전송 수수료 무조건 0.02%로 설정
        
        # 수익 코인 매수에 대한 추가 수수료 (숏 포지션 오픈+클로즈 수수료)
        self.profit_coin_extra_fee = 0.0011  # 숏 오픈+클로즈 수수료 0.11%
        
        logging.info(f"수수료 설정 강제 변경: 해외 거래소 0.1%, 전송 수수료 0.02%, 수익 코인 추가 수수료(숏) 0.11%")
        
        # 환율
        self.usdt_krw = 1350.0
        
        # 최저/최고 김프 캐싱 구조
        self.min_premium_cache = {
            'premium': float('inf'),
            'coin_idx': -1,
            'dom_idx': -1,
            'for_idx': -1
        }
        self.max_premium_cache = {
            'premium': float('-inf'),
            'coin_idx': -1,
            'dom_idx': -1,
            'for_idx': -1
        }
        
        # 마지막 업데이트된 코인 추적
        self.last_updated_coin = None
        
        # 캐시 초기화 여부
        self.is_cache_initialized = False
        
        # 성능 측정용 카운터
        self.perf_stats = {
            'process_count': 0,
            'update_premiums_time': 0,
            'find_opportunity_time': 0,
            'resize_arrays_time': 0,
            'add_coin_time': 0,
            'update_cache_time': 0,
            'method_times': {},  # 각 메소드별 실행 시간 저장
            'last_report_time': time.time(),
            'report_interval': 30  # 30초마다 성능 보고
        }
        
        # 마지막 처리한 거래 경로
        self.last_opportunity_path = None
        
        self.apply_fee_discount = apply_fee_discount
        
        logging.debug(f"OptimizedKimpProcessor 초기화: 국내거래소={self.domestic_exchanges}, 해외거래소={self.foreign_exchanges}")
        logging.debug(f"수수료 설정: {self.exchange_fees}, 전송 수수료: {self.transfer_fee:.4%}")
    
    def _load_fee_settings(self):
        """설정 파일에서 거래소별 수수료 로드"""
        # 프로젝트 루트로부터 상대 경로로 설정 파일 찾기 시도
        possible_paths = [
            os.path.join('common', 'config', 'exchange_settings.cfg'),
            os.path.join('src', 'common', 'config', 'exchange_settings.cfg'),
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'common', 'config', 'exchange_settings.cfg'),
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'common', 'config', 'exchange_settings.cfg')
        ]
        
        config_path = None
        for path in possible_paths:
            if os.path.exists(path):
                config_path = path
                break
        
        if not config_path:
            logging.warning(f"설정 파일을 찾을 수 없음, 기본 수수료 값 사용")
            return
        
        try:
            config = configparser.ConfigParser()
            config.read(config_path)
            
            # 수수료 매핑 설정 (거래소 이름 -> 설정 파일 섹션 이름)
            fee_mapping = {
                'upbit': 'exchanges.upbit',
                'bithumb': 'exchanges.bithumb',
                'binance_spot': 'exchanges.binance',
                'bybit_spot': 'exchanges.bybit'
            }
            
            for exchange, section in fee_mapping.items():
                if section in config:
                    # taker_fee 사용 (시장가 주문 기준)
                    taker_fee = config.getfloat(section, 'taker_fee', fallback=0.1) / 100.0
                    self.exchange_fees[exchange] = taker_fee
            
            # 코인 전송 수수료 로드
            if 'trading.settings' in config:
                self.transfer_fee = config.getfloat('trading.settings', 'transfer_coin_fee', fallback=0.02) / 100.0
                
            logging.info(f"수수료 설정 로드 완료: {self.exchange_fees}, 전송 수수료: {self.transfer_fee:.4%}")
            
        except Exception as e:
            logging.error(f"설정 파일 로드 오류: {e}, 기본 수수료 값 유지")
    
    def _time_method(self, method_name):
        """메소드 실행 시간을 측정하는 데코레이터 역할의 컨텍스트 매니저"""
        class TimerContext:
            def __init__(self, processor, method_name):
                self.processor = processor
                self.method_name = method_name
                
            def __enter__(self):
                self.start_time = time.time()
                return self
                
            def __exit__(self, exc_type, exc_val, exc_tb):
                duration = time.time() - self.start_time
                if self.method_name not in self.processor.perf_stats['method_times']:
                    self.processor.perf_stats['method_times'][self.method_name] = []
                self.processor.perf_stats['method_times'][self.method_name].append(duration)
                
                # 너무 오래 걸리는 경우 로깅
                if duration > 0.05:  # 50ms 이상 소요되면 로깅
                    logging.debug(f"성능 경고: {self.method_name} 실행에 {duration:.6f}초 소요됨")
        
        return TimerContext(self, method_name)
    
    def _log_performance_stats(self):
        """성능 통계 로깅"""
        current_time = time.time()
        if current_time - self.perf_stats['last_report_time'] < self.perf_stats['report_interval']:
            return  # 보고 간격이 아직 안 됨
        
        # 각 메소드별 평균 실행 시간
        logging.info(f"===== 성능 통계 ({self.perf_stats['process_count']} 처리) =====")
        for method, times in self.perf_stats['method_times'].items():
            if times:
                avg_time = sum(times) / len(times)
                max_time = max(times)
                total_time = sum(times)
                calls = len(times)
                logging.info(f"{method}: {avg_time:.6f}초 평균, {max_time:.6f}초 최대, {total_time:.2f}초 총시간, {calls}회 호출")
        
        # 컬렉션 상태
        logging.info(f"컬렉션 크기: {self.n_coins}개 코인, {self.n_dom}개 국내, {self.n_for}개 해외")
        
        # 배열 크기
        if self.premiums is not None:
            mem_usage = self.premiums.nbytes / (1024 * 1024)
            logging.info(f"김프율 배열: {self.premiums.shape}, 메모리 사용: {mem_usage:.2f}MB")
        
        # 캐시 상태
        if self.is_cache_initialized:
            min_coin = self.idx_to_coin[self.min_premium_cache['coin_idx']] if self.min_premium_cache['coin_idx'] != -1 else "없음"
            max_coin = self.idx_to_coin[self.max_premium_cache['coin_idx']] if self.max_premium_cache['coin_idx'] != -1 else "없음"
            logging.info(f"김프 캐시: 최소={self.min_premium_cache['premium']:.4%} ({min_coin}), 최대={self.max_premium_cache['premium']:.4%} ({max_coin})")
        
        self.perf_stats['last_report_time'] = current_time
    
    def add_coin(self, coin: str) -> int:
        """새 코인 추가 및 인덱스 반환"""
        with self._time_method('add_coin'):
            if coin in self.coin_to_idx:
                return self.coin_to_idx[coin]
            
            idx = self.n_coins
            self.idx_to_coin[idx] = coin
            self.coin_to_idx[coin] = idx
            self.n_coins += 1
            
            # 배열 크기 조정
            self._resize_arrays()
            
            logging.debug(f"새 코인 추가: {coin}, 인덱스: {idx}, 현재 코인 수: {self.n_coins}")
            return idx
    
    def _resize_arrays(self):
        """코인 추가시 배열 크기 조정"""
        with self._time_method('_resize_arrays'):
            if self.premiums is None:
                # 김프율 저장소 초기화 (전체 코인 x 국내거래소 x 해외거래소)
                self.premiums = np.full((self.n_coins, self.n_dom, self.n_for), np.nan)
                self.valid_pairs = np.zeros((self.n_coins, self.n_dom, self.n_for), dtype=bool)
            else:
                # 배열 확장 (마지막 차원으로 추가)
                premiums_new = np.full((self.n_coins, self.n_dom, self.n_for), np.nan)
                valid_pairs_new = np.zeros((self.n_coins, self.n_dom, self.n_for), dtype=bool)
                
                # 기존 데이터 복사
                old_n_coins = self.premiums.shape[0]
                premiums_new[:old_n_coins, :, :] = self.premiums
                valid_pairs_new[:old_n_coins, :, :] = self.valid_pairs
                
                # 새 배열로 교체
                self.premiums = premiums_new
                self.valid_pairs = valid_pairs_new
            
            logging.debug(f"배열 크기 조정 완료: {self.premiums.shape}")
    
    def process_orderbook_data(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """오더북 데이터 처리 및 거래 기회 탐색"""
        start_time = time.time()
        
        # 성능 카운터 증가
        self.perf_stats['process_count'] += 1
        
        # 필요한 필드 검증
        required_fields = ['exchange', 'symbol', 'buy', 'sell']
        if not all(field in data for field in required_fields):
            logging.error(f"오더북 데이터 누락 필드: {[f for f in required_fields if f not in data]}")
            return None
        
        exchange = data['exchange']
        symbol = data['symbol']
        
        # 선물거래소 데이터는 무시
        if exchange in ['binance_future', 'bybit_future']:
            return None
        
        # 거래소 이름 매핑 (필요한 경우)
        if exchange == 'bybit':
            exchange = 'bybit_spot'  # bybit을 bybit_spot으로 처리
        
        # 국내/해외 거래소 확인
        if exchange not in self.domestic_exchanges and exchange not in self.foreign_exchanges:
            logging.warning(f"지원하지 않는 거래소: {exchange}")
            return None
        
        # 환율 업데이트
        if 'usdt_krw' in data and data['usdt_krw'] > 0:
            self.usdt_krw = data['usdt_krw']
        
        # 기존 데이터 확인 - 변경사항이 없으면 처리 스킵
        if exchange in self.orderbooks and symbol in self.orderbooks[exchange]:
            old_data = self.orderbooks[exchange][symbol]
            if 'buy' in old_data and 'sell' in old_data:
                # 기존 매수/매도 가격
                old_buy = old_data['buy'].get('avg_price', 0)
                old_sell = old_data['sell'].get('avg_price', 0)
                
                # 새 매수/매도 가격
                new_buy = data['buy'].get('avg_price', 0)
                new_sell = data['sell'].get('avg_price', 0)
                
                # 가격 변화 비율 확인 (0.1% 미만이면 무시)
                if old_buy > 0 and old_sell > 0:
                    buy_change_pct = abs(new_buy - old_buy) / old_buy
                    sell_change_pct = abs(new_sell - old_sell) / old_sell
                    
                    if buy_change_pct < 0.001 and sell_change_pct < 0.001:
                        logging.debug(f"가격 변화 미미: {exchange} {symbol} (매수 변화: {buy_change_pct:.6%}, 매도 변화: {sell_change_pct:.6%})")
                        return None
        
        # 코인 인덱스 확인/추가
        add_coin_start = time.time()
        c_idx = self.add_coin(symbol)
        self.perf_stats['add_coin_time'] += time.time() - add_coin_start
        
        # 오더북 업데이트
        self.orderbooks[exchange][symbol] = data
        
        # 마지막 업데이트 코인 기록
        self.last_updated_coin = symbol
        
        # 관련 페어의 김프율 업데이트
        update_premiums_start = time.time()
        self._update_premiums_for_coin(symbol)
        self.perf_stats['update_premiums_time'] += time.time() - update_premiums_start
        
        # 캐시 업데이트
        update_cache_start = time.time()
        if not self.is_cache_initialized:
            self._initialize_premium_cache()
        else:
            self._update_premium_cache_for_coin(symbol)
        self.perf_stats['update_cache_time'] += time.time() - update_cache_start
        
        # 최적 거래 기회 탐색
        find_opp_start = time.time()
        result = self._find_best_opportunity()
        self.perf_stats['find_opportunity_time'] += time.time() - find_opp_start
        
        # 총 처리 시간 로깅 (100번째마다)
        total_time = time.time() - start_time
        if self.perf_stats['process_count'] % 100 == 0 or total_time > 0.1:
            logging.debug(f"데이터 처리 시간: {total_time:.6f}초 ({symbol}, {exchange})")
            self._log_performance_stats()
        
        return result
    
    def _update_premiums_for_coin(self, coin: str):
        """특정 코인에 대한 모든 페어의 김프율 업데이트"""
        with self._time_method('_update_premiums_for_coin'):
            if coin not in self.coin_to_idx:
                return
            
            c_idx = self.coin_to_idx[coin]
            
            # 국내 거래소 순회
            for d_idx, dom_ex in enumerate(self.domestic_exchanges):
                # 해외 거래소 순회
                for f_idx, for_ex in enumerate(self.foreign_exchanges):
                    self._update_premium_for_pair(c_idx, d_idx, f_idx)
    
    def _update_premium_for_pair(self, c_idx: int, d_idx: int, f_idx: int):
        """특정 페어의 김프율 계산"""
        with self._time_method('_update_premium_for_pair'):
            coin = self.idx_to_coin[c_idx]
            dom_ex = self.domestic_exchanges[d_idx]
            for_ex = self.foreign_exchanges[f_idx]
            
            # 두 거래소 모두에 오더북 데이터가 있는지 확인
            if (coin not in self.orderbooks[dom_ex] or 
                coin not in self.orderbooks[for_ex]):
                self.valid_pairs[c_idx, d_idx, f_idx] = False
                self.premiums[c_idx, d_idx, f_idx] = np.nan
                return
            
            dom_data = self.orderbooks[dom_ex][coin]
            for_data = self.orderbooks[for_ex][coin]
            
            # 매수/매도 가격 확인
            if ('buy' not in dom_data or 'sell' not in dom_data or 
                'buy' not in for_data or 'sell' not in for_data):
                self.valid_pairs[c_idx, d_idx, f_idx] = False
                self.premiums[c_idx, d_idx, f_idx] = np.nan
                return
            
            # 평균 가격 사용
            dom_bid = dom_data['buy']['avg_price']  # 국내 매수 호가
            dom_ask = dom_data['sell']['avg_price']  # 국내 매도 호가
            for_bid = for_data['buy']['avg_price']  # 해외 매수 호가
            for_ask = for_data['sell'].get('avg_price', 0)  # 해외 매도 호가
            
            # 유효한 가격인지 확인
            if dom_bid <= 0 or dom_ask <= 0 or for_bid <= 0 or for_ask <= 0:
                self.valid_pairs[c_idx, d_idx, f_idx] = False
                self.premiums[c_idx, d_idx, f_idx] = np.nan
                return
            
            # 김프율 계산 (국내 매수 / (해외 매도 * 환율) - 1)
            # 국내에서 파는 가격 / (해외에서 사는 가격 * 환율) - 1
            premium = dom_bid / (for_ask * self.usdt_krw) - 1
            
            # 김프율 저장
            self.premiums[c_idx, d_idx, f_idx] = premium
            self.valid_pairs[c_idx, d_idx, f_idx] = True
            
            # 로깅 (큰 변화가 있을 때만)
            old_premium = self.premiums[c_idx, d_idx, f_idx] if not np.isnan(self.premiums[c_idx, d_idx, f_idx]) else 0
            if abs(premium - old_premium) > 0.005:  # 0.5% 이상 변화가 있을 때만
                logging.debug(f"김프율 업데이트: {coin} {dom_ex}-{for_ex} = {premium:.4%}")
    
    def _initialize_premium_cache(self):
        """전체 김프 캐시 초기화 (첫 실행 또는 재초기화용)"""
        with self._time_method('_initialize_premium_cache'):
            if self.premiums is None or self.n_coins == 0:
                return
            
            # 최소/최대 김프 초기화
            self.min_premium_cache = {
                'premium': float('inf'),
                'coin_idx': -1,
                'dom_idx': -1,
                'for_idx': -1
            }
            self.max_premium_cache = {
                'premium': float('-inf'),
                'coin_idx': -1,
                'dom_idx': -1,
                'for_idx': -1
            }
            
            # 모든 코인, 모든 페어에 대한 최소/최대 김프 찾기
            for c_idx in range(self.n_coins):
                coin_premiums = np.where(self.valid_pairs[c_idx], self.premiums[c_idx], np.nan)
                
                # 유효한 데이터가 없으면 스킵
                if np.all(np.isnan(coin_premiums)):
                    continue
                
                # 최소 김프 찾기
                min_premium = np.nanmin(coin_premiums)
                if min_premium < self.min_premium_cache['premium']:
                    min_indices = np.where(coin_premiums == min_premium)
                    self.min_premium_cache = {
                        'premium': min_premium,
                        'coin_idx': c_idx,
                        'dom_idx': min_indices[0][0],
                        'for_idx': min_indices[1][0]
                    }
                
                # 최대 김프 찾기
                max_premium = np.nanmax(coin_premiums)
                if max_premium > self.max_premium_cache['premium']:
                    max_indices = np.where(coin_premiums == max_premium)
                    self.max_premium_cache = {
                        'premium': max_premium,
                        'coin_idx': c_idx,
                        'dom_idx': max_indices[0][0],
                        'for_idx': max_indices[1][0]
                    }
            
            self.is_cache_initialized = True
            logging.debug(f"김프 캐시 초기화 완료: 최소 {self.min_premium_cache['premium']:.4%}, 최대 {self.max_premium_cache['premium']:.4%}")
    
    def _update_premium_cache_for_coin(self, coin: str):
        """특정 코인에 대한 최소/최대 김프율 캐시 업데이트"""
        with self._time_method('_update_premium_cache_for_coin'):
            if coin not in self.coin_to_idx:
                return
            
            c_idx = self.coin_to_idx[coin]
            
            # 해당 코인에 대한 모든 김프율 검사
            local_min_premium = float('inf')
            local_min_d_idx = -1
            local_min_f_idx = -1
            
            local_max_premium = float('-inf')
            local_max_d_idx = -1
            local_max_f_idx = -1
            
            for d_idx in range(self.n_dom):
                for f_idx in range(self.n_for):
                    if not self.valid_pairs[c_idx, d_idx, f_idx]:
                        continue
                    
                    premium = self.premiums[c_idx, d_idx, f_idx]
                    
                    # NaN 확인
                    if np.isnan(premium):
                        continue
                    
                    # 최소 김프 업데이트
                    if premium < local_min_premium:
                        local_min_premium = premium
                        local_min_d_idx = d_idx
                        local_min_f_idx = f_idx
                    
                    # 최대 김프 업데이트
                    if premium > local_max_premium:
                        local_max_premium = premium
                        local_max_d_idx = d_idx
                        local_max_f_idx = f_idx
            
            # 결과 로깅
            if local_min_d_idx != -1 and local_max_d_idx != -1:
                dom_min = self.domestic_exchanges[local_min_d_idx]
                for_min = self.foreign_exchanges[local_min_f_idx]
                dom_max = self.domestic_exchanges[local_max_d_idx]
                for_max = self.foreign_exchanges[local_max_f_idx]
                
                # 코인마다 10개 중 1개 정도만 로깅 (너무 많은 로그 방지)
                if c_idx % 10 == 0:
                    premium_diff = local_max_premium - local_min_premium
                    logging.debug(f"코인 {coin} 김프: 최소 {local_min_premium:.4%} ({for_min}-{dom_min}), 최대 {local_max_premium:.4%} ({for_max}-{dom_max}), 차이: {premium_diff:.4%}")
            
            # 현재 코인의 최소 김프가 전체 최소 김프보다 작으면 업데이트
            if local_min_premium < self.min_premium_cache['premium'] and local_min_d_idx != -1:
                self.min_premium_cache = {
                    'premium': local_min_premium,
                    'coin_idx': c_idx,
                    'dom_idx': local_min_d_idx,
                    'for_idx': local_min_f_idx
                }
                logging.debug(f"최소 김프 캐시 업데이트: {self.idx_to_coin[c_idx]} ({local_min_premium:.4%})")
            
            # 현재 코인의 최대 김프가 전체 최대 김프보다 크면 업데이트
            if local_max_premium > self.max_premium_cache['premium'] and local_max_d_idx != -1:
                self.max_premium_cache = {
                    'premium': local_max_premium,
                    'coin_idx': c_idx,
                    'dom_idx': local_max_d_idx,
                    'for_idx': local_max_f_idx
                }
                logging.debug(f"최대 김프 캐시 업데이트: {self.idx_to_coin[c_idx]} ({local_max_premium:.4%})")
            
            # 기존 코인이 최소/최대였는데 값이 변경된 경우 전체 재계산 필요
            if (self.min_premium_cache['coin_idx'] == c_idx and local_min_premium > self.min_premium_cache['premium']) or \
               (self.max_premium_cache['coin_idx'] == c_idx and local_max_premium < self.max_premium_cache['premium']):
                logging.debug(f"코인 {coin}의 김프율 변경으로 전체 캐시 재계산 시작")
                self._initialize_premium_cache()
    
    def _calculate_pair_premium(self, coin_idx: int, dom_idx: int, for_idx: int) -> float:
        """특정 코인/거래소 페어의 김프 계산"""
        coin = self.idx_to_coin[coin_idx]
        dom_ex = self.domestic_exchanges[dom_idx]
        for_ex = self.foreign_exchanges[for_idx]
        
        # 필요한 오더북 데이터 확인
        if dom_ex not in self.orderbooks or coin not in self.orderbooks[dom_ex]:
            return np.nan
        if for_ex not in self.orderbooks or coin not in self.orderbooks[for_ex]:
            return np.nan
        
        # 가격 정보 추출
        dom_data = self.orderbooks[dom_ex][coin]
        for_data = self.orderbooks[for_ex][coin]
        
        # 필수 필드 체크
        if not ('buy' in dom_data and 'sell' in dom_data and 'buy' in for_data and 'sell' in for_data):
            return np.nan
        
        # 시퀀스 확인 (필수는 아님)
        dom_seq = dom_data.get('sequence', 0)
        for_seq = for_data.get('sequence', 0)
        
        # 원화/USDT 가격 가져오기
        dom_buy_price = dom_data['buy'].get('avg_price', 0)
        dom_sell_price = dom_data['sell'].get('avg_price', 0)
        for_buy_price = for_data['buy'].get('avg_price', 0)
        for_sell_price = for_data['sell'].get('avg_price', 0)
        
        # 볼륨 확인 (필수는 아님)
        dom_buy_vol = dom_data['buy'].get('volume', 0)
        dom_sell_vol = dom_data['sell'].get('volume', 0)
        for_buy_vol = for_data['buy'].get('volume', 0)
        for_sell_vol = for_data['sell'].get('volume', 0)
        
        # 가격 유효성 검사
        if dom_buy_price <= 0 or dom_sell_price <= 0 or for_buy_price <= 0 or for_sell_price <= 0:
            return np.nan
        
        # 해외 가격을 원화로 변환
        for_buy_price_krw = for_buy_price * self.usdt_krw
        for_sell_price_krw = for_sell_price * self.usdt_krw
        
        # 해외 매수 / 국내 매도 (김프 포착)
        kimp_rate = dom_buy_price / for_sell_price_krw - 1
        
        # 디버그 로깅 (일부 샘플만)
        if coin_idx % 10 == 0 and dom_idx == 0 and for_idx == 0:  # 10번째 코인마다 샘플링
            logging.debug(f"김프 계산: {coin} {for_ex}-{dom_ex}, 국내 매수: {dom_buy_price:,.2f}원, 해외 매도: {for_sell_price:,.2f}$ ({for_sell_price_krw:,.2f}원), 김프율: {kimp_rate:.4%}, 환율: {self.usdt_krw:,.2f}")
        
        return kimp_rate
    
    def _find_best_opportunity(self) -> Optional[Dict[str, Any]]:
        """최적의 거래 기회 탐색 (최저 전송 김프 + 최고 수익 김프 조합)"""
        with self._time_method('_find_best_opportunity'):
            # 캐시가 초기화되지 않았으면 초기화
            if not self.is_cache_initialized:
                self._initialize_premium_cache()
                if not self.is_cache_initialized:  # 여전히 초기화되지 않았으면 유효한 데이터가 없는 것
                    return None
            
            # 캐시된 최소/최대값 사용
            transfer_c_idx = self.min_premium_cache['coin_idx']
            transfer_d_idx = self.min_premium_cache['dom_idx']
            transfer_f_idx = self.min_premium_cache['for_idx'] 
            transfer_premium = self.min_premium_cache['premium']
            
            profit_c_idx = self.max_premium_cache['coin_idx']
            profit_d_idx = self.max_premium_cache['dom_idx']
            profit_f_idx = self.max_premium_cache['for_idx']
            profit_premium = self.max_premium_cache['premium']
            
            # 유효한 값이 없으면 종료
            if transfer_c_idx == -1 or profit_c_idx == -1:
                return None
            
            # 코인 정보
            transfer_coin = self.idx_to_coin[transfer_c_idx]
            profit_coin = self.idx_to_coin[profit_c_idx]
            
            # 거래소 정보
            transfer_dom = self.domestic_exchanges[transfer_d_idx]
            transfer_for = self.foreign_exchanges[transfer_f_idx]
            profit_dom = self.domestic_exchanges[profit_d_idx]
            profit_for = self.foreign_exchanges[profit_f_idx]
            
            # 김프 정보 디버그 로깅 (매 10번째 호출마다)
            if self.perf_stats['process_count'] % 10 == 0:
                # 현재 캐시된 최저/최고 김프 정보
                logging.debug(f"현재 캐시: 최저 김프 {transfer_coin} {transfer_premium:.4%} ({transfer_for}-{transfer_dom}), 최고 김프 {profit_coin} {profit_premium:.4%} ({profit_for}-{profit_dom})")
                
                # 전체 유효한 데이터의 김프 범위 확인
                valid_premiums = []
                for c_idx in range(self.n_coins):
                    for d_idx in range(self.n_dom):
                        for f_idx in range(self.n_for):
                            if self.valid_pairs[c_idx, d_idx, f_idx]:
                                premium = self.premiums[c_idx, d_idx, f_idx]
                                if not np.isnan(premium):
                                    valid_premiums.append(premium)
                
                if valid_premiums:
                    min_p = min(valid_premiums)
                    max_p = max(valid_premiums)
                    logging.debug(f"전체 김프 범위: 최소 {min_p:.4%}, 최대 {max_p:.4%}, 개수: {len(valid_premiums)}")
            
            # 수수료 계산
            transfer_dom_fee = self.exchange_fees.get(transfer_dom, 0.0005)  # 국내 전송코인 매수 수수료
            transfer_for_fee = self.exchange_fees.get(transfer_for, 0.001)   # 해외 전송코인 매도 수수료
            profit_for_fee = self.exchange_fees.get(profit_for, 0.001)      # 해외 수익코인 매수 수수료
            profit_dom_fee = self.exchange_fees.get(profit_dom, 0.0005)      # 국내 수익코인 매도 수수료
            
            # 수익 코인 매수 시 추가 수수료 (숏 포지션 오픈)
            profit_for_short_open_fee = getattr(self, 'profit_coin_extra_fee', 0.0011)
            
            # 숏 청산 수수료는 이미 위에 포함됨
            profit_for_short_close_fee = 0.0  # 0% (이미 profit_for_short_open_fee에 포함됨)
            
            # 총 수수료 (문서 기준)
            # 전송 코인: 국내 매수 + 해외 매도 + 전송 수수료
            transfer_total_fee = transfer_dom_fee + transfer_for_fee + self.transfer_fee
            
            # 수익 코인: 해외 매수 + 숏 수수료(오픈+클로즈) + 전송 수수료 + 국내 매도
            profit_total_fee = profit_for_fee + profit_for_short_open_fee + self.transfer_fee + profit_dom_fee
            
            # 총 수수료 합계
            original_total_fee = transfer_total_fee + profit_total_fee
            
            # 수수료 할인 적용 (옵션에 따라)
            if self.apply_fee_discount:
                reduced_total_fee = original_total_fee  # 할인 없이 실제 수수료 적용
                total_fee = reduced_total_fee
                
                # 실제 수수료 로깅
                if self.perf_stats['process_count'] % 1000 == 0:
                    logging.debug(f"수수료 계산: 실제 수수료 {original_total_fee:.6f} 적용")
            else:
                total_fee = original_total_fee
            
            # 김프 차이 계산
            premium_diff = profit_premium - transfer_premium
            
            # 총 효율성 계산 (수수료 고려 및 전송코인 음수 김프 처리 개선)
            # 전송 코인이 음수 김프인 경우 (해외가격이 더 높음): 더 유리하게 작용해야 함
            # 단계별 계산 로직을 직접 구현하여 효율성 계산
            
            # 원가 계산 (500만원 기준)
            initial_amount = 5000000  # 500만원
            
            # 1) 전송코인 국내 매수
            amount_after_buy = initial_amount * (1 - transfer_dom_fee)
            
            # 2) 코인 해외로 전송
            amount_after_transfer = amount_after_buy * (1 - self.transfer_fee)
            
            # 3) 해외 가치 (김프 반영)
            if transfer_premium < 0:
                # 음수 김프: 해외가격이 더 높음 (유리)
                foreign_value = amount_after_transfer * (1 + abs(transfer_premium))
            else:
                # 양수 김프: 국내가격이 더 높음 (불리)
                foreign_value = amount_after_transfer / (1 + transfer_premium)
            
            # 4) 전송코인 해외 매도
            amount_after_sell = foreign_value * (1 - transfer_for_fee)
            
            # 5) 수익코인 해외 매수
            amount_after_profit_buy = amount_after_sell * (1 - profit_for_fee)
            
            # 6) 코인 국내로 전송
            amount_after_profit_transfer = amount_after_profit_buy * (1 - self.transfer_fee)
            
            # 7) 국내 가치 (김프 반영)
            domestic_value = amount_after_profit_transfer * (1 + profit_premium)
            
            # 8) 수익코인 국내 매도
            final_amount = domestic_value * (1 - profit_dom_fee)
            
            # 전체 수익률 계산
            total_efficiency = final_amount / initial_amount
            net_profit = total_efficiency - 1
            
            # 매 100번째 호출마다 계산 과정 자세히 로깅
            if self.perf_stats['process_count'] % 100 == 0:
                if transfer_premium < 0:
                    logging.debug(f"효율성 계산(음수 김프): 단계별 계산 사용, 순이익: {net_profit:.6f}")
                else:
                    logging.debug(f"효율성 계산(양수 김프): 단계별 계산 사용, 순이익: {net_profit:.6f}")
            
            # 특정 코인 조합에 대한 자세한 로깅 유지
            if (transfer_coin.lower() == 'doge' and profit_coin.lower() == 'joe') or \
               (transfer_coin.lower() == 'ada' and profit_coin.lower() == 'joe'):
                logging.info(f"최종 수익률: {net_profit*100:.4f}% (시작: {initial_amount:.2f}원, 최종: {final_amount:.2f}원)")

            # 수익성 확인 (실제 거래를 위한 순이익 임계값 적용)
            if self.apply_fee_discount:
                # 테스트 모드: 매우 완화된 기준 (-5%까지 허용)
                if net_profit <= -0.05:
                    return None
            else:
                # 실제 거래 모드: 극단적으로 낮은 수익도 기록 (거의 모든 기회 기록)
                if net_profit <= -0.02:  # -2% 이상이면 기록
                    return None
            
            # 수익률이 10% 이상인 비정상 거래는 필터링
            if net_profit >= 0.1:  # 10% 이상 수익률은 비정상으로 간주
                logging.warning(f"비정상 고수익 거래 필터링: {transfer_coin}->{profit_coin}, 순이익: {net_profit:.4%}")
                return None
            
            # 김프 차이가 매우 작으면 제외
            if self.apply_fee_discount:
                # 테스트 모드: 완화된 기준 (0.45%이상이면 OK)
                if premium_diff <= 0.0045:
                    return None
            else:
                # 실제 거래 모드: 매우 낮은 기준 (거의 모든 김프 차이 기록)
                if premium_diff <= 0.0045:
                    return None
            
            # 거래 경로 문자열
            opportunity_path = f"{transfer_coin}_{profit_coin}_{transfer_dom}_{transfer_for}_{profit_dom}_{profit_for}"
            
            # 중복 거래 방지를 위한 확인 (마지막 거래 경로만 기억)
            if hasattr(self, 'last_opportunity_path') and self.last_opportunity_path == opportunity_path:
                logging.debug(f"연속 중복 거래 경로 무시: {opportunity_path}")
                return None
            
            # 마지막 거래 경로 업데이트
            self.last_opportunity_path = opportunity_path
            
            # 거래 기회 저장
            opportunity = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                'transfer_coin': transfer_coin,
                'profit_coin': profit_coin,
                'transfer_pair': f"{transfer_for}-{transfer_dom}",
                'profit_pair': f"{profit_for}-{profit_dom}",
                'transfer_premium': transfer_premium,
                'profit_premium': profit_premium,
                'premium_diff': premium_diff,
                'total_efficiency': total_efficiency,
                'net_profit': net_profit,
                'original_fee': original_total_fee * 100,  # 퍼센트로 변환
                'reduced_fee': reduced_total_fee * 100,  # 퍼센트로 변환
                'usdt_krw': self.usdt_krw,
                'fees': {
                    'transfer_dom': transfer_dom_fee * 100,
                    'transfer_for': transfer_for_fee * 100,
                    'profit_dom': profit_dom_fee * 100,
                    'profit_for': profit_for_fee * 100,
                    'transfer': self.transfer_fee * 100
                }
            }
            
            # 가격 정보 추가
            try:
                # 전송 코인 가격 정보
                transfer_dom_data = self.orderbooks[transfer_dom][transfer_coin]
                transfer_for_data = self.orderbooks[transfer_for][transfer_coin]
                
                # 수익 코인 가격 정보
                profit_dom_data = self.orderbooks[profit_dom][profit_coin]
                profit_for_data = self.orderbooks[profit_for][profit_coin]
                
                # 가격 데이터 추출
                opportunity['dom_transfer_buy'] = transfer_dom_data['sell']['avg_price']  # 국내 전송코인 매수가
                opportunity['for_transfer_sell'] = transfer_for_data['buy']['avg_price']  # 해외 전송코인 매도가
                opportunity['for_profit_buy'] = profit_for_data['sell']['avg_price']      # 해외 수익코인 매수가
                opportunity['dom_profit_sell'] = profit_dom_data['buy']['avg_price']      # 국내 수익코인 매도가
                
            except (KeyError, TypeError) as e:
                logging.error(f"가격 정보 추출 오류: {e}")
                # 오류 발생 시 기본값 설정
                opportunity['dom_transfer_buy'] = 0
                opportunity['for_transfer_sell'] = 0
                opportunity['for_profit_buy'] = 0
                opportunity['dom_profit_sell'] = 0
            
            self.best_opportunity = opportunity
            
            # 수익률 로깅
            log_level = logging.INFO
            if net_profit > 0.01:  # 1% 이상 순이익
                log_level = logging.WARNING  # 높은 수익은 경고 레벨로 표시
            elif net_profit < 0:  # 손실인 경우
                log_level = logging.DEBUG   # 디버그 레벨로 로깅
            
            # 실제 수익/손실을 모두 표시 (백테스트 목적)
            profit_status = "이익" if net_profit > 0 else "손실"
            
            # 가격 정보 포함해서 로깅
            price_info = ""
            if 'dom_transfer_buy' in opportunity and opportunity['dom_transfer_buy'] > 0:
                price_info += f"전송매수: {opportunity['dom_transfer_buy']:,.2f}원, "
                price_info += f"전송매도: {opportunity['for_transfer_sell']:.6f}$, "
                price_info += f"수익매수: {opportunity['for_profit_buy']:.6f}$, "
                price_info += f"수익매도: {opportunity['dom_profit_sell']:,.2f}원, "
            
            logging.log(log_level, 
                f"거래 기회({profit_status}): {transfer_coin}->{profit_coin} "
                f"(전송: {transfer_premium:.4%}, 수익: {profit_premium:.4%}, 차이: {premium_diff:.4%}, "
                f"순이익: {net_profit:.4%}, 원본수수료: {original_total_fee:.4%}, "
                f"{price_info}환율: {self.usdt_krw:,.2f}원)"
            )
            
            return opportunity
    
    def get_best_opportunity(self) -> Optional[Dict[str, Any]]:
        """현재까지 발견된 최고 수익 기회 반환"""
        return self.best_opportunity 