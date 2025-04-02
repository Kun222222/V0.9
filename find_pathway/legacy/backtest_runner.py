import json
import csv
import sys
import logging
import time
import os
import re
from datetime import datetime
import numpy as np

# 현재 디렉토리를 모듈 검색 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

# src 디렉토리도 모듈 검색 경로에 추가
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# 절대 경로를 사용하여 모듈 가져오기
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
from optimized_processor import OptimizedKimpProcessor

# 기본 로깅 설정 제거 (여기서는 설정하지 않음)

class KimpBacktester:
    def __init__(self, log_file_path='src/logs/market_order_data/250401_123323_market_orderbook.log'):
        """KIMP 백테스터 초기화"""
        self.log_file_path = log_file_path
        self.opportunities = []
        
        # 현재 시간으로 로그 파일 이름 생성
        current_time = datetime.now().strftime("%y%m%d_%H%M%S")
        
        # 절대 경로 사용으로 변경
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        
        # tests 폴더에 로그 저장 (절대 경로 사용)
        log_dir = os.path.join(base_dir, 'tests', 'crosskimp', 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # 로그 파일 경로
        log_file = os.path.join(log_dir, f"backtest_performance_{current_time}.log")
        
        # 로거 생성 및 설정
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)  # 루트 로거의 레벨 설정
        
        # 기존 핸들러 제거 (중복 방지)
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # 파일 핸들러 추가
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # 콘솔 핸들러 추가
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)  # 콘솔에는 INFO 이상만 출력
        console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        logging.info(f"로그 파일 생성: {log_file}")
        
        # 프로세서 초기화
        self.processor = OptimizedKimpProcessor(apply_fee_discount=True)  # 수수료 할인 적용 (99%)
        logging.info(f"수수료 할인 적용 (99% 할인): {self.processor.apply_fee_discount}")
        
        # 성능 측정용 카운터
        self.stats = {
            'total_events': 0,
            'valid_events': 0,
            'opportunities': 0,
            'errors': 0,
            'processing_times': [],
            'last_report_time': time.time(),
            'report_interval': 5,  # 초 단위로 보고서 출력 간격
            'start_time': None,
            'end_time': None
        }
        
        # 결과 저장 파일 설정 (tests 폴더에 저장)
        results_dir = os.path.join(base_dir, 'tests', 'crosskimp', 'results')
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
            logging.info(f"결과 디렉토리 생성: {results_dir}")
        
        self.results_file = os.path.join(results_dir, f"crosskimp_backtest_results_{current_time}.csv")
        logging.info(f"결과 파일 경로: {self.results_file}")
        
        # 로깅 구성 완료
        logging.debug("로깅 구성 완료. 디버그 로그 활성화됨.")
    
    def run_backtest(self):
        """백테스트 실행"""
        logging.info(f"백테스트 시작: {self.log_file_path}")
        self.stats['start_time'] = time.time()
        
        # 로그 파일 읽기
        try:
            with open(self.log_file_path, 'r') as f:
                for line in f:
                    self.stats['total_events'] += 1
                    
                    # 주기적으로 진행 상황 출력 (시간 기준)
                    current_time = time.time()
                    if current_time - self.stats['last_report_time'] >= self.stats['report_interval']:
                        self._log_progress()
                        self.stats['last_report_time'] = current_time
                    
                    try:
                        # 처리 시작 시간
                        start_process = time.time()
                        
                        # JSON 부분 추출
                        match = re.search(r'{.*}', line)
                        if not match:
                            continue
                        
                        json_str = match.group(0)
                        json_data = json.loads(json_str)
                        
                        # 디버그 로깅
                        symbol = json_data.get('symbol', 'unknown')
                        exchange = json_data.get('exchange', 'unknown')
                        logging.debug(f"처리 중: {exchange} - {symbol}")
                        
                        # 오더북 데이터 처리
                        result = self.processor.process_orderbook_data(json_data)
                        self.stats['valid_events'] += 1
                        
                        # 거래 기회 발견 시 저장
                        if result is not None:
                            self.stats['opportunities'] += 1
                            self.opportunities.append(result)
                            
                            # 기회 발견 로깅
                            logging.info(
                                f"기회 발견 #{self.stats['opportunities']}: "
                                f"전송({result['transfer_coin']}): {result['transfer_pair']} ({result['transfer_premium']:.4%}), "
                                f"수익({result['profit_coin']}): {result['profit_pair']} ({result['profit_premium']:.4%}), "
                                f"순이익: {result['net_profit']:.4%}"
                            )
                            
                            # CSV에 결과 저장
                            self._save_opportunity(result)
                        
                        # 처리 종료 시간 및 소요 시간 기록
                        process_time = time.time() - start_process
                        self.stats['processing_times'].append(process_time)
                        
                        # 타이밍 디버그 (10,000개마다 또는 너무 오래 걸릴 때)
                        if self.stats['total_events'] % 10000 == 0 or process_time > 0.1:
                            logging.debug(f"이벤트 처리 시간: {process_time:.6f}초 (평균: {np.mean(self.stats['processing_times'][-1000:]):.6f}초)")
                    
                    except json.JSONDecodeError:
                        self.stats['errors'] += 1
                        continue
                    except Exception as e:
                        self.stats['errors'] += 1
                        logging.error(f"처리 오류: {e}")
                        continue
        
        except Exception as e:
            logging.error(f"백테스트 오류: {e}")
        
        # 백테스트 종료 시간 기록
        self.stats['end_time'] = time.time()
        
        # 백테스트 결과 요약
        self._log_results()
        
        return {
            'total_events': self.stats['total_events'],
            'valid_events': self.stats['valid_events'],
            'opportunities': self.stats['opportunities'],
            'errors': self.stats['errors'],
            'results_file': self.results_file,
            'duration': self.stats['end_time'] - self.stats['start_time'],
            'events_per_second': self.stats['total_events'] / (self.stats['end_time'] - self.stats['start_time'])
        }
    
    def _log_progress(self):
        """현재 진행 상황 로깅"""
        elapsed = time.time() - self.stats['start_time']
        events_per_second = self.stats['total_events'] / elapsed if elapsed > 0 else 0
        
        logging.info(
            f"진행 상황: {self.stats['total_events']} 이벤트 처리 "
            f"({events_per_second:.2f} 이벤트/초), "
            f"유효: {self.stats['valid_events']}, "
            f"기회: {self.stats['opportunities']}, "
            f"오류: {self.stats['errors']}, "
            f"소요시간: {elapsed:.2f}초"
        )
        
        # 프로세서 상태 정보
        if hasattr(self.processor, 'n_coins'):
            logging.info(f"프로세서 상태: {self.processor.n_coins}개 코인, 환율: {self.processor.usdt_krw:.2f}")
        
        # 최근 처리 시간 평균
        if self.stats['processing_times']:
            avg_time = np.mean(self.stats['processing_times'][-1000:])
            max_time = np.max(self.stats['processing_times'][-1000:])
            logging.info(f"최근 처리 시간: 평균 {avg_time:.6f}초, 최대 {max_time:.6f}초")
    
    def _log_results(self):
        """백테스트 최종 결과 로깅"""
        duration = self.stats['end_time'] - self.stats['start_time']
        events_per_second = self.stats['total_events'] / duration if duration > 0 else 0
        
        logging.info(f"\n===== 백테스트 완료 =====")
        logging.info(f"총 {self.stats['total_events']} 이벤트 처리")
        logging.info(f"유효 이벤트: {self.stats['valid_events']}")
        logging.info(f"거래 기회: {self.stats['opportunities']}")
        logging.info(f"오류: {self.stats['errors']}")
        logging.info(f"소요시간: {duration:.2f}초")
        logging.info(f"처리 속도: {events_per_second:.2f} 이벤트/초")
        logging.info(f"결과 저장: {self.results_file}")
        
        # 평균 처리 시간
        if self.stats['processing_times']:
            avg_time = np.mean(self.stats['processing_times'])
            max_time = np.max(self.stats['processing_times'])
            logging.info(f"평균 처리 시간: {avg_time:.6f}초")
            logging.info(f"최대 처리 시간: {max_time:.6f}초")
        
        # 샘플 결과 출력
        self._print_sample_results()
    
    def _save_opportunity(self, opportunity):
        """기회 데이터를 CSV 파일에 저장"""
        try:
            file_exists = os.path.isfile(self.results_file)
            
            with open(self.results_file, 'a', newline='') as f:
                writer = csv.writer(f)
                
                # 헤더 작성 (파일이 없을 경우만)
                if not file_exists:
                    writer.writerow([
                        'timestamp', 'transfer_coin', 'profit_coin', 
                        'transfer_pair', 'profit_pair',
                        'transfer_premium', 'profit_premium', 
                        'total_efficiency', 'net_profit', 'usdt_krw'
                    ])
                    logging.info(f"CSV 파일 헤더 생성: {self.results_file}")
                
                # 데이터 행 작성
                writer.writerow([
                    opportunity['timestamp'],
                    opportunity['transfer_coin'],
                    opportunity['profit_coin'],
                    opportunity['transfer_pair'],
                    opportunity['profit_pair'],
                    opportunity['transfer_premium'],
                    opportunity['profit_premium'],
                    opportunity['total_efficiency'],
                    opportunity['net_profit'],
                    opportunity['usdt_krw']
                ])
                
                logging.debug(f"기회 데이터 저장 성공: {opportunity['transfer_coin']}-{opportunity['profit_coin']}")
        except Exception as e:
            logging.error(f"CSV 파일 저장 오류: {e}, 파일 경로: {self.results_file}")
    
    def _print_sample_results(self):
        """샘플 결과 출력"""
        if not self.opportunities:
            logging.info("거래 기회가 없습니다.")
            return
        
        # 최대 5개 샘플 출력
        sample_size = min(5, len(self.opportunities))
        logging.info(f"샘플 거래 기회 ({sample_size}개):")
        
        for i in range(sample_size):
            opp = self.opportunities[i]
            logging.info(f"{i+1}. 전송: {opp['transfer_coin']} ({opp['transfer_pair']}, {opp['transfer_premium']:.4%}), " +
                        f"수익: {opp['profit_coin']} ({opp['profit_pair']}, {opp['profit_premium']:.4%}), " +
                        f"순이익: {opp['net_profit']:.4%}")

if __name__ == "__main__":
    # 백테스트 실행
    backtest = KimpBacktester()
    results = backtest.run_backtest()
    
    # 결과 출력
    print(f"\n백테스트 결과:")
    print(f"총 이벤트: {results['total_events']}")
    print(f"유효 이벤트: {results['valid_events']}")
    print(f"거래 기회: {results['opportunities']}")
    print(f"소요시간: {results['duration']:.2f}초")
    print(f"처리 속도: {results['events_per_second']:.2f} 이벤트/초")
    print(f"결과 파일: {results['results_file']}") 