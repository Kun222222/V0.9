#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
오더북 스냅샷 데이터 검증 도구 (최신 데이터만 검증)

이 스크립트는 각 심볼의 가장 최근 오더북 데이터만 검증합니다:
1. 가격 역전 현상 (최고 매수가 >= 최저 매도가)
2. 각 심볼별 최신 상태만 검증하여 성능 개선
"""

import os
import re
import json
import logging
import glob
from typing import Dict, List, Tuple, Any, Set
from datetime import datetime
from collections import defaultdict

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 로그 폴더 경로 하드코딩 (수정됨)
LOG_FOLDER = "/Users/kun/Desktop/CrossKimpArbitrage/v0.6/src/logs/raw_data"

# 출력 파일 경로
OUTPUT_FILE = "latest_orderbook_validation.txt"

# 거래소 목록
EXCHANGES = [
    "binance_future",
    "binance",
    "bithumb",
    "bybit_future",
    "bybit",
    "upbit"
]

def find_latest_log_files(folder_path: str) -> List[str]:
    """
    지정된 폴더에서 각 거래소별 가장 최신 로그 파일을 찾습니다.
    
    파일 이름 형식: [날짜]_[시간]_[거래소]_raw_logger.log
    """
    latest_files = {}
    
    # 폴더 존재 여부 확인
    if not os.path.exists(folder_path):
        logger.error(f"폴더가 존재하지 않습니다: {folder_path}")
        return []
    
    # 모든 로그 파일 목록 가져오기
    log_files = glob.glob(os.path.join(folder_path, "*_raw_logger.log"))
    
    if not log_files:
        logger.warning(f"폴더 {folder_path}에서 로그 파일을 찾을 수 없습니다.")
        return []
    
    logger.info(f"폴더에서 {len(log_files)}개의 로그 파일 발견")
    
    # 각 거래소별 최신 파일 찾기
    for exchange in EXCHANGES:
        # 해당 거래소 로그 파일 필터링
        exchange_logs = [f for f in log_files if f"_{exchange}_raw_logger.log" in f]
        
        if not exchange_logs:
            logger.warning(f"거래소 {exchange}에 대한 로그 파일을 찾을 수 없습니다.")
            continue
        
        # 파일 이름에서 날짜+시간 추출 (파일 이름 형식: YYMMDD_HHMMSS_exchange_raw_logger.log)
        def extract_datetime(filename):
            basename = os.path.basename(filename)
            match = re.search(r'^(\d{6}_\d{6})_', basename)
            if match:
                date_time_str = match.group(1)
                try:
                    # 날짜+시간 문자열을 datetime 객체로 변환
                    return datetime.strptime(date_time_str, "%y%m%d_%H%M%S")
                except ValueError:
                    logger.warning(f"파일 {basename}에서 날짜/시간 형식이 잘못되었습니다.")
            return datetime.min  # 날짜 추출 실패시 가장 오래된 날짜 반환
        
        # 날짜 기준으로 정렬하여 가장 최신 파일 선택
        latest_file = max(exchange_logs, key=extract_datetime)
        
        if latest_file:
            latest_files[exchange] = latest_file
            logger.info(f"거래소 {exchange}의 최신 로그 파일: {os.path.basename(latest_file)}")
    
    return list(latest_files.values())

class LatestOrderbookValidator:
    """최신 오더북 데이터만 검증하는 검증기"""
    
    def __init__(self):
        self.exchange_symbols = {}  # 거래소별 심볼 목록
        self.latest_orderbooks = {}  # 거래소+심볼별 최신 오더북 (타임스탬프 기준)
        self.results = defaultdict(lambda: defaultdict(lambda: {
            'data_count': 0,
            'price_inversions': 0,
            'violation_details': []
        }))
    
    def extract_exchange_from_filename(self, filename: str) -> str:
        """파일명에서 거래소 이름 추출"""
        basename = os.path.basename(filename)
        for exchange in EXCHANGES:
            if f"_{exchange}_raw_logger.log" in basename:
                return exchange
        
        # 기존 정규식 방식 (폴백)
        match = re.search(r'_(\w+)_raw_logger\.log$', basename)
        if match:
            return match.group(1)
        return "unknown"
    
    def process_log_file(self, file_path: str) -> None:
        """로그 파일 처리 - 각 심볼별 최신 데이터만 유지"""
        try:
            exchange = self.extract_exchange_from_filename(file_path)
            logger.info(f"파일 처리 중: {file_path} (거래소: {exchange})")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                line_count = 0
                processed_count = 0
                latest_by_symbol = {}  # 심볼별 최신 데이터 (시퀀스 기준)
                
                for line in f:
                    line_count += 1
                    
                    try:
                        # JSON 데이터 추출
                        json_match = re.search(r'{.*}', line)
                        if not json_match:
                            continue
                            
                        json_str = json_match.group(0)
                        data = json.loads(json_str)
                        
                        # 오더북 스냅샷 데이터 확인
                        if 'exchange' in data and 'symbol' in data and 'bids' in data and 'asks' in data:
                            symbol = data['symbol']
                            
                            # 시퀀스나 타임스탬프 가져오기 (시퀀스 우선)
                            sequence = data.get('seq', data.get('ts', 0))
                            if sequence is None:  # None인 경우 0으로 처리
                                sequence = 0
                            
                            # 거래소+심볼 키 생성
                            key = f"{exchange}_{symbol}"
                            
                            # 심볼 목록에 추가
                            if exchange not in self.exchange_symbols:
                                self.exchange_symbols[exchange] = set()
                            self.exchange_symbols[exchange].add(symbol)
                            
                            # 최신 데이터인지 확인하고 저장
                            # 처음 보는 심볼이거나 현재 시퀀스가 더 큰 경우 업데이트
                            is_newer = False
                            
                            if symbol not in latest_by_symbol:
                                is_newer = True
                            elif sequence is not None and latest_by_symbol[symbol]['sequence'] is not None:
                                # 둘 다 None이 아닌 경우만 비교
                                is_newer = sequence > latest_by_symbol[symbol]['sequence']
                            
                            if is_newer:
                                latest_by_symbol[symbol] = {
                                    'data': data,
                                    'sequence': sequence
                                }
                            
                            processed_count += 1
                    
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        logger.error(f"라인 {line_count} 처리 중 오류 발생: {e}")
                
                # 최신 데이터만 검증
                for symbol, entry in latest_by_symbol.items():
                    self.validate_orderbook(exchange, symbol, entry['data'])
                
                logger.info(f"파일 처리 완료: 총 {line_count}줄, 처리된 오더북 메시지 {processed_count}개, 심볼 수 {len(latest_by_symbol)}개")
            
        except Exception as e:
            logger.error(f"파일 {file_path} 처리 중 오류 발생: {e}")
    
    def validate_orderbook(self, exchange: str, symbol: str, data: Dict[str, Any]) -> None:
        """단일 오더북 메시지 검증 (가격 역전만 검사)"""
        key = f"{exchange}_{symbol}"
        
        # 결과 데이터 카운트 증가
        self.results[exchange][symbol]['data_count'] += 1
        
        # 가격 역전 검증
        bid_count = len(data['bids'])
        ask_count = len(data['asks'])
        
        if bid_count > 0 and ask_count > 0:
            best_bid = data['bids'][0][0]
            best_ask = data['asks'][0][0]
            
            # 저장된 상태 덮어쓰기
            self.latest_orderbooks[key] = {
                'symbol': symbol,
                'exchange': exchange,
                'best_bid': best_bid,
                'best_ask': best_ask,
                'bid_count': bid_count,
                'ask_count': ask_count,
                'timestamp': data.get('ts', 0),
                'sequence': data.get('seq', 0)
            }
            
            if best_bid >= best_ask:
                self.results[exchange][symbol]['price_inversions'] += 1
                self.results[exchange][symbol]['violation_details'].append({
                    'type': 'price_inversion',
                    'ts': data.get('ts', 0),
                    'seq': data.get('seq', 0),
                    'details': f"가격 역전: 최고 매수가 {best_bid} >= 최저 매도가 {best_ask}"
                })
    
    def generate_report(self, output_file: str) -> None:
        """검증 결과 보고서 생성"""
        # 거래소 및 심볼별 결과 집계
        exchange_summary = defaultdict(lambda: {
            'symbols': 0,
            'data_count': 0,
            'price_inversions': 0,
            'inversion_rate': 0
        })
        
        total_summary = {
            'exchanges': len(self.results),
            'symbols': 0,
            'data_count': 0,
            'price_inversions': 0
        }
        
        # 심볼별 결과 정리
        all_results = []
        for exchange, symbols in self.results.items():
            exchange_summary[exchange]['symbols'] = len(symbols)
            total_summary['symbols'] += len(symbols)
            
            for symbol, result in symbols.items():
                exchange_summary[exchange]['data_count'] += result['data_count']
                exchange_summary[exchange]['price_inversions'] += 1 if result['price_inversions'] > 0 else 0
                
                total_summary['data_count'] += result['data_count']
                total_summary['price_inversions'] += 1 if result['price_inversions'] > 0 else 0
                
                key = f"{exchange}_{symbol}"
                latest = self.latest_orderbooks.get(key, {})
                
                # 결과 배열에 추가
                all_results.append({
                    'exchange': exchange,
                    'symbol': symbol,
                    'data_count': result['data_count'],
                    'price_inversions': result['price_inversions'],
                    'has_price_inversion': result['price_inversions'] > 0,
                    'best_bid': latest.get('best_bid', 0),
                    'best_ask': latest.get('best_ask', 0),
                    'bid_count': latest.get('bid_count', 0),
                    'ask_count': latest.get('ask_count', 0),
                    'spread': latest.get('best_ask', 0) - latest.get('best_bid', 0) if latest else 0,
                })
        
        # 가격 역전 비율 계산
        for exchange, summary in exchange_summary.items():
            if summary['symbols'] > 0:
                summary['inversion_rate'] = summary['price_inversions'] / summary['symbols'] * 100
        
        # 결과 정렬 (거래소 > 심볼)
        all_results.sort(key=lambda x: (x['exchange'], x['symbol']))
        
        # 텍스트 보고서 생성
        with open(output_file, 'w', encoding='utf-8') as f:
            # 제목 및 생성 시간
            f.write("===== 오더북 최신 데이터 검증 보고서 =====\n\n")
            f.write(f"생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"검증된 거래소 수: {len(self.results)}\n")
            f.write(f"검증된 심볼 수: {total_summary['symbols']}\n")
            
            # 전체 심볼 수가 0이 아닌 경우에만 백분율 계산
            if total_summary['symbols'] > 0:
                inversion_percentage = total_summary['price_inversions'] / total_summary['symbols'] * 100
                f.write(f"가격 역전 발생 심볼 수: {total_summary['price_inversions']} / {total_summary['symbols']} ({inversion_percentage:.2f}%)\n\n")
            else:
                f.write(f"가격 역전 발생 심볼 수: 0 / 0 (0.00%)\n\n")
            
            # 가격 역전이 있는 심볼 제목
            f.write("== 가격 역전 발생 심볼 목록 ==\n")
            inversion_symbols = [r for r in all_results if r['has_price_inversion']]
            if inversion_symbols:
                for r in inversion_symbols:
                    f.write(f"- {r['exchange']} {r['symbol']}: 최고 매수가 {r['best_bid']} >= 최저 매도가 {r['best_ask']}\n")
            else:
                f.write("가격 역전 발생 심볼 없음\n")
            f.write("\n")
            
            # 거래소별 심볼 목록
            f.write("== 거래소별 심볼 목록 ==\n")
            for exchange, symbols in self.exchange_symbols.items():
                f.write(f"- {exchange}: {', '.join(sorted(symbols))}\n")
            f.write("\n")
            
            # 거래소별 요약
            f.write("== 거래소별 검증 요약 ==\n")
            for exchange, summary in exchange_summary.items():
                f.write(f"** {exchange} **\n")
                f.write(f"  심볼 수: {summary['symbols']}\n")
                f.write(f"  가격 역전 심볼: {summary['price_inversions']}개 ({summary['inversion_rate']:.2f}%)\n\n")
            
            # 심볼별 상세 결과 표
            f.write("== 심볼별 상세 결과 ==\n")
            # 표 헤더
            headers = [
                "거래소", "심볼", "최고 매수가", "최저 매도가", "스프레드", "가격 역전"
            ]
            
            col_widths = [14, 10, 12, 12, 12, 8]
            
            header_line = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |"
            separator = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
            
            f.write(separator + "\n")
            f.write(header_line + "\n")
            f.write(separator + "\n")
            
            # 데이터 행
            for result in all_results:
                inversion = "있음" if result['has_price_inversion'] else "없음"
                
                # 스프레드 계산 (가격 역전인 경우 음수가 됨)
                spread_value = result['best_ask'] - result['best_bid']
                spread = f"{spread_value:.8g}" if spread_value < 0 else f"{spread_value:.8g}"
                
                row = [
                    result['exchange'].ljust(col_widths[0]),
                    result['symbol'].ljust(col_widths[1]),
                    f"{result['best_bid']:.8g}".ljust(col_widths[2]),
                    f"{result['best_ask']:.8g}".ljust(col_widths[3]),
                    spread.ljust(col_widths[4]),
                    inversion.ljust(col_widths[5])
                ]
                
                f.write("| " + " | ".join(row) + " |\n")
            
            f.write(separator + "\n\n")
            
            # 결론
            f.write("== 검증 결론 ==\n")
            f.write("1. 가격 역전 현상: 최고 매수가가 최저 매도가보다 크거나 같은 현상으로, 일시적인 시장 비효율성을 나타냅니다.\n")
            f.write("2. 검증은 각 심볼의 가장 최근 오더북 데이터만을 대상으로 수행되었습니다.\n\n")
            
            # 거래소별 특이사항
            f.write("== 거래소별 특이사항 ==\n")
            for exchange, summary in exchange_summary.items():
                if summary['price_inversions'] > 0:
                    f.write(f"- {exchange}: 가격 역전 발생 심볼 {summary['price_inversions']}개 ({summary['inversion_rate']:.2f}%)\n")
                else:
                    f.write(f"- {exchange}: 가격 역전 없음\n")
        
        logger.info(f"검증 보고서가 {output_file}에 생성되었습니다.")

def main():
    """메인 함수"""
    start_time = datetime.now()
    logger.info(f"검증 시작: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 최신 로그 파일 찾기
    log_files = find_latest_log_files(LOG_FOLDER)
    
    if not log_files:
        logger.error("로그 파일을 찾을 수 없습니다. 프로그램을 종료합니다.")
        return
    
    logger.info(f"검증할 로그 파일 수: {len(log_files)}")
    
    # 검증 객체 생성
    validator = LatestOrderbookValidator()
    
    # 각 로그 파일 처리
    for log_file in log_files:
        validator.process_log_file(log_file)
    
    # 보고서 생성
    validator.generate_report(OUTPUT_FILE)
    
    end_time = datetime.now()
    logger.info(f"검증 완료: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"총 소요 시간: {(end_time - start_time).total_seconds():.2f}초")

if __name__ == "__main__":
    main() 