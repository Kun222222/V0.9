#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
오더북 스냅샷 데이터 검증 도구

이 스크립트는 다음 항목을 검증합니다:
1. 실제 매수/매도 주문의 개수가 10개씩 있는지 확인 (뎁스 검증)
2. 가격 역전 현상 (최고 매수가 >= 최저 매도가)
3. 시퀀스 역전 (이전 메시지의 시퀀스 번호가 현재보다 큰 경우)
4. 증분 업데이트 오류 (델타 메시지 적용 후 예상 오더북과 실제 오더북 불일치)
"""

import os
import re
import json
import argparse
import logging
import glob
from typing import Dict, List, Tuple, Any, Set
from datetime import datetime
from collections import defaultdict

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 로그 폴더 경로 하드코딩
LOG_FOLDER = "/Users/kun/Desktop/CrossKimpArbitrage/v0.6/src/logs/raw_data"

# 출력 파일 경로
OUTPUT_FILE = "/Users/kun/Desktop/CrossKimpArbitrage/v0.6/src/crosskimp/tools/orderbook_snapshot_validation2.txt"

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

class OrderbookValidator:
    """오더북 데이터 검증기"""
    
    def __init__(self):
        self.exchange_symbols = {}  # 거래소별 심볼 목록
        self.orderbooks = {}  # 거래소+심볼별 최신 오더북 상태
        self.sequences = {}  # 거래소+심볼별 마지막 시퀀스
        self.results = defaultdict(lambda: defaultdict(lambda: {
            'data_count': 0,
            'depth_violations': 0,
            'price_inversions': 0,
            'sequence_inversions': 0,
            'delta_errors': 0,
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
        """로그 파일 처리"""
        try:
            exchange = self.extract_exchange_from_filename(file_path)
            logger.info(f"파일 처리 중: {file_path} (거래소: {exchange})")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                line_count = 0
                processed_count = 0
                
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
                            
                            # 거래소+심볼 키 생성
                            key = f"{exchange}_{symbol}"
                            
                            # 심볼 목록에 추가
                            if exchange not in self.exchange_symbols:
                                self.exchange_symbols[exchange] = set()
                            self.exchange_symbols[exchange].add(symbol)
                            
                            # 데이터 검증
                            self.validate_orderbook(exchange, symbol, data)
                            processed_count += 1
                    
                    except json.JSONDecodeError:
                        continue
                    except Exception as e:
                        logger.error(f"라인 {line_count} 처리 중 오류 발생: {e}")
                
                logger.info(f"파일 처리 완료: 총 {line_count}줄, 처리된 오더북 메시지 {processed_count}개")
            
        except Exception as e:
            logger.error(f"파일 {file_path} 처리 중 오류 발생: {e}")
    
    def validate_orderbook(self, exchange: str, symbol: str, data: Dict[str, Any]) -> None:
        """단일 오더북 메시지 검증"""
        key = f"{exchange}_{symbol}"
        
        # 결과 데이터 카운트 증가
        self.results[exchange][symbol]['data_count'] += 1
        
        # 1. 뎁스 검증 (매수/매도 주문의 개수 확인)
        bid_count = len(data['bids'])
        ask_count = len(data['asks'])
        
        # 실제 뎁스가 10보다 작은 경우
        if bid_count < 10 or ask_count < 10:
            self.results[exchange][symbol]['depth_violations'] += 1
            self.results[exchange][symbol]['violation_details'].append({
                'type': 'depth',
                'ts': data.get('ts', 0),
                'details': f"뎁스 위반: 매수 ({bid_count}) / 매도 ({ask_count})"
            })
        
        # 2. 가격 역전 검증
        if bid_count > 0 and ask_count > 0:
            best_bid = data['bids'][0][0]
            best_ask = data['asks'][0][0]
            
            if best_bid >= best_ask:
                self.results[exchange][symbol]['price_inversions'] += 1
                self.results[exchange][symbol]['violation_details'].append({
                    'type': 'price_inversion',
                    'ts': data.get('ts', 0),
                    'details': f"가격 역전: 최고 매수가 {best_bid} >= 최저 매도가 {best_ask}"
                })
        
        # 3. 시퀀스 역전 검증
        if 'seq' in data and data['seq'] is not None:
            current_seq = data['seq']
            
            if key in self.sequences and self.sequences[key] is not None:
                prev_seq = self.sequences[key]
                
                # 시퀀스가 역전된 경우
                if prev_seq > current_seq:
                    self.results[exchange][symbol]['sequence_inversions'] += 1
                    self.results[exchange][symbol]['violation_details'].append({
                        'type': 'sequence_inversion',
                        'ts': data.get('ts', 0),
                        'details': f"시퀀스 역전: 이전 {prev_seq} > 현재 {current_seq}"
                    })
            
            # 현재 시퀀스 기록
            self.sequences[key] = current_seq
        
        # 4. 증분 업데이트 오류 검증
        # 델타 메시지인 경우 (스냅샷이 아닌 경우)
        if key in self.orderbooks and data.get('is_snapshot', False) is False:
            # 이전 오더북
            prev_orderbook = self.orderbooks[key]
            
            # 이전 오더북에 델타 적용
            expected_orderbook = self.apply_delta(prev_orderbook, data)
            
            # 현재 오더북과 비교
            if not self.compare_orderbooks(expected_orderbook, data):
                self.results[exchange][symbol]['delta_errors'] += 1
                self.results[exchange][symbol]['violation_details'].append({
                    'type': 'delta_error',
                    'ts': data.get('ts', 0),
                    'details': f"증분 업데이트 오류"
                })
        
        # 현재 오더북 상태 저장
        self.orderbooks[key] = data
    
    def apply_delta(self, base_orderbook: Dict[str, Any], delta: Dict[str, Any]) -> Dict[str, Any]:
        """델타 메시지를 적용하여 예상 오더북 생성"""
        # 깊은 복사
        expected = {
            'bids': {price: qty for price, qty in base_orderbook['bids']},
            'asks': {price: qty for price, qty in base_orderbook['asks']}
        }
        
        # 델타 적용
        for side, deltas in [('bids', delta.get('bids', [])), ('asks', delta.get('asks', []))]:
            for price, qty in deltas:
                if qty == 0:
                    # 해당 가격 삭제
                    if price in expected[side]:
                        del expected[side][price]
                else:
                    # 해당 가격 추가 또는 업데이트
                    expected[side][price] = qty
        
        # 딕셔너리에서 리스트로 변환
        result = {
            'bids': sorted([(float(price), qty) for price, qty in expected['bids'].items()], reverse=True),
            'asks': sorted([(float(price), qty) for price, qty in expected['asks'].items()])
        }
        
        return result
    
    def compare_orderbooks(self, expected: Dict[str, List], actual: Dict[str, Any]) -> bool:
        """예상 오더북과 실제 오더북 비교"""
        actual_bids = {price: qty for price, qty in actual['bids']}
        actual_asks = {price: qty for price, qty in actual['asks']}
        
        expected_bids = {price: qty for price, qty in expected['bids']}
        expected_asks = {price: qty for price, qty in expected['asks']}
        
        return (expected_bids == actual_bids and expected_asks == actual_asks)
    
    def generate_report(self, output_file: str) -> None:
        """검증 결과 보고서 생성"""
        # 거래소 및 심볼별 결과 집계
        exchange_summary = defaultdict(lambda: {
            'symbols': 0,
            'data_count': 0,
            'depth_violations': 0,
            'price_inversions': 0,
            'sequence_inversions': 0,
            'delta_errors': 0
        })
        
        total_summary = {
            'exchanges': len(self.results),
            'symbols': 0,
            'data_count': 0,
            'depth_violations': 0,
            'price_inversions': 0,
            'sequence_inversions': 0,
            'delta_errors': 0
        }
        
        # 심볼별 결과 정리
        all_results = []
        for exchange, symbols in self.results.items():
            exchange_summary[exchange]['symbols'] = len(symbols)
            total_summary['symbols'] += len(symbols)
            
            for symbol, result in symbols.items():
                exchange_summary[exchange]['data_count'] += result['data_count']
                exchange_summary[exchange]['depth_violations'] += result['depth_violations']
                exchange_summary[exchange]['price_inversions'] += result['price_inversions']
                exchange_summary[exchange]['sequence_inversions'] += result['sequence_inversions']
                exchange_summary[exchange]['delta_errors'] += result['delta_errors']
                
                total_summary['data_count'] += result['data_count']
                total_summary['depth_violations'] += result['depth_violations']
                total_summary['price_inversions'] += result['price_inversions']
                total_summary['sequence_inversions'] += result['sequence_inversions']
                total_summary['delta_errors'] += result['delta_errors']
                
                # 결과 배열에 추가
                all_results.append({
                    'exchange': exchange,
                    'symbol': symbol,
                    'data_count': result['data_count'],
                    'depth_violations': result['depth_violations'],
                    'depth_violations_pct': (result['depth_violations'] / result['data_count'] * 100) if result['data_count'] > 0 else 0,
                    'price_inversions': result['price_inversions'],
                    'price_inversions_pct': (result['price_inversions'] / result['data_count'] * 100) if result['data_count'] > 0 else 0,
                    'sequence_inversions': result['sequence_inversions'],
                    'sequence_inversions_pct': (result['sequence_inversions'] / result['data_count'] * 100) if result['data_count'] > 0 else 0,
                    'delta_errors': result['delta_errors'],
                    'delta_errors_pct': (result['delta_errors'] / result['data_count'] * 100) if result['data_count'] > 0 else 0
                })
                
        # 결과 정렬 (거래소 > 심볼)
        all_results.sort(key=lambda x: (x['exchange'], x['symbol']))
        
        # 텍스트 보고서 생성
        with open(output_file, 'w', encoding='utf-8') as f:
            # 제목 및 생성 시간
            f.write("===== 오더북 스냅샷 검증 보고서 =====\n\n")
            f.write(f"생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"검증된 거래소 수: {len(self.results)}\n")
            f.write(f"검증된 심볼 수: {total_summary['symbols']}\n")
            f.write(f"처리된 데이터 수: {total_summary['data_count']}\n\n")
            
            # 거래소별 심볼 목록
            f.write("== 거래소별 심볼 목록 ==\n")
            for exchange, symbols in self.exchange_symbols.items():
                f.write(f"- {exchange}: {', '.join(sorted(symbols))}\n")
            f.write("\n")
            
            # 전체 요약
            f.write("== 전체 검증 요약 ==\n")
            total_data = total_summary['data_count']
            if total_data > 0:
                depth_pct = (total_summary['depth_violations'] / total_data * 100)
                price_pct = (total_summary['price_inversions'] / total_data * 100)
                seq_pct = (total_summary['sequence_inversions'] / total_data * 100)
                delta_pct = (total_summary['delta_errors'] / total_data * 100)
            else:
                depth_pct = price_pct = seq_pct = delta_pct = 0
                
            f.write(f"총 뎁스 위반: {total_summary['depth_violations']}건 ({depth_pct:.2f}%)\n")
            f.write(f"총 가격 역전: {total_summary['price_inversions']}건 ({price_pct:.2f}%)\n")
            f.write(f"총 시퀀스 역전: {total_summary['sequence_inversions']}건 ({seq_pct:.2f}%)\n")
            f.write(f"총 증분 업데이트 오류: {total_summary['delta_errors']}건 ({delta_pct:.2f}%)\n\n")
            
            # 거래소별 요약
            f.write("== 거래소별 검증 요약 ==\n")
            for exchange, summary in exchange_summary.items():
                ex_data = summary['data_count']
                f.write(f"** {exchange} **\n")
                f.write(f"  심볼 수: {summary['symbols']}\n")
                f.write(f"  데이터 수: {ex_data}\n")
                
                if ex_data > 0:
                    depth_pct = (summary['depth_violations'] / ex_data * 100)
                    price_pct = (summary['price_inversions'] / ex_data * 100)
                    seq_pct = (summary['sequence_inversions'] / ex_data * 100)
                    delta_pct = (summary['delta_errors'] / ex_data * 100)
                else:
                    depth_pct = price_pct = seq_pct = delta_pct = 0
                    
                f.write(f"  뎁스 위반: {summary['depth_violations']}건 ({depth_pct:.2f}%)\n")
                f.write(f"  가격 역전: {summary['price_inversions']}건 ({price_pct:.2f}%)\n")
                f.write(f"  시퀀스 역전: {summary['sequence_inversions']}건 ({seq_pct:.2f}%)\n")
                f.write(f"  증분 업데이트 오류: {summary['delta_errors']}건 ({delta_pct:.2f}%)\n\n")
            
            # 심볼별 상세 결과 표
            f.write("== 심볼별 상세 결과 ==\n")
            # 표 헤더
            headers = [
                "거래소", "심볼", "데이터 수", 
                "뎁스 위반", "위반 %", 
                "가격 역전", "역전 %", 
                "시퀀스 역전", "역전 %", 
                "증분 오류", "오류 %"
            ]
            
            col_widths = [10, 10, 10, 10, 10, 10, 10, 12, 10, 10, 10]
            
            header_line = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |"
            separator = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
            
            f.write(separator + "\n")
            f.write(header_line + "\n")
            f.write(separator + "\n")
            
            # 데이터 행
            for result in all_results:
                row = [
                    result['exchange'].ljust(col_widths[0]),
                    result['symbol'].ljust(col_widths[1]),
                    str(result['data_count']).ljust(col_widths[2]),
                    str(result['depth_violations']).ljust(col_widths[3]),
                    f"{result['depth_violations_pct']:.2f}%".ljust(col_widths[4]),
                    str(result['price_inversions']).ljust(col_widths[5]),
                    f"{result['price_inversions_pct']:.2f}%".ljust(col_widths[6]),
                    str(result['sequence_inversions']).ljust(col_widths[7]),
                    f"{result['sequence_inversions_pct']:.2f}%".ljust(col_widths[8]),
                    str(result['delta_errors']).ljust(col_widths[9]),
                    f"{result['delta_errors_pct']:.2f}%".ljust(col_widths[10])
                ]
                
                f.write("| " + " | ".join(row) + " |\n")
            
            f.write(separator + "\n\n")
            
            # 결론
            f.write("== 검증 결론 ==\n")
            f.write("1. 뎁스 위반: 오더북의 깊이가 최소 기준(10개)보다 적은 경우\n")
            f.write("2. 가격 역전 현상: 최고 매수가가 최저 매도가보다 크거나 같은 현상으로, 일시적인 시장 비효율성을 나타냅니다.\n")
            f.write("3. 시퀀스 역전 현상: 이전 시퀀스 번호가 현재 시퀀스 번호보다 큰 경우로, 메시지 처리 순서가 뒤바뀐 경우입니다.\n")
            f.write("4. 증분 업데이트 오류: 델타 메시지가 제대로 적용되지 않아 발생하는 오류입니다.\n\n")
            
            # 거래소별 특이사항
            f.write("== 거래소별 특이사항 ==\n")
            for exchange, summary in exchange_summary.items():
                ex_data = summary['data_count']
                if ex_data == 0:
                    continue
                
                f.write(f"- {exchange}: ")
                issues = []
                
                if summary['depth_violations'] > 0:
                    depth_pct = (summary['depth_violations'] / ex_data * 100)
                    issues.append(f"뎁스 위반 {summary['depth_violations']}건 ({depth_pct:.2f}%)")
                    
                if summary['price_inversions'] > 0:
                    price_pct = (summary['price_inversions'] / ex_data * 100)
                    issues.append(f"가격 역전 {summary['price_inversions']}건 ({price_pct:.2f}%)")
                    
                if summary['sequence_inversions'] > 0:
                    seq_pct = (summary['sequence_inversions'] / ex_data * 100)
                    issues.append(f"시퀀스 역전 {summary['sequence_inversions']}건 ({seq_pct:.2f}%)")
                    
                if summary['delta_errors'] > 0:
                    delta_pct = (summary['delta_errors'] / ex_data * 100)
                    issues.append(f"증분 업데이트 오류 {summary['delta_errors']}건 ({delta_pct:.2f}%)")
                    
                if issues:
                    f.write(", ".join(issues) + "\n")
                else:
                    f.write("특이사항 없음\n")
        
        logger.info(f"검증 보고서가 {output_file}에 생성되었습니다.")

def main():
    """메인 함수"""
    # 명령행 인수 처리
    parser = argparse.ArgumentParser(description="오더북 스냅샷 데이터 검증 도구")
    parser.add_argument("--output", "-o", default=OUTPUT_FILE, 
                        help=f"출력 파일 경로 (기본값: {OUTPUT_FILE})")
    parser.add_argument("--verbose", "-v", action="store_true", help="상세 로깅 활성화")
    parser.add_argument("--use-args", "-a", action="store_true", help="명령행에서 로그 파일 지정 (자동 검색 대신)")
    parser.add_argument("log_files", nargs="*", help="분석할 로그 파일 경로 (--use-args와 함께 사용)")
    
    args = parser.parse_args()
    
    # 상세 로깅 설정
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    start_time = datetime.now()
    logger.info(f"검증 시작: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 검증 객체 생성
    validator = OrderbookValidator()
    
    # 로그 파일 목록 결정
    if args.use_args and args.log_files:
        # 명령행 인수로 지정된 파일 사용
        log_files = args.log_files
        logger.info(f"명령행에서 지정된 {len(log_files)}개의 로그 파일 사용")
    else:
        # 자동으로 최신 로그 파일 검색
        log_files = find_latest_log_files(LOG_FOLDER)
        if not log_files:
            logger.error("로그 파일을 찾을 수 없습니다. 프로그램을 종료합니다.")
            return
        
        logger.info(f"자동으로 검색된 최신 로그 파일 {len(log_files)}개 사용")
    
    # 각 로그 파일 처리
    for log_file in log_files:
        validator.process_log_file(log_file)
    
    # 보고서 생성
    validator.generate_report(args.output)
    
    end_time = datetime.now()
    logger.info(f"검증 완료: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"총 소요 시간: {(end_time - start_time).total_seconds():.2f}초")

if __name__ == "__main__":
    main() 