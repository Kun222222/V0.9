"""
모든 거래소의 오더북 증분 업데이트 검증 스크립트

이 스크립트는 지정된 폴더에서 모든 거래소 로그 파일을 찾아 증분 업데이트 검증을 수행합니다.
"""

import os
import argparse
import glob
import json
from datetime import datetime
from typing import List, Dict, Any

from delta_validator import OrderbookDeltaValidator

def find_log_files(log_dir: str) -> List[str]:
    """
    주어진 디렉토리에서 로그 파일 목록 반환
    
    Args:
        log_dir: 로그 디렉토리 경로
        
    Returns:
        로그 파일 경로 목록
    """
    pattern = os.path.join(log_dir, "*_raw_logger.log")
    return glob.glob(pattern)

def validate_orderbooks(log_files: List[str], max_lines: int = None, symbols: List[str] = None) -> Dict[str, Any]:
    """
    여러 로그 파일의 오더북 검증
    
    Args:
        log_files: 로그 파일 경로 목록
        max_lines: 각 파일당 최대 처리할 줄 수
        symbols: 검증할 심볼 목록
        
    Returns:
        검증 결과
    """
    results = {}
    
    for log_file in log_files:
        validator = OrderbookDeltaValidator(log_file)
        result = validator.validate(max_lines=max_lines, symbols=symbols)
        
        # 결과 출력
        print(validator.get_report())
        print("\n" + "=" * 80 + "\n")
        
        # 결과 저장
        exchange = result["exchange"]
        results[exchange] = result
    
    return results

def save_results(results: Dict[str, Any], output_file: str = None) -> None:
    """
    검증 결과를 파일로 저장
    
    Args:
        results: 검증 결과
        output_file: 출력 파일 경로
    """
    if not output_file:
        timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
        output_file = f"orderbook_validation_{timestamp}.json"
    
    # processed_symbols를 리스트로 변환 (set은 JSON 직렬화 불가)
    for exchange, result in results.items():
        if "processed_symbols" in result["stats"]:
            result["stats"]["processed_symbols"] = sorted(list(result["stats"]["processed_symbols"]))
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"검증 결과가 {output_file}에 저장되었습니다.")

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="모든 거래소의 오더북 증분 업데이트 검증 도구")
    parser.add_argument("--log-dir", help="로그 파일 디렉토리", default="src/logs/raw_data")
    parser.add_argument("--max-lines", type=int, help="각 파일당 최대 처리할 줄 수", default=None)
    parser.add_argument("--symbols", help="검증할 심볼 목록 (쉼표로 구분)", default=None)
    parser.add_argument("--output", help="결과 저장 파일 경로", default=None)
    
    args = parser.parse_args()
    
    # 심볼 목록 파싱
    symbols = None
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
    
    # 로그 파일 찾기
    log_files = find_log_files(args.log_dir)
    if not log_files:
        print(f"로그 파일을 찾을 수 없습니다: {args.log_dir}")
        return
    
    print(f"총 {len(log_files)}개의 로그 파일을 찾았습니다:")
    for f in log_files:
        print(f"  - {f}")
    print()
    
    # 검증 실행
    results = validate_orderbooks(log_files, max_lines=args.max_lines, symbols=symbols)
    
    # 결과 저장
    save_results(results, args.output)

if __name__ == "__main__":
    main() 