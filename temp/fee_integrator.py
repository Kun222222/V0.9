import configparser
import os
import json
import re
import csv
from prettytable import PrettyTable

# 로그에서 추출한 각 거래소별 심볼 목록
symbols_by_exchange = {
    "빗썸": ["DOGE", "LAYER", "SOL", "SUI", "USDT", "XRP"],
    "업비트": ["ADA", "ARKM", "AUCTION", "BERA", "CARV", "CELO", "DOGE", "EOS", "HBAR", "HIFI", 
             "LAYER", "MEW", "NEAR", "NEO", "ONDO", "ORCA", "SAFE", "SEI", "SOL", "SUI", 
             "TRUMP", "USDT", "VANA", "WAXP", "XLM", "XRP", "ZRO"],
    "바이낸스": ["ADA", "ARKM", "AUCTION", "BERA", "CELO", "DOGE", "EOS", "HBAR", "HIFI", 
              "LAYER", "NEAR", "NEO", "ORCA", "SEI", "SOL", "SUI", "TRUMP", "VANA", 
              "WAXP", "XLM", "XRP", "ZRO"],
    "바이빗": ["ADA", "ARKM", "BERA", "CARV", "CELO", "DOGE", "EOS", "HBAR", "MEW", 
             "NEAR", "ONDO", "SAFE", "SEI", "SOL", "SUI", "TRUMP", "VANA"]
}

# 거래소 이름 매핑
exchange_mapping = {
    "업비트": "Upbit",
    "빗썸": "Bithumb",
    "바이낸스": "Binance",
    "바이빗": "Bybit"
}

def load_config():
    """설정 파일을 로드합니다."""
    config = configparser.ConfigParser()
    config_path = os.path.join('src', 'crosskimp', 'common', 'config', 'exchange_settings.cfg')
    config.read(config_path)
    return config

def extract_number(value_str):
    """문자열에서 숫자만 추출합니다."""
    # 주석이나 기타 문자 제거하고 숫자만 추출
    number_str = re.search(r'^\s*(\d+)', value_str).group(1)
    return float(number_str)

def calculate_transfer_fee(exchange, base_amount, symbol=None):
    """
    주어진 거래소와 심볼에 대한 이체 수수료를 계산합니다.
    
    Args:
        exchange (str): 거래소 이름 (예: 'upbit', 'binance')
        base_amount (float): 기본 거래 금액 (KRW)
        symbol (str, optional): 코인 심볼 (없으면 기본 수식 사용)
        
    Returns:
        float: 계산된 이체 수수료 비율
    """
    config = load_config()
    
    # 거래소 이름 정규화
    exchange = exchange.lower()
    if exchange == 'binance_spot':
        exchange = 'binance'
    
    # 거래소 섹션 이름 정의
    section_name = f'exchanges.{exchange}'
    
    if section_name not in config:
        raise ValueError(f"거래소 '{exchange}'에 대한 설정을 찾을 수 없습니다")
    
    # 이체 수수료 수식 가져오기
    fee_formula = config[section_name]['transfer_fee_formula']
    
    # 수식 평가
    if "base_order_amount_krw" in fee_formula:
        # 현재는 단순 수식만 지원 (1000 / base_order_amount_krw)
        numerator_str = fee_formula.split('/')[0].strip().replace("'", "").replace('"', '')
        numerator = float(numerator_str)
        fee = numerator / base_amount
    else:
        # 상수인 경우
        fee = float(fee_formula)
    
    return fee

def find_latest_csv():
    """최신 CSV 파일을 찾습니다."""
    directory = "temp/api_results"
    files = [f for f in os.listdir(directory) if f.startswith("withdrawal_fees_") and f.endswith(".csv")]
    if not files:
        return None
    latest_file = max(files, key=lambda x: os.path.getmtime(os.path.join(directory, x)))
    return os.path.join(directory, latest_file)

def read_fees_data(csv_file):
    """CSV 파일에서 데이터를 읽어옵니다."""
    fees_data = {}
    
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            exchange = row['exchange']
            symbol = row['symbol']
            
            if exchange not in fees_data:
                fees_data[exchange] = {}
            
            # 수수료 정보 추출
            fee = float(row['fee'])
            krw_fee = float(row['krw_fee'])
            name = row['name']
            network = row['network']
            
            # 동일 코인에 대해 여러 네트워크가 있는 경우, 가장 비싼 수수료를 저장
            if symbol not in fees_data[exchange]:
                fees_data[exchange][symbol] = {
                    'fee': fee,
                    'krw_fee': krw_fee,
                    'name': name,
                    'network': network
                }
            else:
                # 이미 있는 수수료보다 현재 수수료가 더 비싸면 대체
                if krw_fee > fees_data[exchange][symbol]['krw_fee']:
                    fees_data[exchange][symbol] = {
                        'fee': fee,
                        'krw_fee': krw_fee,
                        'name': name,
                        'network': network
                    }
    
    return fees_data

def main():
    """메인 함수"""
    # 설정 로드
    config = load_config()
    base_order_amount_str = config['trading.settings']['base_order_amount_krw']
    base_order_amount_krw = extract_number(base_order_amount_str)
    
    # 수수료 데이터 로드
    latest_csv = find_latest_csv()
    if not latest_csv:
        print("수수료 데이터 CSV 파일을 찾을 수 없습니다.")
        return
    
    print(f"데이터 소스: {latest_csv}")
    print(f"기본 거래 금액: {base_order_amount_krw:,.0f} KRW")
    fees_data = read_fees_data(latest_csv)
    
    # 모든 심볼 통합 (중복 제거)
    all_symbols = set()
    for symbols in symbols_by_exchange.values():
        all_symbols.update(symbols)
    
    # 테이블 생성
    table = PrettyTable()
    table.field_names = ["심볼", "업비트(원)", "빗썸(원)", "바이낸스(원)", "바이빗(원)", "최고 수수료(원)", "최고/기본금액(%)"]
    
    # 데이터 채우기
    for symbol in sorted(all_symbols):
        row = [symbol]
        fees_list = []
        
        for exchange_kr, exchange_en in exchange_mapping.items():
            # 해당 거래소에서 이 심볼을 처리하는지 확인
            if symbol in symbols_by_exchange[exchange_kr] and exchange_en in fees_data and symbol in fees_data[exchange_en]:
                krw_fee = fees_data[exchange_en][symbol]['krw_fee']
                row.append(f"{krw_fee:,.0f}")
                fees_list.append(krw_fee)
            else:
                row.append("N/A")
        
        # 최고 수수료와 비율 계산
        if fees_list:
            max_fee = max(fees_list)
            max_fee_ratio = (max_fee / base_order_amount_krw) * 100  # 백분율로 변환
            row.append(f"{max_fee:,.0f}")
            row.append(f"{max_fee_ratio:.6f}")
        else:
            row.append("N/A")
            row.append("N/A")
        
        table.add_row(row)
    
    # 테이블 정렬 및 출력
    table.align = "r"
    table.align["심볼"] = "c"
    print("\n=== 거래소별 심볼 전송 수수료 (원화) - 실제 수수료 기준 ===")
    print(table)
    
    # 거래소별 평균 수수료 계산
    print("\n=== 거래소별 평균 전송 수수료 및 비율 ===")
    print(f"{'거래소':15} | {'평균 수수료(KRW)':15} | {'수수료/기본금액(%)':15} | {'심볼 수':8}")
    print("-" * 70)
    
    for exchange_kr, exchange_en in exchange_mapping.items():
        fees_sum = 0
        count = 0
        
        for symbol in symbols_by_exchange[exchange_kr]:
            if exchange_en in fees_data and symbol in fees_data[exchange_en]:
                fees_sum += fees_data[exchange_en][symbol]['krw_fee']
                count += 1
        
        if count > 0:
            avg_fee = fees_sum / count
            avg_fee_ratio = (avg_fee / base_order_amount_krw) * 100  # 백분율로 변환
            print(f"{exchange_kr:15} | {avg_fee:15,.0f} | {avg_fee_ratio:15.6f} | {count:8}")
        else:
            print(f"{exchange_kr:15} | {'N/A':15} | {'N/A':15} | {0:8}")
    
    # 각 코인별 가장 저렴하고 비싼 전송 방법 비교 (5개만)
    print("\n=== 수수료/기본금액 비율이 가장 높은 5개 코인 ===")
    fee_ratios = []
    
    for symbol in all_symbols:
        max_fee = 0
        for exchange_kr, exchange_en in exchange_mapping.items():
            if symbol in symbols_by_exchange[exchange_kr] and exchange_en in fees_data and symbol in fees_data[exchange_en]:
                krw_fee = fees_data[exchange_en][symbol]['krw_fee']
                if krw_fee > max_fee:
                    max_fee = krw_fee
        
        if max_fee > 0:
            ratio = (max_fee / base_order_amount_krw) * 100  # 백분율로 변환
            fee_ratios.append((symbol, max_fee, ratio))
    
    # 비율이 높은 순으로 정렬
    fee_ratios.sort(key=lambda x: x[2], reverse=True)
    
    # 상위 5개만 출력
    print(f"{'심볼':10} | {'최고 수수료(KRW)':15} | {'수수료/기본금액(%)':15}")
    print("-" * 50)
    
    for symbol, fee, ratio in fee_ratios[:5]:
        print(f"{symbol:10} | {fee:15,.0f} | {ratio:15.6f}")

if __name__ == "__main__":
    main() 