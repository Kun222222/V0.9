import configparser
import os
import json
import re

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

def calculate_transfer_fee(exchange, symbol=None):
    """
    주어진 거래소와 심볼에 대한 이체 수수료를 계산합니다.
    
    Args:
        exchange (str): 거래소 이름 (예: 'upbit', 'binance')
        symbol (str, optional): 코인 심볼 (없으면 기본 수식 사용)
        
    Returns:
        float: 계산된 이체 수수료 (KRW)
    """
    config = load_config()
    
    # 기본 거래 금액 가져오기
    base_order_amount_str = config['trading.settings']['base_order_amount_krw']
    base_order_amount_krw = extract_number(base_order_amount_str)
    
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
        fee = numerator / base_order_amount_krw
    else:
        # 상수인 경우
        fee = float(fee_formula)
    
    return fee

def main():
    """각 거래소별 이체 수수료를 계산하고 출력합니다."""
    exchanges = ["upbit", "bithumb", "binance", "binance_future", "bybit", "bybit_future"]
    
    print("\n===== 거래소별 이체 수수료 (%) =====")
    print(f"{'거래소':15} | {'이체 수수료(%)':15} | {'기준 금액 1천만원':20}")
    print("-" * 60)
    
    for exchange in exchanges:
        fee = calculate_transfer_fee(exchange)
        fee_pct = fee * 100  # 백분율로 변환
        fee_amount = fee * 10000000  # 실제 이체 수수료 금액 (기준 1천만원)
        print(f"{exchange:15} | {fee_pct:15.6f} | {fee_amount:15,.0f} KRW")

if __name__ == "__main__":
    main() 