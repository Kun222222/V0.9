import csv
import os
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

# 최신 CSV 파일 찾기
def find_latest_csv():
    directory = "temp/api_results"
    files = [f for f in os.listdir(directory) if f.startswith("withdrawal_fees_") and f.endswith(".csv")]
    if not files:
        return None
    latest_file = max(files, key=lambda x: os.path.getmtime(os.path.join(directory, x)))
    return os.path.join(directory, latest_file)

# CSV 파일에서 데이터 읽기
def read_fees_data(csv_file):
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

# 메인 함수
def main():
    latest_csv = find_latest_csv()
    if not latest_csv:
        print("수수료 데이터 CSV 파일을 찾을 수 없습니다.")
        return
    
    print(f"데이터 소스: {latest_csv}")
    fees_data = read_fees_data(latest_csv)
    
    # 각 거래소별로 심볼에 대한 수수료 출력
    exchange_mapping = {
        "업비트": "Upbit",
        "빗썸": "Bithumb",
        "바이낸스": "Binance",
        "바이빗": "Bybit"
    }
    
    # 모든 심볼 통합 (중복 제거)
    all_symbols = set()
    for symbols in symbols_by_exchange.values():
        all_symbols.update(symbols)
    
    # 테이블 생성
    table = PrettyTable()
    table.field_names = ["심볼", "업비트 수수료(KRW)", "빗썸 수수료(KRW)", "바이낸스 수수료(KRW)", "바이빗 수수료(KRW)", "최고 수수료(KRW)", "최저 수수료(KRW)"]
    
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
        
        # 최고 수수료와 최저 수수료 계산
        if fees_list:
            max_fee = max(fees_list)
            min_fee = min(fees_list)
            row.append(f"{max_fee:,.0f}")
            row.append(f"{min_fee:,.0f}")
        else:
            row.append("N/A")
            row.append("N/A")
        
        table.add_row(row)
    
    # 테이블 정렬 및 출력
    table.align = "r"
    table.align["심볼"] = "c"
    print("\n=== 거래소별 심볼 전송 수수료 (원화) - 각 코인별 가장 비싼 경로 적용 ===")
    print(table)
    
    # 거래소별 평균 수수료 계산
    print("\n=== 거래소별 평균 전송 수수료 ===")
    for exchange_kr, exchange_en in exchange_mapping.items():
        fees_sum = 0
        count = 0
        
        for symbol in symbols_by_exchange[exchange_kr]:
            if exchange_en in fees_data and symbol in fees_data[exchange_en]:
                fees_sum += fees_data[exchange_en][symbol]['krw_fee']
                count += 1
        
        if count > 0:
            avg_fee = fees_sum / count
            print(f"{exchange_kr} 평균 수수료: {avg_fee:,.0f} KRW ({count}개 심볼 기준)")
        else:
            print(f"{exchange_kr} 수수료 데이터 없음")

    # 각 코인별 가장 저렴한 전송 방법 찾기
    print("\n=== 각 코인별 가장 저렴하고 비싼 전송 경로 ===")
    cheapest_table = PrettyTable()
    cheapest_table.field_names = ["심볼", "가장 저렴한 거래소", "저렴한 수수료(KRW)", "가장 비싼 거래소", "비싼 수수료(KRW)", "차이(KRW)"]
    
    for symbol in sorted(all_symbols):
        fees_by_exchange = {}
        
        for exchange_kr, exchange_en in exchange_mapping.items():
            if symbol in symbols_by_exchange[exchange_kr] and exchange_en in fees_data and symbol in fees_data[exchange_en]:
                fees_by_exchange[exchange_kr] = fees_data[exchange_en][symbol]['krw_fee']
        
        if len(fees_by_exchange) >= 2:  # 두 개 이상의 거래소에서 데이터가 있는 경우만
            cheapest_exchange = min(fees_by_exchange, key=fees_by_exchange.get)
            cheapest_fee = fees_by_exchange[cheapest_exchange]
            
            most_expensive_exchange = max(fees_by_exchange, key=fees_by_exchange.get)
            most_expensive_fee = fees_by_exchange[most_expensive_exchange]
            
            fee_difference = most_expensive_fee - cheapest_fee
            
            cheapest_table.add_row([
                symbol, 
                cheapest_exchange, 
                f"{cheapest_fee:,.0f}", 
                most_expensive_exchange, 
                f"{most_expensive_fee:,.0f}",
                f"{fee_difference:,.0f}"
            ])
    
    cheapest_table.align = "r"
    cheapest_table.align["심볼"] = "c"
    cheapest_table.align["가장 저렴한 거래소"] = "l"
    cheapest_table.align["가장 비싼 거래소"] = "l"
    print(cheapest_table)

if __name__ == "__main__":
    main() 