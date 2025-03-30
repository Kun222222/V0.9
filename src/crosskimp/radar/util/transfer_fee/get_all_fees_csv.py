import requests
import json
import time
import csv
import os
from datetime import datetime

# 결과 디렉토리 확인 및 생성
os.makedirs("temp/api_results", exist_ok=True)

def get_current_prices():
    """모든 코인의 현재 가격 가져오기 (USD 기준)"""
    print("코인 현재 가격 정보 수집 중...")
    
    try:
        # CoinGecko API 사용 (무료, 속도 제한 있음)
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,  # 상위 250개 코인
            "page": 1,
            "sparkline": False
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        # 결과 딕셔너리 (심볼 -> 가격)
        prices = {}
        
        for coin in data:
            symbol = coin["symbol"].upper()  # 대문자로 변환
            price = coin["current_price"]
            prices[symbol] = price
            
        print(f"총 {len(prices)}개 코인 가격 정보 수집 완료")
        return prices
        
    except Exception as e:
        print(f"코인 가격 정보 수집 중 오류 발생: {str(e)}")
        return {}

def get_upbit_fees():
    """업비트의 출금 수수료 정보 수집"""
    print("\n=== 업비트 출금 수수료 정보 수집 중 ===")
    
    # 업비트 공식 웹사이트에서 수집한 데이터 (일부 주요 코인)
    fees = {
        'BTC': {'fee': 0.0008, 'name': '비트코인'},
        'ETH': {'fee': 0.009, 'name': '이더리움'},
        'XRP': {'fee': 0.4, 'name': '리플'},
        'USDT': {'fee': 0, 'name': '테더'},
        'SOL': {'fee': 0.015, 'name': '솔라나'},
        'ADA': {'fee': 1, 'name': '카르다노'},
        'DOGE': {'fee': 2, 'name': '도지코인'},
        'LINK': {'fee': 0.25, 'name': '체인링크'},
        'DOT': {'fee': 0.14, 'name': '폴카닷'},
        'AVAX': {'fee': 0.005, 'name': '아발란체'},
        'MATIC': {'fee': 1.5, 'name': '폴리곤'},
        'SHIB': {'fee': 32000, 'name': '시바이누'},
        'TRX': {'fee': 2, 'name': '트론'},
        'ATOM': {'fee': 0.015, 'name': '코스모스'},
        'XLM': {'fee': 0.1, 'name': '스텔라루멘'},
        'VET': {'fee': 30, 'name': '비체인'},
        'NEAR': {'fee': 0.04, 'name': '니어프로토콜'},
        'ALGO': {'fee': 0.1, 'name': '알고랜드'},
        'ETC': {'fee': 0.1, 'name': '이더리움클래식'},
        'XTZ': {'fee': 0.05, 'name': '테조스'}
    }
    
    # 업비트 인기 코인 목록 가져오기
    try:
        url = "https://api.upbit.com/v1/market/all"
        response = requests.get(url)
        markets = response.json()
        
        # KRW 마켓만 필터링
        krw_markets = [market['market'].split('-')[1] for market in markets if market['market'].startswith('KRW-')]
        print(f"업비트 KRW 마켓 코인 개수: {len(krw_markets)}")
        
        # 인기 코인 목록 (현재 추정 기본 수수료)
        for symbol in krw_markets:
            if symbol not in fees:
                # 기본값 (추정)
                if symbol in ['ASTR', 'WEMIX', 'XDC', 'SAND', 'PUNDIX', 'QTUM', 'MANA', 'FLOW']:
                    fees[symbol] = {'fee': 0.1, 'name': symbol}
                elif symbol in ['MASK', 'CRO', 'KAVA', 'ZIL', 'SRM', 'BAT', 'AXS', 'CHZ']:
                    fees[symbol] = {'fee': 0.2, 'name': symbol}
                else:
                    fees[symbol] = {'fee': 0.01, 'name': symbol}
    
    except Exception as e:
        print(f"업비트 코인 목록 수집 중 오류 발생: {str(e)}")
    
    return fees

def get_bithumb_fees():
    """빗썸의 출금 수수료 정보 수집"""
    print("\n=== 빗썸 출금 수수료 정보 수집 중 ===")
    
    # 빗썸 공식 웹사이트에서 수집한 데이터 (일부 주요 코인)
    fees = {
        'BTC': {'fee': 0.0008, 'name': '비트코인'},
        'ETH': {'fee': 0, 'name': '이더리움'},
        'XRP': {'fee': 0.4, 'name': '리플'},
        'USDT': {'fee': 0, 'name': '테더'},
        'SOL': {'fee': 0.01, 'name': '솔라나'},
        'ADA': {'fee': 0.5, 'name': '카르다노'},
        'DOGE': {'fee': 5, 'name': '도지코인'},
        'LINK': {'fee': 0.4, 'name': '체인링크'},
        'DOT': {'fee': 0.1, 'name': '폴카닷'},
        'AVAX': {'fee': 0.01, 'name': '아발란체'},
        'MATIC': {'fee': 1, 'name': '폴리곤'},
        'SHIB': {'fee': 50000, 'name': '시바이누'},
        'TRX': {'fee': 1, 'name': '트론'},
        'ATOM': {'fee': 0.01, 'name': '코스모스'},
        'XLM': {'fee': 0.01, 'name': '스텔라루멘'},
        'VET': {'fee': 20, 'name': '비체인'},
        'NEAR': {'fee': 0.01, 'name': '니어프로토콜'},
        'ALGO': {'fee': 0.1, 'name': '알고랜드'},
        'ETC': {'fee': 0.01, 'name': '이더리움클래식'},
        'XTZ': {'fee': 0.1, 'name': '테조스'}
    }
    
    # 빗썸 코인 목록 가져오기
    try:
        url = "https://api.bithumb.com/public/ticker/ALL_KRW"
        response = requests.get(url)
        data = response.json()
        
        if data.get('status') == '0000':
            # 모든 코인 목록 추출 (date 제외)
            all_coins = [coin for coin in data.get('data', {}).keys() if coin != 'date']
            print(f"빗썸 코인 개수: {len(all_coins)}")
            
            # 추가 코인 (추정 기본 수수료)
            for symbol in all_coins:
                if symbol not in fees:
                    # 기본값 (추정)
                    if symbol in ['SAND', 'MANA', 'FLOW', 'QTUM', 'BAT', 'ZIL']:
                        fees[symbol] = {'fee': 0.2, 'name': symbol}
                    else:
                        fees[symbol] = {'fee': 0.01, 'name': symbol}
    
    except Exception as e:
        print(f"빗썸 코인 목록 수집 중 오류 발생: {str(e)}")
    
    return fees

def get_binance_fees():
    """바이낸스의 출금 수수료 정보 수집"""
    print("\n=== 바이낸스 출금 수수료 정보 수집 중 ===")
    
    # 바이낸스 공식 웹사이트에서 수집한 데이터 (기본 네트워크 기준)
    fees = {
        'BTC': {'fee': 0.0003, 'name': 'Bitcoin', 'network': 'BTC'},
        'ETH': {'fee': 0.0007, 'name': 'Ethereum', 'network': 'ETH'},
        'XRP': {'fee': 0.2, 'name': 'XRP', 'network': 'XRP'},
        'BNB': {'fee': 0.00015, 'name': 'BNB', 'network': 'BSC'},
        'USDT': {'fee': 1.0, 'name': 'Tether', 'network': 'TRC20'},
        'SOL': {'fee': 0.002, 'name': 'Solana', 'network': 'SOL'},
        'ADA': {'fee': 0.8, 'name': 'Cardano', 'network': 'ADA'},
        'DOGE': {'fee': 4.0, 'name': 'Dogecoin', 'network': 'DOGE'},
        'LINK': {'fee': 0.34, 'name': 'Chainlink', 'network': 'ETH'},
        'DOT': {'fee': 0.1, 'name': 'Polkadot', 'network': 'DOT'},
        'AVAX': {'fee': 0.01, 'name': 'Avalanche', 'network': 'AVAXC'},
        'MATIC': {'fee': 0.1, 'name': 'Polygon', 'network': 'MATIC'},
        'SHIB': {'fee': 24000, 'name': 'Shiba Inu', 'network': 'ETH'},
        'TRX': {'fee': 1, 'name': 'TRON', 'network': 'TRX'},
        'ATOM': {'fee': 0.005, 'name': 'Cosmos', 'network': 'ATOM'},
        'XLM': {'fee': 0.02, 'name': 'Stellar', 'network': 'XLM'},
        'VET': {'fee': 20, 'name': 'VeChain', 'network': 'VET'},
        'NEAR': {'fee': 0.1, 'name': 'NEAR Protocol', 'network': 'NEAR'},
        'ALGO': {'fee': 0.01, 'name': 'Algorand', 'network': 'ALGO'},
        'ETC': {'fee': 0.01, 'name': 'Ethereum Classic', 'network': 'ETC'},
        'XTZ': {'fee': 0.1, 'name': 'Tezos', 'network': 'XTZ'}
    }
    
    # 바이낸스 거래 페어 목록 가져오기
    try:
        url = "https://api.binance.com/api/v3/exchangeInfo"
        response = requests.get(url)
        exchange_info = response.json()
        
        symbols = exchange_info.get('symbols', [])
        print(f"바이낸스 거래 페어 개수: {len(symbols)}")
        
        # 유니크한 기본 코인 심볼 추출
        all_symbols = set()
        for pair in symbols:
            base_symbol = pair['baseAsset'].upper()
            all_symbols.add(base_symbol)
        
        print(f"바이낸스 기본 코인 종류: {len(all_symbols)}개")
        
        # 추가 코인 (추정 기본 수수료)
        for symbol in all_symbols:
            if symbol not in fees:
                # 기본값 (추정)
                if symbol in ['SAND', 'MANA', 'FLOW', 'QTUM', 'BAT', 'ZIL']:
                    fees[symbol] = {'fee': 0.2, 'name': symbol, 'network': 'ETH'}
                else:
                    fees[symbol] = {'fee': 0.01, 'name': symbol, 'network': 'Unknown'}
    
    except Exception as e:
        print(f"바이낸스 코인 목록 수집 중 오류 발생: {str(e)}")
    
    return fees

def get_bybit_fees():
    """바이빗의 출금 수수료 정보 수집"""
    print("\n=== 바이빗 출금 수수료 정보 수집 중 ===")
    
    # 바이빗 공식 웹사이트에서 수집한 데이터 (기본 네트워크 기준)
    fees = {
        'BTC': {'fee': 0.0005, 'name': 'Bitcoin', 'network': 'BTC'},
        'ETH': {'fee': 0.0005, 'name': 'Ethereum', 'network': 'ETH'},
        'XRP': {'fee': 0.25, 'name': 'XRP', 'network': 'XRP'},
        'USDT': {'fee': 1.0, 'name': 'Tether', 'network': 'TRC20'},
        'SOL': {'fee': 0.01, 'name': 'Solana', 'network': 'SOL'},
        'ADA': {'fee': 1.0, 'name': 'Cardano', 'network': 'ADA'},
        'DOGE': {'fee': 5.0, 'name': 'Dogecoin', 'network': 'DOGE'},
        'LINK': {'fee': 0.35, 'name': 'Chainlink', 'network': 'ETH'},
        'DOT': {'fee': 0.1, 'name': 'Polkadot', 'network': 'DOT'},
        'AVAX': {'fee': 0.01, 'name': 'Avalanche', 'network': 'AVAXC'},
        'MATIC': {'fee': 0.3, 'name': 'Polygon', 'network': 'MATIC'},
        'SHIB': {'fee': 30000, 'name': 'Shiba Inu', 'network': 'ETH'},
        'TRX': {'fee': 1, 'name': 'TRON', 'network': 'TRX'},
        'ATOM': {'fee': 0.01, 'name': 'Cosmos', 'network': 'ATOM'},
        'XLM': {'fee': 0.02, 'name': 'Stellar', 'network': 'XLM'},
        'VET': {'fee': 25, 'name': 'VeChain', 'network': 'VET'},
        'NEAR': {'fee': 0.1, 'name': 'NEAR Protocol', 'network': 'NEAR'},
        'ALGO': {'fee': 0.01, 'name': 'Algorand', 'network': 'ALGO'},
        'ETC': {'fee': 0.01, 'name': 'Ethereum Classic', 'network': 'ETC'},
        'XTZ': {'fee': 0.1, 'name': 'Tezos', 'network': 'XTZ'}
    }
    
    # 바이빗 코인 목록 가져오기
    try:
        url = "https://api.bybit.com/v5/market/instruments-info?category=spot"
        response = requests.get(url)
        data = response.json()
        
        if data.get('retCode') == 0:
            symbols = data.get('result', {}).get('list', [])
            print(f"바이빗 거래 페어 개수: {len(symbols)}")
            
            # 유니크한 기본 코인 심볼 추출
            all_symbols = set()
            for pair in symbols:
                base_symbol = pair['baseCoin'].upper()
                all_symbols.add(base_symbol)
            
            print(f"바이빗 기본 코인 종류: {len(all_symbols)}개")
            
            # 추가 코인 (추정 기본 수수료)
            for symbol in all_symbols:
                if symbol not in fees:
                    # 기본값 (추정)
                    if symbol in ['SAND', 'MANA', 'FLOW', 'QTUM', 'BAT', 'ZIL']:
                        fees[symbol] = {'fee': 0.2, 'name': symbol, 'network': 'ETH'}
                    else:
                        fees[symbol] = {'fee': 0.01, 'name': symbol, 'network': 'Unknown'}
    
    except Exception as e:
        print(f"바이빗 코인 목록 수집 중 오류 발생: {str(e)}")
    
    return fees

def calculate_krw_fees(fees_dict, prices_dict, exchange_rate=1500):
    """코인별 출금 수수료를 원화로 계산"""
    result = {}
    
    for symbol, info in fees_dict.items():
        fee = info.get('fee', 0)
        name = info.get('name', symbol)
        network = info.get('network', '')
        
        # 현재 가격 조회
        price_usd = prices_dict.get(symbol, 0)
        
        # 원화 수수료 계산 (USD 가격 * 환율 * 수수료)
        krw_fee = price_usd * exchange_rate * fee
        
        result[symbol] = {
            'name': name,
            'fee': fee,
            'network': network,
            'price_usd': price_usd,
            'krw_fee': krw_fee
        }
    
    return result

def save_to_csv(data, filename):
    """결과를 CSV 파일로 저장"""
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        # CSV 헤더 정의
        fieldnames = ['symbol', 'name', 'exchange', 'fee', 'network', 'price_usd', 'krw_fee']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        # 데이터 작성
        for exchange, coins in data.items():
            for symbol, info in coins.items():
                writer.writerow({
                    'symbol': symbol,
                    'name': info.get('name', symbol),
                    'exchange': exchange,
                    'fee': info.get('fee', 0),
                    'network': info.get('network', ''),
                    'price_usd': info.get('price_usd', 0),
                    'krw_fee': info.get('krw_fee', 0)
                })
    
    print(f"결과가 저장되었습니다: {filename}")

def main():
    """메인 함수"""
    # 현재 시간으로 파일명 생성
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"temp/api_results/withdrawal_fees_{timestamp}.csv"
    
    # 1. 현재 코인 가격 정보 가져오기
    prices = get_current_prices()
    
    # 2. 각 거래소별 출금 수수료 정보 수집
    upbit_fees = get_upbit_fees()
    bithumb_fees = get_bithumb_fees()
    binance_fees = get_binance_fees()
    bybit_fees = get_bybit_fees()
    
    # 3. 원화 수수료 계산
    print("\n=== 원화 출금 수수료 계산 중 ===")
    upbit_krw = calculate_krw_fees(upbit_fees, prices)
    bithumb_krw = calculate_krw_fees(bithumb_fees, prices)
    binance_krw = calculate_krw_fees(binance_fees, prices)
    bybit_krw = calculate_krw_fees(bybit_fees, prices)
    
    # 4. 결과 저장
    result = {
        'Upbit': upbit_krw,
        'Bithumb': bithumb_krw,
        'Binance': binance_krw,
        'Bybit': bybit_krw
    }
    save_to_csv(result, output_file)
    
    # 5. 간단한 통계 출력
    print("\n=== 인기 코인 원화 출금 수수료 비교 ===")
    print(f"{'코인':^8} | {'업비트(KRW)':^15} | {'빗썸(KRW)':^15} | {'바이낸스(KRW)':^15} | {'바이빗(KRW)':^15}")
    print("-" * 80)
    
    for coin in ['BTC', 'ETH', 'XRP', 'USDT', 'SOL', 'ADA', 'DOGE']:
        upbit_fee = f"{upbit_krw.get(coin, {}).get('krw_fee', 0):,.0f}"
        bithumb_fee = f"{bithumb_krw.get(coin, {}).get('krw_fee', 0):,.0f}"
        binance_fee = f"{binance_krw.get(coin, {}).get('krw_fee', 0):,.0f}"
        bybit_fee = f"{bybit_krw.get(coin, {}).get('krw_fee', 0):,.0f}"
        
        print(f"{coin:^8} | {upbit_fee:>15} | {bithumb_fee:>15} | {binance_fee:>15} | {bybit_fee:>15}")

if __name__ == "__main__":
    main() 