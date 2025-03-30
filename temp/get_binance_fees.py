import os
import json
import time
from binance.client import Client

# .env 파일에서 API 키 정보 가져오기
BINANCE_API_KEY = "d17CQG6oE87sygpOIAcsG3uHDwQQvZLXqlpBRkoTRBhCY6ODW4UNsH1JBfMoOYFm"
BINANCE_API_SECRET = "2gh0LtrdCFzmyBYNpISy8lVjBa4Wkr72DKmyAkYwPZwjipXKXdXntLTWAYYpQC2p"

def get_binance_withdrawal_fees():
    """바이낸스에서 모든 코인의 출금 수수료를 조회"""
    print("바이낸스 API 연결 시도...")
    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    
    try:
        # 출금 수수료 정보 조회
        print("바이낸스 출금 수수료 정보 조회 중...")
        coins_info = client.get_all_coins_info()
        withdrawal_fees = {}
        
        for coin in coins_info:
            symbol = coin['coin']
            name = coin.get('name', symbol)
            fee = coin.get('networkList', [])
            
            networks = {}
            for network in fee:
                network_name = network.get('network', 'Unknown')
                withdrawal_fee = network.get('withdrawFee', 'Unknown')
                withdrawal_enabled = network.get('withdrawEnable', False)
                min_withdraw = network.get('withdrawMin', 'Unknown')
                
                networks[network_name] = {
                    'fee': withdrawal_fee,
                    'enabled': withdrawal_enabled,
                    'min_withdraw': min_withdraw
                }
            
            withdrawal_fees[symbol] = {
                'name': name,
                'networks': networks
            }
        
        # 결과를 JSON 파일로 저장
        with open('temp/api_results/binance_withdrawal_fees.json', 'w', encoding='utf-8') as f:
            json.dump(withdrawal_fees, f, ensure_ascii=False, indent=2)
        
        print(f"바이낸스 출금 수수료 정보 저장 완료: temp/api_results/binance_withdrawal_fees.json")
        
        # 일부 주요 코인 출력
        print("\n=== 바이낸스 주요 코인 출금 수수료 ===")
        for symbol in ['BTC', 'ETH', 'XRP', 'BNB', 'USDT', 'SOL', 'ADA', 'DOGE']:
            if symbol in withdrawal_fees:
                print(f"\n{symbol} ({withdrawal_fees[symbol]['name']}):")
                for network, info in withdrawal_fees[symbol]['networks'].items():
                    print(f"  네트워크: {network}, 수수료: {info['fee']}, 최소출금: {info['min_withdraw']}, 출금가능: {'Yes' if info['enabled'] else 'No'}")
    
    except Exception as e:
        print(f"바이낸스 API 조회 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    get_binance_withdrawal_fees() 