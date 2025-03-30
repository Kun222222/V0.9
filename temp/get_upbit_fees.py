import os
import json
import time
import jwt
import uuid
import hashlib
import requests
from urllib.parse import urlencode

# .env 파일에서 API 키 정보 가져오기
UPBIT_API_KEY = "AEypu0SecUIboXsuxwIhbiWXu6P3KeGUsmzJ7Byq"
UPBIT_API_SECRET = "Eq6vPS9Rj4PxnnxKxqUZRmGjRNywxdjLv9ZEvLFn"

def get_upbit_withdrawal_fees():
    """업비트에서 모든 코인의 출금 수수료를 조회"""
    print("업비트 API 연결 시도...")
    
    try:
        # 통화 목록 조회
        url = "https://api.upbit.com/v1/market/all"
        response = requests.get(url)
        markets = response.json()
        
        # KRW 마켓만 필터링
        krw_markets = [market['market'].split('-')[1] for market in markets if market['market'].startswith('KRW-')]
        print(f"업비트 KRW 마켓 코인 개수: {len(krw_markets)}")
        
        # 출금 수수료 정보 조회
        print("업비트 출금 수수료 정보 조회 중...")
        
        # 일부 주요 코인만 조회 (시간 절약을 위해)
        important_coins = ['BTC', 'ETH', 'XRP', 'SOL', 'ADA', 'DOGE', 'USDT']
        coins_to_check = important_coins + krw_markets[:10]  # 주요 코인 + 첫 10개만 확인
        coins_to_check = list(set(coins_to_check))  # 중복 제거
        
        withdrawal_fees = {}
        
        # 각 코인별로 출금 수수료 조회
        for symbol in coins_to_check:
            try:
                # 출금 가능 조회 API 호출
                query = {
                    'currency': symbol,
                }
                query_string = urlencode(query)

                # JWT 인증 토큰 생성 (각 요청마다 새로운 nonce 필요)
                payload = {
                    'access_key': UPBIT_API_KEY,
                    'nonce': str(uuid.uuid4()),
                    'query_hash': hashlib.sha512(query_string.encode()).hexdigest(),
                    'query_hash_alg': 'SHA512',
                }
                
                # PyJWT 1.7.0 이상: jwt.encode()는 문자열 반환
                # PyJWT 2.0.0 이상: jwt.encode()는 바이트가 아닌 문자열 반환
                jwt_token = jwt.encode(payload, UPBIT_API_SECRET, algorithm="HS256")
                # 바이트인 경우 문자열로 변환
                if isinstance(jwt_token, bytes):
                    jwt_token = jwt_token.decode('utf-8')
                    
                headers = {
                    'Authorization': f'Bearer {jwt_token}',
                }
                
                url = f"https://api.upbit.com/v1/withdraws/chance?{query_string}"
                response = requests.get(url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    currency = data.get('currency', {})
                    symbol_name = currency.get('code', symbol)
                    
                    withdrawal_fee = data.get('withdraw_fee', 'Unknown')
                    min_amount = data.get('minimum', 'Unknown')
                    
                    withdrawal_fees[symbol] = {
                        'name': currency.get('name', symbol),
                        'fee': withdrawal_fee,
                        'min_amount': min_amount
                    }
                    
                    print(f"조회 완료: {symbol} - 수수료: {withdrawal_fee}")
                else:
                    print(f"조회 실패: {symbol} - 상태 코드: {response.status_code}, 메시지: {response.text}")
                
                # API 속도 제한 방지
                time.sleep(0.1)
                
            except Exception as e:
                print(f"코인 {symbol} 조회 중 오류: {str(e)}")
        
        # 결과를 JSON 파일로 저장
        with open('temp/api_results/upbit_withdrawal_fees.json', 'w', encoding='utf-8') as f:
            json.dump(withdrawal_fees, f, ensure_ascii=False, indent=2)
        
        print(f"업비트 출금 수수료 정보 저장 완료: temp/api_results/upbit_withdrawal_fees.json")
        
        # 일부 주요 코인 출력
        print("\n=== 업비트 주요 코인 출금 수수료 ===")
        for symbol in ['BTC', 'ETH', 'XRP', 'SOL', 'ADA', 'DOGE', 'USDT']:
            if symbol in withdrawal_fees:
                print(f"{symbol} ({withdrawal_fees[symbol]['name']}): 수수료 {withdrawal_fees[symbol]['fee']}, 최소출금: {withdrawal_fees[symbol]['min_amount']}")
    
    except Exception as e:
        print(f"업비트 API 조회 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    get_upbit_withdrawal_fees() 