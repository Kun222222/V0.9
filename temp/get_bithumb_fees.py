import os
import json
import time
import base64
import hmac
import hashlib
import requests
from urllib.parse import urlencode

# .env 파일에서 API 키 정보 가져오기
BITHUMB_API_KEY = "f6445807c1f4a16e76ace6a4dcf1a7cc4ce90f092a23a6"
BITHUMB_API_SECRET = "ZTQ5YmNkNWNjMGQ0ZTQzZDU0MGJjY2Y0YTM2MDBlZGNlMGE3ZGJmMWE1Y2I0OGUyMDIwZjJiMjRhNmVhNA=="

def get_bithumb_withdrawal_fees():
    """빗썸에서 모든 코인의 출금 수수료를 조회"""
    print("빗썸 API 연결 시도...")
    
    try:
        # 통화 목록 조회
        url = "https://api.bithumb.com/public/ticker/ALL_KRW"
        response = requests.get(url)
        data = response.json()
        
        if data.get('status') != '0000':
            print(f"빗썸 코인 목록 조회 실패: {data.get('message')}")
            return
        
        # 모든 코인 목록 추출 (date 제외)
        all_coins = [coin for coin in data.get('data', {}).keys() if coin != 'date']
        print(f"빗썸 코인 개수: {len(all_coins)}")
        
        # 출금 수수료 정보 저장 딕셔너리
        withdrawal_fees = {}
        
        # 출금 수수료 및 정보 조회
        print("빗썸 출금 수수료 정보 조회 중...")
        
        # 각 코인별로 입출금 정보 조회
        for symbol in all_coins:
            try:
                # 1. 개별 코인 정보 조회 (공개 API)
                coin_info_url = f"https://api.bithumb.com/public/ticker/{symbol}_KRW"
                info_response = requests.get(coin_info_url)
                info_data = info_response.json()
                
                if info_data.get('status') != '0000':
                    print(f"코인 {symbol} 정보 조회 실패: {info_data.get('message')}")
                    continue
                
                # 2. 입출금 상태 조회 (공개 API)
                endpoint = "/info/wallet_status"
                url = "https://api.bithumb.com" + endpoint
                
                # 필요한 파라미터
                nonce = str(int(time.time() * 1000))
                params = {
                    'currency': symbol
                }
                
                # POST 요청 준비
                encoded_params = urlencode(params)
                
                # 서명 생성
                secret_key = BITHUMB_API_SECRET
                if secret_key.endswith('='):
                    secret_key = base64.b64decode(secret_key)
                else:
                    secret_key = base64.b64decode(secret_key + '==')
                
                message = endpoint + chr(0) + encoded_params + chr(0) + nonce
                signature = hmac.new(secret_key, message.encode('utf-8'), hashlib.sha512).hexdigest()
                
                # API 요청 헤더
                headers = {
                    'Api-Key': BITHUMB_API_KEY,
                    'Api-Sign': signature,
                    'Api-Nonce': nonce,
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
                
                # API 요청 실행
                response = requests.post(url, headers=headers, data=encoded_params)
                
                if response.status_code == 200:
                    wallet_data = response.json()
                    if wallet_data.get('status') == '0000':
                        status_data = wallet_data.get('data', {})
                        withdrawal_fee = status_data.get('withdraw_fee', 'Unknown')
                        withdrawal_status = status_data.get('withdraw_status', 0) == 1
                        
                        withdrawal_fees[symbol] = {
                            'name': symbol,
                            'fee': withdrawal_fee,
                            'withdrawal_enabled': withdrawal_status
                        }
                        
                        print(f"조회 완료: {symbol} - 수수료: {withdrawal_fee}")
                    else:
                        print(f"지갑 상태 조회 실패: {symbol} - {wallet_data.get('message')}")
                else:
                    print(f"API 요청 실패: {symbol} - 상태 코드: {response.status_code}, 메시지: {response.text}")
                
                # API 속도 제한 방지
                time.sleep(0.1)
                
            except Exception as e:
                print(f"코인 {symbol} 조회 중 오류: {str(e)}")
        
        # 결과를 JSON 파일로 저장
        with open('temp/api_results/bithumb_withdrawal_fees.json', 'w', encoding='utf-8') as f:
            json.dump(withdrawal_fees, f, ensure_ascii=False, indent=2)
        
        print(f"빗썸 출금 수수료 정보 저장 완료: temp/api_results/bithumb_withdrawal_fees.json")
        
        # 일부 주요 코인 출력
        print("\n=== 빗썸 주요 코인 출금 수수료 ===")
        for symbol in ['BTC', 'ETH', 'XRP', 'SOL', 'ADA', 'DOGE', 'USDT']:
            if symbol in withdrawal_fees:
                print(f"{symbol} ({withdrawal_fees[symbol]['name']}): 수수료 {withdrawal_fees[symbol]['fee']}, 출금가능: {'Yes' if withdrawal_fees[symbol].get('withdrawal_enabled', False) else 'No'}")
    
    except Exception as e:
        print(f"빗썸 API 조회 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    get_bithumb_withdrawal_fees() 