"""
빗썸 전송 수수료 조회 모듈

빗썸 API를 통해 암호화폐 전송(출금) 수수료 정보를 조회합니다.
"""

import base64
import hashlib
import hmac
import time
import json
import requests
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime
import urllib.parse

from crosskimp.common.config.app_config import get_config
from crosskimp.common.config.common_constants import SystemComponent

logger = logging.getLogger(SystemComponent.RADAR.value)

class BithumbTransferFeeClient:
    """빗썸 전송 수수료 클라이언트"""
    
    BASE_URL = "https://api.bithumb.com"
    
    def __init__(self):
        """초기화"""
        self.config = get_config()
        self.connect_key = self.config.get_env("bithumb.api_key")
        self.secret_key = self.config.get_env("bithumb.api_secret")
        
        if not self.connect_key or not self.secret_key:
            logger.warning("빗썸 API 키가 설정되지 않았습니다. 수수료 조회가 제한될 수 있습니다.")
    
    def _create_signature(self, endpoint: str, params: Dict) -> Dict:
        """
        API 요청용 서명 생성
        
        Args:
            endpoint: API 엔드포인트
            params: 요청 파라미터
            
        Returns:
            Dict: 헤더 정보
        """
        if not self.connect_key or not self.secret_key:
            return {}
            
        # 요청 데이터 정렬 및 문자열 변환
        e_string = urllib.parse.urlencode(params)
        
        # nonce 값 생성 (현재 타임스탬프)
        nonce = str(int(time.time() * 1000))
        
        # 서명 생성 문자열
        message = endpoint + ";" + e_string + ";" + nonce
        
        # 서명 생성
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha512
        ).hexdigest()
        
        # API 요청 헤더
        headers = {
            'Api-Key': self.connect_key,
            'Api-Sign': signature,
            'Api-Nonce': nonce,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        return headers
    
    def get_withdrawal_fee(self, currency: str) -> Dict:
        """
        특정 화폐의 출금 수수료 조회
        
        Args:
            currency: 화폐 코드 (예: BTC)
            
        Returns:
            Dict: 출금 수수료 정보
        """
        try:
            # 공개 API로 화폐 정보 조회 먼저 시도
            public_url = f"{self.BASE_URL}/public/ticker/{currency}"
            response = requests.get(public_url)
            response.raise_for_status()
            
            # 회원 API로 출금 수수료 조회
            endpoint = "/info/account"
            url = f"{self.BASE_URL}{endpoint}"
            
            params = {
                'currency': currency
            }
            
            # 회원 API 키가 있는 경우만 회원 API 호출
            if self.connect_key and self.secret_key:
                headers = self._create_signature(endpoint, params)
                response = requests.post(url, headers=headers, data=params)
                response.raise_for_status()
                
                data = response.json()
                
                if data.get('status') == '0000':
                    # 출금 수수료 정보는 data.available_withdraw 필드에 있음
                    # 형태가 API마다 다를 수 있어 로깅
                    logger.debug(f"빗썸 응답 데이터: {json.dumps(data, indent=2, ensure_ascii=False)}")
                    
                    return {
                        'currency': currency,
                        'fee': data.get('data', {}).get('withdraw_fee', '0'),
                        'timestamp': datetime.now().isoformat()
                    }
                else:
                    logger.warning(f"빗썸 API 응답 오류: {data.get('message')}")
                    return {'error': data.get('message')}
            else:
                # 회원 API 키가 없는 경우 공개 API만으로도 일부 정보 제공
                return {
                    'currency': currency,
                    'fee': None,  # 수수료 정보는 회원 API 필요
                    'note': '회원 API 키가 필요합니다.',
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"빗썸 출금 수수료 조회 실패: {str(e)}")
            return {"error": str(e)}
    
    def get_withdrawal_fees(self, currencies: List[str]) -> Dict[str, Any]:
        """
        여러 화폐의 출금 수수료 조회
        
        Args:
            currencies: 화폐 코드 목록 (예: ["BTC", "ETH"])
            
        Returns:
            Dict: 화폐별 출금 수수료 정보
        """
        result = {}
        
        for currency in currencies:
            try:
                data = self.get_withdrawal_fee(currency)
                
                if 'error' not in data:
                    result[currency] = {
                        'fee': data.get('fee'),
                        'timestamp': data.get('timestamp'),
                        'unit': currency
                    }
                
                # API 호출 제한 방지를 위한 대기
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"빗썸 {currency} 출금 수수료 조회 실패: {str(e)}")
        
        return result 