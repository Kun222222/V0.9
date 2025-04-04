"""
업비트 전송 수수료 조회 모듈

업비트 API를 통해 암호화폐 전송(출금) 수수료 정보를 조회합니다.
"""

import json
import uuid
import jwt
import requests
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime
import time

from crosskimp.common.config.app_config import get_config
from crosskimp.common.config.common_constants import SystemComponent

logger = logging.getLogger(SystemComponent.RADAR.value)

class UpbitTransferFeeClient:
    """업비트 전송 수수료 클라이언트"""
    
    BASE_URL = "https://api.upbit.com"
    
    def __init__(self):
        """초기화"""
        self.config = get_config()
        self.api_key = self.config.get_env("upbit.api_key")
        self.api_secret = self.config.get_env("upbit.api_secret")
        
        if not self.api_key or not self.api_secret:
            logger.warning("업비트 API 키가 설정되지 않았습니다. 수수료 조회가 제한될 수 있습니다.")
    
    def _get_headers(self, params: Optional[Dict] = None) -> Dict:
        """
        API 요청용 헤더 생성
        
        Args:
            params: 쿼리 파라미터
            
        Returns:
            Dict: 헤더 정보
        """
        if not self.api_key or not self.api_secret:
            return {'Accept': 'application/json'}
            
        payload = {
            'access_key': self.api_key,
            'nonce': str(uuid.uuid4())
        }
        
        if params:
            query_string = '&'.join([f"{key}={value}" for key, value in params.items()])
            payload['query'] = query_string
            
        jwt_token = jwt.encode(payload, self.api_secret, algorithm='HS256')
        
        return {
            'Accept': 'application/json',
            'Authorization': f'Bearer {jwt_token}'
        }
    
    def get_withdraw_chance(self, currency: str) -> Dict:
        """
        특정 화폐의 출금 정보 조회
        
        Args:
            currency: 화폐 코드 (예: BTC)
            
        Returns:
            Dict: 출금 정보
        """
        try:
            params = {
                'currency': currency
            }
            
            headers = self._get_headers(params)
            
            url = f"{self.BASE_URL}/v1/withdraws/chance"
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            
            # 출금 수수료 추출
            withdraw_fee = data.get('currency', {}).get('withdraw_fee', None)
            
            if withdraw_fee is not None:
                logger.debug(f"업비트 {currency} 출금 수수료: {withdraw_fee}")
            else:
                logger.warning(f"업비트 {currency} 출금 수수료 정보를 찾을 수 없습니다.")
            
            return data
            
        except Exception as e:
            logger.error(f"업비트 출금 정보 조회 실패: {str(e)}")
            return {"error": str(e)}
            
    def get_withdraw_fees(self, currencies: List[str]) -> Dict[str, Any]:
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
                data = self.get_withdraw_chance(currency)
                fee = data.get('currency', {}).get('withdraw_fee', None)
                
                if fee is not None:
                    result[currency] = {
                        'fee': fee,
                        'timestamp': datetime.now().isoformat(),
                        'unit': currency
                    }
                
                # API 호출 제한 방지를 위한 대기
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"업비트 {currency} 출금 수수료 조회 실패: {str(e)}")
        
        return result 