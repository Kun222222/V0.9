"""
바이낸스 전송 수수료 조회 모듈

바이낸스 API를 통해 암호화폐 전송(출금) 수수료 정보를 조회합니다.
"""

import json
import hmac
import hashlib
import time
import requests
from typing import Dict, List, Any, Optional
from urllib.parse import urlencode
import logging
from datetime import datetime

from crosskimp.common.config.app_config import get_config
from crosskimp.common.config.common_constants import SystemComponent

logger = logging.getLogger(SystemComponent.RADAR.value)

class BinanceTransferFeeClient:
    """바이낸스 전송 수수료 클라이언트"""
    
    BASE_URL = "https://api.binance.com"
    
    def __init__(self):
        """초기화"""
        self.config = get_config()
        self.api_key = self.config.get_env("binance.api_key")
        self.api_secret = self.config.get_env("binance.api_secret")
        
        if not self.api_key or not self.api_secret:
            logger.warning("바이낸스 API 키가 설정되지 않았습니다. 수수료 조회가 제한될 수 있습니다.")
    
    def _generate_signature(self, params: Dict) -> str:
        """
        API 요청용 서명 생성
        
        Args:
            params: 요청 파라미터
            
        Returns:
            str: HMAC 서명
        """
        query_string = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return signature
    
    def _get_headers(self) -> Dict:
        """
        API 요청용 헤더 생성
        
        Returns:
            Dict: 헤더 정보
        """
        return {
            'X-MBX-APIKEY': self.api_key
        }
    
    def get_all_coins_info(self) -> Dict:
        """
        모든 코인 정보 조회 (출금 수수료 포함)
        
        Returns:
            Dict: 모든 코인 정보
        """
        try:
            if not self.api_key or not self.api_secret:
                logger.error("바이낸스 API 키가 필요합니다.")
                return {"error": "API 키가 설정되지 않았습니다."}
            
            # API 요청 파라미터
            params = {
                'timestamp': int(time.time() * 1000)
            }
            
            # 서명 생성 및 파라미터에 추가
            params['signature'] = self._generate_signature(params)
            
            # API 요청
            url = f"{self.BASE_URL}/sapi/v1/capital/config/getall"
            response = requests.get(url, params=params, headers=self._get_headers())
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"바이낸스 모든 코인 정보 조회 성공: {len(data)} 코인")
            
            return data
            
        except Exception as e:
            logger.error(f"바이낸스 모든 코인 정보 조회 실패: {str(e)}")
            return {"error": str(e)}
    
    def get_withdrawal_fees(self, currencies: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        특정 화폐 목록 또는 모든 화폐의 출금 수수료 조회
        
        Args:
            currencies: 화폐 코드 목록 (None인 경우 모든 화폐)
            
        Returns:
            Dict: 화폐별 출금 수수료 정보
        """
        try:
            # 모든 코인 정보 조회
            coins_info = self.get_all_coins_info()
            
            if isinstance(coins_info, dict) and "error" in coins_info:
                return coins_info
            
            result = {}
            
            for coin in coins_info:
                coin_name = coin.get('coin')
                
                # 특정 화폐만 요청한 경우 필터링
                if currencies and coin_name not in currencies:
                    continue
                
                # 출금 가능 여부 확인
                if not coin.get('isWithdrawEnabled', False):
                    logger.debug(f"바이낸스 {coin_name} 출금 비활성화됨")
                    continue
                
                # 네트워크별 출금 수수료 정보
                networks = []
                for network in coin.get('networkList', []):
                    if network.get('isWithdrawEnabled', False):
                        networks.append({
                            'network': network.get('network'),
                            'name': network.get('name', ''),
                            'fee': network.get('withdrawFee', '0'),
                            'min': network.get('withdrawMin', '0'),
                            'default': network.get('isDefault', False)
                        })
                
                # 결과 저장
                result[coin_name] = {
                    'fee': networks[0].get('fee') if networks else None,  # 기본 네트워크 수수료
                    'networks': networks,
                    'timestamp': datetime.now().isoformat(),
                    'unit': coin_name
                }
            
            return result
            
        except Exception as e:
            logger.error(f"바이낸스 출금 수수료 조회 실패: {str(e)}")
            return {"error": str(e)} 