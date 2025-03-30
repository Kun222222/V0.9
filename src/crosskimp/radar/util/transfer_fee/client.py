"""
전송 수수료 조회 통합 클라이언트 모듈

여러 거래소의 전송(출금) 수수료 정보를 통합하여 조회하는 클라이언트를 제공합니다.
"""

import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from crosskimp.common.config.app_config import get_config
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.common.config.env_loader import get_env_loader

from .upbit import UpbitTransferFeeClient
from .bithumb import BithumbTransferFeeClient
from .binance import BinanceTransferFeeClient

logger = logging.getLogger(SystemComponent.RADAR.value)

class TransferFeeClient:
    """전송 수수료 조회 통합 클라이언트"""
    
    def __init__(self):
        """초기화"""
        self.config = get_config()
        self.env_loader = get_env_loader()
        
        # 거래소별 클라이언트 초기화
        self.upbit_client = None
        self.bithumb_client = None
        self.binance_client = None
        
        # API 키가 설정된 거래소만 초기화
        self._initialize_exchange_clients()
    
    def _initialize_exchange_clients(self):
        """거래소별 클라이언트 초기화"""
        # 업비트
        if self.env_loader.is_api_key_set('upbit'):
            logger.info("업비트 전송 수수료 클라이언트 초기화")
            self.upbit_client = UpbitTransferFeeClient()
            
        # 빗썸
        if self.env_loader.is_api_key_set('bithumb'):
            logger.info("빗썸 전송 수수료 클라이언트 초기화")
            self.bithumb_client = BithumbTransferFeeClient()
            
        # 바이낸스
        if self.env_loader.is_api_key_set('binance'):
            logger.info("바이낸스 전송 수수료 클라이언트 초기화")
            self.binance_client = BinanceTransferFeeClient()
    
    def get_available_exchanges(self) -> List[str]:
        """
        API 키가 설정된 사용 가능한 거래소 목록 반환
        
        Returns:
            List[str]: 사용 가능한 거래소 코드 목록
        """
        available = []
        
        if self.upbit_client:
            available.append('upbit')
            
        if self.bithumb_client:
            available.append('bithumb')
            
        if self.binance_client:
            available.append('binance')
            
        return available
    
    def get_transfer_fees(self, exchange: str, currencies: List[str]) -> Dict[str, Any]:
        """
        특정 거래소의 전송 수수료 조회
        
        Args:
            exchange: 거래소 코드 (upbit, bithumb, binance)
            currencies: 화폐 코드 목록
            
        Returns:
            Dict: 전송 수수료 정보
        """
        exchange = exchange.lower()
        
        if exchange == 'upbit':
            if not self.upbit_client:
                return {"error": "업비트 API 키가 설정되지 않았습니다."}
            return self.upbit_client.get_withdraw_fees(currencies)
            
        elif exchange == 'bithumb':
            if not self.bithumb_client:
                return {"error": "빗썸 API 키가 설정되지 않았습니다."}
            return self.bithumb_client.get_withdrawal_fees(currencies)
            
        elif exchange == 'binance':
            if not self.binance_client:
                return {"error": "바이낸스 API 키가 설정되지 않았습니다."}
            return self.binance_client.get_withdrawal_fees(currencies)
            
        else:
            return {"error": f"지원하지 않는 거래소입니다: {exchange}"}
    
    def get_all_transfer_fees(self, currencies: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        모든 거래소의 전송 수수료 조회
        
        Args:
            currencies: 화폐 코드 목록
            
        Returns:
            Dict: 거래소별 전송 수수료 정보
        """
        result = {}
        available_exchanges = self.get_available_exchanges()
        
        for exchange in available_exchanges:
            try:
                logger.info(f"{exchange} 전송 수수료 조회 시작")
                fees = self.get_transfer_fees(exchange, currencies)
                result[exchange] = fees
                logger.info(f"{exchange} 전송 수수료 조회 완료")
            except Exception as e:
                logger.error(f"{exchange} 전송 수수료 조회 실패: {str(e)}")
                result[exchange] = {"error": str(e)}
        
        return result
    
    def save_transfer_fees(self, currencies: List[str], output_file: str) -> bool:
        """
        모든 거래소의 전송 수수료를 조회하여 파일로 저장
        
        Args:
            currencies: 화폐 코드 목록
            output_file: 출력 파일 경로
            
        Returns:
            bool: 저장 성공 여부
        """
        try:
            # 모든 거래소의 전송 수수료 조회
            fees_data = self.get_all_transfer_fees(currencies)
            
            # 조회 시간 추가
            fees_data["timestamp"] = datetime.now().isoformat()
            fees_data["currencies"] = currencies
            
            # JSON 파일로 저장
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(fees_data, f, indent=2, ensure_ascii=False)
                
            logger.info(f"전송 수수료 데이터 저장 완료: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"전송 수수료 데이터 저장 실패: {str(e)}")
            return False 