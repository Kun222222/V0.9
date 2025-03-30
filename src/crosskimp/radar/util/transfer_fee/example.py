"""
전송 수수료 조회 사용 예제

이 예제는 전송 수수료 조회 기능을 어떻게 사용하는지 보여줍니다.
"""

import os
import json
import logging
from datetime import datetime

from crosskimp.radar.util.transfer_fee import TransferFeeClient
from crosskimp.common.config.app_config import get_config
from crosskimp.common.logger.logger import initialize_logging

def main():
    """메인 실행 함수"""
    # 로거 설정
    initialize_logging()
    logger = logging.getLogger("transfer_fee_example")
    
    # 설정 로드
    config = get_config()
    
    # 클라이언트 초기화
    client = TransferFeeClient()
    
    # 사용 가능한 거래소 확인
    available_exchanges = client.get_available_exchanges()
    logger.info(f"사용 가능한 거래소: {available_exchanges}")
    
    # 조회할 화폐 목록
    currencies = ["BTC", "ETH", "XRP", "SOL", "ADA"]
    
    # 모든 거래소의 전송 수수료 조회
    all_fees = client.get_all_transfer_fees(currencies)
    
    # 결과 출력
    logger.info("전송 수수료 조회 결과:")
    for exchange, fees in all_fees.items():
        if "error" in fees:
            logger.warning(f"  {exchange}: 오류 - {fees['error']}")
            continue
        
        logger.info(f"  {exchange}:")
        for currency, fee_info in fees.items():
            if isinstance(fee_info, dict):
                fee = fee_info.get('fee')
                logger.info(f"    {currency}: {fee}")

    # 결과 저장
    output_dir = config.get_path("temp_dir")
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"transfer_fees_{timestamp}.json")
    
    if client.save_transfer_fees(currencies, output_file):
        logger.info(f"전송 수수료 데이터 저장 완료: {output_file}")
    else:
        logger.error("전송 수수료 데이터 저장 실패")
    
    # 하나의 거래소만 조회 예제
    if "binance" in available_exchanges:
        logger.info("\n바이낸스 전송 수수료만 조회:")
        binance_fees = client.get_transfer_fees("binance", currencies)
        
        for currency, fee_info in binance_fees.items():
            if isinstance(fee_info, dict):
                fee = fee_info.get('fee')
                networks = fee_info.get('networks', [])
                
                logger.info(f"  {currency}: {fee}")
                for network in networks:
                    logger.info(f"    네트워크: {network['name']}, 수수료: {network['fee']}")

if __name__ == "__main__":
    main() 