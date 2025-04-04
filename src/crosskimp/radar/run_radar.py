#!/usr/bin/env python3
"""
레이더 모듈 실행 스크립트

이미 실행 중인 오더북 콜렉터에 연결하여 레이더 모듈만 독립적으로 실행합니다.
"""

import asyncio
import time
import argparse
import signal
from typing import Dict, Any

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent
from crosskimp.radar import get_radar_processor


async def main():
    """메인 함수"""
    # 로거 설정
    logger = get_unified_logger(SystemComponent.RADAR.value)
    logger.info("레이더 모듈 실행 중...")
    
    # 레이더 프로세서 인스턴스 가져오기
    radar = get_radar_processor()
    
    # 종료 이벤트 설정
    shutdown_event = asyncio.Event()
    
    # 시그널 핸들러 설정
    def signal_handler():
        logger.info("종료 신호 수신...")
        shutdown_event.set()
    
    # SIGINT와 SIGTERM에 대한 핸들러 등록
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(sig, signal_handler)
    
    # 샘플 데이터 출력 콜백
    def print_sample_data(result: Dict[str, Any]):
        logger.info(f"계산 결과: 거래소={result.get('exchange')}, "
                   f"{result.get('symbols_count')}개 심볼, "
                   f"{result.get('data_count')}건 데이터")
    
    # 콜백 등록
    radar.add_result_callback(print_sample_data)
    
    # 레이더 프로세서 시작
    await radar.start()
    logger.info("레이더 프로세서 시작 완료")
    
    # 주기적으로 캐시된 데이터 통계 출력
    async def report_statistics():
        while not shutdown_event.is_set():
            try:
                # 캐시된 오더북 통계 출력
                all_data = radar.get_all_orderbooks()
                if all_data:
                    logger.info(f"캐싱된 거래소: {len(all_data)}개")
                    
                    total_symbols = 0
                    for exchange, symbols in all_data.items():
                        symbol_count = len(symbols)
                        total_symbols += symbol_count
                        logger.info(f"  거래소 {exchange}: {symbol_count}개 심볼")
                        
                        # 각 거래소별 첫 번째 심볼 샘플 출력
                        if symbol_count > 0:
                            first_symbol = next(iter(symbols.keys()))
                            ob_data = radar.get_orderbook_data(exchange, first_symbol)
                            if ob_data:
                                # 오더북 데이터 깊이 정보
                                bids = ob_data.get('bids', [])
                                asks = ob_data.get('asks', [])
                                bid_depth = len(bids) if isinstance(bids, list) else 0
                                ask_depth = len(asks) if isinstance(asks, list) else 0
                                
                                logger.info(f"    샘플 {exchange}/{first_symbol}: "
                                           f"매수호가 {bid_depth}단계, 매도호가 {ask_depth}단계")
                    
                    logger.info(f"총 {total_symbols}개 심볼 캐싱됨")
                else:
                    logger.warning("아직 캐싱된 오더북 데이터가 없습니다.")
                    
                # 30초 대기
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"통계 보고 중 오류: {str(e)}")
                await asyncio.sleep(60)  # 오류 발생 시 1분 대기
    
    # 통계 보고 태스크 시작
    stats_task = asyncio.create_task(report_statistics())
    
    try:
        # 종료 신호 대기
        await shutdown_event.wait()
    finally:
        # 모든 태스크 종료
        stats_task.cancel()
        try:
            await stats_task
        except asyncio.CancelledError:
            pass
        
        # 레이더 프로세서 종료
        logger.info("레이더 프로세서 종료 중...")
        await radar.stop()
        logger.info("레이더 모듈 종료 완료")


if __name__ == "__main__":
    # 명령행 인수 처리
    parser = argparse.ArgumentParser(description="레이더 모듈 실행")
    parser.add_argument("--verbose", "-v", action="store_true", help="상세 로그 출력")
    args = parser.parse_args()
    
    # 실행
    asyncio.run(main())