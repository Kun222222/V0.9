import asyncio
import logging
from src.crosskimp.ob_collector.orderbook.websocket.binance_f_ws import BinanceFutureWebsocket

# 로깅 설정
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_binance_f_ws():
    logger.info("바이낸스 퓨처 웹소켓 테스트 시작")
    
    # 웹소켓 인스턴스 생성
    ws = BinanceFutureWebsocket({'api_key': '', 'api_secret': ''})
    
    try:
        # 웹소켓 시작
        task = asyncio.create_task(ws.start({'binancefuture': ['BTCUSDT']}))
        
        # 10초 동안 실행
        await asyncio.sleep(10)
        
        # 웹소켓 중지
        await ws.stop()
        logger.info("웹소켓 중지 완료")
        
    except Exception as e:
        logger.error(f"테스트 중 오류 발생: {e}", exc_info=True)
    
    logger.info("테스트 종료")

if __name__ == "__main__":
    asyncio.run(test_binance_f_ws()) 