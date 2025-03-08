import asyncio
import json
import time
from datetime import datetime
import websockets
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/binance_f_ws_test_{datetime.now().strftime("%y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BinanceFuturesWebSocketTest:
    def __init__(self):
        self.ws_url = "wss://fstream.binance.com/stream"
        self.symbols = [
            'ADA', 'AUCTION', 'BCH', 'DOGE', 'ENS', 'ETC', 'HBAR', 'JUP', 
            'KAITO', 'LAYER', 'LINK', 'NEAR', 'SEI', 'SHELL', 'SOL', 'SUI', 
            'TRUMP', 'WLD', 'XLM', 'XRP'
        ]
        self.message_count = 0
        self.start_time = None
        self.last_message_time = None
        self.max_delay = 0
        self.delays = []
        self.symbol_updates = {symbol: 0 for symbol in self.symbols}

    async def handle_message(self, message):
        current_time = time.time()
        
        if self.last_message_time:
            delay = current_time - self.last_message_time
            self.delays.append(delay)
            self.max_delay = max(self.max_delay, delay)
        
        self.last_message_time = current_time
        self.message_count += 1

        try:
            data = json.loads(message)
            if 'stream' in data and 'data' in data:
                stream = data['stream']
                symbol = stream.split('@')[0].upper().replace('USDT', '')
                if symbol in self.symbol_updates:
                    self.symbol_updates[symbol] += 1
        except Exception as e:
            logger.error(f"메시지 처리 중 오류 발생: {e}")

    def print_statistics(self):
        if not self.start_time or not self.delays:
            return

        duration = time.time() - self.start_time
        avg_delay = sum(self.delays) / len(self.delays)
        
        logger.info("\n=== 테스트 통계 ===")
        logger.info(f"총 실행 시간: {duration:.2f}초")
        logger.info(f"총 메시지 수: {self.message_count}")
        logger.info(f"초당 평균 메시지: {self.message_count/duration:.2f}")
        logger.info(f"평균 메시지 간격: {avg_delay*1000:.2f}ms")
        logger.info(f"최대 메시지 간격: {self.max_delay*1000:.2f}ms")
        logger.info("\n심볼별 업데이트 횟수:")
        for symbol, count in sorted(self.symbol_updates.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"{symbol}: {count} ({count/duration:.2f}/s)")

    async def connect_and_subscribe(self):
        streams = [f"{symbol.lower()}usdt@depth@100ms" for symbol in self.symbols]
        ws_url = f"{self.ws_url}?streams={'/'.join(streams)}"
        
        try:
            async with websockets.connect(ws_url) as websocket:
                logger.info("웹소켓 연결 성공")
                self.start_time = time.time()
                
                # 5분 동안 실행
                end_time = self.start_time + 300  # 5분 = 300초
                
                while time.time() < end_time:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=30)
                        await self.handle_message(message)
                    except asyncio.TimeoutError:
                        logger.warning("메시지 수신 타임아웃")
                    except Exception as e:
                        logger.error(f"메시지 처리 중 오류: {e}")
                
                self.print_statistics()
                
        except Exception as e:
            logger.error(f"웹소켓 연결 오류: {e}")

async def main():
    test = BinanceFuturesWebSocketTest()
    await test.connect_and_subscribe()

if __name__ == "__main__":
    logger.info("바이낸스 선물 웹소켓 테스트 시작")
    asyncio.run(main())
    logger.info("바이낸스 선물 웹소켓 테스트 종료") 