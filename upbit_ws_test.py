#!/usr/bin/env python
import asyncio
import json
import time
import websockets
from typing import Dict, List
from datetime import datetime

# 기본 설정
WS_URL = "wss://api.upbit.com/websocket/v1"
MAX_SYMBOLS_PER_SUBSCRIPTION = 15

# 테스트할 심볼 목록
SYMBOLS = [
    'ADA', 'AERGO', 'ARK', 'AUCTION', 'CKB', 'DOGE', 'ENS', 'EOS', 'GMT', 'HBAR', 
    'HIFI', 'LAYER', 'NEAR', 'ONDO', 'QTUM', 'SOL', 'SUI', 'T', 'TRUMP', 'TRX', 
    'USDT', 'VANA', 'XLM', 'XRP', 'ZRO'
]

class UpbitWsTest:
    """
    업비트 웹소켓 테스트 클래스
    - 지정된 심볼 목록에 대한 웹소켓 메시지 수신 속도 측정
    """
    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self.ws = None
        self.stop_event = asyncio.Event()
        
        # 메시지 수신 통계
        self.message_count = 0
        self.message_rates = []
        self.last_count = 0
        self.last_time = time.time()
        
        # 심볼별 메시지 수신 카운트
        self.symbol_counts: Dict[str, int] = {symbol: 0 for symbol in symbols}
        
        print(f"업비트 웹소켓 테스트 초기화 | 심볼 수: {len(symbols)}")
        
    async def create_subscribe_message(self, symbols: List[str]) -> List[Dict]:
        """
        구독 메시지 생성
        """
        # 심볼 형식 변환 (KRW-{symbol})
        market_codes = [f"KRW-{s.upper()}" for s in symbols]
        
        # 구독 메시지 생성
        message = [
            {"ticket": f"upbit_test_{int(time.time())}"},
            {
                "type": "orderbook",  # 오더북 구독
                "codes": market_codes,
                "isOnlyRealtime": False  # 스냅샷 포함 요청
            },
            {"format": "DEFAULT"}
        ]
        
        return message
    
    async def connect_and_subscribe(self):
        """
        웹소켓 연결 및 구독
        """
        try:
            # 웹소켓 연결
            self.ws = await websockets.connect(WS_URL)
            print(f"업비트 웹소켓 연결 성공")
            
            # 배치 단위로 구독 처리
            total_batches = (len(self.symbols) + MAX_SYMBOLS_PER_SUBSCRIPTION - 1) // MAX_SYMBOLS_PER_SUBSCRIPTION
            print(f"구독 시작 | 총 {len(self.symbols)}개 심볼, {total_batches}개 배치로 나눔")
            
            for i in range(0, len(self.symbols), MAX_SYMBOLS_PER_SUBSCRIPTION):
                batch_symbols = self.symbols[i:i + MAX_SYMBOLS_PER_SUBSCRIPTION]
                batch_num = (i // MAX_SYMBOLS_PER_SUBSCRIPTION) + 1
                
                # 구독 메시지 생성 및 전송
                subscribe_message = await self.create_subscribe_message(batch_symbols)
                await self.ws.send(json.dumps(subscribe_message))
                
                print(f"구독 요청 전송 | 배치 {batch_num}/{total_batches}, {len(batch_symbols)}개 심볼")
                await asyncio.sleep(0.1)  # 요청 간 짧은 딜레이
                
            # 초기 시간 기록
            self.last_time = time.time()
            
            return True
        except Exception as e:
            print(f"웹소켓 연결 또는 구독 오류: {str(e)}")
            return False
    
    async def receive_messages(self):
        """
        메시지 수신 및 통계 처리
        """
        try:
            while not self.stop_event.is_set():
                # 메시지 수신
                message = await self.ws.recv()
                
                # 메시지 파싱
                if isinstance(message, bytes):
                    message = message.decode('utf-8')
                    
                data = json.loads(message)
                
                # 카운트 증가
                self.message_count += 1
                
                # 심볼별 카운트 추적 (KRW-BTC -> BTC)
                if 'code' in data:
                    symbol = data['code'].replace('KRW-', '')
                    if symbol in self.symbol_counts:
                        self.symbol_counts[symbol] += 1
                
                # 1초마다 처리율 계산
                current_time = time.time()
                time_diff = current_time - self.last_time
                
                if time_diff >= 1.0:
                    message_diff = self.message_count - self.last_count
                    rate = message_diff / time_diff
                    
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    print(f"{timestamp} - [업비트] 처리율 계산 | current_count={self.message_count}, "
                          f"last_count={self.last_count}, message_diff={message_diff}, "
                          f"time_diff={time_diff:.2f}초, rate={rate:.2f}건/초")
                    
                    self.message_rates.append(rate)
                    self.last_count = self.message_count
                    self.last_time = current_time
                    
                    # 10초마다 심볼별 통계 출력
                    if len(self.message_rates) % 10 == 0:
                        self.print_symbol_stats()
                        
        except websockets.exceptions.ConnectionClosed as e:
            print(f"웹소켓 연결 종료: {e}")
        except Exception as e:
            print(f"메시지 수신 중 오류: {str(e)}")
    
    def print_symbol_stats(self):
        """
        심볼별 메시지 수신 통계 출력
        """
        print("\n== 심볼별 메시지 수신 통계 ==")
        # 수신 건수 내림차순 정렬
        sorted_symbols = sorted(self.symbol_counts.items(), key=lambda x: x[1], reverse=True)
        
        for symbol, count in sorted_symbols:
            print(f"{symbol}: {count}건")
        print("============================\n")
    
    async def run(self, duration_seconds: int = 60):
        """
        테스트 실행
        
        Args:
            duration_seconds: 테스트 실행 시간 (초)
        """
        try:
            # 연결 및 구독
            if not await self.connect_and_subscribe():
                return
            
            # 메시지 수신 태스크 생성
            receive_task = asyncio.create_task(self.receive_messages())
            
            # 지정된 시간 동안 실행
            print(f"{duration_seconds}초 동안 테스트 실행...")
            await asyncio.sleep(duration_seconds)
            
            # 테스트 종료
            self.stop_event.set()
            
            # 태스크 정리
            if not receive_task.done():
                receive_task.cancel()
                try:
                    await receive_task
                except asyncio.CancelledError:
                    pass
            
            # 최종 통계 출력
            self.print_final_stats()
            
        finally:
            # 웹소켓 연결 종료
            if self.ws:
                await self.ws.close()
    
    def print_final_stats(self):
        """
        최종 통계 결과 출력
        """
        if not self.message_rates:
            print("수집된 통계 데이터가 없습니다.")
            return
            
        avg_rate = sum(self.message_rates) / len(self.message_rates)
        max_rate = max(self.message_rates) if self.message_rates else 0
        min_rate = min(self.message_rates) if self.message_rates else 0
        
        print("\n========== 테스트 결과 ==========")
        print(f"총 메시지 수신: {self.message_count}건")
        print(f"평균 처리율: {avg_rate:.2f}건/초")
        print(f"최대 처리율: {max_rate:.2f}건/초")
        print(f"최소 처리율: {min_rate:.2f}건/초")
        print(f"측정 횟수: {len(self.message_rates)}회")
        print("==================================")
        
        # 심볼별 최종 통계 출력
        self.print_symbol_stats()


async def main():
    # 테스트 실행 (60초 동안)
    tester = UpbitWsTest(SYMBOLS)
    await tester.run(duration_seconds=60)


if __name__ == "__main__":
    print(f"업비트 WebSocket 메시지 수신 테스트 시작")
    print(f"대상 심볼 {len(SYMBOLS)}개: {', '.join(SYMBOLS)}")
    asyncio.run(main()) 