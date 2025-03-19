"""
업비트 웹소켓 연결 및 구독 예제

이 예제는 업비트 거래소의 오더북 데이터를 수집하는 방법을 보여줍니다.
WebSocketConnector와 Subscription을 분리한 새로운 구조를 사용합니다.
"""

import asyncio
import signal
import sys
from typing import Dict

from crosskimp.ob_collector.orderbook.connection.upbit_s_cn import UpbitWebSocketConnector
from crosskimp.ob_collector.orderbook.subscription.upbit_s_sub import UpbitSubscription


# 콜백 함수 정의
async def on_snapshot(symbol: str, data: Dict):
    """
    스냅샷 데이터 수신 시 호출되는 콜백 함수
    
    Args:
        symbol: 심볼 (예: "BTC")
        data: 오더북 데이터
    """
    # 받은 오더북 데이터 중 일부만 출력
    bid_count = len(data.get("bids", []))
    ask_count = len(data.get("asks", []))
    timestamp = data.get("timestamp")
    
    # 최고 매수호가와 최저 매도호가 추출
    best_bid = data.get("bids", [[0, 0]])[0][0] if data.get("bids") else 0
    best_ask = data.get("asks", [[0, 0]])[0][0] if data.get("asks") else 0
    
    print(f"[{symbol}] 스냅샷: {bid_count}개 매수 / {ask_count}개 매도 호가, "
          f"최고매수: {best_bid:,}, 최저매도: {best_ask:,}, 시간: {timestamp}")


async def on_error(symbol: str, error: str):
    """
    에러 발생 시 호출되는 콜백 함수
    
    Args:
        symbol: 심볼 (예: "BTC")
        error: 에러 메시지
    """
    print(f"[{symbol}] 오류: {error}")


# 종료 처리 함수
def handle_exit(connector, subscription):
    """
    프로그램 종료 시 호출되는 함수
    
    Args:
        connector: 웹소켓 연결 객체
        subscription: 구독 객체
    """
    loop = asyncio.get_event_loop()
    
    async def cleanup():
        print("프로그램 종료 중...")
        # 구독 취소 및 종료
        await subscription.unsubscribe()
        # 연결 종료
        await connector.disconnect()
        loop.stop()
    
    loop.create_task(cleanup())


# 메인 실행 함수
async def main():
    """
    메인 함수
    
    1. 웹소켓 연결 객체 초기화
    2. 구독 객체 초기화
    3. 심볼 구독 및 데이터 수신
    """
    try:
        # 테스트용 설정
        settings = {
            "telegram": {
                "enabled": False,
                "token": "",
                "chat_id": ""
            },
            "log": {
                "level": "INFO",
                "file": True,
                "console": True
            }
        }
        
        # 1. 웹소켓 연결 객체 초기화
        connector = UpbitWebSocketConnector(settings)
        
        # 2. 구독 객체 초기화
        subscription = UpbitSubscription(connector)
        
        # 종료 시그널 핸들러 등록
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, lambda: handle_exit(connector, subscription))
        
        # 3. 웹소켓 연결
        if not await connector.connect():
            print("웹소켓 연결 실패")
            return
            
        print("웹소켓 연결 성공")
        
        # 4. 심볼 구독
        symbols = ["BTC", "ETH"]  # 구독할 심볼 리스트
        
        subscription_result = await subscription.subscribe(
            symbols,
            on_snapshot=on_snapshot,
            on_error=on_error
        )
        
        if not subscription_result:
            print("구독 실패")
            await connector.disconnect()
            return
            
        print(f"{', '.join(symbols)} 구독 완료")
        
        # 프로그램 계속 실행
        while True:
            await asyncio.sleep(1)
            
    except Exception as e:
        print(f"예상치 못한 오류 발생: {e}")
    finally:
        # 명시적인 종료 처리
        if 'subscription' in locals():
            await subscription.unsubscribe()
        if 'connector' in locals():
            await connector.disconnect()


# 프로그램 시작
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("사용자에 의해 프로그램 종료")
    except Exception as e:
        print(f"프로그램 실행 중 오류 발생: {e}")
        sys.exit(1) 