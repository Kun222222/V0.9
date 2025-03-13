import asyncio
import json
import websockets

async def test_bybit_websocket():
    uri = "wss://stream.bybit.com/v5/public/linear"
    
    print(f"연결 시도: {uri}")
    async with websockets.connect(uri) as websocket:
        print("연결 성공!")
        
        # ELX 오더북 구독
        subscribe_msg = {
            "op": "subscribe",
            "args": ["orderbook.50.ELXUSDT"]
        }
        
        print(f"구독 요청: {json.dumps(subscribe_msg)}")
        await websocket.send(json.dumps(subscribe_msg))
        
        # 응답 대기
        for _ in range(5):  # 5개 메시지만 받아보기
            response = await websocket.recv()
            print(f"수신: {response}")
            
            # JSON으로 파싱하여 예쁘게 출력
            try:
                data = json.loads(response)
                if "data" in data:
                    # 오더북 데이터인 경우
                    ob_data = data.get("data", {})
                    bids = ob_data.get("b", [])
                    asks = ob_data.get("a", [])
                    print(f"매수 호가 수: {len(bids)}, 매도 호가 수: {len(asks)}")
                    
                    # 첫 5개 항목만 출력
                    print(f"매수 호가 (최대 5개): {bids[:5]}")
                    print(f"매도 호가 (최대 5개): {asks[:5]}")
            except json.JSONDecodeError:
                print("JSON 파싱 실패")
            
            await asyncio.sleep(1)  # 1초 대기

if __name__ == "__main__":
    asyncio.run(test_bybit_websocket()) 