import json
import asyncio
import websockets

async def test_bybit_ws():
    uri = "wss://stream.bybit.com/v5/public/spot"
    
    async with websockets.connect(uri) as websocket:
        # 구독 메시지 전송
        subscribe_message = {
            "op": "subscribe",
            "args": ["orderbook.50.BTCUSDT"]
        }
        await websocket.send(json.dumps(subscribe_message))
        
        # 구독 응답 수신
        response = await websocket.recv()
        print("구독 응답:", json.dumps(json.loads(response), indent=2))
        
        # 첫 메시지 수신 (스냅샷 또는 델타)
        data_message = await websocket.recv()
        data = json.loads(data_message)
        print("\n첫 메시지 타입:", data.get("type"))
        print(json.dumps(data, indent=2))
        
        # 타임스탬프 확인
        if "data" in data:
            print("\n타임스탬프 정보:")
            print("- ts 필드:", data.get("data", {}).get("ts"))
            print("- 전체 데이터 필드들:", list(data.get("data", {}).keys()))

async def main():
    await test_bybit_ws()

if __name__ == "__main__":
    asyncio.run(main()) 