# 웹소켓 재연결 및 재구독 테스트

이 테스트는 웹소켓 연결이 끊어진 후 재연결되었을 때 자동으로 재구독이 이루어지는지 확인하는 테스트입니다.

## 테스트 내용

1. `test_websocket_reconnect.py` - 웹소켓 연결 끊김과 재연결 후 구독 상태 테스트
   - `MockWebSocket` - 테스트용 웹소켓 Mock 클래스
   - `MockConnector` - `BaseWebsocketConnector`를 상속받은 Mock 클래스
   - `MockSubscription` - `BaseSubscription`을 상속받은 Mock 클래스
   - `TestWebSocketReconnect` - 테스트 케이스 클래스

## 테스트 시나리오

1. 초기 웹소켓 연결 및 심볼 구독
2. 웹소켓 연결 끊김 시뮬레이션
3. 재연결 시도
4. 재연결 후 자동 재구독 확인
5. 구독 상태 및 메시지 전송 검증

## 테스트 실행 방법

```bash
# pytest로 실행
python -m pytest tests/test_websocket_reconnect.py -v

# 또는 실행 스크립트로 실행
./tests/run_reconnect_test.py
```

## 주요 확인 사항

- ObCollector의 `_connect_and_subscribe` 메서드가 연결과 구독을 모두 처리
- 연결 끊김 발생 시 재연결 시도가 이루어짐
- 재연결 후 구독 정보가 유지됨
- 구독 메시지가 정상적으로 전송됨

## 테스트 결과 분석

구독 상태 (`self.subscription_status`)와 구독 심볼 목록 (`self.subscribed_symbols`)이 재연결 전후로 일치하는지 확인합니다. 또한 연결 및 구독 시도 횟수가 예상대로 증가하는지도 확인합니다. 