# 각 거래소별 API 공식 문서 정리

## 목차
1. 업비트
2. 빗썸
3. 바이낸스
4. 바이낸스 선물
5. 바이빗
6. 바이빗 선물


## 퍼블릭 관련 문서

1. 업비트
- 기본정보: https://docs.upbit.com/reference/general-info
- 오더북 정보조회: https://docs.upbit.com/reference/%ED%98%B8%EA%B0%80-%EC%A0%95%EB%B3%B4-%EC%A1%B0%ED%9A%8C
- 연결 관리: https://docs.upbit.com/reference/connection
- REST API: https://docs.upbit.com/docs/upbit-quotation-restful-api
- 요청수 제한: https://docs.upbit.com/docs/user-request-guide

2. 빗썸
- 기본정보: 
- 오더북 정보조회: https://apidocs.bithumb.com/reference/%ED%98%B8%EA%B0%80-%EC%A0%95%EB%B3%B4-%EC%A1%B0%ED%9A%8C
- 웹소켓 스냅샷: https://apidocs.bithumb.com/changelog/%EC%97%85%EB%8D%B0%EC%9D%B4%ED%8A%B8-public-websocket-snapshot-%EA%B8%B0%EB%8A%A5-%EC%A7%80%EC%9B%90-%EC%95%88%EB%82%B4\
- 웹소켓 스냅샷2: https://apidocs.bithumb.com/v2.1.4/reference/%EC%9A%94%EC%B2%AD-%ED%8F%AC%EB%A7%B7

3. 바이낸스-현물
- General: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-api/general-api-information
- rate limit: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-api/rate-limits
- Market data requests: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-api/market-data-requests
- General WSS information: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams

4. 바이낸스-선물
- General: https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info
- API: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-api-general-info
- orderbbok: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/websocket-api
- market stream: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams

5. 바이빗-현물
- connect: https://bybit-exchange.github.io/docs/v5/ws/connect
- orderbook: https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
- rate limit: https://bybit-exchange.github.io/docs/v5/rate-limit
바이빗 오더북 업데이트 가이드
바이빗 오더북 데이터는 두 가지 유형으로 제공됩니다:
스냅샷(snapshot): 전체 오더북 데이터
델타(delta): 변경된 부분만 포함하는 업데이트
오더북 데이터 처리 흐름
초기 스냅샷 수신:
구독 직후 type: "snapshot" 메시지를 받게 됩니다.
이 데이터로 로컬 오더북을 초기화합니다.
델타 업데이트 처리:
이후 type: "delta" 메시지를 통해 변경사항을 받습니다.
각 델타 업데이트는 변경된 가격 레벨만 포함합니다.
수량이 "0"인 항목은 해당 가격 레벨을 삭제해야 함을 의미합니다.
시퀀스 번호 확인:
각 메시지에는 seq 필드가 포함되어 있습니다.
이 번호는 순차적으로 증가해야 합니다.
만약 시퀀스 번호가 연속적이지 않다면, 데이터 손실이 발생한 것이므로 재구독해야 합니다.
업데이트 ID 확인:
각 메시지에는 u (업데이트 ID) 필드가 포함되어 있습니다.
이 ID도 순차적으로 증가해야 합니다.
오더북 업데이트 예시 코드
다음은 바이빗 오더북 데이터를 처리하는 예시 코드입니다:
True
주의사항
재연결 시 처리:
연결이 끊어진 후 재연결할 때는 항상 새로운 스냅샷부터 시작해야 합니다.
이전 오더북 상태를 완전히 버리고 새로 초기화하세요.
오더북 정렬:
매수 주문(bids)은 가격 내림차순으로 정렬해야 합니다(높은 가격이 먼저).
매도 주문(asks)은 가격 오름차순으로 정렬해야 합니다(낮은 가격이 먼저).
업데이트 속도:
바이빗은 오더북 업데이트를 매우 빠르게 제공합니다.
특히 orderbook.50 또는 orderbook.500을 구독할 경우 많은 양의 데이터를 처리해야 합니다.
하트비트 유지:
연결을 유지하기 위해 20초마다 ping 메시지를 보내는 것이 중요합니다.
이러한 가이드라인을 따라 바이빗 오더북 데이터를 정확하게 처리할 수 있습니다.


네, 정확하게 이해하셨습니다! 데이터 흐름과 파일 구조에 대한 설명을 더 자세히 해드리겠습니다.
데이터 흐름
웹소켓 연결 및 메시지 수신 흐름:
   cnt_manager.py > subscription_manager.py > bybit_subscriber.py(각 거래소)
cnt_manager.py의 WebSocketManager 클래스가 웹소켓 연결을 관리하고 메시지를 수신합니다.
수신된 메시지는 subscription_manager.py의 SubscriptionManager 클래스로 전달됩니다.
SubscriptionManager는 거래소별 구독자(bybit_subscriber.py 등)에게 메시지를 전달합니다.
오더북 처리 흐름:

   bybit_subscriber.py > bybit_orderbook.py > bybit_orderbook_manager.py
   
bybit_subscriber.py는 구독 메시지 형식을 관리하고 수신된 메시지를 처리합니다.
처리된 메시지는 bybit_orderbook.py의 BybitOrderbook 클래스로 전달되어 오더북 데이터로 변환됩니다.
bybit_orderbook_manager.py의 BybitOrderbookManager 클래스는 여러 심볼의 오더북을 관리합니다.
테스트 파일
맞습니다. 정상적인 통합이 이루어지면 다음 테스트 파일들은 실제 운영 환경에서는 필요하지 않습니다:
bybit_test_without_connection.py: 웹소켓 연결 없이 메시지 처리 기능만 테스트하는 파일입니다.
bybit_subscription_integration.py: 구독 관리자와의 통합을 테스트하는 파일입니다.
bybit_orderbook_example.py: 오더북 처리 예제를 보여주는 파일입니다.
bybit_integration.py: 전체 통합 테스트를 위한 파일입니다.
이 파일들은 개발 및 테스트 단계에서 각 컴포넌트가 올바르게 작동하는지 확인하기 위한 것으로, 실제 운영 환경에서는 필요하지 않습니다. 실제 운영 환경에서는 cnt_manager.py가 모든 거래소의 웹소켓 연결을 관리하고, subscription_manager.py가 구독을 관리하며, 각 거래소별 구독자와 오더북 클래스가 데이터를 처리합니다.
테스트 결과에서 언급한 대로, 이제 실제 환경에서는 웹소켓 연결 관련 코드를 추가하여 실제 Bybit 거래소와 통신할 수 있습니다. 테스트 파일들은 이 기능이 올바르게 작동하는지 확인하는 데 사용되었으며, 이제 실제 구현에 통합할 수 있습니다.




6. 바이빗-선물