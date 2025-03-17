# 웹소켓 연결 코드 작성 가이드

## 1. 기본 구조

### 1.1 상수 정의
```python
# 거래소 정보
EXCHANGE_CODE = "거래소코드"  # 예: "upbit"
EXCHANGE_NAME_KR = "[거래소한글명]"  # 예: "[업비트]"

# 웹소켓 연결 설정
WS_URL = "wss://api.example.com/websocket/v1"  # 웹소켓 URL
PING_INTERVAL = 30  # 핑 전송 간격 (초)
PING_TIMEOUT = 10   # 핑 응답 타임아웃 (초)
MESSAGE_TIMEOUT = 60  # 메시지 타임아웃 (초)
HEALTH_CHECK_INTERVAL = 30  # 헬스 체크 간격 (초)
```

### 1.2 클래스 구조
```python
class ExchangeWebSocketConnector(BaseWebsocketConnector):
    def __init__(self, settings: dict):
        super().__init__(settings, EXCHANGE_CODE)
        self.ws_url = WS_URL
        
        # 거래소 전용 설정
        self.ping_interval = PING_INTERVAL
        self.ping_timeout = PING_TIMEOUT
        self.message_timeout = MESSAGE_TIMEOUT
        self.health_check_interval = HEALTH_CHECK_INTERVAL
        
        # 재연결 전략 설정
        self.reconnect_strategy = ReconnectStrategy(
            initial_delay=1.0,    # 초기 재연결 대기 시간
            max_delay=60.0,       # 최대 재연결 대기 시간
            multiplier=2.0,       # 대기 시간 증가 배수
            max_attempts=0        # 0 = 무제한 재시도
        )
```

## 2. 필수 구현 메서드

### 2.1 connect() - 웹소켓 연결
```python
async def connect(self) -> bool:
    if self.is_connected and self.ws:
        return True
        
    if self.connecting:
        self.log_debug("이미 연결 중")
        return True
        
    self.connecting = True
    
    try:
        # 웹소켓 연결
        self.ws = await connect(
            self.ws_url,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout
        )
        
        # 연결 성공 처리
        self.is_connected = True
        self.stats.connection_start_time = time.time()
        self.reconnect_strategy.reset()
        
        # 메트릭 업데이트
        self.metrics.update_connection_state(self.exchangename, "connected")
        
        # 연결 성공 알림
        connect_msg = f"{self.exchange_korean_name} 웹소켓 연결 성공"
        await self.send_telegram_notification("connect", connect_msg)
        
        return True
        
    except Exception as e:
        self.log_error(f"연결 오류: {str(e)}")
        return False
        
    finally:
        self.connecting = False
```

### 2.2 disconnect() - 웹소켓 연결 종료
```python
async def disconnect(self) -> bool:
    try:
        if self.ws:
            await self.ws.close()
            
        self.is_connected = False
        self.metrics.update_connection_state(self.exchangename, "disconnected")
        return True
        
    except Exception as e:
        self.log_error(f"웹소켓 연결 종료 실패: {str(e)}")
        return False
```

### 2.3 send_message() - 메시지 전송
```python
async def send_message(self, message: str) -> bool:
    try:
        if not self.ws or not self.is_connected:
            self.log_error("웹소켓이 연결되지 않음")
            return False
            
        await self.ws.send(message)
        return True
    except Exception as e:
        self.log_error(f"메시지 전송 실패: {str(e)}")
        return False
```

### 2.4 health_check() - 상태 체크
```python
async def health_check(self) -> None:
    while not self.stop_event.is_set():
        try:
            current_time = time.time()
            
            # 메시지 타임아웃 체크
            if self.is_connected and self.stats.last_message_time > 0:
                if (current_time - self.stats.last_message_time) > self.message_timeout:
                    error_msg = f"{self.exchange_korean_name} 웹소켓 메시지 타임아웃"
                    self.log_error(error_msg)
                    await self.send_telegram_notification("error", error_msg)
                    await self.reconnect()
            
            await asyncio.sleep(self.health_check_interval)
            
        except Exception as e:
            self.log_error(f"웹소켓 상태 체크 중 오류: {str(e)}")
            await asyncio.sleep(1)
```

### 2.5 reconnect() - 재연결
```python
async def reconnect(self) -> bool:
    try:
        self.stats.reconnect_count += 1
        reconnect_msg = f"{self.exchange_korean_name} 웹소켓 재연결 시도"
        self.log_info(reconnect_msg)
        await self.send_telegram_notification("reconnect", reconnect_msg)
        
        await self.disconnect()
        
        # 재연결 대기
        delay = self.reconnect_strategy.next_delay()
        await asyncio.sleep(delay)
        
        success = await self.connect()
        return success
        
    except Exception as e:
        self.log_error(f"웹소켓 재연결 실패: {str(e)}")
        return False
```

### 2.6 receive_raw() - 메시지 수신
```python
async def receive_raw(self) -> Optional[str]:
    try:
        if not self.ws or not self.is_connected:
            return None
            
        message = await self.ws.recv()
        
        if message:
            self.update_message_metrics(message)
            
        return message
        
    except websockets.exceptions.ConnectionClosed:
        self.log_error("웹소켓 연결 끊김")
        self.is_connected = False
        self.metrics.update_connection_state(self.exchangename, "disconnected")
        return None
        
    except Exception as e:
        self.log_error(f"메시지 수신 실패: {e}")
        self.metrics.record_error(self.exchangename)
        return None
```

## 3. 로깅 가이드

### 3.1 로깅 메서드
```python
# 오류 로깅
self.log_error("오류 메시지")

# 정보 로깅
self.log_info("정보 메시지")

# 디버그 로깅
self.log_debug("디버그 메시지")

# 경고 로깅
self.log_warning("경고 메시지")
```

### 3.2 텔레그램 알림
```python
# 중요 이벤트 알림
await self.send_telegram_notification("error", "오류 메시지")
await self.send_telegram_notification("connect", "연결 성공")
await self.send_telegram_notification("disconnect", "연결 종료")
await self.send_telegram_notification("reconnect", "재연결 시도")
```

## 4. 주의사항

1. 모든 예외 처리는 구체적으로 해야 합니다.
2. 연결 상태 변경 시 메트릭을 업데이트해야 합니다.
3. 중요 이벤트는 텔레그램으로 알림을 보내야 합니다.
4. 재연결 전략은 거래소 특성에 맞게 조정해야 합니다.
5. 타임아웃 설정은 거래소 API 문서를 참고하여 설정해야 합니다. 