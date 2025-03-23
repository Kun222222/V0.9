"""
오더북 도메인 이벤트 타입 정의

이 모듈은 오더북 관련 이벤트 타입들을 정의합니다.
기존 오더북 이벤트 시스템과 통합 이벤트 시스템을 연결하는 역할을 합니다.
"""

class OrderbookEventTypes:
    """오더북 관련 이벤트 타입 정의 클래스"""
    
    # 연결 이벤트
    CONNECTION_STATUS = "orderbook:connection_status"       # 웹소켓 연결 상태
    CONNECTION_ATTEMPT = "orderbook:connection_attempt"     # 연결 시도
    CONNECTION_SUCCESS = "orderbook:connection_success"     # 연결 성공
    CONNECTION_FAILURE = "orderbook:connection_failure"     # 연결 실패
    CONNECTION_CLOSED = "orderbook:connection_closed"       # 연결 종료
    
    # 구독 이벤트
    SUBSCRIPTION_STATUS = "orderbook:subscription_status"   # 구독 상태
    SUBSCRIPTION_REQUEST = "orderbook:subscription_request" # 구독 요청
    SUBSCRIPTION_SUCCESS = "orderbook:subscription_success" # 구독 성공
    SUBSCRIPTION_FAILURE = "orderbook:subscription_failure" # 구독 실패
    UNSUBSCRIBE_REQUEST = "orderbook:unsubscribe_request"   # 구독 해제 요청
    
    # 메시지 이벤트
    MESSAGE_RECEIVED = "orderbook:message_received"         # 메시지 수신
    MESSAGE_PROCESSED = "orderbook:message_processed"       # 메시지 처리
    
    # 데이터 이벤트
    SNAPSHOT_RECEIVED = "orderbook:snapshot_received"       # 스냅샷 수신
    DELTA_RECEIVED = "orderbook:delta_received"             # 델타 수신
    ORDERBOOK_UPDATED = "orderbook:updated"                 # 오더북 업데이트
    TICKER_UPDATED = "orderbook:ticker_updated"             # 티커 업데이트
    
    # 오류 이벤트
    ERROR_EVENT = "orderbook:error"                         # 일반 오류
    VALIDATION_ERROR = "orderbook:validation_error"         # 검증 오류
    WEBSOCKET_ERROR = "orderbook:websocket_error"           # 웹소켓 오류
    PARSING_ERROR = "orderbook:parsing_error"               # 파싱 오류
    
    # 메트릭 이벤트
    EXCHANGE_METRIC = "orderbook:exchange_metric"           # 거래소 메트릭
    LATENCY_METRIC = "orderbook:latency_metric"             # 지연시간 메트릭
    METRIC_UPDATE = "orderbook:metric_update"               # 메트릭 업데이트
    
    # 마켓 이벤트 
    MARKET_STATUS = "orderbook:market_status"               # 마켓 상태
    SYMBOL_STATUS = "orderbook:symbol_status"               # 심볼 상태

# 이전 이벤트 타입과 새 이벤트 타입 간의 매핑
# 이 매핑은 마이그레이션 과정에서 이전 이벤트 타입을 새 이벤트 타입으로 변환하는 데 사용됩니다.
EVENT_TYPE_MAPPING = {
    # 기존 이벤트 타입: 새 이벤트 타입
    "connection_status": OrderbookEventTypes.CONNECTION_STATUS,
    "subscription_status": OrderbookEventTypes.SUBSCRIPTION_STATUS,
    "message_received": OrderbookEventTypes.MESSAGE_RECEIVED,
    "message_processed": OrderbookEventTypes.MESSAGE_PROCESSED,
    "data_snapshot": OrderbookEventTypes.SNAPSHOT_RECEIVED,
    "data_delta": OrderbookEventTypes.DELTA_RECEIVED,
    "data_updated": OrderbookEventTypes.ORDERBOOK_UPDATED,
    "error_event": OrderbookEventTypes.ERROR_EVENT,
    "exchange_metric": OrderbookEventTypes.EXCHANGE_METRIC,
    "metric_update": OrderbookEventTypes.METRIC_UPDATE,
} 