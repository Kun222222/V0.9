"""
오더북 콜렉터 이벤트 타입 정의

이 모듈은 오더북 콜렉터에서 사용되는 이벤트 상수와 타입을 정의합니다.
모든 컴포넌트가 동일한 이벤트 타입을 참조하도록 합니다.
"""
from enum import Enum
# 필요한 경우 새로운 config 모듈에서 필요한 상수를 가져옵니다.
from crosskimp.common.config.common_constants import EventPriority

# 이벤트 타입 정의
class EventTypes:
    # 연결 이벤트 - 웹소켓 연결 관련
    CONNECTION_STATUS = "connection_status"      # 연결 상태 변경 (연결됨, 연결 안됨 등)
    CONNECTION_SUCCESS = "connection_success"    # 연결 성공 (웹소켓 연결 성공)
    CONNECTION_FAILURE = "connection_failure"    # 연결 실패 (웹소켓 연결 실패)
    CONNECTION_CLOSED = "connection_closed"      # 연결 종료 (정상적인 연결 종료)
    CONNECTION_ATTEMPT = "connection_attempt"    # 연결 시도 (연결 시도 시작)
    RECONNECT_EVENT = "reconnect_event"          # 재연결 이벤트 (연결 끊김 후 재연결)
    
    # 구독 이벤트 - 심볼 구독 관련
    SUBSCRIPTION_STATUS = "subscription_status"  # 구독 상태 변경 (구독됨, 구독 안됨 등)
    SUBSCRIPTION_SUCCESS = "subscription_success" # 구독 성공 (심볼 구독 성공)
    SUBSCRIPTION_FAILURE = "subscription_failure" # 구독 실패 (심볼 구독 실패)
    UNSUBSCRIBE_REQUEST = "unsubscribe_request"   # 구독 해제 요청
    
    # 메시지 이벤트 - 웹소켓 메시지 관련
    MESSAGE_RECEIVED = "message_received"         # 메시지 수신 (웹소켓 메시지 도착)
    MESSAGE_PROCESSED = "message_processed"       # 메시지 처리 완료
    PING_PONG = "ping_pong"                       # 핑퐁 메시지 (연결 유지용)
    
    # 데이터 이벤트 - 오더북 데이터 관련
    SNAPSHOT_RECEIVED = "snapshot_received"       # 스냅샷 수신 (전체 오더북)
    DELTA_RECEIVED = "delta_received"             # 델타 수신 (부분 오더북 변경)
    ORDERBOOK_UPDATED = "orderbook_updated"       # 오더북 업데이트 (처리된 오더북 데이터)
    TICKER_UPDATED = "ticker_updated"             # 티커 업데이트 (가격 정보 업데이트)
    
    # 오류 이벤트 - 오류 처리 관련
    ERROR_EVENT = "error_event"                   # 일반 오류 (모든 오류의 기본 타입)
    VALIDATION_ERROR = "validation_error"         # 검증 오류 (오더북 데이터 검증 실패)
    WEBSOCKET_ERROR = "websocket_error"           # 웹소켓 오류 (웹소켓 통신 관련 오류)
    PARSING_ERROR = "parsing_error"               # 파싱 오류 (메시지 파싱 실패)
    
    # 시스템 이벤트 - 시스템 제어 관련
    SYSTEM_STARTUP = "system_startup"             # 시스템 시작 (오더북 콜렉터 시작)
    SYSTEM_SHUTDOWN = "system_shutdown"           # 시스템 종료 (오더북 콜렉터 종료)
    SYSTEM_HEARTBEAT = "system_heartbeat"         # 시스템 하트비트 (주기적 상태 체크)
    
    # 메트릭 이벤트 - 성능 측정 관련
    EXCHANGE_METRIC = "exchange_metric"           # 거래소 메트릭 (거래소별 성능 지표)
    LATENCY_METRIC = "latency_metric"             # 지연시간 메트릭 (네트워크 지연 등)
    METRIC_UPDATE = "metric_update"               # 메트릭 업데이트 (일반적인 성능 지표)
    
    # 마켓 이벤트 - 마켓 상태 관련
    MARKET_STATUS = "market_status"               # 마켓 상태 (거래소 마켓 상태)
    SYMBOL_STATUS = "symbol_status"               # 심볼 상태 (개별 심볼 거래 상태)
