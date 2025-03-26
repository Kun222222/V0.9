"""
크로스 김프 아비트리지 - 시스템 이벤트 타입 모듈

이 모듈은 이벤트 버스 시스템에서 사용되는 모든 이벤트 경로 상수를 정의합니다.
계층적 구조를 가진 경로 문자열을 통해 이벤트를 카테고리화합니다.
"""

from enum import Enum
from typing import Dict, List, Optional, Any

####################################
# 이벤트 경로 체계 (계층 구조)
####################################
class EventPaths:
    """
    이벤트 경로 체계 - 계층적 문자열 상수
    
    이벤트 버스에서 사용되는 모든 이벤트의 경로 상수를 정의합니다.
    계층적 구조로 체계화되어 이벤트의 소속과 목적을 명확히 합니다.
    
    경로 구조: <카테고리>/<서브카테고리>/<이벤트이름>
    
    사용 방법:
        # 이벤트 발행
        await event_bus.publish(EventPaths.SYSTEM_START, data)
        
        # 이벤트 구독
        event_bus.register_handler(EventPaths.PROCESS_START, handler)
    
    주요 이벤트 카테고리:
    1. system/* - 시스템 전체 수준의 이벤트 (시작, 종료 등)
    2. process/* - 프로세스 관리 이벤트 (시작/종료 명령)
    3. component/* - 컴포넌트별 상태 및 기능 이벤트
    """
    
    ###################################
    # 시스템 레벨 이벤트
    ###################################
    # 시스템 이벤트: 전체 애플리케이션 수준의 이벤트
    # 주로 시스템 시작/종료 및 글로벌 상태에 관련됨
    SYSTEM = "system"
    SYSTEM_START = "system/start"                    # 시스템 시작
    SYSTEM_STOP = "system/stop"                      # 시스템 종료
    SYSTEM_HEARTBEAT = "system/heartbeat"            # 시스템 하트비트
    SYSTEM_ERROR = "system/error"                    # 시스템 오류
    SYSTEM_WARNING = "system/warning"                # 시스템 경고
    SYSTEM_INFO = "system/info"                      # 시스템 정보
    SYSTEM_COMMAND = "system/command"                # 시스템 명령
    SYSTEM_STATUS = "system/status"                  # 시스템 상태
    
    ###################################
    # 프로세스 공통 이벤트
    ###################################
    # 프로세스 이벤트: 오케스트레이터가 프로세스 생명주기를 관리하기 위해 발행하는 명령 이벤트
    # "이 프로세스를 시작/중지하라"는 명령의 성격을 가짐
    PROCESS = "process"
    PROCESS_START = "process/start"                  # 프로세스 시작 요청 - 오케스트레이터가 발행
    PROCESS_STOP = "process/stop"                    # 프로세스 중지 요청 - 오케스트레이터가 발행
    PROCESS_RESTART = "process/restart"              # 프로세스 재시작 요청 - 오케스트레이터가 발행
    PROCESS_STATUS = "process/status"                # 프로세스 상태 변경 - 프로세스 핸들러가 발행
    PROCESS_ERROR = "process/error"                  # 프로세스 오류 - 프로세스 핸들러가 발행
    
    ###################################
    # 프로세스 상태 값
    ###################################
    # 프로세스 상태: 프로세스의 현재 실행 상태를 나타내는 값
    PROCESS_STATUS_STOPPED = "process/stopped"    # 중지됨
    PROCESS_STATUS_STARTING = "process/starting"  # 시작 중
    PROCESS_STATUS_RUNNING = "process/running"    # 실행 중
    PROCESS_STATUS_STOPPING = "process/stopping"  # 종료 중
    PROCESS_STATUS_ERROR = "process/error"        # 오류 상태
    
    ###################################
    # 프로세스 이벤트 타입 값
    ###################################
    # 프로세스 이벤트 타입: 프로세스 생명주기 관련 이벤트 유형
    PROCESS_EVENT_START_REQUESTED = "process/start_requested"     # 시작 요청됨
    PROCESS_EVENT_STARTED = "process/started"                     # 시작됨
    PROCESS_EVENT_STOP_REQUESTED = "process/stop_requested"       # 종료 요청됨
    PROCESS_EVENT_STOPPED = "process/stopped"                     # 종료됨
    PROCESS_EVENT_RESTART_REQUESTED = "process/restart_requested" # 재시작 요청됨
    PROCESS_EVENT_ERROR = "process/error"                         # 오류 발생
    
    ###################################
    # 컴포넌트별 이벤트 (오더북 수집기)
    ###################################
    # 컴포넌트별 이벤트: 각 컴포넌트가 자신의 상태 변화를 알리기 위해 발행하는 이벤트
    # "나(컴포넌트)는 지금 이런 상태다"라는 알림의 성격을 가짐
    # 세부적인 모니터링과 알림에 활용됨
    
    # 오더북 수집기 이벤트
    OB_COLLECTOR = "component/ob_collector"
    OB_COLLECTOR_START = "component/ob_collector/start"          # 오더북 시작됨 (상태 알림)
    OB_COLLECTOR_RUNNING = "component/ob_collector/running"          # 오더북 구동 완료됨 (텔레그램 알림용)
    OB_COLLECTOR_STOP = "component/ob_collector/stop"            # 오더북 중지됨 (상태 알림)
    OB_COLLECTOR_METRICS = "component/ob_collector/metrics"      # 오더북 메트릭 데이터
    OB_COLLECTOR_CONNECTION = "component/ob_collector/connection" # 오더북 연결 상태
    OB_COLLECTOR_SUBSCRIPTION = "component/ob_collector/subscription" # 구독 상태
    OB_COLLECTOR_ERROR = "component/ob_collector/error"          # 오더북 오류 발생
    OB_COLLECTOR_MESSAGE = "component/ob_collector/message"      # 메시지 수신 통계
    
    ###################################
    # 컴포넌트별 이벤트 (텔레그램)
    ###################################
    # 텔레그램 관련 이벤트
    TELEGRAM = "component/telegram"
    
    # 텔레그램 커맨더 이벤트 - 사용자 명령 수신 및 처리 관련
    TELEGRAM_COMMANDER = "component/telegram/commander"
    TELEGRAM_COMMANDER_START = "component/telegram/commander/start"        # 커맨더 시작됨
    TELEGRAM_COMMANDER_STOP = "component/telegram/commander/stop"          # 커맨더 중지됨
    TELEGRAM_COMMANDER_RECEIVE = "component/telegram/commander/receive"    # 메시지 수신됨
    TELEGRAM_COMMANDER_PARSE = "component/telegram/commander/parse"        # 명령 해석됨
    TELEGRAM_COMMANDER_EXECUTE = "component/telegram/commander/execute"    # 명령 실행됨
    
    # 텔레그램 노티파이어 이벤트 - 알림 처리 관련
    TELEGRAM_NOTIFY = "component/telegram/notifier"
    TELEGRAM_NOTIFY_START = "component/telegram/notifier/start"          # 노티파이어 시작됨
    TELEGRAM_NOTIFY_STOP = "component/telegram/notifier/stop"            # 노티파이어 중지됨
    TELEGRAM_NOTIFY_SEND = "component/telegram/notifier/send"            # 알림 전송됨
    TELEGRAM_NOTIFY_SYSTEM = "component/telegram/notifier/system"        # 시스템 알림
    TELEGRAM_NOTIFY_PROCESS = "component/telegram/notifier/process"      # 프로세스 알림
    TELEGRAM_NOTIFY_ERROR = "component/telegram/notifier/error"          # 오류 알림
    TELEGRAM_NOTIFY_TRADE = "component/telegram/notifier/trade"          # 거래 알림
    
    ###################################
    # 컴포넌트별 이벤트 (레이더)
    ###################################
    # 레이더 이벤트
    RADAR = "component/radar"
    RADAR_START = "component/radar/start"                        # 레이더 시작됨
    RADAR_STOP = "component/radar/stop"                          # 레이더 중지됨
    RADAR_SIGNAL = "component/radar/signal"                      # 레이더 신호 발생
    RADAR_METRICS = "component/radar/metrics"                    # 레이더 메트릭 데이터
    
    ###################################
    # 컴포넌트별 이벤트 (트레이더)
    ###################################
    # 트레이더 이벤트
    TRADER = "component/trader"
    TRADER_START = "component/trader/start"                      # 트레이더 시작됨
    TRADER_STOP = "component/trader/stop"                        # 트레이더 중지됨
    TRADER_ORDER_CREATED = "component/trader/order/created"      # 주문 생성됨
    TRADER_ORDER_FILLED = "component/trader/order/filled"        # 주문 체결됨
    TRADER_ORDER_CANCELED = "component/trader/order/canceled"    # 주문 취소됨
    TRADER_TRADE_COMPLETED = "component/trader/trade/completed"  # 거래 완료됨
    
    ###################################
    # 성능 모니터링 이벤트
    ###################################
    # 성능 모니터링 관련 이벤트
    PERFORMANCE = "performance"
    PERFORMANCE_CPU = "performance/cpu"                          # CPU 사용률
    PERFORMANCE_MEMORY = "performance/memory"                    # 메모리 사용량
    PERFORMANCE_LATENCY = "performance/latency"                  # 지연시간
    PERFORMANCE_QUEUE = "performance/queue"                      # 큐 크기

    ###################################
    # 웹서버 이벤트
    ###################################
    # 웹서버 관련 이벤트
    WEB_SERVER = "component/web_server"
    WEB_SERVER_START = "component/web_server/start"              # 웹서버 시작됨
    WEB_SERVER_STOP = "component/web_server/stop"                # 웹서버 중지됨
    WEB_SERVER_REQUEST = "component/web_server/request"          # 웹 요청 발생

####################################
# 이벤트 우선순위
####################################
class EventPriority(Enum):
    """
    이벤트 처리 우선순위
    
    이벤트 버스에서 이벤트를 처리할 때의 우선순위를 정의합니다.
    높은 우선순위 이벤트가 먼저 처리됩니다.
    """
    LOW = 0         # 낮은 우선순위 - 일반 로깅, 메트릭 등 중요도가 낮은 이벤트
    NORMAL = 1      # 일반 우선순위 - 대부분의 일반적인 이벤트
    HIGH = 2        # 높은 우선순위 - 중요한 상태 변경, 알림 등
    CRITICAL = 3    # 중요 우선순위 - 시스템 오류, 중요 경고 등 즉시 처리해야 하는 이벤트

