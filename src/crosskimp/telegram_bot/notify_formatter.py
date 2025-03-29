"""
텔레그램 알림 포맷팅 모듈

이벤트 타입별로 텔레그램 알림 메시지를 포맷팅하는 기능을 제공합니다.
표 형식의 알림 메시지를 생성하고, 프로세스 이름 등을 한글로 표시합니다.
"""

from typing import Dict, Any, Tuple, Optional
from datetime import datetime
import time

from crosskimp.common.events.system_types import EventChannels, EventValues
from crosskimp.common.config.common_constants import SystemComponent, COMPONENT_NAMES_KR, EXCHANGE_NAMES_KR
from crosskimp.common.logger.logger import get_unified_logger

# 알림 레벨
class NotificationLevel:
    """알림 레벨 열거형"""
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"

# 프로세스 이름을 한글 표시명으로 변환하는 매핑 테이블
PROCESS_DISPLAY_NAMES = {
    "ob_collector": "오더북 수집기",
    "radar": "레이더",
    "trader": "트레이더",
    "web_server": "웹 서버"
}

def get_process_display_name(process_name: str) -> str:
    """프로세스 코드명을 사용자 친화적인 한글 표시명으로 변환"""
    return PROCESS_DISPLAY_NAMES.get(process_name, process_name)

def format_timestamp(timestamp: float) -> str:
    """타임스탬프를 HH:MM:SS 형식으로 변환"""
    return datetime.fromtimestamp(timestamp).strftime("%H시 %M분 %S초")

def get_status_emoji(level: str) -> str:
    """알림 레벨에 따른 상태 이모지 반환"""
    emoji_map = {
        NotificationLevel.INFO: "🔵",
        NotificationLevel.SUCCESS: "🟢",
        NotificationLevel.WARNING: "🟠",
        NotificationLevel.ERROR: "🔴"
    }
    return emoji_map.get(level, "🔵")

def add_level_prefix(message: str, level: str) -> str:
    """알림 레벨에 맞는 이모지 접두어 추가 - 모바일 가독성 향상 버전"""
    # 새 포맷에서는 메시지 첫 줄에 상태 이모지가 포함되어 있으므로 별도 접두어 추가 안함
    return message

def format_system_error(data: Dict[str, Any]) -> Tuple[str, str]:
    """시스템 오류 이벤트 포맷팅"""
    error_message = data.get("error_message", "알 수 없는 오류")
    timestamp = data.get("timestamp", time.time())
    datetime_str = format_timestamp(timestamp)
    level = NotificationLevel.ERROR
    
    message = (
        f"{get_status_emoji(level)} <b>시스템 오류</b>\n"
        f"━━━━━━━━\n"
        f"시간: {datetime_str}\n"
        f"오류: {error_message}"
    )
    
    return message, level

def format_process_command_start(data: Dict[str, Any]) -> Tuple[str, str]:
    """프로세스 시작 명령 이벤트 포맷팅"""
    logger = get_unified_logger(component=SystemComponent.TELEGRAM.value)
    
    logger.info(f"[디버깅] format_process_command_start 호출됨, 데이터: {data}")
    
    process_name = data.get("process_name", "")
    timestamp = data.get("timestamp", time.time())
    datetime_str = format_timestamp(timestamp)
    level = NotificationLevel.INFO
    
    # 프로세스 한글 이름 가져오기
    display_name = get_process_display_name(process_name)
    
    # 시작 유형 확인 (기본값은 "자동 시작")
    start_type = data.get("event_type", "process/start_requested")
    if "manual" in start_type:
        type_str = "수동 시작"
    elif "restart" in start_type:
        type_str = "재시작"
    else:
        type_str = "자동 시작"
    
    message = (
        f"{get_status_emoji(level)} <b>프로세스 시작 요청</b>\n"
        f"━━━━━━━━\n"
        f"이름: {display_name}\n"
        f"시간: {datetime_str}\n"
        f"유형: {type_str}"
    )
    
    logger.info(f"[디버깅] format_process_command_start 메시지 생성 완료: {message[:30]}...")
    return message, level

def format_process_command_stop(data: Dict[str, Any]) -> Tuple[str, str]:
    """프로세스 중지 명령 이벤트 포맷팅"""
    process_name = data.get("process_name", "")
    timestamp = data.get("timestamp", time.time())
    datetime_str = format_timestamp(timestamp)
    level = NotificationLevel.INFO
    
    # 프로세스 한글 이름 가져오기
    display_name = get_process_display_name(process_name)
    
    # 중지 유형 확인
    stop_type = data.get("event_type", "process/stop_requested")
    if "manual" in stop_type:
        type_str = "수동 중지"
    else:
        type_str = "자동 중지"
    
    message = (
        f"{get_status_emoji(level)} <b>프로세스 중지 요청</b>\n"
        f"━━━━━━━━\n"
        f"이름: {display_name}\n"
        f"시간: {datetime_str}\n"
        f"유형: {type_str}"
    )
    
    return message, level

def format_process_command_restart(data: Dict[str, Any]) -> Tuple[str, str]:
    """프로세스 재시작 명령 이벤트 포맷팅"""
    process_name = data.get("process_name", "")
    timestamp = data.get("timestamp", time.time())
    datetime_str = format_timestamp(timestamp)
    level = NotificationLevel.INFO
    
    # 프로세스 한글 이름 가져오기
    display_name = get_process_display_name(process_name)
    
    # 재시작 유형 확인
    restart_type = data.get("event_type", "process/restart_requested")
    if "manual" in restart_type:
        type_str = "수동 재시작"
    else:
        type_str = "자동 재시작"
    
    message = (
        f"{get_status_emoji(level)} <b>프로세스 재시작 요청</b>\n"
        f"━━━━━━━━\n"
        f"이름: {display_name}\n"
        f"시간: {datetime_str}\n"
        f"유형: {type_str}"
    )
    
    return message, level

def format_process_status(data: Dict[str, Any]) -> Tuple[Optional[str], str]:
    """프로세스 상태 이벤트 포맷팅"""
    process_name = data.get("process_name", "")
    status = data.get("status", "")
    timestamp = data.get("timestamp", time.time())
    datetime_str = format_timestamp(timestamp)
    error_message = data.get("error_message", "")
    
    # 프로세스 한글 이름 가져오기
    display_name = get_process_display_name(process_name)
    
    # 상태에 따른 제목과 레벨 결정
    if status == EventValues.PROCESS_RUNNING:
        # PROCESS_RUNNING 상태인 경우 알림을 발송하지 않음
        # 특화 이벤트만 발송하도록 None 반환
        level = NotificationLevel.SUCCESS
        return None, level
    elif status == EventValues.PROCESS_STOPPED:
        title = "프로세스 중지 완료"
        status_str = "중지됨"
        level = NotificationLevel.INFO
    elif status == EventValues.PROCESS_ERROR:
        title = "프로세스 오류 발생"
        status_str = "오류"
        level = NotificationLevel.ERROR
    elif status == EventValues.PROCESS_STARTING:
        title = "프로세스 시작 중"
        status_str = "시작 중"
        level = NotificationLevel.INFO
        # 시작 중, 종료 중 상태는 특별한 경우가 아니면 알림 발송하지 않을 수 있음
        return None, level
    elif status == EventValues.PROCESS_STOPPING:
        title = "프로세스 종료 중"
        status_str = "종료 중"
        level = NotificationLevel.INFO
        # 시작 중, 종료 중 상태는 특별한 경우가 아니면 알림 발송하지 않을 수 있음
        return None, level
    else:
        title = "프로세스 상태 변경"
        status_str = status
        level = NotificationLevel.INFO
    
    # 메시지 구성
    message = (
        f"{get_status_emoji(level)} <b>{title}</b>\n"
        f"━━━━━━━━\n"
        f"이름: {display_name}\n"
        f"시간: {datetime_str}\n"
        f"상태: {status_str}"
    )
    
    # 오류 메시지가 있으면 추가
    if error_message and status == EventValues.PROCESS_ERROR:
        message += f"\n오류: {error_message}"
    
    return message, level

def format_ob_collector_running(data: Dict[str, Any]) -> Tuple[str, str]:
    """오더북 수집기 실행 완료 이벤트 포맷팅"""
    timestamp = data.get("timestamp", time.time())
    datetime_str = format_timestamp(timestamp)
    exchanges = data.get("exchanges", [])
    level = NotificationLevel.SUCCESS
    
    # 이미 포맷팅된 메시지가 있으면 그대로 사용
    message = data.get("message", "")
    if message:
        return f"<b>✅ 웹소켓 연결</b>\n📡 거래소: 모두 연결 성공 | ⏱️ {datetime_str}\n━━━━━━━━━━━━━━\n{message}", level
    
    # 거래소 정보가 details에 있는 경우 (더 상세한 정보)
    exchanges_details = data.get("details", {}).get("exchanges", [])
    
    # 거래소 연결 상태 요약
    connected_count = data.get("details", {}).get("connected_count", len(exchanges_details))
    total_count = data.get("details", {}).get("total_count", len(exchanges_details))
    
    # 헤더
    message = f"<b>✅ 웹소켓 연결</b>\n"
    message += f"📡 거래소: {connected_count}/{total_count} 연결 | ⏱️ {datetime_str}\n"
    message += f"━━━━━━━━━━━━━━\n\n"
    
    # 거래소 상세 정보가 있는 경우
    if exchanges_details:
        # 요청한 순서대로 거래소 정렬 (바이낸스 현물, 선물, 바이빗 현물, 선물, 업비트, 빗썸)
        exchange_order = {
            "binance_spot": 1,
            "binance_futures": 2,
            "bybit_spot": 3,
            "bybit_futures": 4,
            "upbit": 5,
            "bithumb": 6
        }
        
        # 거래소 정렬 함수
        def sort_exchanges(exchange):
            exchange_name = exchange.get("name", "")
            return exchange_order.get(exchange_name, 99)  # 알 수 없는 거래소는 맨 뒤로
        
        # 거래소별 상태 추가
        for ex in sorted(exchanges_details, key=sort_exchanges):
            # 거래소명
            exchange_name = ex.get("display_name", ex.get("name", ""))
            # 연결 상태
            is_connected = ex.get("connected", False)
            status_emoji = "🟢" if is_connected else "🔴"
            
            # 구독 심볼 수
            symbols_count = ex.get("subscribed_symbols_count", 0)
            symbols_info = f"{symbols_count}개 심볼" if symbols_count > 0 else "0개 심볼"
            
            # 업타임 - 업타임 값이 없으면 "0분 0초"로 표시
            uptime = ex.get("uptime_formatted", "0분 0초")
            
            # 메시지 구성 - 항상 업타임 표시
            message += f"{status_emoji} <b>{exchange_name}</b> ⟶ {symbols_info} | {uptime}\n"
    
    # 거래소 상세 정보가 없고 단순 목록만 있는 경우
    elif exchanges:
        for exchange in exchanges:
            exchange_name = EXCHANGE_NAMES_KR.get(exchange, exchange)
            message += f"🟢 <b>{exchange_name}</b>\n"
    else:
        message += "연결된 거래소 없음\n"
    
    return message, level

def format_ob_collector_connection_lost(data: Dict[str, Any]) -> Tuple[str, str]:
    """오더북 수집기 연결 끊김 이벤트 포맷팅"""
    timestamp = data.get("timestamp", time.time())
    datetime_str = format_timestamp(timestamp)
    exchange = data.get("exchange", "알 수 없음")
    reason = data.get("reason", "알 수 없는 이유")
    level = NotificationLevel.WARNING
    
    # 거래소 이름 포맷팅
    exchange_name = EXCHANGE_NAMES_KR.get(exchange, exchange)
    
    # 메시지 구성
    message = (
        f"{get_status_emoji(level)} <b>거래소 연결 끊김</b>\n"
        f"━━━━━━━━\n"
        f"시간: {datetime_str}\n"
        f"거래소: {exchange_name}\n"
        f"이유: {reason}"
    )
    
    return message, level

def format_system_status(data: Dict[str, Any]) -> Tuple[str, str]:
    """시스템 상태 이벤트 포맷팅"""
    # 이미 포맷팅된 메시지가 있으면 그대로 사용
    message = data.get("message", "")
    level = NotificationLevel.INFO
    
    if message:
        # 이미 포맷팅된 메시지가 있더라도 새 형식에 맞게 조정
        return f"{get_status_emoji(level)} <b>시스템 상태</b>\n━━━━━━━━\n{message}", level
    
    # 포맷팅된 메시지가 없으면 기본 포맷팅
    timestamp = data.get("timestamp", time.time())
    datetime_str = format_timestamp(timestamp)
    processes = data.get("processes", {})
    
    process_list = ""
    for process_name, status in processes.items():
        display_name = get_process_display_name(process_name)
        is_running = status.get("running", False)
        status_str = "✅ 실행 중" if is_running else "⚫ 중지됨"
        process_list += f"- {display_name}: {status_str}\n"
    
    # 메시지 구성
    message = (
        f"{get_status_emoji(level)} <b>시스템 상태</b>\n"
        f"━━━━━━━━\n"
        f"시간: {datetime_str}\n"
        f"프로세스 상태:\n{process_list}"
    )
    
    return message, level

def format_event(data: Dict[str, Any]) -> Tuple[Optional[str], str]:
    """
    이벤트 데이터를 받아 포맷팅된 알림 메시지와 레벨을 반환합니다.
    
    Args:
        data: 이벤트 데이터
        
    Returns:
        Tuple[Optional[str], str]: (포맷팅된 메시지, 알림 레벨)
        메시지가 None이면 알림을 보내지 않아도 됨을 의미합니다.
    """
    logger = get_unified_logger(component=SystemComponent.TELEGRAM.value)
    
    event_path = data.get("_event_path", "unknown")
    logger.info(f"[디버깅] format_event 호출됨, 이벤트 경로: {event_path}")
    
    # 이벤트 경로에 따라 적절한 포맷팅 함수 호출
    formatters = {
        EventChannels.System.ERROR: format_system_error,
        EventChannels.System.STATUS: format_system_status,
        EventChannels.Process.COMMAND_START: format_process_command_start,
        EventChannels.Process.COMMAND_STOP: format_process_command_stop,
        EventChannels.Process.COMMAND_RESTART: format_process_command_restart,
        EventChannels.Process.STATUS: format_process_status,
        EventChannels.Component.ObCollector.RUNNING: format_ob_collector_running,
        EventChannels.Component.ObCollector.CONNECTION_LOST: format_ob_collector_connection_lost
    }
    
    formatter = formatters.get(event_path)
    if formatter:
        logger.info(f"[디버깅] 이벤트 {event_path}를 위한 포맷터 찾음: {formatter.__name__}")
        message, level = formatter(data)
        # 메시지가 None이 아닌 경우에만 접두어 추가
        if message is not None:
            message = add_level_prefix(message, level)
            logger.info(f"[디버깅] 포맷터에서 메시지 생성됨, 레벨: {level}")
        else:
            logger.info(f"[디버깅] 포맷터에서 None 반환됨, 알림 발송 건너뜀")
        return message, level
    
    logger.warning(f"[디버깅] 이벤트 {event_path}를 위한 포맷터 없음, 기본 포맷팅 사용")
    
    # 기본 포맷팅 (매핑된 포맷터가 없는 경우)
    process_name = data.get("process_name", "")
    message = data.get("message", "")
    timestamp = data.get("timestamp", time.time())
    datetime_str = format_timestamp(timestamp)
    level = NotificationLevel.INFO
    
    if process_name:
        display_name = get_process_display_name(process_name)
    else:
        display_name = ""
    
    if not message:
        message = f"이벤트: {event_path}"
        if display_name:
            message += f" | 프로세스: {display_name}"
    
    # 기본 메시지 구성
    formatted_message = (
        f"{get_status_emoji(level)} <b>시스템 이벤트</b>\n"
        f"━━━━━━━━\n"
        f"시간: {datetime_str}\n"
        f"내용: {message}"
    )
    
    formatted_message = add_level_prefix(formatted_message, level)
    logger.info(f"[디버깅] 기본 포맷팅으로 메시지 생성됨: {formatted_message[:30]}...")
    return formatted_message, level 