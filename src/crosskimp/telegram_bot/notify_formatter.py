"""
텔레그램 알림 포맷팅 모듈

이벤트 타입별로 텔레그램 알림 메시지를 포맷팅하는 기능을 제공합니다.
표 형식의 알림 메시지를 생성하고, 프로세스 이름 등을 한글로 표시합니다.
또한 알림 레벨, 이모지 등 공통 상수도 함께 정의합니다.
"""

from typing import Dict, Any, Tuple, Optional
from datetime import datetime
import time

from crosskimp.common.events.system_types import EventChannels, EventValues
from crosskimp.common.config.common_constants import SystemComponent, COMPONENT_NAMES_KR, EXCHANGE_NAMES_KR
from crosskimp.common.logger.logger import get_unified_logger

# 로거 설정
logger = get_unified_logger(component=SystemComponent.TELEGRAM.value)

# 알림 레벨
class NotificationLevel:
    """알림 레벨 열거형"""
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"

def get_status_emoji(level: str) -> str:
    """알림 레벨에 따른 상태 이모지 반환"""
    emoji_map = {
        NotificationLevel.INFO: "🔵",
        NotificationLevel.SUCCESS: "🟢",
        NotificationLevel.WARNING: "🟠",
        NotificationLevel.ERROR: "🔴"
    }
    return emoji_map.get(level, "🔵")

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

# 거래소 상태 알림 관련 상수
EXCHANGE_STATUS_TYPES = {
    "all_connected": {
        "emoji": "✅",
        "subtitle": "모든 거래소 연결 완료"
    },
    "disconnected": {
        "emoji": "⚠️",
        "subtitle": "일부 거래소 연결 끊김"
    },
    "reconnected": {
        "emoji": "🔄",
        "subtitle": "일부 거래소 재연결됨"
    }
}

def format_timestamp(timestamp: float) -> str:
    """타임스탬프를 HH:MM:SS 형식으로 변환"""
    return datetime.fromtimestamp(timestamp).strftime("%H시 %M분 %S초")

def format_event(data: Dict[str, Any]) -> Tuple[Optional[str], str]:
    """
    이벤트 데이터를 받아 포맷팅된 알림 메시지와 레벨을 반환합니다.
    
    Args:
        data: 이벤트 데이터
        
    Returns:
        Tuple[Optional[str], str]: (포맷팅된 메시지, 알림 레벨)
        메시지가 None이면 알림을 보내지 않아도 됨을 의미합니다.
    """
    logger.info(f"[디버깅] format_event 호출됨, 이벤트 경로: {data.get('_event_path', 'unknown')}")
    
    event_path = data.get("_event_path", "unknown")
    process_name = data.get("process_name", "")
    timestamp = data.get("timestamp", time.time())
    datetime_str = format_timestamp(timestamp)
    level = NotificationLevel.INFO
    message = None
    
    # 이벤트 경로에 따라 적절한 포맷팅 수행
    # 1. 시스템 오류 이벤트
    if event_path == EventChannels.System.ERROR:
        error_message = data.get("error_message", "알 수 없는 오류")
        level = NotificationLevel.ERROR
        
        message = (
            f"{get_status_emoji(level)} <b>시스템 오류</b>\n"
            f"━━━━━━━━\n"
            f"시간: {datetime_str}\n"
            f"오류: {error_message}"
        )
    
    # 2. 프로세스 명령 이벤트
    elif event_path in [EventChannels.Process.COMMAND_START, 
                       EventChannels.Process.COMMAND_STOP, 
                       EventChannels.Process.COMMAND_RESTART]:
        
        display_name = get_process_display_name(process_name)
        
        # 명령 타입에 따른 제목과 유형 결정
        if event_path == EventChannels.Process.COMMAND_START:
            title = "프로세스 시작 요청"
            command_type = data.get("event_type", "process/start_requested")
            if "manual" in command_type:
                type_str = "수동 시작"
            elif "restart" in command_type:
                type_str = "재시작"
            else:
                type_str = "자동 시작"
                
        elif event_path == EventChannels.Process.COMMAND_STOP:
            title = "프로세스 중지 요청"
            command_type = data.get("event_type", "process/stop_requested")
            type_str = "수동 중지" if "manual" in command_type else "자동 중지"
            
        elif event_path == EventChannels.Process.COMMAND_RESTART:
            title = "프로세스 재시작 요청"
            command_type = data.get("event_type", "process/restart_requested")
            type_str = "수동 재시작" if "manual" in command_type else "자동 재시작"
        
        # 메시지 구성
        message = (
            f"{get_status_emoji(level)} <b>{title}</b>\n"
            f"━━━━━━━━\n"
            f"이름: {display_name}\n"
            f"시간: {datetime_str}\n"
            f"유형: {type_str}"
        )
    
    # 3. 프로세스 상태 이벤트 
    elif event_path == EventChannels.Process.STATUS:
        status = data.get("status", "")
        error_message = data.get("error_message", "")
        display_name = get_process_display_name(process_name)
        
        # 상태에 따른 제목과 레벨 결정
        if status == EventValues.PROCESS_RUNNING:
            # PROCESS_RUNNING 상태인 경우 알림을 발송하지 않음
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
            # 시작 중 상태는 알림 발송하지 않음
            return None, NotificationLevel.INFO
        elif status == EventValues.PROCESS_STOPPING:
            # 종료 중 상태는 알림 발송하지 않음
            return None, NotificationLevel.INFO
        else:
            title = "프로세스 상태 변경"
            status_str = status
        
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
    
    # 4. 오더북 수집기 실행 완료 이벤트
    elif event_path == EventChannels.Component.ObCollector.RUNNING:
        level = NotificationLevel.SUCCESS
        
        # 이미 포맷팅된 메시지가 있으면 그대로 사용
        pre_message = data.get("message", "")
        if pre_message:
            formatted_dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
            message = f"<b>✅ 웹소켓 연결</b>\n📡 거래소: 모두 연결 성공 | ⏱️ {formatted_dt}\n━━━━━━━━━━━━━━\n{pre_message}"
            return message, level
        
        # 거래소 정보가 details에 있는 경우
        exchanges_details = data.get("details", {}).get("exchanges", [])
        connected_count = data.get("details", {}).get("connected_count", len(exchanges_details))
        total_count = data.get("details", {}).get("total_count", len(exchanges_details))
        
        # exchanges 리스트에서 거래소 이름 추출
        exchanges = data.get("exchanges", [])
        highlight_exchanges = exchanges if exchanges else [ex.get("name", "") for ex in exchanges_details if ex.get("connected", False)]
        
        # 거래소 상태 메시지 생성
        message, level = _format_exchange_status(
            timestamp,
            connected_count,
            total_count,
            exchanges_details,
            highlight_exchanges,
            "all_connected",
            level
        )
    
    # 5. 오더북 수집기 연결 끊김 이벤트
    elif event_path == EventChannels.Component.ObCollector.CONNECTION_LOST:
        exchange = data.get("exchange", "알 수 없음")
        reason = data.get("reason", "알 수 없는 이유")
        level = NotificationLevel.WARNING
        
        # 거래소 이름 확인
        if not exchange or exchange == "알 수 없음":
            # 간단한 메시지로 처리
            message = (
                f"{get_status_emoji(level)} <b>거래소 연결 끊김</b>\n"
                f"━━━━━━━━\n"
                f"시간: {datetime_str}\n"
                f"이유: {reason}"
            )
        else:
            # 해당 거래소 정보만으로 가상의 거래소 상태 정보 생성
            exchange_info = [{
                "name": exchange,
                "display_name": EXCHANGE_NAMES_KR.get(exchange, exchange),
                "connected": False
            }]
            
            # 이유 정보 추가
            if not data.get("details"):
                data["details"] = {"connection_lost_reason": reason}
            
            # 거래소 상태 메시지 생성
            message, level = _format_exchange_status(
                timestamp,
                0,  # 연결된 거래소 없음
                1,  # 총 1개 거래소에 대한 정보
                exchange_info,
                [exchange],  # 강조 표시할 거래소
                "disconnected",
                level
            )
    
    # 5.1 특별: 거래소 상태 이벤트 (event_subscriber에서 생성된 통합 이벤트)
    elif event_path == "exchange/status":
        # 통합 이벤트에서 필요한 정보 추출
        connected_count = data.get("connected_count", 0)
        total_count = data.get("total_count", 0)
        all_exchanges_info = data.get("details", {}).get("exchanges", [])
        highlight_exchanges = data.get("highlight_exchanges", [])
        highlight_type = data.get("highlight_type", "")
        
        # 알림 레벨 결정
        if highlight_type == "disconnected":
            level = NotificationLevel.WARNING
        else:
            level = NotificationLevel.SUCCESS
            
        # 거래소 상태 메시지 생성
        message, level = _format_exchange_status(
            timestamp,
            connected_count,
            total_count,
            all_exchanges_info,
            highlight_exchanges,
            highlight_type,
            level
        )
    
    # 6. 시스템 상태 이벤트
    elif event_path == EventChannels.System.STATUS:
        pre_message = data.get("message", "")
        
        if pre_message:
            # 이미 포맷팅된 메시지가 있더라도 새 형식에 맞게 조정
            message = f"{get_status_emoji(level)} <b>시스템 상태</b>\n━━━━━━━━\n{pre_message}"
        else:
            # 포맷팅된 메시지가 없으면 기본 포맷팅
            processes = data.get("processes", {})
            
            process_list = ""
            for proc_name, status in processes.items():
                disp_name = get_process_display_name(proc_name)
                is_running = status.get("running", False)
                status_str = "✅ 실행 중" if is_running else "⚫ 중지됨"
                process_list += f"- {disp_name}: {status_str}\n"
            
            # 메시지 구성
            message = (
                f"{get_status_emoji(level)} <b>시스템 상태</b>\n"
                f"━━━━━━━━\n"
                f"시간: {datetime_str}\n"
                f"프로세스 상태:\n{process_list}"
            )
    
    # 7. 매핑된 포맷터가 없는 경우 기본 포맷팅
    else:
        logger.warning(f"[디버깅] 이벤트 {event_path}를 위한 포맷터 없음, 기본 포맷팅 사용")
        
        pre_message = data.get("message", "")
        display_name = get_process_display_name(process_name) if process_name else ""
        
        if not pre_message:
            pre_message = f"이벤트: {event_path}"
            if display_name:
                pre_message += f" | 프로세스: {display_name}"
        
        # 기본 메시지 구성
        message = (
            f"{get_status_emoji(level)} <b>시스템 이벤트</b>\n"
            f"━━━━━━━━\n"
            f"시간: {datetime_str}\n"
            f"내용: {pre_message}"
        )
    
    # 결과 반환
    if message is not None:
        logger.info(f"[디버깅] 포맷터에서 메시지 생성됨, 레벨: {level}")
        return message, level
    else:
        logger.info(f"[디버깅] 포맷터에서 None 반환됨, 알림 발송 건너뜀")
        return None, level

def _format_exchange_status(timestamp, connected_count, total_count, 
                          all_exchanges_info, highlight_exchanges, highlight_type, level):
    """
    거래소 상태 메시지 포맷팅 (내부 헬퍼 함수)
    """
    try:
        # 로깅 추가 - 전달된 데이터 확인
        logger.info(f"_format_exchange_status 호출됨 - exchanges_info={len(all_exchanges_info) if all_exchanges_info else 0}, highlights={highlight_exchanges}")
        
        # 타임스탬프 포맷팅
        datetime_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
        
        # 제목 및 요약 정보
        message = f"<b>📡 거래소 연결 상태</b>\n"
        message += f"{datetime_str}\n"
        message += f"━━━━━━━━━\n\n"
        
        # 거래소 정보 포맷팅
        if all_exchanges_info and len(all_exchanges_info) > 0:
            # 세부 정보를 로깅
            logger.info(f"거래소 상태 정보: {all_exchanges_info}")
            
            # 이름순으로 정렬
            sorted_exchanges = sorted(all_exchanges_info, 
                                     key=lambda ex: ex.get("display_name", ""))
            
            for exchange_info in sorted_exchanges:
                # 기본 정보 추출
                exchange_name = exchange_info.get("display_name", "")
                exchange_code = exchange_info.get("name", "")
                is_connected = exchange_info.get("connected", False)
                symbols_count = exchange_info.get("subscribed_symbols_count", 0)
                uptime = exchange_info.get("uptime_formatted", "0분")
                
                # EXCHANGE_NAMES_KR에서 이름을 가져온 경우에는 대괄호를 제거
                # 아닌 경우 그대로 사용 (이미 정규화되어 있을 수 있음)
                if exchange_name.startswith("[") and exchange_name.endswith("]"):
                    exchange_name = exchange_name[1:-1]  # 양쪽 대괄호 제거
                
                # 상태 이모지
                status_emoji = "🟢" if is_connected else "🔴"
                
                # 강조 표시 여부 확인
                if exchange_code in highlight_exchanges:
                    if (highlight_type == "disconnected" and not is_connected) or \
                       (highlight_type in ["all_connected", "reconnected"] and is_connected):
                        exchange_name = f"<b>{exchange_name}</b>"
                
                # 심볼 정보 문자열 생성
                symbol_info = f"-> {symbols_count}개" if symbols_count > 0 else ""
                
                # 거래소 정보 추가 (대괄호는 한 번만 사용)
                message += f"{status_emoji} [{exchange_name}] {symbol_info}\n"
                
                # 업타임 정보만 표시
                if uptime and uptime != "0분":
                    message += f"   ⏱️ 업타임: {uptime}\n\n"
                else:
                    message += f"   ⏱️ 업타임: 정보 없음\n\n"
        else:
            # 거래소 상세 정보가 없는 경우에도 기본 형식 유지
            logger.warning("거래소 상세 정보가 없습니다. 기본 형식으로 표시합니다.")
            
            for exchange in highlight_exchanges:
                # EXCHANGE_NAMES_KR에서 이름 가져오기
                exchange_name = EXCHANGE_NAMES_KR.get(exchange, exchange)
                
                # 대괄호가 있는 경우 제거
                if exchange_name.startswith("[") and exchange_name.endswith("]"):
                    exchange_name = exchange_name[1:-1]  # 양쪽 대괄호 제거
                
                status_emoji = "🔴" if highlight_type == "disconnected" else "🟢"
                
                # 기본 정보 표시 - 심볼 정보는 없음
                message += f"{status_emoji} [{exchange_name}]\n"
                message += f"   ⏱️ 업타임: 정보 없음\n\n"
                    
        return message, level
        
    except Exception as e:
        logger.error(f"통합 상태 알림 포맷팅 중 오류: {str(e)}", exc_info=True)
        return f"<b>거래소 연결 상태</b>\n오류: 포맷팅 실패", level 