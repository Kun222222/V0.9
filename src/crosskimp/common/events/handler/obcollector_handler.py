"""
오더북 프로세스 핸들러

ProcessComponent를 상속받아 오더북 프로세스의 시작/종료를 관리합니다.
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List, Set
from datetime import datetime, timedelta

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.handler.process_component import ProcessComponent
from crosskimp.common.config.common_constants import SystemComponent, EXCHANGE_NAMES_KR
from crosskimp.common.events.system_types import EventPaths
from crosskimp.ob_collector.metric.reporter import ObcMetricReporter

# 오더북 수집기 임포트
from crosskimp.ob_collector.obcollector import OrderbookCollectorManager

# 로거 설정
logger = get_unified_logger(component=SystemComponent.OB_COLLECTOR.value)

class OrderbookProcess(ProcessComponent):
    """
    오더북 수집 프로세스
    
    오더북 수집을 위한 프로세스 컴포넌트입니다.
    ProcessComponent를 상속받아 프로세스 생명주기와 상태 관리를 담당합니다.
    기술적 작업은 OrderbookCollectorManager에 위임하고, 이 클래스는 상태 관리만 담당합니다.
    """

    def __init__(self, eventbus=None, config=None):
        """
        초기화
        
        Args:
            eventbus: 이벤트 버스 객체 (기본값: None, None일 경우 get_event_bus()로 가져옴)
            config: 설정 딕셔너리 (기본값: None, None일 경우 get_config()로 가져옴)
        """
        # 부모 클래스 초기화 (process_name과 eventbus 전달)
        super().__init__(process_name="ob_collector", event_bus=eventbus)
        
        # 설정 저장
        from crosskimp.common.config.app_config import get_config
        self.config = config if config is not None else get_config()
        
        # 데이터 수집을 위한 OrderbookCollectorManager 인스턴스 생성
        self.collector = OrderbookCollectorManager()
        
        # 메트릭 수집 관련 변수
        self.metric_interval = self.config.get("metrics", {}).get("interval", 20)  # 기본값 20초
        self.metric_reporter = None
        
        # 거래소 상태 추적용 변수
        self.exchange_uptimes = {}  # 각 거래소별 연결 시작 시간
        self.connected_exchanges = set()  # 현재 연결된 거래소
        self.all_connected = False  # 모든 거래소 연결 여부
        
        # 모니터링 태스크 참조
        self._monitoring_task = None

    async def _do_start(self) -> bool:
        """
        오더북 프로세스 시작
        
        Returns:
            bool: 시작 성공 여부
        """
        try:
            self.logger.info("오더북 프로세스 시작 중...")
            
            # 1. 초기화 필요성 확인 및 수행
            if not self.collector.initialization_complete:
                self.logger.info("오더북 수집기 초기화 시작")
                
                # 초기화 수행
                init_success = await self.collector.initialize()
                if not init_success:
                    self.logger.error("오더북 수집기 초기화 실패")
                    return False
                    
                self.logger.info("오더북 수집기 초기화 완료")
            else:
                self.logger.info("오더북 수집기가 이미 초기화되어 있음")
            
            # 2. 기술적 작업 시작
            self.logger.info("오더북 수집기 시작 요청")
            success = await self.collector.start()
            
            if not success:
                self.logger.error("오더북 수집기 시작 실패")
                return False
                
            # 3. 메트릭 수집 시작 (백그라운드에서, 실패해도 계속 진행)
            try:
                self.logger.info("메트릭 수집 시작")
                self.metric_reporter = ObcMetricReporter(self.collector, self.metric_interval)
                await self.metric_reporter.start()
            except Exception as e:
                self.logger.error(f"메트릭 수집 시작 실패: {str(e)}")
                # 메트릭 실패는 치명적이지 않으므로 계속 진행
            
            # 4. 상태 모니터링 태스크 시작 (백그라운드)
            self._monitoring_task = asyncio.create_task(self._monitor_collector_status())
            self.add_task(self._monitoring_task)  # 부모 클래스의 태스크 추적 메서드 사용
            
            self.logger.info("오더북 프로세스가 성공적으로 시작되었습니다. (거래소 연결은 백그라운드에서 계속됩니다)")
            return True
            
        except Exception as e:
            self.logger.error(f"오더북 프로세스 시작 중 오류 발생: {str(e)}")
            return False

    def _format_uptime(self, seconds: float) -> str:
        """거래소 업타임을 가독성 있는 형태로 포맷팅"""
        if seconds < 0.1:  # 거의 0에 가까운 값일 경우
            return "방금 연결됨"
        elif seconds < 60:
            return f"{int(seconds)}초"
        elif seconds < 3600:
            return f"{int(seconds / 60)}분 {int(seconds % 60)}초"
        else:
            hours = int(seconds / 3600)
            minutes = int((seconds % 3600) / 60)
            return f"{hours}시간 {minutes}분"

    def _get_formatted_exchange_status(self, exchange_status: Dict[str, bool]) -> List[Dict[str, Any]]:
        """모든 거래소 상태 정보를 포맷팅"""
        current_time = time.time()
        exchanges_info = []
        
        # 지원하는 거래소 목록 (필터링된 거래소 목록이 있으면 사용)
        supported_exchanges = list(self.collector.filtered_symbols.keys() if hasattr(self.collector, 'filtered_symbols') else exchange_status.keys())
        
        for exchange in supported_exchanges:
            is_connected = exchange_status.get(exchange, False)
            
            # 업타임 계산
            uptime_seconds = 0
            if is_connected and exchange in self.exchange_uptimes:
                uptime_seconds = current_time - self.exchange_uptimes[exchange]
                
            # 거래소 표시명 가져오기 (한글명 있으면 사용)
            exchange_name = EXCHANGE_NAMES_KR.get(exchange, exchange)
            
            exchanges_info.append({
                "name": exchange,
                "display_name": exchange_name,
                "connected": is_connected,
                "uptime_seconds": uptime_seconds,
                "uptime_formatted": self._format_uptime(uptime_seconds) if uptime_seconds > 0 else "연결 안됨"
            })
            
        return exchanges_info

    async def _monitor_collector_status(self):
        """오더북 수집기의 상태를 모니터링하고 이벤트 발행"""
        self.logger.info("오더북 수집기 상태 모니터링 태스크 시작")
        
        # 초기 상태 저장
        prev_exchange_status = {}
        check_count = 0
        all_connected_event_sent = False
        current_time = time.time()
        
        # 상태 모니터링 루프
        while True:
            try:
                # 프로세스가 시작 중이 아니거나 이미 실행 중인 경우 (상태가 변경된 경우) 모니터링 중단
                if not self.is_starting() and not self.is_running():
                    self.logger.info("프로세스가 시작 중 또는 실행 중 상태가 아니므로 모니터링 종료")
                    break
                
                check_count += 1
                current_time = time.time()
                
                # 거래소 연결 상태 가져오기
                exchange_status = self.collector.get_exchange_status()
                
                # 거래소 연결 상태에 따라 self.exchange_uptimes 업데이트
                for exchange, is_connected in exchange_status.items():
                    # 연결된 거래소의 경우 시작 시간 기록
                    if is_connected:
                        if exchange not in self.exchange_uptimes:
                            self.logger.debug(f"거래소 '{exchange}' 첫 연결 감지, 업타임 기록 시작")
                            self.exchange_uptimes[exchange] = current_time
                        elif exchange not in self.connected_exchanges:
                            self.logger.debug(f"거래소 '{exchange}' 재연결 감지, 업타임 갱신")
                            self.exchange_uptimes[exchange] = current_time
                        self.connected_exchanges.add(exchange)
                    else:
                        # 연결 끊긴 거래소는 연결된 거래소 목록에서 제거
                        if exchange in self.connected_exchanges:
                            self.logger.debug(f"거래소 '{exchange}' 연결 끊김 감지")
                            self.connected_exchanges.discard(exchange)
                            if exchange in self.exchange_uptimes:
                                self.exchange_uptimes.pop(exchange)
                
                # 전체 거래소 상태 포맷팅
                exchanges_info = self._get_formatted_exchange_status(exchange_status)
                
                # 연결된 거래소 수와 총 거래소 수
                connected_count = sum(1 for ex in exchanges_info if ex["connected"])
                total_count = len(exchanges_info)
                
                # 모든 거래소 연결 여부 확인
                prev_all_connected = self.all_connected
                self.all_connected = (connected_count == total_count) and (total_count > 0)
                
                # 주기적 로깅 (5회마다)
                if check_count % 5 == 0:
                    self.logger.debug(f"오더북 수집기 상태: {connected_count}/{total_count} 거래소 연결됨")
                    # 업타임 상태 로깅 (디버깅용)
                    self.logger.debug(f"현재 거래소 업타임 상태: {self.exchange_uptimes}")
                
                # 거래소 상태 변경 감지
                status_changed = False
                for exchange, status in exchange_status.items():
                    if exchange not in prev_exchange_status or prev_exchange_status[exchange] != status:
                        status_changed = True
                        break
                        
                # 연결 상태 변경 또는 모든 거래소 처음 연결된 경우 이벤트 발행
                if status_changed or (self.all_connected and not prev_all_connected):
                    # 상태 메시지 생성
                    status_message = self._create_status_message(exchanges_info, connected_count, total_count)
                    
                    # 이벤트 데이터 준비
                    event_data = {
                        "process_name": self.process_name,
                        "message": status_message,
                        "details": {
                            "exchanges": exchanges_info,
                            "connected_count": connected_count,
                            "total_count": total_count,
                            "all_connected": self.all_connected
                        }
                    }
                    
                    # 연결 상태에 따라 다른 이벤트 발행
                    if self.all_connected and not all_connected_event_sent:
                        # 모든 거래소 연결 완료 - 컴포넌트 이벤트 발행 후 프로세스 상태 변경
                        all_connected_event_sent = True
                        
                        # 헤더 메시지 추가
                        event_data["message"] = f"🚀 오더북 수집기 구동 완료\n{status_message}"
                        
                        # 1. 컴포넌트 이벤트 발행 (오더북 수집기 특화 이벤트)
                        await self.event_bus.publish(EventPaths.OB_COLLECTOR_RUNNING, event_data)
                        
                        # 2. 프로세스 상태 변경 (프로세스 생명주기 이벤트)
                        await self._publish_status(EventPaths.PROCESS_STATUS_RUNNING)
                        
                        self.logger.info("모든 거래소 연결 완료, 프로세스 상태를 RUNNING으로 변경")
                        
                    elif prev_all_connected and not self.all_connected:
                        # 연결 끊김 이벤트 (컴포넌트 특화 이벤트)
                        event_data["message"] = f"⚠️ 오더북 수집기 연결 끊김\n{status_message}"
                        await self.event_bus.publish(EventPaths.OB_COLLECTOR_CONNECTION_LOST, event_data)
                        self.logger.info("거래소 연결 끊김 이벤트 발행됨")
                        
                    elif status_changed:
                        # 일반 상태 변경 이벤트 (컴포넌트 특화 이벤트)
                        await self.event_bus.publish(EventPaths.OB_COLLECTOR_EXCHANGE_STATUS, event_data)
                        self.logger.info("거래소 상태 변경 이벤트 발행됨")
                
                # 현재 상태를 이전 상태로 저장
                prev_exchange_status = exchange_status.copy()
                
                # 1초마다 확인
                await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                self.logger.info("오더북 수집기 상태 모니터링 태스크가 취소되었습니다.")
                break
            except Exception as e:
                self.logger.error(f"상태 모니터링 중 오류: {str(e)}")
                await asyncio.sleep(5)  # 오류 발생 시 더 긴 간격으로 재시도
                
        self.logger.info("오더북 수집기 상태 모니터링 태스크 종료")
        
    def _create_status_message(self, exchanges_info: List[Dict[str, Any]], connected_count: int, total_count: int) -> str:
        """거래소 상태 알림 메시지 생성"""
        # 헤더
        # message = "📊 오더북 수집기 상태:\n"
        message = "───────────────\n"
        message += f"📡 거래소 연결 상태 ({connected_count}/{total_count})\n\n"
        
        # 한글 문자는 영문보다 시각적으로 더 넓기 때문에 시각적 길이를 계산
        def visual_length(text):
            # 한글은 2칸, 나머지는 1칸으로 계산 (텔레그램 고정폭 글꼴 기준)
            length = 0
            for char in text:
                if '\uAC00' <= char <= '\uD7A3':  # 한글 유니코드 범위
                    length += 2
                else:
                    length += 1
            return length
        
        # 시각적 길이 기준으로 가장 긴 거래소명 찾기
        max_visual_length = max(visual_length(ex["display_name"]) for ex in exchanges_info)
        
        # 각 거래소별 상태
        for ex in sorted(exchanges_info, key=lambda x: x["name"]):
            # 상태 이모지 선택
            status_emoji = "🟢" if ex["connected"] else "🔴"
            
            # 거래소명과 이모지
            exchange_name = f"{status_emoji} {ex['display_name']}"
            
            # 현재 항목의 시각적 길이 계산
            current_visual_length = visual_length(exchange_name)
            
            # 필요한 공백 수 계산 (이모지 + 거래소명 + 추가 공백)
            spaces_needed = max_visual_length + 8 - current_visual_length
            
            # 공백 추가로 패딩
            padded_exchange = exchange_name + " " * spaces_needed
            
            # 연결 상태에 따른 업타임 또는 상태 메시지
            if ex["connected"]:
                message += f"{padded_exchange}│ {ex['uptime_formatted']}\n"
            else:
                message += f"{padded_exchange}│ 연결 끊김\n"
        
        return message

    async def _do_stop(self) -> bool:
        """
        오더북 프로세스 중지
        
        Returns:
            bool: 중지 성공 여부
        """
        try:
            self.logger.info("오더북 프로세스 중지 중...")
            
            # 1. 메트릭 수집 중지
            if self.metric_reporter:
                try:
                    await self.metric_reporter.stop()
                    self.logger.info("메트릭 리포터 종료됨")
                except Exception as e:
                    self.logger.warning(f"메트릭 리포터 종료 중 오류: {str(e)}")
                self.metric_reporter = None
            
            # 2. 수집기 중지
            stop_success = await self.collector.stop()
            if not stop_success:
                self.logger.error("오더북 수집기 중지 실패")
                return False
            
            self.logger.info("오더북 프로세스가 성공적으로 중지되었습니다.")
            return True
            
        except Exception as e:
            self.logger.error(f"오더북 프로세스 중지 중 오류 발생: {str(e)}")
            return False

# 싱글톤 인스턴스
_instance = None

def get_orderbook_process(eventbus, config=None) -> OrderbookProcess:
    """
    오더북 프로세스 컴포넌트 인스턴스를 반환합니다.
    
    싱글톤 패턴을 사용하여 하나의 인스턴스만 생성합니다.
    
    Args:
        eventbus: 이벤트 버스 객체
        config: 설정 딕셔너리 (기본값: None)
        
    Returns:
        OrderbookProcess: 오더북 프로세스 컴포넌트 인스턴스
    """
    global _instance
    if _instance is None:
        _instance = OrderbookProcess(eventbus=eventbus, config=config)
    return _instance

async def initialize_orderbook_process(eventbus, config=None):
    """
    오더북 프로세스 컴포넌트를 초기화합니다.
    
    Args:
        eventbus: 이벤트 버스 객체
        config: 설정 딕셔너리 (기본값: None)
        
    Returns:
        OrderbookProcess: 초기화된 오더북 프로세스 인스턴스
    """
    process = get_orderbook_process(eventbus=eventbus, config=config)
    await process.setup()
    return process