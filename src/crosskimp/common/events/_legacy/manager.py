"""
시스템 이벤트 관리자 모듈

이 모듈은 시스템 전체의 이벤트 흐름을 관리하는 이벤트 관리자를 제공합니다.
- 컴포넌트별 이벤트 버스 생성 및 관리
- 이벤트 브릿지를 통한 컴포넌트 간 이벤트 라우팅
- 시스템 이벤트 흐름 제어
"""

import asyncio
import logging
from typing import Dict, List, Set, Optional

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.events.bus import ComponentEventBus, EventFilter
from crosskimp.common.events.types import EventTypes, Component

# 로거 설정
logger = get_unified_logger()

def validate_event_system():
    """이벤트 시스템 일관성 검증"""
    logger.info("이벤트 시스템 일관성 검증 시작...")
    
    # 필수 컴포넌트 검증
    missing_components = []
    for required in ["ORDERBOOK", "CALCULATOR", "TRADER", "SERVER"]:
        if not hasattr(Component, required):
            missing_components.append(required)
    
    # 필수 이벤트 타입 검증
    missing_events = []
    for required in ["SYSTEM_EVENT", "SYSTEM_STARTUP", "CONNECTION_STATUS"]:
        if not hasattr(EventTypes, required):
            missing_events.append(required)
            
    # 문제가 있으면 로그에 기록
    if missing_components or missing_events:
        logger.warning(f"이벤트 시스템 검증 실패: 누락된 컴포넌트={missing_components}, 누락된 이벤트={missing_events}")
        return False
        
    logger.info("이벤트 시스템 일관성 검증 성공")
    return True

class EventManager:
    """시스템 이벤트 관리자"""
    
    _instance = None
    
    @classmethod
    def get_instance(cls) -> 'EventManager':
        """싱글톤 인스턴스 반환"""
        if cls._instance is None:
            cls._instance = EventManager()
        return cls._instance
    
    def __init__(self):
        """이벤트 관리자 초기화"""
        if EventManager._instance is not None:
            raise Exception("EventManager는 싱글톤입니다. get_instance()를 사용하세요.")
            
        # 컴포넌트별 이벤트 버스 저장
        self._event_buses = {}
        
        # 브릿지 설정 (컴포넌트 간 이벤트 전달을 위한 매핑)
        self._bridges = {}
        
        # 시스템 상태
        self.running = False
        
        # 로거
        self._logger = logger
        
        self._logger.info("[이벤트관리자] 초기화 완료")
    
    def create_event_bus(self, component_name: str) -> ComponentEventBus:
        """
        컴포넌트별 이벤트 버스 생성 또는 가져오기
        
        Args:
            component_name: 컴포넌트 이름
            
        Returns:
            ComponentEventBus: 생성된 또는 기존 이벤트 버스
        """
        if component_name in self._event_buses:
            return self._event_buses[component_name]
            
        # 새 이벤트 버스 생성
        event_bus = ComponentEventBus(component_name)
        self._event_buses[component_name] = event_bus
        
        self._logger.info(f"[이벤트관리자] '{component_name}' 이벤트 버스 생성됨")
        return event_bus
    
    def get_event_bus(self, component_name: str) -> Optional[ComponentEventBus]:
        """
        컴포넌트별 이벤트 버스 가져오기
        
        Args:
            component_name: 컴포넌트 이름
            
        Returns:
            Optional[ComponentEventBus]: 찾은 이벤트 버스 또는 None
        """
        return self._event_buses.get(component_name)
    
    async def start(self):
        """모든 이벤트 버스 시작"""
        if self.running:
            return
            
        self.running = True
        
        # 모든 이벤트 버스 시작
        for component_name, event_bus in self._event_buses.items():
            await event_bus.start()
            
        # 브릿지 활성화
        await self._activate_bridges()
        
        self._logger.info("[이벤트관리자] 시작됨")
    
    async def stop(self):
        """모든 이벤트 버스 중지"""
        if not self.running:
            return
            
        self.running = False
        
        # 모든 이벤트 버스 중지
        for component_name, event_bus in self._event_buses.items():
            await event_bus.stop()
            
        self._logger.info("[이벤트관리자] 중지됨")
    
    def create_bridge(self, source_component: str, target_component: str, event_types: List[str]):
        """
        컴포넌트 간 이벤트 브릿지 생성
        
        특정 이벤트 타입에 대해 소스 컴포넌트에서 타겟 컴포넌트로 이벤트를 전달합니다.
        
        Args:
            source_component: 소스 컴포넌트 이름
            target_component: 타겟 컴포넌트 이름
            event_types: 브릿지할 이벤트 타입 목록
        """
        # 소스 컴포넌트 브릿지 맵 초기화
        if source_component not in self._bridges:
            self._bridges[source_component] = {}
            
        # 타겟 컴포넌트 이벤트 타입 셋 초기화
        if target_component not in self._bridges[source_component]:
            self._bridges[source_component][target_component] = set()
            
        # 이벤트 타입 추가
        for event_type in event_types:
            self._bridges[source_component][target_component].add(event_type)
            
        self._logger.info(f"[이벤트관리자] '{source_component}'에서 '{target_component}'로의 브릿지 생성됨 ({len(event_types)}개 이벤트)")
    
    async def _activate_bridges(self):
        """이벤트 브릿지 활성화"""
        for source_component, targets in self._bridges.items():
            source_bus = self.get_event_bus(source_component)
            if not source_bus:
                self._logger.warning(f"[이벤트관리자] 소스 컴포넌트 '{source_component}'의 이벤트 버스를 찾을 수 없음")
                continue
                
            for target_component, event_types in targets.items():
                target_bus = self.get_event_bus(target_component)
                if not target_bus:
                    self._logger.warning(f"[이벤트관리자] 타겟 컴포넌트 '{target_component}'의 이벤트 버스를 찾을 수 없음")
                    continue
                    
                # 각 이벤트 타입에 대한 브릿지 생성
                for event_type in event_types:
                    # 브릿지 함수 정의
                    async def bridge_event(data, target_bus=target_bus, event_type=event_type, src=source_component, tgt=target_component):
                        # 컴포넌트 정보 업데이트
                        if isinstance(data, dict):
                            data = data.copy()  # 원본 데이터 변경 방지
                            data["original_component"] = data.get("component")
                            data["bridge_info"] = {
                                "source": src,
                                "target": tgt,
                                "bridged": True
                            }
                            
                        # 타겟 버스로 이벤트 발행
                        await target_bus.publish(event_type, data)
                    
                    # 소스 버스에 구독 추가
                    await source_bus.subscribe(event_type, bridge_event)
                    
                self._logger.debug(f"[이벤트관리자] '{source_component}'에서 '{target_component}'로의 브릿지 활성화됨")
    
    async def publish_system_event(self, event_type: str, data: Dict):
        """
        시스템 이벤트 발행 (모든 컴포넌트에게 전달)
        
        Args:
            event_type: 이벤트 타입
            data: 이벤트 데이터
        """
        if not self.running:
            return

        # 시스템 이벤트 정보 추가
        if isinstance(data, dict):
            data = data.copy()
            data["system_event"] = True
            data["component"] = "system"

        # 모든 컴포넌트의 이벤트 버스에 발행
        tasks = []
        for component_name, event_bus in self._event_buses.items():
            tasks.append(event_bus.publish(event_type, data))
            
        # 비동기 발행 대기
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    def get_stats(self):
        """
        모든 이벤트 버스의 통계 정보 반환
        
        Returns:
            Dict: 통계 정보
        """
        stats = {}
        for component_name, event_bus in self._event_buses.items():
            stats[component_name] = event_bus.get_stats()
            
        # 브릿지 정보 추가
        bridge_stats = {}
        for source, targets in self._bridges.items():
            bridge_stats[source] = {}
            for target, events in targets.items():
                bridge_stats[source][target] = list(events)
                
        stats["bridges"] = bridge_stats
        return stats
    
    async def flush_all_events(self, timeout: float = 10.0):
        """
        모든 이벤트 버스의 큐 처리 대기
        
        Args:
            timeout: 최대 대기 시간 (초)
            
        Returns:
            bool: 성공 여부
        """
        tasks = []
        for component_name, event_bus in self._event_buses.items():
            tasks.append(event_bus.flush_queues(timeout))
            
        if not tasks:
            return True
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return all(isinstance(r, bool) and r for r in results)
    
    def setup_default_bridges(self):
        """기본 브릿지 설정"""
        # 시스템 이벤트는 모든 컴포넌트에게 전달
        system_events = [
            EventTypes.SYSTEM_STARTUP, 
            EventTypes.SYSTEM_SHUTDOWN,
            EventTypes.SYSTEM_STATUS_CHANGE,
            EventTypes.CONFIG_UPDATED
        ]
        
        components = list(self._event_buses.keys())
        system_bus = self.get_event_bus(Component.SYSTEM)
        
        if system_bus:
            # 시스템 → 모든 컴포넌트
            for component in components:
                if component != Component.SYSTEM:
                    self.create_bridge(Component.SYSTEM, component, system_events)
        
        # 오더북 → 레이더, 계산기 (오더북 데이터)
        if Component.ORDERBOOK in self._event_buses:
            orderbook_events = [
                EventTypes.ORDERBOOK_UPDATED,
                EventTypes.MARKET_DATA_UPDATED
            ]
            
            if Component.RADAR in self._event_buses:
                self.create_bridge(Component.ORDERBOOK, Component.RADAR, orderbook_events)
                
            if Component.CALCULATOR in self._event_buses:
                self.create_bridge(Component.ORDERBOOK, Component.CALCULATOR, orderbook_events)
        
        # 레이더 → 트레이더 (기회 감지)
        if Component.RADAR in self._event_buses and Component.TRADER in self._event_buses:
            radar_events = [
                EventTypes.OPPORTUNITY_DETECTED,
                EventTypes.OPPORTUNITY_EVALUATION
            ]
            self.create_bridge(Component.RADAR, Component.TRADER, radar_events)
        
        # 트레이더 → 텔레그램 (거래 알림)
        if Component.TRADER in self._event_buses and Component.TELEGRAM in self._event_buses:
            trader_events = [
                EventTypes.TRADE_EXECUTION,
                EventTypes.TRADE_COMPLETED,
                EventTypes.TRADE_FAILED
            ]
            self.create_bridge(Component.TRADER, Component.TELEGRAM, trader_events)
        
        # 오류 → 텔레그램 (오류 알림)
        error_events = [EventTypes.ERROR_CRITICAL]
        for component in components:
            if component != Component.TELEGRAM and Component.TELEGRAM in self._event_buses:
                self.create_bridge(component, Component.TELEGRAM, error_events)
        
        self._logger.info("[이벤트관리자] 기본 브릿지 설정 완료")


# 전역 인스턴스 접근 함수
def get_event_manager() -> EventManager:
    """
    EventManager 싱글톤 인스턴스 반환
    
    Returns:
        EventManager: 이벤트 관리자 싱글톤 인스턴스
    """
    return EventManager.get_instance()

# 컴포넌트별 이벤트 버스 획득 함수
def get_component_event_bus(component_name: str) -> ComponentEventBus:
    """
    지정된 컴포넌트의 이벤트 버스 반환
    
    Args:
        component_name: 컴포넌트 이름
        
    Returns:
        ComponentEventBus: 컴포넌트 이벤트 버스
    """
    event_manager = get_event_manager()
    
    # 컴포넌트 이름을 내부 구현 이름으로 변환
    impl_name = Component.get_impl_name(component_name)
    
    return event_manager.create_event_bus(impl_name)

# 편의 함수: 시스템 이벤트 발행
async def publish_system_event(event_type: str, data: Dict):
    """
    시스템 이벤트 발행 (모든 컴포넌트에게 전달)
    
    Args:
        event_type: 이벤트 타입
        data: 이벤트 데이터
    """
    event_manager = get_event_manager()
    await event_manager.publish_system_event(event_type, data)

# 이벤트 시스템 초기화 함수
async def initialize_event_system():
    """
    이벤트 시스템 초기화 및 시작
    
    이 함수는 애플리케이션 시작 시 호출하여 이벤트 시스템을 설정합니다.
    
    Returns:
        EventManager: 이벤트 관리자 인스턴스
    """
    # 이벤트 시스템 검증
    validate_event_system()
    
    event_manager = get_event_manager()
    
    # 기본 컴포넌트 이벤트 버스 생성
    event_manager.create_event_bus(Component.SYSTEM)
    event_manager.create_event_bus(Component.ORDERBOOK)
    event_manager.create_event_bus(Component.CALCULATOR)
    event_manager.create_event_bus(Component.TRADER)
    event_manager.create_event_bus(Component.RADAR)
    event_manager.create_event_bus(Component.TELEGRAM)
    event_manager.create_event_bus(Component.SERVER)
    
    # 기본 브릿지 설정
    event_manager.setup_default_bridges()
    
    # 이벤트 시스템 시작
    await event_manager.start()
    
    # 시스템 시작 이벤트 발행
    await publish_system_event(EventTypes.SYSTEM_STARTUP, {
        "message": "이벤트 시스템이 시작되었습니다."
    })
    
    return event_manager

# 이벤트 시스템 종료 함수
async def shutdown_event_system():
    """
    이벤트 시스템 종료
    
    이 함수는 애플리케이션 종료 시 호출하여 이벤트 시스템을 정리합니다.
    """
    event_manager = get_event_manager()
    
    # 시스템 종료 이벤트 발행
    await publish_system_event(EventTypes.SYSTEM_SHUTDOWN, {
        "message": "이벤트 시스템이 종료됩니다."
    })
    
    # 모든 이벤트 처리 대기
    await event_manager.flush_all_events(timeout=5.0)
    
    # 이벤트 시스템 중지
    await event_manager.stop() 