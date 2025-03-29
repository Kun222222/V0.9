"""
웹소켓 연결 끊김 및 자동 재연결 테스트 스크립트

이 스크립트는 OrderbookCollectorManager의 웹소켓 연결이 끊겼을 때
자동으로 재연결 및 재구독이 되는지 테스트합니다.
"""

import asyncio
import sys
import time
import signal
import argparse
import logging
import random
from typing import Dict, Any, List, Optional

# 시스템 경로에 src 디렉토리 추가
sys.path.append('.')

from crosskimp.common.logger.logger import get_unified_logger
from crosskimp.common.config.common_constants import SystemComponent, Exchange
from crosskimp.ob_collector.obcollector import OrderbookCollectorManager

# 로거 생성
logger = get_unified_logger(SystemComponent.OB_COLLECTOR.value)

# 수신된 오더북 데이터 카운터
orderbook_counter = {}
reconnect_success_count = 0
reconnect_failure_count = 0
test_results = {}

def configure_logging(target_exchange):
    """
    로그 필터링 설정 - 타겟 거래소 관련 로그만 표시
    """
    logger_root = logging.getLogger()
    
    class ExchangeFilter(logging.Filter):
        def __init__(self, target_exchange):
            super().__init__()
            self.target_exchange = target_exchange
        
        def filter(self, record):
            # 타겟 거래소 관련 로그만 허용 (정확한 거래소 코드 매칭)
            if self.target_exchange == 'binance' and 'binance_spot' in record.getMessage():
                return True
            
            if self.target_exchange in record.getMessage():
                return True
            
            # OrderbookCollectorManager, OBCollector 관련 로그는 항상 허용
            if "OrderbookCollector" in record.getMessage() or "OBCollector" in record.getMessage():
                return True
            
            return False
    
    # 필터 적용
    for handler in logger_root.handlers:
        handler.addFilter(ExchangeFilter(target_exchange))
    
    logger.info(f"로그 필터링 설정 완료: {target_exchange} 관련 로그만 표시")

def orderbook_callback(data: Dict[str, Any]):
    """
    오더북 데이터 수신 콜백 함수
    데이터 수신 시 카운터 증가
    """
    exchange = data.get('exchange', 'unknown')
    symbol = data.get('symbol', 'unknown')
    key = f"{exchange}:{symbol}"
    
    if key not in orderbook_counter:
        orderbook_counter[key] = 0
    orderbook_counter[key] += 1
    
    # 너무 많은 로그가 생성되므로 100개마다 한 번씩만 출력
    if orderbook_counter[key] % 100 == 0:
        logger.info(f"오더북 데이터 수신: {key} - {orderbook_counter[key]}개")

def print_connection_status(manager):
    """현재 거래소 연결 상태 출력"""
    status = manager.get_exchange_status()
    
    connected = 0
    disconnected = 0
    
    for exchange, is_connected in status.items():
        if is_connected:
            connected += 1
        else:
            disconnected += 1
            
    logger.info(f"연결 상태: {connected}개 연결됨, {disconnected}개 연결 안됨")
    
    for exchange, is_connected in status.items():
        logger.info(f"거래소: {exchange} - {'연결됨' if is_connected else '연결 안됨'}")
    
    return status

async def wait_for_event(condition_func, timeout=30, check_interval=1.0):
    """
    특정 조건이 충족될 때까지 대기하는 유틸리티 함수
    
    Args:
        condition_func: 완료 조건을 확인하는 함수(True/False 반환)
        timeout: 최대 대기 시간(초)
        check_interval: 확인 간격(초)
        
    Returns:
        bool: 조건 충족 여부
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if condition_func():
            return True
        await asyncio.sleep(check_interval)
    return False

async def simulate_disconnection(manager, exchange_to_disconnect):
    """
    특정 거래소의 웹소켓 연결을 강제로 끊는 함수
    다양한 방법을 시도하여 가장 안정적인 방법으로 연결 종료
    """
    logger.info(f"\n------ {exchange_to_disconnect} 연결 강제 종료 시작 ------")
    
    # 해당 거래소의 커넥터 가져오기
    connector = manager.connection_manager.get_connector(exchange_to_disconnect)
    
    if not connector:
        logger.error(f"{exchange_to_disconnect} 커넥터를 찾을 수 없습니다.")
        return False
    
    # 방법 1: 웹소켓 객체 직접 접근하여 강제 종료
    try:
        logger.info(f"방법 1: 웹소켓 객체 직접 접근하여 종료 시도")
        ws = await connector.get_websocket()
        
        if ws and ws.open:
            try:
                # 비정상 종료 코드를 사용하여 즉시 종료
                await ws.close(code=1006, reason="Test forced disconnect")
                logger.info(f"{exchange_to_disconnect} 웹소켓 연결 종료 성공 (방법 1)")
                
                # 상태 확인 위해 잠시 대기
                await asyncio.sleep(1)
                if not connector.is_connected:
                    return True
            except Exception as e:
                logger.warning(f"방법 1 실패: {str(e)}")
    except Exception as e:
        logger.warning(f"방법 1 실패(웹소켓 접근 오류): {str(e)}")
    
    # 방법 2: 커넥터의 disconnect 메서드 직접 호출
    try:
        logger.info(f"방법 2: 커넥터의 disconnect 메서드 직접 호출")
        disconnect_result = await connector.disconnect()
        
        if disconnect_result:
            logger.info(f"{exchange_to_disconnect} disconnect 메서드 호출 성공 (방법 2)")
            return True
        else:
            logger.warning(f"방법 2 실패: disconnect 메서드 결과 {disconnect_result}")
    except Exception as e:
        logger.warning(f"방법 2 실패: {str(e)}")
    
    # 방법 3: 연결 상태 속성 직접 변경 (최후의 방법)
    try:
        logger.info(f"방법 3: 연결 상태 속성 직접 변경 (최후의 방법)")
        
        # 연결 상태 강제 변경
        connector.is_connected = False
        
        # 연결 관리자에 상태 업데이트
        manager.connection_manager.update_exchange_status(exchange_to_disconnect, False)
        
        logger.info(f"{exchange_to_disconnect} 연결 상태 직접 변경 성공 (방법 3)")
        return True
    except Exception as e:
        logger.warning(f"방법 3 실패: {str(e)}")
    
    # 모든 방법이 실패한 경우
    logger.error(f"{exchange_to_disconnect} 연결 종료 실패: 모든 방법 실패")
    return False

async def test_reconnection_for_exchange(manager, exchange_code: str, wait_time: int = 60, test_count: int = 1):
    """
    특정 거래소에 대한 재연결 테스트 수행
    
    Args:
        manager: OrderbookCollectorManager 인스턴스
        exchange_code: 테스트할 거래소 코드
        wait_time: 재연결 대기 시간(초)
        test_count: 테스트 반복 횟수
        
    Returns:
        Dict: 테스트 결과 요약
    """
    global reconnect_success_count, reconnect_failure_count, test_results
    
    results = {
        "성공": 0,
        "실패": 0,
        "평균재연결시간": 0,
        "상세결과": []
    }
    
    total_reconnect_time = 0
    
    for test_num in range(1, test_count + 1):
        try:
            logger.info(f"\n===== {exchange_code} 재연결 테스트 {test_num}/{test_count} 시작 =====")
            
            # 이전 테스트가 종료되지 않았을 경우를 대비해 잠시 대기
            if test_num > 1:
                await asyncio.sleep(10)
                
                # 커넥터가 연결상태인지 확인
                status = manager.get_exchange_status()
                if not status.get(exchange_code, False):
                    logger.warning(f"{exchange_code}가 아직 연결되지 않았습니다. 30초 추가 대기...")
                    await asyncio.sleep(30)
                    
                    # 다시 상태 확인
                    status = manager.get_exchange_status()
                    if not status.get(exchange_code, False):
                        logger.error(f"{exchange_code}를 연결할 수 없습니다. 테스트 중단.")
                        results["실패"] += 1
                        results["상세결과"].append({"테스트번호": test_num, "결과": "실패", "원인": "연결 상태 회복 실패"})
                        continue
            
            # 연결 상태 확인
            status = manager.get_exchange_status()
            if not status.get(exchange_code, False):
                logger.warning(f"{exchange_code}가 연결되어 있지 않습니다. 테스트를 건너뜁니다.")
                results["실패"] += 1
                results["상세결과"].append({"테스트번호": test_num, "결과": "실패", "원인": "초기 연결 없음"})
                continue
            
            # 연결 끊기 전 데이터 수신 상태 기록
            pre_disconnect_count = {k: v for k, v in orderbook_counter.items() if k.startswith(exchange_code)}
            logger.info(f"연결 끊기 전 데이터 수신 상태: {pre_disconnect_count}")
            
            if not pre_disconnect_count:
                logger.warning(f"{exchange_code}에서 데이터가 수신되지 않았습니다. 테스트를 건너뜁니다.")
                results["실패"] += 1
                results["상세결과"].append({"테스트번호": test_num, "결과": "실패", "원인": "초기 데이터 없음"})
                continue
            
            # 선택된 거래소 연결 강제 종료
            disconnection_success = await simulate_disconnection(manager, exchange_code)
            
            if not disconnection_success:
                logger.error(f"{exchange_code} 연결 강제 종료 실패")
                results["실패"] += 1
                results["상세결과"].append({"테스트번호": test_num, "결과": "실패", "원인": "연결 종료 실패"})
                continue
            
            # 연결이 끊겼는지 확인
            await asyncio.sleep(1)
            status = manager.get_exchange_status()
            if status.get(exchange_code, False):
                logger.warning(f"{exchange_code} 연결이 끊겼지만 상태가 업데이트되지 않았습니다.")
            
            # 재연결 확인을 위해 대기
            logger.info(f"재연결 확인을 위해 최대 {wait_time}초 대기...")
            
            reconnection_start_time = time.time()
            is_reconnected = False
            data_resumption = False
            
            # 5초마다 연결 상태 확인 (빠른 확인을 위해 간격 축소)
            check_interval = 5
            for i in range(wait_time // check_interval):
                await asyncio.sleep(check_interval)
                
                # 현재 연결 상태
                status = print_connection_status(manager)
                
                # 데이터 수신 상태 확인
                current_count = {k: v for k, v in orderbook_counter.items() if k.startswith(exchange_code)}
                logger.info(f"현재 데이터 수신 상태: {current_count}")
                
                # 재연결 여부 확인
                if status.get(exchange_code, False):
                    reconnection_time = time.time() - reconnection_start_time
                    logger.info(f"{exchange_code} 재연결 성공! (소요 시간: {reconnection_time:.2f}초)")
                    is_reconnected = True
                    total_reconnect_time += reconnection_time
                    
                    # 데이터 재수신 여부 확인
                    new_symbols = []
                    for k in current_count:
                        if k in pre_disconnect_count:
                            if current_count[k] > pre_disconnect_count[k]:
                                new_symbols.append(k)
                        else:
                            new_symbols.append(k)
                    
                    if new_symbols:
                        logger.info(f"데이터 재수신 확인: {new_symbols}")
                        data_resumption = True
                        logger.info(f"===== {exchange_code} 자동 재연결 및 재구독 성공 =====")
                        results["성공"] += 1
                        results["상세결과"].append({
                            "테스트번호": test_num, 
                            "결과": "성공", 
                            "재연결시간": f"{reconnection_time:.2f}초",
                            "재수신심볼": len(new_symbols)
                        })
                        break
                    else:
                        logger.warning(f"{exchange_code} 재연결되었지만 아직 데이터가 수신되지 않음")
                
                # 마지막 확인이면서 재연결 실패한 경우
                if i == (wait_time // check_interval) - 1:
                    if not is_reconnected:
                        logger.error(f"===== {exchange_code} 자동 재연결 실패 =====")
                        results["실패"] += 1
                        results["상세결과"].append({"테스트번호": test_num, "결과": "실패", "원인": "자동 재연결 실패"})
                    elif not data_resumption:
                        logger.error(f"===== {exchange_code} 재연결은 성공했지만 데이터 재수신 실패 =====")
                        results["실패"] += 1
                        results["상세결과"].append({"테스트번호": test_num, "결과": "부분 실패", "원인": "데이터 재수신 안됨"})
        
        except Exception as e:
            logger.error(f"{exchange_code} 테스트 {test_num} 중 오류 발생: {str(e)}")
            results["실패"] += 1
            results["상세결과"].append({"테스트번호": test_num, "결과": "실패", "원인": f"예외 발생: {str(e)}"})
    
    # 평균 재연결 시간 계산
    if results["성공"] > 0:
        results["평균재연결시간"] = f"{total_reconnect_time / results['성공']:.2f}초"
    
    # 결과 요약 출력
    logger.info(f"\n===== {exchange_code} 재연결 테스트 결과 요약 =====")
    logger.info(f"총 테스트: {test_count}회")
    logger.info(f"성공: {results['성공']}회, 실패: {results['실패']}회")
    if results["성공"] > 0:
        logger.info(f"평균 재연결 시간: {results['평균재연결시간']}")
    
    return results

async def test_all_exchanges(wait_time: int = 60, specific_exchange: Optional[str] = None, test_count: int = 1):
    """
    모든 거래소(또는 특정 거래소)에 대한 재연결 테스트 수행
    
    Args:
        wait_time: 재연결 대기 시간(초)
        specific_exchange: 특정 거래소 코드 (None이면 모든 거래소 테스트, 'all'이면 모든 거래소 테스트)
        test_count: 테스트 반복 횟수
    """
    global reconnect_success_count, reconnect_failure_count, test_results
    
    # 테스트 결과 초기화
    reconnect_success_count = 0
    reconnect_failure_count = 0
    test_results = {}
    
    logger.info("\n===== 웹소켓 재연결 테스트 시작 =====")
    
    # 로그 필터링 설정
    if specific_exchange and specific_exchange != 'all':
        configure_logging(specific_exchange)
    
    # OrderbookCollectorManager 인스턴스 생성
    manager = OrderbookCollectorManager()
    
    # 오더북 콜백 등록
    manager.add_orderbook_callback(orderbook_callback)
    
    # 시스템 초기화
    logger.info("시스템 초기화 중...")
    if not await manager.initialize():
        logger.error("초기화 실패")
        return
    
    # 시스템 시작
    logger.info("시스템 시작 중...")
    if not await manager.start():
        logger.error("시작 실패")
        await manager.stop()
        return
    
    try:
        # 일정 시간 대기하여 초기 데이터 수신 확인
        logger.info("초기 데이터 수신 대기 중 (30초)...")
        await asyncio.sleep(30)
        
        # 현재 데이터 수신 상태 출력
        logger.info("현재 데이터 수신 상태:")
        for key, count in orderbook_counter.items():
            logger.info(f"  {key}: {count}개")
        
        # 연결 상태 출력
        status = print_connection_status(manager)
        
        # 특정 거래소만 테스트하는 경우
        if specific_exchange and specific_exchange != 'all':
            # 거래소 코드 변환 (호환성을 위한 처리)
            if specific_exchange == 'binance':
                specific_exchange = 'binance_spot'
            
            if specific_exchange in status:
                result = await test_reconnection_for_exchange(manager, specific_exchange, wait_time, test_count)
                test_results[specific_exchange] = result
                
                # 성공/실패 카운터 업데이트
                reconnect_success_count += result["성공"]
                reconnect_failure_count += result["실패"]
            else:
                logger.error(f"{specific_exchange}는 지원하지 않는 거래소 코드입니다.")
        # 모든 거래소 테스트
        else:
            for exchange in status.keys():
                if status[exchange]:  # 연결된 거래소만 테스트
                    result = await test_reconnection_for_exchange(manager, exchange, wait_time, test_count)
                    test_results[exchange] = result
                    
                    # 성공/실패 카운터 업데이트
                    reconnect_success_count += result["성공"]
                    reconnect_failure_count += result["실패"]
        
        # 결과 요약 출력
        logger.info("\n===== 테스트 결과 요약 =====")
        for exchange, result in test_results.items():
            logger.info(f"{exchange}: {'성공' if result['성공'] > 0 else '실패'} - {result['성공']}/{result['성공'] + result['실패']} 테스트 성공")
        logger.info(f"성공: {reconnect_success_count}, 실패: {reconnect_failure_count}")
        
    finally:
        # 테스트 종료 시 시스템 정리
        logger.info("테스트 종료, 시스템 종료 중...")
        await manager.stop()

async def test_concurrent_disconnections(wait_time: int = 60, test_count: int = 1, disconnect_count: int = 2):
    """
    여러 거래소를 동시에 랜덤하게 연결 끊음 테스트
    
    Args:
        wait_time: 재연결 대기 시간(초)
        test_count: 테스트 반복 횟수
        disconnect_count: 동시에 연결을 끊을 거래소 수
    """
    global reconnect_success_count, reconnect_failure_count, test_results
    
    # 테스트 결과 초기화
    reconnect_success_count = 0
    reconnect_failure_count = 0
    test_results = {}
    
    logger.info("\n===== 다중 거래소 동시 연결 끊김 테스트 시작 =====")
    
    # OrderbookCollectorManager 인스턴스 생성
    manager = OrderbookCollectorManager()
    
    # 오더북 콜백 등록
    manager.add_orderbook_callback(orderbook_callback)
    
    # 시스템 초기화
    logger.info("시스템 초기화 중...")
    if not await manager.initialize():
        logger.error("초기화 실패")
        return
    
    # 시스템 시작
    logger.info("시스템 시작 중...")
    if not await manager.start():
        logger.error("시작 실패")
        await manager.stop()
        return
    
    try:
        # 일정 시간 대기하여 초기 데이터 수신 확인
        logger.info("초기 데이터 수신 대기 중 (30초)...")
        await asyncio.sleep(30)
        
        # 현재 데이터 수신 상태 출력
        logger.info("현재 데이터 수신 상태:")
        for key, count in orderbook_counter.items():
            logger.info(f"  {key}: {count}개")
        
        # 연결 상태 출력
        status = print_connection_status(manager)
        
        for test_num in range(1, test_count + 1):
            try:
                logger.info(f"\n===== 다중 거래소 동시 연결 끊김 테스트 {test_num}/{test_count} 시작 =====")
                
                # 이전 테스트가 종료되지 않았을 경우를 대비해 잠시 대기
                if test_num > 1:
                    await asyncio.sleep(30)
                    
                    # 모든 거래소가 연결상태인지 확인
                    status = manager.get_exchange_status()
                    if False in status.values():
                        logger.warning("일부 거래소가 아직 연결되지 않았습니다. 30초 추가 대기...")
                        await asyncio.sleep(30)
                
                # 연결 상태 확인
                status = manager.get_exchange_status()
                
                # 연결된 거래소 목록
                connected_exchanges = [exchange for exchange, is_connected in status.items() if is_connected]
                
                if len(connected_exchanges) < disconnect_count:
                    logger.warning(f"연결된 거래소가 {len(connected_exchanges)}개로, 요청한 {disconnect_count}개보다 적습니다.")
                    disconnect_count = len(connected_exchanges)
                    
                if disconnect_count == 0:
                    logger.error("연결된 거래소가 없습니다. 테스트를 건너뜁니다.")
                    continue
                
                # 랜덤하게 거래소 선택
                exchanges_to_disconnect = random.sample(connected_exchanges, disconnect_count)
                logger.info(f"랜덤하게 선택된 {disconnect_count}개 거래소: {exchanges_to_disconnect}")
                
                # 연결 끊기 전 데이터 수신 상태 기록
                pre_disconnect_count = {}
                for exchange in exchanges_to_disconnect:
                    pre_disconnect_count[exchange] = {k: v for k, v in orderbook_counter.items() if k.startswith(exchange)}
                    logger.info(f"{exchange} 연결 끊기 전 데이터 수신 상태: {pre_disconnect_count[exchange]}")
                
                # 각 거래소에 대한 연결 강제 종료 태스크 생성
                disconnect_tasks = [simulate_disconnection(manager, exchange) for exchange in exchanges_to_disconnect]
                
                # 동시에 모든 거래소 연결 종료
                logger.info(f"선택된 {disconnect_count}개 거래소 연결 동시 종료 시작...")
                disconnect_results = await asyncio.gather(*disconnect_tasks)
                
                # 연결 종료 결과 확인
                for exchange, result in zip(exchanges_to_disconnect, disconnect_results):
                    if not result:
                        logger.error(f"{exchange} 연결 강제 종료 실패")
                    else:
                        logger.info(f"{exchange} 연결 강제 종료 성공")
                
                # 연결이 끊겼는지 확인
                await asyncio.sleep(1)
                status = manager.get_exchange_status()
                
                # 재연결 확인을 위해 대기
                logger.info(f"재연결 확인을 위해 최대 {wait_time}초 대기...")
                
                reconnection_start_time = time.time()
                reconnection_results = {exchange: {"is_reconnected": False, "data_resumption": False, "reconnection_time": 0} for exchange in exchanges_to_disconnect}
                
                # 5초마다 연결 상태 확인
                check_interval = 5
                for i in range(wait_time // check_interval):
                    await asyncio.sleep(check_interval)
                    
                    # 현재 연결 상태
                    status = print_connection_status(manager)
                    
                    # 각 거래소에 대해 데이터 수신 및 재연결 여부 확인
                    for exchange in exchanges_to_disconnect:
                        # 이미 재연결 성공한 거래소는 건너뜀
                        if reconnection_results[exchange]["is_reconnected"] and reconnection_results[exchange]["data_resumption"]:
                            continue
                            
                        # 데이터 수신 상태 확인
                        current_count = {k: v for k, v in orderbook_counter.items() if k.startswith(exchange)}
                        
                        # 재연결 여부 확인
                        if status.get(exchange, False) and not reconnection_results[exchange]["is_reconnected"]:
                            reconnection_time = time.time() - reconnection_start_time
                            logger.info(f"{exchange} 재연결 성공! (소요 시간: {reconnection_time:.2f}초)")
                            reconnection_results[exchange]["is_reconnected"] = True
                            reconnection_results[exchange]["reconnection_time"] = reconnection_time
                            
                            # 이 거래소에 대한 테스트 결과 초기화
                            if exchange not in test_results:
                                test_results[exchange] = {
                                    "성공": 0,
                                    "실패": 0,
                                    "평균재연결시간": 0,
                                    "상세결과": []
                                }
                        
                        # 데이터 재수신 여부 확인
                        if reconnection_results[exchange]["is_reconnected"] and not reconnection_results[exchange]["data_resumption"]:
                            new_symbols = []
                            for k in current_count:
                                if k in pre_disconnect_count[exchange]:
                                    if current_count[k] > pre_disconnect_count[exchange][k]:
                                        new_symbols.append(k)
                                else:
                                    new_symbols.append(k)
                            
                            if new_symbols:
                                logger.info(f"{exchange} 데이터 재수신 확인: {new_symbols}")
                                reconnection_results[exchange]["data_resumption"] = True
                                logger.info(f"===== {exchange} 자동 재연결 및 재구독 성공 =====")
                                
                                # 테스트 결과 업데이트
                                test_results[exchange]["성공"] += 1
                                reconnect_success_count += 1
                                test_results[exchange]["상세결과"].append({
                                    "테스트번호": test_num, 
                                    "결과": "성공", 
                                    "재연결시간": f"{reconnection_results[exchange]['reconnection_time']:.2f}초",
                                    "재수신심볼": len(new_symbols)
                                })
                    
                    # 모든 거래소가 재연결되고 데이터 재수신이 확인되었는지 확인
                    all_reconnected = all(result["is_reconnected"] and result["data_resumption"] for result in reconnection_results.values())
                    if all_reconnected:
                        logger.info("모든 거래소 재연결 및 데이터 재수신 확인 완료!")
                        break
                
                # 재연결 실패한 거래소 처리
                for exchange, result in reconnection_results.items():
                    if not result["is_reconnected"] or not result["data_resumption"]:
                        logger.error(f"{exchange} 재연결 또는 데이터 재수신 실패")
                        
                        # 테스트 결과 업데이트
                        if exchange not in test_results:
                            test_results[exchange] = {
                                "성공": 0,
                                "실패": 0,
                                "평균재연결시간": 0,
                                "상세결과": []
                            }
                        
                        test_results[exchange]["실패"] += 1
                        reconnect_failure_count += 1
                        
                        fail_reason = "자동 재연결 실패" if not result["is_reconnected"] else "데이터 재수신 실패"
                        test_results[exchange]["상세결과"].append({
                            "테스트번호": test_num, 
                            "결과": "실패", 
                            "원인": fail_reason
                        })
                
                # 평균 재연결 시간 계산
                for exchange in test_results:
                    if test_results[exchange]["성공"] > 0:
                        # 성공한 테스트의 재연결 시간 추출
                        reconnect_times = [float(result["재연결시간"].replace("초", "")) 
                                         for result in test_results[exchange]["상세결과"] 
                                         if result["결과"] == "성공"]
                        if reconnect_times:
                            test_results[exchange]["평균재연결시간"] = f"{sum(reconnect_times) / len(reconnect_times):.2f}초"
            
            except Exception as e:
                logger.error(f"테스트 {test_num} 중 오류 발생: {str(e)}")
                continue
        
        # 결과 요약 출력
        logger.info("\n===== 다중 거래소 동시 연결 끊김 테스트 결과 요약 =====")
        for exchange, result in test_results.items():
            if result["성공"] + result["실패"] > 0:  # 테스트가 실행된 거래소만
                logger.info(f"{exchange}: {'성공' if result['성공'] > 0 else '실패'} - {result['성공']}/{result['성공'] + result['실패']} 테스트 성공")
                if result["성공"] > 0 and "평균재연결시간" in result:
                    logger.info(f"  평균 재연결 시간: {result['평균재연결시간']}")
        
        logger.info(f"총 성공: {reconnect_success_count}, 총 실패: {reconnect_failure_count}")
    
    finally:
        # 테스트 종료 시 시스템 정리
        logger.info("테스트 종료, 시스템 종료 중...")
        await manager.stop()

def signal_handler(sig, frame):
    """
    Ctrl+C 처리 핸들러
    """
    logger.info("\n사용자에 의해 테스트가 중단되었습니다.")
    sys.exit(0)

if __name__ == "__main__":
    # Ctrl+C 처리 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    
    # 명령행 인자 파싱
    parser = argparse.ArgumentParser(description="웹소켓 재연결 테스트")
    parser.add_argument("--exchange", type=str, help="테스트할 거래소 코드 (기본값: 모든 거래소, 'all': 모든 거래소, 'concurrent': 동시 테스트)", default=None)
    parser.add_argument("--wait-time", type=int, help="재연결 대기 시간(초) (기본값: 60)", default=60)
    parser.add_argument("--test-count", type=int, help="테스트 반복 횟수 (기본값: 1)", default=1)
    parser.add_argument("--disconnect-count", type=int, help="동시에 연결을 끊을 거래소 수 (기본값: 2)", default=2)
    args = parser.parse_args()
    
    # 비동기 이벤트 루프 실행
    if args.exchange == 'concurrent':
        asyncio.run(test_concurrent_disconnections(args.wait_time, args.test_count, args.disconnect_count))
    else:
        asyncio.run(test_all_exchanges(args.wait_time, args.exchange, args.test_count)) 