# import asyncio
# import json
# import time
# from typing import Dict, List, Optional, Set, Any

# from crosskimp.logger.logger import get_unified_logger
# from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG

# from crosskimp.ob_collector.orderbook.connection.bybit_s_cn import BybitWebSocketConnector
# from crosskimp.ob_collector.orderbook.orderbook.bybit_s_ob import BybitSpotOrderBookManager
# from crosskimp.ob_collector.orderbook.parser.bybit_s_pa import BybitParser

# # 로거 인스턴스 가져오기
# logger = get_unified_logger()

# # ============================
# # 바이빗 현물 웹소켓 관련 상수
# # ============================
# # 기본 설정
# EXCHANGE_CODE = Exchange.BYBIT.value  # 거래소 코드
# BYBIT_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 바이빗 설정

# # 오더북 관련 설정
# DEFAULT_DEPTH = BYBIT_CONFIG["default_depth"]  # 기본 오더북 깊이

# class BybitSpotWebsocket(BybitWebSocketConnector):
#     """
#     Bybit 현물 WebSocket
    
#     메시지 처리 및 오더북 관리를 담당합니다.
#     연결 관리는 BybitWebSocketConnector 클래스에서 처리합니다.
#     """
#     def __init__(self, settings: dict):
#         super().__init__(settings)
#         self.depth_level = settings.get("depth", DEFAULT_DEPTH)
#         self.ob_manager = BybitSpotOrderBookManager(self.depth_level)
#         self.ob_manager.set_websocket(self)  # 웹소켓 연결 설정
        
#         # 파서 초기화
#         self.parser = BybitParser()
        
#         # 메시지 처리 통계
#         self._raw_log_count = 0

#     def set_output_queue(self, queue: asyncio.Queue) -> None:
#         """
#         출력 큐 설정
#         - 부모 클래스의 output_queue 설정
#         - 오더북 매니저의 output_queue 설정
#         """
#         # 부모 클래스의 output_queue 설정
#         super().set_output_queue(queue)
        
#         # 오더북 매니저의 output_queue 설정
#         self.ob_manager.set_output_queue(queue)
        
#         # 로깅 추가
#         self.log_info(f"웹소켓 출력 큐 설정 완료 (큐 ID: {id(queue)})")
        
#         # 큐 설정 확인
#         if not hasattr(self.ob_manager, '_output_queue') or self.ob_manager._output_queue is None:
#             self.log_error("오더북 매니저 큐 설정 실패!")
#         else:
#             self.log_info(f"오더북 매니저 큐 설정 확인 (큐 ID: {id(self.ob_manager._output_queue)})")

#     async def parse_message(self, message: str) -> Optional[dict]:
#         """
#         수신된 메시지 파싱
        
#         Args:
#             message: 수신된 웹소켓 메시지
            
#         Returns:
#             Optional[dict]: 파싱된 메시지 또는 None
#         """
#         try:
#             try:
#                 data = json.loads(message)
#             except json.JSONDecodeError as e:
#                 self.log_error(f"JSON 파싱 실패: {str(e)}, 메시지: {message[:100]}...")
#                 return None
            
#             if data.get("op") == "subscribe":
#                 if self.connection_status_callback:
#                     self.connection_status_callback(self.exchangename, "subscribe_response")
#                 self.log_debug(f"구독 응답 수신: {data}")
#                 return None
                
#             if data.get("op") == "pong" or (data.get("ret_msg") == "pong" and data.get("op") == "ping"):
#                 self._handle_pong(data)
#                 return None

#             if "topic" in data and "data" in data:
#                 topic = data.get("topic", "")
#                 if "orderbook" in topic:
#                     parts = topic.split(".")
#                     if len(parts) >= 3:
#                         symbol = parts[-1].replace("USDT", "")
#                         msg_type = data.get("type", "delta")
                        
#                         # 원본 메시지 로깅 강화
#                         self.log_raw_message("orderbook", message, symbol)
                        
#                         # 로깅 성공 메시지 (처음 5개만)
#                         self._raw_log_count += 1
                        
#                         if self._raw_log_count <= 5:
#                             self.log_debug(f"원본 데이터 로깅 성공 #{self._raw_log_count}: {symbol}")
                        
#                         if msg_type == "snapshot":
#                             self.log_info(f"스냅샷 메시지 수신: {symbol}, type={msg_type}")
#                         return data
#             return None
            
#         except Exception as e:
#             self.log_error(f"메시지 파싱 실패: {str(e)}")
#             return None

#     async def handle_parsed_message(self, parsed: dict) -> None:
#         """
#         파싱된 메시지 처리
        
#         Args:
#             parsed: 파싱된 메시지
#         """
#         try:
#             if not parsed or "topic" not in parsed:
#                 return

#             topic = parsed["topic"]
#             if not topic.startswith("orderbook"):
#                 return

#             symbol = topic.split(".")[-1].replace("USDT", "")
            
#             start_time = time.time()
#             await self.ob_manager.update(symbol, parsed)
#             processing_time = (time.time() - start_time) * 1000
            
#             if self.stats:
#                 self.stats.message_count += 1
#                 if not hasattr(self.stats, 'processing_times'):
#                     self.stats.processing_times = []
#                 self.stats.processing_times.append(processing_time)
                
#                 if self.stats.message_count % 1000 == 0:
#                     avg_time = sum(self.stats.processing_times[-1000:]) / min(1000, len(self.stats.processing_times))
#                     self.log_info(
#                         f"{symbol} 처리 성능 | 평균={avg_time:.2f}ms, 총={self.stats.message_count:,}개"
#                     )
            
#             if self.connection_status_callback:
#                 self.connection_status_callback(self.exchangename, "message")
                
#         except Exception as e:
#             self.log_error(f"메시지 처리 실패: {str(e)}")

#     async def process_message(self, message: str) -> None:
#         """
#         수신된 메시지 처리 (BybitWebSocketConnector 클래스의 추상 메서드 구현)
        
#         Args:
#             message: 수신된 웹소켓 메시지
#         """
#         parsed = await self.parse_message(message)
#         if parsed:
#             await self.handle_parsed_message(parsed)

#     async def _prepare_start(self, symbols: List[str]) -> None:
#         """
#         시작 전 초기화 및 설정 (BaseWebsocketConnector 템플릿 메서드 구현)
#         """
#         if self.current_retry > 0:
#             self.log_info(f"재연결 감지 (retry={self.current_retry}), 오더북 초기화 수행")
#             self.ob_manager.clear_all()
#             self.log_info(f"재연결 후 오더북 매니저 초기화 완료")
        
#         self.log_info(f"시작 준비 완료 | 심볼 수: {len(symbols)}개")