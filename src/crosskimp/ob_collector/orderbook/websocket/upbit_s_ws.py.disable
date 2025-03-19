# # file: orderbook/websocket/upbit_websocket.py

# import asyncio
# import json
# import time
# from typing import Dict, List, Optional, Tuple, Any
# from dataclasses import dataclass

# from crosskimp.config.ob_constants import Exchange, WEBSOCKET_CONFIG

# from crosskimp.ob_collector.orderbook.connection.upbit_s_cn import UpbitWebSocketConnector, PONG_RESPONSE
# from crosskimp.ob_collector.orderbook.orderbook.upbit_s_ob import UpbitOrderBookManager
# from crosskimp.ob_collector.orderbook.parser.upbit_s_pa import UpbitParser


# # ============================
# # 업비트 웹소켓 관련 상수
# # ============================
# # 기본 설정
# EXCHANGE_CODE = Exchange.UPBIT.value  # 거래소 코드
# UPBIT_CONFIG = WEBSOCKET_CONFIG[EXCHANGE_CODE]  # 업비트 설정

# # 오더북 관련 설정
# DEFAULT_DEPTH = UPBIT_CONFIG["default_depth"]  # 기본 오더북 깊이

# @dataclass
# class UpbitOrderbookUnit:
#     """업비트 오더북 단위 데이터 구조체"""
#     bid_price: float
#     bid_size: float
#     ask_price: float
#     ask_size: float

# class UpbitWebsocket(UpbitWebSocketConnector):
#     """
#     Upbit WebSocket 클라이언트
    
#     업비트 거래소의 실시간 오더북 데이터를 수신하고 처리하는 웹소켓 클라이언트입니다.
    
#     특징:
#     - 메시지 형식: 전체 오더북 스냅샷 방식 (델타 업데이트 아님)
#     """
#     def __init__(self, settings: dict):
#         """
#         업비트 웹소켓 클라이언트 초기화
        
#         Args:
#             settings: 설정 딕셔너리
#         """
#         super().__init__(settings)
        
#         # 오더북 설정
#         depth = settings.get("websocket", {}).get("orderbook_depth", DEFAULT_DEPTH)
#         self.orderbook_manager = UpbitOrderBookManager(depth=depth)
        
#         # 파서 초기화
#         self.parser = UpbitParser()
        
#         # 초기화된 심볼 추적
#         self.initialized_symbols = set()
        
#         # 메시지 처리 통계 초기화 - 리팩토링으로 인해 추가 필요
#         self.message_stats = {
#             "total_received": 0,
#             "ping_pong": 0,
#             "orderbook": 0
#         }
        
#         # 메시지 처리 통계 확장
#         self.message_stats.update({
#             "valid_parsed": 0,
#             "invalid_parsed": 0,
#             "processing_errors": 0
#         })

#     async def subscribe(self, symbols: List[str]) -> None:
#         """
#         지정된 심볼에 대한 오더북 구독
        
#         Args:
#             symbols: 구독할 심볼 목록
        
#         Raises:
#             ConnectionError: 웹소켓이 연결되지 않은 경우
#             Exception: 구독 처리 중 오류 발생
#         """
#         try:
#             # 연결 상태 확인
#             if not self.ws or not self.is_connected:
#                 error_msg = "웹소켓이 연결되지 않음"
#                 self.log_error(error_msg)
#                 raise ConnectionError(error_msg)
            
#             # 구독 메시지 생성
#             sub_message = self.parser.create_subscribe_message(symbols)
            
#             # 구독 시작 상태 콜백
#             if self.connection_status_callback:
#                 self.connection_status_callback(self.exchangename, "subscribe")
                
#             # 구독 메시지 전송
#             await self.ws.send(json.dumps(sub_message))
            
#             # 구독 완료 상태 콜백
#             if self.connection_status_callback:
#                 self.connection_status_callback(self.exchangename, "subscribe_complete")
                
#             self.log_info(f"오더북 구독 완료: {len(symbols)}개 심볼")
            
#         except Exception as e:
#             error_msg = f"구독 처리 중 오류 발생: {str(e)}"
#             self.log_error(error_msg)
#             raise

#     async def process_message(self, message: str) -> None:
#         """
#         수신된 메시지 처리 (UpbitWebSocketConnector 메서드 구현)
        
#         Args:
#             message: 수신된 웹소켓 메시지
#         """
#         # PONG 메시지는 이미 처리됨
#         if message == PONG_RESPONSE:
#             return
            
#         # 메시지 파싱 및 처리
#         parsed = self.parser.parse_message(message)
#         if parsed:
#             await self.handle_parsed_message(parsed)
#             if self.connection_status_callback:
#                 self.connection_status_callback(self.exchangename, "message")
            
#             # 원본 메시지 로깅
#             self.log_raw_message("orderbook", message, parsed["symbol"])

#     async def handle_parsed_message(self, parsed: dict) -> None:
#         """
#         파싱된 메시지 처리
        
#         Args:
#             parsed: 파싱된 오더북 데이터
#         """
#         try:
#             symbol = parsed["symbol"]
            
#             # 업비트는 매 메시지가 전체 오더북이므로, 항상 스냅샷으로 처리
#             result = await self.orderbook_manager.initialize_orderbook(symbol, parsed)
            
#             if result.is_valid:
#                 # 시퀀스 업데이트
#                 self.orderbook_manager.update_sequence(symbol, parsed["timestamp"])
                
#                 # 오더북 데이터 가져오기
#                 ob_dict = self.orderbook_manager.get_orderbook(symbol).to_dict()
                
#                 # 출력 큐에 추가
#                 if self.output_queue:
#                     await self.output_queue.put((self.exchangename, ob_dict))
#             else:
#                 self.log_error(f"{symbol} 오더북 업데이트 실패: {result.error_messages}")

#         except KeyError as e:
#             self.log_error(f"필수 필드 누락: {e}")
#         except Exception as e:
#             self.log_error(f"메시지 처리 중 오류: {e}")

#     async def stop(self) -> None:
#         """
#         웹소켓 연결 종료
#         """
#         self.log_info("웹소켓 연결 종료 중...")
#         await super().stop()
        
#         # 오더북 데이터 정리
#         self.orderbook_manager.clear_all()
        
#         self.log_info("웹소켓 연결 종료 완료")        
#         # 통계 로깅
#         self.log_info(
#             f"웹소켓 종료 | "
#             f"총 메시지: {self.message_stats['total_received']}개, "
#             f"유효 파싱: {self.parser.stats['valid_parsed']}개, "
#             f"핑퐁: {self.message_stats['ping_pong']}개, "
#             f"오류: {self.parser.stats['processing_errors']}개"
#         )
