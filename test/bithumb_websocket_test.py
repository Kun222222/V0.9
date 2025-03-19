import asyncio
import websockets
import json
import time
import hashlib
from datetime import datetime

async def test_bithumb_websocket():
    """빗썸 웹소켓 API에서 중복 메시지 발생 여부를 테스트합니다."""
    # 빗썸 웹소켓 URL
    ws_url = "wss://ws-api.bithumb.com/websocket/v1"
    
    # 로그 파일 설정
    log_file = "bithumb_test_log.txt"
    
    # 메시지 ID 추적을 위한 딕셔너리
    message_counts = {}
    last_timestamps = {}
    
    # 메시지 해시값 저장 (내용 비교용)
    message_hashes = {}
    content_duplicates = 0
    
    print(f"테스트 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"로그 파일: {log_file}")
    print("웹소켓 연결 중...")
    
    try:
        # 웹소켓 연결 및 메시지 수신
        async with websockets.connect(ws_url) as websocket:
            print("웹소켓 연결 성공")
            
            # 구독 메시지 생성 (XRP 선택)
            subscribe_message = [
                {"ticket": f"bithumb_test_{int(time.time() * 1000)}"},
                {"type": "orderbook", "codes": ["KRW-XRP"]},
                {"format": "DEFAULT"}
            ]
            
            # 구독 메시지 전송
            await websocket.send(json.dumps(subscribe_message))
            print(f"구독 요청 전송: {json.dumps(subscribe_message)}")
            
            # 테스트 시작 메시지 로깅
            with open(log_file, "w", encoding="utf-8") as f:
                f.write(f"===== 빗썸 웹소켓 중복 메시지 테스트 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====\n")
                f.write(f"구독 요청: {json.dumps(subscribe_message)}\n\n")
            
            # 30초 동안 메시지 수신하며 중복 여부 확인
            start_time = time.time()
            message_counter = 0
            
            while time.time() - start_time < 30:
                try:
                    # 메시지 수신
                    message = await websocket.recv()
                    message_counter += 1
                    
                    # 현재 시간 기록
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    
                    # 메시지 해시값 계산 (전체 내용 비교용)
                    msg_hash = hashlib.md5(message).hexdigest()
                    
                    # 메시지 해시값으로 중복 확인
                    if msg_hash in message_hashes:
                        content_duplicates += 1
                        content_dup_info = f"완전 동일한 메시지 감지! 이전 메시지 번호: #{message_hashes[msg_hash]}, 현재 번호: #{message_counter}"
                        print(f"🔄 {content_dup_info}")
                        
                        with open(log_file, "a", encoding="utf-8") as f:
                            f.write(f"[{current_time}] CONTENT_DUPLICATE: {content_dup_info}\n")
                    else:
                        message_hashes[msg_hash] = message_counter
                    
                    # bytes를 문자열로 변환
                    message_str = message.decode('utf-8')
                    
                    # JSON 파싱 시도
                    try:
                        data = json.loads(message_str)
                        
                        # orderbook 타입 메시지만 분석
                        if isinstance(data, dict) and data.get("type") == "orderbook":
                            # 타임스탬프 추출
                            timestamp = data.get("timestamp")
                            stream_type = data.get("stream_type", "")
                            code = data.get("code", "")
                            
                            # 메시지 식별 정보
                            msg_info = f"Code: {code}, Type: {stream_type}, Timestamp: {timestamp}"
                            
                            # 로그 파일에 메시지 기록
                            with open(log_file, "a", encoding="utf-8") as f:
                                f.write(f"[{current_time}] #{message_counter} {msg_info}\n")
                                
                            print(f"#{message_counter} {msg_info}")
                            
                            # 타임스탬프 중복 여부 확인
                            key = f"{code}_{timestamp}"
                            if key in message_counts:
                                message_counts[key] += 1
                                duplicate_info = f"중복 타임스탬프 감지! 코드: {code}, 타임스탬프: {timestamp}, 횟수: {message_counts[key]}"
                                print(f"⚠️ {duplicate_info}")
                                
                                with open(log_file, "a", encoding="utf-8") as f:
                                    f.write(f"[{current_time}] TIMESTAMP_DUPLICATE: {duplicate_info}\n")
                            else:
                                message_counts[key] = 1
                            
                            # 타임스탬프 흐름 확인
                            if code in last_timestamps:
                                if timestamp < last_timestamps[code]:
                                    # 타임스탬프 역전 현상 감지
                                    reverse_info = f"타임스탬프 역전! 코드: {code}, 이전: {last_timestamps[code]}, 현재: {timestamp}"
                                    print(f"⚠️ {reverse_info}")
                                    
                                    with open(log_file, "a", encoding="utf-8") as f:
                                        f.write(f"[{current_time}] TIMESTAMP_REVERSE: {reverse_info}\n")
                                elif timestamp == last_timestamps[code]:
                                    # 동일 타임스탬프 감지 (위의 중복 체크와 중복될 수 있음)
                                    same_info = f"동일 타임스탬프! 코드: {code}, 타임스탬프: {timestamp}"
                                    print(f"ℹ️ {same_info}")
                            
                            # 마지막 타임스탬프 업데이트
                            last_timestamps[code] = timestamp
                            
                            # 주문 데이터를 파일에 저장 (5개마다 한 번씩)
                            if message_counter % 5 == 0:
                                # 메시지 내용 저장 
                                data_file = f"bithumb_msg_{message_counter}.json"
                                with open(data_file, "w", encoding="utf-8") as f:
                                    json.dump(data, f, indent=2)
                                
                                with open(log_file, "a", encoding="utf-8") as f:
                                    f.write(f"[{current_time}] 메시지 #{message_counter} 저장: {data_file}\n")
                        else:
                            # orderbook 외 다른 메시지 타입 처리
                            with open(log_file, "a", encoding="utf-8") as f:
                                f.write(f"[{current_time}] #{message_counter} 기타 메시지: {message_str[:200]}...\n")
                            
                    except json.JSONDecodeError:
                        print(f"JSON 파싱 실패: {message_str[:100]}...")
                        with open(log_file, "a", encoding="utf-8") as f:
                            f.write(f"[{current_time}] JSON_ERROR: {message_str[:200]}\n")
                
                except Exception as e:
                    print(f"오류 발생: {str(e)}")
                    with open(log_file, "a", encoding="utf-8") as f:
                        f.write(f"[{current_time}] ERROR: {str(e)}\n")
            
            # 테스트 결과 요약
            print("\n===== 테스트 결과 요약 =====")
            
            # 중복 메시지 통계
            duplicate_keys = [k for k, v in message_counts.items() if v > 1]
            duplicate_count = len(duplicate_keys)
            total_unique = len(message_counts)
            total_messages = sum(message_counts.values())
            
            print(f"총 메시지 수: {total_messages}")
            print(f"고유 메시지 수 (타임스탬프 기준): {total_unique}")
            print(f"타임스탬프 중복 메시지 수: {duplicate_count}")
            print(f"완전히 동일한 내용 중복 메시지 수: {content_duplicates}")
            
            if duplicate_count > 0:
                print("\n타임스탬프 중복 메시지 상세:")
                for key in duplicate_keys:
                    code, timestamp = key.split('_')
                    print(f"  코드: {code}, 타임스탬프: {timestamp}, 횟수: {message_counts[key]}")
            
            # 로그 파일에 요약 정보 저장
            with open(log_file, "a", encoding="utf-8") as f:
                f.write("\n===== 테스트 결과 요약 =====\n")
                f.write(f"테스트 종료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"총 메시지 수: {total_messages}\n")
                f.write(f"고유 메시지 수 (타임스탬프 기준): {total_unique}\n")
                f.write(f"타임스탬프 중복 메시지 수: {duplicate_count}\n")
                f.write(f"완전히 동일한 내용 중복 메시지 수: {content_duplicates}\n")
                
                if duplicate_count > 0:
                    f.write("\n타임스탬프 중복 메시지 상세:\n")
                    for key in duplicate_keys:
                        code, timestamp = key.split('_')
                        f.write(f"  코드: {code}, 타임스탬프: {timestamp}, 횟수: {message_counts[key]}\n")
    
    except Exception as e:
        print(f"테스트 실행 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    print("빗썸 웹소켓 중복 메시지 테스트를 시작합니다...")
    asyncio.run(test_bithumb_websocket())
    print("테스트 완료!")