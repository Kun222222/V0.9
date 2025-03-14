import os
import sys
print(f"Python 경로: {sys.path}")
print(f"현재 작업 디렉토리: {os.getcwd()}")

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger, LOG_DIRS

def test_logger():
    print("=== 로그 디렉토리 테스트 ===")
    print(f"로그 디렉토리 설정: {LOG_DIRS}")
    
    # 각 디렉토리 존재 여부 확인
    for dir_name, dir_path in LOG_DIRS.items():
        exists = os.path.exists(dir_path)
        print(f"{dir_name} 디렉토리 ({dir_path}): {'존재함' if exists else '없음'}")
        if exists:
            print(f"권한: {oct(os.stat(dir_path).st_mode)[-3:]}")
            try:
                test_file = os.path.join(dir_path, "test.txt")
                with open(test_file, "w") as f:
                    f.write("test")
                os.remove(test_file)
                print(f"쓰기 권한: 가능")
            except Exception as e:
                print(f"쓰기 권한: 실패 - {str(e)}")
    
    print("\n=== 로거 테스트 ===")
    logger = get_unified_logger()
    logger.info("테스트 로그 메시지")
    print("로그 메시지 전송 완료")

if __name__ == "__main__":
    test_logger() 