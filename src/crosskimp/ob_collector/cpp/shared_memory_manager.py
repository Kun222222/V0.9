"""
공유 메모리 관리자 모듈

이 모듈은 오더북 데이터를 공유 메모리에 저장하고 관리하는 기능을 제공합니다.
"""

import os
import mmap
import struct
import time
import threading
import posix_ipc
from typing import Dict, Optional, Tuple, Any

from crosskimp.logger.logger import get_unified_logger

# 로거 인스턴스 가져오기
logger = get_unified_logger()

# 공유 메모리 관련 상수
SHARED_MEMORY_NAME = "orderbook_shared_memory"
SEMAPHORE_NAME = "orderbook_semaphore"
HEADER_SIZE = 64  # 헤더 크기 (바이트)
MAX_BUFFER_SIZE = 100 * 1024 * 1024  # 최대 버퍼 크기 (100MB)
DOUBLE_BUFFER_COUNT = 2  # 더블 버퍼링을 위한 버퍼 개수

# 헤더 형식: 
# - magic (4바이트): 매직 넘버 ('SMHR')
# - buffer_index (4바이트): 현재 활성 버퍼 인덱스
# - data_size (4바이트): 데이터 크기
# - timestamp (8바이트): 타임스탬프
# - status_flags (4바이트): 상태 플래그
# - reserved (40바이트): 예약 영역
HEADER_FORMAT = "=4sIIQIx40s"
HEADER_MAGIC = b'SMHR'  # 공유 메모리 헤더 매직 넘버

# 상태 플래그 정의
STATUS_READY = 0x01  # 데이터 준비됨
STATUS_PROCESSING = 0x02  # 데이터 처리 중
STATUS_ERROR = 0x04  # 오류 발생

class SharedMemoryManager:
    """
    공유 메모리 관리자 클래스
    
    이 클래스는 오더북 데이터를 공유 메모리에 저장하고 관리하는 기능을 제공합니다.
    - 공유 메모리 세그먼트 생성 및 관리
    - 메모리 매핑 헤더 관리
    - 세마포어를 통한 동기화 처리
    - 더블 버퍼링 구현
    """
    
    def __init__(self, name: str = SHARED_MEMORY_NAME, size: int = MAX_BUFFER_SIZE):
        """
        공유 메모리 관리자 초기화
        
        Args:
            name: 공유 메모리 이름
            size: 공유 메모리 크기 (바이트)
        """
        self.name = name
        self.size = size
        self.total_size = HEADER_SIZE + (size * DOUBLE_BUFFER_COUNT)
        
        self.memory = None
        self.mmap = None
        self.semaphore = None
        self.is_initialized = False
        self.lock = threading.Lock()
        self.current_buffer_index = 0
        
        # 메트릭 초기화
        self.metrics = {
            "write_count": 0,
            "write_errors": 0,
            "total_bytes_written": 0,
            "last_write_time": 0,
            "max_data_size": 0,
            "buffer_utilization": 0.0,
        }
    
    def initialize(self) -> bool:
        """
        공유 메모리 초기화
        
        Returns:
            bool: 초기화 성공 여부
        """
        try:
            # 이미 초기화된 경우 재사용
            if self.is_initialized:
                return True
            
            # 기존 공유 메모리 및 세마포어 정리
            self._cleanup_existing_resources()
            
            # 공유 메모리 생성
            self.memory = posix_ipc.SharedMemory(
                self.name,
                posix_ipc.O_CREAT,
                size=self.total_size
            )
            
            # 메모리 매핑
            self.mmap = mmap.mmap(
                self.memory.fd,
                self.total_size,
                mmap.MAP_SHARED,
                mmap.PROT_WRITE | mmap.PROT_READ
            )
            
            # 파일 디스크립터 닫기 (메모리 매핑 후에는 필요 없음)
            os.close(self.memory.fd)
            
            # 세마포어 생성
            self.semaphore = posix_ipc.Semaphore(
                SEMAPHORE_NAME,
                posix_ipc.O_CREAT,
                initial_value=1
            )
            
            # 헤더 초기화
            self._initialize_header()
            
            self.is_initialized = True
            logger.info(f"공유 메모리 초기화 완료: {self.name} (크기: {self.total_size:,} 바이트)")
            return True
            
        except Exception as e:
            logger.error(f"공유 메모리 초기화 실패: {str(e)}", exc_info=True)
            self._cleanup()
            return False
    
    def _cleanup_existing_resources(self):
        """기존 공유 메모리 및 세마포어 정리"""
        try:
            # 기존 공유 메모리 제거 시도
            try:
                existing_memory = posix_ipc.SharedMemory(self.name)
                existing_memory.unlink()
                logger.info(f"기존 공유 메모리 제거: {self.name}")
            except posix_ipc.ExistentialError:
                pass  # 존재하지 않는 경우 무시
            
            # 기존 세마포어 제거 시도
            try:
                existing_semaphore = posix_ipc.Semaphore(SEMAPHORE_NAME)
                existing_semaphore.unlink()
                logger.info(f"기존 세마포어 제거: {SEMAPHORE_NAME}")
            except posix_ipc.ExistentialError:
                pass  # 존재하지 않는 경우 무시
                
        except Exception as e:
            logger.warning(f"기존 리소스 정리 중 오류 발생: {str(e)}")
    
    def _initialize_header(self):
        """헤더 초기화"""
        with self.lock:
            self.mmap.seek(0)
            header = struct.pack(
                HEADER_FORMAT,
                HEADER_MAGIC,  # 매직 넘버
                0,  # buffer_index
                0,  # data_size
                int(time.time() * 1000),  # timestamp
                0,  # status_flags
                b''  # reserved
            )
            self.mmap.write(header)
            logger.debug(f"헤더 초기화 완료: 매직={HEADER_MAGIC}, 버퍼 인덱스=0, 데이터 크기=0")
    
    def write_data(self, data: bytes) -> bool:
        """
        데이터를 공유 메모리에 쓰기
        
        Args:
            data: 쓸 데이터 (바이트)
            
        Returns:
            bool: 쓰기 성공 여부
        """
        if not self.is_initialized:
            if not self.initialize():
                return False
        
        try:
            # 데이터 크기 검증
            data_size = len(data)
            # logger.debug(f"공유 메모리 쓰기 시작: 데이터 크기={data_size} 바이트, 최대 버퍼 크기={self.size} 바이트")
            
            if data_size > self.size:
                logger.error(f"데이터 크기가 버퍼 크기를 초과: {data_size} > {self.size}")
                self.metrics["write_errors"] += 1
                return False
            
            # FlatBuffers 데이터 검증
            if data_size >= 8:
                # 파일 식별자 확인 (4-8 바이트에 "ORDB" 있어야 함)
                file_id = data[4:8]
                expected_file_id = b'ORDB'
                
                if file_id != expected_file_id:
                    logger.error(f"잘못된 FlatBuffers 파일 식별자: {file_id} (예상: {expected_file_id})")
                    
                    # 파일 식별자 수정 시도
                    data_list = bytearray(data)
                    data_list[4:8] = expected_file_id
                    data = bytes(data_list)
                    logger.debug(f"파일 식별자 수정 후: {data[4:8]}")
                
                # 루트 오프셋 확인
                root_offset = struct.unpack("<I", data[0:4])[0]
                if root_offset == 0 or root_offset >= data_size:
                    logger.error(f"잘못된 FlatBuffers 루트 오프셋: {root_offset}")
                    self.metrics["write_errors"] += 1
                    return False
            
            # 세마포어 획득
            # logger.debug(f"세마포어 획득 시도")
            try:
                self.semaphore.acquire(timeout=0.5)  # 타임아웃 추가 (0.5초)
            except posix_ipc.BusyError:
                logger.error("세마포어 획득 타임아웃")
                self.metrics["write_errors"] += 1
                return False
                
            # logger.debug(f"세마포어 획득 성공")
            
            try:
                with self.lock:
                    # 다음 버퍼 인덱스 계산 (더블 버퍼링)
                    next_buffer_index = (self.current_buffer_index + 1) % DOUBLE_BUFFER_COUNT
                    
                    # 버퍼 오프셋 계산
                    buffer_offset = HEADER_SIZE + (next_buffer_index * self.size)
                    # logger.debug(f"버퍼 인덱스={next_buffer_index}, 오프셋={buffer_offset}")
                    
                    # 데이터 쓰기
                    self.mmap.seek(buffer_offset)
                    self.mmap.write(data)
                    self.mmap.flush()  # 전체 메모리 플러시
                    # logger.debug(f"데이터 쓰기 완료: 오프셋={buffer_offset}, 크기={data_size}")
                    
                    # 데이터 헤더 디버깅
                    data_header = ' '.join([f'{b:02x}' for b in data[:16]])
                    # logger.debug(f"데이터 헤더: {data_header}")
                    
                    # 헤더 업데이트
                    self.mmap.seek(0)
                    current_time = int(time.time() * 1000)
                    header = struct.pack(
                        HEADER_FORMAT,
                        HEADER_MAGIC,  # 매직 넘버
                        next_buffer_index,  # buffer_index
                        data_size,  # data_size
                        current_time,  # timestamp
                        STATUS_READY,  # status_flags
                        b''  # reserved
                    )
                    self.mmap.write(header)
                    self.mmap.flush()  # 전체 메모리 플러시
                    
                    # 헤더 디버깅
                    header_hex = ' '.join([f'{b:02x}' for b in header[:16]])
                    # logger.debug(f"헤더 업데이트 완료: {header_hex}")
                    
                    # 현재 버퍼 인덱스 업데이트
                    self.current_buffer_index = next_buffer_index
                    
                    # 메트릭 업데이트
                    self.metrics["write_count"] += 1
                    self.metrics["total_bytes_written"] += data_size
                    self.metrics["last_write_time"] = time.time()
                    self.metrics["max_data_size"] = max(self.metrics["max_data_size"], data_size)
                    self.metrics["buffer_utilization"] = data_size / self.size
                    
                    # logger.info(f"공유 메모리 쓰기 성공: 크기={data_size} 바이트, 버퍼 인덱스={next_buffer_index}, 버퍼 사용률={self.metrics['buffer_utilization']:.2%}")
                    return True
                    
            finally:
                # 세마포어 해제
                self.semaphore.release()
                logger.debug(f"세마포어 해제 완료")
                
        except Exception as e:
            logger.error(f"데이터 쓰기 실패: {str(e)}", exc_info=True)
            self.metrics["write_errors"] += 1
            
            # 세마포어가 획득된 경우에만 해제
            if 'self.semaphore' in locals() and self.semaphore is not None:
                try:
                    self.semaphore.release()
                    logger.debug("오류 처리: 세마포어 해제 완료")
                except:
                    pass
                    
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        메트릭 정보 반환
        
        Returns:
            Dict[str, Any]: 메트릭 정보
        """
        return self.metrics
    
    def _cleanup(self):
        """리소스 정리"""
        try:
            if self.mmap:
                self.mmap.close()
                self.mmap = None
            
            if self.memory:
                self.memory.close_fd()
                try:
                    self.memory.unlink()
                except:
                    pass
                self.memory = None
            
            if self.semaphore:
                try:
                    self.semaphore.unlink()
                except:
                    pass
                self.semaphore = None
            
            self.is_initialized = False
            
        except Exception as e:
            logger.error(f"리소스 정리 중 오류 발생: {str(e)}", exc_info=True)
    
    def __del__(self):
        """소멸자"""
        self._cleanup()


# 싱글톤 인스턴스
_shared_memory_manager = None

def get_shared_memory_manager() -> SharedMemoryManager:
    """
    공유 메모리 관리자 싱글톤 인스턴스 반환
    
    Returns:
        SharedMemoryManager: 공유 메모리 관리자 인스턴스
    """
    global _shared_memory_manager
    if _shared_memory_manager is None:
        _shared_memory_manager = SharedMemoryManager()
    return _shared_memory_manager 