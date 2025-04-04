"""
크로스 김프 아비트리지 - Redis 상수 모듈

이 모듈은 Redis 연결 및 채널 관련 상수를 정의합니다.
시스템 전체에서 일관된 Redis 사용을 위한 중앙 설정 파일입니다.
"""

class RedisConfig:
    """
    Redis 연결 설정
    
    Redis 서버 연결에 사용되는 기본 설정값을 정의합니다.
    """
    HOST = "localhost"
    PORT = 6379
    DB = 0
    PASSWORD = None
    SOCKET_TIMEOUT = 5.0
    SOCKET_CONNECT_TIMEOUT = 10.0
    SOCKET_KEEPALIVE = True
    RETRY_ON_TIMEOUT = True
    MAX_CONNECTIONS = 10  # 연결 풀 최대 크기
    
    # 환경별 설정 (개발/테스트/운영)
    class Development:
        """개발 환경 설정"""
        HOST = "localhost"
        PORT = 6379
        DB = 0
        
    class Test:
        """테스트 환경 설정"""
        HOST = "localhost"
        PORT = 6379
        DB = 1
        
    class Production:
        """운영 환경 설정"""
        HOST = "localhost"  # 실제 운영에서는 변경 필요
        PORT = 6379
        DB = 0
        # PASSWORD는 환경변수에서 로드 권장


class RedisChannels:
    """
    Redis 채널 정의
    
    Redis PubSub 및 Streams에 사용되는 채널 이름을 정의합니다.
    일관된 채널 이름 사용을 위해 이 클래스의 상수를 사용하세요.
    """
    # 채널 네임스페이스 접두사
    PREFIX = "crosskimp"
    
    # 이벤트 버스 채널 (PubSub)
    EVENT_BUS_PREFIX = f"{PREFIX}:events"
    
    # 시스템 관련 채널
    SYSTEM_STATUS = f"{PREFIX}:system:status"
    SYSTEM_ERROR = f"{PREFIX}:system:error"
    
    # 프로세스 관련 채널
    PROCESS_COMMAND = f"{PREFIX}:process:command"
    PROCESS_STATUS = f"{PREFIX}:process:status"
    
    # 컴포넌트별 채널
    ORDERBOOK_DATA = f"{PREFIX}:data:orderbook"
    ORDERBOOK_STATUS = f"{PREFIX}:status:orderbook"
    RADAR_DATA = f"{PREFIX}:data:radar"
    RADAR_STATUS = f"{PREFIX}:status:radar"


class RedisKeys:
    """
    Redis 키 패턴 정의
    
    Redis 데이터 저장에 사용되는 키 패턴을 정의합니다.
    일관된 키 사용을 위해 이 클래스의 상수와 메서드를 사용하세요.
    """
    # 키 네임스페이스 접두사
    PREFIX = "crosskimp"
    
    # 오더북 데이터 키
    ORDERBOOK_TEMPLATE = f"{PREFIX}:orderbook:{{exchange}}:{{symbol}}"
    
    # 레이더 데이터 키
    RADAR_OPPORTUNITY_TEMPLATE = f"{PREFIX}:radar:opportunity:{{id}}"
    RADAR_ACTIVE_OPPORTUNITIES = f"{PREFIX}:radar:opportunities:active"
    
    # 시스템 설정 및 상태 키
    SYSTEM_CONFIG = f"{PREFIX}:config:system"
    PROCESS_STATUS_TEMPLATE = f"{PREFIX}:status:process:{{process_name}}"
    
    # 캐시 키
    CACHE_TEMPLATE = f"{PREFIX}:cache:{{name}}"
    
    @staticmethod
    def orderbook_key(exchange: str, symbol: str) -> str:
        """오더북 데이터 키 생성"""
        return RedisKeys.ORDERBOOK_TEMPLATE.format(exchange=exchange, symbol=symbol)
    
    @staticmethod
    def process_status_key(process_name: str) -> str:
        """프로세스 상태 키 생성"""
        return RedisKeys.PROCESS_STATUS_TEMPLATE.format(process_name=process_name)
    
    @staticmethod
    def cache_key(name: str) -> str:
        """캐시 키 생성"""
        return RedisKeys.CACHE_TEMPLATE.format(name=name)


class RedisStreamConfig:
    """
    Redis Streams 관련 설정
    
    Redis Streams 기능 사용에 관련된 설정을 정의합니다.
    """
    # 스트림 데이터 최대 길이
    MAX_LEN = 1000
    
    # 스트림 소비자 그룹 이름
    CONSUMER_GROUP = "crosskimp-consumers"
    
    # 스트림 처리 배치 크기
    BATCH_SIZE = 10 