#!/usr/bin/env python
"""
텔레그램 봇 실행 상태 확인 스크립트

이 스크립트는 현재 실행 중인 텔레그램 봇 인스턴스를 확인하고
충돌 문제의 원인을 진단합니다.
"""

import os
import sys
import asyncio
import logging
import traceback
import psutil
import json
from datetime import datetime
import signal

# 프로젝트 루트 디렉토리를 시스템 경로에 추가
src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(src_dir)
print(f"추가된 경로: {src_dir}")

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger('bot_checker')

async def check_bot_connection():
    """텔레그램 봇 연결 상태 확인"""
    logger.info("==== 텔레그램 봇 연결 상태 확인 ====")
    
    try:
        # 설정 확인
        from crosskimp.common.config.app_config import get_config
        config = get_config()
        
        telegram_token = config.get_env("telegram.bot_token")
        
        if not telegram_token:
            logger.error("텔레그램 토큰이 설정되지 않았습니다.")
            return False
        
        # 직접 봇 객체 생성하여 연결 테스트
        from telegram import Bot
        
        logger.info("봇 객체 생성 중...")
        bot = Bot(telegram_token)
        
        logger.info("봇 정보 조회 중...")
        bot_info = await bot.get_me()
        logger.info(f"봇 정보: {bot_info.username} (ID: {bot_info.id})")
        
        # 웹훅 설정 확인 (웹훅이 설정되어 있으면 polling 모드에서 충돌 발생)
        logger.info("웹훅 설정 확인 중...")
        webhook_info = await bot.get_webhook_info()
        
        if webhook_info.url:
            logger.error(f"웹훅이 설정되어 있습니다: {webhook_info.url}")
            logger.error("웹훅과 polling 모드를 동시에 사용할 수 없습니다.")
            logger.info("웹훅 삭제 시도 중...")
            
            # 웹훅 삭제
            success = await bot.delete_webhook()
            logger.info(f"웹훅 삭제 결과: {'성공' if success else '실패'}")
        else:
            logger.info("웹훅이 설정되어 있지 않습니다.")
        
        return True
    except Exception as e:
        logger.error(f"봇 연결 확인 중 오류 발생: {str(e)}")
        logger.error(traceback.format_exc())
        return False

def check_running_processes():
    """텔레그램 관련 프로세스 확인"""
    logger.info("==== 실행 중인 프로세스 확인 ====")
    
    bot_processes = []
    python_processes = []
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            # Python 프로세스 필터링
            if proc.info['name'] == 'python' or proc.info['name'] == 'python3':
                cmdline = " ".join(proc.info['cmdline']) if proc.info['cmdline'] else ""
                python_processes.append({
                    'pid': proc.info['pid'],
                    'cmdline': cmdline
                })
                
                # 텔레그램 관련 프로세스 감지
                if 'telegram' in cmdline.lower() or 'bot' in cmdline.lower():
                    bot_processes.append({
                        'pid': proc.info['pid'],
                        'cmdline': cmdline,
                        'create_time': datetime.fromtimestamp(proc.create_time()).strftime('%Y-%m-%d %H:%M:%S')
                    })
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    
    logger.info(f"현재 실행 중인 Python 프로세스: {len(python_processes)}개")
    
    if bot_processes:
        logger.info(f"텔레그램 관련 프로세스 감지: {len(bot_processes)}개")
        for i, proc in enumerate(bot_processes, 1):
            logger.info(f"  {i}. PID {proc['pid']} - 시작 시간: {proc['create_time']}")
            logger.info(f"     명령어: {proc['cmdline']}")
    else:
        logger.info("텔레그램 관련 프로세스가 감지되지 않았습니다.")
    
    return bot_processes

def check_system_resources():
    """시스템 리소스 상태 확인"""
    logger.info("==== 시스템 리소스 상태 확인 ====")
    
    # CPU 사용량
    cpu_percent = psutil.cpu_percent(interval=1)
    logger.info(f"CPU 사용량: {cpu_percent}%")
    
    # 메모리 사용량
    memory = psutil.virtual_memory()
    logger.info(f"메모리 사용량: {memory.percent}% (사용 중: {memory.used / (1024**3):.2f}GB, 전체: {memory.total / (1024**3):.2f}GB)")
    
    # 디스크 사용량
    disk = psutil.disk_usage('/')
    logger.info(f"디스크 사용량: {disk.percent}% (사용 중: {disk.used / (1024**3):.2f}GB, 전체: {disk.total / (1024**3):.2f}GB)")

def check_log_files():
    """로그 파일 확인"""
    logger.info("==== 로그 파일 확인 ====")
    
    log_dir = os.path.join(src_dir, 'src', 'logs')
    
    if not os.path.exists(log_dir):
        logger.error(f"로그 디렉토리가 존재하지 않습니다: {log_dir}")
        return
    
    log_files = sorted([f for f in os.listdir(log_dir) if f.endswith('.log')], reverse=True)
    
    if not log_files:
        logger.error("로그 파일이 존재하지 않습니다.")
        return
    
    logger.info(f"최근 로그 파일 ({min(5, len(log_files))}개):")
    
    for i, log_file in enumerate(log_files[:5], 1):
        log_path = os.path.join(log_dir, log_file)
        file_size = os.path.getsize(log_path) / 1024  # KB
        modified_time = datetime.fromtimestamp(os.path.getmtime(log_path)).strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"  {i}. {log_file} - 크기: {file_size:.2f}KB, 수정 시간: {modified_time}")
        
        # 로그 파일 마지막 10줄 확인
        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                logger.info(f"  마지막 로그 ({min(10, len(lines))}줄):")
                for line in lines[-10:]:
                    if 'telegram' in line.lower() or 'error' in line.lower() or 'exception' in line.lower():
                        logger.info(f"  > {line.strip()}")
        except Exception as e:
            logger.error(f"  로그 파일 읽기 실패: {str(e)}")

def check_env_vars():
    """텔레그램 관련 환경 변수 확인"""
    logger.info("==== 환경 변수 확인 ====")
    
    telegram_vars = {key: value for key, value in os.environ.items() if 'telegram' in key.lower()}
    
    if telegram_vars:
        logger.info(f"텔레그램 관련 환경 변수 ({len(telegram_vars)}개):")
        for key, value in telegram_vars.items():
            # 토큰 마스킹
            masked_value = value
            if 'token' in key.lower():
                masked_value = f"...{value[-5:]}" if len(value) > 5 else "***"
            logger.info(f"  {key}: {masked_value}")
    else:
        logger.info("텔레그램 관련 환경 변수가 설정되지 않았습니다.")

def show_diagnostic_info():
    """종합 진단 정보 표시"""
    logger.info("==== 종합 진단 정보 ====")
    
    # 시스템 정보
    logger.info(f"운영체제: {sys.platform}")
    logger.info(f"Python 버전: {sys.version}")
    
    # 작업 디렉토리
    logger.info(f"현재 작업 디렉토리: {os.getcwd()}")
    
    # Python 경로
    logger.info("Python 경로:")
    for i, path in enumerate(sys.path, 1):
        logger.info(f"  {i}. {path}")

async def main():
    """메인 함수"""
    logger.info("===== 텔레그램 봇 진단 시작 =====")
    
    # 시스템 리소스 확인
    check_system_resources()
    
    # 실행 중인 프로세스 확인
    bot_processes = check_running_processes()
    
    # 환경 변수 확인
    check_env_vars()
    
    # 종합 진단 정보
    show_diagnostic_info()
    
    # 봇 연결 확인
    await check_bot_connection()
    
    # 로그 파일 확인
    check_log_files()
    
    logger.info("===== 텔레그램 봇 진단 완료 =====")
    
    # 문제 해결 방법 제안
    if bot_processes:
        logger.info("\n==== 문제 해결 방법 ====")
        logger.info("1. 여러 봇 인스턴스가 실행 중인 것으로 보입니다. 다음 명령으로 중지해 보세요:")
        for proc in bot_processes:
            logger.info(f"   kill {proc['pid']}  # PID {proc['pid']} 프로세스 종료")
        logger.info("2. 모든 봇 프로세스를 종료한 후 다시 시작해 보세요.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        logger.error(f"테스트 실행 중 예외 발생: {str(e)}")
        logger.error(traceback.format_exc()) 