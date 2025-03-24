import unittest
import asyncio
import os
import sys
from unittest.mock import patch, MagicMock, AsyncMock

# 모듈 경로 추가 (프로젝트 루트 디렉토리에서 실행 가정)
sys.path.append(os.path.abspath('.'))

from crosskimp.services.orchestrator import Orchestrator
from crosskimp.services.process_manager import ProcessManager
from crosskimp.services.telegram_commander import TelegramCommander


class TestTelegramProcess(unittest.TestCase):
    """텔레그램 프로세스 시작 과정 테스트"""

    def setUp(self):
        """테스트 셋업"""
        # 테스트용 설정 및 의존성 처리
        self.event_bus_mock = MagicMock()
        self.event_bus_mock.register_handler = AsyncMock()
        self.event_bus_mock.publish = AsyncMock()
        
        # 텔레그램 봇 관련 모의 객체
        self.telegram_app_mock = MagicMock()
        self.telegram_app_mock.initialize = AsyncMock()
        self.telegram_app_mock.start = AsyncMock()
        self.telegram_app_mock.updater = MagicMock()
        self.telegram_app_mock.updater.start_polling = AsyncMock()
        
        # 환경 변수 설정을 위한 패치
        self.env_patcher = patch.dict('os.environ', {
            'TELEGRAM_BOT_TOKEN': 'test_token',
            'TELEGRAM_CHAT_ID': '1234567890'
        })
        self.env_patcher.start()
        
        # 설정 모의 객체
        self.config_mock = MagicMock()
        self.config_mock.get_env = MagicMock()
        self.config_mock.get_env.side_effect = lambda key, default=None: {
            'telegram.bot_token': 'test_token',
            'telegram.chat_id': '1234567890'
        }.get(key, default)

    def tearDown(self):
        """테스트 정리"""
        self.env_patcher.stop()

    @patch('crosskimp.services.telegram_commander.get_config')
    @patch('crosskimp.services.telegram_commander.get_event_bus')
    @patch('crosskimp.services.telegram_commander.Application')
    async def test_telegram_initialization(self, app_mock, get_event_bus_mock, get_config_mock):
        """텔레그램 초기화 과정 테스트"""
        # 모의 객체 설정
        get_config_mock.return_value = self.config_mock
        get_event_bus_mock.return_value = self.event_bus_mock
        app_mock.builder.return_value.token.return_value.build.return_value = self.telegram_app_mock
        
        # 텔레그램 커맨더 인스턴스 생성
        telegram = TelegramCommander()
        
        # 초기화 실행
        await telegram.initialize()
        
        # 텔레그램 봇 초기화 확인
        app_mock.builder.assert_called_once()
        self.telegram_app_mock.initialize.assert_called_once()
        self.telegram_app_mock.start.assert_called_once()
        self.telegram_app_mock.updater.start_polling.assert_called_once()
        
        # 초기화 상태 확인
        self.assertTrue(telegram.initialized)

    @patch('crosskimp.services.orchestrator.get_event_bus')
    @patch('crosskimp.services.orchestrator.get_process_manager')
    @patch('crosskimp.services.telegram_commander.get_telegram_commander')
    @patch('crosskimp.services.telegram_commander.get_config')
    @patch('crosskimp.services.telegram_commander.get_event_bus')
    @patch('crosskimp.services.telegram_commander.Application')
    async def test_telegram_process_start_via_orchestrator(
        self, app_mock, get_event_bus_tc_mock, get_config_mock, 
        get_telegram_mock, get_process_manager_mock, get_event_bus_orc_mock
    ):
        """오케스트레이터를 통한 텔레그램 프로세스 시작 테스트"""
        # 모의 객체 설정
        get_config_mock.return_value = self.config_mock
        get_event_bus_tc_mock.return_value = self.event_bus_mock
        get_event_bus_orc_mock.return_value = self.event_bus_mock
        app_mock.builder.return_value.token.return_value.build.return_value = self.telegram_app_mock
        
        # 텔레그램 커맨더 모의 객체 생성 및 설정
        telegram_mock = MagicMock()
        telegram_mock.initialize = AsyncMock()
        get_telegram_mock.return_value = telegram_mock
        
        # 프로세스 매니저 모의 객체 생성 및 설정
        process_manager_mock = MagicMock()
        process_manager_mock.start_process = AsyncMock(return_value=True)
        get_process_manager_mock.return_value = process_manager_mock
        
        # 오케스트레이터 인스턴스 생성
        orchestrator = Orchestrator()
        await orchestrator.initialize()
        
        # 텔레그램 프로세스 시작
        result = await orchestrator.start_process('telegram')
        
        # 프로세스 매니저를 통해 프로세스 시작 호출 확인
        process_manager_mock.start_process.assert_called_once()
        
        # 텔레그램 initialize 호출 확인
        telegram_mock.initialize.assert_called_once()
        
        # 시작 결과 확인
        self.assertTrue(result)
        self.assertIn('telegram', orchestrator.running_processes)


if __name__ == '__main__':
    asyncio.run(unittest.main()) 