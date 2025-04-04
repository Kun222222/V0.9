"""
레이더 모듈

오더북 데이터를 수신하고 분석하여 차익거래 기회를 찾는 모듈입니다.
"""

from crosskimp.radar.processor import RadarProcessor

# 싱글톤 인스턴스
_radar_processor_instance = None

def get_radar_processor() -> RadarProcessor:
    """
    레이더 프로세서 싱글톤 인스턴스 반환
    
    Returns:
        RadarProcessor: 레이더 프로세서 인스턴스
    """
    global _radar_processor_instance
    
    if _radar_processor_instance is None:
        _radar_processor_instance = RadarProcessor()
    
    return _radar_processor_instance

# 주요 클래스 노출
__all__ = ['get_radar_processor', 'RadarProcessor']
