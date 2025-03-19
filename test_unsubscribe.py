import inspect
from src.crosskimp.ob_collector.orderbook.subscription.base_subscription import BaseSubscription
from src.crosskimp.ob_collector.orderbook.subscription.bithumb_s_sub import BithumbSubscription 
from src.crosskimp.ob_collector.orderbook.subscription.upbit_s_sub import UpbitSubscription
from src.crosskimp.ob_collector.orderbook.subscription.bybit_s_sub import BybitSubscription
from src.crosskimp.ob_collector.orderbook.subscription.bybit_f_sub import BybitFutureSubscription

# 함수 소스코드 비교
def compare_source(cls):
    print(f"{cls.__name__} 클래스:")
    try:
        source = inspect.getsource(cls.unsubscribe)
        # 간단하게 처음 두 줄만 출력
        source_lines = source.strip().split('\n')[:2]
        print(f"  소스코드: {' '.join(source_lines)}")
    except Exception as e:
        print(f"  소스코드 추출 실패: {e}")
    print()

# 각 클래스의 메소드 소스코드 출력
print("부모 클래스와 자식 클래스들의 unsubscribe 메서드 구현 비교:\n")
compare_source(BaseSubscription)
compare_source(BithumbSubscription)
compare_source(UpbitSubscription)
compare_source(BybitSubscription)
compare_source(BybitFutureSubscription) 