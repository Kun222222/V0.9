# file: orderbook/common/market_price_monitor.py

import asyncio
import time
from typing import Dict, Optional
from datetime import datetime

from crosskimp.ob_collector.utils.logging.logger import get_unified_logger
from crosskimp.ob_collector.utils.config.config_loader import get_settings

class MarketPriceMonitor:
    """
    각 거래소의 오더북 데이터를 모니터링하여 시장가격을 계산하고 로깅
    - 설정된 매수금액 기준으로 시장가 계산
    - 모든 거래소의 매수/매도 평균가 계산
    """
    def __init__(self):
        self.settings = get_settings()
        self.base_order_amount = self.settings["trading"]["general"]["base_order_amount_krw"]
        self.logger = get_unified_logger()
        
        # 각 거래소별 최신 오더북 데이터 저장
        self.orderbooks = {
            "binance": {},
            "binancefuture": {},
            "bybit": {},
            "bybitfuture": {},
            "upbit": {},
            "bithumb": {}
        }
        
        # 마지막 계산 시간
        self.last_calculation = 0.0
        self.calculation_interval = 1.0  # 1초마다 계산
        
        # USDT/KRW 환율
        self.usdt_krw_rate = 1300.0  # 기본값
        
        self.logger.info(
            f"[MarketPrice] 모니터 초기화 | "
            f"base_amount={self.base_order_amount:,} KRW"
        )

    def update_orderbook(self, exchange: str, data: dict) -> None:
        """
        각 거래소의 오더북 데이터 업데이트
        """
        try:
            symbol = data.get("symbol")
            if not symbol:
                return
                
            # 오더북 데이터 저장
            if exchange not in self.orderbooks:
                self.orderbooks[exchange] = {}
                
            # 모든 메시지 즉시 로깅
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 업비트 USDT 가격 확인 및 업데이트
            if exchange == "upbit" and symbol == "USDT":
                if data.get("bids"):
                    self.usdt_krw_rate = float(data["bids"][0][0])
            
            # 가격 데이터 추출
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            
            if not bids and not asks:
                return
                
            try:
                # timestamp 변환
                ts = data.get("timestamp", int(time.time() * 1000))
                if ts > 1000000000000000:  # 마이크로초
                    ts = ts / 1000000
                elif ts > 1000000000000:  # 밀리초
                    ts = ts / 1000
                timestamp_str = datetime.fromtimestamp(ts).strftime("%H:%M:%S.%f")[:-3]
                
                # 최우선 호가 기준 가격 계산
                if bids and asks:
                    buy_price = float(asks[0][0])  # 매수 가격은 매도호가
                    sell_price = float(bids[0][0])  # 매도 가격은 매수호가
                    
                    # 해외 거래소는 KRW로 변환
                    if exchange in ["binance", "binancefuture", "bybit", "bybitfuture"]:
                        buy_price *= self.usdt_krw_rate
                        sell_price *= self.usdt_krw_rate
                    
                    log_data = {
                        "timestamp": current_time,
                        "exchange": exchange,
                        "symbol": symbol,
                        "buy": f"{buy_price:,.0f}",
                        "sell": f"{sell_price:,.0f}",
                        "orderbook_time": timestamp_str,
                        "type": data.get("type", "unknown")
                    }
                    
                    if exchange in ["binance", "binancefuture", "bybit", "bybitfuture"]:
                        log_data["usdt_rate"] = self.usdt_krw_rate
                    
                    self.logger.info(f"[MarketPrice] {log_data}")
                
            except Exception as e:
                self.logger.warning(
                    f"[MarketPrice] 가격 계산 실패: {str(e)} | "
                    f"exchange={exchange}, symbol={symbol}, "
                    f"raw_ts={data.get('timestamp')}"
                )
                
            # 오더북 상태 업데이트
            self.orderbooks[exchange][symbol] = {
                "bids": bids,
                "asks": asks,
                "timestamp": data.get("timestamp", int(time.time() * 1000))
            }
            
        except Exception as e:
            self.logger.error(f"[MarketPrice] 오더북 업데이트 실패: {str(e)}")

    def update_usdt_rate(self, rate: float) -> None:
        """USDT/KRW 환율 업데이트"""
        self.usdt_krw_rate = rate

    def calculate_market_price(self) -> None:
        """
        모든 거래소의 시장가격 계산 및 로깅
        - 매수: asks의 평균가
        - 매도: bids의 평균가
        """
        # 이 메서드는 더 이상 사용하지 않음 - 모든 처리를 update_orderbook에서 수행
        pass

    def _calculate_buy_price(self, exchange: str, asks: list, amount_krw: float) -> Optional[float]:
        """매수 평균가 계산"""
        try:
            if not asks:
                return None
                
            total_quantity = 0.0
            total_price = 0.0
            remaining_amount = amount_krw
            
            # 해외 거래소는 USDT 기준으로 변환
            if exchange in ["binance", "binancefuture", "bybit", "bybitfuture"]:
                remaining_amount = amount_krw / self.usdt_krw_rate
            
            for price, quantity in asks:
                price = float(price)
                quantity = float(quantity)
                
                order_amount = min(remaining_amount, price * quantity)
                order_quantity = order_amount / price
                
                total_quantity += order_quantity
                total_price += order_quantity * price
                
                remaining_amount -= order_amount
                if remaining_amount <= 0:
                    break
            
            if total_quantity > 0:
                avg_price = total_price / total_quantity
                # 해외 거래소는 KRW로 변환
                if exchange in ["binance", "binancefuture", "bybit", "bybitfuture"]:
                    avg_price *= self.usdt_krw_rate
                return avg_price
                
            return None
            
        except Exception as e:
            self.logger.error(f"[MarketPrice] 매수가격 계산 실패: {str(e)}")
            return None

    def _calculate_sell_price(self, exchange: str, bids: list, amount_krw: float) -> Optional[float]:
        """매도 평균가 계산"""
        try:
            if not bids:
                return None
                
            total_quantity = 0.0
            total_price = 0.0
            remaining_amount = amount_krw
            
            # 해외 거래소는 USDT 기준으로 변환
            if exchange in ["binance", "binancefuture", "bybit", "bybitfuture"]:
                remaining_amount = amount_krw / self.usdt_krw_rate
            
            for price, quantity in bids:
                price = float(price)
                quantity = float(quantity)
                
                order_amount = min(remaining_amount, price * quantity)
                order_quantity = order_amount / price
                
                total_quantity += order_quantity
                total_price += order_quantity * price
                
                remaining_amount -= order_amount
                if remaining_amount <= 0:
                    break
            
            if total_quantity > 0:
                avg_price = total_price / total_quantity
                # 해외 거래소는 KRW로 변환
                if exchange in ["binance", "binancefuture", "bybit", "bybitfuture"]:
                    avg_price *= self.usdt_krw_rate
                return avg_price
                
            return None
            
        except Exception as e:
            self.logger.error(f"[MarketPrice] 매도가격 계산 실패: {str(e)}")
            return None

    def _log_market_prices(self, prices: Dict) -> None:
        """시장가격 로깅 - 각 오더북별로 개별 로깅"""
        try:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 업비트 USDT 가격 확인
            upbit_usdt_price = None
            if "upbit" in prices:
                if "USDT" in prices["upbit"]:
                    upbit_usdt_price = float(prices["upbit"]["USDT"]["buy"])
            
            # USDT 가격이 없으면 현재 usdt_krw_rate 사용
            krw_rate = upbit_usdt_price or self.usdt_krw_rate
            
            # 각 거래소의 각 심볼별로 개별 로깅
            for exchange, symbols in prices.items():
                if not symbols:
                    continue
                    
                for symbol, price_data in symbols.items():
                    try:
                        # timestamp 변환
                        ts = price_data['timestamp']
                        if ts > 1000000000000000:  # 마이크로초
                            ts = ts / 1000000
                        elif ts > 1000000000000:  # 밀리초
                            ts = ts / 1000
                        timestamp_str = datetime.fromtimestamp(ts).strftime("%H:%M:%S.%f")[:-3]
                        
                        # 가격 변환 (해외 거래소는 KRW로 변환)
                        buy_price = float(price_data['buy'])
                        sell_price = float(price_data['sell'])
                        
                        if exchange in ["binance", "binancefuture", "bybit", "bybitfuture"]:
                            buy_price *= krw_rate
                            sell_price *= krw_rate
                        
                        log_data = {
                            "timestamp": current_time,
                            "exchange": exchange,
                            "symbol": symbol,
                            "buy": f"{buy_price:,.0f}",
                            "sell": f"{sell_price:,.0f}",
                            "orderbook_time": timestamp_str
                        }
                        
                        if exchange in ["binance", "binancefuture", "bybit", "bybitfuture"]:
                            log_data["usdt_rate"] = krw_rate
                        
                        self.logger.info(f"[MarketPrice] {log_data}")
                        
                    except Exception as e:
                        self.logger.warning(
                            f"[MarketPrice] 개별 오더북 로깅 실패: {str(e)} | "
                            f"exchange={exchange}, symbol={symbol}, "
                            f"raw_ts={price_data['timestamp']}"
                        )
            
        except Exception as e:
            self.logger.error(f"[MarketPrice] 가격 로깅 실패: {str(e)}") 