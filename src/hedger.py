import asyncio
from pybit.unified_trading import HTTP
from prometheus_client import Gauge, Counter
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hedger")

delta_gauge = Gauge('option_total_delta', 'Total delta of option portfolio')
hedge_trade_counter = Counter('hedge_trades_total', 'Hedge trades executed')

API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET = os.getenv("TESTNET", "true").lower() == "true"

class DeltaHedger:
    def __init__(self, delta_threshold=0.1):
        self.delta_threshold = delta_threshold
        self.total_delta = 0.0
        self.futures_position = 0.0
        self.session = HTTP(testnet=TESTNET, api_key=API_KEY, api_secret=API_SECRET)

    async def update_delta(self, delta):
        self.total_delta = delta
        delta_gauge.set(self.total_delta)
        if abs(self.total_delta) > self.delta_threshold:
            await self._hedge()

    async def _hedge(self):
        target_futures = -self.total_delta
        delta_change = target_futures - self.futures_position
        if abs(delta_change) < 0.01:
            return
        order = self.session.place_order(
            category="linear", symbol="BTCUSDT",
            side="Buy" if delta_change > 0 else "Sell",
            orderType="Market", qty=str(abs(delta_change)), timeInForce="GTC"
        )
        if order.get('retCode') == 0:
            hedge_trade_counter.inc()
            self.futures_position = target_futures
            logger.info(f"Hedge: {delta_change:.3f} BTCUSDT")

async def main():
    hedger = DeltaHedger()
    # Симуляция: обновление дельты каждые 2 секунды
    import math, random
    t = 0
    while True:
        delta = 0.5 * math.sin(t / 10) + random.uniform(-0.1, 0.1)
        await hedger.update_delta(delta)
        await asyncio.sleep(2)
        t += 1

if __name__ == "__main__":
    asyncio.run(main())
