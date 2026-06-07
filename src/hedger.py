import asyncio
from pybit.unified_trading import WebSocket, HTTP
from prometheus_client import Gauge, Counter, start_http_server
import os
import logging

# Запуск HTTP-сервера для Prometheus
start_http_server(8002)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hedger")

delta_gauge = Gauge('option_total_delta', 'Total delta of option portfolio')
hedge_trade_counter = Counter('hedge_trades_total', 'Hedge trades executed')

API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET = os.getenv("TESTNET", "true").lower() == "true"
DELTA_THRESHOLD = 0.1

class DeltaHedger:
    def __init__(self):
        self.session = HTTP(testnet=TESTNET, api_key=API_KEY, api_secret=API_SECRET)
        self.total_delta = 0.0
        self.futures_position = 0.0
        self._lock = asyncio.Lock()

    async def _on_position_update(self, message):
        """
        Колбек WebSocket-потока позиций.
        message – уже десериализованный словарь (pybit), не строка.
        """
        try:
            if 'data' in message:
                positions = message['data'] if isinstance(message['data'], list) else [message['data']]
                delta_sum = 0.0
                for pos in positions:
                    if pos.get('category') == 'option':
                        delta_val = float(pos.get('delta', 0.0))
                        delta_sum += delta_val
                async with self._lock:
                    self.total_delta = delta_sum
                delta_gauge.set(self.total_delta)
                logger.debug(f"Delta updated: {self.total_delta:.4f}")
        except Exception as e:
            logger.error(f"Error parsing position update: {e}")

    async def _hedge(self):
        """Выполняет хеджирующую сделку на фьючерсах."""
        async with self._lock:
            target = -self.total_delta
            delta_change = target - self.futures_position
            if abs(delta_change) < 0.01:
                return
            side = "Buy" if delta_change > 0 else "Sell"
            qty = str(abs(delta_change))
            try:
                order = self.session.place_order(
                    category="linear",
                    symbol="BTCUSDT",
                    side=side,
                    orderType="Market",
                    qty=qty,
                    timeInForce="GTC"
                )
                if order.get('retCode') == 0:
                    hedge_trade_counter.inc()
                    self.futures_position = target
                    logger.info(f"Hedge: {side} {qty} BTCUSDT, futures_pos={self.futures_position:.4f}")
                else:
                    logger.error(f"Hedge failed: {order}")
            except Exception as e:
                logger.error(f"Hedge exception: {e}")

    async def start(self):
        """Запускает WebSocket-поток позиций и цикл проверки порога."""
        ws = WebSocket(testnet=TESTNET, channel_type="private")
        ws.position_stream("option", self._on_position_update)
        logger.info("Delta hedger started, waiting for position updates...")
        while True:
            await asyncio.sleep(2)
            if abs(self.total_delta) > DELTA_THRESHOLD:
                await self._hedge()

async def main():
    hedger = DeltaHedger()
    await hedger.start()

if __name__ == "__main__":
    asyncio.run(main())
