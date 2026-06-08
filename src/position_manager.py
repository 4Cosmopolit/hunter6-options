import asyncio
import os
import logging
from prometheus_client import Gauge

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("position_manager")

unrealized_pnl_gauge = Gauge('strangle_unrealized_pnl', 'Unrealized PnL of active strangle')
strangle_active = Gauge('strangle_active', 'Whether a strangle position is currently open')

TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "50")) / 100
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "30")) / 100

class PositionManager:
    def __init__(self, executor):
        self.executor = executor
        self.entry_premium = 0.0
        self.current_premium = 0.0
        self.is_active = False
        self.put_symbol = None
        self.call_symbol = None
        self.put_qty = 0.0
        self.call_qty = 0.0

    async def open_strangle(self, put_symbol, call_symbol, put_qty, call_qty, entry_premium):
        self.is_active = True
        self.put_symbol = put_symbol
        self.call_symbol = call_symbol
        self.put_qty = put_qty
        self.call_qty = call_qty
        self.entry_premium = entry_premium
        strangle_active.set(1)
        logger.info(f"Strangle opened: put={put_symbol} qty={put_qty}, call={call_symbol} qty={call_qty}")

    async def update_and_check(self):
        if not self.is_active:
            return
        put_price = await self.executor.get_option_price_from_symbol(self.put_symbol)
        call_price = await self.executor.get_option_price_from_symbol(self.call_symbol)
        if put_price is None or call_price is None:
            return
        self.current_premium = (put_price * self.put_qty) + (call_price * self.call_qty)
        unrealized_pnl_gauge.set(self.current_premium - self.entry_premium)
        pnl_pct = (self.current_premium - self.entry_premium) / self.entry_premium
        if pnl_pct >= TAKE_PROFIT_PCT or pnl_pct <= -STOP_LOSS_PCT:
            await self._close_position()

    async def _close_position(self):
        await self.executor.place_close_order(self.put_symbol, self.put_qty, "Sell")
        await self.executor.place_close_order(self.call_symbol, self.call_qty, "Sell")
        self.is_active = False
        strangle_active.set(0)
        logger.info("Strangle position closed")
