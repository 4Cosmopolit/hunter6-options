import asyncio
import json
import time
import hmac
import hashlib
import aiohttp
from kafka import KafkaConsumer
from prometheus_client import Counter, start_http_server
import os
import logging

start_http_server(8001)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("executor")

order_counter = Counter('orders_placed_total', 'Total orders placed', ['type'])
order_failure_counter = Counter('order_failures_total', 'Order failures', ['reason'])

API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET = os.getenv("TESTNET", "true").lower() == "true"
BASE_URL = "https://api-testnet.bybit.com" if TESTNET else "https://api.bybit.com"

def sign_request(api_key, api_secret, timestamp, method, path, body):
    body_str = json.dumps(body) if body else ''
    recv_window = "10000"
    sign_str = f"{timestamp}{api_key}{recv_window}{body_str}"
    return hmac.new(api_secret.encode(), sign_str.encode(), hashlib.sha256).hexdigest()

async def get_option_price(symbol, expiry, strike, opt_type):
    ticker_symbol = f"{symbol}-{expiry}-{strike}-{opt_type}"
    url = f"{BASE_URL}/v5/market/tickers?category=option&symbol={ticker_symbol}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if data.get('retCode') != 0 or not data.get('result', {}).get('list'):
                    return None
                ticker = data['result']['list'][0]
                bid = float(ticker.get('bid1Price', 0))
                ask = float(ticker.get('ask1Price', 0))
                if bid == 0 and ask == 0:
                    return None
                mid_price = (bid + ask) / 2 if (bid > 0 and ask > 0) else (bid or ask)
                return mid_price
    except Exception:
        return None

async def place_strangle(symbol, expiry, put_strike, call_strike, size_usdt):
    put_price = await get_option_price(symbol, expiry, put_strike, "P")
    call_price = await get_option_price(symbol, expiry, call_strike, "C")
    if not put_price or not call_price:
        return None
    put_qty = size_usdt / put_price if put_price > 0 else 0
    call_qty = size_usdt / call_price if call_price > 0 else 0
    legs = [
        {"symbol": f"{symbol}-{expiry}-{put_strike}-P", "side": "Buy", "qty": f"{put_qty:.3f}", "orderType": "Limit", "price": f"{put_price:.2f}"},
        {"symbol": f"{symbol}-{expiry}-{call_strike}-C", "side": "Buy", "qty": f"{call_qty:.3f}", "orderType": "Limit", "price": f"{call_price:.2f}"}
    ]
    payload = {"category": "option", "legs": legs, "orderLinkId": f"strangle_{int(time.time())}", "timeInForce": "GTC"}
    timestamp = int(time.time() * 1000)
    signature = sign_request(API_KEY, API_SECRET, timestamp, "POST", "/v5/order/create-batch", payload)
    headers = {"X-BAPI-API-KEY": API_KEY, "X-BAPI-TIMESTAMP": str(timestamp), "X-BAPI-SIGN": signature, "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{BASE_URL}/v5/order/create-batch", headers=headers, json=payload) as resp:
            result = await resp.json()
            if result.get('retCode') == 0:
                order_counter.labels(type='strangle').inc()
                logger.info(f"Strangle placed: {legs}")
                return result, put_price, call_price, put_qty, call_qty
            else:
                order_failure_counter.labels(reason=result.get('retMsg', 'unknown')).inc()
                return None, None, None, None, None

async def main():
    from position_manager import PositionManager
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    consumer = KafkaConsumer('option_signals', bootstrap_servers=bootstrap,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    pm = PositionManager(None)  # executor будет передан после инициализации
    pm.executor = executor_module  # ссылка на модуль (будет исправлена ниже)
    logger.info("Executor waiting for signals...")
    for msg in consumer:
        signal = msg.value
        if signal.get('type') in ('PUT_SKEW', 'CALL_SKEW'):
            result, put_price, call_price, put_qty, call_qty = await place_strangle('BTC', signal['expiry'], 60000, 70000, 1000)
            if result:
                entry_premium = (put_price * put_qty) + (call_price * call_qty)
                await pm.open_strangle(f"BTC-{signal['expiry']}-60000-P", f"BTC-{signal['expiry']}-70000-C", put_qty, call_qty, entry_premium)
                asyncio.create_task(pm_loop(pm))

async def pm_loop(pm):
    while pm.is_active:
        await pm.update_and_check()
        await asyncio.sleep(2)

# Заглушка для метода get_option_price_from_symbol
async def get_option_price_from_symbol(symbol):
    parts = symbol.split('-')
    if len(parts) != 4:
        return None
    base, expiry, strike, opt_type = parts
    return await get_option_price(base, expiry, float(strike), opt_type)

if __name__ == "__main__":
    asyncio.run(main())
