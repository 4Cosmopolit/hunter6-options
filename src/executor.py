import asyncio
import json
import time
import hmac
import hashlib
import aiohttp
from kafka import KafkaConsumer
from prometheus_client import Counter
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("executor")

order_counter = Counter('orders_placed_total', 'Total orders placed', ['type'])
order_failure_counter = Counter('order_failures_total', 'Order failures', ['reason'])

API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
TESTNET = os.getenv("TESTNET", "true").lower() == "true"
BASE_URL = "https://api-testnet.bybit.com" if TESTNET else "https://api.bybit.com"

def sign_request(api_secret, timestamp, method, path, body):
    body_str = json.dumps(body) if body else ''
    sign_str = f"{timestamp}{api_key}{10000}{body_str}"
    return hmac.new(api_secret.encode(), sign_str.encode(), hashlib.sha256).hexdigest()

async def place_strangle(symbol, expiry, put_strike, call_strike, size_usdt):
    # Упрощённо: фиксированные цены опционов
    put_price = 0.1 * size_usdt / 1000
    call_price = 0.1 * size_usdt / 1000
    put_qty = size_usdt / put_price if put_price > 0 else 0
    call_qty = size_usdt / call_price if call_price > 0 else 0
    legs = [
        {"symbol": f"{symbol}-{expiry}-{put_strike}-P", "side": "Buy", "qty": f"{put_qty:.3f}", "orderType": "Limit", "price": f"{put_price:.2f}"},
        {"symbol": f"{symbol}-{expiry}-{call_strike}-C", "side": "Buy", "qty": f"{call_qty:.3f}", "orderType": "Limit", "price": f"{call_price:.2f}"}
    ]
    payload = {"category": "option", "legs": legs, "orderLinkId": f"strangle_{int(time.time())}", "timeInForce": "GTC"}
    timestamp = int(time.time() * 1000)
    signature = sign_request(API_SECRET, timestamp, "POST", "/v5/order/create-batch", payload)
    headers = {"X-BAPI-API-KEY": API_KEY, "X-BAPI-TIMESTAMP": str(timestamp), "X-BAPI-SIGN": signature, "Content-Type": "application/json"}
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{BASE_URL}/v5/order/create-batch", headers=headers, json=payload) as resp:
            result = await resp.json()
            if result.get('retCode') == 0:
                order_counter.labels(type='strangle').inc()
                logger.info(f"Strangle placed: {legs}")
            else:
                order_failure_counter.labels(reason=result.get('retMsg', 'unknown')).inc()
                logger.error(f"Order failed: {result}")

async def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    consumer = KafkaConsumer('option_signals', bootstrap_servers=bootstrap,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    for msg in consumer:
        signal = msg.value
        if signal.get('type') == 'PUT_SKEW':
            await place_strangle('BTC', signal['expiry'], 60000, 70000, 1000)

if __name__ == "__main__":
    asyncio.run(main())
