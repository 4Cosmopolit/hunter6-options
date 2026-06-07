import hmac
import hashlib
import json

def sign_request(api_key: str, api_secret: str, timestamp: int, method: str, path: str, body: dict = None):
    param_str = str(timestamp) + api_key + "10000" + (json.dumps(body) if body else "") # Было: "10000" без api_key
    # В оригинале было str(timestamp) + "10000" ..., но Bybit требует api_key в строке подписи
    # Актуальный формат: timestamp + api_key + recv_window + body
    # Исправляем:
    recv_window = "10000"
    param_str = str(timestamp) + api_key + recv_window + (json.dumps(body) if body else "")
    return hmac.new(api_secret.encode('utf-8'), param_str.encode('utf-8'), hashlib.sha256).hexdigest()
