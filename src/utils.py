import hmac
import hashlib
import json

def sign_request(api_secret, timestamp, method, path, body):
    body_str = json.dumps(body) if body else ''
    sign_str = f"{timestamp}{api_key}{10000}{body_str}"
    return hmac.new(api_secret.encode(), sign_str.encode(), hashlib.sha256).hexdigest()
