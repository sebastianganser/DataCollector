import hmac
import hashlib
import base64
import time
import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("BG_API_KEY")
SECRET_KEY = os.getenv("BG_SECRET_KEY")
PASSPHRASE = os.getenv("BG_PASSPHRASE")
BASE_URL = "https://api.bitget.com"

def get_signature(timestamp, method, request_path, body=""):
    message = str(timestamp) + method + request_path + body
    mac = hmac.new(bytes(SECRET_KEY, encoding='utf8'), bytes(message, encoding='utf8'), digestmod=hashlib.sha256)
    d = mac.digest()
    return base64.b64encode(d).decode()

def request_api(endpoint, params=None):
    if params is None: params = {}
    timestamp = str(int(time.time() * 1000))
    query = "&".join([f"{k}={v}" for k, v in params.items()])
    url = f"{BASE_URL}{endpoint}?{query}"
    
    headers = {
        "ACCESS-KEY": API_KEY,
        "ACCESS-PASSPHRASE": PASSPHRASE,
        "ACCESS-TIMESTAMP": timestamp,
        "Content-Type": "application/json",
        "locale": "en-US"
    }
    signature = get_signature(timestamp, "GET", f"{endpoint}?{query}")
    headers["ACCESS-SIGN"] = signature
    
    try:
        resp = requests.get(url, headers=headers)
        return resp.json()
    except Exception as e:
        return str(e)

print("--- BTCUSDT FUNDING CHECK ---")

# 1. Historical Funding (What collector uses)
print("\n[Historical Funding (Settled)]")
hist_data = request_api("/api/v2/mix/market/history-fund-rate", {
    "symbol": "BTCUSDT",
    "productType": "usdt-futures",
    "pageSize": "5",
    "pageNo": "1"
})
if 'data' in hist_data:
    # Print only the first item (latest) to avoid buffer issues
    if len(hist_data['data']) > 0:
        item = hist_data['data'][0]
        ts = int(item['fundingTime'])
        rate = float(item['fundingRate'])
        import datetime
        dt = datetime.datetime.fromtimestamp(ts/1000, datetime.timezone.utc)
        print(f"LATEST SETTLED HISTORY: Time={dt} Rate={rate} ({rate*100:.6f}%)")
    else:
        print("No history data found.")
else:
    print(hist_data)

# 2. Current Funding (Live)
print("\n[Current/Next Funding (Live)]")
current_data = request_api("/api/v2/mix/market/current-fund-rate", {
    "symbol": "BTCUSDT",
    "productType": "usdt-futures"
})
# Note: Endpoint might be different, checking v1/v2 docs usually:
# v2: /api/v2/mix/market/current-fund-rate
if 'data' in current_data and current_data['data']:
    item = current_data['data'][0]
    rate = float(item['fundingRate'])
    print(f"Current Rate: {rate} ({rate*100:.4f}%)")
else:
    print(f"Current Raw: {current_data}")
