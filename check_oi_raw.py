# Check if "Open Interest" endpoint has details about "Buy/Sell" sides
import requests
import json
import time

def check_oi_details():
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    for sym in symbols:
        url = f"https://api.bitget.com/api/v2/mix/market/open-interest?symbol={sym}&productType=usdt-futures"
        print(f"\n--- {sym} ---")
        try:
            resp = requests.get(url, timeout=5).json()
            if 'data' in resp and resp['data']:
                data = resp['data']
                item = None
                if isinstance(data, dict) and 'openInterestList' in data and data['openInterestList']:
                    item = data['openInterestList'][0]
                elif isinstance(data, list) and data:
                    item = data[0]
                
                if item:
                    print(f"Size: {item.get('size')}")
                    print(f"Amount: {item.get('amount')}")
                    print(f"Timestamp: {item.get('timestamp')}")
                else:
                    print(f"No item found in data: {data}")
            else:
                print(f"No data: {resp}")
        except Exception as e:
            print(f"Error: {e}")

check_oi_details()
