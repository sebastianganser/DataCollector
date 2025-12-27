import requests
import json

def get_symbol_info():
    url = "https://api.bitget.com/api/v2/mix/market/contracts?productType=usdt-futures"
    try:
        resp = requests.get(url)
        data = resp.json()
        if data['code'] == '00000':
            for item in data['data']:
                sym = item['symbol']
                if sym in ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']:
                    print(f"SYMBOL: {sym}")
                    print(f"  sizeMultiplier: {item.get('sizeMultiplier')}")
                    print(f"  pricePlace: {item.get('pricePlace')}")
                    print(f"  volumePlace: {item.get('volumePlace')}")
                    print(f"  minTradeNum: {item.get('minTradeNum')}")
                    print("-" * 20)
        else:
            print(f"Error: {data}")
    except Exception as e:
        print(f"Exception: {e}")

get_symbol_info()
