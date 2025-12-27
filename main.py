import os
import time
import hmac
import hashlib
import base64
import json
import logging
import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import requests
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bitget_collector.log")
    ]
)
logger = logging.getLogger(__name__)

# Load Environment Variables
load_dotenv()

# Constants
BITGET_HOST = "https://api.bitget.com"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"] 
PRODUCT_TYPE = "usdt-futures"

class PostgresStorage:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD")
            )
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            self.ensure_tables()
            logger.info("Connected to PostgreSQL Database.")
        except Exception as e:
            logger.critical(f"Database Connection Failed: {e}")
            raise

    def ensure_tables(self):
        queries = [
            """
            CREATE TABLE IF NOT EXISTS ohlcv_1h (
                asset TEXT,
                ts TIMESTAMPTZ,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume NUMERIC,
                PRIMARY KEY (asset, ts)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS funding_1h (
                asset TEXT,
                ts TIMESTAMPTZ,
                funding_rate NUMERIC,
                PRIMARY KEY (asset, ts)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS oi_1h (
                asset TEXT,
                ts TIMESTAMPTZ,
                open_interest NUMERIC,
                PRIMARY KEY (asset, ts)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS collector_logs (
                id SERIAL PRIMARY KEY,
                execution_time TIMESTAMPTZ DEFAULT NOW(),
                status TEXT,
                message TEXT
            );
            """
        ]
        for query in queries:
            try:
                self.cursor.execute(query)
            except Exception as e:
                logger.error(f"Failed to create table: {e}")
                raise

        # Settings Table
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
            """)
        except Exception as e:
            logger.error(f"Failed to create settings table: {e}")

    def get_setting(self, key):
        try:
            self.cursor.execute("SELECT value FROM app_settings WHERE key = %s", (key,))
            res = self.cursor.fetchone()
            return res[0] if res else None
        except Exception as e:
            logger.error(f"Failed to get setting {key}: {e}")
            return None

    def set_setting(self, key, value):
        try:
            self.cursor.execute("""
                INSERT INTO app_settings (key, value) VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, (key, value))
        except Exception as e:
            logger.error(f"Failed to set setting {key}: {e}")

    def upsert_ohlcv(self, data):
        query = """
            INSERT INTO ohlcv_1h (asset, ts, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (asset, ts) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """
        try:
            extras.execute_values(self.cursor, query, data)
            logger.info(f"Upserted {len(data)} OHLCV records.")
        except Exception as e:
            logger.error(f"Failed to upsert OHLCV: {e}")

    def upsert_funding(self, data):
        query = """
            INSERT INTO funding_1h (asset, ts, funding_rate)
            VALUES %s
            ON CONFLICT (asset, ts) DO UPDATE SET
                funding_rate = EXCLUDED.funding_rate;
        """
        try:
            extras.execute_values(self.cursor, query, data)
            logger.info(f"Upserted {len(data)} Funding records.")
        except Exception as e:
            logger.error(f"Failed to upsert Funding: {e}")

    def upsert_oi(self, data):
        query = """
            INSERT INTO oi_1h (asset, ts, open_interest)
            VALUES (%s, %s, %s)
            ON CONFLICT (asset, ts) DO UPDATE SET
                open_interest = EXCLUDED.open_interest;
        """
        try:
            for row in data:
                self.cursor.execute(query, row)
            logger.info(f"Upserted {len(data)} Open Interest records.")
        except Exception as e:
            logger.error(f"Failed to upsert OI: {e}")

    def log_run(self, status, message):
        query = "INSERT INTO collector_logs (status, message) VALUES (%s, %s)"
        try:
            self.cursor.execute(query, (status, message))
            logger.info(f"Run logged: {status}")
        except Exception as e:
            logger.error(f"Failed to log run: {e}")

    def get_last_timestamp(self, table, asset):
        query = f"SELECT MAX(ts) FROM {table} WHERE asset = %s"
        try:
            self.cursor.execute(query, (asset,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Failed to get last timestamp from {table}: {e}")
            return None

    def get_last_oi(self, asset):
        query = "SELECT ts, open_interest FROM oi_1h WHERE asset = %s ORDER BY ts DESC LIMIT 1"
        try:
            self.cursor.execute(query, (asset,))
            result = self.cursor.fetchone()
            if result:
                return result[0], result[1]
            return None, None
        except Exception as e:
            logger.error(f"Failed to get last OI: {e}")
            return None, None

    def close(self):
        if self.cursor: self.cursor.close()
        if self.conn: self.conn.close()

# ... (BitgetClient and process functions remain unchanged)

class StorageBase:
    def upsert_ohlcv(self, data): raise NotImplementedError
    def upsert_funding(self, data): raise NotImplementedError
    def upsert_oi(self, data): raise NotImplementedError
    def log_run(self, status, message): pass # JSON storage doesn't rely on DB logs but we could impl file log if needed
    def get_last_timestamp(self, table, asset): return None
    def get_last_oi(self, asset): return None, None
    def close(self): pass

class JSONStorage(StorageBase):
    # ... (init and _save_json remain unchanged)
    def __init__(self, output_dir="data_dump"):
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        logger.info(f"Initialized JSON Storage in {self.output_dir}/")

    def _save_json(self, filename, new_data):
        filepath = os.path.join(self.output_dir, filename)
        existing_data = []
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r') as f:
                    existing_data = json.load(f)
            except json.JSONDecodeError:
                pass
        existing_data.extend(new_data)
        with open(filepath, 'w') as f:
            json.dump(existing_data, f, indent=2, default=str)
        logger.info(f"Saved {len(new_data)} records to {filename}")

    # ... (upsert methods logic remains similar, adding log_run stub)
    def upsert_ohlcv(self, data):
        # ... existing logic ...
        records_by_asset = {}
        for row in data:
            asset = row[0]
            if asset not in records_by_asset: records_by_asset[asset] = []
            records_by_asset[asset].append({
                "asset": row[0],
                "ts": row[1],
                "open": row[2],
                "high": row[3],
                "low": row[4],
                "close": row[5],
                "volume": row[6]
            })
        for asset, records in records_by_asset.items():
            self._save_json(f"ohlcv_{asset}.json", records)

    def upsert_funding(self, data):
        records_by_asset = {}
        for row in data:
            asset = row[0]
            if asset not in records_by_asset: records_by_asset[asset] = []
            records_by_asset[asset].append({
                "asset": row[0],
                "ts": row[1],
                "funding_rate": row[2]
            })
        for asset, records in records_by_asset.items():
            self._save_json(f"funding_{asset}.json", records)

    def upsert_oi(self, data):
        records_by_asset = {}
        for row in data:
            asset = row[0]
            if asset not in records_by_asset: records_by_asset[asset] = []
            records_by_asset[asset].append({
                "asset": row[0],
                "ts": row[1],
                "open_interest": row[2]
            })
        for asset, records in records_by_asset.items():
            self._save_json(f"oi_{asset}.json", records)
    
    def log_run(self, status, message):
        logger.info(f"[JSON LOG] Status: {status}, Message: {message}")

# ... (BitgetClient remains unchanged)

# ... (main function updates)
def main():
    parser = argparse.ArgumentParser(description="Bitget Market Data Collector")
    parser.add_argument("--mode", choices=["initial", "update"], required=True, help="initial (90 days) or update (recent)")
    parser.add_argument("--storage", choices=["db", "json"], default="json", help="Storage backend: 'db' (PostgreSQL) or 'json' (JSON files)")
    parser.add_argument("--start-date", help="Custom start date (YYYY-MM-DD) for initial mode")
    args = parser.parse_args()
    
    storage = None
    try:
        if args.storage == "db":
            storage = PostgresStorage()
        else:
            storage = JSONStorage()
        
        # Log Start
        storage.log_run("STARTED", f"Mode: {args.mode}, StartDate: {args.start_date or 'Auto'}")
            
        client = BitgetClient()
        
        now = datetime.now(timezone.utc)
        
        if args.start_date:
            try:
                start_time_dt = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                days_history = (now - start_time_dt).days + 1
            except ValueError:
                logger.error("Invalid date format. Use YYYY-MM-DD.")
                storage.log_run("ERROR", "Invalid Start Date format")
                return
        elif args.mode == "initial":
            days_history = 90
            start_time_dt = now - timedelta(days=90)
        else:
            days_history = 1
            start_time_dt = now - timedelta(hours=5)
            
        start_time_ms = int(start_time_dt.timestamp() * 1000)
        end_time_ms = int(now.timestamp() * 1000)

        for symbol in SYMBOLS:
            logger.info(f"--- Starting collection for {symbol} ---")
            logger.info(f"Time Range: {start_time_dt} to {now}")
            
            process_candles(storage, client, symbol, start_time_ms, end_time_ms)
            process_funding(storage, client, symbol, lookback_days=days_history)
            process_oi(storage, client, symbol)
            
        storage.log_run("SUCCESS", "Cycle completed successfully")
        storage.close()
        logger.info("Cycle completed successfully.")
        
    except Exception as e:
        logger.critical(f"Fatal Error: {e}")
        if storage:
            storage.log_run("ERROR", str(e))
            storage.close()
        exit(1)

class BitgetClient:
    def __init__(self):
        self.api_key = os.getenv("BG_API_KEY")
        self.secret_key = os.getenv("BG_SECRET_KEY")
        self.passphrase = os.getenv("BG_PASSPHRASE")
        self.session = requests.Session()

    def _get_signature(self, timestamp, method, request_path, body=""):
        message = str(timestamp) + method.upper() + request_path + body
        mac = hmac.new(self.secret_key.encode("utf-8") if self.secret_key else b'', message.encode("utf-8"), digestmod=hashlib.sha256)
        return base64.b64encode(mac.digest()).decode("utf-8")

    def _request(self, method, endpoint, params=None):
        url = BITGET_HOST + endpoint
        timestamp = str(int(time.time() * 1000))
        
        query_string = ""
        if params:
            sorted_params = sorted(params.items())
            query_string = "?" + "&".join([f"{k}={v}" for k, v in sorted_params])
        
        request_path = endpoint + query_string
        
        headers = {
            "Content-Type": "application/json",
            "ACCESS-KEY": self.api_key if self.api_key else "",
            "ACCESS-PASSPHRASE": self.passphrase if self.passphrase else "",
            "ACCESS-TIMESTAMP": timestamp,
            "locale": "en-US"
        }

        if self.api_key and self.secret_key and self.passphrase:
             signature = self._get_signature(timestamp, method, request_path)
             headers["ACCESS-SIGN"] = signature

        full_url = url + query_string
        
        try:
            response = self.session.request(method, full_url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Request Error: {e}")
            raise

    def get_history_candles(self, symbol, start_time_ms, end_time_ms):
        endpoint = "/api/v2/mix/market/history-candles"
        params = {
            "symbol": symbol,
            "productType": PRODUCT_TYPE,
            "granularity": "1H",
            "startTime": str(int(start_time_ms)),
            "endTime": str(int(end_time_ms)),
            "limit": "200" 
        }
        return self._request("GET", endpoint, params)

    def get_funding_history(self, symbol, page_no=1):
        endpoint = "/api/v2/mix/market/history-fund-rate"
        params = {
            "symbol": symbol,
            "productType": PRODUCT_TYPE,
            "pageSize": "100",
            "pageNo": str(page_no) 
        }
        return self._request("GET", endpoint, params)

    def get_open_interest(self, symbol):
        endpoint = "/api/v2/mix/market/open-interest"
        params = {
            "symbol": symbol,
            "productType": PRODUCT_TYPE
        }
        return self._request("GET", endpoint, params)

def process_candles(storage, client, symbol, start_time_ms, end_time_ms):
    logger.info(f"Processing candles for {symbol} from {datetime.fromtimestamp(start_time_ms/1000, timezone.utc)} to {datetime.fromtimestamp(end_time_ms/1000, timezone.utc)}")
    
    current_start = start_time_ms
    # 200 candles limit for 1H = 200 hours
    chunk_size_ms = 200 * 60 * 60 * 1000 
    
    while current_start < end_time_ms:
        req_end = min(current_start + chunk_size_ms, end_time_ms)
        
        try:
            # logger.info(f"Requesting candles: {current_start} to {req_end}")
            resp = client.get_history_candles(symbol, current_start, req_end)
            if resp['code'] != '00000':
                logger.error(f"API Error fetching candles for {symbol}: {resp}")
                # If invalid params, maybe range is still issue? But 200h should be fine.
                # If we get error, skip this chunk to avoid infinite loop
                current_start = req_end
                time.sleep(1)
                continue
                
            data = resp['data']
            if not data:
                # No data in this chunk, move to next
                current_start = req_end
                time.sleep(0.1)
                continue
            
            # Bitget Candles: [ts, open, high, low, close, vol, ...]
            # Sort by TS ascending
            data.sort(key=lambda x: int(x[0]))
            
            db_rows = []
            for candle in data:
                ts = int(candle[0])
                op = float(candle[1])
                hi = float(candle[2])
                lo = float(candle[3])
                cl = float(candle[4])
                vol = float(candle[5])
                
                dt = datetime.fromtimestamp(ts / 1000, timezone.utc)
                asset_name = symbol.replace("USDT", "")
                
                db_rows.append((asset_name, dt, op, hi, lo, cl, vol))
            
            if db_rows:
                storage.upsert_ohlcv(db_rows)
            
            # Move current_start based on last received candle
            last_ts = int(data[-1][0])
            next_step = last_ts + (60 * 60 * 1000) # +1 hour
            
            # Ensure we progress
            if next_step <= current_start:
                 next_step = current_start + chunk_size_ms
                 
            current_start = next_step
            time.sleep(0.1) # Rate limit
            
        except Exception as e:
            logger.error(f"Error in candle processing loop: {e}")
            break

def process_funding(storage, client, symbol, start_time_ms):
    logger.info(f"Processing funding for {symbol} (from {datetime.fromtimestamp(start_time_ms/1000, timezone.utc)})")
    page = 1
    cutoff_time_ms = start_time_ms
    
    while True:
        try:
            resp = client.get_funding_history(symbol, page_no=page)
            if resp['code'] != '00000':
                logger.error(f"API Error fetching funding: {resp}")
                break
            
            data = resp['data']
            if not data:
                break
                
            db_rows = []
            stop_fetching = False
            
            for item in data:
                ts = int(item['fundingTime'])
                rate = float(item['fundingRate'])
                
                # Fetch until we hit the already known timestamp (exclusive)
                if ts <= cutoff_time_ms:
                    stop_fetching = True
                    continue
                
                dt = datetime.fromtimestamp(ts / 1000, timezone.utc)
                asset_name = symbol.replace("USDT", "")
                
                db_rows.append((asset_name, dt, rate))
            
            if db_rows:
                storage.upsert_funding(db_rows)
            
            if stop_fetching or len(data) < 100:
                break
                
            page += 1
            time.sleep(0.2)
            
        except Exception as e:
            logger.error(f"Error in funding loop: {e}")
            break

def process_oi(storage, client, symbol):
    asset_name = symbol.replace("USDT", "")
    try:
        resp = client.get_open_interest(symbol)
        if resp['code'] == '00000' and resp['data']:
            data_raw = resp['data']
            oi_item = None
            
            # Handle API variations (List within dict, or flat dict)
            if isinstance(data_raw, dict) and 'openInterestList' in data_raw and data_raw['openInterestList']:
                oi_item = data_raw['openInterestList'][0]
            elif isinstance(data_raw, dict):
                oi_item = data_raw
            
            if not oi_item:
                logger.warning(f"No parseable OI data for {symbol}")
                return

            # ...
            logger.info(f"DEBUG: OI Raw Item for {symbol}: {oi_item}")
            
            # Current Data
            current_ts = int(oi_item.get('timestamp', oi_item.get('time', int(time.time() * 1000))))
            try:
                val = oi_item.get('amount', oi_item.get('size', 0))
                current_oi = Decimal(str(val))

                # Correction: Bitget reports BTC OI as 2-sided (Long+Short), causing 2x value vs Coinglass
                # ETH/SOL appear to be 1-sided.
                if asset_name == 'BTC':
                    current_oi = current_oi / 2
                
                # Limit precision to 3 decimal places
                current_oi = round(current_oi, 3)
            except:
                current_oi = Decimal(0)

            logger.info(f"DEBUG: Extracted OI for {symbol}: {current_oi}")
            
            current_dt = datetime.fromtimestamp(current_ts / 1000, timezone.utc)
            
            rows_to_insert = []
            
            # Check for gaps if storage supports retrieval
            try:
                last_ts_db, last_oi_db = storage.get_last_oi(asset_name)
            except:
                last_ts_db, last_oi_db = None, None
            
            if last_ts_db and last_oi_db:
                 # Calculate gap
                 last_dt = last_ts_db 
                 # Ensure last_dt is aware if current is aware
                 if last_dt.tzinfo is None:
                     last_dt = last_dt.replace(tzinfo=timezone.utc)
                     
                 time_diff = current_dt - last_dt
                 hours_diff = int(time_diff.total_seconds() / 3600)
                 
                 # Only interpolate if gap is significant (> 1.1 hours to avoid slight drifts)
                 if hours_diff > 1:
                     logger.info(f"Gap detected for {symbol} OI: {hours_diff} hours. Interpolating...")
                     step_val = (current_oi - last_oi_db) / hours_diff
                     
                     for i in range(1, hours_diff):
                         interp_dt = last_dt + timedelta(hours=i)
                         interp_oi = last_oi_db + (step_val * i)
                         interp_oi = round(interp_oi, 3)
                         rows_to_insert.append((asset_name, interp_dt, interp_oi))

            # Always add current
            rows_to_insert.append((asset_name, current_dt, current_oi))
            
            storage.upsert_oi(rows_to_insert)
            logger.info(f"Updated OI for {symbol}: {current_oi} (TS: {current_dt}). Total rows: {len(rows_to_insert)}")
        else:
            logger.warning(f"Failed to get OI for {symbol}: {resp}")
            
    except Exception as e:
        logger.error(f"Error fetching OI for {symbol}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Bitget Market Data Collector")
    parser.add_argument("--mode", choices=["initial", "update"], required=True, help="initial (90 days) or update (recent)")
    parser.add_argument("--storage", choices=["db", "json"], default="json", help="Storage backend: 'db' (PostgreSQL) or 'json' (JSON files)")
    parser.add_argument("--start-date", help="Custom start date (YYYY-MM-DD) for initial mode")
    args = parser.parse_args()
    
    storage = None
    try:
        if args.storage == "db":
            storage = PostgresStorage()
        else:
            storage = JSONStorage()
        
        # Log Start
        storage.log_run("STARTED", f"Mode: {args.mode}, StartDate: {args.start_date or 'Auto'}")
            
        client = BitgetClient()
        
        now = datetime.now(timezone.utc)
        
        if args.start_date:
            try:
                start_time_dt = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                days_history = (now - start_time_dt).days + 1
            except ValueError:
                logger.error("Invalid date format. Use YYYY-MM-DD.")
                storage.log_run("ERROR", "Invalid Start Date format")
                return
        elif args.mode == "initial":
            days_history = 90
            start_time_dt = now - timedelta(days=90)
        else:
            days_history = 1
            start_time_dt = now - timedelta(hours=5)
            
        start_time_ms = int(start_time_dt.timestamp() * 1000)
        end_time_ms = int(now.timestamp() * 1000)

        for symbol in SYMBOLS:
            asset_name = symbol.replace("USDT", "")
            logger.info(f"--- Starting collection for {symbol} ---")
            
            # --- Dynamic Start for OHLCV ---
            ohlcv_start = start_time_ms
            if args.mode == "update":
                last_ts = storage.get_last_timestamp("ohlcv_1h", asset_name)
                if last_ts:
                    # Ensure timezone awareness
                    if last_ts.tzinfo is None: last_ts = last_ts.replace(tzinfo=timezone.utc)
                    logger.info(f"Found last OHLCV: {last_ts}")
                    ohlcv_start = int(last_ts.timestamp() * 1000) + (3600 * 1000) # Next hour
                else:
                    # Check settings for target start date
                    target_start = storage.get_setting("target_start_date")
                    if target_start:
                        try:
                            ts_dt = datetime.strptime(target_start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                            logger.info(f"No previous data. Using configured start date: {target_start}")
                            ohlcv_start = int(ts_dt.timestamp() * 1000)
                        except:
                            logger.warning(f"Invalid target_start_date in settings: {target_start}")
            
            # --- Dynamic Start for Funding ---
            funding_start = start_time_ms
            if args.mode == "update":
                last_fund_ts = storage.get_last_timestamp("funding_1h", asset_name)
                if last_fund_ts:
                     if last_fund_ts.tzinfo is None: last_fund_ts = last_fund_ts.replace(tzinfo=timezone.utc)
                     logger.info(f"Found last Funding: {last_fund_ts}")
                     funding_start = int(last_fund_ts.timestamp() * 1000)
                else:
                     # Check settings for target start date
                    target_start = storage.get_setting("target_start_date")
                    if target_start:
                        try:
                            ts_dt = datetime.strptime(target_start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                            logger.info(f"No previous funding data. Using configured start date: {target_start}")
                            funding_start = int(ts_dt.timestamp() * 1000)
                        except:
                             pass
                    elif funding_start == start_time_ms: # If fall through to default 1 day
                         # Default 1 day lookback if no data found in update mode AND no setting
                         funding_start = int((now - timedelta(days=1)).timestamp() * 1000)
            
            process_candles(storage, client, symbol, ohlcv_start, end_time_ms)
            process_funding(storage, client, symbol, start_time_ms=funding_start)
            process_oi(storage, client, symbol)
            
        storage.log_run("SUCCESS", "Cycle completed successfully")
        storage.close()
        logger.info("Cycle completed successfully.")
        
    except Exception as e:
        logger.critical(f"Fatal Error: {e}")
        if storage:
            storage.log_run("ERROR", str(e))
            storage.close()
        exit(1)

if __name__ == "__main__":
    main()
