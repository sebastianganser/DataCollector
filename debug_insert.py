import psycopg2
import os
from decimal import Decimal
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # 1. Check Schema
    print("--- SCHEMA ---")
    cur.execute("SELECT column_name, data_type, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = 'oi_1h' AND column_name = 'open_interest'")
    print(cur.fetchone())
    
    # 2. Force Insert Test Value
    print("\n--- TEST INSERT ---")
    test_ts = datetime.now(timezone.utc)
    test_val = Decimal('12345.678')
    
    query = """
        INSERT INTO oi_1h (asset, ts, open_interest)
        VALUES (%s, %s, %s)
        ON CONFLICT (asset, ts) DO UPDATE SET
            open_interest = EXCLUDED.open_interest
        RETURNING open_interest;
    """
    
    print(f"Inserting: asset='TEST_ASSET', ts={test_ts}, val={test_val}")
    cur.execute(query, ('TEST_ASSET', test_ts, test_val))
    returned = cur.fetchone()[0]
    print(f"DB Returned on Insert: {returned}")
    
    # 3. Read Back
    print("\n--- READ BACK ---")
    cur.execute("SELECT open_interest FROM oi_1h WHERE asset = 'TEST_ASSET' ORDER BY ts DESC LIMIT 1")
    read_val = cur.fetchone()[0]
    print(f"Read Value: {read_val} (Type: {type(read_val)})")
    
    # 4. Clean up
    cur.execute("DELETE FROM oi_1h WHERE asset = 'TEST_ASSET'")
    print("Cleanup done.")
    
    conn.close()

except Exception as e:
    print(f"CRITICAL ERROR: {e}")
