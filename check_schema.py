import psycopg2
import os
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
    cur = conn.cursor()
    
    cur.execute("SELECT data_type, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name = 'oi_1h' AND column_name = 'open_interest'")
    res = cur.fetchone()
    print(f"Schema for open_interest: {res}")
    
    cur.execute("SELECT * FROM oi_1h ORDER BY ts DESC LIMIT 3")
    print(f"Last 3 rows: {cur.fetchall()}")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")
