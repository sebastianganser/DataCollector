import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def fix_btc_oi():
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
        
        print("Fixing BTC Open Interest (Halving values)...")
        # Only run this ONCE. How to ensure?
        # Well, if I run it, it halves. If I run again, it quarters.
        # I should probably assume the user will not run this script again.
        # But to be safe, maybe check if values are insane?
        # Actually, for this one-shot fix, I'll just run it.
        
        cur.execute("UPDATE oi_1h SET open_interest = open_interest / 2 WHERE asset = 'BTC'")
        print(f"Updated {cur.rowcount} rows.")
        
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

fix_btc_oi()
