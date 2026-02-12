import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def update_schema():
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
        
        # Columns to add
        new_columns = [
            ("event_name", "TEXT"),
            ("event_category", "TEXT"),
            ("market_regime", "TEXT"),
            ("signal_type_overall", "TEXT"),
            ("confidence_score", "INTEGER"),
            ("impact_strength", "INTEGER"),
            ("similar_pattern_refs", "TEXT") # Adding this too as it's useful
        ]
        
        print("Checking/Updating schema for 'market_episodes'...")
        
        for col_name, col_type in new_columns:
            try:
                cur.execute(f"ALTER TABLE market_episodes ADD COLUMN IF NOT EXISTS {col_name} {col_type};")
                print(f"- Column '{col_name}' checked/added.")
            except Exception as e:
                print(f"Error adding column '{col_name}': {e}")
        
        print("Schema update completed.")
        conn.close()
    except Exception as e:
        print(f"Database connection error: {e}")

if __name__ == "__main__":
    update_schema()
