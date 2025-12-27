import os
import logging
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load Environment Variables
load_dotenv()

def create_database():
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    target_db_name = os.getenv("DB_NAME")

    if not all([db_host, db_port, db_user, db_password, target_db_name]):
        logger.error("Missing database environment variables in .env")
        return

    try:
        # Connect to default 'postgres' database to create new db
        logger.info(f"Connecting to {db_host} as {db_user}...")
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname="postgres",
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if database exists
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (target_db_name,))
        exists = cursor.fetchone()

        if not exists:
            logger.info(f"Database '{target_db_name}' does not exist. Creating...")
            # Use sql module for safe identifier quoting
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(target_db_name)
            ))
            logger.info(f"Database '{target_db_name}' created successfully!")
        else:
            logger.info(f"Database '{target_db_name}' already exists.")

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Failed to create database: {e}")

if __name__ == "__main__":
    create_database()
