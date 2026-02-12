import os
import time
import json
import logging
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
from datetime import datetime

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("sync_workflow.log")
    ]
)
logger = logging.getLogger(__name__)

# Load Environment Variables
load_dotenv()

# Configuration
STORAGE_PATH_OUTPUT = os.getenv("STORAGE_PATH_OUTPUT", "./data/output")
STORAGE_PATH_FEEDBACK = os.getenv("STORAGE_PATH_FEEDBACK", "./data/feedback")

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        logger.error(f"DB Connection failed: {e}")
        return None

def save_episode_to_db(conn, episode_data):
    """Saves a single episode to the DB, including new columns."""
    try:
        cur = conn.cursor()
        
        # Extract fields
        event_id = episode_data.get("event_id")
        asset = episode_data.get("asset")
        time_start = episode_data.get("time_start")
        time_end = episode_data.get("time_end")
        duration = episode_data.get("duration")
        created_at = episode_data.get("created_at")
        updated_at = episode_data.get("updated_at")
        
        # New columns
        event_name = episode_data.get("event_name")
        event_category = episode_data.get("event_category")
        market_regime = episode_data.get("market_regime")
        signal_type_overall = episode_data.get("signal_type_overall")
        confidence_score = episode_data.get("confidence_score")
        impact_strength = episode_data.get("impact_strength")
        similar_pattern_refs = episode_data.get("similar_pattern_refs")

        # Raw data (full JSON)
        raw_data = json.dumps(episode_data)

        # UPSERT Logic
        sql = """
            INSERT INTO market_episodes (
                event_id, asset, time_start, time_end, duration, 
                created_at, updated_at, raw_data,
                event_name, event_category, market_regime, 
                signal_type_overall, confidence_score, impact_strength, similar_pattern_refs
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                asset = EXCLUDED.asset,
                time_start = EXCLUDED.time_start,
                time_end = EXCLUDED.time_end,
                duration = EXCLUDED.duration,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                raw_data = EXCLUDED.raw_data,
                event_name = EXCLUDED.event_name,
                event_category = EXCLUDED.event_category,
                market_regime = EXCLUDED.market_regime,
                signal_type_overall = EXCLUDED.signal_type_overall,
                confidence_score = EXCLUDED.confidence_score,
                impact_strength = EXCLUDED.impact_strength,
                similar_pattern_refs = EXCLUDED.similar_pattern_refs;
        """
        
        cur.execute(sql, (
            event_id, asset, time_start, time_end, duration, 
            created_at, updated_at, raw_data,
            event_name, event_category, market_regime, 
            signal_type_overall, confidence_score, impact_strength, similar_pattern_refs
        ))
        logger.info(f"Saved/Updated episode {event_id} in DB.")
        cur.close()
        return True
    except Exception as e:
        logger.error(f"Failed to save episode {episode_data.get('event_id')}: {e}")
        return False

def process_file(filepath):
    """Reads file, extracts episodes, saves to DB."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # Handle Markdown code blocks if present
        if content.strip().startswith("```json"):
            content = content.replace("```json", "").replace("```", "")
        
        data = json.loads(content)
        
        conn = get_db_connection()
        if not conn: return False

        # Support both single object and list of episodes
        episodes = []
        if isinstance(data, list):
            episodes = data
        elif isinstance(data, dict):
            if "episodes" in data:
                 episodes = data["episodes"]
            else:
                 # Single episode object
                 episodes = [data]
        
        success = True
        for ep in episodes:
            if not save_episode_to_db(conn, ep):
                success = False
        
        conn.close()
        return success
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in {filepath}")
        return False
    except Exception as e:
        logger.error(f"Error processing file {filepath}: {e}")
        return False

def main_loop():
    logger.info("Starting Sync Workflow Loop...")
    logger.info(f"Monitoring Output: {os.path.abspath(STORAGE_PATH_OUTPUT)}")
    logger.info(f"Monitoring Feedback: {os.path.abspath(STORAGE_PATH_FEEDBACK)}")

    # Ensure directories exist
    os.makedirs(STORAGE_PATH_OUTPUT, exist_ok=True)
    os.makedirs(STORAGE_PATH_FEEDBACK, exist_ok=True)

    while True:
        try:
            # 1. Scan Output for files
            files = [f for f in os.listdir(STORAGE_PATH_OUTPUT) if f.endswith('.json') or f.endswith('.md')]
            
            if not files:
                time.sleep(5) # Idle wait
                continue

            # Sort by modification time (oldest first)
            files.sort(key=lambda x: os.path.getmtime(os.path.join(STORAGE_PATH_OUTPUT, x)))
            current_file = files[0]
            current_file_path = os.path.join(STORAGE_PATH_OUTPUT, current_file)

            logger.info(f"Processing oldest file: {current_file}")

            # 2. Process to DB
            if process_file(current_file_path):
                logger.info("File successfully saved to DB. Waiting for n8n feedback...")
                
                # 3. Wait for Feedback
                feedback_received = False
                while not feedback_received:
                    feedback_files = os.listdir(STORAGE_PATH_FEEDBACK)
                    
                    if feedback_files:
                        # Assumption: Any file in feedback triggers continuation
                        # Ideally, we should match filenames, but user said "a file"
                        # to keep it simple, we take the first feedback file found
                        feedback_file = feedback_files[0]
                        feedback_path = os.path.join(STORAGE_PATH_FEEDBACK, feedback_file)
                        
                        logger.info(f"Feedback received: {feedback_file}")
                        
                        # 4. Cleanup
                        try:
                            # Delete Feedback File
                            os.remove(feedback_path)
                            logger.info(f"Deleted feedback file: {feedback_file}")
                            
                            # Delete Original Output File
                            if os.path.exists(current_file_path):
                                os.remove(current_file_path)
                                logger.info(f"Deleted processed file: {current_file}")
                            
                            feedback_received = True

                        except Exception as e:
                            logger.error(f"Error during cleanup: {e}")
                            time.sleep(5) # Retry loop on error?
                    
                    else:
                        time.sleep(2) # Poll frequency for feedback

            else:
                logger.error(f"Failed to process {current_file}. Moving to 'error' folder (optional) or retrying...")
                time.sleep(10) # Validation backoff

        except KeyboardInterrupt:
            logger.info("Stopping Sync Workflow.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main_loop()
