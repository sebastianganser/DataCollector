import os
import logging
import psycopg2
import subprocess
import sys
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
import shutil
import json
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from urllib.parse import quote_plus

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load Environment Variables
load_dotenv()

# Scheduler Global
scheduler = None

def run_collector_job():
    """Executes the main.py script in update mode."""
    logger.info("Scheduler: Starting collector job...")
    
    # Manually log STARTED so UI reacts immediately
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO collector_logs (status, message) VALUES ('STARTED', 'Scheduler triggered job')")
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to log manual start: {e}")

    try:
        # Use absolute path to ensure main.py is found
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "main.py")
        cmd = [sys.executable, script_path, "--mode", "update", "--storage", "db"]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("Scheduler: Job completed successfully.")
        else:
            logger.error(f"Scheduler: Job failed. Stderr: {result.stderr}")
            # Log error to DB so UI shows red
            conn = get_db_connection()
            if conn:
                try:
                    cur = conn.cursor()
                    cur.execute("INSERT INTO collector_logs (status, message) VALUES ('ERROR', %s)", (f"Script failed: {result.stderr[:200]}",))
                    conn.commit()
                    conn.close()
                except: pass
            
    except Exception as e:
        logger.error(f"Scheduler: Execution error: {e}")
        conn = get_db_connection()
        if conn:
            try:
                cur = conn.cursor()
                cur.execute("INSERT INTO collector_logs (status, message) VALUES ('ERROR', %s)", (str(e),))
                conn.commit()
                conn.close()
            except: pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global scheduler
    
    # URL encode password to handle special chars like @, :, etc.
    password = quote_plus(os.getenv('DB_PASSWORD'))
    db_url = f"postgresql://{os.getenv('DB_USER')}:{password}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    
    jobstores = {
        'default': SQLAlchemyJobStore(url=db_url)
    }
    executors = {
        'default': ThreadPoolExecutor(10)
    }
    
    scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors)
    
    if os.getenv("DISABLE_SCHEDULER", "False").lower() in ("true", "1", "yes"):
        logger.info("Scheduler disabled via environment variable.")
    else:
        scheduler.start()
        logger.info("Scheduler started.")
    
    # Ensure tables exist
    create_tables() 
    
    
    yield
    
    # Shutdown
    if scheduler and scheduler.running:
        try:
             scheduler.shutdown()
             logger.info("Scheduler shut down.")
        except Exception as e:
             logger.warning(f"Scheduler shutdown error: {e}")

app = FastAPI(title="Bitget Collector Dashboard", lifespan=lifespan)

# Context7: Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

from datetime import datetime, timedelta

# ... imports ...

class ScheduleRequest(BaseModel):
    interval_minutes: int
    active: bool
    start_time: Optional[str] = None  # Format: "HH:MM"

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        return conn
    except Exception as e:
        logger.error(f"DB Connection failed: {e}")
        return None

@app.get("/api/status")
def get_status():
    conn = get_db_connection()
    if not conn:
        return {"status": "ERROR", "message": "Database connection failed", "execution_time": None}
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT status, message, execution_time FROM collector_logs ORDER BY id DESC LIMIT 1;")
        row = cur.fetchone()
        conn.close()
        
        if row:
            return {"status": row[0], "message": row[1], "execution_time": row[2]}
        else:
            return {"status": "UNKNOWN", "message": "No logs found", "execution_time": None}
    except Exception as e:
        return {"status": "ERROR", "message": str(e), "execution_time": None}

@app.get("/api/market-data/latest")
def get_latest_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB Connection Error")
    
    data = []
    try:
        cur = conn.cursor()
        assets = ["BTC", "ETH", "SOL"]
        
        for asset in assets:
            item = {"asset": asset}
            
            # OHLCV (Full Candle)
            cur.execute("SELECT ts, open, high, low, close, volume FROM ohlcv_1h WHERE asset = %s ORDER BY ts DESC LIMIT 1", (asset,))
            res_ohlcv = cur.fetchone()
            if res_ohlcv:
                item["OHLCV"] = {
                    "ts": res_ohlcv[0], 
                    "o": float(res_ohlcv[1]),
                    "h": float(res_ohlcv[2]),
                    "l": float(res_ohlcv[3]),
                    "c": float(res_ohlcv[4]),
                    "v": float(res_ohlcv[5])
                }
            else:
                item["OHLCV"] = None
            
            # Funding Rate
            cur.execute("SELECT ts, funding_rate FROM funding_1h WHERE asset = %s ORDER BY ts DESC LIMIT 1", (asset,))
            res_fund = cur.fetchone()
            item["Funding"] = {"ts": res_fund[0], "val": float(res_fund[1])} if res_fund else None

            # Open Interest
            cur.execute("SELECT ts, open_interest FROM oi_1h WHERE asset = %s ORDER BY ts DESC LIMIT 1", (asset,))
            res_oi = cur.fetchone()
            item["Open Interest"] = {"ts": res_oi[0], "val": float(res_oi[1])} if res_oi else None
            
            data.append(item)
            
        conn.close()
        return data
    except Exception as e:
        logger.error(f"Error fetching latest data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- Scheduler Endpoints ---

@app.get("/api/schedule")
def get_schedule():
    # 1. Fetch Config from DB
    conn = get_db_connection()
    db_config = {"interval_minutes": 60, "start_time": ""}
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT key, value FROM app_settings WHERE key IN ('schedule_interval', 'schedule_start_time')")
            rows = cur.fetchall()
            for r in rows:
                if r[0] == 'schedule_interval':
                    db_config['interval_minutes'] = int(r[1])
                elif r[0] == 'schedule_start_time':
                    db_config['start_time'] = r[1]
            conn.close()
        except: pass

    # 2. Check Job Status
    job = scheduler.get_job('collector_update')
    active = job is not None
    next_run = job.next_run_time if job else None

    # If job exists, trust its interval logic? 
    # Actually, we want to return what's in DB as the "configured" state.
    # But next_run comes from the job.
    
    return {
        "active": active, 
        "interval_minutes": db_config['interval_minutes'], 
        "start_time": db_config['start_time'],
        "next_run": next_run
    }

@app.post("/api/schedule")
def set_schedule(req: ScheduleRequest):
    # 1. Persist Config
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB Connection Error")
    
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO app_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", ('schedule_interval', str(req.interval_minutes)))
        st_val = req.start_time if req.start_time else ""
        cur.execute("INSERT INTO app_settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", ('schedule_start_time', st_val))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Config Save Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to save settings")

    job_id = 'collector_update'
    
    if req.active:
        if req.interval_minutes < 1:
            raise HTTPException(status_code=400, detail="Minimum interval is 1 minute")
            
        start_date = None
        if req.start_time:
            try:
                # Parse Requested Time
                target_time = datetime.strptime(req.start_time, "%H:%M").time()
                now = datetime.now()
                start_date = now.replace(hour=target_time.hour, minute=target_time.minute, second=0, microsecond=0)
                
                # If time is in the past for today, schedule for tomorrow
                if start_date <= now:
                    start_date += timedelta(days=1)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid start_time format. Use HH:MM")

        # Add or Replace job
        scheduler.add_job(
            run_collector_job, 
            'interval', 
            minutes=req.interval_minutes, 
            start_date=start_date,
            id=job_id, 
            replace_existing=True
        )
        
        msg = f"Schedule enabled (every {req.interval_minutes} min)"
        if start_date:
            msg += f", starting at {start_date}"
        logger.info(msg)
        
        return {"message": msg}
    else:
        # Remove job
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
            logger.info("Schedule removed.")
        return {"message": "Schedule disabled"}

@app.delete("/api/schedule")
def delete_schedule():
    job_id = 'collector_update'
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
        return {"message": "Schedule deleted"}
    return {"message": "No active schedule found"}

@app.delete("/api/cleanup")
def cleanup_data(target: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB Connection Error")
    
    table_map = {
        "ohlcv": "ohlcv_1h",
        "funding": "funding_1h",
        "oi": "oi_1h",
        "logs": "collector_logs"
    }
    
    try:
        cur = conn.cursor()
        if target == "all":
            for tbl in table_map.values():
                cur.execute(f"TRUNCATE TABLE {tbl}")
            msg = "All data cleared."
        elif target in table_map:
            cur.execute(f"TRUNCATE TABLE {table_map[target]}")
            msg = f"Table {table_map[target]} cleared."
        else:
            raise HTTPException(status_code=400, detail="Invalid target")
        
        conn.commit()
        conn.close()
        logger.info(f"Cleanup executed: {target}")
        return {"status": "success", "message": msg}
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/view/{data_type}")
def get_data_view(data_type: str, page: int = 1, limit: int = 50, asset: Optional[str] = None):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB Connection Error")
    
    # Map types to tables and columns
    # Returns [columns], table_name, order_by
    config = {
        "ohlcv": {
            "table": "ohlcv_1h",
            "cols": ["ts", "asset", "open", "high", "low", "close", "volume"],
            "order": "ts DESC, asset"
        },
        "funding": {
            "table": "funding_1h",
            "cols": ["ts", "asset", "funding_rate"],
            "order": "ts DESC, asset"
        },
        "oi": {
            "table": "oi_1h",
            "cols": ["ts", "asset", "open_interest"],
            "order": "ts DESC, asset"
        },
        "logs": {
            "table": "collector_logs",
            "cols": ["execution_time", "status", "message"],
            "order": "id DESC" # logs usually have ID
        }
    }
    
    if data_type not in config:
        raise HTTPException(status_code=404, detail="Invalid data type")
        
    cfg = config[data_type]
    offset = (page - 1) * limit
    
    try:
        cur = conn.cursor()
        
        # Base Query
        query_cols = ", ".join(cfg["cols"])
        where_clause = ""
        params = []
        
        if asset and data_type != "logs":
            where_clause = "WHERE asset = %s"
            params.append(asset)
            
        # Count Total
        count_query = f"SELECT COUNT(*) FROM {cfg['table']} {where_clause}"
        cur.execute(count_query, tuple(params))
        total_count = cur.fetchone()[0]
        
        # Fetch Data
        data_query = f"SELECT {query_cols} FROM {cfg['table']} {where_clause} ORDER BY {cfg['order']} LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        cur.execute(data_query, tuple(params))
        rows = cur.fetchall()
        
        conn.close()
        
        # Format Result
        result = []
        for r in rows:
            obj = {}
            for i, col in enumerate(cfg["cols"]):
                val = r[i]
                # Handle dates for JSON
                if isinstance(val, datetime):
                    val = val.isoformat()
                obj[col] = val
            result.append(obj)
            
        return {
            "data": result,
            "total": total_count,
            "page": page,
            "limit": limit,
            "pages": (total_count + limit - 1) // limit
        }
        
    except Exception as e:
        logger.error(f"View Fetch Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/settings")
def get_settings():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB Connection Error")
    try:
        cur = conn.cursor()
        # Ensure table exists (main.py does it, but good to be safe)
        cur.execute("SELECT key, value FROM app_settings")
        rows = cur.fetchall()
        settings = {r[0]: r[1] for r in rows}
        conn.close()
        return settings
    except Exception as e:
        # Table might not exist yet if main.py hasn't run
        return {}

class SettingsUpdate(BaseModel):
    key: str
    value: str

@app.post("/api/settings")
def update_setting(req: SettingsUpdate):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB Connection Error")
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO app_settings (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (req.key, req.value))
        conn.commit()
        conn.close()
        return {"status": "success", "key": req.key, "value": req.value}
    except Exception as e:
        logger.error(f"Settings Update Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- File Upload & Processing ---

# Configurable Paths with defaults for local dev
STORAGE_PATH_INPUT = os.getenv("STORAGE_PATH_INPUT", "./data/input")
STORAGE_PATH_OUTPUT = os.getenv("STORAGE_PATH_OUTPUT", "./data/output")
STORAGE_PATH_ARCHIVE = os.getenv("STORAGE_PATH_ARCHIVE", "./data/archiv")
STORAGE_PATH_FEEDBACK = os.getenv("STORAGE_PATH_FEEDBACK", "./data/feedback")

# Ensure directories exist
for path in [STORAGE_PATH_INPUT, STORAGE_PATH_OUTPUT, STORAGE_PATH_ARCHIVE, STORAGE_PATH_FEEDBACK]:
    if not os.path.exists(path):
        try:
            os.makedirs(path)
            logger.info(f"Created directory: {path}")
        except Exception as e:
            logger.error(f"Failed to create directory {path}: {e}")

UPLOAD_FOLDER = STORAGE_PATH_INPUT

# --- Validation & Schema ---

SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Vorgabefile", "standard_structure.json")
SCHEMA_REF = None

def load_schema():
    global SCHEMA_REF
    if os.path.exists(SCHEMA_PATH):
        try:
             with open(SCHEMA_PATH, 'r') as f:
                SCHEMA_REF = json.load(f)
                logger.info("Validation schema loaded.")
        except Exception as e:
            logger.error(f"Failed to load schema: {e}")

load_schema()

def validate_rec(data, schema_dict, path=""):
    errors = []
    
    # Iterate over expected fields in schema
    for key, expected_type_str in schema_dict.items():
        if key not in data:
            errors.append(f"Missing field: {path}{key}")
            continue
            
        value = data[key]
        
        # Type Validation
        if expected_type_str == "string":
            if not isinstance(value, str):
                errors.append(f"Field '{path}{key}' must be string, got {type(value).__name__}")
        elif expected_type_str == "integer":
             if not isinstance(value, int) and not (isinstance(value, str) and value.isdigit()):
                errors.append(f"Field '{path}{key}' must be integer")
        elif expected_type_str == "boolean":
             if not isinstance(value, bool):
                 errors.append(f"Field '{path}{key}' must be boolean")
        # Add more types from standard_structure.json if needed
        # 'datetime' is usually a string in JSON, check format if strict
        elif expected_type_str == "datetime":
             pass # TODO: Strict ISO format check?
             
    return errors

def validate_json_logic(data: dict) -> list:
    """
    Validates data against SCHEMA_REF and enforcing strict non-empty strings.
    """
    errors = []
    if not SCHEMA_REF:
        return ["Server Error: Schema not loaded"]
    
    if not isinstance(data, dict):
        return ["Root must be a JSON object"]

    # 1. Validate Root fields
    required_top = ["asset", "period_start", "period_end", "episodes"]
    for req in required_top:
        if req not in data:
            errors.append(f"Missing top-level field: {req}")
        elif isinstance(data[req], str) and not data[req].strip():
             errors.append(f"Top-level field '{req}' cannot be empty")
            
    if "episodes" in data:
        if not isinstance(data["episodes"], list):
            errors.append("'episodes' must be a list")
        else:
            # 2. Validate Episodes
            for i, ep in enumerate(data["episodes"]):
                if not isinstance(ep, dict):
                     errors.append(f"Episode {i}: Must be an object")
                     continue

                # Critical fields that MUST be present and non-empty
                critical_fields = ["event_id", "created_at", "updated_at", "time_start"]
                
                for field in critical_fields:
                    if field not in ep:
                        errors.append(f"Episode {i}: Missing field '{field}'")
                    else:
                        val = ep[field]
                        # Check strict string emptiness
                        if isinstance(val, str):
                            if not val.strip():
                                errors.append(f"Episode {i}: Field '{field}' cannot be empty")
                        elif val is None:
                             errors.append(f"Episode {i}: Field '{field}' cannot be null")

    return errors

def transform_json_logic(data: dict) -> list:
    """
    Flattens structure.
    """
    # ... Placeholder for now, postponed
    return []

def create_tables():
    """Ensures the market_episodes table exists."""
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS market_episodes (
                    event_id TEXT PRIMARY KEY,
                    asset TEXT,
                    time_start TIMESTAMP,
                    time_end TIMESTAMP,
                    duration FLOAT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    raw_data JSONB,
                    event_name TEXT,
                    event_category TEXT,
                    market_regime TEXT,
                    signal_type_overall TEXT,
                    confidence_score INTEGER,
                    impact_strength INTEGER,
                    similar_pattern_refs TEXT
                );
            """)
            conn.commit()
            logger.info("Table 'market_episodes' verified/created.")
            conn.close()
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")

def save_to_db_logic(data: dict):
    """
    Saves transformed episodes to the database.
    Expects data to have 'asset' and 'episodes' list.
    """
    conn = get_db_connection()
    if not conn:
        logger.error("Skipping DB save: No connection.")
        return

    try:
        cur = conn.cursor()
        asset = data.get("asset", "UNKNOWN")
        
        saved_count = 0
        for ep in data.get("episodes", []):
            if not isinstance(ep, dict): continue
            
            event_id = ep.get("event_id")
            if not event_id: continue
            
            # Extract standard fields
            time_start = ep.get("time_start")
            time_end = ep.get("time_end")
            duration = ep.get("duration")
            created_at = ep.get("created_at")
            updated_at = ep.get("updated_at")
            
            # Use raw_data for the whole object to preserve everything
            raw_data = json.dumps(ep)
            
            # UPSERT logic
            sql = """
                INSERT INTO market_episodes (event_id, asset, time_start, time_end, duration, created_at, updated_at, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    asset = EXCLUDED.asset,
                    time_start = EXCLUDED.time_start,
                    time_end = EXCLUDED.time_end,
                    duration = EXCLUDED.duration,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    raw_data = EXCLUDED.raw_data;
            """
            cur.execute(sql, (event_id, asset, time_start, time_end, duration, created_at, updated_at, raw_data))
            saved_count += 1
            
        conn.commit()
        logger.info(f"Saved {saved_count} episodes to DB.")
        conn.close()
    except Exception as e:
        logger.error(f"DB Save failed: {e}")

# --- Sync Workflow Control ---

sync_process = None

@app.post("/api/sync/start")
def start_sync():
    global sync_process
    if sync_process and sync_process.poll() is None:
        return {"status": "already_running", "pid": sync_process.pid}
    
    try:
        # Use sys.executable to ensure we use the same python interpreter
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sync_workflow.py")
        # Run as a separate process
        # creationflags=subprocess.CREATE_NEW_CONSOLE specifically for Windows if we wanted a separate window, 
        # but for background we stick to default or DETACHED. 
        # For now, standard Popen.
        sync_process = subprocess.Popen([sys.executable, script_path])
        return {"status": "started", "pid": sync_process.pid}
    except Exception as e:
        logger.error(f"Failed to start sync: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/sync/stop")
def stop_sync():
    global sync_process
    if sync_process and sync_process.poll() is None:
        try:
            sync_process.terminate()
            # Windows optional: sync_process.kill() if terminate doesn't work well with python scripts
            sync_process.wait(timeout=5)
            sync_process = None
            return {"status": "stopped"}
        except Exception as e:
            logger.error(f"Failed to stop sync: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    return {"status": "not_running"}

@app.get("/api/sync/status")
def get_sync_status():
    global sync_process
    is_running = sync_process is not None and sync_process.poll() is None
    return {"running": is_running, "pid": sync_process.pid if is_running else None}

class FileUpdate(BaseModel):
    content: str # JSON string

# --- File Management API ---

@app.get("/api/file/{filename}")
async def get_file(filename: str):
    safe_name = os.path.basename(filename)
    file_path = os.path.join(STORAGE_PATH_INPUT, safe_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return {"filename": safe_name, "content": content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/file/{filename}")
async def update_file(filename: str, update: FileUpdate):
    safe_name = os.path.basename(filename)
    file_path = os.path.join(STORAGE_PATH_INPUT, safe_name)
    
    # 1. Validate content is valid JSON first
    try:
        json_data = json.loads(update.content)
    except json.JSONDecodeError:
         raise HTTPException(status_code=400, detail="Invalid JSON format")
         
    # 2. Schema Validation
    validation_errors = validate_json_logic(json_data)
    if validation_errors:
        # Save anyway? User said "Editor... correction". verification logic happens on save.
        # If we return error here, frontend stays in editor.
        return JSONResponse(status_code=400, content={"status": "validation_error", "errors": validation_errors, "saved": False})

    # 3. Save if valid
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(update.content)
    except Exception as e:
         raise HTTPException(status_code=500, detail=f"Save failed: {e}")

    return {"status": "success", "message": "File updated and verified successfully"}

@app.delete("/api/file/{filename}")
async def delete_file(filename: str):
    safe_name = os.path.basename(filename)
    file_path = os.path.join(STORAGE_PATH_INPUT, safe_name)
    if os.path.exists(file_path):
        os.remove(file_path)
        return {"status": "success", "message": "File deleted"}
    return {"status": "ignored", "message": "File not found"}

@app.get("/api/output/stats")
def get_output_stats():
    if not os.path.exists(STORAGE_PATH_OUTPUT):
        return {"count": 0}
    try:
        files = [name for name in os.listdir(STORAGE_PATH_OUTPUT) if os.path.isfile(os.path.join(STORAGE_PATH_OUTPUT, name))]
        return {"count": len(files)}
    except Exception as e:
        logger.error(f"Error counting output files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/process/{filename}")
async def process_file(filename: str):
    safe_name = os.path.basename(filename)
    input_path = os.path.join(STORAGE_PATH_INPUT, safe_name)
    output_path = os.path.join(STORAGE_PATH_OUTPUT, safe_name)
    
    if not os.path.exists(input_path):
        raise HTTPException(status_code=404, detail="File not found in input buffer")
        
    try:
        # 1. Read Input
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # 2. Transformation: Calculate Duration
        if "episodes" in data and isinstance(data["episodes"], list):
            for i, ep in enumerate(data["episodes"]):
                if isinstance(ep, dict):
                    # Parse times
                    try:
                        # Assumes ISO format like "2025-01-01T00:00:00Z" or "2025-01-01 00:00:00"
                        # We use dateutil if available or rudimentary substitution if strictly ISO
                        # Let's try simple replacement of T/Z for standard datetime.fromisoformat support in Py3.7+
                        t_start_str = ep.get("time_start", "").replace("Z", "+00:00")
                        t_end_str = ep.get("time_end", "").replace("Z", "+00:00")
                        
                        if t_start_str and t_end_str:
                            t_start = datetime.fromisoformat(t_start_str)
                            t_end = datetime.fromisoformat(t_end_str)
                            
                            # Diff in hours
                            diff = t_end - t_start
                            duration_hours = diff.total_seconds() / 3600.0
                            val_duration = round(duration_hours, 2)
                            
                            # Rebuild dict to preserve order (replace duration_category with duration at same pos)
                            new_ep = {}
                            processed_duration = False
                            
                            for k, v in ep.items():
                                if k == "duration_category":
                                    new_ep["duration"] = val_duration
                                    processed_duration = True
                                else:
                                    # Fallback: if duration already existed (unlikely), update it? 
                                    # Logic: If we happen to have "duration" key already, just overwrite or keep?
                                    # Simplest: Just copy.
                                    new_ep[k] = v
                            
                            # If duration_category wasn't found but we calculated duration, append it
                            if not processed_duration:
                                new_ep["duration"] = val_duration # fallback if not replaced
                                
                            # Replace the episode object in the list
                            data["episodes"][i] = new_ep
                    except Exception as trans_err:
                        logger.warning(f"Transformation warning in episode {i}: {trans_err}")
                        # Continue processing other episodes? Or fail hard? 
                        # User requested this transform, so maybe better to proceed but log.

            # 3. New Transformation: Event ID based on Filename
            # Logic: Remove first word (prefix) before first underscore, then append iterator
            try:
                base_name = os.path.splitext(safe_name)[0] # remove extension
                if "_" in base_name:
                    parts = base_name.split("_")
                    # Remove the first part (prefix like 'base' or 'refined')
                    if len(parts) > 1:
                        new_base = "_".join(parts[1:])
                    else:
                        new_base = base_name # Fallback if no underscore
                else:
                    new_base = base_name

                for i, ep in enumerate(data["episodes"]):
                    if isinstance(ep, dict):
                        # Construct new event_id: new_base + "_" + iterator (1-based)
                        ep["event_id"] = f"{new_base}_{i+1}"
                        
            except Exception as e:
                 logger.warning(f"Error transforming event IDs: {e}")

        # 4. File Routing (Archive Only / Revised)
        # Logic: ALL files -> Archive. Output -> Disabled.
        
        lower_name = safe_name.lower()
        is_refined = lower_name.startswith("refined_")
        
        
        archive_path = os.path.join(STORAGE_PATH_ARCHIVE, safe_name)
        
        # Save to Archive (ALL files)
        with open(archive_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        logger.info(f"Saved to Archive: {safe_name}")

        # 4b. Split to Markdown (Refined files ONLY)
        if is_refined and "episodes" in data:
            try:
                for ep in data["episodes"]:
                    if not isinstance(ep, dict): continue
                    
                    ev_id = ep.get("event_id")
                    if not ev_id: continue
                    
                    # Create content
                    md_filename = f"{ev_id}.md"
                    md_path = os.path.join(STORAGE_PATH_OUTPUT, md_filename)
                    
                    md_content = "```json\n" + json.dumps(ep, indent=4) + "\n```"
                    
                    with open(md_path, 'w', encoding='utf-8') as f:
                        f.write(md_content)
                        
                logger.info(f"Split {len(data['episodes'])} episodes to Output for: {safe_name}")
            except Exception as e:
                logger.error(f"Failed to split episodes to MD: {e}")

        # Output saving of FULL JSON disabled per user request (Archive only for full file)
        # if not is_base: ...
            
        # 5. Save to Database
        # try:
        #    save_to_db_logic(data) # DISABLED per user request
        # except Exception as e:
        #    logger.error(f"Failed to save to DB: {e}")

        # 6. Remove Input
        os.remove(input_path)
        
        logger.info(f"Processed file: {safe_name} -> {output_path}")
        return {"status": "success", "message": f"Processed successfully: {safe_name} -> Output"}
    except Exception as e:
        logger.error(f"Process failed for {safe_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        # 1. Save File
        file_location = os.path.join(UPLOAD_FOLDER, file.filename)
        with open(file_location, "wb+") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        logger.info(f"File saved to {file_location}")
        
        # 2. Parse JSON
        with open(file_location, 'r') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                return JSONResponse(status_code=400, content={"status": "error", "message": "Invalid JSON format", "filename": file.filename})
        
        # 3. Validate
        validation_errors = validate_json_logic(data)
        if validation_errors:
             return JSONResponse(status_code=400, content={"status": "validation_error", "errors": validation_errors, "filename": file.filename})

        # 4. Transform (Postponed)
        # try:
        #     transformed_data = transform_json_logic(data)
        # except Exception as e:
        #      return JSONResponse(status_code=500, content={"status": "error", "message": f"Transformation failed: {str(e)}"})

        # 5. Insert to DB
        try:
            save_to_db_logic(data)
        except Exception as e:
            return JSONResponse(status_code=500, content={"status": "error", "message": f"Database insertion failed: {str(e)}"})
            
        return {"status": "success", "message": "File Validated Successfully (DB Writing is disabled)", "filename": file.filename}
        
    except Exception as e:
        logger.error(f"Upload Error: {e}")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

@app.get("/api/gaps")
def check_gaps():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="DB Connection Error")
    
    gaps_report = {}
    assets = ["BTC", "ETH", "SOL"]
    
    try:
        cur = conn.cursor()
        
        # Determine Start Date
        cur.execute("SELECT value FROM app_settings WHERE key = 'target_start_date'")
        res = cur.fetchone()
        target_start_dt = None
        if res:
            try:
                target_start_dt = datetime.strptime(res[0], "%Y-%m-%d")
            except: pass
            
        now = datetime.now()
        
        for asset in assets:
            asset_gaps = []
            
            # --- OHLCV Gaps ---
            # If target start is set, check from there. Else check from first record.
            search_start = target_start_dt
            
            # Get actual min/max
            cur.execute("SELECT MIN(ts), MAX(ts) FROM ohlcv_1h WHERE asset = %s", (asset,))
            min_ts, max_ts = cur.fetchone()
            
            if min_ts and max_ts:
                # If target is set and earlier than min_ts, we have a "missing head" gap
                if target_start_dt and target_start_dt.replace(tzinfo=min_ts.tzinfo) < min_ts:
                     asset_gaps.append({
                        "type": "OHLCV",
                        "start": target_start_dt.isoformat(),
                        "end": min_ts.isoformat(),
                        "desc": "Missing data before current history"
                    })
                
                # Check internal gaps
                # Fetch all timestamps
                cur.execute("SELECT ts FROM ohlcv_1h WHERE asset = %s ORDER BY ts ASC", (asset,))
                timestamps = [row[0] for row in cur.fetchall()]
                
                # Iterate and check 1h diff
                for i in range(len(timestamps) - 1):
                    t1 = timestamps[i]
                    t2 = timestamps[i+1]
                    diff = (t2 - t1).total_seconds()
                    if diff > 3600 * 1.1: # Allow small drift, but > 1h 6m is a gap
                         asset_gaps.append({
                            "type": "OHLCV",
                            "start": t1.isoformat(),
                            "end": t2.isoformat(),
                            "desc": f"Gap of {int(diff/3600)} hours"
                        })
            
            elif target_start_dt:
                # No data at all, but target set
                 asset_gaps.append({
                    "type": "OHLCV",
                    "start": target_start_dt.isoformat(),
                    "end": "NOW",
                    "desc": "No data found"
                })

            # --- OI Gaps ---
            # OI logic: check internal gaps only (no hard start date enforcement usually)
            cur.execute("SELECT ts FROM oi_1h WHERE asset = %s ORDER BY ts ASC", (asset,))
            oi_timestamps = [row[0] for row in cur.fetchall()]
            
            for i in range(len(oi_timestamps) - 1):
                t1 = oi_timestamps[i]
                t2 = oi_timestamps[i+1]
                diff = (t2 - t1).total_seconds()
                if diff > 3600 * 1.5: # OI allows bigger drift, but checking for large holes
                     asset_gaps.append({
                        "type": "OI",
                        "start": t1.isoformat(),
                        "end": t2.isoformat(),
                        "desc": f"Gap of {int(diff/3600)} hours"
                    })

            if asset_gaps:
                gaps_report[asset] = asset_gaps

        conn.close()
        return {"gaps": gaps_report}

    except Exception as e:
        logger.error(f"Gap Check Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/logs")
def get_logs_legacy():
    # UI uses /api/logs for the console.
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT execution_time, status, message FROM collector_logs ORDER BY id DESC LIMIT 50")
            rows = cur.fetchall()
            conn.close()
            # Format: "[TIME] [STATUS] Message"
            logs = []
            for r in rows:
                ts = r[0].strftime("%H:%M:%S") if r[0] else ""
                logs.append(f"[{ts}] [{r[1]}] {r[2]}")
            return {"logs": logs}
        except: pass
    return {"logs": ["DB Log Fetch Error"]}

# Mount frontend
app.mount("/", StaticFiles(directory="web", html=True), name="static")

if __name__ == "__main__":
    import uvicorn
    # Note: reload=True in dev might cause double scheduler init issues unless careful.
    # lifespan handles it better.
    uvicorn.run("server:app", host="0.0.0.0", port=8888, reload=True)
