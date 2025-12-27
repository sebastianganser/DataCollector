import os
import logging
import psycopg2
import subprocess
import sys
from fastapi import FastAPI, HTTPException
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
    scheduler.start()
    logger.info("Scheduler started.")
    
    yield
    
    # Shutdown
    if scheduler:
        scheduler.shutdown()
        logger.info("Scheduler shut down.")

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
