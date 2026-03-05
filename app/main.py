from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.services.mqtt_service import start_mqtt
from app.api.api import api_router 
from app.database.db import get_db_connection
from app.database.repositories.area_repository import AreaRepository
import threading
import time

def check_manual_timeout():
    """Chạy ngầm mỗi 60s để kiểm tra và xóa override manual hết hạn"""
    import sqlite3
    from app.database.db import DB_PATH
    while True:
        try:
            conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
            conn.row_factory = sqlite3.Row
            try:
                repo = AreaRepository(conn)
                expired_areas = repo.check_and_clear_manual_timeouts()
                if expired_areas:
                    for aid in expired_areas:
                        print(f"Area {aid} returned to AUTO mode after manual timeout")
            finally:
                conn.close()
        except Exception as e:
            print(f"Error in check_manual_timeout loop: {e}")
        time.sleep(60)

def check_schedules_loop():
    """Chạy ngầm mỗi 60s để kiểm tra và áp dụng Schedule chủ động"""
    import sqlite3
    import time
    from app.database.db import DB_PATH
    from app.database.repositories.area_repository import AreaRepository
    from app.core.lighting_controller import LightingController
    
    while True:
        try:
            conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
            conn.row_factory = sqlite3.Row
            try:
                repo = AreaRepository(conn)
                controller = LightingController(repo)
                areas = repo.get_all_areas_status()
                for area in areas:
                    area_id = area["area_id"]
                    
                    # 1. Bỏ qua nếu đang bị Override (P1)
                    override = repo.get_override_status(area_id)
                    if override and override.get("is_overridden"):
                        continue
                        
                    # 2. Lấy Schedule
                    schedule = repo.get_active_schedule(area_id)
                    if schedule:
                        desired = controller._normalize_state_from_schedule(schedule)
                        if desired:
                            # 3. Tránh spam: kiểm tra mode hiện tại
                            current_mode = override.get("current_mode") if override else None
                            if current_mode == desired:
                                continue
                                
                            print(f"[SCHEDULE] Proactive apply {desired} for Area {area_id}")
                            decision = {
                                "action": desired,
                                "source": "schedule",
                                "reason": f"System Schedule Enforced: {desired}"
                            }
                            controller.process_decision(area_id, decision)
            finally:
                conn.close()
        except Exception as e:
            print(f"Error in check_schedules_loop: {e}")
        time.sleep(60)

@asynccontextmanager
async def lifespan(app: FastAPI):  
    print("Starting up services...")         
    
    # Start MQTT Service
    try:
        start_mqtt()
        print("MQTT Service start listenting...")
    except Exception as e:
        print(f"Error when starting MQTT: {e}")

    # Start Background Task kiểm tra timeout
    timeout_thread = threading.Thread(target=check_manual_timeout, daemon=True)
    timeout_thread.start()
    print("Background Job 'check_manual_timeout' started...")

    # Start Background Task kiểm tra Lịch trình (Schedules)
    schedule_thread = threading.Thread(target=check_schedules_loop, daemon=True)
    schedule_thread.start()
    print("Background Job 'check_schedules_loop' started...")

    yield
    print("Shutting down services...")

app = FastAPI(title="Campus Lighting System", lifespan=lifespan)

# Config CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
app.include_router(api_router, prefix="/api")

@app.get("/")
def read_root():
    return {
        "status": "Backend is running",
        "services": {
            "mqtt": "active"
        }
    }