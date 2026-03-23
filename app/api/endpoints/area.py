from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from app.api.deps import get_area_repo, get_device_repo
from app.database import db
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, validator
from app.database.repositories.device_repository import DeviceRepository
from app.database.repositories.area_repository import AreaRepository
from app.core.area_controller import AreaController
from app.core.device_controller import DeviceController

router = APIRouter()

import json
from app.services.mqtt_service import _mqtt_instance

class AreaManualRequest(BaseModel):
    duration_minutes: Optional[int] = Field(None, ge=0, description="Duration in minutes. Leave empty to use area's default override timeout.")
    state: str = Field(..., description="Desired state: ON or OFF")

    @validator("state")
    def state_must_be_on_off(cls, v: str) -> str:
        v2 = v.strip().upper()
        if v2 not in ("ON", "OFF"):
            raise ValueError("state must be 'ON' or 'OFF'")
        return v2

class AreaConfigUpdateRequest(BaseModel):
    min_person: Optional[int] = Field(None, ge=0, description="Minimum number of people to trigger lights ON")
    lux_threshold: Optional[int] = Field(None, ge=0, description="Maximum lux value to trigger lights ON")
    override_timeout: Optional[int] = Field(None, ge=0, description="Duration of manual override before reverting to AUTO")
    off_delay: Optional[int] = Field(None, ge=0, description="Delay in seconds before turning lights OFF when empty")

@router.get("/status", response_model=List[Dict[str, Any]])
def get_list_areas_status(
                    area_controller: AreaController = Depends(get_area_repo),
                    device_controller: DeviceController = Depends(get_device_repo)
                    ) -> List[Dict[str, Any]]:
    """Return all areas with their status, current mode and config."""
    areas = area_controller.get_all_areas_status()
    results = []
    for a in areas:
        aid = a["area_id"]
        status = area_controller.get_override_status(aid)
        config = area_controller.get_config(aid)
        schedule = area_controller.get_active_schedule(aid)
        relays = device_controller.get_relays_for_area(aid)
        results.append(
            {
                "area_id": aid,
                "area_name": a.get("area_name"),
                "area_type": a.get("area_type"),
                "status": status,
                "config": config,
                "active_schedule": schedule or None,
                "relays": relays,
            }
        )
    return results

@router.post("/{area_id}/manual", response_model=Dict[str, Any])
def override_area(
                    area_id: int, 
                    payload: AreaManualRequest, 
                    area_controller: AreaController = Depends(get_area_repo),
                    device_controller: DeviceController = Depends(get_device_repo)
) -> Dict[str, Any]: 
    """Set an manual override (P1) for an area for given minutes and desired state (ON/OFF)."""
    
    # 1. Kiểm tra tồn tại thông qua controller
    if not area_controller.check_area_exists(area_id):
        raise HTTPException(status_code=404, detail="Area not found")

    # Xác định duration_minutes nếu không được truyền vào
    duration = payload.duration_minutes
    if not duration:
        config = area_controller.get_config(area_id)
        # Lấy thời gian giữ override mặc định từ Database, giả sử 60 phút nếu lấy lỗi
        duration = config.get("override_timeout", 60)
        # Nếu DB lưu là None thì lấy 60
        if not duration:
             duration = 60

    # 2. Tính toán override_until = datetime.now(...) + duration
    from datetime import timezone
    tz_vn = timezone(timedelta(hours=7))
    override_until = datetime.now(tz_vn) + timedelta(minutes=duration)

    # 3. Gọi controller.update_area_status để cập nhật current_mode='MANUAL', last_priority=1 và override_until
    try:
        area_controller.update_area_status(
            area_id=area_id, 
            current_mode=f'MANUAL_{payload.state}', 
            last_priority=1, 
            override_until=override_until
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    # 4. Gọi lighting_controller._cancel_off_timer(area_id) và thực thi lệnh Bật/Tắt ngay lập tức qua MQTT
    if _mqtt_instance and _mqtt_instance.lighting_controller:
        _mqtt_instance.lighting_controller._cancel_off_timer(area_id)
        
        # Khởi tạo payload metadata rõ ràng
        decision_meta = {"source": "override", "reason": "manual_override"}
        
        try:
            # Gọi trực tiếp _publish_mqtt, tính năng gửi tới tất cả relay topic đã được hàm này bao hàm
            _mqtt_instance.lighting_controller._publish_mqtt(area_id, payload.state, decision_meta)
        except Exception as e:
            # Nên in log lỗi ra để dễ debug thay vì pass ngầm
            import logging
            logging.error(f"Failed to publish manual override MQTT for area {area_id}: {e}")

    # 5. Trả về kết quả mới nhất
    status = area_controller.get_override_status(area_id)
    if not status:
         raise HTTPException(status_code=500, detail="Failed to retrieve updated status")         
    return status

@router.get("/{area_id}/history", response_model=List[Dict[str, Any]])
def get_history(
    area_id: int, area_controller: AreaController = Depends(get_area_repo)
    ) -> List[Dict[str, Any]]:
    """Return last 20 history log entries for the area."""
    rows = area_controller.get_history_logs(area_id, limit=20)
    return rows

@router.put("/{area_id}/config", response_model=Dict[str, Any])
def update_area_config(
        area_id: int,
        payload: AreaConfigUpdateRequest,
        area_controller: AreaController = Depends(get_area_repo)
) -> Dict[str, Any]:
    """Update configuration parameters (AI thresholds) for an area."""
    # 1. Kiểm tra tồn tại
    if not area_controller.check_area_exists(area_id):
        raise HTTPException(status_code=404, detail="Area not found")

    # 2. Xây dựng dictionary các giá trị cần cập nhật
    updates = payload.dict(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No parameters provided for update")

    # 3. Gọi Repo cập nhật CSDL
    try:
        area_controller.update_config(area_id, updates)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

    # 4. Trả về cấu hình mới nhất
    config = area_controller.get_config(area_id)
    if not config:
        raise HTTPException(status_code=500, detail="Failed to retrieve updated config")
        
    return config
