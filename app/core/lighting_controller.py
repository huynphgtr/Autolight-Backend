"""Lighting controller implementing the priority logic for turning lights ON/OFF.

Priority (highest -> lowest):
- P1 Manual Override: if override is active, do nothing (NOOP) and preserve state
- P2 Schedule: if an active schedule exists, follow its action_state
- P3 Auto AI: use person_count and lux compared to area's config (min_person, lux_threshold).
  If conditions for ON are not met, apply off_delay before returning OFF (return OFF_DELAYED if delay > 0).

All methods return a dictionary describing the resulting action and metadata for the caller.
"""
from __future__ import annotations
import threading
from typing import Any, Dict, Optional

import logging
from app.database import db
from app.database.repositories.area_repository import AreaRepository

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class LightingController:

    def __init__(self, area_repository: AreaRepository) -> None:
        self.repo = area_repository
        self._off_timers: Dict[int, threading.Timer] = {}
        self._lock = threading.Lock()
    
    def process_decision(self, area_id: int, decision: Dict[str, Any]):
        """
        Hàm điều phối: Nhận kết quả từ hàm decide() và thực thi hành động.
        """
        action = decision.get("action")

        # 1. Nếu có lệnh ON hoặc MANUAL, phải hủy Timer tắt trễ ngay lập tức
        if action in ["ON", "MANUAL", "MANUAL_ON", "MANUAL_OFF"]:
            self._cancel_off_timer(area_id)   
            if action == "ON":
                self._execute_on(area_id, decision)
        
        # 2. Xử lý logic Tắt trễ
        elif action == "OFF_DELAYED":
            off_delay = decision.get("off_delay", 0)
            self._start_off_timer(area_id, off_delay, decision)

        # 3. Nếu là OFF thuần túy (không delay)
        elif action == "OFF":
            self._cancel_off_timer(area_id)
            self._execute_off(area_id, decision)

    def _start_off_timer(self, area_id: int, delay: int, decision: Dict[str, Any] = None):
        """Khởi tạo bộ đếm ngược để tắt đèn"""
        with self._lock:
            # Nếu đã có Timer đang chạy cho khu vực này, không tạo thêm cái mới
            if area_id in self._off_timers:
                return

            logger.info(f"[TIMER] Starting {delay}s OFF delay for Area {area_id}")
            
            # Tạo Timer: sau 'delay' giây sẽ gọi hàm _execute_off
            timer = threading.Timer(delay, self._execute_off, args=[area_id, decision])
            self._off_timers[area_id] = timer
            timer.start()

    def _cancel_off_timer(self, area_id: int):
        """Cancel off timer when having override or presence detected"""
        with self._lock:
            timer = self._off_timers.pop(area_id, None)
            if timer:
                timer.cancel()
                logger.info(f"[TIMER] Cancelled OFF delay for Area {area_id} - Presence detected!")

    def _publish_mqtt(self, area_id: int, action: str, decision: dict = None):
        """Gửi lệnh MQTT thực tế đến tất cả các Relay trong Area"""
        try:
            from app.services.mqtt_service import _mqtt_instance
            if not _mqtt_instance or not getattr(_mqtt_instance, '_client', None):
                logger.error("[ACTUATOR] MQTT client not ready, cannot send command.")
                return

            relays = _mqtt_instance.device_controller.get_relays_for_area(area_id)
            if not relays:
                logger.warning(f"[ACTUATOR] No relays found for area {area_id}")
                return

            import json
            pub_payload = {"command": action, "meta": decision or {"source": "auto", "reason": "timer_expiration"}}
            text = json.dumps(pub_payload)

            for topic in relays:
                _mqtt_instance._client.publish(topic, text, qos=1)
                logger.info(f"--- [ACTUATOR] Published MQTT Command: {action} to Relay {topic} ---")
        except Exception as e:
            logger.error(f"[ACTUATOR] Failed to publish MQTT: {e}")

    def _execute_off(self, area_id: int, decision: Dict[str, Any] = None):
        """Hành động thực thi tắt đèn khi Timer kết thúc"""
        with self._lock:
            # Xóa khỏi danh sách quản lý nếu Timer tự kết thúc
            self._off_timers.pop(area_id, None)

        # 1. Cập nhật Database
        self.repo.set_area_auto(area_id=area_id, state="OFF", description="Auto OFF by system", decision=decision)
        
        # 2. Gửi lệnh tới Relay 
        self._publish_mqtt(area_id, "OFF")

    def _execute_on(self, area_id: int, decision: Dict[str, Any]):
        """Hành động thực thi bật đèn"""
        # Cập nhật DB
        self.repo.set_area_auto(area_id=area_id, state="ON", description=decision.get("reason", "Auto ON by system"), decision=decision)
        
        # Gửi lệnh tới Relay
        self._publish_mqtt(area_id, "ON", decision)

    def decide(self, ip_address: str, person_count: int, lux: float) -> Dict[str, Any]:
        """Decide the target light action for a device identified by ip_address.

        Returns a dict with at least the key 'action' which is one of:
          - 'NOOP' (do nothing / keep current state)
          - 'ON'
          - 'OFF'
          - 'OFF_DELAYED' (apply off_delay then off)

        Additional keys: 'source' ('override'|'schedule'|'auto'), and other metadata.
        """
        # Find area by device IP
        area = self.repo.get_area_by_device_ip(ip_address)
        if not area:
            return {"action": "NOOP", "reason": "area_not_found", "ip": ip_address}

        area_id = area.get("area_id") or area.get("id")
        if area_id is None:
            return {"action": "NOOP", "reason": "area_id_missing", "ip": ip_address}

        # P1: Manual override
        override = self.repo.get_override_status(area_id)
        if override and override.get("is_overridden"):
            # Keep current state; return NOOP and include override info
            return {
                "action": "NOOP",
                "source": "override",
                "override_until": override.get("override_until"),
                "detail": override,
            }

        # P2: Schedule
        schedule = self.repo.get_active_schedule(area_id)
        if schedule:
            desired = self._normalize_state_from_schedule(schedule)
            if desired in ("ON", "OFF"):
                return {"action": desired, "source": "schedule", "schedule": schedule}
            # if schedule exists but doesn't specify an action, fall through to P3

        # P3: Auto AI
        config = self.repo.get_config(area_id)
        if not config:
            return {"action": "NOOP", "reason": "config_missing", "area_id": area_id}

        min_person = self._to_number(config.get("min_person"), default=1)
        lux_threshold = self._to_number(config.get("lux_threshold"), default=0)
        off_delay = self._to_number(config.get("off_delay"), default=0)

        # Decide ON condition
        try:
            person_ok = (int(person_count) >= int(min_person))
        except Exception:
            person_ok = False

        try:
            lux_ok = (float(lux) < float(lux_threshold))
        except Exception:
            lux_ok = False

        if person_ok and lux_ok:
            return {"action": "ON", "source": "auto", "reason": "presence_and_low_lux", "person_count": person_count, "lux": lux, "min_person": min_person, "lux_threshold": lux_threshold}

        # Not meeting ON condition -> apply off_delay before OFF
        if off_delay and off_delay > 0:
            return {"action": "OFF_DELAYED", "source": "auto", "off_delay": off_delay, "reason": "thresholds_not_met", "person_count": person_count, "lux": lux, "min_person": min_person, "lux_threshold": lux_threshold}

        return {"action": "OFF", "source": "auto", "reason": "thresholds_not_met", "person_count": person_count, "lux": lux, "min_person": min_person, "lux_threshold": lux_threshold}
    
    # ----- Helper methods -----
    def _normalize_state_from_schedule(self, schedule_row: Dict[str, Any]) -> Optional[str]:
        """Kiểm tra thời gian hiện tại có nằm trong lịch trình hay không và trả về action_state.
        Nếu không nằm trong lịch trình, trả về None để Logic rơi xuống P3 (Auto AI).
        """
        import datetime

        # 1. Trích xuất action_state
        candidates = ("action_state", "state", "action", "command", "desired_state")
        action_val = None
        for k in candidates:
            if k in schedule_row:
                action_val = schedule_row.get(k)
                if action_val is not None:
                    break
        
        if action_val is None:
            return None
            
        desired_state = self._normalize_state(action_val)

        # 2. Kiểm tra ngày trong tuần (days_of_week)
        # Sử dụng timezone locale để đảm bảo đồng bộ với giờ hệ thống
        now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=7)))
        
        # Mapping weekday() từ 0-6 sang Mon-Sun
        day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        current_day_name = day_names[now.weekday()]
        
        days_str = str(schedule_row.get("days_of_week", schedule_row.get("days", "")))
        if days_str:
            # Xử lý chuỗi "Mon,Tue,Wed,Thu,Fri,Sat,Sun"
            valid_days = [d.strip() for d in days_str.split(",") if d.strip()]
            if valid_days and current_day_name not in valid_days:
                return None
                
        # 3. Kiểm tra thời gian (start_time -> end_time)
        try:
            start_str = schedule_row.get("start_time")
            end_str = schedule_row.get("end_time")
            
            if not start_str or not end_str:
                return desired_state  # Nếu không cài đặt giờ cụ thể, mặc định coi như active theo ngày
                
            start_time = datetime.datetime.strptime(str(start_str), "%H:%M:%S").time()
            end_time = datetime.datetime.strptime(str(end_str), "%H:%M:%S").time()
            current_time = now.time()
            
            # Xử lý trường hợp xuyên qua nửa đêm (VD: 22h tối đến 6h sáng)
            if start_time <= end_time:
                is_active = start_time <= current_time <= end_time
            else:
                is_active = current_time >= start_time or current_time <= end_time
                
            if is_active:
                return desired_state
                
        except Exception as e:
            logger.error(f"[SCHEDULE] Error parsing time: {e}")
            
        return None

    def _normalize_state(self, v: Any) -> Optional[str]:
        if isinstance(v, bool):
            return "ON" if v else "OFF"
        s = str(v).strip().lower()
        if s in ("on", "1", "true", "active", "enable", "enabled"):
            return "ON"
        if s in ("off", "0", "false", "inactive", "disable", "disabled"):
            return "OFF"
        return None

    def _to_number(self, v: Any, default: Optional[float] = None) -> Optional[float]:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return v
        try:
            if isinstance(v, str) and "." in v:
                return float(v)
            return int(v)
        except Exception:
            try:
                return float(v)
            except Exception:
                return default
