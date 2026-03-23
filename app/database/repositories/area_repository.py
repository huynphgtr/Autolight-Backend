"""Repository for area-related database queries using sqlite3.

All methods use context managers and return dictionaries (or empty dict if nothing found).
"""
from __future__ import annotations
import sqlite3
from datetime import datetime, time, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import null

class AreaRepository:
    
    def __init__(self, db_conn: sqlite3.Connection) -> None:
        self.db = db_conn

    def get_all_areas_status(self) -> List[Dict[str, Any]]:
        """Return a list of all areas with basic info: area_id, area_name, area_type."""
        cur = self.db.execute("SELECT area_id, area_name, area_type FROM areas ORDER BY area_id")
        rows = cur.fetchall()
        return [dict(r) for r in rows]

    def check_area_exists(self, area_id: int) -> bool:
        cur = self.db.execute("SELECT 1 FROM areas WHERE area_id = ? LIMIT 1", (area_id,))
        return cur.fetchone() is not None

    def update_area_status(self, area_id: int, current_mode: str, last_priority: int, override_until: datetime) -> None:
        """Cập nhật trạng thái area_status với mode và priority."""
        override_str = override_until.strftime("%Y-%m-%d %H:%M:%S")

        cur = self.db.execute("SELECT area_id FROM area_status WHERE area_id = ?", (area_id,))
        if cur.fetchone():
            self.db.execute(
                "UPDATE area_status SET override_until = ?, current_mode = ?, last_priority = ? WHERE area_id = ?",
                (override_str, current_mode, last_priority, area_id),
            )
        else:
            self.db.execute(
                "INSERT INTO area_status (area_id, override_until, last_priority, current_mode) VALUES (?, ?, ?, ?)",
                (area_id, override_str, last_priority, current_mode),
            )
        
        self.db.execute(
            "INSERT INTO history_log (area_id, event_type, description, created_at) VALUES (?, ?, ?, ?)",
            (
                area_id,
                "override",
                f"Override set to {current_mode} until {override_str} (Priority {last_priority})",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ),
        )
        self.db.commit()

    def check_and_clear_manual_timeouts(self) -> List[int]:
        """Tìm các khu vực có current_mode='MANUAL_ON/OFF' và override_until < now, cập nhật về AUTO."""
        from datetime import timezone
        tz_vn = timezone(timedelta(hours=7))
        now_str = datetime.now(tz_vn).strftime("%Y-%m-%d %H:%M:%S")
        
        cur = self.db.execute(
            "SELECT area_id FROM area_status WHERE current_mode LIKE 'MANUAL_%' AND override_until < ?",
            (now_str,)
        )
        expired_areas = [row["area_id"] for row in cur.fetchall()]
        
        if not expired_areas:
            return []

        for area_id in expired_areas:
            self.db.execute(
                "UPDATE area_status SET current_mode = 'AUTO', last_priority = 3, override_until = NULL WHERE area_id = ?",
                (area_id,)
            )
            self.db.execute(
                "INSERT INTO history_log (area_id, event_type, description, created_at) VALUES (?, ?, ?, ?)",
                (
                    area_id,
                    "auto",
                    f"Area {area_id} returned to AUTO mode after manual timeout",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ),
            )
            
        self.db.commit()
        return expired_areas

    def set_area_auto(self, area_id: int, state: str, description: str, decision: dict = None) -> None:
        """Excute login update area_status and history_log"""
        if decision:
             p_count = decision.get("person_count", "N/A")
             lux = decision.get("lux", "N/A")
             min_p = decision.get("min_person", "N/A")
             min_lux = decision.get("lux_threshold", "N/A")
             time_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
             detail_str = f"{description} Time: {time_str} | People (real/min): {p_count}/{min_p} | Light (real/min): {lux}/{min_lux}"
        else:
             detail_str = description

        # update or insert area_status
        cur = self.db.execute("SELECT area_id FROM area_status WHERE area_id = ?", (area_id,))
        if cur.fetchone():
            self.db.execute(
                "UPDATE area_status SET override_until = ?, current_mode = ?, last_priority = ? WHERE area_id = ?",
                (None, state, 3, area_id),
            )
        else:
            self.db.execute(
                "INSERT INTO area_status (area_id, override_until, last_priority, current_mode) VALUES (?, ?, ?, ?)",
                (area_id, None, 3, state),
            )
        # write log to history_log
        self.db.execute(
            "INSERT INTO history_log (area_id, event_type, description, created_at) VALUES (?, ?, ?, ?)",
            (
                area_id,
                "auto",
                f"Auto mode set to {state}, reason: {detail_str}",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ),
        )
        # save changes
        self.db.commit()

    # def get_override_status(self, area_id: int) -> Optional[Dict[str, Any]]:
    #     """Lấy trạng thái override hiện tại."""
    #     cur = self.db.execute("SELECT * FROM area_status WHERE area_id = ?", (area_id,))
    #     row = cur.fetchone()
    #     return dict(row) if row else None
    
    def get_area_by_device_ip(self, ip_address: str) -> Dict[str, Any]:
        """Return a dict with the area's id for a device IP, or empty dict if not found."""
        query = (
            "SELECT a.area_id AS area_id, a.* "
            "FROM devices d JOIN areas a ON d.area_id = a.area_id "
            "WHERE d.ip_address = ? LIMIT 1"
        )        
        cur = self.db.execute(query, (ip_address,))
        row = cur.fetchone()
        if not row:
            return {}
        return dict(row)

    def get_active_schedule(self, area_id: int) -> Dict[str, Any]:
        """Return the active schedule (as dict) for given area_id based on current local time/day.
        If multiple schedules match, return the first one found. If none, return empty dict.
        This method is tolerant to a few common schedule schema variants (columns names).
        """        
        cur = self.db.execute("SELECT * FROM schedules WHERE area_id = ?", (area_id,))
        rows = cur.fetchall()
        if not rows:
            return {}
        from datetime import timezone, timedelta
        tz_vn = timezone(timedelta(hours=7))
        now = datetime.now(tz_vn)
        current_time = now.time()
        # Normalize weekday to 0-6 where Monday == 0
        python_weekday = now.weekday()
        # SQLite strftime('%w') uses 0=Sunday, but we will compare in normalized form

        def parse_days(value) -> List[int]:
            """Try to parse a schedule 'days' representation into list of weekdays 0-6 (Mon=0)."""
            if value is None:
                return []
            if isinstance(value, int):
                # Could be 0-6 (Mon-Sun?) or 1-7, try both
                if 0 <= value <= 6:
                    return [value]
                if 1 <= value <= 7:
                    # convert 1-7 (Sun=1?) Unclear; assume 1=Monday -> convert to 0-6
                    return [value - 1]
                return []
            s = str(value).strip()
            if not s:
                return []
            # comma-separated list e.g. "0,1,2" or "Mon,Tue"
            parts = [p.strip() for p in s.split(",") if p.strip()]
            result = []
            name_map = {
                "mon": 0,
                "monday": 0,
                "tue": 1,
                "tuesday": 1,
                "wed": 2,
                "wednesday": 2,
                "thu": 3,
                "thursday": 3,
                "fri": 4,
                "friday": 4,
                "sat": 5,
                "saturday": 5,
                "sun": 6,
                "sunday": 6,
            }
            for p in parts:
                if p.isdigit():
                    try:
                        n = int(p)
                        # If it's 0-6 assume Mon=0; if 1-7 assume 1=Monday
                        if 0 <= n <= 6:
                            result.append(n)
                        elif 1 <= n <= 7:
                            result.append(n - 1)
                    except ValueError:
                        continue
                else:
                    key = p.lower()
                    if key in name_map:
                        result.append(name_map[key])
            return sorted(set(result))

        def parse_time(val: Any) -> Optional[time]:
            if val is None:
                return None
            if isinstance(val, time):
                return val
            s = str(val).strip()
            for fmt in ("%H:%M:%S", "%H:%M"):
                try:
                    return datetime.strptime(s, fmt).time()
                except Exception:
                    continue
            # try extracting HH:MM at start
            try:
                parts = s.split(":")
                hh = int(parts[0])
                mm = int(parts[1]) if len(parts) > 1 else 0
                return time(hh, mm)
            except Exception:
                return None

        for row in rows:
            # Respect an 'enabled' or 'is_active' flag if present
            enabled = None
            for key in ("enabled", "is_active", "active"):
                if key in row.keys():
                    v = row[key]
                    if v is None:
                        enabled = None
                    else:
                        enabled = bool(v)
                    break
            if enabled is False:
                continue

            # Determine applicable days
            days = []
            for key in ("days", "days_of_week", "day_of_week", "day", "weekday"):
                if key in row.keys():
                    days = parse_days(row[key])
                    break

            if days and python_weekday not in days:
                continue

            # Start / end times
            start = None
            end = None
            for key in ("start_time", "start", "time_from"):
                if key in row.keys():
                    start = parse_time(row[key])
                    break
            for key in ("end_time", "end", "time_to"):
                if key in row.keys():
                    end = parse_time(row[key])
                    break

            # If no time constraints, then this schedule is active for the day
            if start is None and end is None:
                return dict(row)

            # If times exist, check if current_time is within the interval, handle overnight wrap
            if start is None or end is None:
                # If only one bound present, be conservative: require current time to be after start or before end
                if start and current_time >= start:
                    return dict(row)
                if end and current_time < end:
                    return dict(row)
                continue

            if start <= end:
                if start <= current_time < end:
                    return dict(row)
            else:
                # overnight schedule (e.g., 22:00 -> 06:00)
                if current_time >= start or current_time < end:
                    return dict(row)

        return {}

    def get_override_status(self, area_id: int) -> Dict[str, Any]:
        """Return override status for area_id, including 'override_until' and a boolean 'is_overridden'.
        If not found, return empty dict.
        """
        query = "SELECT * FROM area_status WHERE area_id = ? LIMIT 1"
        cur = self.db.execute(query, (area_id,))
        row = cur.fetchone()
        if not row:
            return {}
        result = dict(row)

        # Normalize and compute is_overridden if override_until exists
        override_until = result.get("override_until")
        if override_until:
            try:
                # Expect ISO or sqlite datetime string
                dt = (
                    datetime.fromisoformat(override_until)
                    if "T" in override_until
                    else datetime.strptime(override_until, "%Y-%m-%d %H:%M:%S")
                )
                from datetime import timezone, timedelta
                tz_vn = timezone(timedelta(hours=7))
                dt = dt.replace(tzinfo=tz_vn) if dt.tzinfo is None else dt
                result["is_overridden"] = dt > datetime.now(tz_vn)
                # also return parsed ISO string in a consistent format
                result["override_until"] = dt.isoformat(" ")
            except Exception:
                # If parsing fails, leave raw value and set is_overridden to False
                result["is_overridden"] = False
        else:
            result["is_overridden"] = False

        return result

    def get_config(self, area_id: int) -> Dict[str, Any]:
        """Return configuration parameters for an area as a dict.
        Expected keys: min_person, lux_threshold, off_delay. If not found, return empty dict.
        """
        query = "SELECT * FROM config_param WHERE area_id = ? LIMIT 1"
        cur = self.db.execute(query, (area_id,))
        row = cur.fetchone()
        if not row:
            return {}
        cfg = dict(row)
        for key in ("min_person", "lux_threshold", "override_timeout", "off_delay"):
            if key in cfg and cfg[key] is not None:
                try:
                    val = cfg[key]
                    if isinstance(val, (int, float)):
                        cfg[key] = val
                    else:
                        sval = str(val)
                        if "." in sval:
                            cfg[key] = float(sval)
                        else:
                            cfg[key] = int(sval)
                except Exception:
                    pass
        return cfg

    def update_config(self, area_id: int, updates: Dict[str, Any]) -> None:
        """Update configuration AI parameters (min_person, lux_threshold, override_timeout, off_delay) for an area."""
        if not updates:
            return            
        valid_keys = {"min_person", "lux_threshold", "override_timeout", "off_delay"}
        filtered_updates = {k: v for k, v in updates.items() if k in valid_keys and v is not None}
        
        if not filtered_updates:
            return  
        
        set_clauses = [f"{k} = ?" for k in filtered_updates.keys()]
        values = list(filtered_updates.values())
        values.append(area_id)
        sql = f"UPDATE config_param SET {', '.join(set_clauses)} WHERE area_id = ?"
        self.db.execute(sql, tuple(values))
        self.db.commit()

    def get_history_logs(self, area_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        """Return list of history log entries for an area, ordered by most recent first."""
        query = "SELECT * FROM history_log WHERE area_id = ? ORDER BY created_at DESC LIMIT ?"       
        cur = self.db.execute(query, (area_id, limit,))
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    
    

