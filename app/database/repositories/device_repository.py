"""Repository for device-related database queries using sqlite3.
Provides:
- load_camera_topics()
- get_relays_by_area_id(area_id)
- get_relays_for_area(area_id)
- get_device_by_ip(ip)
- get_device_by_topic(topic)
All methods use context managers and return dictionaries/list (or empty dict/list if nothing found).
"""
from __future__ import annotations
import sqlite3
from typing import Any, Dict, List, Optional
import logging
from app.database.db import get_db_connection
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DeviceRepository:
    
    def __init__(self, db_conn: sqlite3.Connection) -> None:
        self.db = db_conn
        self._camera_topic_map: Dict[str, str] = {}
        self._relay_topic_map: Dict[str, str] = {}
        
    def load_camera_topics(self) -> List[str]:
        """Load camera topics and their ip addresses from devices table."""        
        cur = self.db.execute("SELECT ip_address, mqtt_topic FROM devices WHERE UPPER(device_type) = 'CAMERA'")
        rows = cur.fetchall()
        topics = []
        self._camera_topic_map.clear()
        for r in rows:
            topic = r["mqtt_topic"] 
            ip = r["ip_address"]
            if topic:
                topics.append(topic)
                if ip:
                    self._camera_topic_map[topic] = ip
        # logger.info("Loaded %d camera topics from DB", len(topics))
        return topics
    
    def load_relay_topics(self) -> List[str]:
        """Load relay topics and their ip addresses from devices table."""
        cur = self.db.execute("SELECT ip_address, mqtt_topic FROM devices WHERE UPPER(device_type) = 'RELAY'")
        rows = cur.fetchall()
        topics = []
        self._relay_topic_map.clear()
        for r in rows:
            topic = r["mqtt_topic"] 
            ip = r["ip_address"]
            if topic:
                topics.append(topic)
                if ip:
                    self._relay_topic_map[topic] = ip
        # logger.info("Loaded %d relay topics from DB", len(topics))
        return topics
    
    def get_relays_by_area_id(self, area_id: int) -> List[Dict[str, Any]]:                  
        cur = self.db.execute(
            "SELECT device_id, device_name, mqtt_topic, status FROM devices WHERE area_id = ? AND UPPER(device_type) = 'RELAY'",
            (area_id,),
        )
        relays = [dict(r) for r in cur.fetchall()]
        return relays

    def get_relays_for_area(self, area_id: int) -> List[str]:        
        cur = self.db.execute(
            "SELECT mqtt_topic FROM devices WHERE area_id = ? AND UPPER(device_type) = 'RELAY'",
            (area_id,),
        )
        rows = cur.fetchall()
        return [r["mqtt_topic"] for r in rows if r["mqtt_topic"]]

    def get_device_by_ip(self, ip: str) -> Optional[Dict[str, Any]]:        
        cur = self.db.execute("SELECT * FROM devices WHERE ip_address = ? LIMIT 1", (ip,))
        row = cur.fetchone()
        return dict(row) if row else None

    def get_device_by_topic(self, topic: str) -> Optional[Dict[str, Any]]:        
        cur = self.db.execute("SELECT * FROM devices WHERE mqtt_topic = ? LIMIT 1", (topic,))
        row = cur.fetchone()
        return dict(row) if row else None

