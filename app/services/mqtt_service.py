"""MQTT Service using paho-mqtt to subscribe to camera topics and publish relay commands.

- Connects to broker and subscribes to camera topics stored in DB.
- On JSON message: extracts ip, person_count, lux and calls LightingController.decide.
- Publishes resulting command to relay topic(s) in the same area.

Usage:
    svc = MqttService(broker_host='localhost', broker_port=1883)
    svc.start()
    ...
    svc.stop()
"""
from __future__ import annotations
import json
import logging
import os
from typing import Dict, Optional
import paho.mqtt.client as mqtt
from app.database.repositories.device_repository import DeviceRepository
from app.database.repositories.area_repository import AreaRepository
from app.core.lighting_controller import LightingController
from app.core.device_controller import DeviceController
import sqlite3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class MqttService:
    def __init__(
        self,
        broker_host: Optional[str] = None,
        broker_port: Optional[int] = None,
        keepalive: Optional[int] = None,
        client_id: Optional[str] = None,
        DATABASE_URL: Optional[str] = None,
    ) -> None:
        self.broker_host = os.getenv("BROKER_HOST", "broker.emqx.io")
        self.broker_port = int(os.getenv("BROKER_PORT", "1883"))
        self.keepalive = int(os.getenv("KEEPALIVE", "60"))
        self.client_id = os.getenv("CLIENT_ID", "autolight-controller")
        raw_url = os.getenv("DATABASE_URL", "sqlite:///./app.db")
        self.DATABASE_URL = raw_url
        db_path = raw_url.replace("sqlite:///", "")
        self._client: Optional[mqtt.Client] = None
        self._running = False
        self.db_conn = sqlite3.connect(db_path, check_same_thread=False)
        self.db_conn.row_factory = sqlite3.Row          
        self.lighting_controller = LightingController(area_repository=AreaRepository(db_conn=self.db_conn))
        self.device_controller = DeviceController(device_repository=DeviceRepository(db_conn=self.db_conn))
        self.area_repository = AreaRepository(db_conn=self.db_conn)
        self._camera_topic_map: Dict[str, str] = {}
        self._relay_topic_map: Dict[str, str] = {}

    # ----- MQTT callbacks -----
    def _on_connect(self, client, userdata, flags, rc):
        logger.info("Connected to MQTT broker %s:%d (rc=%s)", self.broker_host, self.broker_port, rc)
        camera_topics = self.device_controller.load_camera_topics()
        relay_topics = self.device_controller.load_relay_topics()

        for c in camera_topics:
            try:
                client.subscribe(c, qos=1)
                logger.info("Subscribed to camera topic: %s", c)
            except Exception as e:
                logger.exception("Failed to subscribe to %s: %s", c, e)        
        
        # for r in relay_topics:
        #     try:
        #         client.subscribe(r, qos=1)
        #         logger.info("Subscribed to relay topic: %s", r)
        #     except Exception as e:
        #         logger.exception("Failed to subscribe to %s: %s", r, e)

    def _on_disconnect(self, client, userdata, rc):
        logger.warning("Disconnected from MQTT broker (rc=%s)", rc)

    def _on_message(self, client, userdata, msg):
        payload = msg.payload.decode("utf-8", errors="ignore")
        logger.debug("Received message on %s: %s", msg.topic, payload)

        # Try to parse JSON
        try:
            data = json.loads(payload)
        except Exception:
            logger.warning("Skipping non-JSON message from %s", msg.topic)
            return

        # Extract fields with tolerant keys
        ip = (
            data.get("ip")
            or data.get("ip_address")
            or data.get("camera_ip")
            or self._camera_topic_map.get(msg.topic)
        )

        person_count = (
            data.get("person_count")
            or data.get("current_person_count")
            or data.get("count")
            or data.get("people")
        )

        lux = data.get("lux") or data.get("illuminance") or data.get("lux_value")

        if ip is None:
            logger.warning("Message on %s missing ip and topic not mapped; skipping", msg.topic)
            return

        # Normalize person_count and lux
        # lux: dark, dim, medium, bright -> numeric
        try:
            person_count = int(person_count) if person_count is not None else 0
        except Exception:
            person_count = 0
        try:
            if isinstance(lux, str):
                s = lux.strip().lower()
                if s in ("dark", "low", "dim"):
                    lux = 100.0
                elif s in ("medium", "med", "normal"):
                    lux = 300.0
                elif s in ("bright", "high"):
                    lux = 600.0
                else:
                    lux = float(s)
            else:
                lux = float(lux) if lux is not None else 99999.0
        except Exception:
            lux = 99999.0

        logger.info("Processing camera message: ip=%s persons=%s lux=%s", ip, person_count, lux)

        # Lookup area by device ip FIRST before passing to process_decision
        dev = self.device_controller.get_device_by_ip(ip)
        if not dev:
            logger.warning("No device found with ip=%s; cannot find relay topics", ip)
            return
        area_id = dev.get("area_id")
        if area_id is None:
            logger.warning("Device record for ip=%s missing area_id", ip)
            return

        # Decide action
        decision = self.lighting_controller.decide(ip, person_count, lux)
        action = decision.get("action")
        if not action or action == "NOOP":
            return
        logger.info("Area %s: AI decision: %s due to %s", area_id, action, decision.get('reason', ''))

        # Delegate execution, DB update, Timer, and MQTT publishing to the Controller
        self.lighting_controller.process_decision(area_id, decision)

    # ----- Lifecycle -----
    def start(self) -> None:
        if self._running:
            logger.warning("MQTT service already running")
            return
        self._client = mqtt.Client(client_id=self.client_id)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect

        try:
            self._client.connect(self.broker_host, self.broker_port, keepalive=self.keepalive)
            # Start background loop
            self._client.loop_start()
            self._running = True
            logger.info("MQTT service started and connecting to %s:%d", self.broker_host, self.broker_port)
        except Exception:
            logger.exception("Failed to start MQTT client")
            self._running = False

    def stop(self) -> None:
        if not self._running or self._client is None:
            logger.warning("MQTT service not running")
            return
        try:
            self._client.loop_stop()
            self._client.disconnect()
            logger.info("MQTT service stopped")
        except Exception:
            logger.exception("Error stopping MQTT service")
        finally:
            self._client = None
            self._running = False

_mqtt_instance: Optional[MqttService] = None

def start_mqtt(broker_host: str = "localhost", broker_port: int = 1883) -> None:
    global _mqtt_instance
    if _mqtt_instance is not None:
        logger.info("MQTT service already started")
        return
    _mqtt_instance = MqttService(broker_host=broker_host, broker_port=broker_port)
    _mqtt_instance.start()

def stop_mqtt() -> None:
    global _mqtt_instance
    if _mqtt_instance is None:
        logger.info("MQTT service not running")
        return
    _mqtt_instance.stop()
    _mqtt_instance = None
