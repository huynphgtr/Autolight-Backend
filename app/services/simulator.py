import sqlite3
import json
import time
import random
import logging
import paho.mqtt.client as mqtt
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

BROKER = "broker.emqx.io"
PORT = 1883
DB_PATH = "app.db"

class CameraSimulator:
    def __init__(self):
        self.client = mqtt.Client(f"Sim_AI_Camera_{random.randint(1000, 9999)}")
        self.camera_list = [] 
        self.start_time = time.time()

    def load_cameras(self):
        """Lấy thông tin IP và Topic của các Camera từ DB"""
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            query = "SELECT ip_address, mqtt_topic FROM devices WHERE UPPER(device_type) = 'CAMERA'"
            cur.execute(query)
            rows = cur.fetchall()
            
            self.camera_list = [
                {"topic": r["mqtt_topic"], "ip": r["ip_address"]} 
                for r in rows if r["mqtt_topic"]
            ]
            conn.close()
            logger.info(f"Đã nạp {len(self.camera_list)} Camera từ DB.")
        except Exception as e:
            logger.error(f"Lỗi nạp dữ liệu: {e}")
            raise

    def run(self):
        self.load_cameras()
        self.client.connect(BROKER, PORT, 60)
        self.client.loop_start()

        try:
            while True:
                elapsed = time.time() - self.start_time
                is_empty = 30 < elapsed < 60 # Kịch bản không người

                for cam in self.camera_list:
                    # Tạo dữ liệu giả lập
                    p_count = 0 if is_empty else random.randint(0, 10)
                    lux_val = 500 if is_empty else random.randint(50, 800)

                    # PAYLOAD QUAN TRỌNG: Gửi kèm IP để Backend tra cứu map
                    payload = {
                        "ip": cam["ip"],
                        "person_count": p_count,
                        "lux": lux_val,
                        "timestamp": datetime.now().isoformat()
                    }

                    self.client.publish(cam["topic"], json.dumps(payload))
                    logger.info(f"Sent to {cam['topic']} (IP: {cam['ip']}): {p_count} người, {lux_val} lux")

                time.sleep(5)
                if elapsed > 90: self.start_time = time.time()

        except KeyboardInterrupt:
            self.client.disconnect()

if __name__ == "__main__":
    CameraSimulator().run()