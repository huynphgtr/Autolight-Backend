import sqlite3
import json
import time
import random
import logging
import paho.mqtt.client as mqtt

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
        """Lấy danh sách camera từ DB"""
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            # Lấy thêm area_id để log cho dễ theo dõi kịch bản
            query = "SELECT ip_address, mqtt_topic, area_id FROM devices WHERE UPPER(device_type) = 'CAMERA'"
            cur.execute(query)
            rows = cur.fetchall()
            
            self.camera_list = [
                {"topic": r["mqtt_topic"], "ip": r["ip_address"], "area_id": r["area_id"]} 
                for r in rows if r["mqtt_topic"]
            ]
            conn.close()
            logger.info(f"--- [SIMULATOR] Đã nạp {len(self.camera_list)} Camera từ DB. ---")
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
                
                # KỊCH BẢN TEST:
                # 0-30s: Có người (Rải rác các camera)
                # 30-60s: Trống không (Tất cả camera báo 0)
                # 60-90s: Một camera thấy người, camera kia không thấy (Test cộng dồn)
                
                # if elapsed < 30:
                #     scenario = "CÓ NGƯỜI RẢI RÁC"
                #     def get_data(): return random.randint(1, 3), random.randint(1, 2)
                # elif 30 <= elapsed < 60:
                #     scenario = "KHU VỰC TRỐNG"
                #     def get_data(): return 0, 4
                # else:
                scenario = "TEST CỘNG DỒN (Cam 1 có, Cam 2 không)"
                # Giả lập: chỉ cam đầu tiên của mỗi area thấy người
                def get_data(is_first): return (random.randint(2, 5), 1) if is_first else (0, 4)

                logger.info(f"=== Kịch bản: {scenario} (Elapsed: {int(elapsed)}s) ===")

                # Lưu vết để biết cam nào là cam đầu tiên của mỗi area trong vòng lặp này
                processed_areas = set()

                for cam in self.camera_list:
                    is_first_in_area = cam['area_id'] not in processed_areas
                    processed_areas.add(cam['area_id'])

                    if scenario == "TEST CỘNG DỒN (Cam 1 có, Cam 2 không)":
                        p_count, light_level = get_data(is_first_in_area)
                    else:
                        p_count, light_level = get_data()

                    payload = {
                        "people": p_count,
                        "light_level": light_level
                    }

                    self.client.publish(cam["topic"], json.dumps(payload))
                    logger.info(f"Sent to {cam['topic']} [Area {cam['area_id']}]: {p_count} người, light_level={light_level}")

                # Chờ 8 giây theo yêu cầu
                time.sleep(8)
                
                if elapsed > 20: 
                    self.start_time = time.time()
                    logger.info("--- Reset chu kỳ kịch bản ---")

        except KeyboardInterrupt:
            logger.info("Simulator stopping...")
            self.client.disconnect()

if __name__ == "__main__":
    CameraSimulator().run()