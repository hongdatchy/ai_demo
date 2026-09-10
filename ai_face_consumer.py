import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

import os
import cv2
import json
import time
import pickle
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer
from deepface import DeepFace

# =====================================================================
# CẤU HÌNH KAFKA CONSUMER (LOCAL PROFILE)
# =====================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '27.71.24.102:9093')
KAFKA_USER = os.getenv('KAFKA_USER', 'admin')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD', 'Admin@123')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'ai_frame_topic')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'face_recognition_group')

# ĐƯA RA NGOÀI ĐỒNG CẤP VỚI temp_frames (D:\ViettelCloudCamera\demo_ai)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DEMO_AI_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..")) if os.path.basename(CURRENT_DIR) == "cloud_camera" else CURRENT_DIR

DB_PATH = os.path.join(DEMO_AI_DIR, "db_faces")
PROCESSED_DIR = os.path.join(DEMO_AI_DIR, "processed_faces")
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(DB_PATH, exist_ok=True)

THRESHOLD = 0.30  
MAX_WORKERS = int(os.getenv('AI_MAX_WORKERS', 4))

db_data = []

def load_face_database():
    global db_data
    if os.path.exists(DB_PATH):
        for file in os.listdir(DB_PATH):
            if file.endswith(".pkl"):
                try:
                    os.remove(os.path.join(DB_PATH, file))
                    print(f"Da lam moi CSDL khuon mat (Xoa cache cu: {file})")
                except Exception:
                    pass

    print("Dang quet CSDL anh va trich xuat dac trung khuon mat bang yolov8n + VGG-Face...")
    dummy_path = os.path.join(DEMO_AI_DIR, "dummy_init.jpg")
    dummy_frame = np.zeros((100, 100, 3), dtype=np.uint8)
    cv2.imwrite(dummy_path, dummy_frame)
    try:
        DeepFace.find(
            img_path=dummy_path, 
            db_path=DB_PATH, 
            model_name="VGG-Face", 
            detector_backend="yolov8n", 
            enforce_detection=False, 
            silent=True
        )
    except Exception as e:
        print(f"Loi khi quet CSDL: {e}")
    finally:
        if os.path.exists(dummy_path):
            os.remove(dummy_path)

    pkl_file_path = None
    if os.path.exists(DB_PATH):
        for file in os.listdir(DB_PATH):
            if file.endswith(".pkl"):
                pkl_file_path = os.path.join(DB_PATH, file)
                break

    if pkl_file_path and os.path.exists(pkl_file_path):
        with open(pkl_file_path, 'rb') as f:
            db_data = pickle.load(f)
        print(f"Da nap thanh cong du lieu vector cua {len(db_data)} anh khuon mat tu CSDL: {DB_PATH}")
    else:
        print("Canh bao: Khong the nap CSDL khuon mat (.pkl). Vui long kiem tra lai thu muc db_faces.")


def process_face_recognition(message_data):
    cloud_id = message_data.get("cloudId")
    task_type = message_data.get("taskType")
    image_path = message_data.get("path")

    if task_type != "detect_face":
        return

    if not image_path or not os.path.exists(image_path):
        print(f"[{cloud_id}] Khong tim thay file anh: {image_path}")
        return

    frame = cv2.imread(image_path)
    if frame is None:
        return

    last_name = "Unknown"
    last_box = None

    try:
        face_objs = DeepFace.represent(
            img_path=frame,
            model_name="VGG-Face",
            detector_backend="yolov8n",
            enforce_detection=False
        )

        if len(face_objs) > 0:
            x = face_objs[0]["facial_area"]["x"]
            y = face_objs[0]["facial_area"]["y"]
            w = face_objs[0]["facial_area"]["w"]
            h = face_objs[0]["facial_area"]["h"]

            frame_h, frame_w = frame.shape[:2]
            if w >= frame_w - 10 and h >= frame_h - 10:
                last_box = None
                last_name = "Unknown"
            else:
                last_box = (x, y, w, h)

                if len(db_data) > 0:
                    current_embedding = np.array(face_objs[0]["embedding"])
                    best_match = None
                    min_distance = float('inf')

                    for entry in db_data:
                        db_embedding = np.array(entry["embedding"])
                        distance = 1 - (np.dot(current_embedding, db_embedding) / 
                                        (np.linalg.norm(current_embedding) * np.linalg.norm(db_embedding)))
                        if distance < min_distance:
                            min_distance = distance
                            best_match = entry

                    if best_match is not None and min_distance <= THRESHOLD:
                        best_match_path = best_match["identity"]
                        folder_name = os.path.dirname(best_match_path)
                        last_name = os.path.basename(folder_name)
                        print(f"[{cloud_id}] [KHOP]: {last_name} (Do duoc: {min_distance:.4f} <= {THRESHOLD})")
                    else:
                        last_name = "Unknown"
                        if best_match is not None:
                            folder_name = os.path.dirname(best_match["identity"])
                            name_guess = os.path.basename(folder_name)
                            print(f"[{cloud_id}] [TRUOT]: Gan giong {name_guess} (Do duoc: {min_distance:.4f} > {THRESHOLD})")
                else:
                    last_name = "Unknown"
        else:
            print(f"[{cloud_id}] [DO TIM]: Khong tim thay khuon mat nao...")
            last_box = None
            last_name = "Unknown"

    except Exception as e:
        print(f"[{cloud_id}] [LOI HE THONG]: {e}")
        last_box = None
        last_name = "Unknown"

    if last_box is not None:
        x, y, w, h = last_box
        color = (0, 255, 0) if last_name != "Unknown" else (0, 0, 255)
        cv2.rectangle(frame, (x, y), (x + w, y + h), color, 2)
        cv2.putText(frame, last_name, (x, y - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.8, color, 2)

    cv2.putText(frame, f"Identity: {last_name}", (30, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 0, 0), 2)

    # Lưu vào processed_faces (ĐỒNG CẤP VỚI temp_frames)
    base_name = os.path.basename(image_path)
    output_path = os.path.join(PROCESSED_DIR, f"result_{base_name}")
    cv2.imwrite(output_path, frame)
    print(f"[{cloud_id}] Da luu anh ket qua vao: {output_path}")


def start_consumer():
    load_face_database()

    print(f"[KAFKA CONSUMER] Dang ket noi toi {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=KAFKA_USER,
            sasl_plain_password=KAFKA_PASSWORD,
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print("[KAFKA CONSUMER] Ket noi thanh cong! Dang cho messages...")
    except Exception as e:
        print(f"[KAFKA CONSUMER ERROR] Khong the ket noi Kafka: {e}")
        return

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    try:
        for message in consumer:
            msg_data = message.value
            print(f"\n[KAFKA RECEIVED] Nhan frame tu {msg_data.get('cloudId')}: {msg_data.get('path')}")
            executor.submit(process_face_recognition, msg_data)

    except KeyboardInterrupt:
        print("\n[DUNG CONSUMER] Dang tat he thong...")
    finally:
        consumer.close()
        executor.shutdown(wait=True)
        print("[DA DUNG] Consumer da ngat ket noi.")


if __name__ == "__main__":
    print("=" * 60)
    print("AI CONSUMER: NHAN DIEN KHUON MAT CHUAN DETECT_FACE_REALTIME")
    print(f"Thu muc luu ket qua: {PROCESSED_DIR}")
    print("=" * 60)
    start_consumer()
