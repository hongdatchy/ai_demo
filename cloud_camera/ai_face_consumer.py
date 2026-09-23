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
from s3_helper import download_crop_image, upload_image_bytes, save_ai_event_log, get_s3_config

# =====================================================================
# CẤU HÌNH KAFKA CONSUMER
# =====================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '27.71.24.102:9093')
KAFKA_USER = os.getenv('KAFKA_USER', 'admin')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD', 'Admin@123')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'ai_face_topic')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'face_recognition_group')

# DB khuôn mặt vẫn lưu local (ảnh đăng ký face ID, không phải ảnh camera)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DEMO_AI_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..")) if os.path.basename(CURRENT_DIR) == "cloud_camera" else CURRENT_DIR
DB_PATH = os.path.join(DEMO_AI_DIR, "db_faces")
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
    camera_id = message_data.get("cameraId")  # int cameraId từ DB
    task_type = message_data.get("taskType")
    s3_key = message_data.get("s3Key")

    if task_type != "detect_face":
        return

    if not s3_key or camera_id is None:
        print(f"[camera={camera_id}] Thiếu s3Key hoặc cameraId trong message")
        return

    # Download frame từ S3 bucket cloudcamera-crop
    try:
        image_bytes = download_crop_image(s3_key)
    except Exception as e:
        print(f"[camera={camera_id}] Lỗi download S3 key={s3_key}: {e}")
        return

    frame_array = np.frombuffer(image_bytes, dtype=np.uint8)
    frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
    if frame is None:
        print(f"[camera={camera_id}] Không decode được ảnh từ S3")
        return

    last_name = "Unknown"
    last_box = None
    best_confidence = None
    matched_face_id = None  # TODO: map tên → faceId khi có DB liên kết

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
                        best_confidence = round(1.0 - min_distance, 4)
                        print(f"[camera={camera_id}] [KHỚP]: {last_name} (Độ khớp: {best_confidence} - khoảng cách: {min_distance:.4f} <= {THRESHOLD})")
                    else:
                        last_name = "Unknown"
                        if best_match is not None:
                            folder_name = os.path.dirname(best_match["identity"])
                            name_guess = os.path.basename(folder_name)
                            print(f"[camera={camera_id}] [TRƯỢT]: Gần giống {name_guess} (khoảng cách: {min_distance:.4f} > {THRESHOLD})")
                else:
                    last_name = "Unknown"
        else:
            print(f"[camera={camera_id}] [DÒ TÌM]: Không tìm thấy khuôn mặt nào...")

    except Exception as e:
        print(f"[camera={camera_id}] [LỖI HỆ THỐNG]: {e}")

    # Vẽ kết quả lên frame
    if last_box is not None:
        x, y, w, h = last_box
        color = (0, 255, 0) if last_name != "Unknown" else (0, 0, 255)
        cv2.rectangle(frame, (x, y), (x + w, y + h), color, 2)
        cv2.putText(frame, last_name, (x, y - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.8, color, 2)

    cv2.putText(frame, f"Identity: {last_name}", (30, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 0, 0), 2)

    # Upload ảnh kết quả lên S3 bucket cloudcamera-result: detect_face/{cameraId}/{timestamp}.jpg
    timestamp = int(time.time() * 1000)
    result_s3_key = f"detect_face/{camera_id}/{timestamp}.jpg"
    try:
        _, buf = cv2.imencode(".jpg", frame)
        upload_image_bytes(buf.tobytes(), result_s3_key)
        print(f"[camera={camera_id}] Da upload anh ket qua len S3: {result_s3_key}")
    except Exception as e:
        print(f"[camera={camera_id}] Loi upload ket qua S3: {e}")
        result_s3_key = None

    # Chỉ lưu log khi phát hiện được khuôn mặt (kể cả Unknown)
    if last_box is not None and result_s3_key:
        save_ai_event_log(
            camera_id=camera_id,
            task_type=task_type,
            image_url=result_s3_key,
            confidence=best_confidence,
            face_id=matched_face_id,
            metadata={"label": last_name}
        )


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
            if msg_data.get("taskType") == "detect_face":
                print(f"\n[KAFKA RECEIVED - FACE] Nhận frame camera #{msg_data.get('cameraId')}: s3Key={msg_data.get('s3Key')}")
                executor.submit(process_face_recognition, msg_data)

    except KeyboardInterrupt:
        print("\n[DUNG CONSUMER] Dang tat he thong...")
    finally:
        consumer.close()
        executor.shutdown(wait=True)
        print("[DA DUNG] Consumer da ngat ket noi.")


if __name__ == "__main__":
    print("=" * 60)
    print("AI CONSUMER: NHAN DIEN KHUON MAT - S3 MODE")
    print("=" * 60)
    start_consumer()
