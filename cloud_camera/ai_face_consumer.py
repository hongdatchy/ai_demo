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
import threading
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer
from deepface import DeepFace
from s3_helper import (
    download_crop_image,
    upload_image_bytes,
    save_ai_event_log,
    get_s3_config,
    download_face_image,
    fetch_active_faces,
)

# =====================================================================
# CẤU HÌNH KAFKA CONSUMER
# =====================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '27.71.24.102:9093')
KAFKA_USER = os.getenv('KAFKA_USER', 'admin')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD', 'Admin@123')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'ai_face_topic')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'face_recognition_group')

# Thư mục lưu cache vector đặc trưng (để khởi động nhanh, không tải lại mỗi lần)
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DEMO_AI_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..")) if os.path.basename(CURRENT_DIR) == "cloud_camera" else CURRENT_DIR
CACHE_DIR = os.path.join(DEMO_AI_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_FILE = os.path.join(CACHE_DIR, "face_embeddings_cache.pkl")

THRESHOLD = 0.30
MAX_WORKERS = int(os.getenv('AI_MAX_WORKERS', 4))
SYNC_INTERVAL = int(os.getenv('FACE_SYNC_INTERVAL', 60))  # Đồng bộ định kỳ CSDL khuôn mặt mỗi 60s

db_data = []
db_lock = threading.Lock()
embedding_cache = {}


def load_embedding_cache():
    global embedding_cache
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'rb') as f:
                embedding_cache = pickle.load(f)
            print(f"[CACHE] Đã nạp {len(embedding_cache)} vector đặc trưng từ cache: {CACHE_FILE}")
        except Exception as e:
            print(f"[CACHE] Không đọc được cache ({e}), sẽ tạo mới.")
            embedding_cache = {}
    else:
        embedding_cache = {}


def save_embedding_cache():
    try:
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(embedding_cache, f)
    except Exception as e:
        print(f"[CACHE] Lỗi lưu cache: {e}")


def load_face_database(silent=False):
    """
    Nạp danh sách khuôn mặt ACTIVE từ Java AI service qua S3:
    1. Gọi fetch_active_faces() để lấy metadata (faceId, name, employeeCode, imageKeys).
    2. Với mỗi ảnh mẫu (image_key):
       - Kiểm tra cache: nếu có vector rồi thì tái sử dụng.
       - Nếu chưa có: tải từ S3 (cloudcamera-faces), dùng DeepFace trích xuất vector và lưu cache.
    3. Cập nhật vào db_data trong RAM để phục vụ so khớp realtime.
    """
    global db_data, embedding_cache
    if not silent:
        print("\n" + "=" * 60)
        print("[FACE_DB] Đang nạp danh sách khuôn mặt từ hệ thống (S3 Mode)...")

    active_faces = fetch_active_faces()
    if not active_faces:
        if not silent:
            print("[FACE_DB] Không có khuôn mặt nào ở trạng thái ACTIVE (hoặc chưa kết nối được ai service).")
        with db_lock:
            db_data = []
        return

    new_db_data = []
    cache_updated = False

    for face in active_faces:
        face_id = face.get("faceId")
        name = face.get("name", "Unknown")
        employee_code = face.get("employeeCode")
        account_id = face.get("accountId")
        image_keys = face.get("imageKeys", [])

        for img_key in image_keys:
            if not img_key:
                continue

            emb = embedding_cache.get(img_key)
            if emb is None:
                # Tải ảnh từ S3 và trích xuất vector
                try:
                    if not silent:
                        print(f"[FACE_DB] Tải ảnh từ S3 và tính vector: faceId={face_id} name='{name}' key={img_key}")
                    img_bytes = download_face_image(img_key)
                    img_arr = np.frombuffer(img_bytes, dtype=np.uint8)
                    img = cv2.imdecode(img_arr, cv2.IMREAD_COLOR)

                    if img is not None:
                        rep = DeepFace.represent(
                            img_path=img,
                            model_name="VGG-Face",
                            detector_backend="yolov8n",
                            enforce_detection=False
                        )
                        if rep and len(rep) > 0:
                            emb = rep[0]["embedding"]
                            embedding_cache[img_key] = emb
                            cache_updated = True
                        else:
                            if not silent:
                                print(f"[FACE_DB] Không tìm thấy khuôn mặt trong ảnh: {img_key}")
                    else:
                        if not silent:
                            print(f"[FACE_DB] Không decode được ảnh: {img_key}")
                except Exception as e:
                    if not silent:
                        print(f"[FACE_DB] Lỗi xử lý ảnh S3 key={img_key}: {e}")

            if emb is not None:
                new_db_data.append({
                    "face_id": face_id,
                    "name": name,
                    "employee_code": employee_code,
                    "account_id": account_id,
                    "image_key": img_key,
                    "embedding": emb,
                })

    if cache_updated:
        save_embedding_cache()

    with db_lock:
        db_data = new_db_data

    if not silent:
        print(f"[FACE_DB] Nạp thành công {len(db_data)} vector đặc trưng của {len(active_faces)} đối tượng.")
        print("=" * 60 + "\n")


def face_db_sync_worker():
    """Thread chạy ngầm tự động đồng bộ CSDL khuôn mặt định kỳ."""
    while True:
        time.sleep(SYNC_INTERVAL)
        try:
            load_face_database(silent=True)
        except Exception as e:
            print(f"[FACE_SYNC] Lỗi đồng bộ ngầm: {e}")


def process_face_recognition(message_data):
    camera_id = message_data.get("cameraId")
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
    matched_face_id = None
    matched_entry = None

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

                with db_lock:
                    current_db = list(db_data)

                if len(current_db) > 0:
                    current_embedding = np.array(face_objs[0]["embedding"])
                    best_match = None
                    min_distance = float('inf')

                    for entry in current_db:
                        db_embedding = np.array(entry["embedding"])
                        distance = 1 - (np.dot(current_embedding, db_embedding) /
                                        (np.linalg.norm(current_embedding) * np.linalg.norm(db_embedding)))
                        if distance < min_distance:
                            min_distance = distance
                            best_match = entry

                    if best_match is not None and min_distance <= THRESHOLD:
                        last_name = best_match["name"]
                        matched_face_id = best_match["face_id"]
                        matched_entry = best_match
                        best_confidence = round(1.0 - min_distance, 4)
                        print(f"[camera={camera_id}] [KHỚP]: {last_name} (ID={matched_face_id}, Độ khớp: {best_confidence} - khoảng cách: {min_distance:.4f} <= {THRESHOLD})")
                    else:
                        last_name = "Unknown"
                        if best_match is not None:
                            print(f"[camera={camera_id}] [TRƯỢT]: Gần giống {best_match['name']} (khoảng cách: {min_distance:.4f} > {THRESHOLD})")
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
        print(f"[camera={camera_id}] Đã upload ảnh kết quả lên S3: {result_s3_key}")
    except Exception as e:
        print(f"[camera={camera_id}] Lỗi upload kết quả S3: {e}")
        result_s3_key = None

    # Chỉ lưu log khi phát hiện được khuôn mặt (kể cả Unknown)
    if last_box is not None and result_s3_key:
        metadata = {"label": last_name}
        if matched_entry and matched_entry.get("employee_code"):
            metadata["employeeCode"] = matched_entry["employee_code"]

        save_ai_event_log(
            camera_id=camera_id,
            task_type=task_type,
            image_url=result_s3_key,
            confidence=best_confidence,
            face_id=matched_face_id,
            metadata=metadata
        )


def start_consumer():
    load_embedding_cache()
    load_face_database()

    # Khởi động thread đồng bộ ngầm
    sync_thread = threading.Thread(target=face_db_sync_worker, daemon=True)
    sync_thread.start()
    print(f"[FACE_SYNC] Đã khởi động luồng đồng bộ CSDL khuôn mặt (chu kỳ {SYNC_INTERVAL}s).")

    print(f"[KAFKA CONSUMER] Đang kết nối tới {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}...")
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
        print("[KAFKA CONSUMER] Kết nối thành công! Đang chờ messages...")
    except Exception as e:
        print(f"[KAFKA CONSUMER ERROR] Không thể kết nối Kafka: {e}")
        return

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    try:
        for message in consumer:
            msg_data = message.value
            if msg_data.get("taskType") == "detect_face":
                print(f"\n[KAFKA RECEIVED - FACE] Nhận frame camera #{msg_data.get('cameraId')}: s3Key={msg_data.get('s3Key')}")
                executor.submit(process_face_recognition, msg_data)

    except KeyboardInterrupt:
        print("\n[DỪNG CONSUMER] Đang tắt hệ thống...")
    finally:
        consumer.close()
        executor.shutdown(wait=True)
        print("[ĐÃ DỪNG] Consumer đã ngắt kết nối.")


if __name__ == "__main__":
    print("=" * 60)
    print("AI CONSUMER: NHẬN DIỆN KHUÔN MẶT - S3 DATABASE MODE")
    print("=" * 60)
    start_consumer()
