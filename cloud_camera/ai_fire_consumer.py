import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

import os
import cv2
import json
import time
import urllib.request
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer
from ultralytics import YOLO
from s3_helper import download_crop_image, upload_image_bytes, save_ai_event_log

# =====================================================================
# CẤU HÌNH KAFKA CONSUMER
# =====================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '27.71.24.102:9093')
KAFKA_USER = os.getenv('KAFKA_USER', 'admin')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD', 'Admin@123')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'ai_fire_topic')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'fire_detection_group')

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DEMO_AI_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..")) if os.path.basename(CURRENT_DIR) == "cloud_camera" else CURRENT_DIR

MODEL_PATH = os.path.join(DEMO_AI_DIR, "fire_detect.pt")
MAX_WORKERS = int(os.getenv('FIRE_MAX_WORKERS', 4))


def load_fire_model():
    global MODEL_PATH
    if not os.path.exists(MODEL_PATH):
        print("[MÔ HÌNH CHÁY] Chưa có file fire_detect.pt cục bộ.")
        print("[MÔ HÌNH CHÁY] Đang tự động tải mô hình YOLOv8 Fire/Smoke từ Hugging Face (~6MB)...")
        url = "https://huggingface.co/rabahdev/fire-smoke-yolov8n/resolve/main/best.pt"
        try:
            urllib.request.urlretrieve(url, MODEL_PATH)
            print("[MÔ HÌNH CHÁY] Tải mô hình thành công!")
        except Exception as e:
            print(f"[CẢNH BÁO] Không thể tải từ internet ({e}). Sử dụng tạm yolov8n.pt...")
            MODEL_PATH = os.path.join(DEMO_AI_DIR, "yolov8n.pt")

    print(f"[MÔ HÌNH CHÁY] Đang nạp mô hình YOLO từ: {MODEL_PATH}")
    model = YOLO(MODEL_PATH)
    print("[MÔ HÌNH CHÁY] Nạp mô hình thành công!")
    return model


model = None


def process_fire_detection(message_data):
    camera_id = message_data.get("cameraId")
    task_type = message_data.get("taskType")
    s3_key = message_data.get("s3Key")

    if task_type != "detect_fire":
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

    fire_detected = False
    max_confidence = 0.0

    try:
        results = model(frame, verbose=False)[0]

        for box in results.boxes:
            class_id = int(box.cls[0])
            confidence = float(box.conf[0])
            label = results.names[class_id]

            if label.lower() in ["fire", "smoke"] or "yolov8n.pt" in MODEL_PATH:
                x1, y1, x2, y2 = map(int, box.xyxy[0])
                if confidence >= 0.25:
                    fire_detected = True
                    max_confidence = max(max_confidence, confidence)
                    color = (0, 0, 255)
                    tag = f"{label.upper()} {confidence:.2f}"
                    cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
                    cv2.putText(frame, tag, (x1, y1 - 8), cv2.FONT_HERSHEY_SIMPLEX, 0.6, color, 2)
                    print(f"[camera={camera_id}] CẢNH BÁO PHÁT HIỆN: {tag} tại [{x1},{y1},{x2},{y2}]")
                else:
                    color = (0, 165, 255)
                    tag = f"{label.upper()} {confidence:.2f} (Low)"
                    cv2.rectangle(frame, (x1, y1), (x2, y2), color, 1)
                    cv2.putText(frame, tag, (x1, y1 - 8), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 1)

        if fire_detected:
            cv2.rectangle(frame, (20, 20), (450, 70), (0, 0, 255), -1)
            cv2.putText(frame, "WARNING: FIRE DETECTED!", (30, 55), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (255, 255, 255), 2)
        else:
            cv2.rectangle(frame, (20, 20), (380, 65), (0, 160, 0), -1)
            cv2.putText(frame, "FIRE STATUS: SAFE", (30, 52), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 255, 255), 2)

    except Exception as e:
        print(f"[camera={camera_id}] Lỗi xử lý nhận diện cháy: {e}")
        return

    # Upload ảnh kết quả lên S3 bucket cloudcamera-result: detect_fire/{cameraId}/{timestamp}.jpg
    timestamp = int(time.time() * 1000)
    result_s3_key = f"detect_fire/{camera_id}/{timestamp}.jpg"
    try:
        _, buf = cv2.imencode(".jpg", frame)
        upload_image_bytes(buf.tobytes(), result_s3_key)
        print(f"[camera={camera_id}] Da upload anh ket qua len S3: {result_s3_key} (Chay: {fire_detected})")
    except Exception as e:
        print(f"[camera={camera_id}] Loi upload ket qua S3: {e}")
        result_s3_key = None

    # Chỉ lưu log khi phát hiện cháy
    if fire_detected and result_s3_key:
        save_ai_event_log(
            camera_id=camera_id,
            task_type=task_type,
            image_url=result_s3_key,
            confidence=round(max_confidence, 4),
            face_id=None,
            metadata={"label": "fire_detected"}
        )


def start_consumer():
    global model
    model = load_fire_model()

    print(f"[KAFKA CONSUMER] Đang kết nối tới {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC} (Group: {KAFKA_GROUP_ID})...")
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
        print("[KAFKA CONSUMER] Kết nối thành công! Đang chờ frame để phát hiện cháy...")
    except Exception as e:
        print(f"[KAFKA CONSUMER ERROR] Không thể kết nối Kafka: {e}")
        return

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    try:
        for message in consumer:
            msg_data = message.value
            if msg_data.get("taskType") == "detect_fire":
                print(f"\n[KAFKA RECEIVED - FIRE] Nhận frame camera #{msg_data.get('cameraId')}: s3Key={msg_data.get('s3Key')}")
                executor.submit(process_fire_detection, msg_data)

    except KeyboardInterrupt:
        print("\n[DỪNG CONSUMER] Đang tắt hệ thống...")
    finally:
        consumer.close()
        executor.shutdown(wait=True)
        print("[ĐÃ DỪNG] Consumer phát hiện cháy đã ngắt kết nối.")


if __name__ == "__main__":
    print("=" * 60)
    print("AI CONSUMER: PHÁT HIỆN CHÁY/KHÓI - S3 MODE")
    print("=" * 60)
    start_consumer()
