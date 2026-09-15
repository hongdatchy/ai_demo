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
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer
from ultralytics import YOLO

# =====================================================================
# CẤU HÌNH KAFKA CONSUMER (LOCAL PROFILE)
# =====================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '27.71.24.102:9093')
KAFKA_USER = os.getenv('KAFKA_USER', 'admin')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD', 'Admin@123')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'ai_fire_topic')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'fire_detection_group')

# ĐƯỜNG DẪN THƯ MỤC
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DEMO_AI_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..")) if os.path.basename(CURRENT_DIR) == "cloud_camera" else CURRENT_DIR

PROCESSED_FIRE_DIR = os.path.join(DEMO_AI_DIR, "processed_fire")
os.makedirs(PROCESSED_FIRE_DIR, exist_ok=True)

MODEL_PATH = os.path.join(DEMO_AI_DIR, "fire_detect.pt")
MAX_WORKERS = int(os.getenv('FIRE_MAX_WORKERS', 4))

# =====================================================================
# TẢI VÀ KHỞI TẠO MÔ HÌNH NHẬN DIỆN CHÁY/KHÓI (CHUẨN detect_fire.py)
# =====================================================================
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

# =====================================================================
# HÀM XỬ LÝ PHÁT HIỆN CHÁY TRÊN 1 FRAME (ĐA LUỒNG)
# =====================================================================
def process_fire_detection(message_data):
    cloud_id = message_data.get("cloudId")
    task_type = message_data.get("taskType")
    image_path = message_data.get("path")

    # Chỉ xử lý nếu message yêu cầu đúng bài toán detect_fire
    if task_type != "detect_fire":
        return

    if not image_path or not os.path.exists(image_path):
        print(f"[{cloud_id}] Không tìm thấy file ảnh: {image_path}")
        return

    frame = cv2.imread(image_path)
    if frame is None:
        return

    try:
        # Chạy dự đoán bằng YOLO
        results = model(frame, verbose=False)[0]
        fire_detected = False

        for box in results.boxes:
            class_id = int(box.cls[0])
            confidence = float(box.conf[0])
            label = results.names[class_id]

            # Lọc nhãn cháy / khói (fire / smoke)
            if label.lower() in ["fire", "smoke"] or "yolov8n.pt" in MODEL_PATH:
                x1, y1, x2, y2 = map(int, box.xyxy[0])
                if confidence >= 0.25:
                    fire_detected = True
                    color = (0, 0, 255)  # Màu đỏ cảnh báo
                    tag = f"{label.upper()} {confidence:.2f}"
                    cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
                    cv2.putText(frame, tag, (x1, y1 - 8), cv2.FONT_HERSHEY_SIMPLEX, 0.6, color, 2)
                    print(f"[{cloud_id}] CẢNH BÁO PHÁT HIỆN: {tag} tại [{x1},{y1},{x2},{y2}]")
                else:
                    color = (0, 165, 255)  # Màu cam (độ tin cậy thấp < 0.25)
                    tag = f"{label.upper()} {confidence:.2f} (Low)"
                    cv2.rectangle(frame, (x1, y1), (x2, y2), color, 1)
                    cv2.putText(frame, tag, (x1, y1 - 8), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 1)

        # Hiển thị trạng thái lên khung hình
        if fire_detected:
            cv2.rectangle(frame, (20, 20), (450, 70), (0, 0, 255), -1)
            cv2.putText(frame, "WARNING: FIRE DETECTED!", (30, 55), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (255, 255, 255), 2)
        else:
            cv2.rectangle(frame, (20, 20), (380, 65), (0, 160, 0), -1)
            cv2.putText(frame, "FIRE STATUS: SAFE", (30, 52), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 255, 255), 2)
            
        # Luôn lưu ảnh kết quả vào thư mục processed_fire
        base_name = os.path.basename(image_path)
        output_path = os.path.join(PROCESSED_FIRE_DIR, f"fire_{base_name}")
        cv2.imwrite(output_path, frame)
        print(f"[{cloud_id}] Đã lưu ảnh kết quả vào: {output_path} (Cháy: {fire_detected})")

    except Exception as e:
        print(f"[{cloud_id}] Lỗi xử lý nhận diện cháy: {e}")


# =====================================================================
# KAFKA CONSUMER + ThreadPoolExecutor ĐA LUỒNG
# =====================================================================
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

    # Xử lý nhận diện cháy đa luồng
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    try:
        for message in consumer:
            msg_data = message.value
            # Kiểm tra nhanh taskType trước khi submit vào thread để tối ưu tài nguyên
            if msg_data.get("taskType") == "detect_fire":
                print(f"\n[KAFKA RECEIVED - FIRE] Nhận frame từ {msg_data.get('cloudId')}: {msg_data.get('path')}")
                executor.submit(process_fire_detection, msg_data)

    except KeyboardInterrupt:
        print("\n[DỪNG CONSUMER] Đang tắt hệ thống...")
    finally:
        consumer.close()
        executor.shutdown(wait=True)
        print("[ĐÃ DỪNG] Consumer phát hiện cháy đã ngắt kết nối.")


if __name__ == "__main__":
    print("=" * 60)
    print("AI CONSUMER: PHÁT HIỆN CHÁY/KHÓI ĐA LUỒNG QUA KAFKA (YOLO)")
    print(f"Thư mục lưu ảnh cháy phát hiện: {PROCESSED_FIRE_DIR}")
    print("=" * 60)
    start_consumer()
