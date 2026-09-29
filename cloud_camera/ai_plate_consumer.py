import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

import os
import cv2
import json
import time
import re
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
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'ai_plate_topic')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'plate_detection_group')

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DEMO_AI_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..")) if os.path.basename(CURRENT_DIR) == "cloud_camera" else CURRENT_DIR

MODEL_PATH = os.path.join(DEMO_AI_DIR, "license_plate_detector.pt")
MAX_WORKERS = int(os.getenv('PLATE_MAX_WORKERS', 4))

plate_detector = None
ocr_engine = None


def load_plate_detector_model():
    global MODEL_PATH
    if not os.path.exists(MODEL_PATH):
        print(f"[MÔ HÌNH BIỂN SỐ] Chưa có file {MODEL_PATH} cục bộ.")
        print("[MÔ HÌNH BIỂN SỐ] Đang tự động tải mô hình YOLO License Plate Detector từ Hugging Face (~6MB)...")
        url = "https://huggingface.co/Koushim/yolov8-license-plate-detection/resolve/main/best.pt"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req) as resp, open(MODEL_PATH, "wb") as f:
                f.write(resp.read())
            print("[MÔ HÌNH BIỂN SỐ] Tải mô hình thành công!")
        except Exception as e:
            print(f"[CẢNH BÁO] Không thể tải từ internet ({e}). Sử dụng tạm yolo11n.pt...")
            MODEL_PATH = os.path.join(DEMO_AI_DIR, "yolo11n.pt")

    print(f"[MÔ HÌNH BIỂN SỐ] Đang nạp mô hình YOLO từ: {MODEL_PATH}")
    model = YOLO(MODEL_PATH)
    print("[MÔ HÌNH BIỂN SỐ] Nạp mô hình YOLO thành công!")
    return model


def load_ocr_engine_model():
    try:
        from paddleocr import PaddleOCR
        print("[PaddleOCR] Đang khởi tạo bộ đọc OCR...")
        try:
            # Chuẩn PaddleOCR 3.x
            engine = PaddleOCR(
                lang='en',
                use_textline_orientation=True,
                use_doc_orientation_classify=False,
                use_doc_unwarping=False,
            )
            print("[PaddleOCR] Khởi tạo PaddleOCR 3.x thành công!")
            return engine
        except TypeError:
            # Fallback tương thích PaddleOCR 2.x
            engine = PaddleOCR(use_angle_cls=True, lang='en', show_log=False)
            print("[PaddleOCR] Khởi tạo PaddleOCR 2.x fallback thành công!")
            return engine
    except Exception as e:
        print(f"[CẢNH BÁO] Không khởi tạo được PaddleOCR ({e}). Tiến trình sẽ chỉ phát hiện vị trí biển số.")
        return None


def clean_license_plate_text(raw_text):
    """Làm sạch chuỗi OCR: Giữ lại chữ cái hoa, chữ số, dấu gạch nối và dấu chấm cho biển số VN."""
    if not raw_text:
        return ""
    text = raw_text.upper().strip()
    cleaned = re.sub(r'[^A-Z0-9\-\.]', ' ', text)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def extract_plate_text(crop_img):
    """Thực hiện OCR nhận diện văn bản trên vùng cắt biển số xe."""
    if ocr_engine is None or crop_img is None or crop_img.size == 0:
        return "", 0.0

    raw_texts = []
    scores = []
    try:
        # Dùng predict() - API PaddleOCR 3.x
        ocr_result = ocr_engine.predict(crop_img)
        for res in ocr_result:
            texts = res.get("rec_texts", [])
            scs = res.get("rec_scores", [])
            for text, score in zip(texts, scs):
                if score > 0.4:
                    raw_texts.append(text)
                    scores.append(float(score))
    except Exception:
        try:
            # Fallback PaddleOCR 2.x
            res = ocr_engine.ocr(crop_img)
            if res and res[0]:
                for line in res[0]:
                    raw_texts.append(line[1][0])
                    scores.append(float(line[1][1]))
        except Exception as ex2:
            print(f"[OCR LỖI] {ex2}")

    combined = " - ".join(raw_texts) if len(raw_texts) > 1 else "".join(raw_texts)
    plate_text = clean_license_plate_text(combined)
    avg_score = float(np.mean(scores)) if scores else 0.0
    return plate_text, avg_score


def process_plate_detection(message_data):
    camera_id = message_data.get("cameraId")
    task_type = message_data.get("taskType")
    s3_key = message_data.get("s3Key")

    if task_type != "detect_plate":
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

    h_img, w_img = frame.shape[:2]
    detected_plates = []
    max_confidence = 0.0

    try:
        results = plate_detector(frame, conf=0.30, verbose=False)
        if results and len(results) > 0 and results[0].boxes is not None:
            for box in results[0].boxes:
                x1, y1, x2, y2 = map(int, box.xyxy[0])
                conf = float(box.conf[0])

                # Padding nhẹ vùng biển số để không cắt sát ký tự
                pad_w = int((x2 - x1) * 0.05)
                pad_h = int((y2 - y1) * 0.05)
                crop_x1 = max(0, x1 - pad_w)
                crop_y1 = max(0, y1 - pad_h)
                crop_x2 = min(w_img, x2 + pad_w)
                crop_y2 = min(h_img, y2 + pad_h)

                crop_plate = frame[crop_y1:crop_y2, crop_x1:crop_x2]
                plate_text, ocr_conf = extract_plate_text(crop_plate)

                detected_plates.append({
                    "plateText": plate_text,
                    "bbox": [x1, y1, x2, y2],
                    "confidence": round(conf, 4),
                    "ocrConfidence": round(ocr_conf, 4)
                })
                max_confidence = max(max_confidence, conf)

                # Vẽ Bounding Box và Nhãn biển số lên frame
                color = (0, 255, 0)
                cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)

                label_str = f"{plate_text} ({conf:.2f})" if plate_text else f"PLATE ({conf:.2f})"
                (txt_w, txt_h), _ = cv2.getTextSize(label_str, cv2.FONT_HERSHEY_SIMPLEX, 0.7, 2)
                cv2.rectangle(frame, (x1, y1 - txt_h - 10), (x1 + txt_w + 10, y1), (0, 0, 0), -1)
                cv2.putText(frame, label_str, (x1 + 5, y1 - 5),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 2)

                print(f"[camera={camera_id}] PHÁT HIỆN BIỂN SỐ: '{plate_text}' | Conf: {conf:.2f} | BBox: [{x1},{y1},{x2},{y2}]")

    except Exception as e:
        print(f"[camera={camera_id}] Lỗi xử lý nhận diện biển số: {e}")
        return

    # Chỉ upload kết quả và bắn event khi có biển số được phát hiện
    if detected_plates:
        timestamp = int(time.time() * 1000)
        result_s3_key = f"detect_plate/{camera_id}/{timestamp}.jpg"
        try:
            _, buf = cv2.imencode(".jpg", frame)
            upload_image_bytes(buf.tobytes(), result_s3_key)
            print(f"[camera={camera_id}] Đã upload ảnh kết quả biển số lên S3: {result_s3_key}")
        except Exception as e:
            print(f"[camera={camera_id}] Lỗi upload kết quả S3: {e}")
            result_s3_key = None

        if result_s3_key:
            # Lấy chuỗi biển số đầu tiên hoặc ghép các biển số làm đại diện
            primary_text = detected_plates[0]["plateText"] if detected_plates else ""
            save_ai_event_log(
                camera_id=camera_id,
                task_type=task_type,
                image_url=result_s3_key,
                confidence=round(max_confidence, 4),
                face_id=None,
                metadata={
                    "plateText": primary_text,
                    "plates": detected_plates
                }
            )


def start_consumer():
    global plate_detector, ocr_engine
    plate_detector = load_plate_detector_model()
    ocr_engine = load_ocr_engine_model()

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
        print("[KAFKA CONSUMER] Kết nối thành công! Đang chờ frame để nhận diện biển số...")
    except Exception as e:
        print(f"[KAFKA CONSUMER ERROR] Không thể kết nối Kafka: {e}")
        return

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    try:
        for message in consumer:
            msg_data = message.value
            if msg_data.get("taskType") == "detect_plate":
                print(f"\n[KAFKA RECEIVED - PLATE] Nhận frame camera #{msg_data.get('cameraId')}: s3Key={msg_data.get('s3Key')}")
                executor.submit(process_plate_detection, msg_data)

    except KeyboardInterrupt:
        print("\n[DỪNG CONSUMER] Đang tắt hệ thống nhận diện biển số...")
    finally:
        consumer.close()
        executor.shutdown(wait=True)
        print("[ĐÃ DỪNG] Consumer nhận diện biển số đã ngắt kết nối.")


if __name__ == "__main__":
    print("=" * 60)
    print("AI CONSUMER: NHẬN DIỆN BIỂN SỐ XE (YOLO + PADDLE OCR) - S3 MODE")
    print("=" * 60)
    start_consumer()
