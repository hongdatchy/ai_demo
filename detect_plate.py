import os
import sys
import time
import re
import cv2
import requests
import numpy as np
from ultralytics import YOLO

# Đảm bảo in tiếng Việt không bị lỗi font console Windows
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# Import PaddleOCR
try:
    from paddleocr import PaddleOCR
except ImportError:
    PaddleOCR = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_IMAGE_PATH = os.path.join(BASE_DIR, "bienxemay1.jpg")
OUTPUT_IMAGE_PATH = os.path.join(BASE_DIR, "output_detect_plate.jpg")
MODEL_PATH = os.path.join(BASE_DIR, "license_plate_detector.pt")

print("==========================================================")
print("   NHẬN DIỆN BIỂN SỐ XE (YOLO + PADDLE OCR 3.x)")
print("==========================================================")

# 1. Tải và Khởi tạo Mô hình YOLO chuyên dụng cho Biển số xe (License Plate Detector)
if not os.path.exists(MODEL_PATH):
    print("[DOWNLOAD] Đang tải mô hình YOLO License Plate Detector từ HuggingFace...")
    url = "https://huggingface.co/Koushim/yolov8-license-plate-detection/resolve/main/best.pt"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    with open(MODEL_PATH, "wb") as f:
        f.write(r.content)
    print(f"[DOWNLOAD] Đã tải xong -> {MODEL_PATH} ({os.path.getsize(MODEL_PATH)} bytes)")

print(f"[YOLO] Đang nạp mô hình phát hiện biển số từ: {MODEL_PATH} ...")
plate_detector = YOLO(MODEL_PATH)

# 2. Khởi tạo Mô hình PaddleOCR (Chuẩn API 3.x)
ocr_engine = None
if PaddleOCR is not None:
    print("[PaddleOCR] Đang khởi tạo bộ đọc OCR 3.x...")
    ocr_engine = PaddleOCR(
        lang='en',
        use_textline_orientation=True,
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
    )
else:
    print("[CẢNH BÁO] Thư viện 'paddleocr' chưa được cài đặt!")


def clean_license_plate_text(raw_text):
    """
    Làm sạch chuỗi OCR: Giữ lại chữ cái hoa, chữ số, dấu gạch nối và dấu chấm cho biển số VN
    """
    if not raw_text:
        return ""
    text = raw_text.upper().strip()
    cleaned = re.sub(r'[^A-Z0-9\-\.]', ' ', text)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def process_detect_plate(image_path):
    if not os.path.exists(image_path):
        print(f"[LỖI] Không tìm thấy ảnh test: {image_path}")
        return

    image = cv2.imread(image_path)
    if image is None:
        print(f"[LỖI] Không đọc được ảnh: {image_path}")
        return

    h_img, w_img = image.shape[:2]
    print(f"\n[XỬ LÝ] Đang đọc ảnh: {image_path} ({w_img}x{h_img} px)")

    t0 = time.time()

    # BƯỚC 1: YOLO Phát hiện chính xác vị trí Biển số xe (License Plate Bounding Box)
    results = plate_detector(image, conf=0.35, verbose=False)
    t_yolo = time.time()

    boxes = []
    if results and len(results) > 0 and results[0].boxes is not None:
        for box in results[0].boxes:
            x1, y1, x2, y2 = map(int, box.xyxy[0])
            conf = float(box.conf[0])
            cls_id = int(box.cls[0])
            cls_name = plate_detector.names.get(cls_id, "license_plate")
            boxes.append((x1, y1, x2, y2, conf, cls_name))

    print(f"[YOLO] Tìm thấy {len(boxes)} biển số xe (Thời gian: {(t_yolo - t0)*1000:.1f}ms)")

    output_img = image.copy()

    # BƯỚC 2: Crop từng vùng biển số và đưa vào PaddleOCR đọc chữ
    for idx, (x1, y1, x2, y2, conf, cls_name) in enumerate(boxes):
        # Padding nhẹ để lấy trọn vẹn viền biển số
        pad_w = int((x2 - x1) * 0.05)
        pad_h = int((y2 - y1) * 0.05)
        crop_x1 = max(0, x1 - pad_w)
        crop_y1 = max(0, y1 - pad_h)
        crop_x2 = min(w_img, x2 + pad_w)
        crop_y2 = min(h_img, y2 + pad_h)

        crop_plate = image[crop_y1:crop_y2, crop_x1:crop_x2]
        if crop_plate.size == 0:
            continue

        plate_number = "UNKNOWN"

        if ocr_engine is not None:
            t_ocr_start = time.time()
            ocr_result = ocr_engine.predict(crop_plate)
            t_ocr_end = time.time()

            raw_texts = []
            for res in ocr_result:
                texts = res.get("rec_texts", [])
                scores = res.get("rec_scores", [])
                for text, score in zip(texts, scores):
                    if score > 0.4:
                        raw_texts.append(text)

            combined_text = " - ".join(raw_texts) if len(raw_texts) > 1 else "".join(raw_texts)
            plate_number = clean_license_plate_text(combined_text)
            print(f" -> Biển #{idx+1}: Conf={conf:.2f} | OCR Đọc được: '{plate_number}' (Thời gian OCR: {(t_ocr_end - t_ocr_start)*1000:.1f}ms)")

        # BƯỚC 3: Vẽ Bounding Box & Ghi biển số lên ảnh
        cv2.rectangle(output_img, (x1, y1), (x2, y2), (0, 255, 0), 3)

        label_str = f"{plate_number} ({conf:.2f})"
        font_scale = 0.8
        thickness = 2
        (txt_w, txt_h), baseline = cv2.getTextSize(label_str, cv2.FONT_HERSHEY_SIMPLEX, font_scale, thickness)

        cv2.rectangle(output_img, (x1, y1 - txt_h - 10), (x1 + txt_w + 10, y1), (0, 0, 0), -1)
        cv2.putText(output_img, label_str, (x1 + 5, y1 - 5),
                    cv2.FONT_HERSHEY_SIMPLEX, font_scale, (0, 255, 255), thickness)

    total_time = (time.time() - t0) * 1000
    print(f"\n[TỔNG CỘNG] Xử lý hoàn tất trong: {total_time:.1f}ms")

    # Lưu ảnh kết quả
    cv2.imwrite(OUTPUT_IMAGE_PATH, output_img)
    print(f"[XUẤT ẢNH] Đã lưu ảnh kết quả tại: {OUTPUT_IMAGE_PATH}")


if __name__ == "__main__":
    process_detect_plate(TEST_IMAGE_PATH)
