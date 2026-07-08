import os
import urllib.request
import cv2
from ultralytics import YOLO

# Tên file mô hình lưu cục bộ
MODEL_PATH = "fire_detect.pt"

# Tự động tải mô hình cảnh báo cháy/khói đã được huấn luyện sẵn từ Hugging Face nếu chưa có
if not os.path.exists(MODEL_PATH):
    print("Không thấy file fire_detect.pt cục bộ.")
    print("Đang tự động tải mô hình nhận diện lửa/khói từ Hugging Face (dung lượng khoảng 6MB)...")
    url = "https://huggingface.co/rabahdev/fire-smoke-yolov8n/resolve/main/best.pt"
    try:
        # Tải file về máy
        urllib.request.urlretrieve(url, MODEL_PATH)
        print("Tải mô hình thành công!")
    except Exception as e:
        print(f"Không thể tải từ internet: {e}. Sẽ dùng tạm mô hình mặc định yolo26n.pt.")
        MODEL_PATH = "yolo26n.pt"

# Nạp mô hình
model = YOLO(MODEL_PATH) 

cap = cv2.VideoCapture("sample.mp4")
fps = int(cap.get(cv2.CAP_PROP_FPS))
width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
out = cv2.VideoWriter("output_fire.avi", cv2.VideoWriter_fourcc(*'XVID'), fps, (width, height))

while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break

    results = model(frame, verbose=False)[0]
    fire_detected = False

    for box in results.boxes:
        class_id = int(box.cls[0])
        # Lấy tên nhãn thực tế của mô hình (ví dụ: 'fire', 'smoke')
        label = results.names[class_id]
        
        # Nếu dùng mô hình mặc định yolo26n thì nhận diện mọi vật thể để test, 
        # Nếu dùng mô hình fire_detect thì chỉ lọc nhãn 'fire' hoặc 'smoke'
        if label.lower() in ["fire", "smoke"] or MODEL_PATH == "yolo26n.pt":
            fire_detected = True
            x1, y1, x2, y2 = map(int, box.xyxy[0])
            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 2)
            cv2.putText(frame, label.upper(), (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 255), 2)

    # Hiển thị dòng chữ cảnh báo lớn nếu phát hiện cháy
    if fire_detected:
        cv2.putText(frame, "WARNING: FIRE DETECTED!", (30, 60), cv2.FONT_HERSHEY_SIMPLEX, 1.2, (0, 0, 255), 3)

    out.write(frame)

cap.release()
out.release()
print("Xử lý xong! Kết quả lưu tại: output_fire.mp4")
