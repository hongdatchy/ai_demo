import os
import urllib.request
import cv2
from ultralytics import YOLO

# Tên file mô hình lưu cục bộ
MODEL_PATH = "uniform_detect.pt"

# Tự động tải mô hình nhận diện đồ bảo hộ/đồng phục từ Hugging Face nếu chưa có
if not os.path.exists(MODEL_PATH):
    print("Không thấy file uniform_detect.pt cục bộ.")
    print("Đang tự động tải mô hình đồng phục/đồ bảo hộ từ Hugging Face (dung lượng khoảng 6MB)...")
    url = "https://huggingface.co/Hansung-Cho/yolov8-ppe-detection/resolve/main/best.pt"
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
out = cv2.VideoWriter("output_uniform.avi", cv2.VideoWriter_fourcc(*'XVID'), fps, (width, height))

while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break

    results = model(frame, verbose=False)[0]
    non_compliance_count = 0

    for box in results.boxes:
        class_id = int(box.cls[0])
        label = results.names[class_id]
        
        x1, y1, x2, y2 = map(int, box.xyxy[0])
        
        # Nếu mô hình custom phát hiện lớp 'no-helmet' hoặc 'no-vest' (không mặc đồ bảo hộ/đồng phục)
        # Hoặc nếu dùng mô hình mặc định thì nhận diện mọi vật thể để test
        if label.lower() in ["no-helmet", "no-vest"] or MODEL_PATH == "yolo26n.pt":
            non_compliance_count += 1
            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 2)
            cv2.putText(frame, label.upper(), (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 255), 2)
        else:
            # Vẽ màu xanh lá cho những người/vật thể tuân thủ (ví dụ: 'helmet', 'vest')
            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
            cv2.putText(frame, label.upper(), (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)

    # Hiển thị số ca vi phạm lên video
    if non_compliance_count > 0:
        cv2.putText(frame, f"Violations: {non_compliance_count}", (30, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 3)

    out.write(frame)

cap.release()
out.release()
print("Xử lý xong! Kết quả lưu tại: output_uniform.mp4")
