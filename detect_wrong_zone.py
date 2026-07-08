import cv2
import numpy as np
from ultralytics import YOLO

# Nạp mô hình mặc định YOLO26
model = YOLO("yolo26n.pt")

# Định nghĩa tọa độ vùng cấm để hàng (Dạng đa giác: Polygon)
# Ví dụ: Vùng hình tứ giác góc trên bên trái
forbidden_zone = np.array([[50, 50], [400, 50], [400, 400], [50, 400]], dtype=np.int32)

cap = cv2.VideoCapture("sample.mp4")
fps = int(cap.get(cv2.CAP_PROP_FPS))
width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
out = cv2.VideoWriter("output_zone.avi", cv2.VideoWriter_fourcc(*'XVID'), fps, (width, height))

while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break

    results = model(frame, verbose=False)[0]
    alert_triggered = False

    # Vẽ vùng cấm màu đỏ lên video
    cv2.polylines(frame, [forbidden_zone], isClosed=True, color=(0, 0, 255), thickness=3)

    for box in results.boxes:
        class_id = int(box.cls[0])
        
        # Nhãn hàng hóa trong YOLO mặc định (ví dụ: 24: backpack, 26: handbag, 28: suitcase)
        if class_id in [24, 26, 28]: 
            x1, y1, x2, y2 = map(int, box.xyxy[0])
            
            # Tính điểm trung tâm đáy của vật thể (điểm chạm đất)
            center_x = int((x1 + x2) / 2)
            center_y = y2

            # Kiểm tra xem điểm này có nằm trong vùng cấm không
            is_inside = cv2.pointPolygonTest(forbidden_zone, (center_x, center_y), False) >= 0

            if is_inside:
                alert_triggered = True
                # Vẽ khung màu đỏ nếu hàng đặt sai chỗ
                cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 2)
                cv2.putText(frame, "Goods (MISPLACED!)", (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 255), 2)
            else:
                # Vẽ khung màu xanh lá nếu hàng đặt đúng chỗ
                cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
                cv2.putText(frame, "Goods (OK)", (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)

    # Hiển thị cảnh báo lớn trên màn hình
    if alert_triggered:
        cv2.putText(frame, "ALERT: MISPLACED GOODS!", (30, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 3)

    out.write(frame)

cap.release()
out.release()
print("Xử lý xong! Kết quả lưu tại: output_zone.mp4")
