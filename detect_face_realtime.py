import cv2
import os
import urllib.request
import pickle
import numpy as np
from deepface import DeepFace

# =====================================================================
# SỬA LỖI TỰ ĐỘNG: Thiếu file cấu hình XML của OpenCV (Haarcascade)
# =====================================================================
try:
    cv2_dir = os.path.dirname(cv2.__file__)
    opencv_data_dir = os.path.join(cv2_dir, "data")
    xml_file = os.path.join(opencv_data_dir, "haarcascade_frontalface_default.xml")
    
    if not os.path.exists(xml_file):
        print("Phát hiện thiếu file nhận diện haarcascade_frontalface_default.xml của OpenCV.")
        print("Đang tự động tải về để tự sửa lỗi...")
        if not os.path.exists(opencv_data_dir):
            os.makedirs(opencv_data_dir)
        url = "https://raw.githubusercontent.com/opencv/opencv/master/data/haarcascades/haarcascade_frontalface_default.xml"
        urllib.request.urlretrieve(url, xml_file)
        print("Đã tải và sửa lỗi thành công!")
except Exception as e:
    print(f"Không thể kiểm tra/sửa lỗi OpenCV XML tự động: {e}")

# =====================================================================
# CẤU HÌNH CƠ SỞ DỮ LIỆU & CAMERA
# =====================================================================
DB_PATH = "db_faces"

# Tự động xóa file cache .pkl cũ để tạo file mới sạch sẽ
if os.path.exists(DB_PATH):
    for file in os.listdir(DB_PATH):
        if file.endswith(".pkl"):
            try:
                os.remove(os.path.join(DB_PATH, file))
                print(f"Đã làm mới cơ sở dữ liệu khuôn mặt (Xóa cache: {file})")
            except Exception:
                pass

# Kích hoạt DeepFace để tự quét thư mục db_faces và tạo file .pkl mới
print("Đang quét CSDL ảnh và trích xuất đặc trưng khuôn mặt...")
dummy_frame = np.zeros((100, 100, 3), dtype=np.uint8)
cv2.imwrite("dummy.jpg", dummy_frame)
try:
    # Sử dụng detector yolov8n thay vì opencv để nhận diện chính xác
    DeepFace.find(img_path="dummy.jpg", db_path=DB_PATH, model_name="VGG-Face", detector_backend="yolov8n", enforce_detection=False, silent=True)
except Exception as e:
    print(f"Lỗi khi quét CSDL: {e}")
if os.path.exists("dummy.jpg"):
    os.remove("dummy.jpg")

# Nạp file .pkl chứa embeddings vừa tạo
pkl_file_path = None
for file in os.listdir(DB_PATH):
    if file.endswith(".pkl"):
        pkl_file_path = os.path.join(DB_PATH, file)
        break

db_data = []
if pkl_file_path and os.path.exists(pkl_file_path):
    with open(pkl_file_path, 'rb') as f:
        db_data = pickle.load(f)
    print(f"Đã nạp thành công dữ liệu vector của {len(db_data)} ảnh khuôn mặt từ CSDL.")
else:
    print("Cảnh báo: Không thể nạp cơ sở dữ liệu khuôn mặt (.pkl). Vui lòng kiểm tra lại thư mục db_faces.")

# Mở camera (Mặc định 0 là webcam tích hợp của laptop/máy tính)
cap = cv2.VideoCapture(0)

if not cap.isOpened():
    print("Lỗi: Không thể mở camera (Webcam). Vui lòng kiểm tra lại thiết bị kết nối.")
    exit()

# Cấu hình tần suất nhận diện để tránh giật lag camera (Cứ 10 frames chạy AI 1 lần)
AI_INTERVAL = 10
frame_count = 0

last_name = "Unknown"
last_box = None  # (x, y, w, h)
temp_img_path = "temp_webcam_frame.jpg"

import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# Ngưỡng chấp nhận (THRESHOLD): Đặt về 0.30 để yêu cầu độ giống nhau cực kỳ cao (>= 70%),
# đảm bảo chỉ khi khuôn mặt khớp gần như tuyệt đối mới được nhận diện.
THRESHOLD = 0.30

print("\n" + "="*50)
print("HỆ THỐNG NHẬN DIỆN KHUÔN MẶT REALTIME ĐANG CHẠY")
print(f"Ngưỡng nhận diện tùy chỉnh: <= {THRESHOLD} (Cosine Distance)")
print("Bấm phím 'q' trên màn hình hiển thị để thoát.")
print("="*50)

while True:
    ret, frame = cap.read()
    if not ret:
        print("Lỗi: Không thể nhận luồng hình ảnh từ camera.")
        break

    frame_count += 1

    # Chạy AI nhận dạng định kỳ để đảm bảo camera hiển thị mượt mà không lag
    if frame_count % AI_INTERVAL == 0:
        cv2.imwrite(temp_img_path, frame)
        try:
            # 1. Trích xuất embedding của khuôn mặt trên webcam dùng detector yolov8n
            face_objs = DeepFace.represent(
                img_path=temp_img_path, 
                model_name="VGG-Face", 
                detector_backend="yolov8n",
                enforce_detection=False
            )
            
            if len(face_objs) > 0:
                # Luôn lấy tọa độ khuôn mặt phát hiện được trên webcam (để vẽ khung đỏ báo Unknown)
                x = face_objs[0]["facial_area"]["x"]
                y = face_objs[0]["facial_area"]["y"]
                w = face_objs[0]["facial_area"]["w"]
                h = face_objs[0]["facial_area"]["h"]
                
                # Bỏ qua nếu diện tích khung nhận diện quá nhỏ (nhiễu) hoặc là toàn bộ khung hình (fail detection)
                frame_h, frame_w = frame.shape[:2]
                if w >= frame_w - 10 and h >= frame_h - 10:
                    # Đây là trường hợp detector không tìm thấy mặt và lấy cả khung hình làm mặt
                    last_box = None
                    last_name = "Unknown"
                else:
                    last_box = (x, y, w, h)
                    
                    if len(db_data) > 0:
                        webcam_embedding = np.array(face_objs[0]["embedding"])
                        
                        best_match = None
                        min_distance = float('inf')
                        
                        # 2. So khớp với từng ảnh trong CSDL
                        for entry in db_data:
                            db_embedding = np.array(entry["embedding"])
                            
                            # Tính khoảng cách Cosine giữa 2 vector
                            distance = 1 - (np.dot(webcam_embedding, db_embedding) / 
                                            (np.linalg.norm(webcam_embedding) * np.linalg.norm(db_embedding)))
                            
                            if distance < min_distance:
                                min_distance = distance
                                best_match = entry
                        
                        # 3. Kiểm tra xem khoảng cách nhỏ nhất có nằm trong ngưỡng cho phép không
                        if best_match is not None and min_distance <= THRESHOLD:
                            best_match_path = best_match["identity"]
                            folder_name = os.path.dirname(best_match_path)
                            last_name = os.path.basename(folder_name)
                            
                            # In kết quả khớp ra console
                            print(f"[KHỚP]: {last_name} (Đo được: {min_distance:.4f} <= {THRESHOLD})")
                        else:
                            last_name = "Unknown"
                            if best_match is not None:
                                folder_name = os.path.dirname(best_match["identity"])
                                name_guess = os.path.basename(folder_name)
                                print(f"[TRƯỢT]: Gần giống {name_guess} (Đo được: {min_distance:.4f} > {THRESHOLD})")
                    else:
                        last_name = "Unknown"
            else:
                print("[DÒ TÌM]: Không tìm thấy khuôn mặt nào trên webcam...")
                last_box = None
                last_name = "Unknown"
        except Exception as e:
            print(f"[LỖI HỆ THỐNG]: {e}")
            last_box = None
            last_name = "Unknown"

    # Vẽ kết quả nhận diện lên frame (sử dụng lại kết quả từ lần quét AI gần nhất)
    if last_box is not None:
        x, y, w, h = last_box
        # Xanh lá nếu khớp, Đỏ nếu phát hiện mặt nhưng không khớp (Unknown)
        color = (0, 255, 0) if last_name != "Unknown" else (0, 0, 255)
        cv2.rectangle(frame, (x, y), (x + w, y + h), color, 2)
        cv2.putText(frame, last_name, (x, y - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.8, color, 2)

    # Hiển thị thông tin tên người lên góc màn hình
    cv2.putText(frame, f"Identity: {last_name}", (30, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 0, 0), 2)

    # Hiển thị cửa sổ camera trực tiếp
    cv2.imshow("Realtime Face Recognition (Press 'q' to Quit)", frame)

    # Nhấn 'q' để thoát khỏi vòng lặp
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

# Dọn dẹp
cap.release()
cv2.destroyAllWindows()
if os.path.exists(temp_img_path):
    os.remove(temp_img_path)
print("Đã tắt hệ thống camera.")
