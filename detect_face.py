import cv2
import os
import urllib.request
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
# CẤU HÌNH ĐẦU VÀO & CƠ SỞ DỮ LIỆU
# =====================================================================
DB_PATH = "db_faces"
INPUT_VIDEO = "sample.mp4"
OUTPUT_VIDEO = "output_face_recognition.avi"

# TỰ ĐỘNG XÓA CACHE CỦA DEEPFACE ĐỂ CẬP NHẬT ẢNH MỚI TRONG DB_FACES
if os.path.exists(DB_PATH):
    for file in os.listdir(DB_PATH):
        if file.endswith(".pkl"):
            try:
                os.remove(os.path.join(DB_PATH, file))
                print(f"Đã làm mới cơ sở dữ liệu khuôn mặt (Xóa cache: {file})")
            except Exception:
                pass

# Kiểm tra CSDL khuôn mặt
if not os.path.exists(DB_PATH) or not os.listdir(DB_PATH):
    print(f"Lỗi: Thư mục CSDL '{DB_PATH}' đang trống hoặc không tồn tại.")
    print("Vui lòng bỏ ảnh vào thư mục db_faces/Dat/ hoặc db_faces/Thao/ trước khi chạy.")
    exit()

# FIX: Kiểm tra file video đầu vào tồn tại trước khi xử lý để tránh sinh file lỗi
if not os.path.exists(INPUT_VIDEO):
    print(f"Lỗi: Không tìm thấy file video đầu vào '{INPUT_VIDEO}'!")
    print("Vui lòng bỏ 1 file video mẫu (ví dụ quay cảnh khuôn mặt của bạn) và đổi tên thành 'sample.mp4' vào thư mục này.")
    exit()

cap = cv2.VideoCapture(INPUT_VIDEO)
if not cap.isOpened():
    print(f"Lỗi: Không thể đọc file video '{INPUT_VIDEO}'. File có thể bị lỗi hoặc không đúng định dạng.")
    exit()

# Lấy thông tin video
fps = int(cap.get(cv2.CAP_PROP_FPS))
width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

# Sử dụng codec XVID và đuôi .avi để tương thích 100% với Windows Media Player
out = cv2.VideoWriter(OUTPUT_VIDEO, cv2.VideoWriter_fourcc(*'XVID'), fps, (width, height))

# Biến tạm lưu đường dẫn ảnh tạm để xóa ở cuối
temp_img_path = "temp_frame.jpg"

# Biến lưu trữ kết quả nhận diện của khung hình trước để tối ưu hiệu năng
last_name = "Unknown"
last_box = None  # (x, y, w, h)

# Tần suất nhận diện: Cứ mỗi 15 frames mới chạy AI 1 lần (khoảng 2 lần/giây đối với video 30 FPS)
# Các frames còn lại sẽ dùng lại kết quả cũ để video mượt và cực kỳ nhanh
AI_INTERVAL = 15 

print("Đang khởi động hệ thống nhận diện khuôn mặt...")
print(f"Tổng số khung hình cần xử lý: {total_frames}")

frame_count = 0
# Đọc và xử lý từng frame video
while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        break

    frame_count += 1

    # Chỉ chạy AI nhận diện khi chia hết cho AI_INTERVAL hoặc ở frame đầu tiên
    if frame_count == 1 or frame_count % AI_INTERVAL == 0:
        # In tiến độ xử lý ra Terminal
        percent = int((frame_count / total_frames) * 100)
        print(f"-> Đang xử lý: {percent}% (Khung hình {frame_count}/{total_frames})...", end="\r")

        # Lưu frame hiện tại thành ảnh tạm để DeepFace đọc
        cv2.imwrite(temp_img_path, frame)

        try:
            # Tìm kiếm khuôn mặt trong frame xem khớp với ai trong thư mục db_faces
            results = DeepFace.find(
                img_path=temp_img_path, 
                db_path=DB_PATH, 
                model_name="VGG-Face", 
                enforce_detection=False,
                silent=True
            )
            
            # Nếu tìm thấy kết quả khớp
            if len(results) > 0 and not results[0].empty():
                # Lấy dòng kết quả khớp nhất (độ lệch distance nhỏ nhất)
                best_match_path = results[0].iloc[0]['identity']
                
                # Trích xuất tên người từ đường dẫn thư mục (ví dụ: db_faces/Alice/Alice_1.jpg -> Alice)
                folder_name = os.path.dirname(best_match_path)
                last_name = os.path.basename(folder_name)
                
                # Lấy tọa độ khuôn mặt được nhận diện trong frame
                x = int(results[0].iloc[0]['source_x'])
                y = int(results[0].iloc[0]['source_y'])
                w = int(results[0].iloc[0]['source_w'])
                h = int(results[0].iloc[0]['source_h'])
                last_box = (x, y, w, h)
            else:
                last_name = "Unknown"
                last_box = None

        except Exception as e:
            last_name = "Unknown"
            last_box = None

    # Vẽ kết quả nhận diện (dùng lại kết quả từ frame chạy AI gần nhất)
    if last_box is not None:
        x, y, w, h = last_box
        cv2.rectangle(frame, (x, y), (x + w, y + h), (0, 255, 0), 2)
        cv2.putText(frame, last_name, (x, y - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)

    # Hiển thị tên người nhận diện được lên góc màn hình video
    cv2.putText(frame, f"Identity: {last_name}", (30, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 0, 0), 2)
    out.write(frame)

# Dọn dẹp ảnh tạm
if os.path.exists(temp_img_path):
    os.remove(temp_img_path)

cap.release()
out.release()
print("\n" + "="*40)
print(f"Xử lý xong! Video kết quả đã được lưu tại: {OUTPUT_VIDEO}")
print("="*40)
