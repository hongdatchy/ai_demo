# Core AI Cloud Camera - Standalone Minimal Scripts

Thư mục này chứa 4 file Python chạy độc lập. Mỗi file giải quyết duy nhất 1 bài toán để bạn dễ dàng test và debug riêng lẻ.

## 1. Cài đặt các thư viện cần thiết
Mở terminal tại thư mục này và chạy lệnh sau để cài đặt toàn bộ thư viện:
```bash
pip install -r requirements.txt
```
*(File `requirements.txt` đã cấu hình sẵn các thư viện: `ultralytics`, `opencv-python`, `deepface`, `tf-keras`).*

## 2. Danh sách các bài toán độc lập:

1.  **Nhận dạng khuôn mặt (`detect_face.py`):**
    *   *Tính năng:* So khớp khuôn mặt trong video với cơ sở dữ liệu ảnh có sẵn của 100 người để tìm ra tên người đó.
    *   *Cách chạy:* `python detect_face.py`
    *   *Đầu vào CSDL:* Bạn chỉ cần tạo thư mục `db_faces/` và bỏ ảnh của những người cần nhận diện vào đó (ví dụ: thư mục `db_faces/Alice/` chứa ảnh của Alice, `db_faces/Bob/` chứa ảnh của Bob).
    *   *Kết quả xuất ra:* `output_face_recognition.avi`

1b. **Nhận dạng khuôn mặt Realtime (`detect_face_realtime.py`):**
    *   *Tính năng:* Mở webcam của máy tính trực tiếp và nhận dạng khuôn mặt bạn trong thời gian thực.
    *   *Cách chạy:* `python detect_face_realtime.py`
    *   *Cách thoát:* Nhấn phím `q` trên cửa sổ camera để tắt.

2.  **Cảnh báo cháy/khói (`detect_fire.py`):**
    *   *Tính năng:* Phát hiện đám cháy hoặc làn khói.
    *   *Cách chạy:* `python detect_fire.py`
    *   *Kết quả xuất ra:* `output_fire.avi`
    *   *Lưu ý:* Hãy đổi tên mô hình từ `"yolo26n.pt"` thành file weights custom đã huấn luyện của bạn (ví dụ `"fire_detect.pt"`).

3.  **Nhận diện đồng phục (`detect_uniform.py`):**
    *   *Tính năng:* Phát hiện nhân viên mặc/không mặc đồng phục.
    *   *Cách chạy:* `python detect_uniform.py`
    *   *Kết quả xuất ra:* `output_uniform.avi`
    *   *Lưu ý:* Cần nạp file weights custom đã huấn luyện (ví dụ `"uniform_detect.pt"`).

4.  **Hàng hóa đặt sai nơi quy định (`detect_wrong_zone.py`):**
    *   *Tính năng:* Vẽ vùng cấm màu đỏ. Nếu hàng hóa đi vào vùng cấm sẽ báo động đỏ. Sử dụng thuật toán đa giác của OpenCV, không cần cài thêm thư viện phụ.
    *   *Cách chạy:* `python detect_wrong_zone.py`
    *   *Kết quả xuất ra:* `output_zone.avi`

## 3. Cách chạy thử
1.  Bỏ 1 video bất kỳ đặt tên là `sample.mp4` vào thư mục này.
2.  Chạy từng file python tương ứng bằng lệnh `python <tên_file>.py`.
