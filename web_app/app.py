# SỬA LỖI SEGFAULT: Import torch đầu tiên trước cv2 để tránh xung đột thư viện OpenMP/C++ trên Linux
try:
    import torch
except Exception:
    pass

import os
import cv2
import base64
import pickle
import numpy as np
import sys
import shutil
import re
import time
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from deepface import DeepFace

# Đảm bảo in log Unicode không bị lỗi trên Windows console
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# Đường dẫn tuyệt đối tới cơ sở dữ liệu khuôn mặt ở thư mục cha hoặc Docker
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.exists('/.dockerenv'):
    DB_PATH = "/app/db_faces"
else:
    DB_PATH = os.path.abspath(os.path.join(BASE_DIR, "../db_faces"))

# Đảm bảo thư mục CSDL tồn tại
os.makedirs(DB_PATH, exist_ok=True)

app = FastAPI(title="Face Recognition Web Demo with DB Management")

# Mount thư mục db_faces để Client có thể tải ảnh thumbnail hiển thị trực tiếp
app.mount("/static/db", StaticFiles(directory=DB_PATH), name="db_faces")

db_data = []

def reload_db():
    """
    Hàm quét lại toàn bộ thư mục CSDL ảnh, xóa cache cũ, gọi DeepFace biên dịch lại
    embeddings (.pkl) và nạp đè dữ liệu mới vào RAM.
    """
    global db_data
    print("="*60)
    print("NẠP LẠI/CẬP NHẬT CƠ SỞ DỮ LIỆU AI...")
    
    # 1. Đếm số lượng ảnh trong thư mục để tránh DeepFace báo lỗi nếu CSDL rỗng
    has_images = False
    if os.path.exists(DB_PATH):
        for root, dirs, files in os.walk(DB_PATH):
            for file in files:
                if file.lower().endswith(('.jpg', '.jpeg', '.png')):
                    has_images = True
                    break
    
    if not has_images:
        # Xóa các file .pkl cũ để tránh cache lỗi thời
        for file in os.listdir(DB_PATH):
            if file.endswith(".pkl"):
                try:
                    os.remove(os.path.join(DB_PATH, file))
                except Exception:
                    pass
        db_data = []
        print("Không có bất kỳ ảnh nào trong CSDL. Đã dọn sạch cache.")
        print("="*60)
        return

    # 2. Xóa file cache .pkl cũ để ép DeepFace quét và tạo lại cache mới tinh
    for file in os.listdir(DB_PATH):
        if file.endswith(".pkl"):
            try:
                os.remove(os.path.join(DB_PATH, file))
            except Exception:
                pass

    # 3. Tạo ảnh dummy tạm thời để kích hoạt hàm DeepFace.find tự động quét & tạo file .pkl mới
    dummy_frame = np.zeros((100, 100, 3), dtype=np.uint8)
    dummy_path = os.path.join(BASE_DIR, "dummy.jpg")
    cv2.imwrite(dummy_path, dummy_frame)
    
    try:
        # Sử dụng detector yolov8n đã được gỡ triton giúp chạy siêu ổn định trên CPU
        DeepFace.find(
            img_path=dummy_path, 
            db_path=DB_PATH, 
            model_name="VGG-Face", 
            detector_backend="yolov8n", 
            enforce_detection=False, 
            silent=True
        )
    except Exception as e:
        print(f"Lỗi khi quét CSDL: {e}")
        
    if os.path.exists(dummy_path):
        os.remove(dummy_path)

    # 4. Tìm kiếm và nạp file .pkl vừa được sinh ra vào RAM
    pkl_file_path = None
    for file in os.listdir(DB_PATH):
        if file.endswith(".pkl"):
            pkl_file_path = os.path.join(DB_PATH, file)
            break

    if pkl_file_path and os.path.exists(pkl_file_path):
        try:
            with open(pkl_file_path, 'rb') as f:
                db_data = pickle.load(f)
            print(f"Đã nạp thành công {len(db_data)} ảnh khuôn mặt từ CSDL vào RAM.")
        except Exception as e:
            db_data = []
            print(f"Lỗi khi giải mã file cache .pkl: {e}")
    else:
        db_data = []
        print("CẢNH BÁO: Không tìm thấy file cache .pkl. CSDL có thể chưa được nạp.")
    print("="*60)

# Chạy lần đầu khi khởi động ứng dụng
@app.on_event("startup")
def startup_event():
    reload_db()

class FaceRequest(BaseModel):
    image: str  # Chuỗi Base64: "data:image/jpeg;base64,..."
    threshold: float = 0.40

# --- API NHẬN DIỆN KHUÔN MẶT ---
@app.post("/detect-face")
async def detect_face(req: FaceRequest):
    try:
        # Giải mã ảnh Base64 gửi lên từ trình duyệt
        header, encoded = req.image.split(",", 1)
        img_data = base64.b64decode(encoded)
        nparr = np.frombuffer(img_data, np.uint8)
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if frame is None:
            raise HTTPException(status_code=400, detail="Không thể giải mã hình ảnh.")

        # Trích xuất embedding bằng yolov8n detector
        face_objs = DeepFace.represent(
            img_path=frame,
            model_name="VGG-Face",
            detector_backend="yolov8n",
            enforce_detection=False
        )

        if len(face_objs) == 0:
            return {"detected": False, "name": "Unknown", "distance": 1.0, "box": None}

        # Kiểm tra nếu detector lấy cả khung hình làm khuôn mặt (tức là không dò được mặt thực tế)
        h_f, w_f = frame.shape[:2]
        x = int(face_objs[0]["facial_area"]["x"])
        y = int(face_objs[0]["facial_area"]["y"])
        w = int(face_objs[0]["facial_area"]["w"])
        h = int(face_objs[0]["facial_area"]["h"])

        if w >= w_f - 10 and h >= h_f - 10:
            return {"detected": False, "name": "Unknown", "distance": 1.0, "box": None}

        webcam_embedding = np.array(face_objs[0]["embedding"])
        
        best_match = None
        min_distance = float('inf')

        # So khớp với từng mặt trong CSDL
        for entry in db_data:
            db_embedding = np.array(entry["embedding"])
            # Tính khoảng cách Cosine
            distance = 1 - (np.dot(webcam_embedding, db_embedding) / 
                            (np.linalg.norm(webcam_embedding) * np.linalg.norm(db_embedding)))
            if distance < min_distance:
                min_distance = distance
                best_match = entry

        # Kiểm tra ngưỡng (threshold) động do client gửi lên
        if best_match is not None and min_distance <= req.threshold:
            best_match_path = best_match["identity"]
            # Tách thư mục cha để lấy tên người
            folder_name = os.path.dirname(best_match_path)
            recognized_name = os.path.basename(folder_name)
            return {
                "detected": True,
                "name": recognized_name,
                "distance": float(round(min_distance, 4)),
                "box": {"x": x, "y": y, "w": w, "h": h}
            }
        else:
            name_guess = "Unknown"
            if best_match is not None:
                folder_name = os.path.dirname(best_match["identity"])
                name_guess = os.path.basename(folder_name)
            return {
                "detected": False,
                "name": "Unknown",
                "name_guess": name_guess,
                "distance": float(round(min_distance, 4)) if best_match is not None else 1.0,
                "box": {"x": x, "y": y, "w": w, "h": h}
            }

    except Exception as e:
        return {"detected": False, "name": "Unknown", "error": str(e), "box": None}

# --- API QUẢN LÝ CƠ SỞ DỮ LIỆU (CRUD) ---

@app.get("/api/people")
def get_people():
    """Lấy danh sách tất cả những người và ảnh tương ứng trong CSDL"""
    people = {}
    if os.path.exists(DB_PATH):
        for name in sorted(os.listdir(DB_PATH)):
            dir_path = os.path.join(DB_PATH, name)
            if os.path.isdir(dir_path):
                images = []
                for file in sorted(os.listdir(dir_path)):
                    if file.lower().endswith(('.jpg', '.jpeg', '.png')):
                        images.append(file)
                people[name] = images
    return people

class PersonCreate(BaseModel):
    name: str

@app.post("/api/people")
def create_person(req: PersonCreate):
    """Tạo thêm một người mới (tạo thư mục)"""
    name = req.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Tên không được để trống")
    
    # Kiểm tra tính hợp lệ của tên để tránh path traversal và ký tự dị
    if not re.match(r"^[a-zA-Z0-9_\-\sÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚĂĐĨŨƠàáâãèéêìíòóôõùúăđĩũơƯĂÂÊÔƠƯưăâêôơư  ]+$", name):
        raise HTTPException(status_code=400, detail="Tên chỉ được chứa chữ cái, số, dấu cách, gạch ngang, gạch dưới")
    
    person_dir = os.path.join(DB_PATH, name)
    if os.path.exists(person_dir):
        raise HTTPException(status_code=400, detail="Người này đã tồn tại trong CSDL")
    
    try:
        os.makedirs(person_dir, exist_ok=True)
        return {"message": f"Đã thêm người: {name}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Không thể tạo thư mục: {str(e)}")

@app.delete("/api/people/{name}")
def delete_person(name: str):
    """Xóa một người và toàn bộ ảnh của họ"""
    person_dir = os.path.join(DB_PATH, name)
    if not os.path.exists(person_dir) or not os.path.isdir(person_dir):
        raise HTTPException(status_code=404, detail="Không tìm thấy người này trong CSDL")
    
    try:
        shutil.rmtree(person_dir)
        reload_db()  # Tái cấu trúc CSDL AI ngay lập tức
        return {"message": f"Đã xóa hoàn toàn người: {name}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi xóa người: {str(e)}")

@app.post("/api/people/{name}/upload")
async def upload_image(name: str, file: UploadFile = File(...)):
    """Tải lên hình ảnh khuôn mặt mới cho một người cụ thể"""
    person_dir = os.path.join(DB_PATH, name)
    if not os.path.exists(person_dir) or not os.path.isdir(person_dir):
        raise HTTPException(status_code=404, detail="Không tìm thấy người này")
    
    if not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="File tải lên bắt buộc phải là định dạng hình ảnh")
        
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ['.jpg', '.jpeg', '.png']:
        raise HTTPException(status_code=400, detail="Chỉ hỗ trợ ảnh đuôi .jpg, .jpeg, .png")
        
    # Tạo tên file chống trùng lặp dựa trên timestamp
    clean_filename = f"{int(time.time())}_{file.filename}"
    file_path = os.path.join(person_dir, clean_filename)
    
    try:
        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)
        reload_db()  # Tái cấu trúc CSDL AI ngay lập tức
        return {"message": f"Đã lưu ảnh và cập nhật AI", "filename": clean_filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi lưu ảnh: {str(e)}")

@app.delete("/api/people/{name}/images/{filename}")
def delete_image(name: str, filename: str):
    """Xóa một bức ảnh cụ thể của một người"""
    person_dir = os.path.join(DB_PATH, name)
    if not os.path.exists(person_dir) or not os.path.isdir(person_dir):
        raise HTTPException(status_code=404, detail="Không tìm thấy người này")
        
    file_path = os.path.join(person_dir, filename)
    
    # Bảo mật: Đảm bảo đường dẫn tuyệt đối thực sự thuộc thư mục người đó
    if not os.path.abspath(file_path).startswith(os.path.abspath(person_dir)):
        raise HTTPException(status_code=400, detail="Yêu cầu không hợp lệ")
        
    if not os.path.exists(file_path) or not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="Không tìm thấy hình ảnh cần xóa")
        
    try:
        os.remove(file_path)
        reload_db()  # Tái cấu trúc CSDL AI ngay lập tức
        return {"message": f"Đã xóa ảnh {filename}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi xóa ảnh: {str(e)}")

# --- PHỤC VỤ TRANG GIAO DIỆN CHÍNH ---
@app.get("/")
def read_root():
    return FileResponse(os.path.join(BASE_DIR, "index.html"))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
