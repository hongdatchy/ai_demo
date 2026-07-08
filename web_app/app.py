import os
import cv2
import base64
import pickle
import numpy as np
import sys
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from deepface import DeepFace

# Đảm bảo in log Unicode không bị lỗi trên Windows console
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

# Đường dẫn tuyệt đối tới cơ sở dữ liệu khuôn mặt ở thư mục cha
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.abspath(os.path.join(BASE_DIR, "../db_faces"))

app = FastAPI(title="Face Recognition Web Demo")

db_data = []

@app.on_event("startup")
def load_db():
    global db_data
    print("="*60)
    print("KHỞI ĐỘNG SERVER WEB AI...")
    print(f"Thư mục CSDL: {DB_PATH}")
    
    # Tự động xóa file cache .pkl cũ nếu có để tạo lại sạch sẽ theo yolov8n
    if os.path.exists(DB_PATH):
        for file in os.listdir(DB_PATH):
            if file.endswith(".pkl"):
                try:
                    os.remove(os.path.join(DB_PATH, file))
                except Exception:
                    pass

    # Tạo ảnh dummy để kích hoạt DeepFace quét và tạo cache .pkl mới
    dummy_frame = np.zeros((100, 100, 3), dtype=np.uint8)
    dummy_path = os.path.join(BASE_DIR, "dummy.jpg")
    cv2.imwrite(dummy_path, dummy_frame)
    try:
        DeepFace.find(img_path=dummy_path, db_path=DB_PATH, model_name="VGG-Face", detector_backend="yolov8n", enforce_detection=False, silent=True)
    except Exception as e:
        print(f"Cảnh báo quét CSDL: {e}")
    if os.path.exists(dummy_path):
        os.remove(dummy_path)

    # Nạp file .pkl chứa embeddings vừa tạo
    pkl_file_path = None
    if os.path.exists(DB_PATH):
        for file in os.listdir(DB_PATH):
            if file.endswith(".pkl"):
                pkl_file_path = os.path.join(DB_PATH, file)
                break

    if pkl_file_path and os.path.exists(pkl_file_path):
        with open(pkl_file_path, 'rb') as f:
            db_data = pickle.load(f)
        print(f"Đã nạp thành công {len(db_data)} ảnh khuôn mặt từ CSDL vào RAM.")
    else:
        print("CẢNH BÁO: Không tìm thấy file cache .pkl. Vui lòng thêm ảnh vào db_faces.")
    print("="*60)

class FaceRequest(BaseModel):
    image: str  # Base64 string: "data:image/jpeg;base64,..."
    threshold: float = 0.40

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
            # Không dò thấy mặt thực sự
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

@app.get("/")
def read_root():
    return FileResponse(os.path.join(BASE_DIR, "index.html"))

if __name__ == "__main__":
    import uvicorn
    # Chạy trên tất cả IP (0.0.0.0) cổng 8000 để server từ bên ngoài truy cập được
    uvicorn.run(app, host="0.0.0.0", port=8000)
