import os
import sys
import shutil
import re
import time
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.abspath(os.path.join(BASE_DIR, ".."))
if PARENT_DIR not in sys.path:
    sys.path.append(PARENT_DIR)
CLOUD_CAM_DIR = os.path.join(PARENT_DIR, "cloud_camera")
if CLOUD_CAM_DIR not in sys.path:
    sys.path.append(CLOUD_CAM_DIR)

# Import các hàm quản lý luồng HLS từ dynamic_frame_extractor
try:
    from dynamic_frame_extractor import start_stream, stop_stream, get_all_streams, stop_all_streams
except ImportError:
    from cloud_camera.dynamic_frame_extractor import start_stream, stop_stream, get_all_streams, stop_all_streams

DB_PATH = os.path.abspath(os.path.join(BASE_DIR, "../db_faces"))
PROCESSED_PATH = os.path.abspath(os.path.join(BASE_DIR, "../processed_faces"))
os.makedirs(DB_PATH, exist_ok=True)
os.makedirs(PROCESSED_PATH, exist_ok=True)

app = FastAPI(title="Cloud Camera AI Manager Portal")

# Static mounts
app.mount("/static/db", StaticFiles(directory=DB_PATH), name="db_faces")
app.mount("/static/processed", StaticFiles(directory=PROCESSED_PATH), name="processed_faces")

# =====================================================================
# 1. QUẢN LÝ CSDL KHUÔN MẶT (CRUD)
# =====================================================================

@app.get("/api/people")
def get_people():
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
    name = req.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Tên không được để trống")
    
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
    person_dir = os.path.join(DB_PATH, name)
    if not os.path.exists(person_dir) or not os.path.isdir(person_dir):
        raise HTTPException(status_code=404, detail="Không tìm thấy người này trong CSDL")
    
    try:
        shutil.rmtree(person_dir)
        return {"message": f"Đã xóa người: {name}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi xóa người: {str(e)}")


@app.post("/api/people/{name}/upload")
async def upload_image(name: str, file: UploadFile = File(...)):
    person_dir = os.path.join(DB_PATH, name)
    if not os.path.exists(person_dir) or not os.path.isdir(person_dir):
        raise HTTPException(status_code=404, detail="Không tìm thấy người này")
    
    if not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="File tải lên bắt buộc phải là hình ảnh")
        
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ['.jpg', '.jpeg', '.png']:
        raise HTTPException(status_code=400, detail="Chỉ hỗ trợ ảnh .jpg, .jpeg, .png")
        
    clean_filename = f"{int(time.time())}_{file.filename}"
    file_path = os.path.join(person_dir, clean_filename)
    
    try:
        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)
        return {"message": f"Đã lưu ảnh", "filename": clean_filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi lưu ảnh: {str(e)}")


@app.delete("/api/people/{name}/images/{filename}")
def delete_image(name: str, filename: str):
    person_dir = os.path.join(DB_PATH, name)
    if not os.path.exists(person_dir) or not os.path.isdir(person_dir):
        raise HTTPException(status_code=404, detail="Không tìm thấy người này")
        
    file_path = os.path.join(person_dir, filename)
    if not os.path.abspath(file_path).startswith(os.path.abspath(person_dir)):
        raise HTTPException(status_code=400, detail="Yêu cầu không hợp lệ")
        
    if not os.path.exists(file_path) or not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="Không tìm thấy hình ảnh cần xóa")
        
    try:
        os.remove(file_path)
        return {"message": f"Đã xóa ảnh {filename}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi xóa ảnh: {str(e)}")


# =====================================================================
# 2. QUẢN LÝ CÁC LUỒNG HLS STREAM (ADD / DELETE / LIST)
# =====================================================================

class StreamAddRequest(BaseModel):
    url: str
    task_type: str = "detect_face"


@app.get("/api/streams")
def list_streams():
    """Lấy danh sách các luồng HLS đang cắt ảnh"""
    return {"streams": get_all_streams()}


@app.post("/api/streams")
def add_stream(req: StreamAddRequest):
    """Thêm luồng HLS mới"""
    url = req.url.strip()
    if not url:
        raise HTTPException(status_code=400, detail="URL stream không được để trống")
    
    result = start_stream(url, req.task_type)
    if result["status"] == "ALREADY_RUNNING":
        raise HTTPException(status_code=400, detail=f"Luồng [{result['cloudId']}] đã đang chạy")
    
    return {"message": f"Đã kích hoạt luồng: {result['cloudId']}", "data": result}


@app.delete("/api/streams/{cloud_id}")
def remove_stream(cloud_id: str):
    """Dừng và gỡ bỏ luồng HLS"""
    result = stop_stream(cloud_id)
    if result["status"] == "NOT_FOUND":
        raise HTTPException(status_code=404, detail="Không tìm thấy luồng này")
    
    return {"message": f"Đã ngắt luồng {cloud_id} thành công"}


# =====================================================================
# 3. XEM LẠI KẾT QUẢ DETECT (TỪ FOLDER processed_faces)
# =====================================================================

@app.get("/api/results")
def get_detect_results(cloud_id: str = None, limit: int = 60):
    """Lấy danh sách các ảnh kết quả đã được AI xử lý từ processed_faces"""
    results = []
    if os.path.exists(PROCESSED_PATH):
        files = [f for f in os.listdir(PROCESSED_PATH) if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
        
        # Sắp xếp ảnh mới nhất lên đầu theo thời gian sửa đổi (mtime)
        files.sort(key=lambda f: os.path.getmtime(os.path.join(PROCESSED_PATH, f)), reverse=True)
        
        for f in files:
            # Lọc theo cloud_id nếu có yêu cầu
            if cloud_id and cloud_id not in f:
                continue
            
            file_path = os.path.join(PROCESSED_PATH, f)
            mtime = os.path.getmtime(file_path)
            results.append({
                "fileName": f,
                "url": f"/static/processed/{f}",
                "timestamp": int(mtime * 1000),
                "timeStr": time.strftime('%H:%M:%S %d/%m/%Y', time.localtime(mtime))
            })
            if len(results) >= limit:
                break

    return {"results": results}


@app.delete("/api/results/clear")
def clear_detect_results():
    """Dọn dẹp làm sạch toàn bộ ảnh kết quả cũ trong folder processed_faces"""
    count = 0
    if os.path.exists(PROCESSED_PATH):
        for f in os.listdir(PROCESSED_PATH):
            p = os.path.join(PROCESSED_PATH, f)
            if os.path.isfile(p):
                try:
                    os.remove(p)
                    count += 1
                except Exception:
                    pass
    return {"message": f"Đã xóa sạch {count} ảnh kết quả cũ"}


# Dọn dẹp sạch sẽ toàn bộ luồng camera khi tắt ứng dụng Web
@app.on_event("shutdown")
def shutdown_event():
    print("\n[WEB SHUTDOWN] Dang don dep tat ca cac luong HLS...")
    stop_all_streams()
    # Ép thoát dứt điểm tiến trình tránh thread chạy ngầm
    threading.Timer(0.5, lambda: os._exit(0)).start()


# --- PHỤC VỤ TRANG GIAO DIỆN CHÍNH ---
@app.get("/")
def read_root():
    return FileResponse(os.path.join(BASE_DIR, "index.html"))


if __name__ == "__main__":
    import uvicorn
    import threading
    try:
        uvicorn.run(app, host="0.0.0.0", port=8000)
    finally:
        stop_all_streams()
        os._exit(0)

