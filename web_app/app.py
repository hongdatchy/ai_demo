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

import requests as http

# URL của Stream Coordinator — đổi theo môi trường thực tế
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8200")

DB_PATH = os.path.abspath(os.path.join(BASE_DIR, "../db_faces"))
PROCESSED_PATH = os.path.abspath(os.path.join(BASE_DIR, "../processed_faces"))
PROCESSED_FIRE_PATH = os.path.abspath(os.path.join(BASE_DIR, "../processed_fire"))
TEMP_FRAMES_PATH = os.path.abspath(os.path.join(BASE_DIR, "../temp_frames"))

# Đảm bảo các thư mục luôn tồn tại
os.makedirs(DB_PATH, exist_ok=True)
os.makedirs(PROCESSED_PATH, exist_ok=True)
os.makedirs(PROCESSED_FIRE_PATH, exist_ok=True)
os.makedirs(TEMP_FRAMES_PATH, exist_ok=True)

app = FastAPI(title="Cloud Camera AI Manager Portal")

# Tự động kiểm tra và tạo lại các thư mục khi start webapp
@app.on_event("startup")
def startup_event():
    os.makedirs(DB_PATH, exist_ok=True)
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    os.makedirs(PROCESSED_FIRE_PATH, exist_ok=True)
    os.makedirs(TEMP_FRAMES_PATH, exist_ok=True)
    print(f"[STARTUP] Da kiem tra va tao day du cac thu muc:")
    print(f" - DB Faces: {DB_PATH}")
    print(f" - Temp Frames: {TEMP_FRAMES_PATH}")
    print(f" - Processed Faces: {PROCESSED_PATH}")
    print(f" - Processed Fire: {PROCESSED_FIRE_PATH}")

# Static mounts
app.mount("/static/db", StaticFiles(directory=DB_PATH), name="db_faces")
app.mount("/static/processed", StaticFiles(directory=PROCESSED_PATH), name="processed_faces")
app.mount("/static/processed_fire", StaticFiles(directory=PROCESSED_FIRE_PATH), name="processed_fire")

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
    task_types: list[str] | str | None = None
    task_type: str | None = "detect_face"


@app.get("/api/streams")
def list_streams():
    """Lấy danh sách các luồng HLS đang chạy kèm thông tin node từ Coordinator"""
    try:
        resp = http.get(f"{COORDINATOR_URL}/streams/details", timeout=5)
        return resp.json()
    except Exception as e:
        return {"streams": [], "error": f"Không kết nối được Coordinator: {e}"}


@app.post("/api/streams")
def add_stream(req: StreamAddRequest):
    """Thêm luồng HLS mới — Coordinator tự chọn node ít tải nhất"""
    url = req.url.strip()
    if not url:
        raise HTTPException(status_code=400, detail="URL stream không được để trống")

    tasks = req.task_types or req.task_type or ["detect_face"]

    try:
        resp = http.post(
            f"{COORDINATOR_URL}/stream/start",
            json={
                "url": url,
                "task_types": tasks,
                "task_type": ",".join(tasks) if isinstance(tasks, list) else str(tasks)
            },
            timeout=10
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        result = resp.json()
        if result.get("status") == "ALREADY_RUNNING":
            raise HTTPException(status_code=400, detail=f"Luồng [{result.get('cloudId')}] đã đang chạy")
        return {"message": f"Đã kích hoạt luồng: {result.get('cloudId')}", "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Không kết nối được Coordinator: {e}")


@app.delete("/api/streams/{cloud_id}")
def remove_stream(cloud_id: str):
    """Dừng luồng HLS qua Coordinator"""
    try:
        resp = http.post(
            f"{COORDINATOR_URL}/stream/stop",
            json={"cloud_id": cloud_id},
            timeout=10
        )
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Không tìm thấy luồng này")
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return {"message": f"Đã ngắt luồng {cloud_id} thành công"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Không kết nối được Coordinator: {e}")


# =====================================================================
# 3. XEM LẠI KẾT QUẢ DETECT (processed_faces VÀ processed_fire)
# =====================================================================

@app.get("/api/results")
def get_detect_results(cloud_id: str = None, limit: int = 60):
    """Lấy danh sách các ảnh kết quả đã được AI xử lý từ processed_faces và processed_fire"""
    all_files = []
    
    # 1. Quét ảnh nhận diện khuôn mặt
    if os.path.exists(PROCESSED_PATH):
        for f in os.listdir(PROCESSED_PATH):
            if f.lower().endswith(('.jpg', '.jpeg', '.png')):
                full_p = os.path.join(PROCESSED_PATH, f)
                all_files.append((f, full_p, f"/static/processed/{f}", "Khuôn mặt"))

    # 2. Quét ảnh phát hiện cháy
    if os.path.exists(PROCESSED_FIRE_PATH):
        for f in os.listdir(PROCESSED_FIRE_PATH):
            if f.lower().endswith(('.jpg', '.jpeg', '.png')):
                full_p = os.path.join(PROCESSED_FIRE_PATH, f)
                all_files.append((f, full_p, f"/static/processed_fire/{f}", "Cảnh báo Cháy"))

    # Sắp xếp ảnh mới nhất lên đầu theo mtime
    all_files.sort(key=lambda item: os.path.getmtime(item[1]), reverse=True)

    results = []
    for f, full_p, url, tag in all_files:
        if cloud_id and cloud_id not in f:
            continue
        mtime = os.path.getmtime(full_p)
        results.append({
            "fileName": f,
            "url": url,
            "tag": tag,
            "timestamp": int(mtime * 1000),
            "timeStr": time.strftime('%H:%M:%S %d/%m/%Y', time.localtime(mtime))
        })
        if len(results) >= limit:
            break

    return {"results": results}


@app.delete("/api/results/clear")
def clear_detect_results():
    """Dọn dẹp làm sạch ảnh kết quả cũ trong cả processed_faces, processed_fire VÀ temp_frames"""
    count = 0
    for target_dir in [PROCESSED_PATH, PROCESSED_FIRE_PATH, TEMP_FRAMES_PATH]:
        if os.path.exists(target_dir):
            for f in os.listdir(target_dir):
                p = os.path.join(target_dir, f)
                if os.path.isfile(p):
                    try:
                        os.remove(p)
                        count += 1
                    except Exception:
                        pass
    return {"message": f"Đã xóa sạch {count} ảnh (gồm kết quả và ảnh tạm temp_frames)"}


# =====================================================================
# 4. TRUY VẤN TIẾN ĐỘ CONSUMERS TRỰC TIẾP TỪ KAFKA (KHÔNG ĐẾM FILE)
# =====================================================================
@app.get("/api/kafka/consumers-stats")
def get_kafka_consumers_stats():
    """
    Truy vấn số lượng message đã xử lý và tổng message của từng Topic riêng biệt:
      - 'ai_face_topic' cho AI Nhận diện Khuôn Mặt (face_recognition_group)
      - 'ai_fire_topic' cho AI Phát hiện Cháy/Khói (fire_detection_group)
    """
    from kafka.admin import KafkaAdminClient
    from kafka import KafkaConsumer, TopicPartition

    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '27.71.24.102:9093')
    kafka_user = os.getenv('KAFKA_USER', 'admin')
    kafka_password = os.getenv('KAFKA_PASSWORD', 'Admin@123')

    target_groups = [
        {
            "groupId": "face_recognition_group",
            "name": "AI Nhận diện Khuôn Mặt (ai_face_consumer)",
            "topic": os.getenv('KAFKA_FACE_TOPIC', 'ai_face_topic')
        },
        {
            "groupId": "fire_detection_group",
            "name": "AI Phát hiện Cháy/Khói (ai_fire_consumer)",
            "topic": os.getenv('KAFKA_FIRE_TOPIC', 'ai_fire_topic')
        }
    ]

    consumer_client = None
    admin = None
    try:
        consumer_client = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=kafka_user,
            sasl_plain_password=kafka_password,
            request_timeout_ms=5000
        )
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=kafka_user,
            sasl_plain_password=kafka_password,
            request_timeout_ms=5000
        )

        groups_stats = []
        total_all_messages = 0

        for g in target_groups:
            gid = g["groupId"]
            topic_name = g["topic"]
            try:
                partitions = consumer_client.partitions_for_topic(topic_name)
                if not partitions:
                    groups_stats.append({
                        "groupId": gid,
                        "name": g["name"],
                        "topic": topic_name,
                        "processedMessages": 0,
                        "totalMessages": 0,
                        "lag": 0,
                        "percent": 100.0,
                        "status": "CHƯA CÓ MESSAGE (IDLE)"
                    })
                    continue

                tps = [TopicPartition(topic_name, p) for p in partitions]
                beginning_offsets = consumer_client.beginning_offsets(tps)
                end_offsets = consumer_client.end_offsets(tps)

                topic_total = sum(end_offsets[tp] - beginning_offsets[tp] for tp in tps)
                topic_latest = sum(end_offsets[tp] for tp in tps)
                total_all_messages += topic_total

                raw_offsets = admin.list_consumer_group_offsets(gid) if hasattr(admin, 'list_consumer_group_offsets') else admin.list_group_offsets(gid)
                group_data = raw_offsets.get(gid, {}) if isinstance(raw_offsets, dict) else raw_offsets

                committed_offset = 0
                for tp in tps:
                    offset_obj = group_data.get(tp)
                    if offset_obj:
                        committed_offset += getattr(offset_obj, 'offset', 0)

                processed = committed_offset
                lag = max(0, topic_latest - committed_offset) if topic_total > 0 else 0
                pct = round((processed / topic_total * 100), 1) if topic_total > 0 else 100.0

                groups_stats.append({
                    "groupId": gid,
                    "name": g["name"],
                    "topic": topic_name,
                    "processedMessages": processed if topic_total > 0 else 0,
                    "totalMessages": topic_total,
                    "lag": lag,
                    "percent": min(100.0, pct),
                    "status": "ACTIVE" if lag == 0 else "PROCESSING"
                })
            except Exception as ex:
                groups_stats.append({
                    "groupId": gid,
                    "name": g["name"],
                    "topic": topic_name,
                    "processedMessages": 0,
                    "totalMessages": 0,
                    "lag": 0,
                    "percent": 0.0,
                    "status": f"CHƯA KẾT NỐI ({str(ex)})"
                })

        return {
            "totalMessages": total_all_messages,
            "groups": groups_stats
        }

    except Exception as e:
        return {"error": str(e), "totalMessages": 0, "groups": []}
    finally:
        if consumer_client:
            try: consumer_client.close()
            except Exception: pass
        if admin:
            try: admin.close()
            except Exception: pass



# Dọn dẹp khi tắt webapp
@app.on_event("shutdown")
def shutdown_event():
    print("\n[WEB SHUTDOWN] Web portal da tat.")


# --- PHỤC VỤ TRANG GIAO DIỆN CHÍNH ---
@app.get("/")
def read_root():
    return FileResponse(os.path.join(BASE_DIR, "index.html"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

