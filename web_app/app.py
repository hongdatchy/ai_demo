import os
import sys
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
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
# from s3_helper import (
#     download_image_bytes
# )

# URL của Stream Coordinator & AI Service — đổi theo môi trường thực tế
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8200")
AI_SERVICE_URL = os.getenv("AI_SERVICE_URL", "http://localhost:3333")

PROCESSED_PATH = os.path.abspath(os.path.join(BASE_DIR, "../processed_faces"))
PROCESSED_FIRE_PATH = os.path.abspath(os.path.join(BASE_DIR, "../processed_fire"))
TEMP_FRAMES_PATH = os.getenv("TEMP_FRAMES_PATH", os.path.abspath(os.path.join(BASE_DIR, "../cloud_camera/temp_frames")))

# Đảm bảo các thư mục luôn tồn tại
os.makedirs(PROCESSED_PATH, exist_ok=True)
os.makedirs(PROCESSED_FIRE_PATH, exist_ok=True)
os.makedirs(TEMP_FRAMES_PATH, exist_ok=True)

app = FastAPI(title="Cloud Camera AI Manager Portal")

# Tự động kiểm tra và tạo lại các thư mục khi start webapp
@app.on_event("startup")
def startup_event():
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    os.makedirs(PROCESSED_FIRE_PATH, exist_ok=True)
    os.makedirs(TEMP_FRAMES_PATH, exist_ok=True)
    print(f"[STARTUP] Da kiem tra va tao day du cac thu muc:")
    print(f" - Temp Frames: {TEMP_FRAMES_PATH}")
    print(f" - Processed Faces: {PROCESSED_PATH}")
    print(f" - Processed Fire: {PROCESSED_FIRE_PATH}")

# Static mounts
app.mount("/static/processed", StaticFiles(directory=PROCESSED_PATH), name="processed_faces")
app.mount("/static/processed_fire", StaticFiles(directory=PROCESSED_FIRE_PATH), name="processed_fire")


# =====================================================================
# CẤU HÌNH MÔI TRƯỜNG BACKEND (SERVER GATEWAY / LOCALHOST:3333)
# =====================================================================

class BackendConfigRequest(BaseModel):
    url: str

@app.get("/api/config/backend")
def get_backend_config():
    return {
        "current": AI_SERVICE_URL,
        "options": [
            {"label": "URL hiện tại (27.71.24.102:8082)", "value": "http://27.71.24.102:8082/cloud-camera-microservice/ai"},
            {"label": "Localhost port 3333", "value": "http://localhost:3333"}
        ]
    }

@app.post("/api/config/backend")
def set_backend_config(req: BackendConfigRequest):
    global AI_SERVICE_URL
    AI_SERVICE_URL = req.url.rstrip("/")
    try:
        import s3_helper
        s3_helper.AI_SERVICE_URL = AI_SERVICE_URL
        s3_helper._s3_config = None
        s3_helper._s3_client = None
    except Exception as e:
        print(f"[BACKEND CONFIG] Canh bao reset s3_helper cache: {e}")
    print(f"[BACKEND CONFIG] Đã chuyển Backend sang: {AI_SERVICE_URL}")
    return {"message": "Cập nhật Backend thành công", "current": AI_SERVICE_URL}


# =====================================================================
# 1. QUẢN LÝ CÁC LUỒNG HLS STREAM (ADD / DELETE / LIST)
# =====================================================================

class StreamAddRequest(BaseModel):
    url: str
    task_types: list[str] | str | None = None
    task_type: str | None = "detect_face"
    camera_id: int | None = None


# @app.get("/api/s3/image")
# def get_s3_image(key: str):
#     """Serve ảnh trực tiếp từ S3 về browser để hiển thị trên web portal"""
#     try:
#         data = download_image_bytes(key)
#         return Response(content=data, media_type="image/jpeg")
#     except Exception as e:
#         raise HTTPException(status_code=404, detail=f"Không tải được ảnh từ S3: {e}")


@app.get("/api/streams")
def list_streams():
    """Lấy danh sách các luồng HLS đang chạy kèm thông tin node từ Coordinator"""
    try:
        resp = http.get(f"{COORDINATOR_URL}/streams/details", timeout=10)
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
                "task_type": ",".join(tasks) if isinstance(tasks, list) else str(tasks),
                "camera_id": req.camera_id
            },
            timeout=10
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        result = resp.json()
        stream_id = result.get("cameraId") or result.get("camera_id") or req.camera_id
        if result.get("status") == "ALREADY_RUNNING":
            raise HTTPException(status_code=400, detail=f"Luồng [{stream_id}] đã đang chạy")
        return {"message": f"Đã kích hoạt luồng: {stream_id}", "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Không kết nối được Coordinator: {e}")


@app.delete("/api/streams/{camera_id}")
def remove_stream(camera_id: str):
    """Dừng luồng HLS qua Coordinator"""
    try:
        if not camera_id or str(camera_id).lower() in ("undefined", "null", ""):
            raise HTTPException(status_code=400, detail="Mã camera_id không hợp lệ")

        resp = http.post(
            f"{COORDINATOR_URL}/stream/stop",
            json={"camera_id": camera_id},
            timeout=10
        )
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Không tìm thấy luồng này")
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return {"message": f"Đã ngắt luồng {camera_id} thành công"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Không kết nối được Coordinator: {e}")


# =====================================================================
# 3. XEM LẠI KẾT QUẢ DETECT (S3 VÀ LOCAL FALLBACK)
# =====================================================================

@app.get("/api/results")
def get_detect_results(camera_id: int = None, task_type: str = None, limit: int = 60):
    """Lấy danh sách các ảnh kết quả đã được AI xử lý từ Backend Java (AiEventLog kèm Presigned URL)"""
    results = []

    # Gọi vào AI Service Java lấy danh sách AiEventLog có sẵn Presigned URL chuẩn
    try:
        params = {"size": limit}
        if camera_id is not None:
            params["cameraId"] = camera_id
        if task_type and task_type.strip():
            params["taskType"] = task_type.strip()

        resp = http.get(f"{AI_SERVICE_URL}/api/public/ai-event-logs", params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            for item in data:
                t_type = item.get("taskType", "")
                if "fire" in t_type:
                    tag = "Cảnh báo Cháy"
                elif "vip" in t_type:
                    tag = "Khách VIP"
                elif "attendance" in t_type:
                    tag = "Chấm công"
                else:
                    tag = "Khuôn mặt"

                event_time_str = item.get("eventTime", "")
                confidence_str = f" ({item.get('confidence'):.2f})" if item.get('confidence') else ""
                results.append({
                    "fileName": f"Camera #{item.get('cameraId')} - {tag}{confidence_str}",
                    "url": item.get("imageUrl"),  # Link Presigned URL trực tiếp từ S3
                    "tag": tag,
                    "taskType": t_type,
                    "timestamp": 0,
                    "timeStr": str(event_time_str).replace("T", " ")[:19] if event_time_str else "",
                })
    except Exception as e:
        print(f"[AI SERVICE RESULTS] Lỗi khi gọi ai-event-logs: {e}")

    return {"results": results[:limit]}


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

