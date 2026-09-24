import sys
try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

import time
import json
import os
import re
import cv2
import threading
from concurrent.futures import ThreadPoolExecutor
from vidgear.gears import CamGear
from kafka import KafkaProducer
from s3_helper import upload_crop_image, get_s3_config

# =====================================================================
# CẤU HÌNH KAFKA (TÁCH RIÊNG TOPIC CHO TỪNG BÀI TOÁN)
# =====================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '27.71.24.102:9093')
KAFKA_USER = os.getenv('KAFKA_USER', 'admin')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD', 'Admin@123')

KAFKA_FACE_TOPIC = os.getenv('KAFKA_FACE_TOPIC', 'ai_face_topic')
KAFKA_FIRE_TOPIC = os.getenv('KAFKA_FIRE_TOPIC', 'ai_fire_topic')

TASK_TOPIC_MAP = {
    "detect_face": KAFKA_FACE_TOPIC,
    "detect_fire": KAFKA_FIRE_TOPIC,
}



try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=KAFKA_USER,
        sasl_plain_password=KAFKA_PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"[KAFKA] Ket noi thanh cong toi Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[KAFKA] Topics da cau hinh: Face -> {KAFKA_FACE_TOPIC}, Fire -> {KAFKA_FIRE_TOPIC}")
except Exception as e:
    producer = None
    print(f"[KAFKA CANH BAO] Khong ket noi duoc Kafka ({e}). Se fallback in log.")

# =====================================================================
# HÀM XÁC ĐỊNH CAMERA_ID CHO LUỒNG STREAM HLS
# =====================================================================
def resolve_camera_id(url, camera_id=None):
    if camera_id is not None and str(camera_id).strip():
        return str(camera_id).strip()
    match = re.search(r'/([^/]+)\.stream', url)
    if match:
        return match.group(1)
    
    parts = url.rstrip('/').split('/')
    for p in reversed(parts):
        if p and not p.endswith('.m3u8'):
            return p.replace('.stream', '')
            
    return f"cam_{int(time.time())}"


def normalize_tasks(task_types):
    """Chuẩn hóa danh sách bài toán (hỗ trợ cả list lẫn chuỗi phân tách bằng dấu phẩy)"""
    if not task_types:
        return ["detect_face"]
    if isinstance(task_types, list):
        return [t.strip() for t in task_types if t and str(t).strip()]
    if isinstance(task_types, str):
        return [t.strip() for t in task_types.split(",") if t.strip()]
    return ["detect_face"]


# =====================================================================
# QUẢN LÝ ĐA LUỒNG BẰNG ThreadPoolExecutor
# =====================================================================
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 50))
CAPTURE_INTERVAL = float(os.getenv('CAPTURE_INTERVAL', 5.0))  # Cắt ảnh mỗi 5 giây
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

active_streams = {}
lock = threading.Lock()





def send_to_kafka(topic, message):
    if producer:
        producer.send(topic, value=message)
        print(f"[KAFKA SENT -> {topic}]: {message}")
    else:
        print(f"[MO PHONG KAFKA -> {topic}]: {message}")


def capture_vidgear_worker(camera_id, stream_url, task_types, stop_event):
    tasks = normalize_tasks(task_types)
    print(f"[{camera_id}] Bat dau tien trinh giam sat luong: {stream_url} (Tasks: {tasks})")

    last_capture_time = 0

    while not stop_event.is_set():
        stream = None
        # Vòng lặp kết nối CamGear (chờ cho tới khi luồng sống hoặc người dùng stop)
        try:
            stream = CamGear(source=stream_url, stream_mode=False, logging=False).start()
            print(f"[{camera_id}] Ket noi thanh cong toi stream {stream_url}! Bat dau cat frame.")
        except Exception as e:
            print(f"[{camera_id}] Luong chua san sang hoac offline ({e}). Se tu dong thu lai sau 5s...")
            for _ in range(10):
                if stop_event.is_set():
                    print(f"[{camera_id}] Da huy giam sat do nhan lenh dung.")
                    return
                time.sleep(0.5)
            continue

        consecutive_none = 0
        try:
            while not stop_event.is_set():
                frame = stream.read()
                if frame is None:
                    consecutive_none += 1
                    # Mất frame liên tục quá 10s (20 lần x 0.5s) -> coi như luồng đứt, giải phóng và reconnect
                    if consecutive_none >= 20:
                        print(f"[{camera_id}] Mat tin hieu stream qua 10s! Dang ngat ket noi de thu lai...")
                        break
                    time.sleep(0.5)
                    continue

                if consecutive_none > 0:
                    print(f"[{camera_id}] Da co lai tin hieu video binh thuong.")
                    consecutive_none = 0

                current_time = time.time()
                if current_time - last_capture_time >= CAPTURE_INTERVAL:
                    last_capture_time = current_time
                    timestamp = int(current_time * 1000)

                    # Encode frame thành bytes JPEG (không ghi file local)
                    success, buf = cv2.imencode(".jpg", frame)
                    if not success:
                        continue
                    image_bytes = buf.tobytes()

                    # Upload lên S3 bucket cloudcamera-crop: {cameraId}/{timestamp}.jpg
                    cam_id_str = str(camera_id).strip()
                    cam_id_val = int(cam_id_str) if cam_id_str.isdigit() else cam_id_str
                    s3_key = f"{cam_id_val}/{timestamp}.jpg"
                    try:
                        upload_crop_image(image_bytes, s3_key)
                    except Exception as e:
                        print(f"[{camera_id}] Lỗi upload S3: {e}")
                        continue

                    # Bắn frame vào đúng từng topic của bài toán đã đăng ký
                    with lock:
                        stream_info = active_streams.get(cam_id_str) or active_streams.get(camera_id)
                        current_tasks = list(stream_info.get("task_types", tasks)) if stream_info else list(tasks)

                    for task in current_tasks:
                        topic = TASK_TOPIC_MAP.get(task, f"ai_{task}_topic")
                        message = {
                            "cameraId": cam_id_val,
                            "taskType": task,
                            "s3Key": s3_key,
                        }
                        send_to_kafka(topic, message)
        except Exception as e:
            print(f"[{camera_id}] Loi trong qua trinh doc frame: {e}")
        finally:
            if stream:
                try:
                    stream.stop()
                except Exception:
                    pass

        # Nếu chưa bị dừng bởi người dùng, đợi 3s rồi vòng lặp ngoài sẽ reconnect lại
        if not stop_event.is_set():
            for _ in range(6):
                if stop_event.is_set():
                    break
                time.sleep(0.5)

    print(f"[{camera_id}] Da ngat stream va giai phong tai nguyen.")


def start_stream(stream_url, task_types="detect_face", camera_id=None):
    cam_id = resolve_camera_id(stream_url, camera_id)
    new_tasks = normalize_tasks(task_types)

    with lock:
        if cam_id in active_streams:
            current_tasks = active_streams[cam_id].get("task_types", [])
            merged_tasks = list(dict.fromkeys(current_tasks + new_tasks))
            active_streams[cam_id]["task_types"] = merged_tasks
            active_streams[cam_id]["task_type"] = ",".join(merged_tasks)
            print(f"[{cam_id}] => Đã bổ sung bài toán vào luồng đang chạy: {merged_tasks}")
            return {
                "status": "SUCCESS",
                "cameraId": cam_id,
                "camera_id": cam_id,
                "url": active_streams[cam_id].get("url", stream_url),
                "taskTypes": merged_tasks,
                "taskType": ",".join(merged_tasks)
            }

        stop_event = threading.Event()
        future = executor.submit(capture_vidgear_worker, cam_id, stream_url, new_tasks, stop_event)

        active_streams[cam_id] = {
            "event": stop_event,
            "future": future,
            "url": stream_url,
            "task_types": new_tasks,
            "task_type": ",".join(new_tasks),
            "camera_id": cam_id,
            "start_time": time.time()
        }
        print(f"[{cam_id}] => Da kich hoat luong stream thanh cong! (Tasks: {new_tasks}, cameraId: {cam_id})")
        return {
            "status": "SUCCESS",
            "cameraId": cam_id,
            "camera_id": cam_id,
            "url": stream_url,
            "taskTypes": new_tasks,
            "taskType": ",".join(new_tasks)
        }


def stop_stream(camera_id, task_type=None):
    cam_id = str(camera_id).strip()
    with lock:
        if cam_id not in active_streams:
            return {"status": "NOT_FOUND", "cameraId": cam_id, "camera_id": cam_id, "remaining_tasks": []}

        # Nếu có chỉ định task_type cụ thể cần hủy (không phải 'all' hoặc rỗng)
        if task_type and str(task_type).strip().lower() not in ("all", "none", "", "null", "undefined"):
            t_to_remove = str(task_type).strip()
            current_tasks = active_streams[cam_id].get("task_types", [])
            remaining_tasks = [t for t in current_tasks if t != t_to_remove]

            if remaining_tasks:
                active_streams[cam_id]["task_types"] = remaining_tasks
                active_streams[cam_id]["task_type"] = ",".join(remaining_tasks)
                print(f"[{cam_id}] => Đã gỡ bài toán '{t_to_remove}', các bài toán còn lại: {remaining_tasks}")
                return {
                    "status": "SUCCESS",
                    "cameraId": cam_id,
                    "camera_id": cam_id,
                    "remaining_tasks": remaining_tasks,
                    "taskTypes": remaining_tasks,
                    "taskType": ",".join(remaining_tasks),
                    "action": "TASK_REMOVED"
                }

        # Nếu không còn bài toán nào hoặc lệnh yêu cầu ngắt toàn bộ luồng
        active_streams[cam_id]["event"].set()
        del active_streams[cam_id]
        print(f"[{cam_id}] => Đã ngắt hoàn toàn luồng stream (không còn bài toán nào).")
        return {
            "status": "SUCCESS",
            "cameraId": cam_id,
            "camera_id": cam_id,
            "remaining_tasks": [],
            "action": "STREAM_STOPPED"
        }


def get_all_streams():
    with lock:
        streams = []
        for cam_id, item in active_streams.items():
            tasks = item.get("task_types") or [item.get("task_type", "detect_face")]
            streams.append({
                "cameraId": cam_id,
                "camera_id": cam_id,
                "url": item["url"],
                "taskTypes": tasks,
                "taskType": ",".join(tasks) if isinstance(tasks, list) else str(tasks),
                "uptime": int(time.time() - item.get("start_time", time.time()))
            })
        return streams

