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

# =====================================================================
# CẤU HÌNH KAFKA LOCAL (LẤY TỪ CAMERA SERVICE APPLICATION-LOCAL.YML)
# =====================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '27.71.24.102:9093')
KAFKA_USER = os.getenv('KAFKA_USER', 'admin')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD', 'Admin@123')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'ai_frame_topic')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "temp_frames")
os.makedirs(OUTPUT_DIR, exist_ok=True)

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
except Exception as e:
    producer = None
    print(f"[KAFKA CANH BAO] Khong ket noi duoc Kafka ({e}). Se fallback in log.")

# =====================================================================
# HÀM BÓC TÁCH CLOUD_ID TỪ URL STREAM HLS
# =====================================================================
def extract_cloud_id(url):
    match = re.search(r'/([^/]+)\.stream', url)
    if match:
        return match.group(1)
    
    parts = url.rstrip('/').split('/')
    for p in reversed(parts):
        if p and not p.endswith('.m3u8'):
            return p.replace('.stream', '')
            
    return f"cam_{int(time.time())}"


# =====================================================================
# QUẢN LÝ ĐA LUỒNG BẰNG ThreadPoolExecutor
# =====================================================================
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 50))
executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

active_streams = {}
lock = threading.Lock()


def send_to_kafka(message):
    if producer:
        producer.send(KAFKA_TOPIC, value=message)
        print(f"[KAFKA SENT -> {KAFKA_TOPIC}]: {message}")
    else:
        print(f"[MO PHONG KAFKA]: {message}")


def capture_vidgear_worker(cloud_id, stream_url, task_type, stop_event):
    print(f"[{cloud_id}] Khoi dong luong stream: {stream_url} (Task: {task_type})")

    try:
        stream = CamGear(source=stream_url, stream_mode=False, logging=False).start()
    except Exception as e:
        print(f"[{cloud_id}] Loi ket noi stream: {e}")
        return

    last_capture_time = 0

    while not stop_event.is_set():
        frame = stream.read()
        if frame is None:
            time.sleep(0.5)
            continue

        current_time = time.time()

        if current_time - last_capture_time >= 1.0:
            last_capture_time = current_time
            timestamp = int(current_time * 1000)

            file_name = f"{cloud_id}_{timestamp}.jpg"
            file_path = os.path.join(OUTPUT_DIR, file_name)
            cv2.imwrite(file_path, frame)

            message = {
                "cloudId": cloud_id,
                "taskType": task_type,
                "path": os.path.abspath(file_path)
            }
            send_to_kafka(message)

    stream.stop()
    print(f"[{cloud_id}] Da ngat stream va giai phong tai nguyen.")


def start_stream(stream_url, task_type="detect_face"):
    cloud_id = extract_cloud_id(stream_url)

    with lock:
        if cloud_id in active_streams:
            return {"status": "ALREADY_RUNNING", "cloudId": cloud_id, "url": stream_url}

        stop_event = threading.Event()
        future = executor.submit(capture_vidgear_worker, cloud_id, stream_url, task_type, stop_event)

        active_streams[cloud_id] = {
            "event": stop_event,
            "future": future,
            "url": stream_url,
            "task_type": task_type,
            "start_time": time.time()
        }
        print(f"[{cloud_id}] => Da kich hoat luong stream thanh cong!")
        return {"status": "SUCCESS", "cloudId": cloud_id, "url": stream_url, "taskType": task_type}


def stop_stream(cloud_id):
    with lock:
        if cloud_id not in active_streams:
            return {"status": "NOT_FOUND", "cloudId": cloud_id}

        active_streams[cloud_id]["event"].set()
        del active_streams[cloud_id]
        print(f"[{cloud_id}] => Da dung luong stream.")
        return {"status": "SUCCESS", "cloudId": cloud_id}


def stop_all_streams():
    """Dừng toàn bộ tất cả các luồng camera đang chạy và đóng ThreadPool"""
    with lock:
        print("[SHUTDOWN] Dang dung toan bo cac luong stream camera...")
        for cloud_id, item in list(active_streams.items()):
            item["event"].set()
        active_streams.clear()
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
        print("[SHUTDOWN] Da tat sach tat ca cac luong.")


def get_all_streams():
    with lock:
        streams = []
        for c_id, item in active_streams.items():
            streams.append({
                "cloudId": c_id,
                "url": item["url"],
                "taskType": item["task_type"],
                "uptime": int(time.time() - item.get("start_time", time.time()))
            })
        return streams
