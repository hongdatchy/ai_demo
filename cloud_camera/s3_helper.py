"""
s3_helper.py
------------
Tiện ích upload/download ảnh lên S3, dùng chung cho tất cả AI consumer và web app.
Config S3 được lấy tự động từ ai service Java qua GET /api/public/s3-config.
"""

import os
import io
import json
import time
import requests
import boto3
from botocore.client import Config

# URL của ai service Java (lấy từ env, mặc định localhost:3333)
AI_SERVICE_URL = os.getenv("AI_SERVICE_URL", "http://localhost:3333")

_s3_config = None  # Cache config sau lần đầu lấy
_s3_client = None


def _fetch_s3_config(retry=5, delay=5):
    """Gọi GET /api/public/s3-config, retry nếu service chưa sẵn sàng."""
    url = f"{AI_SERVICE_URL}/api/public/s3-config"
    for attempt in range(1, retry + 1):
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                body = resp.json()
                data = body.get("data") or body
                print(f"[S3] Lay config S3 thanh cong tu {url}")
                return data
            else:
                print(f"[S3] GET {url} tra ve {resp.status_code}: {resp.text}")
        except Exception as e:
            print(f"[S3] Lan thu {attempt}/{retry} - Khong ket noi duoc ai service ({e})")
        time.sleep(delay)
    raise RuntimeError(f"Khong lay duoc S3 config tu {url} sau {retry} lan thu")


def get_s3_config():
    global _s3_config
    if _s3_config is None:
        _s3_config = _fetch_s3_config()
    return _s3_config


def get_s3_client():
    global _s3_client
    if _s3_client is None:
        cfg = get_s3_config()
        _s3_client = boto3.client(
            "s3",
            endpoint_url=cfg["endpoint"],
            aws_access_key_id=cfg["accessKey"],
            aws_secret_access_key=cfg["secretKey"],
            region_name=cfg.get("region", "us-east-1"),
            config=Config(signature_version="s3v4")
        )
    return _s3_client


def get_crop_bucket():
    return get_s3_config().get("cropBucketName", "cloudcamera-crop")


def get_result_bucket():
    return get_s3_config().get("resultBucketName", "cloudcamera-result")


def get_face_bucket():
    return get_s3_config().get("faceBucketName", "cloudcamera-faces")


def upload_image_bytes(image_bytes: bytes, s3_key: str, bucket: str = None) -> str:
    """Upload bytes ảnh lên S3 qua boto3. Mặc định vào result bucket nếu không truyền bucket."""
    s3 = get_s3_client()
    target_bucket = bucket or get_result_bucket()
    s3.put_object(
        Bucket=target_bucket,
        Key=s3_key,
        Body=image_bytes,
        ContentType="image/jpeg"
    )
    return s3_key


def download_image_bytes(s3_key: str, bucket: str = None) -> bytes:
    """Download ảnh từ S3 qua boto3, trả về bytes. Mặc định từ result bucket nếu không truyền bucket."""
    s3 = get_s3_client()
    target_bucket = bucket or get_result_bucket()
    resp = s3.get_object(Bucket=target_bucket, Key=s3_key)
    return resp["Body"].read()


def upload_crop_image(image_bytes: bytes, s3_key: str) -> str:
    """Upload frame cắt vào bucket cloudcamera-crop."""
    return upload_image_bytes(image_bytes, s3_key, bucket=get_crop_bucket())


def download_crop_image(s3_key: str) -> bytes:
    """Download frame cắt từ bucket cloudcamera-crop."""
    return download_image_bytes(s3_key, bucket=get_crop_bucket())


def download_face_image(s3_key: str) -> bytes:
    """Download ảnh khuôn mặt mẫu từ bucket cloudcamera-faces."""
    return download_image_bytes(s3_key, bucket=get_face_bucket())


def fetch_active_faces(account_id: int = None) -> list:
    """Gọi GET /api/public/faces để lấy danh sách khuôn mặt ACTIVE và danh sách S3 imageKeys."""
    url = f"{AI_SERVICE_URL}/api/public/faces"
    params = {}
    if account_id is not None:
        params["accountId"] = account_id
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            body = resp.json()
            return body.get("data", [])
        else:
            print(f"[FACE_API] GET {url} trả về {resp.status_code}: {resp.text}")
            return []
    except Exception as e:
        print(f"[FACE_API] Lỗi gọi {url}: {e}")
        return []


def list_s3_objects(prefix=""):
    """Lấy danh sách object trong bucket theo prefix."""
    s3 = get_s3_client()
    bucket = get_result_bucket()
    paginator = s3.get_paginator("list_objects_v2")
    items = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            items.append(obj)
    return items


def delete_s3_key(s3_key: str):
    """Xóa một object trên S3."""
    s3 = get_s3_client()
    bucket = get_result_bucket()
    s3.delete_object(Bucket=bucket, Key=s3_key)


import threading

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '27.71.24.102:9093')
KAFKA_USER = os.getenv('KAFKA_USER', 'admin')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD', 'Admin@123')
KAFKA_AI_EVENT_TOPIC = os.getenv('KAFKA_AI_EVENT_TOPIC', 'ai_event_topic')

_kafka_event_producer = None
_kafka_producer_lock = threading.Lock()


def get_event_producer():
    global _kafka_event_producer
    if _kafka_event_producer is None:
        with _kafka_producer_lock:
            if _kafka_event_producer is None:
                try:
                    from kafka import KafkaProducer
                    _kafka_event_producer = KafkaProducer(
                        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                        security_protocol="SASL_PLAINTEXT",
                        sasl_mechanism="PLAIN",
                        sasl_plain_username=KAFKA_USER,
                        sasl_plain_password=KAFKA_PASSWORD,
                        value_serializer=lambda v: json.dumps(v).encode('utf-8')
                    )
                    print(f"[KAFKA] Kết nối Kafka Event Producer thành công: topic={KAFKA_AI_EVENT_TOPIC}")
                except Exception as e:
                    print(f"[KAFKA LỖI] Không khởi tạo được Kafka Event Producer ({e})")
                    _kafka_event_producer = None
    return _kafka_event_producer


def save_ai_event_log(camera_id: int, task_type: str, image_url: str,
                      confidence: float = None, face_id: int = None, metadata: dict = None):
    """Bắn kết quả AI vào Kafka topic ai_event_topic."""
    payload = {
        "cameraId": camera_id,
        "taskType": task_type,
        "imageUrl": image_url,
        "confidence": confidence,
        "faceId": face_id,
        "timestamp": int(time.time() * 1000),
        "metadata": metadata if metadata else {},
    }
    producer = get_event_producer()
    if producer:
        try:
            producer.send(KAFKA_AI_EVENT_TOPIC, payload)
            print(f"[KAFKA EVENT] Đã bắn sự kiện AI vào '{KAFKA_AI_EVENT_TOPIC}': camera={camera_id}, task={task_type}, faceId={face_id}")
            return
        except Exception as e:
            print(f"[KAFKA EVENT LỖI] Lỗi bắn Kafka ({e})")
    else:
        print(f"[KAFKA EVENT LỖI] Kafka Event Producer chưa sẵn sàng, bỏ qua gửi sự kiện camera={camera_id}")
