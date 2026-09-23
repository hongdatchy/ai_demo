"""
run_all.py
----------
Dieu phoi va khoi dong toan bo cac tien trinh Python trong 1 cua so duy nhat.
Khi nhan Ctrl + C, tu dong tat sach tat ca cac tien trinh con.
"""

import os
import sys
import time
import subprocess
import signal

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLOUD_CAM_DIR = os.path.join(BASE_DIR, "cloud_camera")
WEB_APP_DIR = os.path.join(BASE_DIR, "web_app")

# 1. Cau hinh moi truong mac dinh
DEFAULT_ENV = {
    "PYTHONUNBUFFERED": "1",
    "KAFKA_BOOTSTRAP_SERVERS": "27.71.24.102:9093",
    "KAFKA_USER": "admin",
    "KAFKA_PASSWORD": "Admin@123",
    "KAFKA_FACE_TOPIC": "ai_face_topic",
    "KAFKA_FIRE_TOPIC": "ai_fire_topic",
    "KAFKA_GROUP_ID_FACE": "face_recognition_group",
    "KAFKA_GROUP_ID_FIRE": "fire_detection_group",
    "REDIS_HOST": "27.71.24.102",
    "REDIS_PORT": "6379",
    "REDIS_PASSWORD": "",
    "COORDINATOR_PORT": "8200",
    "COORDINATOR_URL": "http://localhost:8200",
    "NODES": "http://localhost:8101,http://localhost:8102",
    "HEALTHCHECK_INTERVAL": "10",
    "HEALTHCHECK_TIMEOUT": "3",
    "EXTRACTOR_MAX_WORKERS": "50",
    "CAPTURE_INTERVAL": "5.0",
    "AI_FACE_MAX_WORKERS": "4",
    "AI_FIRE_MAX_WORKERS": "4",
    "AI_SERVICE_URL": "http://27.71.24.102:8082/cloud-camera-microservice/ai",
}

# 2. Danh sach cac service can chay
SERVICES = [
    {
        "name": "Node 1 - Extractor (8101)",
        "cmd": [sys.executable, "frame_extractor_node.py", "--port", "8101"],
        "cwd": CLOUD_CAM_DIR,
        "env": {"NODE_ID": "node_1", "PORT": "8101"},
    },
    {
        "name": "Node 2 - Extractor (8102)",
        "cmd": [sys.executable, "frame_extractor_node.py", "--port", "8102"],
        "cwd": CLOUD_CAM_DIR,
        "env": {"NODE_ID": "node_2", "PORT": "8102"},
    },
    {
        "name": "Stream Coordinator (8200)",
        "cmd": [sys.executable, "stream_coordinator.py"],
        "cwd": CLOUD_CAM_DIR,
        "env": {},
        "delay_before": 2,
    },
    {
        "name": "Consumer - Phat hien Chay",
        "cmd": [sys.executable, "ai_fire_consumer.py"],
        "cwd": CLOUD_CAM_DIR,
        "env": {},
    },
    {
        "name": "Consumer - Nhan dien Mat",
        "cmd": [sys.executable, "ai_face_consumer.py"],
        "cwd": CLOUD_CAM_DIR,
        "env": {},
    },
    {
        "name": "Web Portal (8008)",
        "cmd": [sys.executable, "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8008"],
        "cwd": WEB_APP_DIR,
        "env": {},
        "delay_before": 1,
    },
]

running_processes = []


def kill_proc_tree(proc):
    """Diet sach tien trinh con va cay tien trinh tren Windows."""
    try:
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False
            )
        else:
            proc.terminate()
            proc.wait(timeout=2)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def cleanup():
    """Don dep tat ca cac tien trinh khi nhan Ctrl + C hoac thoat chuong trinh."""
    print("\n[RUN_ALL] Dang dung toan bo cac tien trinh Python...")
    for name, proc in running_processes:
        if proc.poll() is None:
            print(f"  -> Dung: {name} (PID: {proc.pid})")
            kill_proc_tree(proc)
    print("[RUN_ALL] Da dung tat ca cac service thanh cong. Tam biet!\n")


def main():
    print("=" * 70)
    print("      KHOI DONG TOAN BO HE THONG CLOUD CAMERA AI (1 CUA SO)")
    print("          Nhan Ctrl + C bat ky luc nao de tat tat ca service")
    print("=" * 70)

    # Dang ky signal
    def sig_handler(sig, frame):
        cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, sig_handler)

    try:
        for idx, svc in enumerate(SERVICES, 1):
            if svc.get("delay_before"):
                time.sleep(svc["delay_before"])

            env = os.environ.copy()
            env.update(DEFAULT_ENV)
            env.update(svc.get("env", {}))

            print(f"[{idx}/{len(SERVICES)}] Khoi dong {svc['name']}...")
            proc = subprocess.Popen(
                svc["cmd"],
                cwd=svc["cwd"],
                env=env,
                shell=False
            )
            running_processes.append((svc["name"], proc))

        print("\n" + "=" * 70)
        print("  Tat ca 6 tien trinh da khoi dong thanh cong!")
        print("  Web Portal   : http://localhost:8008")
        print("  Coordinator  : http://localhost:8200")
        print("  Extractor 1  : http://localhost:8101 | Extractor 2: http://localhost:8102")
        print("  >> BAM CTRL + C DE DUNG VA THOAT TOAN BO <<")
        print("=" * 70 + "\n")

        # Cho cac tien trinh chay va giam sat
        while True:
            for name, proc in running_processes:
                ret = proc.poll()
                if ret is not None:
                    print(f"\n[CANH BAO] {name} da thoat voi ma code: {ret}")
            time.sleep(2)

    except KeyboardInterrupt:
        cleanup()
    except Exception as e:
        print(f"\n[LOI] {e}")
        cleanup()


if __name__ == "__main__":
    main()
