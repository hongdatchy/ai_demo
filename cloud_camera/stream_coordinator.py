"""
stream_coordinator.py
---------------------
Dieu phoi cac Frame Extractor Node:
  1. Healthcheck dinh ky (10s/lan) - phat hien node chet
  2. Failover - chuyen luong tu node chet sang node song
  3. Load balancing - them luong moi vao node it tai nhat
  4. HTTP API - app.py goi vao day thay vi goi thang extractor

Yeu cau: pip install redis fastapi uvicorn requests

Chay: python stream_coordinator.py

Config nodes qua bien moi truong:
  NODES=http://node1:8100,http://node2:8100,http://node3:8100
"""

import os
import time
import threading
import requests
import redis
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ─────────────────── Config ───────────────────

NODES = os.getenv("NODES", "http://localhost:8101,http://localhost:8102").split(",")
REDIS_HOST = os.getenv("REDIS_HOST", "27.71.24.102")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
HEALTHCHECK_INTERVAL = int(os.getenv("HEALTHCHECK_INTERVAL", 10))  # giay
HEALTHCHECK_TIMEOUT = int(os.getenv("HEALTHCHECK_TIMEOUT", 3))    # giay
PORT = int(os.getenv("PORT", 8200))

try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_timeout=3)
    r.ping()
    print(f"[REDIS] Ket noi thanh cong toi Redis: {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    print(f"[REDIS CANH BAO] Khong ket noi duoc Redis ({e}). Coordinator van hoat dong qua HTTP truc tiep.")
    r = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    t = threading.Thread(target=healthcheck_loop, daemon=True)
    t.start()
    print(f"[COORDINATOR] Healthcheck loop started. Monitoring {len(NODES)} nodes.")
    print(f"[COORDINATOR] Nodes: {NODES}")

    # Đợi 2 giây cho các node phản hồi ping, sau đó tự động khôi phục toàn bộ luồng cũ từ Redis
    def delayed_startup_recovery():
        time.sleep(2)
        reconcile_and_recover()

    threading.Thread(target=delayed_startup_recovery, daemon=True).start()
    yield


app = FastAPI(title="Stream Coordinator", lifespan=lifespan)



# ─────────────────── Schema ───────────────────

class StartRequest(BaseModel):
    url: str
    task_types: list[str] | str | None = None
    task_type: str | None = "detect_face"
    camera_id: int | str | None = None

class StopRequest(BaseModel):
    camera_id: int | str
    task_type: str | None = None


# ─────────────────── Redis helpers ────────────

def redis_add_stream(node_url: str, camera_id: str, url: str, task_type):
    if not r:
        return
    cam_id = str(camera_id).strip()
    if isinstance(task_type, list):
        task_str = ",".join(task_type)
    else:
        task_str = str(task_type)
    try:
        r.set(f"cam:url:{cam_id}", url)
        r.set(f"cam:task:{cam_id}", task_str)
        r.set(f"cam:node:{cam_id}", node_url)
        r.sadd("registered_cams", cam_id)
        if not r.exists(f"cam:start_time:{cam_id}"):
            r.set(f"cam:start_time:{cam_id}", str(int(time.time())))
        r.sadd(f"node:streams:{node_url}", cam_id)
    except Exception as e:
        print(f"[REDIS ERROR] redis_add_stream: {e}")

def redis_remove_stream(node_url: str, camera_id: str):
    if not r:
        return
    cam_id = str(camera_id).strip()
    try:
        r.delete(f"cam:url:{cam_id}")
        r.delete(f"cam:task:{cam_id}")
        r.delete(f"cam:node:{cam_id}")
        r.delete(f"cam:start_time:{cam_id}")
        r.srem("registered_cams", cam_id)
        r.srem(f"node:streams:{node_url}", cam_id)
    except Exception as e:
        print(f"[REDIS ERROR] redis_remove_stream: {e}")


# ─────────────────── Node Cache & Helpers ───────

node_cache = {
    node: {
        "status": "alive",
        "load": 0,
        "streams": [],
    }
    for node in NODES
}
cache_lock = threading.Lock()


def check_and_update_node(node: str) -> bool:
    """Kiểm tra health của 1 node và cập nhật cache trong RAM."""
    try:
        resp = requests.get(f"{node}/health", timeout=(1.0, 1.0))
        if resp.status_code == 200:
            data = resp.json()
            load = data.get("load", 0)
            streams = []
            try:
                s_resp = requests.get(f"{node}/stream/list", timeout=(1.0, 1.0))
                if s_resp.status_code == 200:
                    streams = s_resp.json()
            except Exception:
                pass

            with cache_lock:
                node_cache[node]["status"] = "alive"
                node_cache[node]["load"] = load
                node_cache[node]["streams"] = streams
            return True
    except Exception:
        pass

    with cache_lock:
        node_cache[node]["status"] = "dead"
        node_cache[node]["streams"] = []
        node_cache[node]["load"] = 0
    return False


def get_cached_alive_nodes():
    """Lấy danh sách node đang sống trực tiếp từ cache trong RAM (0ms)."""
    with cache_lock:
        return [
            (node, info["load"])
            for node, info in node_cache.items()
            if info["status"] == "alive"
        ]


def get_cached_streams():
    """Lấy danh sách toàn bộ stream đang chạy trên các node còn sống (0ms)."""
    result = []
    with cache_lock:
        for node, info in node_cache.items():
            if info["status"] == "alive":
                for s in info.get("streams", []):
                    tasks = s.get("taskTypes") or [s.get("taskType", "detect_face")]
                    if isinstance(tasks, str):
                        tasks = [t.strip() for t in tasks.split(",") if t.strip()]
                    cam_id = s.get("cameraId") or s.get("camera_id")
                    if not cam_id and s.get("url"):
                        parts = s.get("url").rstrip("/").split("/")
                        for p in reversed(parts):
                            if p and not p.endswith(".m3u8"):
                                cam_id = p.replace(".stream", "")
                                break
                    result.append({
                        "cameraId": cam_id or "unknown",
                        "camera_id": cam_id or "unknown",
                        "url": s.get("url"),
                        "taskTypes": tasks,
                        "taskType": ",".join(tasks),
                        "node": node,
                        "uptime": s.get("uptime", 0)
                    })
    return result


def get_min_load_node(alive_nodes):
    """Lấy node ít tải nhất."""
    if not alive_nodes:
        return None
    return min(alive_nodes, key=lambda x: x[1])[0]


# ─────────────────── Failover ─────────────────

def failover(dead_node: str):
    """Chuyển toàn bộ luồng camera từ dead_node sang các node sống."""
    camera_ids = r.smembers(f"node:streams:{dead_node}") if r else set()
    if not camera_ids:
        print(f"[FAILOVER] Node {dead_node} chet nhung khong co luong nao trong Redis de chuyen.")
        return

    print(f"[FAILOVER] Node {dead_node} chet! Dang chuyen {len(camera_ids)} luong...")

    for cam_id in camera_ids:
        url = r.get(f"cam:url:{cam_id}") if r else None
        t_str = r.get(f"cam:task:{cam_id}") if r else "detect_face"
        if not url:
            continue

        tasks = [t.strip() for t in t_str.split(",") if t.strip()]

        # Lấy alive nodes trực tiếp từ cache RAM (0ms, không chờ timeout của dead_node)
        alive_nodes = [n for n in get_cached_alive_nodes() if n[0] != dead_node]
        target = get_min_load_node(alive_nodes)
        if not target:
            print(f"[FAILOVER] Khong con node nao song! Bo qua camera {cam_id}")
            continue

        try:
            resp = requests.post(
                f"{target}/stream/start",
                json={
                    "url": url,
                    "task_types": tasks,
                    "task_type": ",".join(tasks),
                    "camera_id": cam_id
                },
                timeout=5
            )
            if resp.status_code == 200:
                if r:
                    r.srem(f"node:streams:{dead_node}", cam_id)
                redis_add_stream(target, cam_id, url, tasks)
                print(f"[FAILOVER] Chuyen camera {cam_id}: {dead_node} -> {target}")
                check_and_update_node(target)
            else:
                print(f"[FAILOVER] Loi khi chuyen camera {cam_id} sang {target}: {resp.text}")
        except Exception as e:
            print(f"[FAILOVER] Exception khi chuyen camera {cam_id}: {e}")

    if r:
        r.delete(f"node:streams:{dead_node}")


# ─────────────────── Reconcile & Recovery ─────

def reconcile_and_recover():
    """
    Tự động đối soát và phục hồi toàn bộ luồng camera từ Redis (Cách B):
    Nếu camera có trong danh sách đăng ký mà dưới các Node chưa chạy -> tự động bật lại!
    """
    if not r:
        return

    try:
        registered = r.smembers("registered_cams")
        # Fallback: Quét các key cam:url:* nếu registered_cams chưa có
        if not registered:
            keys = r.keys("cam:url:*")
            if keys:
                registered = {k.replace("cam:url:", "") for k in keys}
                for c in registered:
                    r.sadd("registered_cams", c)

        if not registered:
            return

        # Danh sách camera đang thực sự chạy trên các node sống
        running_cams = set()
        with cache_lock:
            for node, info in node_cache.items():
                if info["status"] == "alive":
                    for s in info.get("streams", []):
                        cid = s.get("cameraId") or s.get("camera_id")
                        if cid:
                            running_cams.add(str(cid))

        missing_cams = registered - running_cams
        if not missing_cams:
            return

        print(f"[AUTO-RECOVERY] Phat hien {len(missing_cams)} camera can phuc hoi: {list(missing_cams)}")

        for cam_id in missing_cams:
            url = r.get(f"cam:url:{cam_id}")
            t_str = r.get(f"cam:task:{cam_id}") or "detect_face"
            if not url:
                continue

            tasks = [t.strip() for t in t_str.split(",") if t.strip()]

            alive_nodes = get_cached_alive_nodes()
            target = get_min_load_node(alive_nodes)
            if not target:
                print(f"[AUTO-RECOVERY] Khong co node nao online de gan camera {cam_id}. Se thu lai sau.")
                break

            try:
                resp = requests.post(
                    f"{target}/stream/start",
                    json={
                        "url": url,
                        "task_types": tasks,
                        "task_type": ",".join(tasks),
                        "camera_id": cam_id
                    },
                    timeout=5
                )
                if resp.status_code == 200:
                    redis_add_stream(target, cam_id, url, tasks)
                    print(f"[AUTO-RECOVERY] => Da tu dong bat lai camera [{cam_id}] tren {target} thanh cong!")
                    check_and_update_node(target)
                else:
                    print(f"[AUTO-RECOVERY] Node {target} loi khi bat camera {cam_id}: {resp.text}")
            except Exception as e:
                print(f"[AUTO-RECOVERY] Loi ket noi toi {target} khi bat camera {cam_id}: {e}")

    except Exception as e:
        print(f"[AUTO-RECOVERY ERROR] {e}")


# ─────────────────── Healthcheck loop ─────────

def healthcheck_loop():
    # Quét lần đầu khi khởi động
    for node in NODES:
        check_and_update_node(node)

    iteration = 0
    while True:
        time.sleep(HEALTHCHECK_INTERVAL)
        iteration += 1
        for node in NODES:
            was_alive = (node_cache[node]["status"] == "alive")
            is_alive = check_and_update_node(node)

            if is_alive and not was_alive:
                print(f"[HEALTHCHECK] Node {node} phuc hoi thanh cong.")
                threading.Thread(target=reconcile_and_recover, daemon=True).start()
            elif not is_alive and was_alive:
                print(f"[HEALTHCHECK] Node {node} CHET -> bat dau failover...")
                threading.Thread(target=failover, args=(node,), daemon=True).start()

        # Định kỳ mỗi 30s đối soát 1 lần để đảm bảo không camera nào bị bỏ sót
        if iteration % 3 == 0:
            threading.Thread(target=reconcile_and_recover, daemon=True).start()


# ─────────────────── API Endpoints ────────────

@app.post("/stream/start")
def start_stream(req: StartRequest):
    """
    App.py hoặc backend Java gọi endpoint này để bắt đầu 1 luồng camera.
    Nếu camera đã đang chạy trên 1 node còn sống -> Tái sử dụng node đó để gộp bài toán (Stream Multiplexing).
    Nếu camera chưa chạy -> Chọn node ít tải nhất từ cache RAM.
    """
    cam_id_str = str(req.camera_id).strip() if req.camera_id is not None else None
    alive_nodes = get_cached_alive_nodes()
    alive_node_urls = [n[0] for n in alive_nodes]
    target = None

    if cam_id_str:
        # Kiểm tra xem camera này đã được gán và đang chạy trên node nào chưa
        assigned_node = r.get(f"cam:node:{cam_id_str}") if r else None
        if not assigned_node:
            with cache_lock:
                for n, info in node_cache.items():
                    if info["status"] == "alive" and any(str(s.get("cameraId") or s.get("camera_id")) == cam_id_str for s in info.get("streams", [])):
                        assigned_node = n
                        break

        # Nếu node đang chạy camera này vẫn còn sống -> Tái sử dụng luôn node đó để gộp bài toán
        if assigned_node and assigned_node in alive_node_urls:
            target = assigned_node
            print(f"[COORDINATOR] Camera {cam_id_str} dang chay tren {target}. Tai su dung node de gop luong.")

    if not target:
        target = get_min_load_node(alive_nodes)

    if not target:
        raise HTTPException(status_code=503, detail="Khong co node nao hoat dong")

    tasks = req.task_types or req.task_type or ["detect_face"]
    if isinstance(tasks, str):
        tasks = [t.strip() for t in tasks.split(",") if t.strip()]

    try:
        resp = requests.post(
            f"{target}/stream/start",
            json={
                "url": req.url,
                "task_types": tasks,
                "task_type": ",".join(tasks),
                "camera_id": req.camera_id
            },
            timeout=10
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)

        result = resp.json()
        cam_id = result.get("cameraId") or result.get("camera_id") or req.camera_id
        if cam_id and result.get("status") in ("SUCCESS", "ALREADY_RUNNING"):
            # Lấy danh sách tasks đã gộp từ node trả về (hoặc fallback tasks)
            merged_tasks = result.get("taskTypes") or tasks
            redis_add_stream(target, str(cam_id), req.url, merged_tasks)
            # Cập nhật cache node ngay lập tức
            check_and_update_node(target)

        return {**result, "assigned_node": target}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





@app.post("/stream/stop")
def stop_stream(req: StopRequest):
    """
    Dừng 1 luồng camera hoặc gỡ 1 bài toán cụ thể khỏi camera.
    Coordinator tự tìm node nào đang chạy và gọi stop kèm task_type.
    """
    cam_id_str = str(req.camera_id).strip()
    if cam_id_str.lower() in ("undefined", "null", "", "all"):
        return coordinator_stop_all()

    node = r.get(f"cam:node:{cam_id_str}") if r else None
    if not node:
        # Fallback: tim trong cache
        with cache_lock:
            for n, info in node_cache.items():
                if any(str(s.get("cameraId") or s.get("camera_id")) == cam_id_str for s in info.get("streams", [])):
                    node = n
                    break

    if not node:
        # Neu van khong co node chi dinh, thu stop tren tat ca cac node va don sach Redis
        for n, _ in get_cached_alive_nodes():
            try:
                requests.post(f"{n}/stream/stop", json={"camera_id": req.camera_id, "task_type": req.task_type}, timeout=2)
                redis_remove_stream(n, cam_id_str)
                check_and_update_node(n)
            except Exception:
                pass
        if r:
            r.delete(f"cam:url:{cam_id_str}")
            r.delete(f"cam:task:{cam_id_str}")
            r.delete(f"cam:node:{cam_id_str}")
            r.srem("registered_cams", cam_id_str)
        return {"status": "SUCCESS", "cameraId": req.camera_id, "camera_id": req.camera_id, "message": "Đã dọn sạch luồng"}

    try:
        resp = requests.post(
            f"{node}/stream/stop",
            json={"camera_id": req.camera_id, "task_type": req.task_type},
            timeout=5
        )
        if resp.status_code in (200, 404):
            node_resp = resp.json() if resp.status_code == 200 else {}
            remaining = node_resp.get("remaining_tasks", [])
            # Nếu node phản hồi còn bài toán khác đang chạy (action == TASK_REMOVED)
            if remaining and len(remaining) > 0:
                if r:
                    r.set(f"cam:task:{cam_id_str}", ",".join(remaining))
                check_and_update_node(node)
                return {
                    "status": "SUCCESS",
                    "cameraId": req.camera_id,
                    "camera_id": req.camera_id,
                    "remaining_tasks": remaining,
                    "action": "TASK_REMOVED",
                    "message": f"Đã gỡ bài toán '{req.task_type}', camera vẫn tiếp tục chạy bài toán: {remaining}"
                }
            else:
                # Không còn bài toán nào hoặc 404 -> dọn sạch stream hoàn toàn khỏi Redis
                redis_remove_stream(node, cam_id_str)
                check_and_update_node(node)
                return {
                    "status": "SUCCESS",
                    "cameraId": req.camera_id,
                    "camera_id": req.camera_id,
                    "remaining_tasks": [],
                    "action": "STREAM_STOPPED",
                    "message": "Đã ngắt luồng thành công"
                }
        return resp.json()
    except Exception as e:
        # Neu node mat ket noi, van don sach Redis de khong bi luong ma
        redis_remove_stream(node, cam_id_str)
        return {"status": "SUCCESS", "cameraId": req.camera_id, "camera_id": req.camera_id, "message": f"Node gap loi ({e}), da don luong khoi Redis"}



@app.get("/streams")
def list_all_streams():
    """Toan bo cac luong dang chay tren tat ca cac node."""
    result = {}
    with cache_lock:
        for node, info in node_cache.items():
            result[node] = [s.get("cameraId") or s.get("camera_id") for s in info.get("streams", [])]
    return result


@app.get("/nodes")
def list_nodes():
    """Trang thai cac node: alive/dead + so luong dang chay tu cache (0ms)."""
    with cache_lock:
        return [
            {
                "node": node,
                "status": info["status"],
                "streams": info["load"],
            }
            for node, info in node_cache.items()
        ]


@app.get("/streams/details")
def list_streams_details():
    """
    Tra ve tat ca stream dang chay tren moi node con song tu cache.
    Phan hoi trong < 1ms, KHONG BAO GIO BI TIMEOUT du co node bi tat dot ngot.
    """
    cached_streams = get_cached_streams()
    if cached_streams:
        return {"streams": cached_streams}

    # Fallback doc tu Redis neu cache vua khoi dong chua kip nap
    if r:
        try:
            now = int(time.time())
            result = []
            for node in NODES:
                camera_ids = r.smembers(f"node:streams:{node}")
                for cam_id in camera_ids:
                    start_t = r.get(f"cam:start_time:{cam_id}")
                    uptime = (now - int(start_t)) if start_t and str(start_t).isdigit() else 0
                    t_str = r.get(f"cam:task:{cam_id}") or "detect_face"
                    tasks = [t.strip() for t in t_str.split(",") if t.strip()]
                    result.append({
                        "cameraId": cam_id,
                        "camera_id": cam_id,
                        "url": r.get(f"cam:url:{cam_id}") or "",
                        "taskTypes": tasks,
                        "taskType": t_str,
                        "node": node,
                        "uptime": uptime,
                    })
            return {"streams": result}
        except Exception as e:
            print(f"[REDIS ERROR] list_streams_details fallback: {e}")

    return {"streams": []}




if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)

