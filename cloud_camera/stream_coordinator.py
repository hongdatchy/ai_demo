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

class StopRequest(BaseModel):
    cloud_id: str


# ─────────────────── Redis helpers ────────────

def redis_add_stream(node_url: str, cloud_id: str, url: str, task_type):
    if not r:
        return
    if isinstance(task_type, list):
        task_str = ",".join(task_type)
    else:
        task_str = str(task_type)
    try:
        r.set(f"cam:url:{cloud_id}", url)
        r.set(f"cam:task:{cloud_id}", task_str)
        r.set(f"cam:node:{cloud_id}", node_url)
        r.sadd("registered_cams", cloud_id)
        if not r.exists(f"cam:start_time:{cloud_id}"):
            r.set(f"cam:start_time:{cloud_id}", str(int(time.time())))
        r.sadd(f"node:streams:{node_url}", cloud_id)
    except Exception as e:
        print(f"[REDIS ERROR] redis_add_stream: {e}")

def redis_remove_stream(node_url: str, cloud_id: str):
    if not r:
        return
    try:
        r.delete(f"cam:url:{cloud_id}")
        r.delete(f"cam:task:{cloud_id}")
        r.delete(f"cam:node:{cloud_id}")
        r.delete(f"cam:start_time:{cloud_id}")
        r.srem("registered_cams", cloud_id)
        r.srem(f"node:streams:{node_url}", cloud_id)
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
                    result.append({
                        "cloudId": s.get("cloudId"),
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
    cloud_ids = r.smembers(f"node:streams:{dead_node}") if r else set()
    if not cloud_ids:
        print(f"[FAILOVER] Node {dead_node} chet nhung khong co luong nao trong Redis de chuyen.")
        return

    print(f"[FAILOVER] Node {dead_node} chet! Dang chuyen {len(cloud_ids)} luong...")

    for cloud_id in cloud_ids:
        url = r.get(f"cam:url:{cloud_id}") if r else None
        t_str = r.get(f"cam:task:{cloud_id}") if r else "detect_face"
        if not url:
            continue

        tasks = [t.strip() for t in t_str.split(",") if t.strip()]

        # Lấy alive nodes trực tiếp từ cache RAM (0ms, không chờ timeout của dead_node)
        alive_nodes = [n for n in get_cached_alive_nodes() if n[0] != dead_node]
        target = get_min_load_node(alive_nodes)
        if not target:
            print(f"[FAILOVER] Khong con node nao song! Bo qua {cloud_id}")
            continue

        try:
            resp = requests.post(
                f"{target}/stream/start",
                json={
                    "url": url,
                    "task_types": tasks,
                    "task_type": ",".join(tasks)
                },
                timeout=5
            )
            if resp.status_code == 200:
                if r:
                    r.srem(f"node:streams:{dead_node}", cloud_id)
                redis_add_stream(target, cloud_id, url, tasks)
                print(f"[FAILOVER] Chuyen {cloud_id}: {dead_node} -> {target}")
                check_and_update_node(target)
            else:
                print(f"[FAILOVER] Loi khi chuyen {cloud_id} sang {target}: {resp.text}")
        except Exception as e:
            print(f"[FAILOVER] Exception khi chuyen {cloud_id}: {e}")

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
                        running_cams.add(s.get("cloudId"))

        missing_cams = registered - running_cams
        if not missing_cams:
            return

        print(f"[AUTO-RECOVERY] Phat hien {len(missing_cams)} camera can phuc hoi: {list(missing_cams)}")

        for cloud_id in missing_cams:
            url = r.get(f"cam:url:{cloud_id}")
            t_str = r.get(f"cam:task:{cloud_id}") or "detect_face"
            if not url:
                continue

            tasks = [t.strip() for t in t_str.split(",") if t.strip()]

            alive_nodes = get_cached_alive_nodes()
            target = get_min_load_node(alive_nodes)
            if not target:
                print(f"[AUTO-RECOVERY] Khong co node nao online de gan camera {cloud_id}. Se thu lai sau.")
                break

            try:
                resp = requests.post(
                    f"{target}/stream/start",
                    json={
                        "url": url,
                        "task_types": tasks,
                        "task_type": ",".join(tasks)
                    },
                    timeout=5
                )
                if resp.status_code == 200:
                    redis_add_stream(target, cloud_id, url, tasks)
                    print(f"[AUTO-RECOVERY] => Da tu dong bat lai camera [{cloud_id}] tren {target} thanh cong!")
                    check_and_update_node(target)
                else:
                    print(f"[AUTO-RECOVERY] Node {target} loi khi bat {cloud_id}: {resp.text}")
            except Exception as e:
                print(f"[AUTO-RECOVERY] Loi ket noi toi {target} khi bat {cloud_id}: {e}")

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
    App.py goi endpoint nay de bat dau 1 luong camera.
    Coordinator tu chon node it tai nhat dua tren cache trong RAM.
    """
    alive_nodes = get_cached_alive_nodes()
    target = get_min_load_node(alive_nodes)
    if not target:
        raise HTTPException(status_code=503, detail="Khong co node nao hoat dong")

    tasks = req.task_types or req.task_type or ["detect_face"]

    try:
        resp = requests.post(
            f"{target}/stream/start",
            json={
                "url": req.url,
                "task_types": tasks,
                "task_type": ",".join(tasks) if isinstance(tasks, list) else str(tasks)
            },
            timeout=10
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)

        result = resp.json()
        cloud_id = result.get("cloudId")
        if cloud_id and result.get("status") == "SUCCESS":
            redis_add_stream(target, cloud_id, req.url, tasks)
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
    App.py goi endpoint nay de dung 1 luong camera.
    Coordinator tu tim node nao dang chay va goi stop.
    """
    node = r.get(f"cam:node:{req.cloud_id}") if r else None
    if not node:
        # Fallback: tim trong cache
        with cache_lock:
            for n, info in node_cache.items():
                if any(s.get("cloudId") == req.cloud_id for s in info.get("streams", [])):
                    node = n
                    break

    if not node:
        # Neu van khong co node chi dinh, thu stop tren tat ca cac node va don sach Redis
        for n, _ in get_cached_alive_nodes():
            try:
                requests.post(f"{n}/stream/stop", json={"cloud_id": req.cloud_id}, timeout=2)
                redis_remove_stream(n, req.cloud_id)
                check_and_update_node(n)
            except Exception:
                pass
        if r:
            r.delete(f"cam:url:{req.cloud_id}")
            r.delete(f"cam:task:{req.cloud_id}")
            r.delete(f"cam:node:{req.cloud_id}")
            r.srem("registered_cams", req.cloud_id)
        return {"status": "SUCCESS", "cloudId": req.cloud_id, "message": "Đã dọn sạch luồng"}

    try:
        resp = requests.post(
            f"{node}/stream/stop",
            json={"cloud_id": req.cloud_id},
            timeout=5
        )
        # Bat ke node tra ve 200 hay 404 (Node khong chay luong nay), deu phai don sach Redis!
        if resp.status_code in (200, 404):
            redis_remove_stream(node, req.cloud_id)
            check_and_update_node(node)
            return {"status": "SUCCESS", "cloudId": req.cloud_id, "message": "Đã ngắt luồng thành công"}
        return resp.json()
    except Exception as e:
        # Neu node mat ket noi, van don sach Redis de khong bi luong ma
        redis_remove_stream(node, req.cloud_id)
        return {"status": "SUCCESS", "cloudId": req.cloud_id, "message": f"Node gap loi ({e}), da don luong khoi Redis"}



@app.get("/streams")
def list_all_streams():
    """Toan bo cac luong dang chay tren tat ca cac node."""
    result = {}
    with cache_lock:
        for node, info in node_cache.items():
            result[node] = [s.get("cloudId") for s in info.get("streams", [])]
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
                cloud_ids = r.smembers(f"node:streams:{node}")
                for cloud_id in cloud_ids:
                    start_t = r.get(f"cam:start_time:{cloud_id}")
                    uptime = (now - int(start_t)) if start_t and str(start_t).isdigit() else 0
                    t_str = r.get(f"cam:task:{cloud_id}") or "detect_face"
                    tasks = [t.strip() for t in t_str.split(",") if t.strip()]
                    result.append({
                        "cloudId": cloud_id,
                        "url": r.get(f"cam:url:{cloud_id}") or "",
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

