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
        r.srem(f"node:streams:{node_url}", cloud_id)
    except Exception as e:
        print(f"[REDIS ERROR] redis_remove_stream: {e}")


# ─────────────────── Node helpers ─────────────

def get_alive_nodes():
    """Quet tat ca node, tra ve list (node_url, load) cua node dang song."""
    alive = []
    for node in NODES:
        try:
            resp = requests.get(f"{node}/health", timeout=HEALTHCHECK_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                alive.append((node, data.get("load", 0)))
        except Exception:
            pass
    return alive

def get_min_load_node(alive_nodes):
    """Lay node it tai nhat."""
    if not alive_nodes:
        return None
    return min(alive_nodes, key=lambda x: x[1])[0]


# ─────────────────── Failover ─────────────────

def failover(dead_node: str):
    """Chuyen toan bo luong camera tu dead_node sang cac node song."""
    cloud_ids = r.smembers(f"node:streams:{dead_node}")
    if not cloud_ids:
        print(f"[FAILOVER] Node {dead_node} chet nhung khong co luong nao de chuyen.")
        return

    print(f"[FAILOVER] Node {dead_node} chet! Dang chuyen {len(cloud_ids)} luong...")

    for cloud_id in cloud_ids:
        url = r.get(f"cam:url:{cloud_id}")
        task_type = r.get(f"cam:task:{cloud_id}") or "detect_face"
        if not url:
            continue

        # Lay lai alive nodes moi lan de load balancing chinh xac
        alive_nodes = get_alive_nodes()
        target = get_min_load_node(alive_nodes)
        if not target:
            print(f"[FAILOVER] Khong con node nao song! Bo qua {cloud_id}")
            continue

        try:
            resp = requests.post(
                f"{target}/stream/start",
                json={"url": url, "task_type": task_type},
                timeout=10
            )
            if resp.status_code == 200:
                # Cap nhat Redis
                r.srem(f"node:streams:{dead_node}", cloud_id)
                redis_add_stream(target, cloud_id, url, task_type)
                print(f"[FAILOVER] Chuyen {cloud_id}: {dead_node} -> {target}")
            else:
                print(f"[FAILOVER] Loi khi chuyen {cloud_id} sang {target}: {resp.text}")
        except Exception as e:
            print(f"[FAILOVER] Exception khi chuyen {cloud_id}: {e}")

    # Xoa trang thai node chet
    r.delete(f"node:streams:{dead_node}")


# ─────────────────── Healthcheck loop ─────────

def healthcheck_loop():
    node_states = {node: "alive" for node in NODES}

    while True:
        for node in NODES:
            try:
                resp = requests.get(f"{node}/health", timeout=HEALTHCHECK_TIMEOUT)
                is_alive = resp.status_code == 200
            except Exception:
                is_alive = False

            if is_alive:
                if node_states[node] == "dead":
                    print(f"[HEALTHCHECK] Node {node} phuc hoi.")
                node_states[node] = "alive"
            else:
                if node_states[node] == "alive":
                    print(f"[HEALTHCHECK] Node {node} CHET -> bat dau failover...")
                    node_states[node] = "dead"
                    # Failover chay tren thread rieng, khong block healthcheck
                    threading.Thread(target=failover, args=(node,), daemon=True).start()

        time.sleep(HEALTHCHECK_INTERVAL)


# ─────────────────── API Endpoints ────────────

@app.post("/stream/start")
def start_stream(req: StartRequest):
    """
    App.py goi endpoint nay de bat dau 1 luong camera.
    Coordinator tu chon node it tai nhat.
    """
    alive_nodes = get_alive_nodes()
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
            raise HTTPException(status_code=502, detail=resp.text)

        result = resp.json()
        cloud_id = result.get("cloudId")
        if cloud_id and result.get("status") == "SUCCESS":
            redis_add_stream(target, cloud_id, req.url, tasks)

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
        # Neu khong co trong Redis, thu stop tren tat ca cac node
        stopped = False
        for n in NODES:
            try:
                resp = requests.post(f"{n}/stream/stop", json={"cloud_id": req.cloud_id}, timeout=3)
                if resp.status_code == 200:
                    stopped = True
                    redis_remove_stream(n, req.cloud_id)
                    return resp.json()
            except Exception:
                pass
        if not stopped:
            raise HTTPException(status_code=404, detail=f"cloudId {req.cloud_id} khong ton tai")

    try:
        resp = requests.post(
            f"{node}/stream/stop",
            json={"cloud_id": req.cloud_id},
            timeout=10
        )
        if resp.status_code == 200:
            redis_remove_stream(node, req.cloud_id)
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/streams")
def list_all_streams():
    """Toan bo cac luong dang chay tren tat ca cac node."""
    result = {}
    for node in NODES:
        cloud_ids = r.smembers(f"node:streams:{node}") if r else set()
        result[node] = list(cloud_ids)
    return result


@app.get("/nodes")
def list_nodes():
    """Trang thai cac node: alive/dead + so luong dang chay."""
    alive_nodes = {n for n, _ in get_alive_nodes()}
    return [
        {
            "node": node,
            "status": "alive" if node in alive_nodes else "dead",
            "streams": r.scard(f"node:streams:{node}") if r else 0,
        }
        for node in NODES
    ]


@app.get("/streams/details")
def list_streams_details():
    """
    Tra ve tat ca stream dang chay tren moi node, kem thong tin node va uptime.
    Uu tien hoi truc tiep cac node song de lay danh sach real-time chinh xac nhat.
    """
    result = []
    # 1. Hoi truc tiep cac node dang song (real-time, 100% biet ro node, khong phu thuoc vao Redis)
    for node in NODES:
        try:
            resp = requests.get(f"{node}/stream/list", timeout=2)
            if resp.status_code == 200:
                for s in resp.json():
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
        except Exception:
            pass

    # 2. Neu cac node chua phan hoi hoac result rong, fallback doc tu Redis
    if not result and r:
        try:
            now = int(time.time())
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
        except Exception as e:
            print(f"[REDIS ERROR] list_streams_details fallback: {e}")

    return {"streams": result}



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)

