"""
stream_coordinator.py
---------------------
Dieu phoi cac Frame Extractor Node:
  1. Healthcheck dinh ky (10s/lan) - phat hien node chet
  2. Failover - chuyen luong tu node chet sang node song
  3. Load balancing - them luong moi vao node it tai nhat
  4. HTTP API - app.py goi vao day thay vi goi thang extractor

Nguon su that duy nhat: REDIS (khong dung RAM cache cho trang thai node/stream)
  node:status:{node_url}  -> "alive" | "dead"
  node:load:{node_url}    -> so luong stream dang chay
  registered_cams         -> set cac camera_id da dang ky
  cam:url:{id}            -> url stream
  cam:task:{id}           -> task types (comma-separated)
  cam:node:{id}           -> node url dang xu ly camera nay
  cam:start_time:{id}     -> timestamp bat dau
  node:streams:{node_url} -> set cac camera_id dang chay tren node do

Chay: python stream_coordinator.py
Config nodes qua bien moi truong:
  NODES=http://node1:8100,http://node2:8100
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
HEALTHCHECK_TIMEOUT = int(os.getenv("HEALTHCHECK_TIMEOUT", 3))     # giay
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
    camera_id: int | str | None = None

class StopRequest(BaseModel):
    camera_id: int | str
    task_type: str | None = None


# ─────────────────── Redis helpers ────────────

def redis_add_stream(node_url: str, camera_id: str, url: str, task_type):
    if not r:
        return
    cam_id = str(camera_id).strip()
    task_str = ",".join(task_type) if isinstance(task_type, list) else str(task_type)
    try:
        r.set(f"cam:url:{cam_id}", url)
        r.set(f"cam:task:{cam_id}", task_str)
        r.set(f"cam:node:{cam_id}", node_url)
        r.sadd("registered_cams", cam_id)
        if not r.exists(f"cam:start_time:{cam_id}"):
            r.set(f"cam:start_time:{cam_id}", str(int(time.time())))
        r.sadd(f"node:streams:{node_url}", cam_id)
        # Cap nhat load cua node trong Redis
        load = r.scard(f"node:streams:{node_url}")
        r.set(f"node:load:{node_url}", load)
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
        # Cap nhat load cua node trong Redis
        load = r.scard(f"node:streams:{node_url}")
        r.set(f"node:load:{node_url}", load)
    except Exception as e:
        print(f"[REDIS ERROR] redis_remove_stream: {e}")


# ─────────────────── Node Status (Redis) ──────

def set_node_status(node: str, status: str, load: int = 0):
    """Ghi trang thai node vao Redis."""
    if not r:
        return
    try:
        r.set(f"node:status:{node}", status)
        r.set(f"node:load:{node}", load)
    except Exception as e:
        print(f"[REDIS ERROR] set_node_status: {e}")


def get_node_status(node: str) -> str:
    """Doc trang thai node tu Redis. Mac dinh la 'dead'."""
    if not r:
        return "dead"
    try:
        return r.get(f"node:status:{node}") or "dead"
    except Exception:
        return "dead"


def get_alive_nodes():
    """Lay danh sach (node_url, load) cua cac node dang song tu Redis."""
    result = []
    for node in NODES:
        if not r:
            break
        try:
            status = r.get(f"node:status:{node}") or "dead"
            if status == "alive":
                load = int(r.get(f"node:load:{node}") or 0)
                result.append((node, load))
        except Exception:
            pass
    return result


def get_min_load_node(alive_nodes):
    """Lay node it tai nhat."""
    if not alive_nodes:
        return None
    return min(alive_nodes, key=lambda x: x[1])[0]


# ─────────────────── Healthcheck ──────────────

def check_and_update_node(node: str) -> bool:
    """Kiem tra health cua 1 node, ghi ket qua vao Redis."""
    try:
        resp = requests.get(f"{node}/health", timeout=(1.0, 1.0))
        if resp.status_code == 200:
            data = resp.json()
            load = data.get("load", 0)
            set_node_status(node, "alive", load)
            return True
    except Exception:
        pass

    set_node_status(node, "dead", 0)
    return False


# ─────────────────── Failover ─────────────────

def failover(dead_node: str):
    """Chuyen toan bo luong camera tu dead_node sang cac node song."""
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

        alive_nodes = [n for n in get_alive_nodes() if n[0] != dead_node]
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
            else:
                print(f"[FAILOVER] Loi khi chuyen camera {cam_id} sang {target}: {resp.text}")
        except Exception as e:
            print(f"[FAILOVER] Exception khi chuyen camera {cam_id}: {e}")


# ─────────────────── Reconcile & Recovery ─────

def reconcile_and_recover():
    """
    Doi soat: camera nao co trong Redis ma chua chay tren bat ky node nao -> bat lai.
    Kiem tra "dang chay" bang cach hoi thang HTTP tung extractor node (/stream/list).
    """
    if not r:
        return

    try:
        registered = r.smembers("registered_cams")
        # Fallback: quet cac key cam:url:* neu registered_cams chua co
        if not registered:
            keys = r.keys("cam:url:*")
            if keys:
                registered = {k.replace("cam:url:", "") for k in keys}
                for c in registered:
                    r.sadd("registered_cams", c)

        if not registered:
            return

        # Lay danh sach camera dang thuc su chay bang cach hoi HTTP tung node
        running_cams = set()
        alive_nodes = get_alive_nodes()
        for node_url, _ in alive_nodes:
            try:
                s_resp = requests.get(f"{node_url}/stream/list", timeout=2)
                if s_resp.status_code == 200:
                    for s in s_resp.json():
                        cid = s.get("cameraId") or s.get("camera_id")
                        if cid:
                            running_cams.add(str(cid))
            except Exception:
                pass

        missing_cams = {str(c) for c in registered} - running_cams
        if not missing_cams:
            return

        print(f"[AUTO-RECOVERY] Phat hien {len(missing_cams)} camera can phuc hoi: {list(missing_cams)}")

        for cam_id in missing_cams:
            url = r.get(f"cam:url:{cam_id}")
            t_str = r.get(f"cam:task:{cam_id}") or "detect_face"
            if not url:
                continue

            tasks = [t.strip() for t in t_str.split(",") if t.strip()]

            alive_nodes = get_alive_nodes()
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
                else:
                    print(f"[AUTO-RECOVERY] Node {target} loi khi bat camera {cam_id}: {resp.text}")
            except Exception as e:
                print(f"[AUTO-RECOVERY] Loi ket noi toi {target} khi bat camera {cam_id}: {e}")

    except Exception as e:
        print(f"[AUTO-RECOVERY ERROR] {e}")


# ─────────────────── Healthcheck loop ─────────

def healthcheck_loop():
    # Quet lan dau khi khoi dong
    for node in NODES:
        check_and_update_node(node)

    # Sau khi quet xong lan dau, thi chay reconcile de recover luong tu Redis
    threading.Thread(target=reconcile_and_recover, daemon=True).start()

    iteration = 0
    while True:
        time.sleep(HEALTHCHECK_INTERVAL)
        iteration += 1
        for node in NODES:
            was_alive = (get_node_status(node) == "alive")
            is_alive = check_and_update_node(node)

            if is_alive and not was_alive:
                print(f"[HEALTHCHECK] Node {node} phuc hoi thanh cong.")
                threading.Thread(target=reconcile_and_recover, daemon=True).start()
            elif not is_alive and was_alive:
                print(f"[HEALTHCHECK] Node {node} CHET -> bat dau failover...")
                threading.Thread(target=failover, args=(node,), daemon=True).start()

        # Dinh ky moi 30s doi soat 1 lan de dam bao khong camera nao bi bo sot
        if iteration % 3 == 0:
            threading.Thread(target=reconcile_and_recover, daemon=True).start()


# ─────────────────── API Endpoints ────────────

@app.post("/stream/start")
def start_stream(req: StartRequest):
    """
    App.py hoac backend Java goi endpoint nay de bat dau 1 luong camera.
    Neu camera da dang chay tren 1 node con song -> Tai su dung node do de gop bai toan.
    Neu camera chua chay -> Chon node it tai nhat tu Redis.
    """
    cam_id_str = str(req.camera_id).strip() if req.camera_id is not None else None
    alive_nodes = get_alive_nodes()
    alive_node_urls = [n[0] for n in alive_nodes]
    target = None

    if cam_id_str and r:
        # Kiem tra xem camera nay da duoc gan tren node nao chua (tu Redis)
        assigned_node = r.get(f"cam:node:{cam_id_str}")
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
            merged_tasks = result.get("taskTypes") or tasks
            redis_add_stream(target, str(cam_id), req.url, merged_tasks)

        return {**result, "assigned_node": target}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/stream/stop")
def stop_stream(req: StopRequest):
    """
    Dung 1 luong camera hoac go 1 bai toan cu the khoi camera.
    Coordinator tu tim node nao dang chay (tu Redis) va goi stop.
    """
    cam_id_str = str(req.camera_id).strip()
    if cam_id_str.lower() in ("undefined", "null", "", "all"):
        return coordinator_stop_all()

    # Tim node dang chay camera nay tu Redis
    node = r.get(f"cam:node:{cam_id_str}") if r else None

    if not node:
        # Neu Redis khong biet, thu stop tren tat ca node alive va don sach
        for n, _ in get_alive_nodes():
            try:
                requests.post(f"{n}/stream/stop", json={"camera_id": req.camera_id, "task_type": req.task_type}, timeout=2)
                redis_remove_stream(n, cam_id_str)
            except Exception:
                pass
        if r:
            r.delete(f"cam:url:{cam_id_str}")
            r.delete(f"cam:task:{cam_id_str}")
            r.delete(f"cam:node:{cam_id_str}")
            r.srem("registered_cams", cam_id_str)
        return {"status": "SUCCESS", "cameraId": req.camera_id, "camera_id": req.camera_id, "message": "Da don sach luong"}

    try:
        resp = requests.post(
            f"{node}/stream/stop",
            json={"camera_id": req.camera_id, "task_type": req.task_type},
            timeout=5
        )
        if resp.status_code in (200, 404):
            node_resp = resp.json() if resp.status_code == 200 else {}
            remaining = node_resp.get("remaining_tasks", [])
            if remaining and len(remaining) > 0:
                if r:
                    r.set(f"cam:task:{cam_id_str}", ",".join(remaining))
                return {
                    "status": "SUCCESS",
                    "cameraId": req.camera_id,
                    "camera_id": req.camera_id,
                    "remaining_tasks": remaining,
                    "action": "TASK_REMOVED",
                    "message": f"Da go bai toan '{req.task_type}', camera van tiep tuc chay bai toan: {remaining}"
                }
            else:
                redis_remove_stream(node, cam_id_str)
                return {
                    "status": "SUCCESS",
                    "cameraId": req.camera_id,
                    "camera_id": req.camera_id,
                    "remaining_tasks": [],
                    "action": "STREAM_STOPPED",
                    "message": "Da ngat luong thanh cong"
                }
        return resp.json()
    except Exception as e:
        redis_remove_stream(node, cam_id_str)
        return {"status": "SUCCESS", "cameraId": req.camera_id, "camera_id": req.camera_id, "message": f"Node gap loi ({e}), da don luong khoi Redis"}


def coordinator_stop_all():
    """Dung toan bo luong tren tat ca node va xoa sach Redis."""
    for node, _ in get_alive_nodes():
        try:
            requests.post(f"{node}/stream/stop", json={"camera_id": "all"}, timeout=5)
        except Exception:
            pass
    if r:
        try:
            registered = r.smembers("registered_cams")
            for cam_id in registered:
                node_url = r.get(f"cam:node:{cam_id}") or ""
                r.delete(f"cam:url:{cam_id}")
                r.delete(f"cam:task:{cam_id}")
                r.delete(f"cam:node:{cam_id}")
                r.delete(f"cam:start_time:{cam_id}")
                if node_url:
                    r.srem(f"node:streams:{node_url}", cam_id)
            r.delete("registered_cams")
            for node in NODES:
                r.delete(f"node:streams:{node}")
                r.set(f"node:load:{node}", 0)
        except Exception as e:
            print(f"[STOP ALL ERROR] {e}")
    return {"status": "SUCCESS", "message": "Da dung toan bo luong"}


@app.get("/streams")
def list_all_streams():
    """Toan bo cac luong theo node, lay tu Redis."""
    result = {}
    if not r:
        return result
    for node in NODES:
        try:
            cam_ids = r.smembers(f"node:streams:{node}")
            result[node] = list(cam_ids)
        except Exception:
            result[node] = []
    return result


@app.get("/nodes")
def list_nodes():
    """Trang thai cac node: alive/dead + so luong dang chay tu Redis."""
    result = []
    for node in NODES:
        status = "dead"
        load = 0
        if r:
            try:
                status = r.get(f"node:status:{node}") or "dead"
                load = int(r.get(f"node:load:{node}") or 0)
            except Exception:
                pass
        result.append({"node": node, "status": status, "streams": load})
    return result


@app.get("/streams/details")
def list_streams_details():
    """
    Tra ve tat ca stream dang chay tu Redis (nguon su that duy nhat).
    Luon hoat dong dung ngay ca khi coordinator vua moi restart.
    """
    if not r:
        return {"streams": []}
    try:
        now = int(time.time())
        result = []
        registered = r.smembers("registered_cams")
        for cam_id in registered:
            url = r.get(f"cam:url:{cam_id}") or ""
            node = r.get(f"cam:node:{cam_id}") or "Chua ro"
            start_t = r.get(f"cam:start_time:{cam_id}")
            uptime = (now - int(start_t)) if start_t and str(start_t).isdigit() else 0
            t_str = r.get(f"cam:task:{cam_id}") or "detect_face"
            tasks = [t.strip() for t in t_str.split(",") if t.strip()]
            result.append({
                "cameraId": cam_id,
                "camera_id": cam_id,
                "url": url,
                "taskTypes": tasks,
                "taskType": t_str,
                "node": node,
                "uptime": uptime,
            })
        return {"streams": result}
    except Exception as e:
        print(f"[REDIS ERROR] list_streams_details: {e}")
        return {"streams": []}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
