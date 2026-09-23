"""
frame_extractor_node.py
-----------------------
Server HTTP doc lap chay tren MOI Frame Extractor node.
Coordinator se goi HTTP vao day de start/stop stream va kiem tra health.

Chay:
  python frame_extractor_node.py --port 8101 --node-id node_1
  python frame_extractor_node.py --port 8102 --node-id node_2
  # hoac dung env var nhu cu:
  PORT=8101 NODE_ID=node_1 python frame_extractor_node.py
"""

import os
import argparse
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from dynamic_frame_extractor import (
    start_stream,
    
    stop_stream,
    stop_all_streams,
    get_all_streams,
)

app = FastAPI(title="Frame Extractor Node")

# Parse args — fallback ve env var neu khong truyen
parser = argparse.ArgumentParser(description="Frame Extractor Node Server")
parser.add_argument("--port", type=int, default=None, help="Port to listen on (default: PORT env or 8100)")
args, _ = parser.parse_known_args()

NODE_ID = os.getenv("NODE_ID", "node_1")
PORT = args.port or int(os.getenv("PORT", 8100))


# ─────────────────── Schema ───────────────────

class StartRequest(BaseModel):
    url: str
    task_types: list[str] | str | None = None
    task_type: str | None = "detect_face"
    camera_id: int | None = None

class StopRequest(BaseModel):
    cloud_id: str


# ─────────────────── Endpoints ────────────────

@app.get("/health")
def health():
    streams = get_all_streams()
    return {
        "status": "alive",
        "node_id": NODE_ID,
        "load": len(streams),
    }


@app.post("/stream/start")
def api_start(req: StartRequest):
    tasks = req.task_types or req.task_type or ["detect_face"]
    result = start_stream(req.url, tasks, camera_id=req.camera_id)
    if result["status"] not in ("SUCCESS", "ALREADY_RUNNING"):
        raise HTTPException(status_code=500, detail=result)
    return result


@app.post("/stream/stop")
def api_stop(req: StopRequest):
    result = stop_stream(req.cloud_id)
    if result["status"] == "NOT_FOUND":
        raise HTTPException(status_code=404, detail=f"cloudId {req.cloud_id} not found")
    return result


@app.get("/stream/list")
def api_list():
    return get_all_streams()


@app.post("/stream/stop-all")
def api_stop_all():
    stop_all_streams()
    return {"status": "ok"}


# ─────────────────── Lifecycle ────────────────

@app.on_event("shutdown")
def on_shutdown():
    print(f"[{NODE_ID}] Shutdown -> stopping all streams...")
    stop_all_streams()


if __name__ == "__main__":
    print(f"[{NODE_ID}] Frame Extractor Node starting on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
