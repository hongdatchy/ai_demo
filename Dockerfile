FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    OMP_NUM_THREADS=1 \
    DEBIAN_FRONTEND=noninteractive

# 1. Cài đặt các thư viện hệ thống cần thiết cho OpenCV, VidGear, Video Decoding (HLS/RTSP)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libsm6 \
    libxext6 \
    libgl1 \
    libglib2.0-0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Cài đặt dependencies Python (Có cơ chế Cache Mount giữ lại .whl trên máy host)
COPY requirements.txt .

# --------------------------------------------------------------------------------------
# [HIỆN TẠI] BẢN NHẸ CPU (Dành cho Local / Dev - Tải siêu nhanh, bỏ qua ~3GB driver CUDA)
# --------------------------------------------------------------------------------------
# RUN --mount=type=cache,target=/root/.cache/pip \
#     pip install --upgrade pip && \
#     pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu && \
#     pip install -r requirements.txt && \
#     pip uninstall -y triton 2>/dev/null || true

# --------------------------------------------------------------------------------------
# [PRODUCTION] BẢN FULL GPU / CUDA (Mở comment khối này khi chạy Production có card NVIDIA)
# --------------------------------------------------------------------------------------
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip uninstall -y triton 2>/dev/null || true

# 3. Copy toàn bộ mã nguồn
COPY . /app

# 4. Mở các port tương ứng các dịch vụ:
# 8000: Web Portal (app.py)
# 8101: Extractor Node 1
# 8102: Extractor Node 2
# 8200: Stream Coordinator
EXPOSE 8000 8101 8102 8200
