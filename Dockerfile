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

# 2. Cài đặt uv package manager siêu nhanh
RUN pip install --no-cache-dir uv

# --------------------------------------------------------------------------------------
# [PRODUCTION] BẢN FULL GPU / CUDA (Layer AI nặng — Cố định, Docker cache vĩnh viễn)
# Tách riêng khỏi requirements.txt để khi thêm thư viện mới không phải tải lại CUDA/PyTorch
# --------------------------------------------------------------------------------------
RUN uv pip install --no-cache --system \
        ultralytics \
        "opencv-python<5" \
        deepface \
        tf-keras && \
    uv pip uninstall --system -y triton 2>/dev/null || true

# --------------------------------------------------------------------------------------
# [CÁC THƯ VIỆN TIỆN ÍCH / WEB / KAFKA] Thêm/bớt ở requirements.txt chỉ mất 2-3s build
# --------------------------------------------------------------------------------------
COPY requirements.txt .
RUN uv pip install --no-cache --system -r requirements.txt

# 3. Copy toàn bộ mã nguồn
COPY . /app

# 4. Mở các port tương ứng các dịch vụ:
# 8000: Web Portal (app.py)
# 8101: Extractor Node 1
# 8102: Extractor Node 2
# 8200: Stream Coordinator
EXPOSE 8000 8101 8102 8200
