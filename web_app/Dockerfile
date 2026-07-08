# syntax=docker/dockerfile:1
# 1. Sử dụng Python 3.10 Slim làm base image (nhẹ và tối ưu hóa cho server)
FROM python:3.10-slim



# Thiết lập biến môi trường để log hiển thị tức thì
ENV PYTHONUNBUFFERED=1
# Thiết lập biến môi trường để nạp PyTorch trước tránh lỗi OpenMP
ENV OMP_NUM_THREADS=1

# 3. Tạo thư mục làm việc trong container
WORKDIR /app

# 4. Sao chép và cài đặt các thư viện Python
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

# Gỡ bỏ bản opencv-python (bản có GUI) do các thư viện tự động kéo về,
# và ép cài đặt lại bản headless để không bị đòi thư viện đồ họa (libxcb)
# Đồng thời gỡ bỏ luôn triton để triệt tiêu vĩnh viễn lỗi libtriton.so Segfault trên CPU
RUN --mount=type=cache,target=/root/.cache/pip \
    pip uninstall -y opencv-python opencv-python-headless triton && \
    pip install opencv-python-headless

# 5. Sao chép mã nguồn Backend và Frontend vào container
COPY app.py .
COPY index.html .
# Copy các file chứng chỉ SSL (key.pem, cert.pem) để chạy HTTPS
# COPY key.pem cert.pem ./

# 6. Mở cổng 8000
EXPOSE 8000

# 7. Lệnh khởi chạy ứng dụng Web với HTTPS (SSL)
# CMD ["python3", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--ssl-keyfile", "key.pem", "--ssl-certfile", "cert.pem"]
CMD ["python3", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
