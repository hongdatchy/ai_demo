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
RUN pip install --no-cache-dir -r requirements.txt

# 5. Sao chép mã nguồn Backend và Frontend vào container
COPY app.py .
COPY index.html .

# 6. Mở cổng 8000
EXPOSE 8000

# 7. Lệnh khởi chạy ứng dụng Web
CMD ["python3", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
