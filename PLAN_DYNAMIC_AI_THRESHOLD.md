# KẾ HOẠCH KIẾN TRÚC: CẤU HÌNH ĐỘNG FACE THRESHOLD TRÊN HỆ THỐNG THẬT
> **Dự án:** Viettel Cloud Camera - AI Microservice & Core  
> **Trạng thái:** DRAFT / PENDING BUSINESS SPEC  
> **Ngày lập:** 08/10/2026  

---

## 1. MỤC TIÊU & BỐI CẢNH (OBJECTIVE & CONTEXT)
- **Hiện tại:** Ngưỡng nhận diện khuôn mặt (`THRESHOLD = 0.30`) đã được cấu hình động qua Redis và giao diện thử nghiệm của `web_app/app.py`.
- **Yêu cầu tương lai:** Cho phép Người dùng / Quản trị viên (User/Admin) có thể tự điều chỉnh ngưỡng này trực tiếp trên giao diện sản phẩm thật (**CMS Viettel Cloud Camera** hoặc **Web Portal Viettel Cloud Camera**) và lưu trữ bền vững trong CSDL của hệ thống Microservice.

---

## 2. LUỒNG KIẾN TRÚC TỔNG THỂ (END-TO-END DATA FLOW)

```text
[1. User trên CMS / Web Portal thật]
       │
       ▼ (Gửi yêu cầu cập nhật: cameraId, scenarioId, threshold: 0.35)
[2. Java Spring Boot AI Microservice]
       │
       ├─► (A) Lưu bền vững vào CSDL (Bảng SCENARIO_CAMERA / CONFIG)
       │
       └─► (B) Đồng bộ thời gian thực vào Redis Cache:
                Key: "cam:threshold:{cameraId}" -> Value: "0.35"
       │
       ▼ (Theo từng frame camera tới)
[3. Python AI Core - ai_face_consumer]
       │
       ├─► Nhận frame của camera #1963 từ Kafka
       ├─► Đọc Redis key "cam:threshold:1963" (độ trễ < 1ms)
       └─► Thực hiện so khớp với ngưỡng riêng của camera đó:
            if min_distance <= cam_threshold: KHỚP
```

---

## 3. THIẾT KẾ CHI TIẾT TỪNG TẦNG

### 3.1. Tầng Cơ sở dữ liệu (Database Layer - Oracle / PostgreSQL)
- **Đề xuất:** Cấu hình theo **từng Camera** (Per-Camera Threshold) thay vì 1 ngưỡng cho toàn hệ thống, vì góc quay, ánh sáng, khoảng cách mỗi camera là khác nhau.
- **Bảng:** `SCENARIO_CAMERA` (lưu liên kết Camera ↔ Kịch bản AI).
- **Trường bổ sung:**
  ```sql
  ALTER TABLE SCENARIO_CAMERA ADD (
      THRESHOLD NUMBER(4, 2) DEFAULT 0.30
  );
  -- Hoặc dùng cột CONFIG_JSON nếu cần mở rộng nhiều tham số AI khác:
  -- CONFIG_JSON VARCHAR2(1000) DEFAULT '{"threshold": 0.30}'
  ```

### 3.2. Tầng Backend Java Microservice (`ai` module)
- **Entity:** Cập nhật `ScenarioCamera.java`:
  ```java
  @Column(name = "THRESHOLD")
  private Float threshold;
  ```
- **API Cập nhật (Controller & Service):**
  - **Endpoint:** `PUT /api/scenario-cameras/{scenarioCameraId}/config`
  - **Request Body:**
    ```json
    {
      "threshold": 0.35
    }
    ```
  - **Validation:** `0.10 <= threshold <= 0.60`.
- **Cơ chế Sync sang Redis:**
  - Inject `StringRedisTemplate`.
  - Khi lưu DB thành công:
    ```java
    redisTemplate.opsForValue().set("cam:threshold:" + cameraId, String.valueOf(threshold));
    ```
- **API Public Sync lúc khởi động:**
  - Bổ sung trường `threshold` trong API `GET /api/public/faces` hoặc `GET /api/public/camera-config/{cameraId}` để làm cơ chế fallback khi Redis restart.

### 3.3. Tầng Python AI Core (`ai_face_consumer.py`)
- Khi xử lý nhận diện cho frame của `camera_id`:
  ```python
  def get_threshold_for_camera(camera_id: int) -> float:
      if redis_client:
          # 1. Ưu tiên ngưỡng riêng của Camera đó
          val = redis_client.get(f"cam:threshold:{camera_id}")
          if val is not None:
              return float(val)
          # 2. Ngưỡng chung của hệ thống / account
          val_global = redis_client.get("config:face_threshold")
          if val_global is not None:
              return float(val_global)
      # 3. Fallback mặc định
      return DEFAULT_THRESHOLD  # 0.30
  ```
- **Kết quả:** Áp dụng ngay lập tức cho frame tiếp theo, không có độ trễ, không cần khởi động lại container AI.

### 3.4. Tầng Giao diện người dùng (Frontend - CMS / Web Viettel Cloud Camera)
- **Vị trí UI:** 
  - Trong Modal cấu hình Camera AI hoặc màn hình Chi tiết Kịch bản Camera.
- **Thành phần giao diện (UI Control):**
  - **Thanh trượt (Slider) hoặc Input Number:** Dải giá trị từ `0.10` đến `0.60` (bước nhảy `0.01`).
  - **Hướng dẫn cho người dùng (Tooltip / Helper Text):**
    - `0.20 – 0.28`: Rất khắt khe (phù hợp camera chấm công, chống nhận nhầm người lạ).
    - `0.30` *(Mặc định đề xuất)*: Cân bằng tối ưu giữa độ chính xác và độ nhạy.
    - `0.35 – 0.45`: Nhận diện khoảng cách xa, mặt hơi nghiêng hoặc thiếu sáng.

---

## 4. KẾ HOẠCH TRIỂN KHAI (ROADMAP)

1. [ ] **Thống nhất nghiệp vụ:** Chốt thiết kế màn hình trên CMS / Web với đội BA / Product.
2. [ ] **Database Migration:** Thêm cột `THRESHOLD` (hoặc `CONFIG_JSON`) vào bảng `SCENARIO_CAMERA`.
3. [ ] **Java Backend:** Viết API cập nhật cấu hình và logic đồng bộ Redis.
4. [ ] **Python AI Core:** Cập nhật hàm tra cứu threshold theo `camera_id`.
5. [ ] **Frontend CMS/Web:** Ghép giao diện Slider / Input cấu hình.
6. [ ] **Kiểm thử End-to-End:** Thử chỉnh threshold trên UI thật và kiểm tra log `[KHỚP] / [TRƯỢT]` của Python Consumer theo thời gian thực.
