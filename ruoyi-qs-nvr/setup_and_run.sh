#!/bin/bash
set -e

PROJECT_DIR="$(pwd)"

echo "=========================================="
echo "1. Building Backend Jars using Docker Maven..."
echo "=========================================="
# Thêm --net=host và --dns để fix lỗi phân giải DNS (name resolution) trên máy chủ
docker run --rm --net=host \
  --dns 8.8.8.8 --dns 1.1.1.1 \
  -v "$PROJECT_DIR:/app" \
  -v "$HOME/.m2:/root/.m2" \
  -w /app \
  maven:3.9-eclipse-temurin-17 \
  mvn clean package -Dmaven.test.skip=true

echo "=========================================="
echo "2. Building Frontend UI using Docker Node..."
echo "=========================================="
docker run --rm --net=host \
  --dns 8.8.8.8 --dns 1.1.1.1 \
  -v "$PROJECT_DIR/ruoyi-ui:/app" \
  -w /app \
  node:18-alpine \
  sh -c "npm install --legacy-peer-deps && npm run build:prod"

echo "=========================================="
echo "3. Preparing Directories and Copying Files..."
echo "=========================================="
mkdir -p docker/mysql/db
mkdir -p docker/nginx/html/dist
mkdir -p docker/ruoyi/gateway/jar
mkdir -p docker/ruoyi/auth/jar
mkdir -p docker/ruoyi/visual/monitor/jar
mkdir -p docker/ruoyi/modules/system/jar
mkdir -p docker/ruoyi/modules/file/jar
mkdir -p docker/ruoyi/modules/job/jar
mkdir -p docker/ruoyi/modules/gen/jar
mkdir -p docker/ruoyi/modules/haikang/jar
mkdir -p docker/ruoyi/modules/qs/jar
mkdir -p docker/ruoyi/modules/haikang-isup/jar
mkdir -p docker/ruoyi/modules/dahua/jar
mkdir -p docker/ruoyi/modules/onvif/jar
mkdir -p docker/ruoyi/modules/gb28181/jar
mkdir -p docker/ruoyi/modules/jt1078/jar
mkdir -p docker/ruoyi/modules/zlm/jar

# Copy SQL files
cp sql/ry-config.sql docker/mysql/db/
cp sql/ry-cloud.sql docker/mysql/db/
cp sql/ry_seata_20210128.sql docker/mysql/db/ 2>/dev/null || true

# Copy Frontend dist
cp -r ruoyi-ui/dist/* docker/nginx/html/dist/
chmod -R 755 docker/nginx/html/dist

# Copy Backend Jars
cp ruoyi-gateway/target/ruoyi-gateway.jar docker/ruoyi/gateway/jar/
cp ruoyi-auth/target/ruoyi-auth.jar docker/ruoyi/auth/jar/
cp ruoyi-visual/ruoyi-monitor/target/ruoyi-visual-monitor.jar docker/ruoyi/visual/monitor/jar/
cp ruoyi-modules/ruoyi-system/target/ruoyi-modules-system.jar docker/ruoyi/modules/system/jar/
cp ruoyi-modules/ruoyi-file/target/ruoyi-modules-file.jar docker/ruoyi/modules/file/jar/
cp ruoyi-modules/ruoyi-job/target/ruoyi-modules-job.jar docker/ruoyi/modules/job/jar/
cp ruoyi-modules/ruoyi-gen/target/ruoyi-modules-gen.jar docker/ruoyi/modules/gen/jar/
cp ruoyi-modules/ruoyi-haikang/target/ruoyi-modules-haikang.jar docker/ruoyi/modules/haikang/jar/
cp ruoyi-modules/ruoyi-qs/target/ruoyi-modules-qs.jar docker/ruoyi/modules/qs/jar/
cp ruoyi-modules/ruoyi-haikang-isup/target/ruoyi-modules-haikang-isup.jar docker/ruoyi/modules/haikang-isup/jar/
cp ruoyi-modules/ruoyi-dahua/target/ruoyi-modules-dahua.jar docker/ruoyi/modules/dahua/jar/
cp ruoyi-modules/ruoyi-onvif/target/ruoyi-modules-onvif.jar docker/ruoyi/modules/onvif/jar/
cp ruoyi-modules/ruoyi-gb28181/target/ruoyi-modules-gb28181.jar docker/ruoyi/modules/gb28181/jar/
cp ruoyi-modules/ruoyi-jt1078/target/ruoyi-modules-jt1078.jar docker/ruoyi/modules/jt1078/jar/
cp ruoyi-modules/ruoyi-zlm/target/ruoyi-modules-zlm.jar docker/ruoyi/modules/zlm/jar/

echo "=========================================="
echo "4. Building and Starting Docker Containers..."
echo "=========================================="
cd docker
docker compose build
docker compose up -d

echo "=========================================="
echo "5. Initializing Database and Nacos Configurations..."
echo "=========================================="
# Chờ MySQL khởi động sẵn sàng nhận kết nối
echo "Waiting for MySQL database to be ready..."
until docker exec -i ruoyi-mysql mysql -uroot -ppassword -e "SELECT 1;" >/dev/null 2>&1; do
    sleep 3
done
echo "MySQL is ready."

# Tự động cấp quyền root@%, tạo database và nạp dữ liệu
echo "Initializing databases and permissions..."
docker exec -i ruoyi-mysql mysql -uroot -ppassword -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'password' WITH GRANT OPTION; CREATE DATABASE IF NOT EXISTS \`ry-config\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; CREATE DATABASE IF NOT EXISTS \`ry-cloud\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci; FLUSH PRIVILEGES;" 2>/dev/null || true
docker exec -i ruoyi-mysql mysql -uroot -ppassword ry-config < ../sql/ry-config.sql 2>/dev/null || true
docker exec -i ruoyi-mysql mysql -uroot -ppassword ry-cloud < ../sql/ry-cloud.sql 2>/dev/null || true

# Tự động sửa cấu hình sang mạng nội bộ Docker
if [ -f "fix_redis.sql" ]; then
    echo "Applying Docker network host configurations..."
    docker exec -i ruoyi-mysql mysql -uroot -ppassword < fix_redis.sql 2>/dev/null || true
fi

# Khởi động lại Nacos và các dịch vụ để nhận cấu hình hoàn chỉnh
echo "Restarting Nacos & Microservices to load configurations..."
docker restart ruoyi-nacos
sleep 20
docker restart ruoyi-auth ruoyi-gateway ruoyi-modules-system ruoyi-modules-gen

echo "=========================================="
echo "SUCCESS! All services are started and configured."
echo "Application URL: http://<SERVER_IP>"
echo "Nacos URL:       http://<SERVER_IP>:8848/nacos"
echo "=========================================="
