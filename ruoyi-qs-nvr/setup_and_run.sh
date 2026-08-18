#!/bin/bash
set -e

echo "=========================================="
echo "1. Building Backend Jars (Maven)..."
echo "=========================================="
mvn clean package -Dmaven.test.skip=true

echo "=========================================="
echo "2. Building Frontend UI (Vue 3)..."
echo "=========================================="
cd ruoyi-ui
npm install --legacy-peer-deps
npm run build:prod
cd ..

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
echo "5. Waiting for DB to initialize & fixing Nacos Config..."
echo "=========================================="
sleep 30
if [ -f "fix_redis.sql" ]; then
    echo "Applying Nacos MySQL fixes..."
    docker exec -i ruoyi-mysql mysql -uroot -ppassword < fix_redis.sql 2>/dev/null || true
    docker restart ruoyi-auth ruoyi-gateway ruoyi-modules-system ruoyi-modules-gen 2>/dev/null || true
fi

echo "=========================================="
echo "SUCCESS! All services are started."
echo "Application URL: http://<SERVER_IP>"
echo "Nacos URL:       http://<SERVER_IP>:8848/nacos"
echo "=========================================="
