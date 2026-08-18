$ErrorActionPreference = "Stop"

# Function to check exit code of native commands
function Check-LastExitCode {
    if ($LASTEXITCODE -ne 0) {
        throw "Previous command failed with exit code $LASTEXITCODE"
    }
}

# 1. Setup JAVA_HOME
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

Write-Host "=============================" -ForegroundColor Green
Write-Host "1. Building Backend Jars..." -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green
cd "d:\ViettelCloudCamera\demo_ai\ruoyi-qs-nvr"
mvn clean package "-Dmaven.test.skip=true"
Check-LastExitCode

Write-Host "=============================" -ForegroundColor Green
Write-Host "2. Building Frontend UI..." -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green
cd "d:\ViettelCloudCamera\demo_ai\ruoyi-qs-nvr\ruoyi-ui"
npm install --legacy-peer-deps
Check-LastExitCode
npm run build:prod
Check-LastExitCode

Write-Host "=============================" -ForegroundColor Green
Write-Host "3. Preparing SQL Files and Directories..." -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green
cd "d:\ViettelCloudCamera\demo_ai\ruoyi-qs-nvr"

# Create directories
New-Item -ItemType Directory -Force -Path "docker/mysql/db"
New-Item -ItemType Directory -Force -Path "docker/nginx/html/dist"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/gateway/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/auth/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/visual/monitor/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/modules/system/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/modules/file/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/modules/job/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/modules/gen/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/modules/haikang/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/modules/qs/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/modules/haikang-isup/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/modules/dahua/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/modules/onvif/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/modules/gb28181/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/modules/jt1078/jar"
New-Item -ItemType Directory -Force -Path "docker/ruoyi/modules/zlm/jar"

# Modify SQL files to add USE / CREATE DATABASE statements
# For ry-config.sql
$configSql = Get-Content "sql/ry-config.sql" -Raw
$prefix = 'CREATE DATABASE IF NOT EXISTS `ry-config` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;' + "`n" + 'USE `ry-config`;' + "`n"
Set-Content "docker/mysql/db/ry-config.sql" ($prefix + $configSql)

# For ry-cloud.sql
$cloudSql = Get-Content "sql/ry-cloud.sql" -Raw
$prefix2 = 'CREATE DATABASE IF NOT EXISTS `ry-cloud` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;' + "`n" + 'USE `ry-cloud`;' + "`n"
Set-Content "docker/mysql/db/ry-cloud.sql" ($prefix2 + $cloudSql)

# For ry_seata_20210128.sql
Copy-Item "sql/ry_seata_20210128.sql" "docker/mysql/db/ry_seata_20210128.sql" -Force

Write-Host "=============================" -ForegroundColor Green
Write-Host "4. Copying built Jars and Frontend..." -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green

# Copy frontend
Copy-Item -Path "ruoyi-ui/dist/*" -Destination "docker/nginx/html/dist" -Recurse -Force

# Copy Jars
Copy-Item "ruoyi-gateway/target/ruoyi-gateway.jar" "docker/ruoyi/gateway/jar/" -Force
Copy-Item "ruoyi-auth/target/ruoyi-auth.jar" "docker/ruoyi/auth/jar/" -Force
Copy-Item "ruoyi-visual/ruoyi-monitor/target/ruoyi-visual-monitor.jar" "docker/ruoyi/visual/monitor/jar/" -Force
Copy-Item "ruoyi-modules/ruoyi-system/target/ruoyi-modules-system.jar" "docker/ruoyi/modules/system/jar/" -Force
Copy-Item "ruoyi-modules/ruoyi-file/target/ruoyi-modules-file.jar" "docker/ruoyi/modules/file/jar/" -Force
Copy-Item "ruoyi-modules/ruoyi-job/target/ruoyi-modules-job.jar" "docker/ruoyi/modules/job/jar/" -Force
Copy-Item "ruoyi-modules/ruoyi-gen/target/ruoyi-modules-gen.jar" "docker/ruoyi/modules/gen/jar/" -Force
Copy-Item "ruoyi-modules/ruoyi-haikang/target/ruoyi-modules-haikang.jar" "docker/ruoyi/modules/haikang/jar/" -Force
Copy-Item "ruoyi-modules/ruoyi-qs/target/ruoyi-modules-qs.jar" "docker/ruoyi/modules/qs/jar/" -Force
Copy-Item "ruoyi-modules/ruoyi-haikang-isup/target/ruoyi-modules-haikang-isup.jar" "docker/ruoyi/modules/haikang-isup/jar/" -Force
Copy-Item "ruoyi-modules/ruoyi-dahua/target/ruoyi-modules-dahua.jar" "docker/ruoyi/modules/dahua/jar/" -Force
Copy-Item "ruoyi-modules/ruoyi-onvif/target/ruoyi-modules-onvif.jar" "docker/ruoyi/modules/onvif/jar/" -Force
Copy-Item "ruoyi-modules/ruoyi-gb28181/target/ruoyi-modules-gb28181.jar" "docker/ruoyi/modules/gb28181/jar/" -Force
Copy-Item "ruoyi-modules/ruoyi-jt1078/target/ruoyi-modules-jt1078.jar" "docker/ruoyi/modules/jt1078/jar/" -Force
Copy-Item "ruoyi-modules/ruoyi-zlm/target/ruoyi-modules-zlm.jar" "docker/ruoyi/modules/zlm/jar/" -Force

Write-Host "=============================" -ForegroundColor Green
Write-Host "5. Running Docker Compose Build..." -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green
cd "d:\ViettelCloudCamera\demo_ai\ruoyi-qs-nvr\docker"
docker compose build
Check-LastExitCode

Write-Host "=============================" -ForegroundColor Green
Write-Host "6. Launching Services via Docker Compose..." -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green
docker compose up -d
Check-LastExitCode

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "Success! System is starting up." -ForegroundColor Cyan
Write-Host "Please wait 1-2 minutes for MySQL and Nacos to fully initialize." -ForegroundColor Cyan
Write-Host "You can access Nacos at http://localhost:8848/nacos" -ForegroundColor Cyan
Write-Host "You can access the Application at http://localhost" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan
