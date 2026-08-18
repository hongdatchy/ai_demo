$ErrorActionPreference = "Stop"

function Check-LastExitCode {
    if ($LASTEXITCODE -ne 0) {
        throw "Previous command failed with exit code $LASTEXITCODE"
    }
}

# 1. Setup JAVA_HOME (nếu có cài trên máy)
if (Test-Path "C:\Program Files\Java\jdk-17") {
    $env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
    $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
}

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

# Copy SQL files
Copy-Item "sql/ry-config.sql" "docker/mysql/db/ry-config.sql" -Force
Copy-Item "sql/ry-cloud.sql" "docker/mysql/db/ry-cloud.sql" -Force
if (Test-Path "sql/ry_seata_20210128.sql") {
    Copy-Item "sql/ry_seata_20210128.sql" "docker/mysql/db/ry_seata_20210128.sql" -Force
}

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
Write-Host "5. Building Docker Images..." -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green
cd "d:\ViettelCloudCamera\demo_ai\ruoyi-qs-nvr\docker"
docker compose build
Check-LastExitCode

Write-Host "=============================" -ForegroundColor Green
Write-Host "6. Packaging Docker Images to ruoyi_images.tar..." -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green

$imagesToSave = @(
    "docker-ruoyi-gateway",
    "docker-ruoyi-auth",
    "docker-ruoyi-modules-system",
    "docker-ruoyi-modules-gen",
    "docker-ruoyi-modules-job",
    "docker-ruoyi-modules-file",
    "docker-ruoyi-visual-monitor",
    "docker-ruoyi-modules-haikang",
    "docker-ruoyi-modules-qs",
    "docker-ruoyi-modules-haikang-isup",
    "docker-ruoyi-modules-dahua",
    "docker-ruoyi-modules-onvif",
    "docker-ruoyi-modules-gb28181",
    "docker-ruoyi-modules-jt1078",
    "docker-ruoyi-modules-zlm"
)

docker save -o ruoyi_images.tar $imagesToSave
Check-LastExitCode

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "SUCCESS! Packaging complete." -ForegroundColor Cyan
Write-Host "File created: docker\ruoyi_images.tar" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps to deploy on Linux Server:" -ForegroundColor Yellow
Write-Host "1. Copy folder 'docker' to your Linux Server (via WinSCP or SCP):" -ForegroundColor White
Write-Host "   scp -r docker/ root@<SERVER_IP>:/root/ruoyi-deploy/" -ForegroundColor Gray
Write-Host "2. On Linux Server, run:" -ForegroundColor White
Write-Host "   cd /root/ruoyi-deploy/docker" -ForegroundColor Gray
Write-Host "   docker load -i ruoyi_images.tar" -ForegroundColor Gray
Write-Host "   docker compose up -d" -ForegroundColor Gray
Write-Host "=============================================" -ForegroundColor Cyan
