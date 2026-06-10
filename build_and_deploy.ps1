$ErrorActionPreference = "Stop"

Write-Host "Building frontend..."
cd frontend
npm ci --prefer-offline
npm run build
cd ..

Write-Host "Copying frontend dist to backend..."
if (Test-Path "backend/internal/app/web/dist") {
    Remove-Item -Recurse -Force "backend/internal/app/web/dist"
}
New-Item -ItemType Directory -Force -Path "backend/internal/app/web/dist" | Out-Null
Copy-Item -Recurse -Path "frontend/dist\*" -Destination "backend/internal/app/web/dist"

Write-Host "Building backend for linux/arm64..."
cd backend
$env:CGO_ENABLED="0"
$env:GOOS="linux"
$env:GOARCH="arm64"
go build -tags embedded_frontend -trimpath -ldflags="-s -w" -o cpa-helper ./cmd/cpa-helper
cd ..

Write-Host "Uploading to server..."
scp -o StrictHostKeyChecking=no -P 28922 -i "E:/Files/SSH Key/oracle-ssh-key-2026-05-16.key" "backend/cpa-helper" ubuntu@163.192.9.157:/tmp/cpa-helper

Write-Host "Deploying on server..."
ssh -o StrictHostKeyChecking=no -p 28922 -i "E:/Files/SSH Key/oracle-ssh-key-2026-05-16.key" ubuntu@163.192.9.157 "sudo systemctl stop cpa-helper && sudo cp /tmp/cpa-helper /opt/cpa-helper/cpa-helper && sudo chmod +x /opt/cpa-helper/cpa-helper && sudo systemctl start cpa-helper && sudo systemctl status cpa-helper --no-pager"

Write-Host "Done!"
