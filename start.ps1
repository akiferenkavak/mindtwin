# ─────────────────────────────────────────────────────────────────
#   MindTwin Başlatma Scripti (start.ps1)
#   PowerShell'e kopyala-yapıştır ve çalıştır.
#   NOT: Torque producer başladıktan sonra CSV seçim penceresi açılır.
# ─────────────────────────────────────────────────────────────────

$PY = "C:\Users\yusuf\AppData\Local\Programs\Python\Python311\python.exe"
$ROOT = "C:\Users\yusuf\OneDrive\Desktop\SDP Code\mindtwin"

# 1) Eski Python proseslerini temizle
Write-Host "[1/5] Eski prosesler kapatiliyor..." -ForegroundColor Yellow
taskkill /F /IM python.exe /T 2>$null
Start-Sleep -Seconds 1

# 2) Gerekli paketleri kur (sadece ilk kez veya güncellemede)
Write-Host "[2/5] Paketler kontrol ediliyor..." -ForegroundColor Yellow
& $PY -m pip install -q fastapi uvicorn google-generativeai python-dotenv numpy scikit-learn joblib

# 3) Backend (FastAPI consumer)
Write-Host "[3/5] Backend baslatiliyor (port 8000)..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit","-Command",
    "Set-Location '$ROOT'; & '$PY' app\consumer.py"

Start-Sleep -Seconds 2

# 4) Thermal producer
Write-Host "[4/5] Thermal producer baslatiliyor..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit","-Command",
    "Set-Location '$ROOT'; & '$PY' app\producer_thermal.py"

# 5) Torque producer (CSV seçim penceresi açılır)
Write-Host "[5/5] Torque producer baslatiliyor (CSV sec penceresi acilacak)..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit","-Command",
    "Set-Location '$ROOT'; & '$PY' app\producer_torque.py"

Start-Sleep -Seconds 3

# 6) Tarayıcı
Write-Host "Tarayici aciliyor: http://localhost:8000" -ForegroundColor Green
Start-Process "http://localhost:8000"

Write-Host ""
Write-Host "✅ Sistem calisiyor! Torque producer'da CSV dosyasini secin." -ForegroundColor Green
Write-Host "   Ana sayfa : http://localhost:8000"
Write-Host "   Torque    : http://localhost:8000/torque"
Write-Host "   Thermal   : http://localhost:8000/thermal"
Write-Host "   Events    : http://localhost:8000/events"
Write-Host "   AI Model  : http://localhost:8000/autoencoder"
