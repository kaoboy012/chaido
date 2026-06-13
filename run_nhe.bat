@echo off
title Mo Edge Nhe (Co Cua So - Co The Debug)
cd /d "%~dp0"

echo.
echo ============================================================
echo  CHE DO NHE - CO CUA SO (De debug neu can)
echo  Microsoft Edge mo cua so nho (800x600), tat GPU, tat tieng.
echo  Nhe hon chay binh thuong, van co the nhin thay.
echo ============================================================
echo.

set EDGE_PATH=
if exist "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" (
    set EDGE_PATH=C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe
) else if exist "C:\Program Files\Microsoft\Edge\Application\msedge.exe" (
    set EDGE_PATH=C:\Program Files\Microsoft\Edge\Application\msedge.exe
) else if exist "%LOCALAPPDATA%\Microsoft\Edge\Application\msedge.exe" (
    set EDGE_PATH=%LOCALAPPDATA%\Microsoft\Edge\Application\msedge.exe
)

if "%EDGE_PATH%"=="" (
    echo [LOI] Khong tim thay Microsoft Edge!
    echo Hay cai dat Edge hoac kiem tra duong dan trong config.py
    pause
    exit /b 1
)

echo [OK] Dang mo Edge nhe voi CDP port 9222...

start "" "%EDGE_PATH%" ^
    --remote-debugging-port=9222 ^
    --remote-debugging-address=127.0.0.1 ^
    --profile-directory=Default ^
    --disable-gpu ^
    --disable-gpu-sandbox ^
    --disable-software-rasterizer ^
    --disable-dev-shm-usage ^
    --disable-extensions ^
    --disable-background-networking ^
    --disable-sync ^
    --disable-translate ^
    --no-first-run ^
    --mute-audio ^
    --window-size=800,600 ^
    --window-position=0,0 ^
    --memory-pressure-off ^
    --js-flags="--max-old-space-size=256"

echo [OK] Dang cho Edge khoi dong (3 giay)...
timeout /t 3 /nobreak >nul
echo [BOT] Dang chay bot...
echo.

if exist "venv\Scripts\python.exe" (
    venv\Scripts\python.exe main_script.py
) else (
    python main_script.py
)

pause
