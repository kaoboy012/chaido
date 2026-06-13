@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
title Mo Tab Bot - Edge CDP 9222

cd /d "%~dp0"

echo.
echo ============================================================
echo  BUOC 1: Khoi dong Edge CDP 9222
echo ============================================================

REM Dong Edge cu
taskkill /F /IM msedge.exe /T >nul 2>&1
timeout /t 2 /nobreak >nul

REM Tim duong dan Edge
set EDGE_PATH=C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe
if not exist "%EDGE_PATH%" set EDGE_PATH=C:\Program Files\Microsoft\Edge\Application\msedge.exe
if not exist "%EDGE_PATH%" set EDGE_PATH=%LOCALAPPDATA%\Microsoft\Edge\Application\msedge.exe
if not exist "%EDGE_PATH%" (
    echo [LOI] Khong tim thay Microsoft Edge!
    echo [LOI] Hay kiem tra cai dat Edge hoac chinh BROWSER_EXE_PATH trong .env
    pause
    goto :eof
)

echo [OK] Tim thay Edge: %EDGE_PATH%
echo [OK] Mo Edge voi CDP port 9222...
start "" "%EDGE_PATH%" ^
    --remote-debugging-port=9222 ^
    --remote-debugging-address=127.0.0.1 ^
    --user-data-dir="%LOCALAPPDATA%\Microsoft\Edge\User Data" ^
    --profile-directory="Default" ^
    --no-first-run ^
    --mute-audio ^
    --window-size=715,771 ^
    --window-position=1205,295 ^
    --disable-blink-features=AutomationControlled ^
    --exclude-switches=enable-automation ^
    --disable-features=IsolateOrigins,site-per-process ^
    --flag-switches-begin ^
    --flag-switches-end

REM Poll CDP cho den khi san sang (toi da 30 giay)
echo [OK] Cho Edge san sang...
set CDP_OK=0
for /L %%i in (1,1,30) do (
    if !CDP_OK!==0 (
        curl -s --max-time 1 http://127.0.0.1:9222/json/version >nul 2>&1
        if !errorlevel!==0 (
            set CDP_OK=1
            echo [OK] CDP HTTP san sang sau %%i giay
        ) else (
            timeout /t 1 /nobreak >nul
        )
    )
)

if !CDP_OK!==0 (
    echo [LOI] Khong ket noi duoc CDP sau 30 giay!
    echo [LOI] Kiem tra xem Edge co chay khong.
    pause
    goto :eof
)

echo [OK] Cho WebSocket on dinh (3 giay)...
timeout /t 3 /nobreak >nul
echo [OK] Edge san sang!

REM Kiem tra Python
echo.
echo [CHECK] Kiem tra Python...
if exist "venv\Scripts\python.exe" (
    echo [OK] Tim thay Python venv
    set PYTHON_CMD=venv\Scripts\python.exe
) else (
    where python >nul 2>&1
    if !errorlevel! neq 0 (
        echo [LOI] Khong tim thay Python! Hay cai Python hoac tao venv.
        pause
        goto :eof
    )
    echo [OK] Dung Python he thong
    set PYTHON_CMD=python
)

REM Kiem tra main_script.py ton tai
if not exist "main_script.py" (
    echo [LOI] Khong tim thay main_script.py trong thu muc: %cd%
    pause
    goto :eof
)

REM Kiem tra .env
if not exist ".env" (
    echo [CANH BAO] Khong tim thay file .env - kiem tra cau hinh!
)

set RESTART_COUNT=0

:run_bot
echo.
echo ============================================================
echo  BUOC 2: CHAY BOT (lan %RESTART_COUNT%)
echo ============================================================
echo  Thu muc: %cd%
echo  Python : %PYTHON_CMD%
echo  Script : main_script.py
echo ============================================================
echo.

:bot_run
%PYTHON_CMD% main_script.py
set EXIT_CODE=!errorlevel!

echo.
echo ============================================================
echo  [BOT] Thoat voi ma: !EXIT_CODE!
echo ============================================================

REM Ma 0 = thoat binh thuong (chu dong)
if !EXIT_CODE!==0 (
    echo [BOT] Bot thoat binh thuong.
    goto :end
)

REM Ma -1073741510 (0xC000013A) = Ctrl+C / nguoi dung dung
if !EXIT_CODE!==4294967786 goto :user_stop
if !EXIT_CODE!==-1073741510 goto :user_stop
goto :do_restart

:user_stop
echo [BOT] Nguoi dung dung bot ^(Ctrl+C^).
goto :end

:do_restart
set /a RESTART_COUNT+=1
if !RESTART_COUNT! GEQ 5 (
    echo [CANH BAO] Bot da restart !RESTART_COUNT! lan lien tiep.
    echo [CANH BAO] Hay xem log: logs\bot_activity.log
    pause
    goto :end
)

echo [BOT] Loi ma !EXIT_CODE! - restart lan !RESTART_COUNT!/5 sau 10 giay...
echo [BOT] Xem log de biet nguyen nhan: logs\bot_activity.log
timeout /t 10 /nobreak >nul

REM Kiem tra lai Edge con chay khong
curl -s --max-time 3 http://127.0.0.1:9222/json/version >nul 2>&1
if !errorlevel! neq 0 (
    echo [CANH BAO] Edge da tat - dang mo lai...
    start "" "%EDGE_PATH%" ^
        --remote-debugging-port=9222 ^
        --remote-debugging-address=127.0.0.1 ^
        --user-data-dir="%LOCALAPPDATA%\Microsoft\Edge\User Data" ^
        --profile-directory="Default" ^
        --no-first-run --mute-audio ^
        --window-size=715,771 --window-position=1205,295 ^
        --disable-blink-features=AutomationControlled ^
        --exclude-switches=enable-automation
    for /L %%i in (1,1,20) do (
        curl -s --max-time 1 http://127.0.0.1:9222/json/version >nul 2>&1
        if !errorlevel!==0 (
            echo [OK] Edge da mo lai
            goto :edge_ok
        )
        timeout /t 1 /nobreak >nul
    )
    echo [CANH BAO] Edge khong mo lai duoc - thu chay bot truoc
    :edge_ok
    timeout /t 3 /nobreak >nul
)

goto :bot_run

:end
echo.
echo [BOT] Bot da dung hoan toan.
echo [INFO] Log cuoi: logs\bot_activity.log
pause
